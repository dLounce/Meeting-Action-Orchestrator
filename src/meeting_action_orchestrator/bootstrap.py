from __future__ import annotations

import asyncio
import logging
import math
import os
import socket
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Protocol
from uuid import UUID

from fastapi import FastAPI

from meeting_action_orchestrator.agents.specialists import MeetingSpecialists
from meeting_action_orchestrator.api.adapters import (
    AsyncDeliveryFacade,
    AsyncWorkflowFacade,
    UnitOfWorkQueryFacade,
)
from meeting_action_orchestrator.api.app import create_app
from meeting_action_orchestrator.api.auth import StaticBearerAuthenticator
from meeting_action_orchestrator.api.contracts import (
    ApiDependencies,
    DeliveryResult,
    DeliveryService,
    ReadinessCheck,
    ReadinessResult,
)
from meeting_action_orchestrator.application.delivery import (
    ApprovedOutboxExecutor,
    DeliveryBatch,
    PersistedApprovalAuthorizer,
)
from meeting_action_orchestrator.application.delivery import (
    FullJitterRetryScheduler as DeliveryRetryScheduler,
)
from meeting_action_orchestrator.application.delivery_control import DeliveryControlService
from meeting_action_orchestrator.application.errors import OperationConflictError
from meeting_action_orchestrator.application.mapping import DeliveryTargets
from meeting_action_orchestrator.application.processing import (
    FullJitterRetryScheduler as ProcessingRetryScheduler,
)
from meeting_action_orchestrator.application.processing import (
    ProcessingWorker,
)
from meeting_action_orchestrator.application.workflow import MeetingWorkflow, SystemClock
from meeting_action_orchestrator.config import Settings, get_settings
from meeting_action_orchestrator.domain.enums import ProcessingStage
from meeting_action_orchestrator.domain.models import ConnectorTarget
from meeting_action_orchestrator.infrastructure.audio import (
    FFprobeAudioInspector,
    LocalAudioStore,
)
from meeting_action_orchestrator.infrastructure.database import Database
from meeting_action_orchestrator.infrastructure.mcp_client import ManagedMcpHttpClient
from meeting_action_orchestrator.infrastructure.mcp_gateway import McpGateway, McpToolNames
from meeting_action_orchestrator.infrastructure.openai_agents import OpenAIAgentsRunner
from meeting_action_orchestrator.infrastructure.openai_transcription import OpenAITranscriber
from meeting_action_orchestrator.infrastructure.repositories import SqliteUnitOfWork

logger = logging.getLogger(__name__)


class MigratableDatabase(Protocol):
    def migrate(self) -> int: ...

    def healthcheck(self) -> bool: ...


class ProcessingRunner(Protocol):
    async def run_once(
        self,
        stage: ProcessingStage,
        *,
        limit: int = 1,
    ) -> tuple[object, ...]: ...


class DeliveryRunner(Protocol):
    async def run_once(self, limit: int = 20) -> DeliveryBatch: ...


class McpLifecycle(Protocol):
    @property
    def connected(self) -> bool: ...

    async def start(self) -> None: ...

    async def close(self) -> None: ...


class AsyncCloser(Protocol):
    async def close(self) -> None: ...


@dataclass(slots=True)
class RuntimeSupervisor:
    database: MigratableDatabase
    processing: ProcessingRunner
    poll_interval_seconds: float
    processing_batch_size: int
    delivery: DeliveryRunner | None = None
    delivery_batch_size: int = 20
    mcp_client: McpLifecycle | None = None
    closeables: tuple[AsyncCloser, ...] = ()
    _started: bool = field(default=False, init=False)
    _stopped: bool = field(default=False, init=False)
    _tasks: list[asyncio.Task[None]] = field(default_factory=list, init=False)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False)

    def __post_init__(self) -> None:
        if not math.isfinite(self.poll_interval_seconds) or not (
            0 < self.poll_interval_seconds <= 60
        ):
            raise ValueError("poll_interval_seconds must be greater than zero and at most 60")
        if not 1 <= self.processing_batch_size <= 10:
            raise ValueError("processing_batch_size must be between one and 10")
        if not 1 <= self.delivery_batch_size <= 100:
            raise ValueError("delivery_batch_size must be between one and 100")

    @property
    def delivery_mode(self) -> str:
        return "mcp" if self.mcp_client is not None else "disabled"

    @property
    def delivery_ready(self) -> bool:
        return self.mcp_client is None or self.mcp_client.connected

    @property
    def started(self) -> bool:
        return self._started

    @property
    def worker_ready(self) -> bool:
        return self._started and bool(self._tasks) and all(not task.done() for task in self._tasks)

    async def start(self) -> None:
        async with self._lock:
            if self._started:
                return
            if self._stopped:
                raise RuntimeError("The runtime supervisor cannot be restarted after shutdown")
            version = await asyncio.to_thread(self.database.migrate)
            try:
                self._tasks = [
                    asyncio.create_task(
                        self._processing_loop(),
                        name="meeting-processing-worker",
                    )
                ]
                if self.delivery is not None:
                    self._tasks.append(
                        asyncio.create_task(
                            self._delivery_loop(),
                            name="meeting-delivery-worker",
                        )
                    )
                self._started = True
            except BaseException:
                await self._cancel_workers()
                await self._close_resources()
                self._stopped = True
                raise
            logger.info(
                "runtime started",
                extra={
                    "fields": {
                        "database_version": version,
                        "delivery_mode": self.delivery_mode,
                        "worker_count": len(self._tasks),
                    }
                },
            )

    async def stop(self) -> None:
        async with self._lock:
            if not self._started and not self._tasks:
                return
            await self._cancel_workers()
            try:
                await self._close_resources()
            finally:
                self._started = False
                self._stopped = True
            logger.info("runtime stopped")

    @asynccontextmanager
    async def lifespan(self, app: FastAPI) -> AsyncIterator[None]:
        del app
        await self.start()
        try:
            yield
        finally:
            await self.stop()

    async def _processing_loop(self) -> None:
        while True:
            try:
                completed = 0
                for stage in (ProcessingStage.TRANSCRIPTION, ProcessingStage.EXTRACTION):
                    results = await self.processing.run_once(
                        stage,
                        limit=self.processing_batch_size,
                    )
                    completed += len(results)
                if completed:
                    logger.info(
                        "processing batch completed",
                        extra={"fields": {"job_count": completed}},
                    )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "processing worker cycle failed",
                    extra={"fields": {"worker": "processing"}},
                )
            await asyncio.sleep(self.poll_interval_seconds)

    async def _delivery_loop(self) -> None:
        if self.delivery is None:
            return
        while True:
            try:
                await self._connect_mcp()
                for _ in range(self.delivery_batch_size):
                    batch = await self.delivery.run_once(1)
                    if not batch.results or not self.delivery_ready:
                        break
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "delivery worker cycle failed",
                    extra={"fields": {"worker": "delivery"}},
                )
            await asyncio.sleep(self.poll_interval_seconds)

    async def _connect_mcp(self) -> None:
        if self.mcp_client is None or self.mcp_client.connected:
            return
        await self.mcp_client.start()
        logger.info("delivery connector ready")

    async def _cancel_workers(self) -> None:
        tasks, self._tasks = self._tasks, []
        for task in tasks:
            task.cancel()
        if not tasks:
            return
        results = await asyncio.gather(*tasks, return_exceptions=True)
        failures = sum(
            isinstance(result, BaseException) and not isinstance(result, asyncio.CancelledError)
            for result in results
        )
        if failures:
            logger.error(
                "worker shutdown failed",
                extra={"fields": {"failure_count": failures}},
            )

    async def _close_resources(self) -> None:
        resources: tuple[AsyncCloser, ...] = (
            *((self.mcp_client,) if self.mcp_client is not None else ()),
            *self.closeables,
        )
        results = await asyncio.gather(
            *(resource.close() for resource in resources),
            return_exceptions=True,
        )
        failures = sum(isinstance(result, BaseException) for result in results)
        if failures:
            logger.error(
                "runtime resource shutdown failed",
                extra={"fields": {"failure_count": failures}},
            )


class RuntimeReadinessProbe:
    def __init__(
        self,
        database: MigratableDatabase,
        runtime: RuntimeSupervisor,
    ) -> None:
        self._database = database
        self._runtime = runtime

    async def check(self) -> ReadinessResult:
        database_ready = await asyncio.to_thread(self._healthcheck)
        delivery_name = f"delivery:{self._runtime.delivery_mode}"
        return ReadinessResult(
            (
                ReadinessCheck("database", database_ready),
                ReadinessCheck("runtime", self._runtime.worker_ready),
                ReadinessCheck(delivery_name, self._runtime.delivery_ready),
            )
        )

    def _healthcheck(self) -> bool:
        try:
            return self._database.healthcheck() is True
        except Exception:
            return False


class UnavailableDeliveryService:
    async def retry(
        self,
        meeting_id: UUID,
        *,
        intent_ids: tuple[UUID, ...],
        request_key: str,
        actor_id: str,
    ) -> DeliveryResult:
        del meeting_id, intent_ids, request_key, actor_id
        raise OperationConflictError("Delivery connectors are not configured")

    async def reconcile(
        self,
        meeting_id: UUID,
        *,
        intent_ids: tuple[UUID, ...],
        request_key: str,
        actor_id: str,
    ) -> DeliveryResult:
        del meeting_id, intent_ids, request_key, actor_id
        raise OperationConflictError("Delivery connectors are not configured")


def create_application(settings: Settings | None = None) -> FastAPI:
    configured = settings or get_settings()
    api_token = configured.require_api_bearer_token()
    openai_key = configured.require_openai_api_key()
    database = Database(configured.database_path)

    def write_unit_of_work() -> SqliteUnitOfWork:
        return SqliteUnitOfWork(database)

    def read_unit_of_work() -> SqliteUnitOfWork:
        return SqliteUnitOfWork(database, immediate=False)

    clock = SystemClock()
    targets = _delivery_targets(configured)
    recording_store = LocalAudioStore(
        configured.upload_directory,
        FFprobeAudioInspector(),
        configured.max_upload_bytes,
    )
    transcriber = OpenAITranscriber(
        api_key=openai_key,
        model=configured.openai_transcription_model,
        timeout_seconds=configured.openai_timeout_seconds,
        max_retries=configured.openai_max_retries,
    )
    agents_runner = OpenAIAgentsRunner(
        api_key=openai_key,
        timeout_seconds=configured.openai_timeout_seconds,
        max_retries=configured.openai_max_retries,
        tracing_enabled=configured.openai_tracing_enabled,
    )
    specialists = MeetingSpecialists(
        agents_runner,
        worker_model=configured.openai_worker_model,
        recap_model=configured.openai_recap_model,
        extractor_max_output_tokens=configured.openai_extractor_max_output_tokens,
        recap_max_output_tokens=configured.openai_recap_max_output_tokens,
        verifier_max_output_tokens=configured.openai_verifier_max_output_tokens,
    )
    workflow = MeetingWorkflow(
        unit_of_work=write_unit_of_work,
        recording_store=recording_store,
        transcriber=transcriber,
        specialists=specialists,
        clock=clock,
        delivery_targets=targets,
        max_agent_requests=configured.openai_max_requests_per_run,
        max_agent_output_tokens=configured.openai_max_output_tokens_per_run,
    )
    processing = ProcessingWorker(
        unit_of_work=write_unit_of_work,
        handlers=workflow.processing_handlers(),
        clock=clock,
        retry_scheduler=ProcessingRetryScheduler(),
        worker_id=_worker_id("processing"),
    )
    mcp_client, delivery_executor = _delivery_runtime(
        configured,
        database,
        clock,
    )
    runtime = RuntimeSupervisor(
        database=database,
        processing=processing,
        poll_interval_seconds=configured.worker_poll_interval_seconds,
        processing_batch_size=configured.processing_batch_size,
        delivery=delivery_executor,
        delivery_batch_size=configured.delivery_batch_size,
        mcp_client=mcp_client,
        closeables=(transcriber, agents_runner),
    )
    workflow_facade = AsyncWorkflowFacade(workflow)
    delivery_service: DeliveryService
    if delivery_executor is None:
        delivery_service = UnavailableDeliveryService()
    else:
        delivery_control = DeliveryControlService(
            unit_of_work=write_unit_of_work,
            reconciler=delivery_executor,
            clock=clock,
            retry_scheduler=DeliveryRetryScheduler(),
        )
        delivery_service = AsyncDeliveryFacade(delivery_control)
    dependencies = ApiDependencies(
        workflow=workflow_facade,
        queries=UnitOfWorkQueryFacade(read_unit_of_work),
        reviews=workflow_facade,
        deliveries=delivery_service,
        authenticator=StaticBearerAuthenticator(
            api_token,
            configured.api_actor_subject,
        ),
        readiness=RuntimeReadinessProbe(database, runtime),
        max_upload_bytes=configured.max_upload_bytes,
    )
    app = create_app(dependencies, lifespan=runtime.lifespan)
    app.state.runtime = runtime
    return app


def _delivery_targets(settings: Settings) -> DeliveryTargets:
    if settings.mcp_server_url is None and (
        settings.mcp_task_resource_id or settings.mcp_calendar_resource_id
    ):
        raise ValueError("MCP_SERVER_URL is required when a delivery target is configured")
    task = (
        ConnectorTarget(
            connector_id=settings.mcp_connector_id,
            resource_id=settings.mcp_task_resource_id,
        )
        if settings.mcp_task_resource_id is not None
        else None
    )
    calendar = (
        ConnectorTarget(
            connector_id=settings.mcp_connector_id,
            resource_id=settings.mcp_calendar_resource_id,
        )
        if settings.mcp_calendar_resource_id is not None
        else None
    )
    return DeliveryTargets(task=task, calendar=calendar)


def _delivery_runtime(
    settings: Settings,
    database: Database,
    clock: SystemClock,
) -> tuple[ManagedMcpHttpClient | None, ApprovedOutboxExecutor | None]:
    if settings.mcp_server_url is None or not (
        settings.mcp_task_resource_id or settings.mcp_calendar_resource_id
    ):
        return None, None
    auth_token = (
        settings.mcp_auth_token.get_secret_value() if settings.mcp_auth_token is not None else None
    )
    client = ManagedMcpHttpClient(
        settings.mcp_server_url,
        bearer_token=auth_token,
        request_timeout_seconds=settings.mcp_request_timeout_seconds,
        sse_read_timeout_seconds=settings.mcp_sse_timeout_seconds,
        call_timeout_seconds=settings.mcp_call_timeout_seconds,
    )

    def write_unit_of_work() -> SqliteUnitOfWork:
        return SqliteUnitOfWork(database)

    def read_unit_of_work() -> SqliteUnitOfWork:
        return SqliteUnitOfWork(database, immediate=False)

    gateway = McpGateway(
        client,
        McpToolNames(
            task=settings.mcp_task_tool,
            calendar=settings.mcp_calendar_tool,
            lookup=settings.mcp_lookup_tool,
        ),
        PersistedApprovalAuthorizer(read_unit_of_work, clock),
        clock=clock.now,
    )
    executor = ApprovedOutboxExecutor(
        unit_of_work=write_unit_of_work,
        gateway=gateway,
        clock=clock,
        retry_scheduler=DeliveryRetryScheduler(),
        worker_id=_worker_id("delivery"),
    )
    return client, executor


def _worker_id(role: str) -> str:
    return f"{socket.gethostname()}:{os.getpid()}:{role}"[:200]
