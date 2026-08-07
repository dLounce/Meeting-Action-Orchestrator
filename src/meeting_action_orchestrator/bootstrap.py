from __future__ import annotations

import asyncio
import logging
import math
import os
import socket
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import timedelta
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
from meeting_action_orchestrator.api.erasure_adapters import AsyncErasureFacade
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
from meeting_action_orchestrator.application.meeting_erasure import (
    ErasureKeyRegistry,
    MeetingErasureRemediationService,
    MeetingErasureService,
)
from meeting_action_orchestrator.application.meeting_erasure_worker import (
    MeetingErasureWorker,
    MeetingErasureWorkerResult,
)
from meeting_action_orchestrator.application.processing import (
    FullJitterRetryScheduler as ProcessingRetryScheduler,
)
from meeting_action_orchestrator.application.processing import (
    ProcessingScheduler,
    ProcessingWorker,
)
from meeting_action_orchestrator.application.processing_control import ProcessingControlService
from meeting_action_orchestrator.application.provider_budget import ProviderBudgetService
from meeting_action_orchestrator.application.recording_cleanup import (
    OrphanDiscoveryBatch,
    RecordingCleanupOutcome,
    RecordingCleanupResult,
    RecordingCleanupScheduler,
    RecordingCleanupWorker,
    RecordingOrphanDiscoverer,
)
from meeting_action_orchestrator.application.workflow import MeetingWorkflow, SystemClock
from meeting_action_orchestrator.config import Settings, get_settings
from meeting_action_orchestrator.domain.enums import ProcessingStage
from meeting_action_orchestrator.domain.models import ConnectorTarget
from meeting_action_orchestrator.domain.provider_budget import ProviderBudgetLimits
from meeting_action_orchestrator.infrastructure.audio import (
    FFprobeAudioInspector,
    LocalAudioStore,
)
from meeting_action_orchestrator.infrastructure.database import Database
from meeting_action_orchestrator.infrastructure.erasure_tokens import ErasureTokenKeyring
from meeting_action_orchestrator.infrastructure.mcp_client import ManagedMcpHttpClient
from meeting_action_orchestrator.infrastructure.mcp_gateway import McpGateway, McpToolNames
from meeting_action_orchestrator.infrastructure.openai_agents import OpenAIAgentsRunner
from meeting_action_orchestrator.infrastructure.openai_transcription import OpenAITranscriber
from meeting_action_orchestrator.infrastructure.recording_quarantine import (
    LocalRecordingQuarantine,
)
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


class RecordingCleanupRunner(Protocol):
    async def run_once(self, limit: int = 20) -> tuple[RecordingCleanupResult, ...]: ...


class OrphanDiscoveryRunner(Protocol):
    async def run_once(self, limit: int = 100) -> OrphanDiscoveryBatch: ...


class ErasureKeyRegistryLifecycle(Protocol):
    async def ensure_registered(self) -> tuple[str, ...]: ...

    async def validate_registered(self) -> tuple[str, ...]: ...


class MeetingErasureRunner(Protocol):
    async def run_once(self, limit: int = 20) -> tuple[MeetingErasureWorkerResult, ...]: ...


class RecordingStorage(Protocol):
    def healthcheck(self) -> bool: ...


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
    recording_storage: RecordingStorage
    recording_cleanup: RecordingCleanupRunner
    orphan_discovery: OrphanDiscoveryRunner
    erasure_key_registry: ErasureKeyRegistryLifecycle
    meeting_erasure: MeetingErasureRunner
    poll_interval_seconds: float
    processing_batch_size: int
    recording_cleanup_batch_size: int
    meeting_erasure_batch_size: int
    orphan_scan_interval_seconds: float
    orphan_scan_batch_size: int
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
        if not 1 <= self.recording_cleanup_batch_size <= 100:
            raise ValueError("recording_cleanup_batch_size must be between one and 100")
        if not 1 <= self.meeting_erasure_batch_size <= 100:
            raise ValueError("meeting_erasure_batch_size must be between one and 100")
        if not math.isfinite(self.orphan_scan_interval_seconds) or not (
            0 < self.orphan_scan_interval_seconds <= 86_400
        ):
            raise ValueError(
                "orphan_scan_interval_seconds must be greater than zero and at most 86400"
            )
        if not 1 <= self.orphan_scan_batch_size <= 1_000:
            raise ValueError("orphan_scan_batch_size must be between one and 1000")

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

    @property
    def erasure_worker_ready(self) -> bool:
        return self._started and any(
            task.get_name() == "meeting-erasure-worker" and not task.done() for task in self._tasks
        )

    async def erasure_keys_ready(self) -> bool:
        try:
            await self.erasure_key_registry.validate_registered()
        except Exception:
            return False
        return True

    async def start(self) -> None:
        async with self._lock:
            if self._started:
                return
            if self._stopped:
                raise RuntimeError("The runtime supervisor cannot be restarted after shutdown")
            try:
                version = await asyncio.to_thread(self.database.migrate)
                key_ids = await self.erasure_key_registry.ensure_registered()
                storage_ready = await asyncio.to_thread(self.storage_healthcheck)
                if not storage_ready:
                    raise RuntimeError("Recording storage preflight failed")
                self._tasks.append(
                    asyncio.create_task(
                        self._processing_loop(),
                        name="meeting-processing-worker",
                    )
                )
                self._tasks.append(
                    asyncio.create_task(
                        self._recording_cleanup_loop(),
                        name="recording-cleanup-worker",
                    )
                )
                self._tasks.append(
                    asyncio.create_task(
                        self._meeting_erasure_loop(),
                        name="meeting-erasure-worker",
                    )
                )
                self._tasks.append(
                    asyncio.create_task(
                        self._orphan_discovery_loop(),
                        name="recording-orphan-discovery",
                    )
                )
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
                        "erasure_key_count": len(key_ids),
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

    async def _recording_cleanup_loop(self) -> None:
        while True:
            try:
                results = await self.recording_cleanup.run_once(self.recording_cleanup_batch_size)
                if results:
                    logger.info(
                        "recording cleanup batch completed",
                        extra={
                            "fields": {
                                "failed_count": sum(
                                    result.outcome is RecordingCleanupOutcome.FAILED
                                    for result in results
                                ),
                                "job_count": len(results),
                                "lease_lost_count": sum(
                                    result.outcome is RecordingCleanupOutcome.LEASE_LOST
                                    for result in results
                                ),
                            }
                        },
                    )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.error(
                    "recording cleanup worker cycle failed",
                    extra={"fields": {"worker": "recording_cleanup"}},
                )
            await asyncio.sleep(self.poll_interval_seconds)

    async def _orphan_discovery_loop(self) -> None:
        while True:
            try:
                batch = await self.orphan_discovery.run_once(self.orphan_scan_batch_size)
                if batch.scanned:
                    logger.info(
                        "recording orphan scan completed",
                        extra={
                            "fields": {
                                "candidate_count": batch.scanned,
                                "rejected_count": batch.rejected,
                                "scheduled_count": batch.scheduled,
                            }
                        },
                    )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.error(
                    "recording orphan scan failed",
                    extra={"fields": {"worker": "recording_orphan_discovery"}},
                )
            await asyncio.sleep(self.orphan_scan_interval_seconds)

    async def _meeting_erasure_loop(self) -> None:
        while True:
            try:
                results = await self.meeting_erasure.run_once(self.meeting_erasure_batch_size)
                if results:
                    logger.info(
                        "meeting erasure batch completed",
                        extra={"fields": {"job_count": len(results)}},
                    )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.error(
                    "meeting erasure worker cycle failed",
                    extra={"fields": {"worker": "meeting_erasure"}},
                )
            await asyncio.sleep(self.poll_interval_seconds)

    async def _connect_mcp(self) -> None:
        if self.mcp_client is None or self.mcp_client.connected:
            return
        await self.mcp_client.start()
        logger.info("delivery connector ready")

    def storage_healthcheck(self) -> bool:
        try:
            return self.recording_storage.healthcheck() is True
        except Exception:
            return False

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
        database_ready, storage_ready, erasure_keys_ready = await asyncio.gather(
            asyncio.to_thread(self._healthcheck),
            asyncio.to_thread(self._runtime.storage_healthcheck),
            self._runtime.erasure_keys_ready(),
        )
        delivery_name = f"delivery:{self._runtime.delivery_mode}"
        return ReadinessResult(
            (
                ReadinessCheck("database", database_ready),
                ReadinessCheck("recording_storage", storage_ready),
                ReadinessCheck("erasure_keys", erasure_keys_ready),
                ReadinessCheck("runtime", self._runtime.worker_ready),
                ReadinessCheck("erasure_worker", self._runtime.erasure_worker_ready),
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
    active_erasure_key_id, encoded_erasure_keys = configured.require_erasure_hmac_configuration()
    erasure_tokens = ErasureTokenKeyring.from_encoded(
        active_erasure_key_id,
        encoded_erasure_keys,
    )
    api_token = configured.require_api_bearer_token()
    openai_key = configured.require_openai_api_key()
    database = Database(configured.database_path)

    def write_unit_of_work() -> SqliteUnitOfWork:
        return SqliteUnitOfWork(database)

    def read_unit_of_work() -> SqliteUnitOfWork:
        return SqliteUnitOfWork(database, immediate=False)

    clock = SystemClock()
    erasure_key_registry = ErasureKeyRegistry(
        unit_of_work=write_unit_of_work,
        validation_unit_of_work=read_unit_of_work,
        tokens=erasure_tokens,
        clock=clock,
    )
    erasure_requests = MeetingErasureService(
        unit_of_work=write_unit_of_work,
        tokens=erasure_tokens,
        key_registry=erasure_key_registry,
        clock=clock,
        max_remediations=configured.meeting_erasure_max_remediations,
    )
    erasure_remediations = MeetingErasureRemediationService(
        unit_of_work=write_unit_of_work,
        tokens=erasure_tokens,
        key_registry=erasure_key_registry,
        clock=clock,
    )
    targets = _delivery_targets(configured)
    recording_store = LocalAudioStore(
        configured.upload_directory,
        FFprobeAudioInspector(),
        configured.max_upload_bytes,
    )
    recording_quarantine = LocalRecordingQuarantine(configured.upload_directory)
    recording_cleanup_scheduler = RecordingCleanupScheduler(
        unit_of_work=write_unit_of_work,
        clock=clock,
    )
    provider_budget = ProviderBudgetService(
        unit_of_work=write_unit_of_work,
        clock=clock,
    )
    transcriber = OpenAITranscriber(
        api_key=openai_key,
        model=configured.openai_transcription_model,
        budget_controller=provider_budget,
        timeout_seconds=configured.openai_timeout_seconds,
        max_retries=configured.openai_max_retries,
    )
    agents_runner = OpenAIAgentsRunner(
        api_key=openai_key,
        budget_controller=provider_budget,
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
    processing_scheduler = ProcessingScheduler(
        unit_of_work=write_unit_of_work,
        clock=clock,
        budget_limits={
            ProcessingStage.TRANSCRIPTION: ProviderBudgetLimits(
                provider_request_limit=(configured.openai_transcription_provider_request_limit),
                audio_duration_ms_limit=(configured.openai_transcription_audio_duration_ms_limit),
            ),
            ProcessingStage.EXTRACTION: ProviderBudgetLimits(
                preflight_request_limit=(configured.openai_extraction_preflight_request_limit),
                provider_request_limit=(configured.openai_extraction_provider_request_limit),
                input_token_limit=configured.openai_extraction_input_token_limit,
                output_token_limit=configured.openai_extraction_output_token_limit,
            ),
        },
        budget_policy_version=configured.openai_budget_policy_version,
    )
    workflow = MeetingWorkflow(
        unit_of_work=write_unit_of_work,
        recording_store=recording_store,
        erasure_tokens=erasure_tokens,
        transcriber=transcriber,
        specialists=specialists,
        clock=clock,
        delivery_targets=targets,
        processing_scheduler=processing_scheduler,
        recording_cleanup_scheduler=recording_cleanup_scheduler,
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
    recording_cleanup = RecordingCleanupWorker(
        unit_of_work=write_unit_of_work,
        executor=recording_quarantine,
        clock=clock,
        retry_scheduler=ProcessingRetryScheduler(),
        worker_id=_worker_id("recording-cleanup"),
        lease_duration=timedelta(seconds=configured.recording_cleanup_lease_seconds),
    )
    meeting_erasure = MeetingErasureWorker(
        unit_of_work=write_unit_of_work,
        checkpoint=database,
        clock=clock,
        retry_scheduler=ProcessingRetryScheduler(),
        worker_id=_worker_id("meeting-erasure"),
        lease_duration=timedelta(seconds=configured.meeting_erasure_lease_seconds),
    )
    orphan_discovery = RecordingOrphanDiscoverer(
        unit_of_work=read_unit_of_work,
        scheduler=recording_cleanup_scheduler,
        scanner=recording_quarantine,
        active_recordings=recording_store,
        clock=clock,
        grace_period=timedelta(seconds=configured.recording_orphan_grace_seconds),
    )
    mcp_client, delivery_executor = _delivery_runtime(
        configured,
        database,
        clock,
    )
    runtime = RuntimeSupervisor(
        database=database,
        processing=processing,
        recording_storage=recording_quarantine,
        recording_cleanup=recording_cleanup,
        orphan_discovery=orphan_discovery,
        erasure_key_registry=erasure_key_registry,
        meeting_erasure=meeting_erasure,
        poll_interval_seconds=configured.worker_poll_interval_seconds,
        processing_batch_size=configured.processing_batch_size,
        recording_cleanup_batch_size=configured.recording_cleanup_batch_size,
        meeting_erasure_batch_size=configured.meeting_erasure_batch_size,
        orphan_scan_interval_seconds=configured.recording_orphan_scan_interval_seconds,
        orphan_scan_batch_size=configured.recording_orphan_scan_batch_size,
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
            operation_lease_duration=_delivery_lease_duration(configured),
        )
        delivery_service = AsyncDeliveryFacade(delivery_control)
    dependencies = ApiDependencies(
        workflow=workflow_facade,
        queries=UnitOfWorkQueryFacade(read_unit_of_work),
        processing_controls=ProcessingControlService(
            unit_of_work=write_unit_of_work,
            clock=clock,
        ),
        reviews=workflow_facade,
        deliveries=delivery_service,
        erasures=AsyncErasureFacade(
            erasure_requests,
            erasure_remediations,
            read_unit_of_work,
        ),
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
        lease_duration=_delivery_lease_duration(settings),
        reconciliation_lease_duration=_delivery_lease_duration(settings),
    )
    return client, executor


def _delivery_lease_duration(settings: Settings) -> timedelta:
    return timedelta(seconds=(settings.mcp_call_timeout_seconds * 2) + 30)


def _worker_id(role: str) -> str:
    return f"{socket.gethostname()}:{os.getpid()}:{role}"[:200]
