from __future__ import annotations

import asyncio
from pathlib import Path
from threading import get_ident
from typing import Any
from uuid import UUID

import pytest
from fastapi import FastAPI

from meeting_action_orchestrator.application.delivery import DeliveryBatch, DeliveryResult
from meeting_action_orchestrator.application.recording_cleanup import (
    OrphanDiscoveryBatch,
    RecordingCleanupOutcome,
    RecordingCleanupResult,
)
from meeting_action_orchestrator.bootstrap import (
    RuntimeReadinessProbe,
    RuntimeSupervisor,
    _delivery_lease_duration,
    _delivery_targets,
    create_application,
)
from meeting_action_orchestrator.config import Settings
from meeting_action_orchestrator.domain.enums import ProcessingStage, WriteStatus


class FakeDatabase:
    def __init__(self) -> None:
        self.migrations = 0

    def migrate(self) -> int:
        self.migrations += 1
        return 2

    def healthcheck(self) -> bool:
        return True


class FakeProcessingRunner:
    def __init__(self) -> None:
        self.calls: list[tuple[ProcessingStage, int]] = []
        self.called = asyncio.Event()

    async def run_once(
        self,
        stage: ProcessingStage,
        *,
        limit: int = 1,
    ) -> tuple[object, ...]:
        self.calls.append((stage, limit))
        if stage is ProcessingStage.EXTRACTION:
            self.called.set()
        return ()


class FakeDeliveryRunner:
    def __init__(self) -> None:
        self.limits: list[int] = []
        self.called = asyncio.Event()

    async def run_once(self, limit: int = 20) -> DeliveryBatch:
        self.limits.append(limit)
        self.called.set()
        return DeliveryBatch(recovered=(), results=())


class FakeRecordingStorage:
    def __init__(self, ready: bool = True, error: Exception | None = None) -> None:
        self.ready = ready
        self.error = error
        self.thread_ids: list[int] = []

    def healthcheck(self) -> bool:
        self.thread_ids.append(get_ident())
        if self.error is not None:
            raise self.error
        return self.ready


class FakeRecordingCleanupRunner:
    def __init__(
        self,
        results: tuple[RecordingCleanupResult, ...] = (),
    ) -> None:
        self.results = results
        self.limits: list[int] = []
        self.called = asyncio.Event()

    async def run_once(self, limit: int = 20) -> tuple[RecordingCleanupResult, ...]:
        self.limits.append(limit)
        self.called.set()
        return self.results


class FakeOrphanDiscoveryRunner:
    def __init__(self, batch: OrphanDiscoveryBatch | None = None) -> None:
        self.batch = batch or OrphanDiscoveryBatch()
        self.limits: list[int] = []
        self.called = asyncio.Event()

    async def run_once(self, limit: int = 100) -> OrphanDiscoveryBatch:
        self.limits.append(limit)
        self.called.set()
        return self.batch


class FakeCloser:
    def __init__(self) -> None:
        self.closed = False

    async def close(self) -> None:
        self.closed = True


class FakeMcpClient:
    def __init__(self) -> None:
        self.events: list[str] = []
        self._connected = False

    @property
    def connected(self) -> bool:
        return self._connected

    async def start(self) -> None:
        self.events.append("start")
        self._connected = True

    async def close(self) -> None:
        self.events.append("close")
        self._connected = False

    def disconnect(self) -> None:
        self._connected = False


class FlakyMcpClient(FakeMcpClient):
    def __init__(self) -> None:
        super().__init__()
        self.attempts = 0

    async def start(self) -> None:
        self.attempts += 1
        self.events.append("start")
        if self.attempts == 1:
            raise OSError("unavailable")
        self._connected = True


class RecoveringMcpClient(FakeMcpClient):
    def __init__(self) -> None:
        super().__init__()
        self.attempts = 0
        self.reconnect_started = asyncio.Event()
        self.allow_reconnect = asyncio.Event()

    async def start(self) -> None:
        self.attempts += 1
        self.events.append("start")
        if self.attempts > 1:
            self.reconnect_started.set()
            await self.allow_reconnect.wait()
        self._connected = True


class DisconnectingDeliveryRunner(FakeDeliveryRunner):
    def __init__(self, mcp: RecoveringMcpClient) -> None:
        super().__init__()
        self.mcp = mcp
        self.disconnected = asyncio.Event()

    async def run_once(self, limit: int = 20) -> DeliveryBatch:
        self.limits.append(limit)
        self.called.set()
        if len(self.limits) == 1:
            self.mcp.disconnect()
            self.disconnected.set()
            result = DeliveryResult(UUID(int=1), WriteStatus.UNKNOWN)
            return DeliveryBatch(recovered=(), results=(result,))
        return DeliveryBatch(recovered=(), results=())


def settings(root: Path, **updates: object) -> Settings:
    values = {
        "_env_file": None,
        "database_path": root / "runtime.sqlite3",
        "upload_directory": root / "uploads",
        "api_bearer_token": "a" * 32,
        "openai_api_key": "test-openai-key",
        "worker_poll_interval_seconds": 0.01,
    }
    return Settings(**(values | updates))


def test_delivery_lease_covers_connector_timeout_and_session_cleanup(tmp_path: Path) -> None:
    configured = settings(tmp_path, mcp_call_timeout_seconds=300)

    lease = _delivery_lease_duration(configured)

    assert lease.total_seconds() == 630


async def test_supervisor_migrates_runs_workers_and_closes_mcp() -> None:
    database = FakeDatabase()
    processing = FakeProcessingRunner()
    delivery = FakeDeliveryRunner()
    cleanup = FakeRecordingCleanupRunner()
    discovery = FakeOrphanDiscoveryRunner()
    mcp = FakeMcpClient()
    runtime = RuntimeSupervisor(
        database=database,
        processing=processing,
        recording_storage=FakeRecordingStorage(),
        recording_cleanup=cleanup,
        orphan_discovery=discovery,
        delivery=delivery,
        mcp_client=mcp,
        poll_interval_seconds=0.01,
        processing_batch_size=2,
        recording_cleanup_batch_size=3,
        orphan_scan_interval_seconds=0.01,
        orphan_scan_batch_size=11,
        delivery_batch_size=7,
    )

    async with runtime.lifespan(FastAPI()):
        await asyncio.wait_for(processing.called.wait(), timeout=1)
        await asyncio.wait_for(delivery.called.wait(), timeout=1)
        await asyncio.wait_for(cleanup.called.wait(), timeout=1)
        await asyncio.wait_for(discovery.called.wait(), timeout=1)
        assert runtime.started
        assert runtime.delivery_ready

    assert database.migrations == 1
    assert processing.calls[:2] == [
        (ProcessingStage.TRANSCRIPTION, 2),
        (ProcessingStage.EXTRACTION, 2),
    ]
    assert delivery.limits == [1]
    assert cleanup.limits == [3]
    assert discovery.limits == [11]
    assert mcp.events == ["start", "close"]
    assert not runtime.started
    assert not runtime.delivery_ready


async def test_application_lifespan_is_offline_when_delivery_is_disabled(
    tmp_path: Path,
) -> None:
    app = create_application(settings(tmp_path))
    runtime = app.state.runtime

    assert runtime.delivery_mode == "disabled"
    assert _delivery_targets(settings(tmp_path)).task is None
    async with app.router.lifespan_context(app):
        readiness = await app.state.api_dependencies.readiness.check()
        assert readiness.ready
        assert [check.name for check in readiness.checks] == [
            "database",
            "recording_storage",
            "runtime",
            "delivery:disabled",
        ]

    assert (tmp_path / "runtime.sqlite3").is_file()
    assert not runtime.started


async def test_connector_outage_does_not_block_processing_startup() -> None:
    database = FakeDatabase()
    processing = FakeProcessingRunner()
    delivery = FakeDeliveryRunner()
    mcp = FlakyMcpClient()
    runtime = RuntimeSupervisor(
        database=database,
        processing=processing,
        recording_storage=FakeRecordingStorage(),
        recording_cleanup=FakeRecordingCleanupRunner(),
        orphan_discovery=FakeOrphanDiscoveryRunner(),
        delivery=delivery,
        mcp_client=mcp,
        poll_interval_seconds=0.01,
        processing_batch_size=1,
        recording_cleanup_batch_size=1,
        orphan_scan_interval_seconds=0.01,
        orphan_scan_batch_size=1,
    )

    async with runtime.lifespan(FastAPI()):
        await asyncio.wait_for(processing.called.wait(), timeout=1)
        await asyncio.wait_for(delivery.called.wait(), timeout=1)
        assert runtime.started
        assert runtime.delivery_ready

    assert mcp.events == ["start", "start", "close"]


async def test_connector_readiness_tracks_disconnection_and_recovery() -> None:
    database = FakeDatabase()
    processing = FakeProcessingRunner()
    mcp = RecoveringMcpClient()
    delivery = DisconnectingDeliveryRunner(mcp)
    runtime = RuntimeSupervisor(
        database=database,
        processing=processing,
        recording_storage=FakeRecordingStorage(),
        recording_cleanup=FakeRecordingCleanupRunner(),
        orphan_discovery=FakeOrphanDiscoveryRunner(),
        delivery=delivery,
        mcp_client=mcp,
        poll_interval_seconds=0.01,
        processing_batch_size=1,
        recording_cleanup_batch_size=1,
        orphan_scan_interval_seconds=0.01,
        orphan_scan_batch_size=1,
        delivery_batch_size=3,
    )
    readiness = RuntimeReadinessProbe(database, runtime)

    async with runtime.lifespan(FastAPI()):
        await asyncio.wait_for(delivery.disconnected.wait(), timeout=1)
        await asyncio.wait_for(mcp.reconnect_started.wait(), timeout=1)
        assert not runtime.delivery_ready
        assert not (await readiness.check()).ready
        assert delivery.limits == [1]
        mcp.allow_reconnect.set()
        await asyncio.wait_for(delivery.called.wait(), timeout=1)
        for _ in range(100):
            if runtime.delivery_ready and len(delivery.limits) > 1:
                break
            await asyncio.sleep(0.001)
        assert runtime.delivery_ready
        assert (await readiness.check()).ready
        assert delivery.limits[:2] == [1, 1]

    assert mcp.events == ["start", "start", "close"]


@pytest.mark.parametrize(
    "storage",
    [
        FakeRecordingStorage(ready=False),
        FakeRecordingStorage(error=OSError("storage unavailable")),
    ],
)
async def test_storage_preflight_failure_closes_resources_without_starting_workers(
    storage: FakeRecordingStorage,
) -> None:
    processing = FakeProcessingRunner()
    cleanup = FakeRecordingCleanupRunner()
    discovery = FakeOrphanDiscoveryRunner()
    closer = FakeCloser()
    runtime = RuntimeSupervisor(
        database=FakeDatabase(),
        processing=processing,
        recording_storage=storage,
        recording_cleanup=cleanup,
        orphan_discovery=discovery,
        poll_interval_seconds=0.01,
        processing_batch_size=1,
        recording_cleanup_batch_size=1,
        orphan_scan_interval_seconds=0.01,
        orphan_scan_batch_size=1,
        closeables=(closer,),
    )

    with pytest.raises(RuntimeError, match="storage preflight"):
        await runtime.start()

    assert not runtime.started
    assert not runtime.worker_ready
    assert not processing.calls
    assert not cleanup.limits
    assert not discovery.limits
    assert closer.closed


async def test_partial_worker_startup_is_cancelled_and_resources_are_closed(
    monkeypatch: Any,
) -> None:
    original_create_task = asyncio.create_task
    created: list[asyncio.Task[Any]] = []
    attempts = 0

    def create_task(coroutine: Any, *, name: str | None = None) -> asyncio.Task[Any]:
        nonlocal attempts
        attempts += 1
        if attempts == 2:
            coroutine.close()
            raise RuntimeError("task startup unavailable")
        task = original_create_task(coroutine, name=name)
        created.append(task)
        return task

    monkeypatch.setattr(asyncio, "create_task", create_task)
    closer = FakeCloser()
    runtime = RuntimeSupervisor(
        database=FakeDatabase(),
        processing=FakeProcessingRunner(),
        recording_storage=FakeRecordingStorage(),
        recording_cleanup=FakeRecordingCleanupRunner(),
        orphan_discovery=FakeOrphanDiscoveryRunner(),
        poll_interval_seconds=0.01,
        processing_batch_size=1,
        recording_cleanup_batch_size=1,
        orphan_scan_interval_seconds=0.01,
        orphan_scan_batch_size=1,
        closeables=(closer,),
    )

    with pytest.raises(RuntimeError, match="task startup"):
        await runtime.start()

    assert len(created) == 1
    assert created[0].cancelled()
    assert closer.closed
    assert not runtime.worker_ready


async def test_permanent_cleanup_result_does_not_change_readiness() -> None:
    database = FakeDatabase()
    storage = FakeRecordingStorage()
    cleanup = FakeRecordingCleanupRunner(
        (
            RecordingCleanupResult(
                job_id=UUID(int=10),
                outcome=RecordingCleanupOutcome.FAILED,
                job=None,
            ),
        )
    )
    runtime = RuntimeSupervisor(
        database=database,
        processing=FakeProcessingRunner(),
        recording_storage=storage,
        recording_cleanup=cleanup,
        orphan_discovery=FakeOrphanDiscoveryRunner(),
        poll_interval_seconds=0.01,
        processing_batch_size=1,
        recording_cleanup_batch_size=1,
        orphan_scan_interval_seconds=0.01,
        orphan_scan_batch_size=1,
    )
    readiness = RuntimeReadinessProbe(database, runtime)
    loop_thread = get_ident()

    async with runtime.lifespan(FastAPI()):
        await asyncio.wait_for(cleanup.called.wait(), timeout=1)
        result = await readiness.check()

    assert result.ready
    assert storage.thread_ids
    assert loop_thread not in storage.thread_ids


async def test_mcp_url_without_targets_keeps_delivery_disabled(tmp_path: Path) -> None:
    app = create_application(
        settings(tmp_path, mcp_server_url="https://mcp.example.com/actions"),
    )

    assert app.state.runtime.delivery_mode == "disabled"
    async with app.router.lifespan_context(app):
        assert app.state.runtime.delivery_ready


def test_delivery_target_requires_mcp_endpoint(tmp_path: Path) -> None:
    configured = settings(tmp_path, mcp_task_resource_id="inbox")

    with pytest.raises(ValueError, match="MCP_SERVER_URL"):
        create_application(configured)


def test_application_requires_runtime_credentials(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="API_BEARER_TOKEN"):
        create_application(
            settings(tmp_path, api_bearer_token=None),
        )

    with pytest.raises(ValueError, match="OPENAI_API_KEY"):
        create_application(
            settings(tmp_path, openai_api_key=None),
        )
