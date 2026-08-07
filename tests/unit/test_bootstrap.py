from __future__ import annotations

import asyncio
from pathlib import Path
from uuid import UUID

import pytest
from fastapi import FastAPI

from meeting_action_orchestrator.application.delivery import DeliveryBatch, DeliveryResult
from meeting_action_orchestrator.bootstrap import (
    RuntimeReadinessProbe,
    RuntimeSupervisor,
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


async def test_supervisor_migrates_runs_workers_and_closes_mcp() -> None:
    database = FakeDatabase()
    processing = FakeProcessingRunner()
    delivery = FakeDeliveryRunner()
    mcp = FakeMcpClient()
    runtime = RuntimeSupervisor(
        database=database,
        processing=processing,
        delivery=delivery,
        mcp_client=mcp,
        poll_interval_seconds=0.01,
        processing_batch_size=2,
        delivery_batch_size=7,
    )

    async with runtime.lifespan(FastAPI()):
        await asyncio.wait_for(processing.called.wait(), timeout=1)
        await asyncio.wait_for(delivery.called.wait(), timeout=1)
        assert runtime.started
        assert runtime.delivery_ready

    assert database.migrations == 1
    assert processing.calls[:2] == [
        (ProcessingStage.TRANSCRIPTION, 2),
        (ProcessingStage.EXTRACTION, 2),
    ]
    assert delivery.limits == [1]
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
        delivery=delivery,
        mcp_client=mcp,
        poll_interval_seconds=0.01,
        processing_batch_size=1,
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
        delivery=delivery,
        mcp_client=mcp,
        poll_interval_seconds=0.01,
        processing_batch_size=1,
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
