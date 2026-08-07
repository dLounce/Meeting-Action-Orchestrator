from __future__ import annotations

import asyncio
import inspect
from collections.abc import Callable
from datetime import timedelta
from types import TracebackType
from typing import Any

import httpx
import pytest
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client
from mcp.types import CallToolResult

from meeting_action_orchestrator.infrastructure.mcp_client import (
    ManagedMcpHttpClient,
    McpClientClosedError,
    McpClientNotStartedError,
    McpClientStartError,
    _create_http_client,
)


class FakeHttpClient:
    def __init__(self, events: list[str]) -> None:
        self.events = events

    async def __aenter__(self) -> FakeHttpClient:
        self.events.append("http.enter")
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        self.events.append("http.exit")
        return False


class HttpFactory:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.calls: list[tuple[dict[str, str] | None, httpx.Timeout | None, httpx.Auth | None]] = []

    def __call__(
        self,
        *,
        headers: dict[str, str] | None,
        timeout: httpx.Timeout | None,
        auth: httpx.Auth | None,
    ) -> FakeHttpClient:
        self.calls.append((headers, timeout, auth))
        return FakeHttpClient(self.events)


class FakeTransport:
    def __init__(self, events: list[str]) -> None:
        self.events = events

    async def __aenter__(self) -> tuple[object, object, Callable[[], str | None]]:
        self.events.append("transport.enter")
        return object(), object(), lambda: "session-one"

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        self.events.append("transport.exit")
        return False


class TransportFactory:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.calls: list[tuple[str, object, bool]] = []

    def __call__(
        self,
        url: str,
        *,
        http_client: object,
        terminate_on_close: bool,
    ) -> FakeTransport:
        self.calls.append((url, http_client, terminate_on_close))
        return FakeTransport(self.events)


class FakeSession:
    def __init__(
        self,
        events: list[str],
        *,
        initialize_error: Exception | None = None,
        call_error: Exception | None = None,
        block_calls: bool = False,
    ) -> None:
        self.events = events
        self.initialize_error = initialize_error
        self.call_error = call_error
        self.block_calls = block_calls
        self.initialize_count = 0
        self.calls: list[
            tuple[str, dict[str, Any] | None, timedelta | None, dict[str, Any] | None]
        ] = []
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.cancelled = False
        self.result = CallToolResult(content=[], structuredContent={"outcome": "ok"}, isError=False)

    async def __aenter__(self) -> FakeSession:
        self.events.append("session.enter")
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        self.events.append("session.exit")
        return False

    async def initialize(self) -> object:
        self.events.append("session.initialize")
        self.initialize_count += 1
        if self.initialize_error is not None:
            raise self.initialize_error
        return object()

    async def call_tool(
        self,
        name: str,
        arguments: dict[str, Any] | None = None,
        read_timeout_seconds: timedelta | None = None,
        _progress_callback: Any | None = None,
        *,
        meta: dict[str, Any] | None = None,
    ) -> CallToolResult:
        self.events.append("session.call")
        self.calls.append((name, arguments, read_timeout_seconds, meta))
        self.started.set()
        if self.block_calls:
            try:
                await self.release.wait()
            except asyncio.CancelledError:
                self.cancelled = True
                raise
        if self.call_error is not None:
            raise self.call_error
        return self.result


class ConcurrentFailureSession(FakeSession):
    def __init__(self, events: list[str], error: Exception) -> None:
        super().__init__(events)
        self.error = error
        self.slow_started = asyncio.Event()
        self.release_slow = asyncio.Event()

    async def call_tool(
        self,
        name: str,
        arguments: dict[str, Any] | None = None,
        read_timeout_seconds: timedelta | None = None,
        _progress_callback: Any | None = None,
        *,
        meta: dict[str, Any] | None = None,
    ) -> CallToolResult:
        self.events.append("session.call")
        self.calls.append((name, arguments, read_timeout_seconds, meta))
        if name == "tasks.slow":
            self.slow_started.set()
            await self.release_slow.wait()
            return self.result
        await self.slow_started.wait()
        raise self.error


class SessionFactory:
    def __init__(self, *sessions: FakeSession) -> None:
        self.sessions = list(sessions)
        self.calls: list[tuple[object, object, timedelta | None]] = []

    def __call__(
        self,
        read_stream: object,
        write_stream: object,
        read_timeout_seconds: timedelta | None = None,
    ) -> FakeSession:
        self.calls.append((read_stream, write_stream, read_timeout_seconds))
        return self.sessions.pop(0)


def client(
    *,
    token: str | None = None,
    session: FakeSession | None = None,
    sessions: tuple[FakeSession, ...] = (),
    call_timeout: float = 3,
) -> tuple[
    ManagedMcpHttpClient,
    list[str],
    HttpFactory,
    TransportFactory,
    SessionFactory,
    FakeSession,
]:
    events: list[str] = []
    selected = session or FakeSession(events)
    configured_sessions = sessions or (selected,)
    http_factory = HttpFactory(events)
    transport_factory = TransportFactory(events)
    session_factory = SessionFactory(*configured_sessions)
    managed = ManagedMcpHttpClient(
        "https://mcp.example.com/actions",
        bearer_token=token,
        request_timeout_seconds=7,
        sse_read_timeout_seconds=90,
        call_timeout_seconds=call_timeout,
        startup_timeout_seconds=2,
        http_client_factory=http_factory,
        transport_factory=transport_factory,
        session_factory=session_factory,
    )
    return managed, events, http_factory, transport_factory, session_factory, selected


@pytest.mark.asyncio
async def test_call_before_start_is_rejected() -> None:
    managed, _, _, _, _, _ = client()

    with pytest.raises(McpClientNotStartedError):
        await managed.call_tool("tasks.create")


@pytest.mark.asyncio
async def test_start_initializes_once_and_call_tool_matches_the_mcp_protocol() -> None:
    credential = "-".join(("private", "token"))
    managed, events, http_factory, transport_factory, session_factory, session = client(
        token=credential
    )

    await managed.start()
    await managed.start()
    assert managed.connected
    result = await managed.call_tool(
        "tasks.create",
        {"title": "Publish brief"},
        meta={"idempotencyKey": "key-one"},
    )
    await managed.close()
    await managed.close()
    assert not managed.connected

    headers, timeout, auth = http_factory.calls[0]
    assert headers == {"Authorization": f"Bearer {credential}"}
    assert timeout is not None
    assert timeout.connect == 7
    assert timeout.read == 90
    assert auth is None
    assert transport_factory.calls[0][0] == "https://mcp.example.com/actions"
    assert transport_factory.calls[0][2] is True
    assert session_factory.calls[0][2] == timedelta(seconds=3)
    assert session.initialize_count == 1
    assert session.calls == [
        (
            "tasks.create",
            {"title": "Publish brief"},
            timedelta(seconds=3),
            {"idempotencyKey": "key-one"},
        )
    ]
    assert result is session.result
    assert events == [
        "http.enter",
        "transport.enter",
        "session.enter",
        "session.initialize",
        "session.call",
        "session.exit",
        "transport.exit",
        "http.exit",
    ]
    assert credential not in repr(managed)


@pytest.mark.asyncio
async def test_optional_bearer_header_is_omitted() -> None:
    managed, _, http_factory, _, _, _ = client()

    await managed.start()
    await managed.close()

    assert http_factory.calls[0][0] is None


@pytest.mark.asyncio
async def test_default_http_client_does_not_follow_redirects() -> None:
    http_client = _create_http_client(
        headers={"Authorization": "Bearer value"},
        timeout=httpx.Timeout(30),
        auth=None,
    )

    assert http_client.follow_redirects is False
    await http_client.aclose()


@pytest.mark.asyncio
async def test_failed_initialization_closes_every_layer_and_can_be_retried() -> None:
    events: list[str] = []
    credential = "-".join(("private", "token"))
    failed = FakeSession(events, initialize_error=RuntimeError(credential))
    healthy = FakeSession(events)
    managed, actual_events, _, _, _, _ = client(
        token=credential,
        sessions=(failed, healthy),
    )
    failed.events = actual_events
    healthy.events = actual_events

    with pytest.raises(McpClientStartError) as captured:
        await managed.start()

    assert credential not in str(captured.value)
    assert actual_events[-3:] == ["session.exit", "transport.exit", "http.exit"]

    await managed.start()
    await managed.close()

    assert healthy.initialize_count == 1


@pytest.mark.asyncio
async def test_failed_call_invalidates_the_session_and_reconnects_with_authorization() -> None:
    events: list[str] = []
    credential = "-".join(("private", "token"))
    failure = OSError("connection lost")
    failed = FakeSession(events, call_error=failure)
    healthy = FakeSession(events)
    managed, actual_events, http_factory, _, _, _ = client(
        token=credential,
        sessions=(failed, healthy),
    )
    failed.events = actual_events
    healthy.events = actual_events
    await managed.start()

    with pytest.raises(OSError, match="connection lost") as captured:
        await managed.call_tool(
            "tasks.create",
            {"title": "Publish brief"},
            meta={"idempotencyKey": "key-one"},
        )

    assert captured.value is failure
    assert not managed.connected
    assert len(failed.calls) == 1
    assert not healthy.calls
    assert actual_events[-3:] == ["session.exit", "transport.exit", "http.exit"]

    await managed.start()
    assert managed.connected
    result = await managed.call_tool(
        "tasks.create",
        {"title": "Publish brief"},
        meta={"idempotencyKey": "key-one"},
    )
    await managed.close()

    assert result is healthy.result
    assert len(failed.calls) == 1
    assert len(healthy.calls) == 1
    assert [call[0] for call in http_factory.calls] == [
        {"Authorization": f"Bearer {credential}"},
        {"Authorization": f"Bearer {credential}"},
    ]


@pytest.mark.asyncio
async def test_failed_call_waits_for_other_active_calls_before_closing_session() -> None:
    events: list[str] = []
    failure = OSError("connection lost")
    session = ConcurrentFailureSession(events, failure)
    managed, actual_events, _, _, _, _ = client(session=session)
    session.events = actual_events
    await managed.start()
    slow = asyncio.create_task(managed.call_tool("tasks.slow"))
    await session.slow_started.wait()
    failed = asyncio.create_task(managed.call_tool("tasks.fail"))
    await asyncio.sleep(0.01)

    assert not managed.connected
    assert "session.exit" not in actual_events
    with pytest.raises(McpClientNotStartedError):
        await managed.call_tool("tasks.create")

    session.release_slow.set()
    assert await slow is session.result
    with pytest.raises(OSError, match="connection lost") as captured:
        await failed
    await managed.close()

    assert captured.value is failure
    assert actual_events[-3:] == ["session.exit", "transport.exit", "http.exit"]


@pytest.mark.asyncio
async def test_call_timeout_cancels_the_in_flight_session_request() -> None:
    events: list[str] = []
    session = FakeSession(events, block_calls=True)
    managed, actual_events, _, _, _, _ = client(session=session, call_timeout=0.01)
    session.events = actual_events
    await managed.start()

    with pytest.raises(TimeoutError):
        await managed.call_tool("tasks.create")

    assert session.cancelled is True
    assert not managed.connected
    assert actual_events[-3:] == ["session.exit", "transport.exit", "http.exit"]
    await managed.close()


@pytest.mark.asyncio
async def test_caller_cancellation_propagates_and_close_still_completes() -> None:
    events: list[str] = []
    session = FakeSession(events, block_calls=True)
    managed, actual_events, _, _, _, _ = client(session=session)
    session.events = actual_events
    await managed.start()
    task = asyncio.create_task(managed.call_tool("tasks.create"))
    await session.started.wait()

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert not managed.connected
    await managed.close()

    assert session.cancelled is True
    assert actual_events[-3:] == ["session.exit", "transport.exit", "http.exit"]


@pytest.mark.asyncio
async def test_close_waits_for_active_calls_and_rejects_new_calls() -> None:
    events: list[str] = []
    session = FakeSession(events, block_calls=True)
    managed, actual_events, _, _, _, _ = client(session=session)
    session.events = actual_events
    await managed.start()
    call = asyncio.create_task(managed.call_tool("tasks.create"))
    await session.started.wait()
    closing = asyncio.create_task(managed.close())
    await asyncio.sleep(0.01)

    assert "session.exit" not in actual_events
    with pytest.raises(McpClientClosedError):
        await managed.call_tool("tasks.create")
    session.release.set()
    await call
    await closing

    assert actual_events[-3:] == ["session.exit", "transport.exit", "http.exit"]


@pytest.mark.asyncio
async def test_close_before_start_is_idempotent_and_terminal() -> None:
    managed, events, _, _, _, _ = client()

    await managed.close()
    await managed.close()

    with pytest.raises(McpClientClosedError):
        await managed.start()
    assert not events


def test_installed_mcp_signatures_match_the_managed_transport() -> None:
    transport_parameters = inspect.signature(streamable_http_client).parameters
    session_parameters = inspect.signature(ClientSession).parameters
    call_parameters = inspect.signature(ClientSession.call_tool).parameters

    assert "http_client" in transport_parameters
    assert "terminate_on_close" in transport_parameters
    assert "read_timeout_seconds" in session_parameters
    assert "read_timeout_seconds" in call_parameters
    assert call_parameters["meta"].kind is inspect.Parameter.KEYWORD_ONLY


@pytest.mark.parametrize(
    "endpoint",
    [
        "file:///tmp/mcp",
        "https://user:secret@example.com/mcp",
        "https://example.com/mcp#fragment",
    ],
)
def test_unsafe_endpoint_is_rejected(endpoint: str) -> None:
    with pytest.raises(ValueError, match="endpoint"):
        ManagedMcpHttpClient(endpoint)


@pytest.mark.parametrize("token", ["", " secret", "secret\nheader", "contains space"])
def test_unsafe_bearer_token_is_rejected(token: str) -> None:
    with pytest.raises(ValueError, match="bearer_token"):
        ManagedMcpHttpClient("https://mcp.example.com", bearer_token=token)
