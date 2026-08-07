from __future__ import annotations

import math
from collections.abc import Callable
from contextlib import AsyncExitStack, suppress
from datetime import timedelta
from enum import Enum
from types import TracebackType
from typing import Any, Protocol

import anyio
import httpx
from mcp import ClientSession
from mcp.client.streamable_http import streamable_http_client
from mcp.types import CallToolResult


class McpClientLifecycleError(RuntimeError):
    pass


class McpClientNotStartedError(McpClientLifecycleError):
    def __init__(self) -> None:
        super().__init__("The MCP client has not been started")


class McpClientStartError(McpClientLifecycleError):
    def __init__(self) -> None:
        super().__init__("The MCP client could not start")


class McpClientClosedError(McpClientLifecycleError):
    def __init__(self) -> None:
        super().__init__("The MCP client is closed")


class McpClientCloseError(McpClientLifecycleError):
    def __init__(self) -> None:
        super().__init__("The MCP client could not close cleanly")


class AsyncHttpClient(Protocol):
    async def __aenter__(self) -> AsyncHttpClient: ...

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None: ...


class HttpClientFactory(Protocol):
    def __call__(
        self,
        *,
        headers: dict[str, str] | None,
        timeout: httpx.Timeout | None,
        auth: httpx.Auth | None,
    ) -> AsyncHttpClient: ...


class TransportContext(Protocol):
    async def __aenter__(self) -> tuple[Any, Any, Callable[[], str | None]]: ...

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None: ...


class TransportFactory(Protocol):
    def __call__(
        self,
        url: str,
        *,
        http_client: AsyncHttpClient,
        terminate_on_close: bool,
    ) -> TransportContext: ...


class Session(Protocol):
    async def __aenter__(self) -> Session: ...

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None: ...

    async def initialize(self) -> object: ...

    async def call_tool(
        self,
        name: str,
        arguments: dict[str, Any] | None = None,
        read_timeout_seconds: timedelta | None = None,
        progress_callback: Any | None = None,
        *,
        meta: dict[str, Any] | None = None,
    ) -> CallToolResult: ...


class SessionFactory(Protocol):
    def __call__(
        self,
        read_stream: Any,
        write_stream: Any,
        read_timeout_seconds: timedelta | None = None,
    ) -> Session: ...


class _Lifecycle(Enum):
    NEW = "new"
    STARTING = "starting"
    STARTED = "started"
    RECOVERING = "recovering"
    CLOSING = "closing"
    CLOSED = "closed"


def _create_http_client(
    *,
    headers: dict[str, str] | None,
    timeout: httpx.Timeout | None,
    auth: httpx.Auth | None,
) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        headers=headers,
        timeout=timeout,
        auth=auth,
        follow_redirects=False,
    )


class ManagedMcpHttpClient:
    def __init__(
        self,
        endpoint: str,
        *,
        bearer_token: str | None = None,
        request_timeout_seconds: float = 30,
        sse_read_timeout_seconds: float = 300,
        call_timeout_seconds: float = 30,
        startup_timeout_seconds: float = 30,
        terminate_on_close: bool = True,
        http_client_factory: HttpClientFactory = _create_http_client,
        transport_factory: TransportFactory = streamable_http_client,
        session_factory: SessionFactory = ClientSession,
    ) -> None:
        self._endpoint = _validated_endpoint(endpoint)
        self._headers = _authorization_headers(bearer_token)
        self._request_timeout = _positive_timeout(
            request_timeout_seconds,
            "request_timeout_seconds",
        )
        self._sse_read_timeout = _positive_timeout(
            sse_read_timeout_seconds,
            "sse_read_timeout_seconds",
        )
        self._call_timeout = _positive_timeout(
            call_timeout_seconds,
            "call_timeout_seconds",
        )
        self._startup_timeout = _positive_timeout(
            startup_timeout_seconds,
            "startup_timeout_seconds",
        )
        self._terminate_on_close = terminate_on_close
        self._http_client_factory = http_client_factory
        self._transport_factory = transport_factory
        self._session_factory = session_factory
        self._lifecycle = _Lifecycle.NEW
        self._lifecycle_lock = anyio.Lock()
        self._close_lock = anyio.Lock()
        self._idle = anyio.Event()
        self._idle.set()
        self._active_calls = 0
        self._stack: AsyncExitStack | None = None
        self._session: Session | None = None

    async def __aenter__(self) -> ManagedMcpHttpClient:
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.close()

    @property
    def connected(self) -> bool:
        return self._lifecycle is _Lifecycle.STARTED and self._session is not None

    async def start(self) -> None:
        async with self._close_lock, self._lifecycle_lock:
            if self._lifecycle is _Lifecycle.STARTED:
                return
            if self._lifecycle in {_Lifecycle.CLOSING, _Lifecycle.CLOSED}:
                raise McpClientClosedError
            self._lifecycle = _Lifecycle.STARTING
            stack = AsyncExitStack()
            try:
                http_client = self._http_client_factory(
                    headers=dict(self._headers) or None,
                    timeout=httpx.Timeout(
                        self._request_timeout,
                        read=self._sse_read_timeout,
                    ),
                    auth=None,
                )
                managed_http_client = await stack.enter_async_context(http_client)
                streams = await stack.enter_async_context(
                    self._transport_factory(
                        self._endpoint,
                        http_client=managed_http_client,
                        terminate_on_close=self._terminate_on_close,
                    )
                )
                read_stream, write_stream, _ = streams
                session = self._session_factory(
                    read_stream,
                    write_stream,
                    read_timeout_seconds=timedelta(seconds=self._call_timeout),
                )
                managed_session = await stack.enter_async_context(session)
                with anyio.fail_after(self._startup_timeout):
                    await managed_session.initialize()
            except BaseException as error:
                with anyio.CancelScope(shield=True), suppress(BaseException):
                    await stack.aclose()
                self._lifecycle = _Lifecycle.NEW
                if isinstance(error, anyio.get_cancelled_exc_class()):
                    raise
                if isinstance(error, Exception):
                    raise McpClientStartError from None
                raise
            self._stack = stack
            self._session = managed_session
            self._lifecycle = _Lifecycle.STARTED

    async def close(self) -> None:
        async with self._close_lock:
            async with self._lifecycle_lock:
                if self._lifecycle is _Lifecycle.CLOSED:
                    return
                if self._lifecycle is _Lifecycle.NEW:
                    self._lifecycle = _Lifecycle.CLOSED
                    self._headers = {}
                    return
                self._lifecycle = _Lifecycle.CLOSING
                idle = self._idle
                stack = self._stack
                self._session = None
                self._stack = None
            close_error: BaseException | None = None
            with anyio.CancelScope(shield=True):
                await idle.wait()
                try:
                    if stack is not None:
                        await stack.aclose()
                except BaseException as error:
                    close_error = error
                finally:
                    async with self._lifecycle_lock:
                        self._lifecycle = _Lifecycle.CLOSED
                        self._headers = {}
            if close_error is not None:
                if isinstance(close_error, anyio.get_cancelled_exc_class()):
                    raise close_error
                if isinstance(close_error, Exception):
                    raise McpClientCloseError from None
                raise close_error

    async def call_tool(
        self,
        name: str,
        arguments: dict[str, Any] | None = None,
        *,
        meta: dict[str, Any] | None = None,
    ) -> CallToolResult:
        session = await self._acquire_call()
        failed = False
        try:
            timeout = timedelta(seconds=self._call_timeout)
            with anyio.fail_after(self._call_timeout):
                return await session.call_tool(
                    name,
                    arguments=arguments,
                    read_timeout_seconds=timeout,
                    meta=meta,
                )
        except BaseException:
            failed = True
            raise
        finally:
            await self._release_call()
            if failed:
                with anyio.CancelScope(shield=True), suppress(BaseException):
                    await self._invalidate(session)

    async def _acquire_call(self) -> Session:
        async with self._lifecycle_lock:
            if self._lifecycle in {_Lifecycle.CLOSING, _Lifecycle.CLOSED}:
                raise McpClientClosedError
            if self._lifecycle is not _Lifecycle.STARTED or self._session is None:
                raise McpClientNotStartedError
            if self._active_calls == 0:
                self._idle = anyio.Event()
            self._active_calls += 1
            return self._session

    async def _release_call(self) -> None:
        with anyio.CancelScope(shield=True):
            async with self._lifecycle_lock:
                self._active_calls -= 1
                if self._active_calls == 0:
                    self._idle.set()

    async def _invalidate(self, session: Session) -> None:
        async with self._close_lock:
            async with self._lifecycle_lock:
                if self._lifecycle is not _Lifecycle.STARTED or self._session is not session:
                    return
                self._lifecycle = _Lifecycle.RECOVERING
                idle = self._idle
                stack = self._stack
                self._session = None
                self._stack = None
            await idle.wait()
            try:
                if stack is not None:
                    await stack.aclose()
            finally:
                async with self._lifecycle_lock:
                    if self._lifecycle is _Lifecycle.RECOVERING:
                        self._lifecycle = _Lifecycle.NEW


def _validated_endpoint(value: str) -> str:
    if not value or len(value) > 2_000 or value != value.strip():
        raise ValueError("endpoint must be between 1 and 2000 characters")
    try:
        endpoint = httpx.URL(value)
    except Exception:
        raise ValueError("endpoint must be a valid HTTP or HTTPS URL") from None
    if (
        endpoint.scheme not in {"http", "https"}
        or not endpoint.host
        or endpoint.username
        or endpoint.password
        or endpoint.fragment
    ):
        raise ValueError("endpoint must be a valid HTTP or HTTPS URL without credentials")
    return str(endpoint)


def _authorization_headers(token: str | None) -> dict[str, str]:
    if token is None:
        return {}
    if (
        not token
        or len(token) > 4_096
        or token != token.strip()
        or any(ord(character) < 33 or ord(character) > 126 for character in token)
    ):
        raise ValueError("bearer_token is invalid")
    return {"Authorization": f"Bearer {token}"}


def _positive_timeout(value: float, name: str) -> float:
    if not math.isfinite(value) or value <= 0:
        raise ValueError(f"{name} must be a positive finite number")
    return value
