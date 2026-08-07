from __future__ import annotations

from tempfile import SpooledTemporaryFile
from uuid import uuid4

from starlette.types import ASGIApp, Message, Receive, Scope, Send

from meeting_action_orchestrator.api.problems import create_problem, problem_response


class RequestIdMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self._app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self._app(scope, receive, send)
            return
        request_id = uuid4().hex
        scope.setdefault("state", {})["request_id"] = request_id

        async def send_with_request_id(message: Message) -> None:
            if message["type"] == "http.response.start":
                headers = list(message.get("headers", ()))
                headers.append((b"x-request-id", request_id.encode("ascii")))
                message["headers"] = headers
            await send(message)

        await self._app(scope, receive, send_with_request_id)


class RequestBodyLimitMiddleware:
    def __init__(self, app: ASGIApp, *, max_bytes: int) -> None:
        if max_bytes <= 0:
            raise ValueError("max_bytes must be positive")
        self._app = app
        self._max_bytes = max_bytes

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self._app(scope, receive, send)
            return
        values = [value for name, value in scope["headers"] if name == b"content-length"]
        if len(values) > 1:
            await self._reject(scope, receive, send, 400, "Content-Length must be unambiguous.")
            return
        if values:
            if not values[0].isdigit():
                await self._reject(scope, receive, send, 400, "Content-Length must be an integer.")
                return
            content_length = int(values[0])
            if content_length < 0:
                await self._reject(
                    scope, receive, send, 400, "Content-Length must not be negative."
                )
                return
            if content_length > self._max_bytes:
                await self._reject(
                    scope,
                    receive,
                    send,
                    413,
                    "The request body exceeds the configured limit.",
                )
                return
        await self._buffer_and_forward(scope, receive, send)

    async def _buffer_and_forward(self, scope: Scope, receive: Receive, send: Send) -> None:
        with SpooledTemporaryFile(max_size=1024 * 1024, mode="w+b") as body:
            size = 0
            while True:
                message = await receive()
                if message["type"] == "http.disconnect":
                    return
                chunk = message.get("body", b"")
                size += len(chunk)
                if size > self._max_bytes:
                    await self._reject(
                        scope,
                        receive,
                        send,
                        413,
                        "The request body exceeds the configured limit.",
                    )
                    return
                body.write(chunk)
                if not message.get("more_body", False):
                    break
            body.seek(0)
            exhausted = False

            async def replay_receive() -> Message:
                nonlocal exhausted
                if exhausted:
                    return {"type": "http.disconnect"}
                chunk = body.read(64 * 1024)
                more_body = body.tell() < size
                exhausted = not more_body
                return {
                    "type": "http.request",
                    "body": chunk,
                    "more_body": more_body,
                }

            await self._app(scope, replay_receive, send)

    @staticmethod
    async def _reject(
        scope: Scope,
        receive: Receive,
        send: Send,
        status: int,
        detail: str,
    ) -> None:
        state = scope.get("state", {})
        request_id = state.get("request_id") if isinstance(state, dict) else None
        response = problem_response(
            create_problem(
                status,
                detail=detail,
                type_uri="urn:meeting-action-orchestrator:problem:request-body-size",
                instance=scope.get("path"),
                request_id=request_id if isinstance(request_id, str) else None,
            )
        )
        await response(scope, receive, send)
