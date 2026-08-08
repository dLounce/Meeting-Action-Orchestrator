from __future__ import annotations

import json
from collections import deque
from collections.abc import Sequence
from typing import cast
from uuid import UUID

import pytest
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from meeting_action_orchestrator.api import middleware
from meeting_action_orchestrator.api.middleware import (
    RequestBodyLimitMiddleware,
    RequestIdMiddleware,
)


def http_scope(
    *,
    headers: Sequence[tuple[bytes, bytes]] = (),
    state: dict[str, object] | None = None,
) -> Scope:
    return cast(
        Scope,
        {
            "type": "http",
            "asgi": {"version": "3.0"},
            "http_version": "1.1",
            "method": "POST",
            "scheme": "http",
            "path": "/v1/meetings",
            "raw_path": b"/v1/meetings",
            "query_string": b"",
            "root_path": "",
            "headers": list(headers),
            "client": ("127.0.0.1", 1234),
            "server": ("testserver", 80),
            "state": state or {},
        },
    )


async def invoke(
    app: ASGIApp,
    scope: Scope,
    incoming: Sequence[Message],
) -> tuple[list[Message], int]:
    pending = deque(incoming)
    sent: list[Message] = []
    receive_calls = 0

    async def receive() -> Message:
        nonlocal receive_calls
        receive_calls += 1
        return pending.popleft()

    async def send(message: Message) -> None:
        sent.append(message)

    await app(scope, receive, send)
    return sent, receive_calls


def response_payload(messages: Sequence[Message]) -> dict[str, object]:
    body = b"".join(
        bytes(message.get("body", b""))
        for message in messages
        if message["type"] == "http.response.body"
    )
    return cast(dict[str, object], json.loads(body))


def test_body_limit_requires_a_positive_boundary() -> None:
    async def app(_scope: Scope, _receive: Receive, _send: Send) -> None:
        return None

    with pytest.raises(ValueError, match="positive"):
        RequestBodyLimitMiddleware(app, max_bytes=0)


async def test_middlewares_pass_non_http_scopes_through_unchanged() -> None:
    received: list[Scope] = []

    async def app(scope: Scope, _receive: Receive, _send: Send) -> None:
        received.append(scope)

    scope = cast(Scope, {"type": "lifespan", "state": {}})
    wrapped = RequestIdMiddleware(RequestBodyLimitMiddleware(app, max_bytes=10))

    sent, receive_calls = await invoke(wrapped, scope, ())

    assert received == [scope]
    assert sent == []
    assert receive_calls == 0


@pytest.mark.parametrize(
    "headers",
    [
        ((b"content-length", b"3"), (b"content-length", b"4")),
        ((b"content-length", b"three"),),
    ],
)
async def test_body_limit_rejects_ambiguous_or_invalid_content_length_without_reading(
    headers: tuple[tuple[bytes, bytes], ...],
) -> None:
    called = False

    async def app(_scope: Scope, _receive: Receive, _send: Send) -> None:
        nonlocal called
        called = True

    sent, receive_calls = await invoke(
        RequestBodyLimitMiddleware(app, max_bytes=10),
        http_scope(headers=headers, state={"request_id": "request-one"}),
        (),
    )

    assert sent[0]["status"] == 400
    assert response_payload(sent)["request_id"] == "request-one"
    assert not called
    assert receive_calls == 0


async def test_body_limit_stops_silently_when_client_disconnects_during_upload() -> None:
    called = False

    async def app(_scope: Scope, _receive: Receive, _send: Send) -> None:
        nonlocal called
        called = True

    sent, receive_calls = await invoke(
        RequestBodyLimitMiddleware(app, max_bytes=10),
        http_scope(),
        (cast(Message, {"type": "http.disconnect"}),),
    )

    assert sent == []
    assert not called
    assert receive_calls == 1


async def test_body_limit_replays_a_buffered_body_then_disconnects() -> None:
    replayed: list[Message] = []

    async def app(_scope: Scope, receive: Receive, send: Send) -> None:
        replayed.append(await receive())
        replayed.append(await receive())
        await send({"type": "http.response.start", "status": 204, "headers": []})
        await send({"type": "http.response.body", "body": b""})

    sent, receive_calls = await invoke(
        RequestBodyLimitMiddleware(app, max_bytes=10),
        http_scope(headers=((b"content-length", b"3"),)),
        (cast(Message, {"type": "http.request", "body": b"abc", "more_body": False}),),
    )

    assert replayed == [
        {"type": "http.request", "body": b"abc", "more_body": False},
        {"type": "http.disconnect"},
    ]
    assert sent[0]["status"] == 204
    assert receive_calls == 1


async def test_request_id_is_stored_and_appended_to_the_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request_id = UUID("10000000-0000-4000-8000-000000000001")
    observed_state: dict[str, object] = {}

    async def app(scope: Scope, _receive: Receive, send: Send) -> None:
        observed_state.update(cast(dict[str, object], scope["state"]))
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"x-existing", b"value")],
            }
        )
        await send({"type": "http.response.body", "body": b"ok"})

    monkeypatch.setattr(middleware, "uuid4", lambda: request_id)
    sent, _ = await invoke(RequestIdMiddleware(app), http_scope(), ())

    assert observed_state == {"request_id": request_id.hex}
    assert sent[0]["headers"] == [
        (b"x-existing", b"value"),
        (b"x-request-id", request_id.hex.encode("ascii")),
    ]
