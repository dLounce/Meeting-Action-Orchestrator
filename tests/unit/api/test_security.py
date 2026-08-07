from __future__ import annotations

from http import HTTPStatus
from typing import cast

import httpx
from fastapi import FastAPI
from starlette.responses import PlainTextResponse
from starlette.types import Message, Receive, Scope, Send

from meeting_action_orchestrator.api.security import (
    DEFAULT_CONTENT_SECURITY_POLICY,
    SecurityHeadersMiddleware,
)


async def get_response(app: FastAPI) -> httpx.Response:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        return await client.get("/")


async def test_security_headers_are_applied_to_http_responses() -> None:
    app = FastAPI()
    app.add_middleware(SecurityHeadersMiddleware)

    @app.get("/")
    async def index() -> PlainTextResponse:
        return PlainTextResponse("ready", headers={"X-Frame-Options": "SAMEORIGIN"})

    response = await get_response(app)

    assert response.status_code == HTTPStatus.OK
    assert response.headers["content-security-policy"] == DEFAULT_CONTENT_SECURITY_POLICY
    assert response.headers["cross-origin-opener-policy"] == "same-origin"
    assert response.headers["cross-origin-resource-policy"] == "same-origin"
    assert response.headers["permissions-policy"].startswith("camera=()")
    assert response.headers["referrer-policy"] == "no-referrer"
    assert response.headers["x-content-type-options"] == "nosniff"
    assert response.headers["x-frame-options"] == "DENY"
    assert response.headers["cache-control"] == "no-store"
    assert "strict-transport-security" not in response.headers


async def test_security_headers_support_custom_policy_and_hsts() -> None:
    app = FastAPI()
    app.add_middleware(
        SecurityHeadersMiddleware,
        content_security_policy="default-src 'none'",
        enable_hsts=True,
    )

    @app.get("/")
    async def index() -> PlainTextResponse:
        return PlainTextResponse("ready")

    response = await get_response(app)

    assert response.headers["content-security-policy"] == "default-src 'none'"
    assert response.headers["strict-transport-security"] == ("max-age=63072000; includeSubDomains")


async def test_security_headers_preserve_explicit_cache_policy() -> None:
    app = FastAPI()
    app.add_middleware(SecurityHeadersMiddleware)

    @app.get("/")
    async def index() -> PlainTextResponse:
        return PlainTextResponse("ready", headers={"Cache-Control": "public, max-age=300"})

    response = await get_response(app)

    assert response.headers["cache-control"] == "public, max-age=300"


async def test_security_middleware_passes_non_http_scopes_unchanged() -> None:
    messages: list[Message] = []

    async def downstream(_scope: Scope, _receive: Receive, send: Send) -> None:
        await send({"type": "websocket.close", "code": 1000})

    async def receive() -> Message:
        return {"type": "websocket.disconnect", "code": 1000}

    async def send(message: Message) -> None:
        messages.append(message)

    middleware = SecurityHeadersMiddleware(downstream)
    await middleware(cast(Scope, {"type": "websocket"}), receive, send)

    assert messages == [{"type": "websocket.close", "code": 1000}]
