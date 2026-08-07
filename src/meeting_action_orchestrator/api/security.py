from __future__ import annotations

from starlette.datastructures import MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send

DEFAULT_CONTENT_SECURITY_POLICY = "default-src 'none'; base-uri 'none'; frame-ancestors 'none'"

DEFAULT_SECURITY_HEADERS = (
    ("Content-Security-Policy", DEFAULT_CONTENT_SECURITY_POLICY),
    ("Cross-Origin-Opener-Policy", "same-origin"),
    ("Cross-Origin-Resource-Policy", "same-origin"),
    ("Permissions-Policy", "camera=(), geolocation=(), microphone=(), payment=(), usb=()"),
    ("Referrer-Policy", "no-referrer"),
    ("X-Content-Type-Options", "nosniff"),
    ("X-Frame-Options", "DENY"),
)


class SecurityHeadersMiddleware:
    def __init__(
        self,
        app: ASGIApp,
        *,
        content_security_policy: str = DEFAULT_CONTENT_SECURITY_POLICY,
        enable_hsts: bool = False,
        cache_control: str | None = "no-store",
    ) -> None:
        self._app = app
        self._headers = tuple(
            (name, content_security_policy if name == "Content-Security-Policy" else value)
            for name, value in DEFAULT_SECURITY_HEADERS
        )
        self._enable_hsts = enable_hsts
        self._cache_control = cache_control

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self._app(scope, receive, send)
            return

        async def send_with_security_headers(message: Message) -> None:
            if message["type"] == "http.response.start":
                headers = MutableHeaders(scope=message)
                for name, value in self._headers:
                    headers[name] = value
                if self._cache_control is not None and "cache-control" not in headers:
                    headers["Cache-Control"] = self._cache_control
                if self._enable_hsts:
                    headers["Strict-Transport-Security"] = "max-age=63072000; includeSubDomains"
            await send(message)

        await self._app(scope, receive, send_with_security_headers)
