from __future__ import annotations

from fastapi import FastAPI

from meeting_action_orchestrator.api.contracts import ApiDependencies
from meeting_action_orchestrator.api.errors import install_service_error_handlers
from meeting_action_orchestrator.api.middleware import (
    RequestBodyLimitMiddleware,
    RequestIdMiddleware,
)
from meeting_action_orchestrator.api.problems import install_problem_handlers
from meeting_action_orchestrator.api.routes import health_router, meeting_router
from meeting_action_orchestrator.api.security import SecurityHeadersMiddleware

REQUEST_ENVELOPE_BYTES = 1024 * 1024


def create_app(dependencies: ApiDependencies) -> FastAPI:
    app = FastAPI(
        title="Meeting Action Orchestrator API",
        summary="Review-first meeting transcription and action delivery",
        version=dependencies.service_version,
        docs_url=None,
        redoc_url=None,
        openapi_url="/openapi.json",
    )
    app.state.api_dependencies = dependencies
    install_problem_handlers(app)
    install_service_error_handlers(app)
    app.include_router(health_router)
    app.include_router(meeting_router)
    app.add_middleware(
        RequestBodyLimitMiddleware,
        max_bytes=dependencies.max_upload_bytes + REQUEST_ENVELOPE_BYTES,
    )
    app.add_middleware(
        SecurityHeadersMiddleware,
        enable_hsts=False,
    )
    app.add_middleware(RequestIdMiddleware)
    return app
