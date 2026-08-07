from __future__ import annotations

from typing import cast

from fastapi import FastAPI, Request
from starlette.responses import JSONResponse
from starlette.types import ExceptionHandler

from meeting_action_orchestrator.api.problems import create_problem, problem_response
from meeting_action_orchestrator.application.errors import (
    ApplicationError,
    OperationConflictError,
    PermanentWriteError,
    ResourceNotFoundError,
    ReviewDigestMismatchError,
    TransientWriteError,
    UnknownWriteOutcomeError,
)
from meeting_action_orchestrator.domain.errors import (
    DomainError,
    IdempotencyConflictError,
    InvalidDomainValueError,
)


async def application_error_handler(request: Request, exc: ApplicationError) -> JSONResponse:
    status, type_uri, detail, headers = _application_problem(exc)
    problem = create_problem(
        status,
        detail=detail,
        type_uri=type_uri,
        instance=request.url.path,
        request_id=_request_id(request),
    )
    return problem_response(problem, headers=headers)


async def domain_error_handler(request: Request, exc: DomainError) -> JSONResponse:
    status = 422 if isinstance(exc, InvalidDomainValueError) else 409
    if isinstance(exc, IdempotencyConflictError):
        type_uri = "urn:meeting-action-orchestrator:problem:idempotency-conflict"
        detail = "The idempotency key is already bound to a different request."
    else:
        type_uri = "urn:meeting-action-orchestrator:problem:domain-conflict"
        detail = str(exc)
    problem = create_problem(
        status,
        detail=detail,
        type_uri=type_uri,
        instance=request.url.path,
        request_id=_request_id(request),
    )
    return problem_response(problem)


def install_service_error_handlers(app: FastAPI) -> None:
    app.add_exception_handler(
        ApplicationError,
        cast(ExceptionHandler, application_error_handler),
    )
    app.add_exception_handler(DomainError, cast(ExceptionHandler, domain_error_handler))


def _application_problem(
    exc: ApplicationError,
) -> tuple[int, str, str, dict[str, str]]:
    if isinstance(exc, ResourceNotFoundError):
        return (
            404,
            "urn:meeting-action-orchestrator:problem:not-found",
            "The requested resource was not found.",
            {},
        )
    if isinstance(exc, ReviewDigestMismatchError):
        return (
            412,
            "urn:meeting-action-orchestrator:problem:stale-review",
            "The review changed before the operation completed.",
            {},
        )
    if isinstance(exc, TransientWriteError):
        return (
            503,
            "urn:meeting-action-orchestrator:problem:delivery-unavailable",
            "The delivery provider is temporarily unavailable.",
            {"Retry-After": "5"},
        )
    if isinstance(exc, UnknownWriteOutcomeError):
        return (
            502,
            "urn:meeting-action-orchestrator:problem:delivery-outcome-unknown",
            "The delivery outcome is unknown and requires reconciliation.",
            {},
        )
    if isinstance(exc, PermanentWriteError):
        return (
            409,
            "urn:meeting-action-orchestrator:problem:delivery-rejected",
            "The delivery provider rejected the approved action.",
            {},
        )
    if isinstance(exc, OperationConflictError):
        return (
            409,
            "urn:meeting-action-orchestrator:problem:operation-conflict",
            "The operation conflicts with the current workflow state.",
            {},
        )
    return (
        500,
        "urn:meeting-action-orchestrator:problem:application-error",
        "The server could not complete the request.",
        {},
    )


def _request_id(request: Request) -> str | None:
    value = getattr(request.state, "request_id", None)
    return value if isinstance(value, str) and value else None
