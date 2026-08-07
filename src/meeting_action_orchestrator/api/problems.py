from __future__ import annotations

from collections.abc import Mapping, Sequence
from http import HTTPStatus
from typing import Any, cast

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from pydantic import BaseModel, ConfigDict, Field, model_validator
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.responses import JSONResponse
from starlette.types import ExceptionHandler, Scope

VALIDATION_PROBLEM_TYPE = "urn:meeting-action-orchestrator:problem:request-validation"
INTERNAL_PROBLEM_TYPE = "urn:meeting-action-orchestrator:problem:internal-error"
MINIMUM_PROBLEM_STATUS = 400
MAXIMUM_PROBLEM_STATUS = 599
_INVALID_PROBLEM_STATUS_MESSAGE = "Problem status must be between 400 and 599"
_UNMATCHED_PROBLEM_INSTANCE = "/unmatched"


class FieldViolation(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    location: str
    message: str
    code: str


class ProblemDetail(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)

    type_uri: str = Field(default="about:blank", alias="type", serialization_alias="type")
    title: str
    status: int
    detail: str | None = None
    instance: str | None = None
    request_id: str | None = None
    errors: tuple[FieldViolation, ...] | None = None

    @model_validator(mode="after")
    def validate_status(self) -> ProblemDetail:
        if not MINIMUM_PROBLEM_STATUS <= self.status <= MAXIMUM_PROBLEM_STATUS:
            raise ValueError(_INVALID_PROBLEM_STATUS_MESSAGE)
        return self


class ProblemError(Exception):
    def __init__(
        self,
        problem: ProblemDetail,
        *,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        super().__init__(problem.detail or problem.title)
        self.problem = problem
        self.headers = dict(headers or {})


def create_problem(
    status: int,
    *,
    title: str | None = None,
    detail: str | None = None,
    type_uri: str = "about:blank",
    instance: str | None = None,
    request_id: str | None = None,
    errors: Sequence[FieldViolation] | None = None,
) -> ProblemDetail:
    return ProblemDetail(
        type_uri=type_uri,
        title=title or _status_title(status),
        status=status,
        detail=detail,
        instance=instance,
        request_id=request_id,
        errors=tuple(errors) if errors else None,
    )


def problem_response(
    problem: ProblemDetail,
    *,
    headers: Mapping[str, str] | None = None,
) -> JSONResponse:
    return JSONResponse(
        status_code=problem.status,
        content=problem.model_dump(by_alias=True, exclude_none=True, mode="json"),
        headers=dict(headers or {}),
        media_type="application/problem+json",
    )


async def problem_exception_handler(
    request: Request,
    exc: ProblemError,
) -> JSONResponse:
    problem = exc.problem.model_copy(
        update={
            "instance": exc.problem.instance or problem_instance(request),
            "request_id": exc.problem.request_id or _request_id(request),
        }
    )
    return problem_response(problem, headers=exc.headers)


async def http_exception_handler(
    request: Request,
    exc: StarletteHTTPException,
) -> JSONResponse:
    title = _status_title(exc.status_code)
    detail = exc.detail if isinstance(exc.detail, str) else title
    problem = create_problem(
        exc.status_code,
        title=title,
        detail=detail,
        instance=problem_instance(request),
        request_id=_request_id(request),
    )
    return problem_response(problem, headers=exc.headers)


async def validation_exception_handler(
    request: Request,
    exc: RequestValidationError,
) -> JSONResponse:
    missing_headers = {
        str(location[1]).casefold()
        for error in exc.errors()
        if error.get("type") == "missing"
        and isinstance((location := error.get("loc")), tuple)
        and len(location) == 2
        and location[0] == "header"
    }
    if "if-match" in missing_headers:
        return problem_response(
            create_problem(
                428,
                detail="If-Match must identify the meeting version being changed.",
                type_uri="urn:meeting-action-orchestrator:problem:precondition-required",
                instance=problem_instance(request),
                request_id=_request_id(request),
            )
        )
    if "idempotency-key" in missing_headers:
        return problem_response(
            create_problem(
                400,
                detail="Idempotency-Key is required for this operation.",
                type_uri="urn:meeting-action-orchestrator:problem:idempotency-key-required",
                instance=problem_instance(request),
                request_id=_request_id(request),
            )
        )
    violations = tuple(_field_violation(error) for error in exc.errors())
    problem = create_problem(
        422,
        title="Request validation failed",
        detail="One or more request fields are invalid.",
        type_uri=VALIDATION_PROBLEM_TYPE,
        instance=problem_instance(request),
        request_id=_request_id(request),
        errors=violations,
    )
    return problem_response(problem)


async def unhandled_exception_handler(
    request: Request,
    _exc: Exception,
) -> JSONResponse:
    problem = create_problem(
        500,
        detail="The server could not complete the request.",
        type_uri=INTERNAL_PROBLEM_TYPE,
        instance=problem_instance(request),
        request_id=_request_id(request),
    )
    return problem_response(problem)


def install_problem_handlers(app: FastAPI) -> None:
    app.add_exception_handler(ProblemError, cast(ExceptionHandler, problem_exception_handler))
    app.add_exception_handler(
        StarletteHTTPException,
        cast(ExceptionHandler, http_exception_handler),
    )
    app.add_exception_handler(
        RequestValidationError,
        cast(ExceptionHandler, validation_exception_handler),
    )
    app.add_exception_handler(Exception, cast(ExceptionHandler, unhandled_exception_handler))


def problem_instance(request: Request) -> str:
    return problem_instance_from_scope(request.scope)


def problem_instance_from_scope(scope: Scope) -> str:
    route = scope.get("route")
    path = getattr(route, "path", None)
    return path if isinstance(path, str) and path else _UNMATCHED_PROBLEM_INSTANCE


def _field_violation(error: Mapping[str, Any]) -> FieldViolation:
    raw_location = error.get("loc", ())
    location = _format_location(raw_location if isinstance(raw_location, tuple) else ())
    message = error.get("msg")
    code = error.get("type")
    return FieldViolation(
        location=location,
        message=message if isinstance(message, str) else "Invalid value",
        code=code if isinstance(code, str) else "validation_error",
    )


def _format_location(location: tuple[Any, ...]) -> str:
    result = ""
    for part in location:
        if isinstance(part, int):
            result = f"{result}[{part}]"
        elif result:
            result = f"{result}.{part}"
        else:
            result = str(part)
    return result or "request"


def _request_id(request: Request) -> str | None:
    value = getattr(request.state, "request_id", None)
    return value if isinstance(value, str) and value else None


def _status_title(status: int) -> str:
    try:
        return HTTPStatus(status).phrase
    except ValueError:
        return "HTTP error"
