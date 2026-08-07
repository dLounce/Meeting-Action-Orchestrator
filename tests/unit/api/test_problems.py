from __future__ import annotations

from http import HTTPStatus
from typing import Annotated

import httpx
import pytest
from fastapi import FastAPI, Header, HTTPException, Query, Request, Response
from pydantic import ValidationError
from starlette.middleware.base import RequestResponseEndpoint

from meeting_action_orchestrator.api.problems import (
    INTERNAL_PROBLEM_TYPE,
    VALIDATION_PROBLEM_TYPE,
    FieldViolation,
    ProblemDetail,
    ProblemError,
    create_problem,
    install_problem_handlers,
)

_BROKEN_ROUTE_MESSAGE = "sensitive implementation detail"


def test_problem_detail_serializes_rfc_fields_and_extensions() -> None:
    problem = create_problem(
        409,
        detail="The proposal changed.",
        type_uri="urn:test:conflict",
        instance="/proposals/one",
        request_id="request-one",
        errors=[FieldViolation(location="body.digest", message="Mismatch", code="conflict")],
    )

    assert problem.model_dump(by_alias=True, exclude_none=True, mode="json") == {
        "type": "urn:test:conflict",
        "title": "Conflict",
        "status": 409,
        "detail": "The proposal changed.",
        "instance": "/proposals/one",
        "request_id": "request-one",
        "errors": [{"location": "body.digest", "message": "Mismatch", "code": "conflict"}],
    }


def test_problem_detail_rejects_non_error_status() -> None:
    with pytest.raises(ValidationError, match="between 400 and 599"):
        ProblemDetail(title="Invalid", status=200)


def test_problem_factory_handles_unknown_http_error_status() -> None:
    problem = create_problem(499)

    assert problem.title == "HTTP error"
    assert problem.errors is None


def create_test_app() -> FastAPI:
    app = FastAPI()
    install_problem_handlers(app)

    @app.middleware("http")
    async def add_request_id(
        request: Request,
        call_next: RequestResponseEndpoint,
    ) -> Response:
        request.state.request_id = "request-123"
        return await call_next(request)

    @app.get("/protected")
    async def protected() -> None:
        raise ProblemError(
            create_problem(403, detail="Approval is required.", type_uri="urn:test:approval"),
            headers={"X-Approval-State": "required"},
        )

    @app.get("/missing")
    async def missing() -> None:
        raise HTTPException(status_code=404, detail="Meeting not found")

    @app.get("/structured-error")
    async def structured_error() -> None:
        raise HTTPException(
            status_code=401,
            detail={"private": "value"},
            headers={"WWW-Authenticate": "Bearer"},
        )

    @app.get("/validated")
    async def validated(x_count: int = Header()) -> dict[str, int]:
        return {"count": x_count}

    @app.get("/nested-validation")
    async def nested_validation(values: Annotated[list[int], Query()]) -> dict[str, list[int]]:
        return {"values": values}

    @app.get("/broken")
    async def broken() -> None:
        raise RuntimeError(_BROKEN_ROUTE_MESSAGE)

    return app


async def get_response(
    path: str,
    *,
    headers: dict[str, str] | None = None,
    raise_app_exceptions: bool = True,
) -> httpx.Response:
    transport = httpx.ASGITransport(
        app=create_test_app(),
        raise_app_exceptions=raise_app_exceptions,
    )
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        return await client.get(path, headers=headers)


async def test_problem_exception_handler_preserves_headers() -> None:
    response = await get_response("/protected")

    assert response.status_code == HTTPStatus.FORBIDDEN
    assert response.headers["content-type"] == "application/problem+json"
    assert response.headers["x-approval-state"] == "required"
    assert response.json() == {
        "type": "urn:test:approval",
        "title": "Forbidden",
        "status": 403,
        "detail": "Approval is required.",
        "instance": "/protected",
        "request_id": "request-123",
    }


async def test_http_exception_is_rendered_as_problem_detail() -> None:
    response = await get_response("/missing")

    assert response.status_code == HTTPStatus.NOT_FOUND
    assert response.json() == {
        "type": "about:blank",
        "title": "Not Found",
        "status": 404,
        "detail": "Meeting not found",
        "instance": "/missing",
        "request_id": "request-123",
    }


async def test_structured_http_exception_is_not_reflected() -> None:
    response = await get_response("/structured-error")

    assert response.status_code == HTTPStatus.UNAUTHORIZED
    assert response.headers["www-authenticate"] == "Bearer"
    assert response.json()["detail"] == "Unauthorized"
    assert "private" not in response.text


async def test_validation_exception_omits_rejected_input() -> None:
    response = await get_response(
        "/validated",
        headers={"X-Count": "secret-invalid-value"},
    )
    payload = response.json()

    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
    assert response.headers["content-type"] == "application/problem+json"
    assert payload["type"] == VALIDATION_PROBLEM_TYPE
    assert payload["title"] == "Request validation failed"
    assert payload["instance"] == "/validated"
    assert payload["request_id"] == "request-123"
    assert payload["errors"][0]["location"] == "header.x-count"
    assert payload["errors"][0]["code"] == "int_parsing"
    assert "secret-invalid-value" not in response.text


async def test_validation_location_formats_sequence_indexes() -> None:
    response = await get_response("/nested-validation?values=1&values=private-sequence-value")

    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
    assert response.json()["errors"][0]["location"] == "query.values[1]"
    assert "private-sequence-value" not in response.text


async def test_unhandled_exception_returns_generic_problem() -> None:
    response = await get_response("/broken", raise_app_exceptions=False)

    assert response.status_code == HTTPStatus.INTERNAL_SERVER_ERROR
    assert response.json() == {
        "type": INTERNAL_PROBLEM_TYPE,
        "title": "Internal Server Error",
        "status": 500,
        "detail": "The server could not complete the request.",
        "instance": "/broken",
        "request_id": "request-123",
    }
    assert "sensitive implementation detail" not in response.text
