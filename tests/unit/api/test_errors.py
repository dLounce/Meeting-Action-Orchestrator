from __future__ import annotations

import json

import pytest
from starlette.requests import Request

from meeting_action_orchestrator.api.errors import (
    application_error_handler,
    domain_error_handler,
)
from meeting_action_orchestrator.application.errors import (
    ApplicationError,
    OperationConflictError,
    PermanentWriteError,
    ResourceNotFoundError,
    ReviewDigestMismatchError,
    StaleWorkflowVersionError,
    TransientWriteError,
    UnknownWriteOutcomeError,
)
from meeting_action_orchestrator.domain.errors import (
    DomainError,
    DomainValueCode,
    IdempotencyConflictError,
    InvalidDomainValueError,
)

SENSITIVE_DETAIL = "private-provider-diagnostic"
SENSITIVE_KEY = "private-idempotency-key"


def request(*, request_id: str | None = "request-one") -> Request:
    return Request(
        {
            "type": "http",
            "method": "GET",
            "scheme": "http",
            "path": "/v1/meetings/example",
            "raw_path": b"/v1/meetings/example",
            "root_path": "",
            "query_string": b"",
            "headers": [],
            "client": ("127.0.0.1", 1234),
            "server": ("testserver", 80),
            "state": {"request_id": request_id} if request_id is not None else {},
        }
    )


async def test_generic_application_error_does_not_expose_exception_text() -> None:
    response = await application_error_handler(request(), ApplicationError(SENSITIVE_DETAIL))
    body = bytes(response.body)
    payload = json.loads(body)

    assert response.status_code == 500
    assert payload["detail"] == "The server could not complete the request."
    assert SENSITIVE_DETAIL not in body.decode()


async def test_idempotency_conflict_does_not_reflect_the_request_key() -> None:
    response = await domain_error_handler(request(), IdempotencyConflictError(SENSITIVE_KEY))
    body = bytes(response.body)
    payload = json.loads(body)

    assert response.status_code == 409
    assert payload["type"].endswith("idempotency-conflict")
    assert SENSITIVE_KEY not in body.decode()


async def test_stale_meeting_version_is_a_precondition_failure() -> None:
    response = await application_error_handler(request(), StaleWorkflowVersionError())
    payload = json.loads(bytes(response.body))

    assert response.status_code == 412
    assert payload["type"].endswith("stale-meeting")


@pytest.mark.parametrize(
    ("error", "status", "problem_type", "retry_after"),
    [
        (ResourceNotFoundError("Meeting"), 404, "not-found", None),
        (ReviewDigestMismatchError(), 412, "stale-review", None),
        (TransientWriteError("private"), 503, "delivery-unavailable", "5"),
        (UnknownWriteOutcomeError("private"), 502, "delivery-outcome-unknown", None),
        (PermanentWriteError("private"), 409, "delivery-rejected", None),
        (OperationConflictError("private"), 409, "operation-conflict", None),
    ],
)
async def test_application_errors_map_to_stable_private_problem_details(
    error: ApplicationError,
    status: int,
    problem_type: str,
    retry_after: str | None,
) -> None:
    response = await application_error_handler(request(), error)
    body = bytes(response.body)
    payload = json.loads(body)

    assert response.status_code == status
    assert payload["type"].endswith(problem_type)
    assert payload["request_id"] == "request-one"
    assert "private" not in body.decode()
    assert response.headers.get("retry-after") == retry_after


@pytest.mark.parametrize(
    ("error", "status"),
    [
        (DomainError("The review state conflicts with the request."), 409),
        (InvalidDomainValueError(DomainValueCode.TIMEZONE), 422),
    ],
)
async def test_non_idempotency_domain_errors_preserve_safe_domain_detail(
    error: DomainError,
    status: int,
) -> None:
    response = await domain_error_handler(request(request_id=None), error)
    payload = json.loads(bytes(response.body))

    assert response.status_code == status
    assert payload["type"].endswith("domain-conflict")
    assert payload["detail"] == str(error)
    assert "request_id" not in payload
