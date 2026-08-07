from __future__ import annotations

import json

from starlette.requests import Request

from meeting_action_orchestrator.api.errors import (
    application_error_handler,
    domain_error_handler,
)
from meeting_action_orchestrator.application.errors import ApplicationError
from meeting_action_orchestrator.domain.errors import IdempotencyConflictError

SENSITIVE_DETAIL = "private-provider-diagnostic"
SENSITIVE_KEY = "private-idempotency-key"


def request() -> Request:
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
            "state": {"request_id": "request-one"},
        }
    )


async def test_generic_application_error_does_not_expose_exception_text() -> None:
    response = await application_error_handler(request(), ApplicationError(SENSITIVE_DETAIL))
    payload = json.loads(response.body)

    assert response.status_code == 500
    assert payload["detail"] == "The server could not complete the request."
    assert SENSITIVE_DETAIL not in response.body.decode()


async def test_idempotency_conflict_does_not_reflect_the_request_key() -> None:
    response = await domain_error_handler(request(), IdempotencyConflictError(SENSITIVE_KEY))
    payload = json.loads(response.body)

    assert response.status_code == 409
    assert payload["type"].endswith("idempotency-conflict")
    assert SENSITIVE_KEY not in response.body.decode()
