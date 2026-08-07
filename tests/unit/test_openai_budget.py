import json
from types import SimpleNamespace
from typing import Any
from uuid import uuid4

import httpx
import openai
import pytest

from meeting_action_orchestrator.application.errors import ProviderBudgetIntegrityError
from meeting_action_orchestrator.domain.enums import (
    ProviderCallRole,
    ProviderOperation,
    ProviderSettlementOutcome,
    ProviderUsageKind,
)
from meeting_action_orchestrator.domain.hashing import canonical_sha256
from meeting_action_orchestrator.domain.provider_budget import PROVIDER_BUDGET_COUNTER_MAX
from meeting_action_orchestrator.infrastructure.openai_agents import (
    OpenAIAgentInputError,
    OpenAIAgentOutputError,
    OpenAIAgentRateLimitError,
    OpenAIAgentsRunner,
    OpenAIAgentTimeoutError,
    OpenAIAgentTransientError,
)
from meeting_action_orchestrator.infrastructure.openai_budget import (
    OpenAIProviderDispatchError,
    OpenAIResponsesBudgetHooks,
)
from tests.provider_budget_support import FakeBudgetController, dispatch_context


def _responses_body() -> dict[str, Any]:
    return {
        "input": "sensitive meeting content",
        "instructions": "return structured output",
        "max_output_tokens": 50,
        "model": "gpt-5-mini",
        "store": False,
    }


def _request_headers(client_request_id: str) -> dict[str, str]:
    return {
        "Authorization": "Bearer test",
        "X-Client-Request-Id": client_request_id,
    }


def _assert_responses_accounting(
    controller: FakeBudgetController,
    count_request: httpx.Request,
    generation_request: httpx.Request,
    client_request_id: str,
    body: dict[str, Any],
) -> None:
    assert count_request.url == httpx.URL("https://api.openai.com/v1/responses/input_tokens")
    assert count_request.headers["Authorization"] == "Bearer test"
    assert count_request.headers["OpenAI-Organization"] == "org_test"
    assert count_request.headers["OpenAI-Project"] == "proj_test"
    assert "X-Private-Metadata" not in count_request.headers
    assert len(controller.reservations) == 2
    count_context, count_reservation = controller.reservations[0]
    generation_context, generation_reservation = controller.reservations[1]
    assert count_context == generation_context == dispatch_context()
    assert count_reservation.operation is ProviderOperation.RESPONSES_PREFLIGHT
    assert generation_reservation.operation is ProviderOperation.RESPONSES_CREATE
    count_client_request_id = count_request.headers["X-Client-Request-Id"]
    assert count_reservation.dispatch_key == count_client_request_id
    assert generation_reservation.dispatch_key == client_request_id
    assert count_client_request_id != client_request_id
    count_body = json.loads(count_request.content)
    assert count_reservation.operation_digest == canonical_sha256(count_body)
    assert generation_reservation.operation_digest == canonical_sha256(body)
    assert generation_reservation.reserved_input_tokens == 17
    assert generation_reservation.reserved_output_tokens == 50
    assert "sensitive meeting content" not in repr(count_reservation)
    assert "sensitive meeting content" not in repr(generation_reservation)
    assert "meeting_action_orchestrator.provider_dispatch_context" not in (
        generation_request.extensions
    )
    assert len(controller.settlements) == 1
    settled_id, outcome, usage = controller.settlements[0]
    assert (
        settled_id
        == generation_request.extensions["meeting_action_orchestrator.provider_reservation_id"]
    )
    assert outcome is ProviderSettlementOutcome.SUCCEEDED
    assert usage.kind is ProviderUsageKind.TOKENS
    assert usage.input_tokens == 17
    assert usage.output_tokens == 59


@pytest.mark.asyncio
async def test_responses_hooks_reserve_count_and_generation_for_physical_requests() -> None:
    count_requests: list[httpx.Request] = []
    generation_requests: list[httpx.Request] = []

    async def count_handler(request: httpx.Request) -> httpx.Response:
        count_requests.append(request)
        return httpx.Response(200, json={"input_tokens": 17})

    async def generation_handler(request: httpx.Request) -> httpx.Response:
        generation_requests.append(request)
        return httpx.Response(
            200,
            json={
                "usage": {
                    "input_tokens": 17,
                    "output_tokens": 59,
                    "total_tokens": 76,
                }
            },
        )

    controller = FakeBudgetController()
    runner = OpenAIAgentsRunner(
        api_key="test",
        budget_controller=controller,
        generation_transport=httpx.MockTransport(generation_handler),
        count_transport=httpx.MockTransport(count_handler),
    )
    runner._openai = openai
    configured: list[object] = []
    runner._configure_client(
        SimpleNamespace(
            set_default_openai_client=lambda client, **_keywords: configured.append(client)
        )
    )
    client_request_id = str(uuid4())
    body = _responses_body()
    request_headers = _request_headers(client_request_id)
    request_headers.update(
        {
            "OpenAI-Organization": "org_test",
            "OpenAI-Project": "proj_test",
            "X-Private-Metadata": "must-not-forward",
        }
    )
    runner._budget_hooks.register(
        client_request_id,
        dispatch_context(),
        ProviderCallRole.EXTRACT,
    )
    try:
        response = await runner._http_clients[0].post(
            "https://api.openai.com/v1/responses",
            headers=request_headers,
            json=body,
        )
    finally:
        runner._budget_hooks.unregister(client_request_id)
        await runner.close()

    assert response.status_code == 200
    assert len(configured) == 1
    assert len(count_requests) == 1
    assert len(generation_requests) == 1
    _assert_responses_accounting(
        controller,
        count_requests[0],
        generation_requests[0],
        client_request_id,
        body,
    )


@pytest.mark.parametrize(
    ("updates", "removed"),
    [
        pytest.param({}, "max_output_tokens", id="missing-max-output"),
        pytest.param({"max_output_tokens": True}, None, id="boolean-max-output"),
        pytest.param({"max_output_tokens": 0}, None, id="zero-max-output"),
        pytest.param(
            {"max_output_tokens": PROVIDER_BUDGET_COUNTER_MAX + 1},
            None,
            id="oversized-max-output",
        ),
        pytest.param({"prompt": {"id": "pmpt_1"}}, None, id="prompt"),
        pytest.param({"context_management": []}, None, id="context-management"),
        pytest.param({"background": True}, None, id="background"),
        pytest.param({}, "store", id="missing-store"),
        pytest.param({"store": True}, None, id="stored"),
        pytest.param({"stream": True}, None, id="streaming"),
        pytest.param(
            {"tools": [{"type": "web_search"}], "max_tool_calls": 1},
            None,
            id="tool-call-limit",
        ),
    ],
)
@pytest.mark.asyncio
async def test_responses_hook_rejects_unsupported_requests_before_transport(
    updates: dict[str, Any],
    removed: str | None,
) -> None:
    transports = 0

    async def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal transports
        transports += 1
        return httpx.Response(200, json={})

    controller = FakeBudgetController()
    hooks = OpenAIResponsesBudgetHooks(controller)
    hooks.set_count_client(SimpleNamespace())
    client_request_id = str(uuid4())
    hooks.register(client_request_id, dispatch_context(), ProviderCallRole.EXTRACT)
    body = _responses_body()
    body.update(updates)
    if removed is not None:
        body.pop(removed)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        follow_redirects=False,
        event_hooks={"request": [hooks.request], "response": [hooks.response]},
    ) as client:
        with pytest.raises(OpenAIProviderDispatchError):
            await client.post(
                "https://api.openai.com/v1/responses",
                headers=_request_headers(client_request_id),
                json=body,
            )
    hooks.unregister(client_request_id)

    assert transports == 0
    assert controller.reservations == []


@pytest.mark.asyncio
async def test_responses_hook_rejects_unregistered_and_unknown_paths() -> None:
    transports = 0

    async def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal transports
        transports += 1
        return httpx.Response(200, json={})

    hooks = OpenAIResponsesBudgetHooks(FakeBudgetController())
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        follow_redirects=False,
        event_hooks={"request": [hooks.request]},
    ) as client:
        with pytest.raises(OpenAIProviderDispatchError):
            await client.post(
                "https://api.openai.com/v1/responses",
                headers=_request_headers(str(uuid4())),
                json=_responses_body(),
            )
        with pytest.raises(OpenAIProviderDispatchError):
            await client.post(
                "https://api.openai.com/v1/responses/input_tokens",
                headers=_request_headers(str(uuid4())),
                json=_responses_body(),
            )

    assert transports == 0


@pytest.mark.parametrize(
    "headers",
    [
        pytest.param([], id="missing-auth"),
        pytest.param(
            [("Authorization", "Bearer one"), ("Authorization", "Bearer two")],
            id="duplicate-auth",
        ),
    ],
)
@pytest.mark.asyncio
async def test_count_authentication_is_validated_before_reservation(
    headers: list[tuple[str, str]],
) -> None:
    transports = 0

    async def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal transports
        transports += 1
        return httpx.Response(200, json={})

    controller = FakeBudgetController()
    hooks = OpenAIResponsesBudgetHooks(controller)
    hooks.set_count_client(SimpleNamespace())
    client_request_id = str(uuid4())
    hooks.register(client_request_id, dispatch_context(), ProviderCallRole.EXTRACT)
    request_headers = [("X-Client-Request-Id", client_request_id), *headers]
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        event_hooks={"request": [hooks.request]},
    ) as client:
        with pytest.raises(OpenAIProviderDispatchError):
            await client.post(
                "https://api.openai.com/v1/responses",
                headers=request_headers,
                json=_responses_body(),
            )
    hooks.unregister(client_request_id)

    assert controller.reservations == []
    assert transports == 0


@pytest.mark.parametrize(
    ("mode", "status", "expected_type"),
    [
        pytest.param("status", 408, OpenAIAgentTimeoutError, id="408"),
        pytest.param("status", 429, OpenAIAgentRateLimitError, id="429"),
        pytest.param("status", 500, OpenAIAgentTransientError, id="500"),
        pytest.param("status", 400, OpenAIAgentInputError, id="400"),
        pytest.param("timeout", None, OpenAIAgentTimeoutError, id="timeout"),
        pytest.param("connection", None, OpenAIAgentTransientError, id="connection"),
    ],
)
@pytest.mark.asyncio
async def test_count_failures_are_sanitized_and_keep_preflight_reserved(
    mode: str,
    status: int | None,
    expected_type: type[Exception],
) -> None:
    marker = "private count provider detail"
    generation_calls = 0

    async def count_handler(request: httpx.Request) -> httpx.Response:
        if mode == "timeout":
            raise httpx.ReadTimeout(marker, request=request)
        if mode == "connection":
            raise httpx.ConnectError(marker, request=request)
        return httpx.Response(
            status or 500,
            headers={"retry-after": "4", "x-request-id": "req_count_safe"},
            json={"error": {"code": "rate_limit_exceeded", "message": marker}},
        )

    async def generation_handler(_request: httpx.Request) -> httpx.Response:
        nonlocal generation_calls
        generation_calls += 1
        return httpx.Response(200, json={})

    controller = FakeBudgetController()
    runner = OpenAIAgentsRunner(
        api_key="test",
        budget_controller=controller,
        generation_transport=httpx.MockTransport(generation_handler),
        count_transport=httpx.MockTransport(count_handler),
    )
    runner._openai = openai
    runner._configure_client(
        SimpleNamespace(set_default_openai_client=lambda *_args, **_kwargs: None)
    )
    client_request_id = str(uuid4())
    runner._budget_hooks.register(
        client_request_id,
        dispatch_context(),
        ProviderCallRole.EXTRACT,
    )
    try:
        with pytest.raises(expected_type) as captured:
            await runner._http_clients[0].post(
                "https://api.openai.com/v1/responses",
                headers=_request_headers(client_request_id),
                json=_responses_body(),
            )
    finally:
        runner._budget_hooks.unregister(client_request_id)
        await runner.close()

    error = captured.value
    assert error.__cause__ is None
    assert error.__context__ is None
    assert marker not in str(error)
    assert len(controller.reservations) == 1
    assert controller.reservations[0][1].operation is ProviderOperation.RESPONSES_PREFLIGHT
    assert controller.settlements == []
    assert generation_calls == 0
    if status is not None:
        assert getattr(error, "request_id", None) == "req_count_safe"
    if status == 429:
        assert getattr(error, "retry_after_seconds", None) == 4


@pytest.mark.parametrize(
    "content",
    [
        pytest.param(b"{}", id="missing"),
        pytest.param(b'{"input_tokens":true}', id="boolean"),
        pytest.param(b'{"input_tokens":-1}', id="negative"),
        pytest.param(b'{"input_tokens":1,"input_tokens":2}', id="duplicate"),
        pytest.param(b'{"input_tokens":NaN}', id="nonfinite"),
    ],
)
@pytest.mark.asyncio
async def test_invalid_successful_count_output_is_retryable_and_stays_reserved(
    content: bytes,
) -> None:
    async def count_handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=content)

    controller = FakeBudgetController()
    runner = OpenAIAgentsRunner(
        api_key="test",
        budget_controller=controller,
        generation_transport=httpx.MockTransport(lambda _request: httpx.Response(200, json={})),
        count_transport=httpx.MockTransport(count_handler),
    )
    runner._openai = openai
    runner._configure_client(
        SimpleNamespace(set_default_openai_client=lambda *_args, **_kwargs: None)
    )
    client_request_id = str(uuid4())
    runner._budget_hooks.register(
        client_request_id,
        dispatch_context(),
        ProviderCallRole.EXTRACT,
    )
    try:
        with pytest.raises(OpenAIAgentOutputError, match="invalid_output") as captured:
            await runner._http_clients[0].post(
                "https://api.openai.com/v1/responses",
                headers=_request_headers(client_request_id),
                json=_responses_body(),
            )
    finally:
        runner._budget_hooks.unregister(client_request_id)
        await runner.close()

    assert captured.value.__cause__ is None
    assert captured.value.__context__ is None
    assert len(controller.reservations) == 1
    assert controller.settlements == []


@pytest.mark.parametrize(
    ("status", "content"),
    [
        pytest.param(500, b'{"error":{"code":"server_error"}}', id="non-2xx"),
        pytest.param(200, b"{}", id="missing-usage"),
        pytest.param(
            200,
            b'{"usage":{"input_tokens":7,"output_tokens":2,"total_tokens":10}}',
            id="inconsistent-total",
        ),
        pytest.param(
            200,
            b'{"usage":{"input_tokens":true,"output_tokens":2,"total_tokens":3}}',
            id="boolean-usage",
        ),
        pytest.param(200, b'{"usage":NaN}', id="nonfinite-json"),
    ],
)
@pytest.mark.asyncio
async def test_generation_response_without_strict_usage_keeps_reservation_envelope(
    status: int,
    content: bytes,
) -> None:
    async def count_handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"input_tokens": 7})

    async def generation_handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(status, content=content)

    controller = FakeBudgetController()
    runner = OpenAIAgentsRunner(
        api_key="test",
        budget_controller=controller,
        generation_transport=httpx.MockTransport(generation_handler),
        count_transport=httpx.MockTransport(count_handler),
    )
    runner._openai = openai
    runner._configure_client(
        SimpleNamespace(set_default_openai_client=lambda *_args, **_kwargs: None)
    )
    client_request_id = str(uuid4())
    runner._budget_hooks.register(
        client_request_id,
        dispatch_context(),
        ProviderCallRole.EXTRACT,
    )
    try:
        response = await runner._http_clients[0].post(
            "https://api.openai.com/v1/responses",
            headers=_request_headers(client_request_id),
            json=_responses_body(),
        )
    finally:
        runner._budget_hooks.unregister(client_request_id)
        await runner.close()

    assert response.status_code == status
    assert len(controller.reservations) == 2
    assert controller.settlements == []


class _SettlementFailureController(FakeBudgetController):
    def __init__(self, failure: Exception) -> None:
        super().__init__()
        self.failure = failure

    async def settle(self, *_arguments: Any, **_keywords: Any) -> Any:
        raise self.failure


@pytest.mark.asyncio
async def test_response_hook_suppresses_ordinary_settlement_failure() -> None:
    hooks = OpenAIResponsesBudgetHooks(
        _SettlementFailureController(RuntimeError("private persistence detail"))
    )
    request = httpx.Request("POST", "https://api.openai.com/v1/responses")
    request.extensions["meeting_action_orchestrator.provider_reservation_id"] = uuid4()
    response = httpx.Response(
        200,
        request=request,
        json={"usage": {"input_tokens": 7, "output_tokens": 2, "total_tokens": 9}},
    )

    await hooks.response(response)


@pytest.mark.asyncio
async def test_response_hook_propagates_budget_integrity_failure() -> None:
    hooks = OpenAIResponsesBudgetHooks(_SettlementFailureController(ProviderBudgetIntegrityError()))
    request = httpx.Request("POST", "https://api.openai.com/v1/responses")
    request.extensions["meeting_action_orchestrator.provider_reservation_id"] = uuid4()
    response = httpx.Response(
        200,
        request=request,
        json={"usage": {"input_tokens": 7, "output_tokens": 2, "total_tokens": 9}},
    )

    with pytest.raises(ProviderBudgetIntegrityError):
        await hooks.response(response)
