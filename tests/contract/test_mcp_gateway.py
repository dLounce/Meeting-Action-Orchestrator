from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any
from uuid import UUID

import pytest
from mcp.types import CallToolResult, TextContent
from pydantic import ValidationError

from meeting_action_orchestrator.domain.enums import (
    DeadlineResolution,
    FailureCode,
    FailureDisposition,
    Priority,
    WriteStatus,
)
from meeting_action_orchestrator.domain.models import (
    CalendarEventProposal,
    ConnectorTarget,
    DateDeadline,
    DateTimeDeadline,
    PersonRef,
    TaskProposal,
    WriteIntent,
)
from meeting_action_orchestrator.infrastructure.mcp_gateway import (
    McpGateway,
    McpToolNames,
    PermanentMcpError,
    RetryableMcpError,
    UnknownMcpOutcomeError,
)

NOW = datetime(2026, 8, 7, 9, 0, tzinfo=timezone.utc)
TASK_KEY = f"mao_v1_{'a' * 64}"
EVENT_KEY = f"mao_v1_{'b' * 64}"
TOOLS = McpToolNames(
    task="tasks.create",
    calendar="calendar.create_event",
    lookup="actions.find_by_idempotency_key",
)


def uid(value: int) -> UUID:
    return UUID(int=value)


@dataclass(frozen=True)
class RecordedCall:
    name: str
    arguments: dict[str, Any] | None
    meta: dict[str, Any] | None


class FakeMcpClient:
    def __init__(self, *responses: CallToolResult | Exception) -> None:
        self.responses = list(responses)
        self.calls: list[RecordedCall] = []

    async def call_tool(
        self,
        name: str,
        arguments: dict[str, Any] | None = None,
        *,
        meta: dict[str, Any] | None = None,
    ) -> CallToolResult:
        self.calls.append(RecordedCall(name=name, arguments=arguments, meta=meta))
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


class ApprovedRegistry:
    def __init__(self, *intents: WriteIntent) -> None:
        self.intents = {intent.id: intent for intent in intents}

    async def permits(self, intent: WriteIntent) -> bool:
        return self.intents.get(intent.id) == intent


class UnavailableRegistry:
    async def permits(self, intent: WriteIntent) -> bool:
        del intent
        raise RuntimeError("database unavailable")


def task_intent(
    *,
    status: WriteStatus = WriteStatus.IN_FLIGHT,
    description: str = "Publish the reviewed launch brief",
) -> WriteIntent:
    proposal = TaskProposal(
        source_action_id=uid(10),
        target=ConnectorTarget(connector_id="notion", resource_id="launch-board"),
        title="Publish launch brief",
        description=description,
        assignee=PersonRef(display_name="Mira", email="mira@example.com"),
        deadline=DateDeadline(
            value=date(2026, 8, 14),
            timezone="Asia/Calcutta",
            source_text="by August 14",
            resolution=DeadlineResolution.EXPLICIT,
        ),
        priority=Priority.HIGH,
    )
    in_flight = status is WriteStatus.IN_FLIGHT
    return WriteIntent(
        id=uid(20),
        meeting_id=uid(21),
        approval_id=uid(22),
        idempotency_key=TASK_KEY,
        proposal=proposal,
        status=status,
        attempt_count=1 if in_flight else 0,
        lease_owner="worker-1" if in_flight else None,
        lease_expires_at=NOW + timedelta(minutes=2) if in_flight else None,
        created_at=NOW - timedelta(minutes=1),
        updated_at=NOW,
    )


def event_intent() -> WriteIntent:
    proposal = CalendarEventProposal(
        source_action_id=uid(30),
        target=ConnectorTarget(connector_id="google", resource_id="primary"),
        title="Launch brief deadline",
        description="Review the approved brief",
        deadline=DateTimeDeadline(
            at=datetime(2026, 8, 14, 15, 30, tzinfo=timezone.utc),
            timezone="Asia/Calcutta",
            source_text="August 14 at 9 PM",
            resolution=DeadlineResolution.EXPLICIT,
        ),
        duration_minutes=30,
    )
    return WriteIntent(
        id=uid(31),
        meeting_id=uid(32),
        approval_id=uid(33),
        idempotency_key=EVENT_KEY,
        proposal=proposal,
        status=WriteStatus.IN_FLIGHT,
        attempt_count=1,
        lease_owner="worker-1",
        lease_expires_at=NOW + timedelta(minutes=2),
        created_at=NOW - timedelta(minutes=1),
        updated_at=NOW,
    )


def success(
    intent: WriteIntent,
    *,
    outcome: str = "succeeded",
    external_url: str | None = "https://tasks.example.com/actions/remote-1",
    overrides: dict[str, Any] | None = None,
    is_error: bool = False,
) -> CallToolResult:
    content: dict[str, Any] = {
        "outcome": outcome,
        "intent_id": str(intent.id),
        "idempotency_key": intent.idempotency_key,
        "payload_digest": intent.payload_digest,
        "external_id": "remote-1",
        "external_url": external_url,
    }
    content.update(overrides or {})
    return CallToolResult(content=[], structuredContent=content, isError=is_error)


def gateway(
    intent: WriteIntent,
    response: CallToolResult | Exception,
    *,
    approved: bool = True,
    max_argument_bytes: int = 65_536,
    max_response_bytes: int = 32_768,
) -> tuple[McpGateway, FakeMcpClient]:
    client = FakeMcpClient(response)
    registry = ApprovedRegistry(intent) if approved else ApprovedRegistry()
    adapter = McpGateway(
        client,
        TOOLS,
        registry,
        provider="trusted-mcp",
        clock=lambda: NOW,
        max_argument_bytes=max_argument_bytes,
        max_response_bytes=max_response_bytes,
    )
    return adapter, client


@pytest.mark.asyncio
async def test_task_write_uses_only_the_configured_tool_and_bounded_json() -> None:
    intent = task_intent()
    adapter, client = gateway(intent, success(intent))

    receipt = await adapter.ensure_task(intent)

    assert adapter.allowed_tools == {
        "tasks.create",
        "calendar.create_event",
        "actions.find_by_idempotency_key",
    }
    assert len(client.calls) == 1
    call = client.calls[0]
    assert call.name == "tasks.create"
    assert call.meta == {
        "idempotencyKey": intent.idempotency_key,
        "payloadDigest": intent.payload_digest,
    }
    assert call.arguments == {
        "schema_version": 1,
        "intent_id": str(intent.id),
        "meeting_id": str(intent.meeting_id),
        "approval_id": str(intent.approval_id),
        "idempotency_key": intent.idempotency_key,
        "payload_digest": intent.payload_digest,
        "target": {"connector_id": "notion", "resource_id": "launch-board"},
        "task": {
            "source_action_id": str(intent.proposal.source_action_id),
            "title": "Publish launch brief",
            "description": "Publish the reviewed launch brief",
            "assignee": {"display_name": "Mira", "email": "mira@example.com"},
            "deadline": {
                "kind": "date",
                "value": "2026-08-14",
                "timezone": "Asia/Calcutta",
                "source_text": "by August 14",
                "resolution": "explicit",
            },
            "priority": "high",
        },
    }
    assert receipt.intent_id == intent.id
    assert receipt.idempotency_key == intent.idempotency_key
    assert receipt.payload_digest == intent.payload_digest
    assert receipt.provider == "trusted-mcp"
    assert receipt.external_id == "remote-1"
    assert receipt.external_url == "https://tasks.example.com/actions/remote-1"
    assert receipt.reconciled is False
    assert receipt.recorded_at == NOW


@pytest.mark.asyncio
async def test_calendar_write_maps_the_typed_event_proposal() -> None:
    intent = event_intent()
    adapter, client = gateway(
        intent,
        success(intent, external_url="http://calendar.example.com/events/remote-1"),
    )

    receipt = await adapter.ensure_event(intent)

    call = client.calls[0]
    assert call.name == "calendar.create_event"
    assert call.arguments is not None
    assert call.arguments["target"] == {"connector_id": "google", "resource_id": "primary"}
    assert call.arguments["event"] == {
        "source_action_id": str(intent.proposal.source_action_id),
        "title": "Launch brief deadline",
        "description": "Review the approved brief",
        "deadline": {
            "kind": "datetime",
            "at": "2026-08-14T15:30:00Z",
            "timezone": "Asia/Calcutta",
            "source_text": "August 14 at 9 PM",
            "resolution": "explicit",
        },
        "duration_minutes": 30,
    }
    assert receipt.external_url == "http://calendar.example.com/events/remote-1"


@pytest.mark.asyncio
async def test_lookup_returns_a_reconciled_receipt() -> None:
    intent = task_intent()
    adapter, client = gateway(intent, success(intent, outcome="found"))

    receipt = await adapter.find_task(intent.idempotency_key)

    assert receipt is not None
    assert receipt.intent_id == intent.id
    assert receipt.payload_digest == intent.payload_digest
    assert receipt.reconciled is True
    assert client.calls[0] == RecordedCall(
        name="actions.find_by_idempotency_key",
        arguments={
            "schema_version": 1,
            "kind": "task",
            "idempotency_key": intent.idempotency_key,
        },
        meta={"idempotencyKey": intent.idempotency_key},
    )


@pytest.mark.asyncio
async def test_lookup_returns_none_for_a_known_absence() -> None:
    intent = event_intent()
    response = CallToolResult(
        content=[],
        structuredContent={"outcome": "not_found"},
        isError=False,
    )
    adapter, client = gateway(intent, response)

    receipt = await adapter.find_event(intent.idempotency_key)

    assert receipt is None
    assert client.calls[0].arguments == {
        "schema_version": 1,
        "kind": "calendar_event",
        "idempotency_key": intent.idempotency_key,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("candidate", "approved"),
    [
        (task_intent(status=WriteStatus.PENDING), True),
        (task_intent(), False),
    ],
)
async def test_unclaimed_or_unapproved_intents_never_reach_mcp(
    candidate: WriteIntent,
    approved: bool,
) -> None:
    adapter, client = gateway(candidate, success(candidate), approved=approved)

    with pytest.raises(PermanentMcpError) as captured:
        await adapter.ensure_task(candidate)

    assert captured.value.code is FailureCode.INVALID_INPUT
    assert captured.value.disposition is FailureDisposition.PERMANENT
    assert not client.calls


@pytest.mark.asyncio
async def test_indeterminate_authorization_is_retryable_before_dispatch() -> None:
    intent = task_intent()
    client = FakeMcpClient(success(intent))
    adapter = McpGateway(
        client,
        TOOLS,
        UnavailableRegistry(),
        provider="trusted-mcp",
        clock=lambda: NOW,
    )

    with pytest.raises(RetryableMcpError) as captured:
        await adapter.ensure_task(intent)

    assert captured.value.code is FailureCode.PROVIDER_UNAVAILABLE
    assert captured.value.disposition is FailureDisposition.RETRYABLE
    assert not client.calls


@pytest.mark.asyncio
async def test_write_method_rejects_the_wrong_proposal_kind() -> None:
    intent = event_intent()
    adapter, client = gateway(intent, success(intent))

    with pytest.raises(PermanentMcpError):
        await adapter.ensure_task(intent)

    assert not client.calls


@pytest.mark.asyncio
async def test_forged_payload_digest_never_reaches_mcp() -> None:
    intent = task_intent().model_copy(update={"payload_digest": "f" * 64})
    adapter, client = gateway(intent, success(intent))

    with pytest.raises(PermanentMcpError) as captured:
        await adapter.ensure_task(intent)

    assert captured.value.code is FailureCode.INVALID_INPUT
    assert not client.calls


@pytest.mark.asyncio
async def test_invalid_lookup_key_never_reaches_mcp() -> None:
    intent = task_intent()
    adapter, client = gateway(intent, success(intent, outcome="found"))

    with pytest.raises(PermanentMcpError) as captured:
        await adapter.find_task("invalid-key")

    assert captured.value.code is FailureCode.INVALID_INPUT
    assert not client.calls


def test_tool_allowlist_requires_distinct_exact_names() -> None:
    with pytest.raises(ValidationError):
        McpToolNames(task="tasks.create", calendar="tasks.create", lookup="actions.find")

    with pytest.raises(ValidationError):
        McpToolNames(task="tasks create", calendar="calendar.create", lookup="actions.find")


@pytest.mark.asyncio
async def test_text_content_is_never_parsed_as_a_tool_result() -> None:
    intent = task_intent(description="confidential roadmap")
    response = CallToolResult(
        content=[TextContent(type="text", text='{"outcome":"succeeded"}')],
        structuredContent=None,
        isError=False,
    )
    adapter, _ = gateway(intent, response)

    with pytest.raises(UnknownMcpOutcomeError) as captured:
        await adapter.ensure_task(intent)

    assert "confidential roadmap" not in str(captured.value)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "external_url",
    [
        "file:///tmp/task",
        "javascript:alert(1)",
        "https://user:secret@example.com/task",
        "https://example.com\nAuthorization: Bearer secret",
    ],
)
async def test_write_receipts_reject_unsafe_external_urls(external_url: str) -> None:
    intent = task_intent()
    adapter, _ = gateway(intent, success(intent, external_url=external_url))

    with pytest.raises(UnknownMcpOutcomeError):
        await adapter.ensure_task(intent)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("intent_id", str(uid(999))),
        ("idempotency_key", f"mao_v1_{'c' * 64}"),
        ("payload_digest", "d" * 64),
    ],
)
async def test_write_receipt_must_echo_the_exact_intent_binding(
    field: str,
    value: str,
) -> None:
    intent = task_intent()
    adapter, _ = gateway(intent, success(intent, overrides={field: value}))

    with pytest.raises(PermanentMcpError) as captured:
        await adapter.ensure_task(intent)

    assert captured.value.code is FailureCode.IDEMPOTENCY_CONFLICT


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("outcome", "code", "error_type", "disposition", "expected_code"),
    [
        (
            "retryable_failure",
            "rate_limited",
            RetryableMcpError,
            FailureDisposition.RETRYABLE,
            FailureCode.RATE_LIMITED,
        ),
        (
            "permanent_failure",
            "auth",
            PermanentMcpError,
            FailureDisposition.PERMANENT,
            FailureCode.CONNECTOR_AUTH,
        ),
        (
            "unknown_outcome",
            "timeout",
            UnknownMcpOutcomeError,
            FailureDisposition.UNKNOWN_OUTCOME,
            FailureCode.UNKNOWN_REMOTE_OUTCOME,
        ),
    ],
)
async def test_connector_failures_keep_retry_semantics(
    outcome: str,
    code: str,
    error_type: type[Exception],
    disposition: FailureDisposition,
    expected_code: FailureCode,
) -> None:
    intent = task_intent()
    response = CallToolResult(
        content=[],
        structuredContent={
            "outcome": outcome,
            "code": code,
            "message": "server payload must remain private",
            "request_id": "request-123",
        },
        isError=True,
    )
    adapter, _ = gateway(intent, response)

    with pytest.raises(error_type) as captured:
        await adapter.ensure_task(intent)

    error = captured.value
    assert isinstance(error, (RetryableMcpError, PermanentMcpError, UnknownMcpOutcomeError))
    assert error.disposition is disposition
    assert error.code is expected_code
    assert error.provider_request_id == "request-123"
    assert "server payload" not in str(error)


@pytest.mark.asyncio
async def test_transport_failure_after_write_is_an_unknown_outcome() -> None:
    intent = task_intent(description="private board payload")
    adapter, _ = gateway(intent, RuntimeError("Bearer credential-123 private board payload"))

    with pytest.raises(UnknownMcpOutcomeError) as captured:
        await adapter.ensure_task(intent)

    assert captured.value.disposition is FailureDisposition.UNKNOWN_OUTCOME
    assert captured.value.code is FailureCode.UNKNOWN_REMOTE_OUTCOME
    assert "credential-123" not in str(captured.value)
    assert "private board payload" not in str(captured.value)


@pytest.mark.asyncio
async def test_transport_failure_during_lookup_is_retryable() -> None:
    intent = task_intent()
    adapter, _ = gateway(intent, TimeoutError("Bearer credential-123"))

    with pytest.raises(RetryableMcpError) as captured:
        await adapter.find_task(intent.idempotency_key)

    assert captured.value.disposition is FailureDisposition.RETRYABLE
    assert captured.value.code is FailureCode.PROVIDER_UNAVAILABLE
    assert "credential-123" not in str(captured.value)


@pytest.mark.asyncio
async def test_invalid_lookup_result_is_permanent_without_exposing_content() -> None:
    intent = task_intent()
    response = success(
        intent,
        outcome="found",
        external_url="https://user:password@example.com/task",
    )
    adapter, _ = gateway(intent, response)

    with pytest.raises(PermanentMcpError) as captured:
        await adapter.find_task(intent.idempotency_key)

    assert captured.value.code is FailureCode.CONNECTOR_REJECTED
    assert "password" not in str(captured.value)


@pytest.mark.asyncio
async def test_argument_size_limit_blocks_the_call_without_leaking_payload() -> None:
    intent = task_intent(description="sensitive" * 1_000)
    adapter, client = gateway(intent, success(intent), max_argument_bytes=1_024)

    with pytest.raises(PermanentMcpError) as captured:
        await adapter.ensure_task(intent)

    assert captured.value.code is FailureCode.INVALID_INPUT
    assert "sensitive" not in str(captured.value)
    assert not client.calls


@pytest.mark.asyncio
async def test_oversized_write_response_is_an_unknown_outcome() -> None:
    intent = task_intent()
    response = CallToolResult(
        content=[],
        structuredContent={
            "outcome": "unknown_outcome",
            "message": "x" * 2_000,
        },
        isError=True,
    )
    adapter, _ = gateway(intent, response, max_response_bytes=1_024)

    with pytest.raises(UnknownMcpOutcomeError):
        await adapter.ensure_task(intent)


@pytest.mark.asyncio
async def test_success_marked_as_an_mcp_error_is_not_accepted() -> None:
    intent = task_intent()
    adapter, _ = gateway(intent, success(intent, is_error=True))

    with pytest.raises(UnknownMcpOutcomeError):
        await adapter.ensure_task(intent)
