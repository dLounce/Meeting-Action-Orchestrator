from __future__ import annotations

import json
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Annotated, Any, Literal, Protocol, cast
from urllib.parse import urlsplit
from uuid import UUID, uuid5

from mcp.types import CallToolResult
from pydantic import (
    AnyHttpUrl,
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    TypeAdapter,
    ValidationError,
    field_validator,
    model_validator,
)

from meeting_action_orchestrator.application.errors import (
    DeliveryGatewayError,
    PermanentDeliveryError,
    RetryableDeliveryError,
    UnknownDeliveryOutcomeError,
)
from meeting_action_orchestrator.domain.enums import (
    FailureCode,
    FailureDisposition,
    WriteKind,
    WriteStatus,
)
from meeting_action_orchestrator.domain.hashing import canonical_sha256
from meeting_action_orchestrator.domain.models import (
    CalendarEventProposal,
    TaskProposal,
    WriteIntent,
    WriteReceipt,
)

_RECEIPT_NAMESPACE = UUID("71d441ab-77fd-5b85-9581-415d894bbb50")
_TOOL_NAME_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9_.:/-]{0,127}$"
_SAFE_RETRYABLE_MESSAGE = "The connector is temporarily unavailable"
_SAFE_PERMANENT_MESSAGE = "The connector rejected the operation"
_SAFE_UNKNOWN_MESSAGE = "The connector outcome is unknown and requires reconciliation"
_SAFE_INTENT_MESSAGE = "The write intent is not eligible for execution"
_SAFE_RESPONSE_MESSAGE = "The connector returned an invalid response"

ToolName = Annotated[str, StringConstraints(pattern=_TOOL_NAME_PATTERN)]
BoundedIdentifier = Annotated[
    str,
    StringConstraints(min_length=1, max_length=200, pattern=r"^[^\x00-\x1f\x7f]+$"),
]
IdempotencyKey = Annotated[str, StringConstraints(pattern=r"^mao_v1_[0-9a-f]{64}$")]
Digest = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
_HTTP_URL_ADAPTER = TypeAdapter(AnyHttpUrl)


class McpGatewayError(DeliveryGatewayError):
    pass


class RetryableMcpError(McpGatewayError, RetryableDeliveryError):
    pass


class PermanentMcpError(McpGatewayError, PermanentDeliveryError):
    pass


class UnknownMcpOutcomeError(McpGatewayError, UnknownDeliveryOutcomeError):
    pass


class McpToolNames(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=True)

    task: ToolName
    calendar: ToolName
    lookup: ToolName

    @model_validator(mode="after")
    def validate_distinct_names(self) -> McpToolNames:
        if len({self.task, self.calendar, self.lookup}) != 3:
            raise ValueError("MCP tool names must be distinct")
        return self


class ApprovedIntentAuthorizer(Protocol):
    async def permits(self, intent: WriteIntent) -> bool: ...


class McpToolClient(Protocol):
    async def call_tool(
        self,
        name: str,
        arguments: dict[str, Any] | None = None,
        *,
        meta: dict[str, Any] | None = None,
    ) -> CallToolResult: ...


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=True)


class _ReceiptResult(_StrictModel):
    outcome: Literal["succeeded", "found"]
    intent_id: UUID
    idempotency_key: IdempotencyKey
    payload_digest: Digest
    external_id: BoundedIdentifier
    external_url: Annotated[str, StringConstraints(min_length=1, max_length=2_000)] | None = None

    @field_validator("external_url")
    @classmethod
    def validate_external_url(cls, value: str | None) -> str | None:
        if value is None:
            return None
        if any(ord(character) < 32 or ord(character) == 127 for character in value):
            raise ValueError("external_url contains control characters")
        try:
            _HTTP_URL_ADAPTER.validate_python(value)
        except ValidationError as error:
            raise ValueError("external_url is invalid") from error
        parsed = urlsplit(value)
        if parsed.scheme.casefold() not in {"http", "https"}:
            raise ValueError("external_url must use HTTP or HTTPS")
        try:
            hostname = parsed.hostname
            port = parsed.port
        except ValueError as error:
            raise ValueError("external_url is invalid") from error
        if hostname is None or (port is not None and not 1 <= port <= 65_535):
            raise ValueError("external_url must have a valid host")
        if parsed.username is not None or parsed.password is not None:
            raise ValueError("external_url cannot contain credentials")
        return value


class _NotFoundResult(_StrictModel):
    outcome: Literal["not_found"]


class _FailureResult(_StrictModel):
    outcome: Literal["retryable_failure", "permanent_failure", "unknown_outcome"]
    code: Annotated[str, StringConstraints(min_length=1, max_length=100)] | None = None
    message: Annotated[str, StringConstraints(max_length=1_000)] | None = None
    request_id: BoundedIdentifier | None = None


_ToolResult = Annotated[
    _ReceiptResult | _NotFoundResult | _FailureResult,
    Field(discriminator="outcome"),
]
_TOOL_RESULT_ADAPTER = TypeAdapter(_ToolResult)


class _LookupRequest(_StrictModel):
    schema_version: Literal[1] = 1
    kind: WriteKind
    idempotency_key: IdempotencyKey


class McpGateway:
    def __init__(
        self,
        client: McpToolClient,
        tools: McpToolNames,
        authorizer: ApprovedIntentAuthorizer,
        *,
        provider: str = "mcp",
        clock: Callable[[], datetime] | None = None,
        max_argument_bytes: int = 65_536,
        max_response_bytes: int = 32_768,
    ) -> None:
        if not 1_024 <= max_argument_bytes <= 1_048_576:
            raise ValueError("max_argument_bytes must be between 1024 and 1048576")
        if not 1_024 <= max_response_bytes <= 1_048_576:
            raise ValueError("max_response_bytes must be between 1024 and 1048576")
        if not provider or len(provider) > 200 or provider != provider.strip():
            raise ValueError("provider must be between 1 and 200 characters")
        self._client = client
        self._tools = tools
        self._authorizer = authorizer
        self._provider = provider
        self._clock = clock or _utc_now
        self._max_argument_bytes = max_argument_bytes
        self._max_response_bytes = max_response_bytes
        self._allowed_tools = frozenset((tools.task, tools.calendar, tools.lookup))

    @property
    def allowed_tools(self) -> frozenset[str]:
        return self._allowed_tools

    async def ensure_task(self, intent: WriteIntent) -> WriteReceipt:
        approved = await self._approved_intent(intent, WriteKind.TASK)
        proposal = cast(TaskProposal, approved.proposal)
        arguments = {
            "schema_version": 1,
            "intent_id": str(approved.id),
            "meeting_id": str(approved.meeting_id),
            "approval_id": str(approved.approval_id),
            "idempotency_key": approved.idempotency_key,
            "payload_digest": approved.payload_digest,
            "target": proposal.target.model_dump(mode="json"),
            "task": {
                "source_action_id": str(proposal.source_action_id),
                "title": proposal.title,
                "description": proposal.description,
                "assignee": _json_model(proposal.assignee),
                "deadline": _json_model(proposal.deadline),
                "priority": proposal.priority.value,
            },
        }
        result = await self._invoke_write(self._tools.task, approved, arguments)
        return self._receipt(approved, result, reconciled=False)

    async def ensure_event(self, intent: WriteIntent) -> WriteReceipt:
        approved = await self._approved_intent(intent, WriteKind.CALENDAR_EVENT)
        proposal = cast(CalendarEventProposal, approved.proposal)
        arguments = {
            "schema_version": 1,
            "intent_id": str(approved.id),
            "meeting_id": str(approved.meeting_id),
            "approval_id": str(approved.approval_id),
            "idempotency_key": approved.idempotency_key,
            "payload_digest": approved.payload_digest,
            "target": proposal.target.model_dump(mode="json"),
            "event": {
                "source_action_id": str(proposal.source_action_id),
                "title": proposal.title,
                "description": proposal.description,
                "deadline": proposal.deadline.model_dump(mode="json"),
                "duration_minutes": proposal.duration_minutes,
            },
        }
        result = await self._invoke_write(self._tools.calendar, approved, arguments)
        return self._receipt(approved, result, reconciled=False)

    async def find_task(self, idempotency_key: str) -> WriteReceipt | None:
        return await self._find(WriteKind.TASK, idempotency_key)

    async def find_event(self, idempotency_key: str) -> WriteReceipt | None:
        return await self._find(WriteKind.CALENDAR_EVENT, idempotency_key)

    async def _approved_intent(self, intent: WriteIntent, kind: WriteKind) -> WriteIntent:
        if not isinstance(intent, WriteIntent):
            raise _permanent(FailureCode.INVALID_INPUT, _SAFE_INTENT_MESSAGE)
        try:
            validated = WriteIntent.model_validate(intent.model_dump(mode="python"))
        except (TypeError, ValueError, ValidationError):
            raise _permanent(FailureCode.INVALID_INPUT, _SAFE_INTENT_MESSAGE) from None
        if validated.status is not WriteStatus.IN_FLIGHT or validated.proposal.kind is not kind:
            raise _permanent(FailureCode.INVALID_INPUT, _SAFE_INTENT_MESSAGE)
        if canonical_sha256(validated.proposal) != validated.payload_digest:
            raise _permanent(FailureCode.INVALID_INPUT, _SAFE_INTENT_MESSAGE)
        try:
            permitted = await self._authorizer.permits(validated)
        except Exception:
            raise _retryable(FailureCode.PROVIDER_UNAVAILABLE, _SAFE_RETRYABLE_MESSAGE) from None
        if permitted is not True:
            raise _permanent(FailureCode.INVALID_INPUT, _SAFE_INTENT_MESSAGE)
        return validated

    async def _find(
        self,
        kind: WriteKind,
        idempotency_key: str,
    ) -> WriteReceipt | None:
        try:
            request = _LookupRequest(kind=kind, idempotency_key=idempotency_key)
        except ValidationError:
            raise _permanent(FailureCode.INVALID_INPUT, _SAFE_INTENT_MESSAGE) from None
        arguments = request.model_dump(mode="json")
        result = await self._invoke(
            self._tools.lookup,
            arguments,
            {"idempotencyKey": request.idempotency_key},
            write_operation=False,
        )
        if isinstance(result, _NotFoundResult):
            return None
        if not isinstance(result, _ReceiptResult) or result.outcome != "found":
            raise _permanent(FailureCode.CONNECTOR_REJECTED, _SAFE_RESPONSE_MESSAGE)
        if result.idempotency_key != request.idempotency_key:
            raise _permanent(FailureCode.IDEMPOTENCY_CONFLICT, _SAFE_RESPONSE_MESSAGE)
        return self._reconciled_receipt(result)

    async def _invoke_write(
        self,
        tool_name: str,
        intent: WriteIntent,
        arguments: dict[str, Any],
    ) -> _ReceiptResult:
        result = await self._invoke(
            tool_name,
            arguments,
            {
                "idempotencyKey": intent.idempotency_key,
                "payloadDigest": intent.payload_digest,
            },
            write_operation=True,
        )
        if not isinstance(result, _ReceiptResult) or result.outcome != "succeeded":
            raise _unknown(FailureCode.UNKNOWN_REMOTE_OUTCOME, _SAFE_RESPONSE_MESSAGE)
        if result.intent_id != intent.id:
            raise _unknown(FailureCode.IDEMPOTENCY_CONFLICT, _SAFE_RESPONSE_MESSAGE)
        if (
            result.idempotency_key != intent.idempotency_key
            or result.payload_digest != intent.payload_digest
        ):
            raise _unknown(FailureCode.IDEMPOTENCY_CONFLICT, _SAFE_RESPONSE_MESSAGE)
        return result

    async def _invoke(
        self,
        tool_name: str,
        arguments: dict[str, Any],
        meta: dict[str, Any],
        *,
        write_operation: bool,
    ) -> _ToolResult:
        if tool_name not in self._allowed_tools:
            raise _permanent(FailureCode.INVALID_INPUT, _SAFE_INTENT_MESSAGE)
        normalized_arguments, _ = self._bounded_json_object(
            arguments,
            self._max_argument_bytes,
            _permanent(FailureCode.INVALID_INPUT, _SAFE_INTENT_MESSAGE),
        )
        try:
            response = await self._client.call_tool(
                tool_name,
                arguments=normalized_arguments,
                meta=meta,
            )
        except McpGatewayError:
            raise
        except Exception:
            if write_operation:
                raise _unknown(
                    FailureCode.UNKNOWN_REMOTE_OUTCOME,
                    _SAFE_UNKNOWN_MESSAGE,
                ) from None
            raise _retryable(
                FailureCode.PROVIDER_UNAVAILABLE,
                _SAFE_RETRYABLE_MESSAGE,
            ) from None
        try:
            structured = response.structuredContent
            is_error = response.isError
        except (AttributeError, TypeError):
            return self._invalid_response(write_operation)
        if not isinstance(is_error, bool) or not isinstance(structured, dict):
            return self._invalid_response(write_operation)
        response_error: McpGatewayError
        if write_operation:
            response_error = _unknown(
                FailureCode.UNKNOWN_REMOTE_OUTCOME,
                _SAFE_RESPONSE_MESSAGE,
            )
        else:
            response_error = _permanent(
                FailureCode.CONNECTOR_REJECTED,
                _SAFE_RESPONSE_MESSAGE,
            )
        _, encoded_response = self._bounded_json_object(
            structured,
            self._max_response_bytes,
            response_error,
        )
        try:
            result = _TOOL_RESULT_ADAPTER.validate_json(encoded_response, strict=True)
        except ValidationError:
            return self._invalid_response(write_operation)
        if isinstance(result, _FailureResult):
            raise _classified_failure(result)
        if is_error:
            return self._invalid_response(write_operation)
        return result

    def _bounded_json_object(
        self,
        value: dict[str, Any],
        maximum_bytes: int,
        invalid_error: McpGatewayError,
    ) -> tuple[dict[str, Any], bytes]:
        try:
            encoded = json.dumps(
                value,
                allow_nan=False,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
            if len(encoded) > maximum_bytes:
                raise ValueError
            decoded = json.loads(encoded)
        except (TypeError, ValueError, UnicodeError):
            raise invalid_error from None
        if not isinstance(decoded, dict):
            raise invalid_error
        return cast(dict[str, Any], decoded), encoded

    def _invalid_response(self, write_operation: bool) -> _ToolResult:
        if write_operation:
            raise _unknown(FailureCode.UNKNOWN_REMOTE_OUTCOME, _SAFE_RESPONSE_MESSAGE)
        raise _permanent(FailureCode.CONNECTOR_REJECTED, _SAFE_RESPONSE_MESSAGE)

    def _receipt(
        self,
        intent: WriteIntent,
        result: _ReceiptResult,
        *,
        reconciled: bool,
    ) -> WriteReceipt:
        recorded_at = self._recorded_at()
        return WriteReceipt(
            id=_receipt_id(
                self._provider,
                result.idempotency_key,
                result.payload_digest,
                result.external_id,
            ),
            intent_id=intent.id,
            idempotency_key=intent.idempotency_key,
            payload_digest=intent.payload_digest,
            provider=self._provider,
            external_id=result.external_id,
            external_url=result.external_url,
            reconciled=reconciled,
            recorded_at=recorded_at,
        )

    def _reconciled_receipt(self, result: _ReceiptResult) -> WriteReceipt:
        return WriteReceipt(
            id=_receipt_id(
                self._provider,
                result.idempotency_key,
                result.payload_digest,
                result.external_id,
            ),
            intent_id=result.intent_id,
            idempotency_key=result.idempotency_key,
            payload_digest=result.payload_digest,
            provider=self._provider,
            external_id=result.external_id,
            external_url=result.external_url,
            reconciled=True,
            recorded_at=self._recorded_at(),
        )

    def _recorded_at(self) -> datetime:
        try:
            value = self._clock()
        except Exception:
            raise _unknown(FailureCode.INTERNAL, _SAFE_RESPONSE_MESSAGE) from None
        if value.tzinfo is None or value.utcoffset() is None:
            raise _unknown(FailureCode.INTERNAL, _SAFE_RESPONSE_MESSAGE)
        return value


def _json_model(value: BaseModel | None) -> dict[str, Any] | None:
    return None if value is None else value.model_dump(mode="json")


def _receipt_id(
    provider: str,
    idempotency_key: str,
    payload_digest: str,
    external_id: str,
) -> UUID:
    material = {
        "provider": provider,
        "idempotency_key": idempotency_key,
        "payload_digest": payload_digest,
        "external_id": external_id,
    }
    return uuid5(_RECEIPT_NAMESPACE, canonical_sha256(material))


def _classified_failure(result: _FailureResult) -> McpGatewayError:
    if result.outcome == "retryable_failure":
        return _retryable(
            _retryable_code(result.code),
            _SAFE_RETRYABLE_MESSAGE,
            result.request_id,
        )
    if result.outcome == "permanent_failure":
        return _permanent(
            _permanent_code(result.code),
            _SAFE_PERMANENT_MESSAGE,
            result.request_id,
        )
    return _unknown(
        FailureCode.UNKNOWN_REMOTE_OUTCOME,
        _SAFE_UNKNOWN_MESSAGE,
        result.request_id,
    )


def _retryable_code(value: str | None) -> FailureCode:
    codes = {
        "rate_limited": FailureCode.RATE_LIMITED,
        "timeout": FailureCode.PROVIDER_TIMEOUT,
        "unavailable": FailureCode.PROVIDER_UNAVAILABLE,
    }
    return codes.get(value, FailureCode.PROVIDER_UNAVAILABLE)


def _permanent_code(value: str | None) -> FailureCode:
    codes = {
        "auth": FailureCode.CONNECTOR_AUTH,
        "target_missing": FailureCode.CONNECTOR_TARGET_MISSING,
        "rejected": FailureCode.CONNECTOR_REJECTED,
        "invalid_request": FailureCode.INVALID_INPUT,
        "idempotency_conflict": FailureCode.IDEMPOTENCY_CONFLICT,
    }
    return codes.get(value, FailureCode.CONNECTOR_REJECTED)


def _retryable(
    code: FailureCode,
    safe_message: str,
    request_id: str | None = None,
) -> RetryableMcpError:
    return RetryableMcpError(
        code,
        FailureDisposition.RETRYABLE,
        safe_message,
        request_id,
    )


def _permanent(
    code: FailureCode,
    safe_message: str,
    request_id: str | None = None,
) -> PermanentMcpError:
    return PermanentMcpError(
        code,
        FailureDisposition.PERMANENT,
        safe_message,
        request_id,
    )


def _unknown(
    code: FailureCode,
    safe_message: str,
    request_id: str | None = None,
) -> UnknownMcpOutcomeError:
    return UnknownMcpOutcomeError(
        code,
        FailureDisposition.UNKNOWN_OUTCOME,
        safe_message,
        request_id,
    )


def _utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)
