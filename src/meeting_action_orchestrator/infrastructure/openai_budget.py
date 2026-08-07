from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, cast
from uuid import UUID, uuid4

import httpx

from meeting_action_orchestrator.application.errors import ProviderBudgetIntegrityError
from meeting_action_orchestrator.application.ports import (
    ProviderBudgetController,
    TranscriptionRunContext,
)
from meeting_action_orchestrator.application.provider_policy import (
    sanitize_provider_identifier,
)
from meeting_action_orchestrator.domain.enums import (
    ProviderCallRole,
    ProviderOperation,
    ProviderSettlementOutcome,
    ProviderUsageKind,
)
from meeting_action_orchestrator.domain.hashing import canonical_sha256
from meeting_action_orchestrator.domain.provider_budget import (
    PROVIDER_BUDGET_COUNTER_MAX,
    ProviderBudgetReservationRequest,
    ProviderDispatchContext,
    ProviderUsage,
)

_RESPONSES_COUNT_KEYS = frozenset(
    {
        "conversation",
        "input",
        "instructions",
        "model",
        "parallel_tool_calls",
        "personality",
        "previous_response_id",
        "reasoning",
        "text",
        "tool_choice",
        "tools",
        "truncation",
    }
)
_RESPONSES_CREATE_KEYS = frozenset(
    {
        "background",
        "context_management",
        "conversation",
        "include",
        "input",
        "instructions",
        "max_output_tokens",
        "max_tool_calls",
        "metadata",
        "model",
        "moderation",
        "parallel_tool_calls",
        "previous_response_id",
        "prompt",
        "prompt_cache_key",
        "prompt_cache_retention",
        "reasoning",
        "safety_identifier",
        "service_tier",
        "store",
        "stream",
        "stream_options",
        "temperature",
        "text",
        "tool_choice",
        "tools",
        "top_logprobs",
        "top_p",
        "truncation",
        "user",
    }
)
_UNSUPPORTED_TOKEN_KEYS = frozenset({"context_management", "prompt"})
_COUNT_FORWARD_HEADERS = (
    "Authorization",
    "OpenAI-Organization",
    "OpenAI-Project",
)
_COUNT_ERROR_HEADERS = (
    "retry-after",
    "retry-after-ms",
    "x-request-id",
    "x-should-retry",
)
_RESERVATION_ID_EXTENSION = "meeting_action_orchestrator.provider_reservation_id"
_MAX_ACTIVE_DISPATCHES = 1024


class OpenAIProviderDispatchError(RuntimeError):
    pass


class OpenAICountHTTPStatusError(RuntimeError):
    def __init__(self, response: object, body: dict[str, Any]) -> None:
        self.response = response
        self.status_code = getattr(response, "status_code", None)
        self.body = body
        super().__init__("OpenAI input token count request failed")


class OpenAICountOutputError(RuntimeError):
    def __init__(self) -> None:
        super().__init__("OpenAI input token count output is invalid")


@dataclass(frozen=True, slots=True)
class _CountErrorResponse:
    status_code: int
    headers: httpx.Headers


@dataclass(frozen=True, slots=True)
class ActiveResponsesDispatch:
    context: ProviderDispatchContext
    role: ProviderCallRole


class OpenAIResponsesBudgetHooks:
    def __init__(
        self,
        controller: ProviderBudgetController,
        *,
        responses_path: str = "/v1/responses",
        error_translator: Callable[[Exception, str | None], Exception] | None = None,
    ) -> None:
        self._controller = controller
        self._responses_path = responses_path.rstrip("/")
        self._active: dict[str, ActiveResponsesDispatch] = {}
        self._count_client: Any = None
        self._error_translator = error_translator

    def set_count_client(self, client: Any) -> None:
        self._count_client = client

    def register(
        self,
        client_request_id: str,
        context: ProviderDispatchContext,
        role: ProviderCallRole,
    ) -> None:
        if len(self._active) >= _MAX_ACTIVE_DISPATCHES or client_request_id in self._active:
            raise OpenAIProviderDispatchError("Provider dispatch registry is unavailable")
        self._active[client_request_id] = ActiveResponsesDispatch(context, role)

    def unregister(self, client_request_id: str) -> None:
        self._active.pop(client_request_id, None)

    async def request(self, request: httpx.Request) -> None:
        self._validate_request_target(request)
        client_request_id = self._client_request_id(request)
        active = self._active.get(client_request_id)
        if active is None:
            raise OpenAIProviderDispatchError("Provider dispatch context is required")
        content = await request.aread()
        body = self._request_body(content)
        count_body = self._count_body(body)
        model = self._required_text(body.get("model"), "model")
        max_output_tokens = self._positive_integer(
            body.get("max_output_tokens"),
            "max_output_tokens",
        )
        count_client = self._count_client
        if count_client is None:
            raise OpenAIProviderDispatchError("Input token counting is not configured")
        count_request_id = str(uuid4())
        count_url = self._count_url(request)
        count_headers = self._count_headers(request, count_request_id)
        await self._controller.reserve(
            active.context,
            ProviderBudgetReservationRequest(
                dispatch_key=count_request_id,
                operation=ProviderOperation.RESPONSES_PREFLIGHT,
                role=active.role,
                model=model,
                operation_digest=canonical_sha256(count_body),
            ),
        )
        count_response: httpx.Response | None = None
        translated: Exception | None = None
        try:
            count_response = await count_client.post(
                count_url,
                headers=count_headers,
                json=count_body,
            )
            await count_response.aread()
            if not 200 <= count_response.status_code < 300:
                raise OpenAICountHTTPStatusError(
                    self._error_response(count_response),
                    self._error_body(count_response.content),
                )
        except Exception as error:
            if self._error_translator is None:
                translated = OpenAIProviderDispatchError("Input token count request failed")
            else:
                translated = self._error_translator(error, count_request_id)
        if translated is not None:
            raise translated from None
        if count_response is None:
            raise OpenAIProviderDispatchError("Input token count response is missing")
        try:
            count_payload = self._json_mapping(count_response.content)
            input_tokens = self._nonnegative_integer(count_payload.get("input_tokens"))
        except OpenAIProviderDispatchError:
            input_tokens = None
        if input_tokens is None:
            output_error = OpenAICountOutputError()
            if self._error_translator is None:
                raise output_error
            translated = self._error_translator(output_error, count_request_id)
            raise translated from None
        reservation = await self._controller.reserve(
            active.context,
            ProviderBudgetReservationRequest(
                dispatch_key=client_request_id,
                operation=ProviderOperation.RESPONSES_CREATE,
                role=active.role,
                model=model,
                operation_digest=canonical_sha256(body),
                reserved_input_tokens=input_tokens,
                reserved_output_tokens=max_output_tokens,
            ),
        )
        request.extensions[_RESERVATION_ID_EXTENSION] = reservation.id

    async def response(self, response: httpx.Response) -> None:
        await response.aread()
        reservation_id = response.request.extensions.get(_RESERVATION_ID_EXTENSION)
        if not isinstance(reservation_id, UUID):
            raise OpenAIProviderDispatchError("Provider reservation identity is missing")
        if not 200 <= response.status_code < 300:
            return
        usage = self._response_usage(response.content)
        if usage is None:
            return
        await self._settle(
            reservation_id,
            ProviderSettlementOutcome.SUCCEEDED,
            usage,
        )

    def _validate_request_target(self, request: httpx.Request) -> None:
        path = request.url.path.rstrip("/")
        if request.method != "POST" or path != self._responses_path:
            raise OpenAIProviderDispatchError("Unsupported provider request target")

    @classmethod
    def _request_body(cls, content: bytes) -> dict[str, Any]:
        body = cls._json_mapping(content)
        unknown = body.keys() - _RESPONSES_CREATE_KEYS
        if unknown:
            raise OpenAIProviderDispatchError("Unsupported Responses request field")
        if body.keys() & _UNSUPPORTED_TOKEN_KEYS:
            raise OpenAIProviderDispatchError("Unsupported token-affecting request field")
        if body.get("tools") and body.get("max_tool_calls") is not None:
            raise OpenAIProviderDispatchError("Unsupported tool-call request limit")
        if body.get("background") not in {None, False}:
            raise OpenAIProviderDispatchError("Background Responses requests are unsupported")
        if body.get("store") is not False:
            raise OpenAIProviderDispatchError("Responses storage must be disabled")
        if body.get("stream") not in {None, False}:
            raise OpenAIProviderDispatchError("Streaming Responses requests are unsupported")
        return body

    @staticmethod
    def _client_request_id(request: httpx.Request) -> str:
        values = request.headers.get_list("X-Client-Request-Id")
        if len(values) != 1:
            raise OpenAIProviderDispatchError("Provider client request identity is invalid")
        value = values[0]
        try:
            parsed = UUID(value)
        except ValueError:
            raise OpenAIProviderDispatchError(
                "Provider client request identity is invalid"
            ) from None
        if str(parsed) != value:
            raise OpenAIProviderDispatchError("Provider client request identity is invalid")
        return value

    @staticmethod
    def _count_url(request: httpx.Request) -> httpx.URL:
        return request.url.copy_with(
            path=f"{request.url.path.rstrip('/')}/input_tokens",
            query=None,
            fragment=None,
        )

    @staticmethod
    def _count_headers(request: httpx.Request, client_request_id: str) -> dict[str, str]:
        headers = {"X-Client-Request-Id": client_request_id}
        for name in _COUNT_FORWARD_HEADERS:
            values = request.headers.get_list(name)
            if len(values) > 1:
                raise OpenAIProviderDispatchError("Provider authentication headers are invalid")
            if values:
                headers[name] = values[0]
        if "Authorization" not in headers:
            raise OpenAIProviderDispatchError("Provider authentication is missing")
        return headers

    @classmethod
    def _error_body(cls, content: bytes) -> dict[str, Any]:
        try:
            body = cls._json_mapping(content)
        except OpenAIProviderDispatchError:
            return {}
        sanitized: dict[str, Any] = {}
        for key in ("code", "type"):
            value = sanitize_provider_identifier(body.get(key))
            if value is not None:
                sanitized[key] = value
        nested = body.get("error")
        if isinstance(nested, dict):
            safe_nested = {
                key: value
                for key in ("code", "type")
                if (value := sanitize_provider_identifier(nested.get(key))) is not None
            }
            if safe_nested:
                sanitized["error"] = safe_nested
        return sanitized

    @staticmethod
    def _error_response(response: httpx.Response) -> _CountErrorResponse:
        headers: list[tuple[str, str]] = []
        for name in _COUNT_ERROR_HEADERS:
            headers.extend((name, value) for value in response.headers.get_list(name))
        return _CountErrorResponse(response.status_code, httpx.Headers(headers))

    @staticmethod
    def _count_body(body: dict[str, Any]) -> dict[str, Any]:
        return {key: body[key] for key in sorted(_RESPONSES_COUNT_KEYS & body.keys())}

    @classmethod
    def _response_usage(cls, content: bytes) -> ProviderUsage | None:
        try:
            body = cls._json_mapping(content)
        except OpenAIProviderDispatchError:
            return None
        raw_usage = body.get("usage")
        if not isinstance(raw_usage, dict):
            return None
        input_tokens = cls._nonnegative_integer(raw_usage.get("input_tokens"))
        output_tokens = cls._nonnegative_integer(raw_usage.get("output_tokens"))
        total_tokens = cls._nonnegative_integer(raw_usage.get("total_tokens"))
        if (
            input_tokens is None
            or output_tokens is None
            or total_tokens is None
            or total_tokens != input_tokens + output_tokens
        ):
            return None
        return ProviderUsage(
            kind=ProviderUsageKind.TOKENS,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
        )

    async def _settle(
        self,
        reservation_id: UUID,
        outcome: ProviderSettlementOutcome,
        usage: ProviderUsage,
    ) -> None:
        try:
            await self._controller.settle(
                reservation_id,
                outcome=outcome,
                usage=usage,
            )
        except ProviderBudgetIntegrityError:
            raise
        except Exception:
            return

    @classmethod
    def _json_mapping(cls, content: bytes) -> dict[str, Any]:
        try:
            decoded = json.loads(
                content,
                object_pairs_hook=cls._unique_mapping,
                parse_constant=cls._reject_json_constant,
            )
        except (TypeError, UnicodeDecodeError, json.JSONDecodeError, OpenAIProviderDispatchError):
            raise OpenAIProviderDispatchError("Provider request JSON is invalid") from None
        if not isinstance(decoded, dict):
            raise OpenAIProviderDispatchError("Provider request JSON must be an object")
        return cast(dict[str, Any], decoded)

    @staticmethod
    def _unique_mapping(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        value: dict[str, Any] = {}
        for key, item in pairs:
            if key in value:
                raise OpenAIProviderDispatchError("Provider request JSON has duplicate keys")
            value[key] = item
        return value

    @staticmethod
    def _reject_json_constant(_value: str) -> None:
        raise OpenAIProviderDispatchError("Provider request JSON contains a non-finite number")

    @staticmethod
    def _required_text(value: object, field: str) -> str:
        if not isinstance(value, str) or not value or value.strip() != value:
            raise OpenAIProviderDispatchError(f"Provider request {field} is invalid")
        return value

    @classmethod
    def _positive_integer(cls, value: object, field: str) -> int:
        result = cls._nonnegative_integer(value)
        if result is None or result == 0:
            raise OpenAIProviderDispatchError(f"Provider request {field} is invalid")
        return result

    @staticmethod
    def _nonnegative_integer(value: object) -> int | None:
        if (
            isinstance(value, bool)
            or not isinstance(value, int)
            or value < 0
            or value > PROVIDER_BUDGET_COUNTER_MAX
        ):
            return None
        return value


class OpenAITranscriptionBudget:
    def __init__(self, controller: ProviderBudgetController) -> None:
        self._controller = controller

    async def reserve(
        self,
        context: TranscriptionRunContext,
        *,
        client_request_id: str,
        model: str,
        request_parameters: Mapping[str, object],
    ) -> UUID:
        operation_digest = canonical_sha256(
            {
                "audio": {
                    "duration_ms": context.audio_duration_ms,
                    "sha256": context.audio_sha256,
                    "size_bytes": context.audio_size_bytes,
                },
                "request": request_parameters,
            }
        )
        reservation = await self._controller.reserve(
            context.dispatch,
            ProviderBudgetReservationRequest(
                dispatch_key=client_request_id,
                operation=ProviderOperation.TRANSCRIPTION_CREATE,
                role=ProviderCallRole.TRANSCRIPTION,
                model=model,
                operation_digest=operation_digest,
                reserved_audio_duration_ms=context.audio_duration_ms,
            ),
        )
        return reservation.id

    async def settle(self, reservation_id: UUID, usage: ProviderUsage) -> None:
        try:
            await self._controller.settle(
                reservation_id,
                outcome=ProviderSettlementOutcome.SUCCEEDED,
                usage=usage,
            )
        except ProviderBudgetIntegrityError:
            raise
        except Exception:
            return
