from __future__ import annotations

import json
from importlib import import_module
from typing import Any
from uuid import uuid4

from meeting_action_orchestrator.agents.contracts import (
    AgentDefinition,
    AgentResult,
    AgentRunContext,
    AgentUsage,
    OutputT,
    StrictModel,
)
from meeting_action_orchestrator.application.errors import (
    ProviderConfigurationError,
    ProviderError,
    ProviderInputError,
    ProviderOutputError,
    ProviderPermanentError,
    ProviderPermanentOutputError,
    ProviderRateLimitError,
    ProviderTimeoutError,
    ProviderTransientError,
)
from meeting_action_orchestrator.application.provider_policy import (
    ProviderErrorMetadata,
    provider_error_metadata,
    provider_error_requires_action,
    sanitize_provider_identifier,
)


class OpenAIAgentError(ProviderError):
    def __init__(
        self,
        error_type: str | None = None,
        *,
        metadata: ProviderErrorMetadata | None = None,
    ) -> None:
        message = "OpenAI agent request failed"
        if error_type is not None:
            message = f"{message} with {error_type}"
        super().__init__(message, metadata=metadata)


class OpenAIAgentConfigurationError(OpenAIAgentError, ProviderConfigurationError):
    def __init__(self, *, metadata: ProviderErrorMetadata | None = None) -> None:
        super().__init__("configuration_error", metadata=metadata)


class OpenAIAgentInputError(OpenAIAgentError, ProviderInputError):
    def __init__(self, *, metadata: ProviderErrorMetadata | None = None) -> None:
        super().__init__("invalid_input", metadata=metadata)


class OpenAIAgentTransientError(OpenAIAgentError, ProviderTransientError):
    def __init__(
        self,
        error_type: str = "transient_error",
        *,
        metadata: ProviderErrorMetadata | None = None,
    ) -> None:
        super().__init__(error_type, metadata=metadata)


class OpenAIAgentTimeoutError(OpenAIAgentTransientError, ProviderTimeoutError):
    def __init__(self, *, metadata: ProviderErrorMetadata | None = None) -> None:
        super().__init__("timeout", metadata=metadata)


class OpenAIAgentRateLimitError(OpenAIAgentTransientError, ProviderRateLimitError):
    def __init__(self, *, metadata: ProviderErrorMetadata | None = None) -> None:
        super().__init__("rate_limited", metadata=metadata)


class OpenAIAgentOutputError(OpenAIAgentError, ProviderOutputError):
    def __init__(self, *, metadata: ProviderErrorMetadata | None = None) -> None:
        super().__init__("invalid_output", metadata=metadata)


class OpenAIAgentPermanentError(OpenAIAgentError, ProviderPermanentError):
    def __init__(
        self,
        error_type: str = "permanent_error",
        *,
        metadata: ProviderErrorMetadata | None = None,
    ) -> None:
        super().__init__(error_type, metadata=metadata)


class OpenAIAgentPermanentOutputError(OpenAIAgentPermanentError, ProviderPermanentOutputError):
    def __init__(
        self,
        error_type: str = "permanent_output_error",
        *,
        metadata: ProviderErrorMetadata | None = None,
    ) -> None:
        super().__init__(error_type, metadata=metadata)


class OpenAIAgentRefusalError(OpenAIAgentPermanentOutputError):
    def __init__(self, *, metadata: ProviderErrorMetadata | None = None) -> None:
        super().__init__("model_refusal", metadata=metadata)


class OpenAIAgentLimitError(OpenAIAgentPermanentOutputError):
    def __init__(self, *, metadata: ProviderErrorMetadata | None = None) -> None:
        super().__init__("turn_limit", metadata=metadata)


class OpenAIAgentsRunner:
    def __init__(
        self,
        api_key: str,
        timeout_seconds: float = 120.0,
        max_retries: int = 0,
        tracing_enabled: bool = False,
    ) -> None:
        if not api_key:
            raise OpenAIAgentConfigurationError
        if max_retries != 0:
            raise OpenAIAgentConfigurationError
        self._api_key = api_key
        self._timeout_seconds = timeout_seconds
        self._max_retries = max_retries
        self._tracing_enabled = tracing_enabled
        self._sdk: Any = None
        self._openai: Any = None
        self._exceptions: Any = None
        self._reasoning_type: type[Any] | None = None
        self._client: Any = None

    async def close(self) -> None:
        client = self._client
        self._client = None
        if client is not None:
            await client.close()

    async def run(
        self,
        definition: AgentDefinition[OutputT],
        payload: StrictModel,
        context: AgentRunContext,
    ) -> AgentResult[OutputT]:
        client_request_id = str(uuid4())
        try:
            self._load_bindings()
            sdk = self._require_sdk()
            self._configure_client(sdk)
            model_settings = self._build_model_settings(sdk, definition, client_request_id)
            agent = sdk.Agent(
                name=definition.name,
                instructions=definition.instructions,
                model=definition.model,
                model_settings=model_settings,
                output_type=definition.output_type,
            )
            result = await sdk.Runner.run(
                agent,
                input=self._canonical_json(payload),
                max_turns=1,
                run_config=sdk.RunConfig(
                    workflow_name="Meeting Action Orchestrator",
                    group_id=context.job_id,
                    tracing_disabled=not self._tracing_enabled,
                    trace_include_sensitive_data=False,
                    trace_metadata={"job_id": context.job_id, "stage": context.stage},
                ),
            )
            output = result.final_output_as(
                definition.output_type,
                raise_if_incorrect_type=True,
            )
            usage = self._map_usage(result.context_wrapper.usage)
            return AgentResult(
                output=output,
                usage=usage,
                model=definition.model,
                workflow_request_ids=self._request_ids(result, client_request_id),
            )
        except OpenAIAgentError:
            raise
        except Exception as error:
            translated = self._translate_error(error, client_request_id)
        raise translated

    def _load_bindings(self) -> None:
        if self._sdk is not None:
            return
        sdk = None
        exceptions = None
        openai = None
        reasoning_type = None
        try:
            sdk = import_module("agents")
            exceptions = import_module("agents.exceptions")
            openai = import_module("openai")
            shared = import_module("openai.types.shared")
            reasoning_type = shared.Reasoning
        except (AttributeError, ImportError):
            pass
        if sdk is None or exceptions is None or openai is None or reasoning_type is None:
            self._sdk = None
            self._exceptions = None
            self._openai = None
            self._reasoning_type = None
            raise OpenAIAgentConfigurationError
        self._sdk = sdk
        self._exceptions = exceptions
        self._openai = openai
        self._reasoning_type = reasoning_type

    def _require_sdk(self) -> Any:
        if self._sdk is None:
            raise OpenAIAgentConfigurationError
        return self._sdk

    def _configure_client(self, sdk: Any) -> None:
        if self._client is not None:
            return
        if self._openai is None:
            raise OpenAIAgentConfigurationError
        self._client = self._openai.AsyncOpenAI(
            api_key=self._api_key,
            timeout=self._timeout_seconds,
            max_retries=0,
        )
        sdk.set_default_openai_client(
            self._client,
            use_for_tracing=self._tracing_enabled,
        )

    def _build_model_settings(
        self,
        sdk: Any,
        definition: AgentDefinition[OutputT],
        client_request_id: str,
    ) -> Any:
        if self._reasoning_type is None:
            raise OpenAIAgentConfigurationError
        policies = sdk.retry_policies
        retry = sdk.ModelRetrySettings(
            max_retries=self._max_retries,
            backoff={
                "initial_delay": 0.5,
                "max_delay": 5.0,
                "multiplier": 2.0,
                "jitter": True,
            },
            policy=policies.any(
                policies.provider_suggested(),
                policies.retry_after(),
                policies.network_error(),
                policies.http_status([408, 409, 429, 500, 502, 503, 504]),
            ),
        )
        return sdk.ModelSettings(
            reasoning=self._reasoning_type(effort=definition.reasoning_effort),
            verbosity="low",
            max_tokens=definition.max_output_tokens,
            parallel_tool_calls=False,
            store=False,
            extra_headers={"X-Client-Request-Id": client_request_id},
            retry=retry,
        )

    def _translate_error(
        self,
        error: Exception,
        client_request_id: str | None = None,
    ) -> OpenAIAgentError:
        metadata = provider_error_metadata(error, client_request_id)
        if provider_error_requires_action(error):
            return OpenAIAgentConfigurationError(metadata=metadata)
        if metadata.retry_control_rejected or metadata.provider_should_retry is False:
            return self._non_retryable_error(error, metadata)
        if metadata.provider_should_retry is True:
            return self._transient_error(error, metadata)
        if self._is_exception(error, self._exceptions, "ModelRefusalError"):
            translated: OpenAIAgentError = OpenAIAgentRefusalError(metadata=metadata)
        elif self._is_exception(error, self._exceptions, "MaxTurnsExceeded"):
            translated = OpenAIAgentLimitError(metadata=metadata)
        elif self._is_exception(error, self._exceptions, "UserError") or isinstance(
            error, (AttributeError, TypeError)
        ):
            translated = OpenAIAgentConfigurationError(metadata=metadata)
        elif self._is_exception(error, self._exceptions, "ModelBehaviorError"):
            translated = OpenAIAgentOutputError(metadata=metadata)
        elif self._matches_transient_error(error, metadata):
            translated = self._transient_error(error, metadata)
        elif self._matches_configuration_error(error, metadata):
            translated = OpenAIAgentConfigurationError(metadata=metadata)
        elif self._matches_input_error(error, metadata):
            translated = OpenAIAgentInputError(metadata=metadata)
        else:
            translated = OpenAIAgentPermanentError(metadata=metadata)
        return translated

    def _transient_error(
        self,
        error: Exception,
        metadata: ProviderErrorMetadata,
    ) -> OpenAIAgentError:
        if metadata.http_status == 408 or self._is_exception(
            error,
            self._openai,
            "APITimeoutError",
        ):
            return OpenAIAgentTimeoutError(metadata=metadata)
        if metadata.http_status == 429 or self._is_exception(
            error,
            self._openai,
            "RateLimitError",
        ):
            return OpenAIAgentRateLimitError(metadata=metadata)
        return OpenAIAgentTransientError(metadata=metadata)

    def _non_retryable_error(
        self,
        error: Exception,
        metadata: ProviderErrorMetadata,
    ) -> OpenAIAgentError:
        if self._is_exception(error, self._exceptions, "ModelRefusalError"):
            return OpenAIAgentRefusalError(metadata=metadata)
        if self._is_exception(error, self._exceptions, "MaxTurnsExceeded"):
            return OpenAIAgentLimitError(metadata=metadata)
        if self._is_exception(error, self._exceptions, "ModelBehaviorError"):
            return OpenAIAgentPermanentOutputError(metadata=metadata)
        if self._matches_configuration_error(error, metadata):
            return OpenAIAgentConfigurationError(metadata=metadata)
        if self._matches_input_error(error, metadata):
            return OpenAIAgentInputError(metadata=metadata)
        return OpenAIAgentPermanentError(metadata=metadata)

    def _matches_transient_error(
        self,
        error: Exception,
        metadata: ProviderErrorMetadata,
    ) -> bool:
        status = metadata.http_status
        if status in {408, 409, 429} or (status is not None and status >= 500):
            return True
        names = (
            "APIConnectionError",
            "APITimeoutError",
            "RateLimitError",
            "InternalServerError",
        )
        return any(self._is_exception(error, self._openai, name) for name in names)

    def _matches_configuration_error(
        self,
        error: Exception,
        metadata: ProviderErrorMetadata,
    ) -> bool:
        if metadata.http_status in {401, 403, 404}:
            return True
        names = (
            "AuthenticationError",
            "PermissionDeniedError",
            "NotFoundError",
        )
        return any(self._is_exception(error, self._openai, name) for name in names)

    def _matches_input_error(
        self,
        error: Exception,
        metadata: ProviderErrorMetadata,
    ) -> bool:
        status = metadata.http_status
        if status is not None and 400 <= status < 500 and status not in {408, 409, 429}:
            return True
        names = ("BadRequestError", "UnprocessableEntityError")
        return any(self._is_exception(error, self._openai, name) for name in names)

    @staticmethod
    def _is_exception(error: Exception, module: Any, name: str) -> bool:
        if module is None:
            return False
        exception_type = getattr(module, name, None)
        return isinstance(exception_type, type) and isinstance(error, exception_type)

    @staticmethod
    def _canonical_json(payload: StrictModel) -> str:
        return json.dumps(
            payload.model_dump(mode="json"),
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )

    @classmethod
    def _map_usage(cls, usage: Any) -> AgentUsage:
        input_details = getattr(usage, "input_tokens_details", None)
        output_details = getattr(usage, "output_tokens_details", None)
        return AgentUsage(
            requests=cls._integer_attribute(usage, "requests"),
            input_tokens=cls._integer_attribute(usage, "input_tokens"),
            output_tokens=cls._integer_attribute(usage, "output_tokens"),
            total_tokens=cls._integer_attribute(usage, "total_tokens"),
            cached_input_tokens=cls._integer_attribute(input_details, "cached_tokens"),
            reasoning_tokens=cls._integer_attribute(output_details, "reasoning_tokens"),
        )

    @staticmethod
    def _integer_attribute(value: Any, name: str) -> int:
        attribute = getattr(value, name, 0)
        return attribute if isinstance(attribute, int) else 0

    @staticmethod
    def _request_ids(
        result: Any,
        client_request_id: str | None = None,
    ) -> tuple[str, ...]:
        request_ids: list[str] = []
        client_id = sanitize_provider_identifier(client_request_id)
        if client_id is not None:
            request_ids.append(client_id)
        for response in getattr(result, "raw_responses", ()):
            for attribute in ("request_id", "_request_id"):
                value = sanitize_provider_identifier(getattr(response, attribute, None))
                if value is not None and value not in request_ids:
                    request_ids.append(value)
                    break
        return tuple(request_ids)
