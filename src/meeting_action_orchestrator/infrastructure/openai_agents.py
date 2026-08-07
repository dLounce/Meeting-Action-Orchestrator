from __future__ import annotations

import json
from importlib import import_module
from typing import Any

from meeting_action_orchestrator.agents.contracts import (
    AgentDefinition,
    AgentResult,
    AgentRunContext,
    AgentUsage,
    OutputT,
    StrictModel,
)


class OpenAIAgentError(RuntimeError):
    def __init__(self, error_type: str | None = None) -> None:
        message = "OpenAI agent request failed"
        if error_type is not None:
            message = f"{message} with {error_type}"
        super().__init__(message)


class OpenAIAgentConfigurationError(OpenAIAgentError):
    def __init__(self) -> None:
        super().__init__("configuration_error")


class OpenAIAgentTransientError(OpenAIAgentError):
    def __init__(self) -> None:
        super().__init__("transient_error")


class OpenAIAgentOutputError(OpenAIAgentError):
    def __init__(self) -> None:
        super().__init__("invalid_output")


class OpenAIAgentRefusalError(OpenAIAgentError):
    def __init__(self) -> None:
        super().__init__("model_refusal")


class OpenAIAgentLimitError(OpenAIAgentError):
    def __init__(self) -> None:
        super().__init__("turn_limit")


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

    async def run(
        self,
        definition: AgentDefinition[OutputT],
        payload: StrictModel,
        context: AgentRunContext,
    ) -> AgentResult[OutputT]:
        try:
            self._load_bindings()
            sdk = self._require_sdk()
            self._configure_client(sdk)
            model_settings = self._build_model_settings(sdk, definition)
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
                provider_request_ids=self._request_ids(result),
            )
        except OpenAIAgentError:
            raise
        except Exception as error:
            raise self._translate_error(error) from error

    def _load_bindings(self) -> None:
        if self._sdk is not None:
            return
        try:
            self._sdk = import_module("agents")
            self._exceptions = import_module("agents.exceptions")
            self._openai = import_module("openai")
            shared = import_module("openai.types.shared")
            self._reasoning_type = shared.Reasoning
        except (AttributeError, ImportError) as error:
            raise OpenAIAgentConfigurationError from error

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
            retry=retry,
        )

    def _translate_error(self, error: Exception) -> OpenAIAgentError:
        if self._is_exception(error, self._exceptions, "ModelRefusalError"):
            translated: OpenAIAgentError = OpenAIAgentRefusalError()
        elif self._is_exception(error, self._exceptions, "MaxTurnsExceeded"):
            translated = OpenAIAgentLimitError()
        elif self._is_exception(error, self._exceptions, "ModelBehaviorError"):
            translated = OpenAIAgentOutputError()
        elif self._matches_transient_error(error):
            translated = OpenAIAgentTransientError()
        elif self._matches_configuration_error(error):
            translated = OpenAIAgentConfigurationError()
        elif self._is_exception(error, self._openai, "BadRequestError") or isinstance(
            error, TypeError
        ):
            translated = OpenAIAgentOutputError()
        else:
            translated = OpenAIAgentError(type(error).__name__)
        return translated

    def _matches_transient_error(self, error: Exception) -> bool:
        names = (
            "APIConnectionError",
            "APITimeoutError",
            "RateLimitError",
            "InternalServerError",
        )
        return any(self._is_exception(error, self._openai, name) for name in names)

    def _matches_configuration_error(self, error: Exception) -> bool:
        names = (
            "AuthenticationError",
            "PermissionDeniedError",
            "NotFoundError",
        )
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
    def _request_ids(result: Any) -> tuple[str, ...]:
        request_ids: list[str] = []
        for response in getattr(result, "raw_responses", ()):
            for attribute in ("response_id", "id", "_request_id"):
                value = getattr(response, attribute, None)
                if isinstance(value, str) and value and value not in request_ids:
                    request_ids.append(value)
                    break
        return tuple(request_ids)
