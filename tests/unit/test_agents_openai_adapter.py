import json
from types import SimpleNamespace
from typing import ClassVar
from uuid import UUID

import httpx
import pytest

from meeting_action_orchestrator.agents import (
    AgentBudget,
    AgentDefinition,
    AgentRunContext,
    TranscriptInput,
    TranscriptSegmentInput,
)
from meeting_action_orchestrator.application.errors import (
    ProviderConfigurationError,
    ProviderInputError,
    ProviderOutputError,
    ProviderPermanentError,
    ProviderPermanentOutputError,
    ProviderRateLimitError,
    ProviderTimeoutError,
    ProviderTransientError,
)
from meeting_action_orchestrator.infrastructure.openai_agents import (
    OpenAIAgentConfigurationError,
    OpenAIAgentInputError,
    OpenAIAgentLimitError,
    OpenAIAgentOutputError,
    OpenAIAgentPermanentError,
    OpenAIAgentPermanentOutputError,
    OpenAIAgentRateLimitError,
    OpenAIAgentRefusalError,
    OpenAIAgentsRunner,
    OpenAIAgentTimeoutError,
    OpenAIAgentTransientError,
)
from tests.provider_budget_support import FakeBudgetController, dispatch_context


class FakeClosableClient:
    def __init__(self) -> None:
        self.close_calls = 0

    async def close(self) -> None:
        self.close_calls += 1


class FakeOpenAIError(Exception):
    def __init__(
        self,
        *,
        status_code: int | None = None,
        code: str | None = None,
        response: object = None,
        body: object = None,
    ) -> None:
        self.status_code = status_code
        self.code = code
        self.response = response
        self.body = body
        super().__init__("private provider detail")


class APIConnectionError(FakeOpenAIError):
    pass


class APITimeoutError(FakeOpenAIError):
    pass


class RateLimitError(FakeOpenAIError):
    pass


class InternalServerError(FakeOpenAIError):
    pass


class AuthenticationError(FakeOpenAIError):
    pass


class PermissionDeniedError(FakeOpenAIError):
    pass


class NotFoundError(FakeOpenAIError):
    pass


class BadRequestError(FakeOpenAIError):
    pass


class UnprocessableEntityError(FakeOpenAIError):
    pass


class UserError(Exception):
    pass


class ModelRefusalError(Exception):
    pass


class MaxTurnsExceededError(Exception):
    pass


class ModelBehaviorError(Exception):
    pass


class FailingRunner:
    error: Exception

    @classmethod
    async def run(cls, *_arguments: object, **_keywords: object) -> object:
        raise cls.error


class SuccessfulRunner:
    agents: ClassVar[list[SimpleNamespace]] = []

    @classmethod
    async def run(cls, agent: object, **_keywords: object) -> object:
        assert isinstance(agent, SimpleNamespace)
        cls.agents.append(agent)
        return SimpleNamespace(
            context_wrapper=SimpleNamespace(usage=SimpleNamespace()),
            raw_responses=(SimpleNamespace(response_id="resp_not_transport"),),
            final_output_as=lambda *_arguments, **_keywords: transcript_input(),
        )


class FakeReasoning:
    def __init__(self, effort: str) -> None:
        self.effort = effort


def configured_runner() -> OpenAIAgentsRunner:
    runner = OpenAIAgentsRunner(
        api_key="test",
        budget_controller=FakeBudgetController(),
    )
    runner._openai = SimpleNamespace(
        APIConnectionError=APIConnectionError,
        APITimeoutError=APITimeoutError,
        RateLimitError=RateLimitError,
        InternalServerError=InternalServerError,
        AuthenticationError=AuthenticationError,
        PermissionDeniedError=PermissionDeniedError,
        NotFoundError=NotFoundError,
        BadRequestError=BadRequestError,
        UnprocessableEntityError=UnprocessableEntityError,
    )
    runner._exceptions = SimpleNamespace(
        UserError=UserError,
        ModelRefusalError=ModelRefusalError,
        MaxTurnsExceeded=MaxTurnsExceededError,
        ModelBehaviorError=ModelBehaviorError,
    )
    return runner


def transcript_input() -> TranscriptInput:
    return TranscriptInput(
        language="en",
        text="Ship it",
        sha256="a" * 64,
        segments=[
            TranscriptSegmentInput(
                id="segment_1",
                start_ms=0,
                end_ms=1000,
                speaker="A",
                text="Ship it",
            )
        ],
    )


def test_openai_adapter_rejects_hidden_sdk_retries() -> None:
    with pytest.raises(OpenAIAgentConfigurationError):
        OpenAIAgentsRunner(
            api_key="test",
            budget_controller=FakeBudgetController(),
            max_retries=1,
        )


def test_openai_adapter_rejects_unverified_tracing_transport() -> None:
    with pytest.raises(OpenAIAgentConfigurationError):
        OpenAIAgentsRunner(
            api_key="test",
            budget_controller=FakeBudgetController(),
            tracing_enabled=True,
        )


@pytest.mark.parametrize("model", ["", " ", "g" * 201])
@pytest.mark.asyncio
async def test_openai_adapter_rejects_invalid_model_before_dispatch(model: str) -> None:
    controller = FakeBudgetController()
    runner = OpenAIAgentsRunner(api_key="test", budget_controller=controller)
    definition = AgentDefinition(
        name="test",
        instructions="Return the supplied transcript",
        model=model,
        output_type=TranscriptInput,
        reasoning_effort="low",
        max_output_tokens=10,
    )
    context = AgentRunContext(
        job_id="job_1",
        stage="extract",
        budget=AgentBudget(max_requests=1, max_output_tokens=10),
        dispatch=dispatch_context(),
    )

    with pytest.raises(OpenAIAgentConfigurationError) as captured:
        await runner.run(definition, transcript_input(), context)

    assert captured.value.__cause__ is None
    assert captured.value.__context__ is None
    assert controller.reservations == []


def test_openai_adapter_errors_implement_provider_failure_contracts() -> None:
    assert isinstance(OpenAIAgentConfigurationError(), ProviderConfigurationError)
    assert isinstance(OpenAIAgentInputError(), ProviderInputError)
    assert isinstance(OpenAIAgentTransientError(), ProviderTransientError)
    assert isinstance(OpenAIAgentTimeoutError(), ProviderTimeoutError)
    assert isinstance(OpenAIAgentRateLimitError(), ProviderRateLimitError)
    assert isinstance(OpenAIAgentOutputError(), ProviderOutputError)
    assert isinstance(OpenAIAgentPermanentError(), ProviderPermanentError)
    assert isinstance(OpenAIAgentPermanentOutputError(), ProviderPermanentOutputError)
    assert isinstance(OpenAIAgentRefusalError(), ProviderPermanentError)
    assert isinstance(OpenAIAgentLimitError(), ProviderPermanentError)


@pytest.mark.asyncio
async def test_openai_adapter_closes_and_recreates_its_client() -> None:
    first = FakeClosableClient()
    second = FakeClosableClient()
    clients = iter((first, second))
    configured: list[FakeClosableClient] = []

    def create_client(**_arguments: object) -> FakeClosableClient:
        return next(clients)

    def configure_client(
        client: FakeClosableClient,
        *,
        use_for_tracing: bool,
    ) -> None:
        assert use_for_tracing is False
        configured.append(client)

    runner = OpenAIAgentsRunner(
        api_key="test",
        budget_controller=FakeBudgetController(),
    )
    runner._openai = SimpleNamespace(AsyncOpenAI=create_client)
    sdk = SimpleNamespace(set_default_openai_client=configure_client)

    runner._configure_client(sdk)
    await runner.close()
    await runner.close()
    runner._configure_client(sdk)

    assert first.close_calls == 1
    assert second.close_calls == 0
    assert configured == [first, second]


def test_openai_adapter_serializes_payload_canonically() -> None:
    payload = transcript_input()

    serialized = OpenAIAgentsRunner._canonical_json(payload)

    assert serialized == json.dumps(
        payload.model_dump(mode="json"),
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )


def test_openai_adapter_maps_usage_details() -> None:
    usage = SimpleNamespace(
        requests=2,
        input_tokens=100,
        output_tokens=50,
        total_tokens=150,
        input_tokens_details=SimpleNamespace(cached_tokens=25),
        output_tokens_details=SimpleNamespace(reasoning_tokens=10),
    )

    mapped = OpenAIAgentsRunner._map_usage(usage)

    assert mapped.requests == usage.requests
    assert mapped.input_tokens == usage.input_tokens
    assert mapped.output_tokens == usage.output_tokens
    assert mapped.total_tokens == usage.total_tokens
    assert mapped.cached_input_tokens == usage.input_tokens_details.cached_tokens
    assert mapped.reasoning_tokens == usage.output_tokens_details.reasoning_tokens


def test_openai_adapter_collects_unique_workflow_request_ids() -> None:
    response_object = SimpleNamespace(response_id="resp_1", id="resp_1")
    first = SimpleNamespace(request_id="req_1")
    duplicate = SimpleNamespace(_request_id="req_1")
    second = SimpleNamespace(_request_id="req_2")
    unsafe = SimpleNamespace(request_id="private request content")
    result = SimpleNamespace(raw_responses=[response_object, first, duplicate, second, unsafe])

    client_request_id = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    request_ids = OpenAIAgentsRunner._request_ids(result, client_request_id)
    fallback = OpenAIAgentsRunner._request_ids(
        SimpleNamespace(raw_responses=[response_object]),
        client_request_id,
    )

    assert request_ids == (client_request_id, "req_1", "req_2")
    assert fallback == (client_request_id,)


@pytest.mark.asyncio
async def test_openai_adapter_sends_a_unique_client_request_id_per_run() -> None:
    def namespace(**values: object) -> SimpleNamespace:
        return SimpleNamespace(**values)

    def http_status(statuses: list[int]) -> tuple[int, ...]:
        return tuple(statuses)

    policies = SimpleNamespace(
        any=lambda *values: values,
        provider_suggested=lambda: "provider",
        retry_after=lambda: "retry-after",
        network_error=lambda: "network",
        http_status=http_status,
    )
    sdk = SimpleNamespace(
        Agent=namespace,
        Runner=SuccessfulRunner,
        RunConfig=namespace,
        ModelRetrySettings=namespace,
        ModelSettings=namespace,
        retry_policies=policies,
    )
    runner = OpenAIAgentsRunner(
        api_key="test",
        budget_controller=FakeBudgetController(),
    )
    runner._sdk = sdk
    runner._client = object()
    runner._reasoning_type = FakeReasoning
    definition = AgentDefinition(
        name="test",
        instructions="Return the supplied transcript",
        model=" test-model ",
        output_type=TranscriptInput,
        reasoning_effort="low",
        max_output_tokens=10,
    )
    context = AgentRunContext(
        job_id="job_1",
        stage="extract",
        budget=AgentBudget(max_requests=1, max_output_tokens=10),
        dispatch=dispatch_context(),
    )
    SuccessfulRunner.agents = []

    first = await runner.run(definition, transcript_input(), context)
    second = await runner.run(definition, transcript_input(), context)

    client_request_ids = tuple(
        agent.model_settings.extra_headers["X-Client-Request-Id"]
        for agent in SuccessfulRunner.agents
    )
    assert len(set(client_request_ids)) == 2
    assert all(str(UUID(value)) == value for value in client_request_ids)
    assert all(agent.model == "test-model" for agent in SuccessfulRunner.agents)
    assert first.model == second.model == "test-model"
    assert first.workflow_request_ids == (client_request_ids[0],)
    assert second.workflow_request_ids == (client_request_ids[1],)


@pytest.mark.parametrize(
    ("error", "expected_type"),
    [
        (APIConnectionError(), OpenAIAgentTransientError),
        (APITimeoutError(), OpenAIAgentTimeoutError),
        (RateLimitError(status_code=429), OpenAIAgentRateLimitError),
        (InternalServerError(status_code=503), OpenAIAgentTransientError),
        (AuthenticationError(status_code=401), OpenAIAgentConfigurationError),
        (PermissionDeniedError(status_code=403), OpenAIAgentConfigurationError),
        (NotFoundError(status_code=404), OpenAIAgentConfigurationError),
        (BadRequestError(status_code=400), OpenAIAgentInputError),
        (UnprocessableEntityError(status_code=422), OpenAIAgentInputError),
        (UserError(), OpenAIAgentConfigurationError),
        (TypeError("adapter mismatch"), OpenAIAgentConfigurationError),
        (ModelRefusalError(), OpenAIAgentRefusalError),
        (MaxTurnsExceededError(), OpenAIAgentLimitError),
        (ModelBehaviorError(), OpenAIAgentOutputError),
        (FakeOpenAIError(status_code=503), OpenAIAgentTransientError),
        (FakeOpenAIError(status_code=408), OpenAIAgentTimeoutError),
        (FakeOpenAIError(status_code=409), OpenAIAgentTransientError),
        (RuntimeError("unknown"), OpenAIAgentPermanentError),
    ],
)
def test_openai_adapter_classifies_provider_failures(
    error: Exception,
    expected_type: type[Exception],
) -> None:
    translated = configured_runner()._translate_error(error)

    assert isinstance(translated, expected_type)
    assert "private provider detail" not in str(translated)


def test_openai_adapter_preserves_sanitized_retry_metadata() -> None:
    error = RateLimitError(
        status_code=429,
        code="rate_limit_exceeded",
        response=SimpleNamespace(
            status_code=429,
            headers={"retry-after": "17", "x-request-id": "req_rate_1"},
        ),
        body={"message": "private provider detail"},
    )

    translated = configured_runner()._translate_error(
        error,
        "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
    )

    assert isinstance(translated, OpenAIAgentTransientError)
    assert translated.http_status == 429
    assert translated.provider_code == "rate_limit_exceeded"
    assert translated.request_id == "req_rate_1"
    assert translated.retry_after_seconds == 17
    assert translated.__cause__ is None
    assert translated.__context__ is None


def test_openai_adapter_timeout_uses_client_request_id_fallback() -> None:
    client_request_id = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"

    translated = configured_runner()._translate_error(
        APITimeoutError(),
        client_request_id,
    )

    assert isinstance(translated, OpenAIAgentTimeoutError)
    assert translated.request_id == client_request_id


def test_openai_adapter_does_not_retry_quota_or_oversized_retry_after() -> None:
    quota = RateLimitError(
        status_code=429,
        code="insufficient_quota",
        body={"message": "billing detail"},
    )
    oversized = RateLimitError(
        status_code=429,
        response=SimpleNamespace(status_code=429, headers={"retry-after": "601"}),
    )

    quota_error = configured_runner()._translate_error(quota)
    oversized_error = configured_runner()._translate_error(oversized)

    assert isinstance(quota_error, OpenAIAgentConfigurationError)
    assert isinstance(oversized_error, OpenAIAgentPermanentError)
    assert oversized_error.retry_after_seconds is None


def test_openai_adapter_honors_provider_retry_directive_precedence() -> None:
    retry_input = BadRequestError(
        status_code=400,
        response=SimpleNamespace(
            status_code=400,
            headers=httpx.Headers({"X-Should-Retry": "true"}),
        ),
    )
    reject_server = InternalServerError(
        status_code=503,
        response=SimpleNamespace(
            status_code=503,
            headers=httpx.Headers({"x-should-retry": "false"}),
        ),
    )
    quota = RateLimitError(
        status_code=429,
        code="insufficient_quota",
        response=SimpleNamespace(
            status_code=429,
            headers=httpx.Headers({"x-should-retry": "true"}),
        ),
    )

    assert isinstance(configured_runner()._translate_error(retry_input), OpenAIAgentTransientError)
    assert isinstance(
        configured_runner()._translate_error(reject_server),
        OpenAIAgentPermanentError,
    )
    assert isinstance(configured_runner()._translate_error(quota), OpenAIAgentConfigurationError)


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (ModelRefusalError(), OpenAIAgentRefusalError),
        (MaxTurnsExceededError(), OpenAIAgentLimitError),
        (ModelBehaviorError(), OpenAIAgentPermanentOutputError),
    ],
)
def test_no_retry_directive_preserves_permanent_output_category(
    error: Exception,
    expected: type[OpenAIAgentPermanentError],
) -> None:
    error.__dict__["response"] = SimpleNamespace(
        status_code=500,
        headers=httpx.Headers({"x-should-retry": "false"}),
    )

    translated = configured_runner()._translate_error(error)

    assert type(translated) is expected
    assert isinstance(translated, ProviderPermanentOutputError)


def test_openai_adapter_fails_closed_on_ambiguous_retry_headers() -> None:
    error = RateLimitError(
        status_code=429,
        response=SimpleNamespace(
            status_code=429,
            headers=httpx.Headers([("retry-after", "5"), ("Retry-After", "6")]),
        ),
    )

    translated = configured_runner()._translate_error(error)

    assert isinstance(translated, OpenAIAgentPermanentError)
    assert translated.retry_after_seconds is None


def test_openai_adapter_detaches_sdk_import_error_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    marker = "private sdk import detail"

    def fail_import(_name: str) -> object:
        raise ImportError(marker)

    monkeypatch.setattr(
        "meeting_action_orchestrator.infrastructure.openai_agents.import_module",
        fail_import,
    )
    runner = OpenAIAgentsRunner(
        api_key="test",
        budget_controller=FakeBudgetController(),
    )

    with pytest.raises(OpenAIAgentConfigurationError) as captured:
        runner._load_bindings()

    assert captured.value.__cause__ is None
    assert captured.value.__context__ is None
    assert marker not in str(captured.value)


@pytest.mark.asyncio
async def test_openai_adapter_raises_sanitized_error_without_raw_exception_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider_error = RateLimitError(
        status_code=429,
        code="rate_limit_exceeded",
        response=SimpleNamespace(
            status_code=429,
            headers={"retry-after": "11", "x-request-id": "req_agent_1"},
        ),
        body={"message": "private provider detail"},
    )
    runner = configured_runner()
    FailingRunner.error = provider_error
    sdk = SimpleNamespace(
        Agent=lambda **_arguments: object(),
        Runner=FailingRunner,
        RunConfig=lambda **_arguments: object(),
    )
    monkeypatch.setattr(runner, "_load_bindings", lambda: None)
    monkeypatch.setattr(runner, "_require_sdk", lambda: sdk)
    monkeypatch.setattr(runner, "_configure_client", lambda _sdk: None)
    client_request_ids: list[str] = []

    def model_settings(
        _sdk: object,
        _definition: AgentDefinition[TranscriptInput],
        client_request_id: str,
    ) -> object:
        client_request_ids.append(client_request_id)
        return object()

    monkeypatch.setattr(runner, "_build_model_settings", model_settings)
    definition = AgentDefinition(
        name="test",
        instructions="Return the supplied transcript",
        model="test-model",
        output_type=TranscriptInput,
        reasoning_effort="low",
        max_output_tokens=10,
    )
    context = AgentRunContext(
        job_id="job_1",
        stage="extract",
        budget=AgentBudget(max_requests=1, max_output_tokens=10),
        dispatch=dispatch_context(),
    )

    with pytest.raises(OpenAIAgentTransientError) as captured:
        await runner.run(definition, transcript_input(), context)

    error = captured.value
    assert error.request_id == "req_agent_1"
    assert error.retry_after_seconds == 11
    assert error.__cause__ is None
    assert error.__context__ is None
    assert "private provider detail" not in str(error)
    assert len(client_request_ids) == 1
    assert str(UUID(client_request_ids[0])) == client_request_ids[0]
