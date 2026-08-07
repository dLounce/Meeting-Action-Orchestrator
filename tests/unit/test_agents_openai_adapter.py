import json
from types import SimpleNamespace

import pytest

from meeting_action_orchestrator.agents import TranscriptInput, TranscriptSegmentInput
from meeting_action_orchestrator.application.errors import (
    ProviderConfigurationError,
    ProviderOutputError,
    ProviderTransientError,
)
from meeting_action_orchestrator.infrastructure.openai_agents import (
    OpenAIAgentConfigurationError,
    OpenAIAgentOutputError,
    OpenAIAgentsRunner,
    OpenAIAgentTransientError,
)


class FakeClosableClient:
    def __init__(self) -> None:
        self.close_calls = 0

    async def close(self) -> None:
        self.close_calls += 1


def test_openai_adapter_rejects_hidden_sdk_retries() -> None:
    with pytest.raises(OpenAIAgentConfigurationError):
        OpenAIAgentsRunner(api_key="test", max_retries=1)


def test_openai_adapter_errors_implement_provider_failure_contracts() -> None:
    assert isinstance(OpenAIAgentConfigurationError(), ProviderConfigurationError)
    assert isinstance(OpenAIAgentTransientError(), ProviderTransientError)
    assert isinstance(OpenAIAgentOutputError(), ProviderOutputError)


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

    runner = OpenAIAgentsRunner(api_key="test")
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
    payload = TranscriptInput(
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


def test_openai_adapter_collects_unique_provider_request_ids() -> None:
    first = SimpleNamespace(response_id="resp_1")
    duplicate = SimpleNamespace(id="resp_1")
    second = SimpleNamespace(_request_id="req_2")
    result = SimpleNamespace(raw_responses=[first, duplicate, second])

    request_ids = OpenAIAgentsRunner._request_ids(result)

    assert request_ids == ("resp_1", "req_2")
