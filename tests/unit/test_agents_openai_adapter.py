import json
from types import SimpleNamespace

import pytest

from meeting_action_orchestrator.agents import TranscriptInput, TranscriptSegmentInput
from meeting_action_orchestrator.infrastructure.openai_agents import (
    OpenAIAgentConfigurationError,
    OpenAIAgentsRunner,
)


def test_openai_adapter_rejects_hidden_sdk_retries() -> None:
    with pytest.raises(OpenAIAgentConfigurationError):
        OpenAIAgentsRunner(api_key="test", max_retries=1)


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
