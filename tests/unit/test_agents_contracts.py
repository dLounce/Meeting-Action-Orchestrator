import json
from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from meeting_action_orchestrator.agents import (
    AgentBudget,
    AgentBudgetExceededError,
    AgentUsage,
    ExtractionRequest,
    MeetingExtraction,
    RecapDraft,
    StrictModel,
    TranscriptInput,
    TranscriptSegmentInput,
    VerificationReport,
)


def test_agent_contracts_reject_unknown_fields() -> None:
    with pytest.raises(ValidationError):
        TranscriptSegmentInput.model_validate(
            {
                "id": "segment_1",
                "start_ms": 0,
                "end_ms": 1000,
                "speaker": "A",
                "text": "Ship it",
                "unknown": True,
            }
        )


def test_transcript_input_rejects_oversized_content() -> None:
    with pytest.raises(ValidationError, match="at most 250000"):
        TranscriptInput(
            language="en",
            text="x" * 250_001,
            sha256="a" * 64,
            segments=[
                TranscriptSegmentInput(
                    id="segment_1",
                    start_ms=0,
                    end_ms=1000,
                    speaker=None,
                    text="x",
                )
            ],
        )


def test_structured_outputs_enforce_total_size_without_schema_constraints() -> None:
    with pytest.raises(ValidationError, match="exceeds the allowed size"):
        RecapDraft(title="Recap", overview="x" * 50_001, highlights=[])


def test_extraction_request_serializes_timezone_aware_timestamp() -> None:
    request = ExtractionRequest(
        meeting_id="meeting_1",
        meeting_started_at=datetime(2026, 6, 7, 10, 0, tzinfo=timezone.utc),
        timezone="Asia/Calcutta",
        transcript=TranscriptInput(
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
        ),
    )

    assert request.model_dump(mode="json")["meeting_started_at"] == "2026-06-07T10:00:00Z"


def test_agent_budget_records_usage() -> None:
    budget = AgentBudget(max_requests=3, max_output_tokens=1000)
    usage = AgentUsage(
        requests=1,
        input_tokens=200,
        output_tokens=300,
        total_tokens=500,
    )

    budget.ensure_available(800)
    budget.record(usage)

    assert budget.requests_used == 1
    assert budget.output_tokens_used == usage.output_tokens


def test_agent_budget_rejects_output_reservation_over_remaining_limit() -> None:
    budget = AgentBudget(max_requests=3, max_output_tokens=1000, output_tokens_used=700)

    with pytest.raises(AgentBudgetExceededError, match="output token budget exhausted"):
        budget.ensure_available(301)


def test_agent_budget_rejects_usage_over_request_limit() -> None:
    budget = AgentBudget(max_requests=1, max_output_tokens=1000)

    with pytest.raises(AgentBudgetExceededError, match="request budget exceeded"):
        budget.record(
            AgentUsage(
                requests=2,
                input_tokens=100,
                output_tokens=100,
                total_tokens=200,
            )
        )


@pytest.mark.parametrize("output_type", [MeetingExtraction, RecapDraft, VerificationReport])
def test_agent_output_schema_uses_supported_strict_subset(output_type: type[StrictModel]) -> None:
    schema = json.dumps(output_type.model_json_schema())
    unsupported_keywords = (
        '"default"',
        '"maxItems"',
        '"maxLength"',
        '"maximum"',
        '"minItems"',
        '"minLength"',
        '"minimum"',
        '"pattern"',
    )

    assert all(keyword not in schema for keyword in unsupported_keywords)
