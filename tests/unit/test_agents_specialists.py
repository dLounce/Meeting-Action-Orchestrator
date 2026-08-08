from datetime import datetime, timezone
from typing import Literal, cast

import pytest

from meeting_action_orchestrator.agents import (
    AgentBudget,
    AgentBudgetExceededError,
    AgentDefinition,
    AgentResult,
    AgentRunContext,
    AgentUsage,
    CanonicalMeetingRecord,
    ExtractionRequest,
    MeetingExtraction,
    MeetingSpecialists,
    RecapDraft,
    RecapRequest,
    StrictModel,
    TranscriptInput,
    TranscriptSegmentInput,
    VerificationReport,
    VerificationRequest,
)
from meeting_action_orchestrator.agents.contracts import OutputT


class FakeRunner:
    def __init__(self, outputs: list[StrictModel]) -> None:
        self.outputs = outputs
        self.calls: list[tuple[AgentDefinition[StrictModel], StrictModel, AgentRunContext]] = []

    async def run(
        self,
        definition: AgentDefinition[OutputT],
        payload: StrictModel,
        context: AgentRunContext,
    ) -> AgentResult[OutputT]:
        self.calls.append((cast(AgentDefinition[StrictModel], definition), payload, context))
        output = self.outputs.pop(0)
        assert isinstance(output, definition.output_type)
        return AgentResult(
            output=output,
            usage=AgentUsage(
                requests=1,
                input_tokens=100,
                output_tokens=50,
                total_tokens=150,
            ),
            model=definition.model,
            workflow_request_ids=("req_1",),
        )


def transcript() -> TranscriptInput:
    return TranscriptInput(
        language="en",
        text="We will ship it.",
        sha256="a" * 64,
        segments=[
            TranscriptSegmentInput(
                id="segment_1",
                start_ms=0,
                end_ms=1000,
                speaker="A",
                text="We will ship it.",
            )
        ],
    )


def record() -> CanonicalMeetingRecord:
    return CanonicalMeetingRecord(
        title="Launch review",
        purpose=None,
        participants=[],
        items=[],
        warnings=[],
    )


def context(
    stage: Literal["extract", "recap", "verify"],
    max_output_tokens: int = 12_000,
) -> AgentRunContext:
    return AgentRunContext(
        job_id="job_1",
        stage=stage,
        budget=AgentBudget(max_requests=5, max_output_tokens=max_output_tokens),
    )


@pytest.mark.asyncio
async def test_extractor_uses_worker_model_and_typed_output() -> None:
    extractor_max_output_tokens = 6500
    output = MeetingExtraction(
        suggested_title="Launch review",
        purpose=None,
        participants=[],
        decisions=[],
        action_items=[],
        open_questions=[],
        risks=[],
        warnings=[],
    )
    runner = FakeRunner([output])
    specialists = MeetingSpecialists(
        runner,
        worker_model="worker",
        recap_model="recap",
        extractor_max_output_tokens=extractor_max_output_tokens,
    )
    request = ExtractionRequest(
        meeting_id="meeting_1",
        meeting_started_at=datetime(2026, 6, 7, 10, 0, tzinfo=timezone.utc),
        timezone="Asia/Calcutta",
        transcript=transcript(),
    )

    result = await specialists.extract(request, context("extract"))

    definition, payload, run_context = runner.calls[0]
    assert result.output is output
    assert definition.model == "worker"
    assert definition.output_type is MeetingExtraction
    assert definition.reasoning_effort == "low"
    assert definition.max_output_tokens == extractor_max_output_tokens
    assert payload is request
    assert run_context.budget.requests_used == 1


@pytest.mark.asyncio
async def test_recap_writer_uses_recap_model_without_reasoning() -> None:
    output = RecapDraft(title="Launch review", overview="The launch is ready.", highlights=[])
    runner = FakeRunner([output])
    specialists = MeetingSpecialists(runner, "worker", "recap")
    request = RecapRequest(meeting_id="meeting_1", record=record())

    result = await specialists.write_recap(request, context("recap"))

    definition, _, _ = runner.calls[0]
    assert result.output is output
    assert definition.model == "recap"
    assert definition.output_type is RecapDraft
    assert definition.reasoning_effort == "none"


@pytest.mark.asyncio
async def test_verifier_uses_worker_model_and_typed_output() -> None:
    recap = RecapDraft(title="Launch review", overview="The launch is ready.", highlights=[])
    output = VerificationReport(verdict="pass", findings=[])
    runner = FakeRunner([output])
    specialists = MeetingSpecialists(runner, worker_model="worker", recap_model="recap")
    request = VerificationRequest(
        meeting_id="meeting_1",
        transcript=transcript(),
        record=record(),
        recap=recap,
    )

    result = await specialists.verify(request, context("verify"))

    definition, _, _ = runner.calls[0]
    assert result.output is output
    assert definition.model == "worker"
    assert definition.output_type is VerificationReport
    assert definition.reasoning_effort == "low"


@pytest.mark.asyncio
async def test_specialist_rejects_wrong_stage_before_runner_call() -> None:
    runner = FakeRunner([])
    specialists = MeetingSpecialists(runner, worker_model="worker", recap_model="recap")
    request = RecapRequest(meeting_id="meeting_1", record=record())

    with pytest.raises(ValueError, match="Expected recap"):
        await specialists.write_recap(request, context("verify"))

    assert runner.calls == []


@pytest.mark.asyncio
async def test_specialist_rejects_call_when_output_budget_is_insufficient() -> None:
    runner = FakeRunner([])
    specialists = MeetingSpecialists(runner, worker_model="worker", recap_model="recap")
    request = RecapRequest(meeting_id="meeting_1", record=record())

    with pytest.raises(AgentBudgetExceededError, match="output token budget exhausted"):
        await specialists.write_recap(request, context("recap", max_output_tokens=2499))

    assert runner.calls == []
