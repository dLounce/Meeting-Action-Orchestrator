from __future__ import annotations

from meeting_action_orchestrator.agents.contracts import (
    AgentDefinition,
    AgentResult,
    AgentRunContext,
    AgentStageMismatchError,
    ExtractionRequest,
    MeetingExtraction,
    OutputT,
    RecapDraft,
    RecapRequest,
    StrictModel,
    StructuredAgentRunner,
    VerificationReport,
    VerificationRequest,
)
from meeting_action_orchestrator.agents.prompts import (
    EXTRACTOR_PROMPT,
    RECAP_PROMPT,
    VERIFIER_PROMPT,
)


class MeetingSpecialists:
    def __init__(
        self,
        runner: StructuredAgentRunner,
        worker_model: str,
        recap_model: str,
        extractor_max_output_tokens: int = 6500,
        recap_max_output_tokens: int = 2500,
        verifier_max_output_tokens: int = 3000,
    ) -> None:
        self._runner = runner
        self._extractor = AgentDefinition(
            name="Outcome Extractor",
            instructions=EXTRACTOR_PROMPT,
            model=worker_model,
            output_type=MeetingExtraction,
            reasoning_effort="low",
            max_output_tokens=extractor_max_output_tokens,
        )
        self._recap_writer = AgentDefinition(
            name="Recap Writer",
            instructions=RECAP_PROMPT,
            model=recap_model,
            output_type=RecapDraft,
            reasoning_effort="none",
            max_output_tokens=recap_max_output_tokens,
        )
        self._verifier = AgentDefinition(
            name="Package Verifier",
            instructions=VERIFIER_PROMPT,
            model=worker_model,
            output_type=VerificationReport,
            reasoning_effort="low",
            max_output_tokens=verifier_max_output_tokens,
        )

    async def extract(
        self,
        request: ExtractionRequest,
        context: AgentRunContext,
    ) -> AgentResult[MeetingExtraction]:
        self._ensure_stage(context, "extract")
        return await self._run(self._extractor, request, context)

    async def write_recap(
        self,
        request: RecapRequest,
        context: AgentRunContext,
    ) -> AgentResult[RecapDraft]:
        self._ensure_stage(context, "recap")
        return await self._run(self._recap_writer, request, context)

    async def verify(
        self,
        request: VerificationRequest,
        context: AgentRunContext,
    ) -> AgentResult[VerificationReport]:
        self._ensure_stage(context, "verify")
        return await self._run(self._verifier, request, context)

    async def _run(
        self,
        definition: AgentDefinition[OutputT],
        request: StrictModel,
        context: AgentRunContext,
    ) -> AgentResult[OutputT]:
        context.budget.ensure_available(definition.max_output_tokens)
        result = await self._runner.run(definition, request, context)
        context.budget.record(result.usage)
        return result

    @staticmethod
    def _ensure_stage(
        context: AgentRunContext,
        expected: str,
    ) -> None:
        if context.stage != expected:
            raise AgentStageMismatchError(expected)
