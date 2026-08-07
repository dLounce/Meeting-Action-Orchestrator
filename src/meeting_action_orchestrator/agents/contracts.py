from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, Literal, Protocol, TypeVar

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=True)


ModelT = TypeVar("ModelT", bound=StrictModel)


class EvidenceRef(StrictModel):
    segment_id: str
    quote: str


class TranscriptSegmentInput(StrictModel):
    id: str = Field(min_length=1, max_length=128)
    start_ms: int = Field(ge=0)
    end_ms: int | None = Field(default=None, ge=0)
    speaker: str | None = Field(default=None, max_length=200)
    text: str = Field(min_length=1, max_length=10_000)


class TranscriptInput(StrictModel):
    language: str | None = Field(default=None, max_length=32)
    text: str = Field(min_length=1, max_length=250_000)
    sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    segments: list[TranscriptSegmentInput] = Field(min_length=1, max_length=5_000)


class ExtractionRequest(StrictModel):
    meeting_id: str = Field(min_length=1, max_length=128)
    meeting_started_at: AwareDatetime
    timezone: str = Field(min_length=1, max_length=128)
    transcript: TranscriptInput


class ParticipantCandidate(StrictModel):
    display_name: str | None
    speaker_labels: list[str]
    evidence: list[EvidenceRef]


class DecisionCandidate(StrictModel):
    statement: str
    owner: str | None
    rationale: str | None
    confidence: Literal["high", "medium", "low"]
    evidence: list[EvidenceRef]


class ActionItemCandidate(StrictModel):
    description: str
    owner: str | None
    due_expression: str | None
    dependency: str | None
    confidence: Literal["high", "medium", "low"]
    requires_clarification: bool
    evidence: list[EvidenceRef]


class OpenQuestionCandidate(StrictModel):
    question: str
    owner: str | None
    evidence: list[EvidenceRef]


class RiskCandidate(StrictModel):
    description: str
    owner: str | None
    evidence: list[EvidenceRef]


class MeetingExtraction(StrictModel):
    suggested_title: str
    purpose: str | None
    participants: list[ParticipantCandidate]
    decisions: list[DecisionCandidate]
    action_items: list[ActionItemCandidate]
    open_questions: list[OpenQuestionCandidate]
    risks: list[RiskCandidate]
    warnings: list[str]

    @model_validator(mode="after")
    def validate_encoded_size(self) -> MeetingExtraction:
        return _bounded_output(self, 100_000)


class RecordItem(StrictModel):
    id: str = Field(min_length=1, max_length=128)
    kind: Literal["decision", "action_item", "open_question", "risk"]
    text: str = Field(min_length=1, max_length=2000)
    owner: str | None = Field(default=None, max_length=200)
    due_expression: str | None = Field(default=None, max_length=300)
    confidence: Literal["high", "medium", "low"]
    evidence: list[EvidenceRef] = Field(min_length=1)


class CanonicalMeetingRecord(StrictModel):
    title: str = Field(min_length=1, max_length=300)
    purpose: str | None = Field(default=None, max_length=2000)
    participants: list[str] = Field(max_length=200)
    items: list[RecordItem] = Field(max_length=2000)
    warnings: list[str] = Field(max_length=100)


class RecapRequest(StrictModel):
    meeting_id: str = Field(min_length=1, max_length=128)
    record: CanonicalMeetingRecord


class ReferencedText(StrictModel):
    text: str
    source_ids: list[str]


class RecapDraft(StrictModel):
    title: str
    overview: str
    highlights: list[ReferencedText]

    @model_validator(mode="after")
    def validate_encoded_size(self) -> RecapDraft:
        return _bounded_output(self, 50_000)


class VerificationRequest(StrictModel):
    meeting_id: str = Field(min_length=1, max_length=128)
    transcript: TranscriptInput
    record: CanonicalMeetingRecord
    recap: RecapDraft


FindingCode = Literal[
    "unsupported_claim",
    "missing_decision",
    "missing_action",
    "wrong_owner",
    "wrong_deadline",
    "contradiction",
    "invalid_reference",
]


class VerificationFinding(StrictModel):
    severity: Literal["blocker", "warning"]
    code: FindingCode
    subject_id: str | None
    message: str
    evidence: list[EvidenceRef]


class VerificationReport(StrictModel):
    verdict: Literal["pass", "review_required"]
    findings: list[VerificationFinding]

    @model_validator(mode="after")
    def validate_encoded_size(self) -> VerificationReport:
        return _bounded_output(self, 100_000)


def _bounded_output(model: ModelT, max_characters: int) -> ModelT:
    if len(model.model_dump_json()) > max_characters:
        raise ValueError("Structured agent output exceeds the allowed size")
    return model


OutputT = TypeVar("OutputT", bound=StrictModel)


@dataclass(frozen=True)
class AgentDefinition(Generic[OutputT]):
    name: str
    instructions: str
    model: str
    output_type: type[OutputT]
    reasoning_effort: Literal["none", "low", "medium", "high"]
    max_output_tokens: int


@dataclass(frozen=True)
class AgentUsage:
    requests: int
    input_tokens: int
    output_tokens: int
    total_tokens: int
    cached_input_tokens: int = 0
    reasoning_tokens: int = 0


@dataclass(frozen=True)
class AgentResult(Generic[OutputT]):
    output: OutputT
    usage: AgentUsage
    model: str
    workflow_request_ids: tuple[str, ...]


class AgentBudgetExceededError(RuntimeError):
    def __init__(
        self,
        resource: Literal["request", "output"],
        state: Literal["exhausted", "exceeded"],
    ) -> None:
        label = "output token" if resource == "output" else resource
        super().__init__(f"Agent {label} budget {state}")


class AgentStageMismatchError(ValueError):
    def __init__(self, expected: str) -> None:
        super().__init__(f"Expected {expected} agent context")


@dataclass
class AgentBudget:
    max_requests: int
    max_output_tokens: int
    requests_used: int = 0
    output_tokens_used: int = 0

    def ensure_available(self, requested_output_tokens: int) -> None:
        if self.requests_used >= self.max_requests:
            raise AgentBudgetExceededError("request", "exhausted")
        remaining = self.max_output_tokens - self.output_tokens_used
        if requested_output_tokens > remaining:
            raise AgentBudgetExceededError("output", "exhausted")

    def record(self, usage: AgentUsage) -> None:
        self.requests_used += usage.requests
        self.output_tokens_used += usage.output_tokens
        if self.requests_used > self.max_requests:
            raise AgentBudgetExceededError("request", "exceeded")
        if self.output_tokens_used > self.max_output_tokens:
            raise AgentBudgetExceededError("output", "exceeded")


@dataclass(frozen=True)
class AgentRunContext:
    job_id: str
    stage: Literal["extract", "recap", "verify"]
    budget: AgentBudget


class StructuredAgentRunner(Protocol):
    async def run(
        self,
        definition: AgentDefinition[OutputT],
        payload: StrictModel,
        context: AgentRunContext,
    ) -> AgentResult[OutputT]: ...
