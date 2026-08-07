from __future__ import annotations

from datetime import date, datetime, time
from typing import Annotated, Literal
from uuid import UUID

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    model_validator,
)

from meeting_action_orchestrator.api.contracts import (
    DeliveryResult,
    MeetingPageResult,
    ProcessingResult,
    ReadinessResult,
)
from meeting_action_orchestrator.application.processing_control import ProcessingControlResult
from meeting_action_orchestrator.application.workflow import ApprovalResult
from meeting_action_orchestrator.domain.enums import (
    FailureCode,
    FailureDisposition,
    MeetingStatus,
    ProcessingJobStatus,
    ProcessingStage,
    ReviewOrigin,
    WriteKind,
    WriteStatus,
)
from meeting_action_orchestrator.domain.models import (
    ActionItem,
    Decision,
    DeliveryDirective,
    Meeting,
    OpenQuestion,
    PersonRef,
    ProcessingJob,
    RecapArtifact,
    ReviewIssue,
    ReviewRevision,
    Risk,
    TimezoneName,
    Transcript,
    TranscriptSegment,
    WriteIntent,
    WriteProposal,
    WriteReceipt,
)

ShortText = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=200)]
LongText = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=10_000)]
MarkdownText = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1, max_length=100_000),
]


class ApiModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=True)


class ParticipantInput(ApiModel):
    display_name: ShortText
    email: Annotated[str, StringConstraints(min_length=3, max_length=320)] | None = None

    def to_domain(self) -> PersonRef:
        return PersonRef(display_name=self.display_name, email=self.email)


class CreateMeetingRequest(ApiModel):
    title: ShortText
    occurred_at: AwareDatetime
    timezone: TimezoneName
    participants: tuple[ParticipantInput, ...] = Field(default=(), max_length=100)


class ActionRevisionRequest(ApiModel):
    title: ShortText
    owner: ShortText | None = None
    due_date: date | None = None
    due_time: time | None = None
    timezone: TimezoneName
    notes: LongText | None = None
    recap_markdown: MarkdownText | None = None

    @model_validator(mode="after")
    def validate_deadline(self) -> ActionRevisionRequest:
        if self.due_time is not None and self.due_date is None:
            raise ValueError("due_time requires due_date")
        return self


class DeliverySelectionRequest(ApiModel):
    enabled: bool


class IssueResolutionRequest(ApiModel):
    status: Literal["resolved", "accepted_risk"]
    resolution_note: Annotated[
        str,
        StringConstraints(strip_whitespace=True, min_length=1, max_length=1_000),
    ]


class DeliveryOperationRequest(ApiModel):
    intent_ids: tuple[UUID, ...] = Field(default=(), max_length=100)

    @model_validator(mode="after")
    def validate_intent_ids(self) -> DeliveryOperationRequest:
        if len(set(self.intent_ids)) != len(self.intent_ids):
            raise ValueError("intent_ids must be unique")
        return self


class HealthResponse(ApiModel):
    status: Literal["ok"] = "ok"
    service: Literal["meeting-action-orchestrator"] = "meeting-action-orchestrator"
    version: str


class ReadinessCheckResponse(ApiModel):
    name: str
    status: Literal["ready", "not_ready"]


class ReadinessResponse(ApiModel):
    status: Literal["ready", "not_ready"]
    checks: tuple[ReadinessCheckResponse, ...]

    @classmethod
    def from_result(cls, result: ReadinessResult) -> ReadinessResponse:
        return cls(
            status="ready" if result.ready else "not_ready",
            checks=tuple(
                ReadinessCheckResponse(
                    name=check.name,
                    status="ready" if check.ready else "not_ready",
                )
                for check in result.checks
            ),
        )


class FailureResponse(ApiModel):
    code: FailureCode
    disposition: FailureDisposition
    message: str
    occurred_at: datetime


class MeetingResponse(ApiModel):
    id: UUID
    title: str
    occurred_at: datetime | None
    timezone: str
    participants: tuple[PersonRef, ...]
    status: MeetingStatus
    current_transcript_id: UUID | None
    current_review_id: UUID | None
    approved_review_id: UUID | None
    failure: FailureResponse | None
    version: int
    created_at: datetime
    updated_at: datetime

    @classmethod
    def from_domain(cls, meeting: Meeting) -> MeetingResponse:
        failure = meeting.failure
        return cls(
            id=meeting.id,
            title=meeting.title,
            occurred_at=meeting.occurred_at,
            timezone=meeting.timezone,
            participants=meeting.participants,
            status=meeting.status,
            current_transcript_id=meeting.current_transcript_id,
            current_review_id=meeting.current_review_id,
            approved_review_id=meeting.approved_review_id,
            failure=(
                FailureResponse(
                    code=failure.code,
                    disposition=failure.disposition,
                    message=failure.safe_message,
                    occurred_at=failure.occurred_at,
                )
                if failure is not None
                else None
            ),
            version=meeting.version,
            created_at=meeting.created_at,
            updated_at=meeting.updated_at,
        )


class MeetingListResponse(ApiModel):
    items: tuple[MeetingResponse, ...]
    next_cursor: str | None

    @classmethod
    def from_result(
        cls,
        result: MeetingPageResult,
        next_cursor: str | None,
    ) -> MeetingListResponse:
        return cls(
            items=tuple(MeetingResponse.from_domain(item) for item in result.items),
            next_cursor=next_cursor,
        )


class ProcessingJobResponse(ApiModel):
    id: UUID
    stage: ProcessingStage
    status: ProcessingJobStatus
    attempt_count: int
    max_attempts: int
    next_attempt_at: datetime | None
    failure: FailureResponse | None
    created_at: datetime
    updated_at: datetime

    @classmethod
    def from_domain(cls, job: ProcessingJob) -> ProcessingJobResponse:
        failure = job.last_failure
        return cls(
            id=job.id,
            stage=job.stage,
            status=job.status,
            attempt_count=job.attempt_count,
            max_attempts=job.max_attempts,
            next_attempt_at=job.next_attempt_at,
            failure=(
                FailureResponse(
                    code=failure.code,
                    disposition=failure.disposition,
                    message=failure.safe_message,
                    occurred_at=failure.occurred_at,
                )
                if failure is not None
                else None
            ),
            created_at=job.created_at,
            updated_at=job.updated_at,
        )


class ProcessingResponse(ApiModel):
    meeting_id: UUID
    jobs: tuple[ProcessingJobResponse, ...]

    @classmethod
    def from_result(cls, result: ProcessingResult) -> ProcessingResponse:
        return cls(
            meeting_id=result.meeting_id,
            jobs=tuple(ProcessingJobResponse.from_domain(job) for job in result.jobs),
        )


class ProcessingControlResponse(ApiModel):
    meeting: MeetingResponse
    jobs: tuple[ProcessingJobResponse, ...]
    replayed: bool

    @classmethod
    def from_result(cls, result: ProcessingControlResult) -> ProcessingControlResponse:
        return cls(
            meeting=MeetingResponse.from_domain(result.meeting),
            jobs=tuple(ProcessingJobResponse.from_domain(job) for job in result.jobs),
            replayed=result.replayed,
        )


class RecapResponse(ApiModel):
    id: UUID
    meeting_id: UUID
    approval_id: UUID
    format: Literal["markdown"] = "markdown"
    content: str
    sha256: str
    created_at: datetime

    @classmethod
    def from_domain(cls, recap: RecapArtifact) -> RecapResponse:
        return cls(
            id=recap.id,
            meeting_id=recap.meeting_id,
            approval_id=recap.approval_id,
            content=recap.content,
            sha256=recap.sha256,
            created_at=recap.created_at,
        )


class TranscriptResponse(ApiModel):
    id: UUID
    meeting_id: UUID
    language: str
    text: str
    segments: tuple[TranscriptSegment, ...]
    sha256: str
    created_at: datetime

    @classmethod
    def from_domain(cls, transcript: Transcript) -> TranscriptResponse:
        return cls(
            id=transcript.id,
            meeting_id=transcript.meeting_id,
            language=transcript.language,
            text=transcript.text,
            segments=transcript.segments,
            sha256=transcript.sha256,
            created_at=transcript.created_at,
        )


class ReviewResponse(ApiModel):
    id: UUID
    meeting_id: UUID
    transcript_id: UUID
    revision_number: int
    origin: ReviewOrigin
    purpose: str | None
    recap_markdown: str
    decisions: tuple[Decision, ...]
    action_items: tuple[ActionItem, ...]
    open_questions: tuple[OpenQuestion, ...]
    risks: tuple[Risk, ...]
    issues: tuple[ReviewIssue, ...]
    directives: tuple[DeliveryDirective, ...]
    content_digest: str
    actor_id: str | None
    created_at: datetime

    @classmethod
    def from_domain(cls, review: ReviewRevision) -> ReviewResponse:
        return cls.model_validate(review.model_dump(mode="python"))


class WriteIntentResponse(ApiModel):
    id: UUID
    meeting_id: UUID
    approval_id: UUID
    kind: WriteKind
    source_action_id: UUID
    proposal: WriteProposal
    status: WriteStatus
    attempt_count: int
    failure: FailureResponse | None
    created_at: datetime
    updated_at: datetime

    @classmethod
    def from_domain(cls, intent: WriteIntent) -> WriteIntentResponse:
        failure = intent.last_failure
        return cls(
            id=intent.id,
            meeting_id=intent.meeting_id,
            approval_id=intent.approval_id,
            kind=intent.proposal.kind,
            source_action_id=intent.proposal.source_action_id,
            proposal=intent.proposal,
            status=intent.status,
            attempt_count=intent.attempt_count,
            failure=(
                FailureResponse(
                    code=failure.code,
                    disposition=failure.disposition,
                    message=failure.safe_message,
                    occurred_at=failure.occurred_at,
                )
                if failure is not None
                else None
            ),
            created_at=intent.created_at,
            updated_at=intent.updated_at,
        )


class WriteReceiptResponse(ApiModel):
    intent_id: UUID
    provider: str
    external_id: str
    external_url: str | None
    reconciled: bool
    recorded_at: datetime

    @classmethod
    def from_domain(cls, receipt: WriteReceipt) -> WriteReceiptResponse:
        return cls(
            intent_id=receipt.intent_id,
            provider=receipt.provider,
            external_id=receipt.external_id,
            external_url=receipt.external_url,
            reconciled=receipt.reconciled,
            recorded_at=receipt.recorded_at,
        )


class ApprovalResponse(ApiModel):
    approval_id: UUID
    meeting_id: UUID
    review_id: UUID
    review_digest: str
    approved_at: datetime
    recap: str
    recap_sha256: str
    intents: tuple[WriteIntentResponse, ...]
    replayed: bool

    @classmethod
    def from_result(cls, result: ApprovalResult) -> ApprovalResponse:
        return cls(
            approval_id=result.approval.id,
            meeting_id=result.approval.meeting_id,
            review_id=result.approval.review_revision_id,
            review_digest=result.approval.review_digest,
            approved_at=result.approval.approved_at,
            recap=result.recap.content,
            recap_sha256=result.recap.sha256,
            intents=tuple(WriteIntentResponse.from_domain(item) for item in result.intents),
            replayed=result.replayed,
        )


class DeliveryResponse(ApiModel):
    meeting: MeetingResponse
    intents: tuple[WriteIntentResponse, ...]
    receipts: tuple[WriteReceiptResponse, ...]
    replayed: bool

    @classmethod
    def from_result(cls, result: DeliveryResult) -> DeliveryResponse:
        return cls(
            meeting=MeetingResponse.from_domain(result.meeting),
            intents=tuple(WriteIntentResponse.from_domain(item) for item in result.intents),
            receipts=tuple(WriteReceiptResponse.from_domain(item) for item in result.receipts),
            replayed=result.replayed,
        )
