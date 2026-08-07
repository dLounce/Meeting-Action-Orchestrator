from __future__ import annotations

import re
from datetime import date
from typing import Annotated, Literal
from uuid import UUID
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import (
    AfterValidator,
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    field_validator,
    model_validator,
)

from meeting_action_orchestrator.domain.enums import (
    AudioMediaType,
    DeadlineKind,
    DeadlineResolution,
    DeliveryOperationKind,
    FailureCode,
    FailureDisposition,
    IssueSeverity,
    IssueStatus,
    MeetingStatus,
    Priority,
    ProcessingJobStatus,
    ProcessingStage,
    ReviewOrigin,
    WriteKind,
    WriteStatus,
)
from meeting_action_orchestrator.domain.errors import (
    DomainInvariantError,
    DomainValueCode,
    InvalidDomainValueError,
    InvariantCode,
)
from meeting_action_orchestrator.domain.hashing import canonical_sha256, text_sha256

ShortText = Annotated[str, StringConstraints(min_length=1, max_length=200)]
MediumText = Annotated[str, StringConstraints(min_length=1, max_length=1_000)]
DetailedText = Annotated[str, StringConstraints(min_length=1, max_length=2_000)]
LongText = Annotated[str, StringConstraints(min_length=1, max_length=10_000)]
Sha256Digest = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
Confidence = Annotated[float, Field(ge=0.0, le=1.0)]


def _validate_timezone(value: str) -> str:
    try:
        ZoneInfo(value)
    except ZoneInfoNotFoundError as exc:
        raise InvalidDomainValueError(DomainValueCode.TIMEZONE) from exc
    return value


TimezoneName = Annotated[
    str,
    StringConstraints(min_length=1, max_length=100),
    AfterValidator(_validate_timezone),
]


class DomainModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=True)


class WorkflowFailure(DomainModel):
    code: FailureCode
    disposition: FailureDisposition
    safe_message: MediumText
    provider_request_id: ShortText | None = None
    occurred_at: AwareDatetime


class DeliveryOperationBinding(DomainModel):
    request_key: ShortText
    meeting_id: UUID
    operation: DeliveryOperationKind
    actor_id: ShortText
    selection_fingerprint: Sha256Digest
    created_at: AwareDatetime


PROCESSING_MAX_ATTEMPTS = {
    ProcessingStage.TRANSCRIPTION: 3,
    ProcessingStage.EXTRACTION: 2,
}


class ProcessingJob(DomainModel):
    id: UUID
    meeting_id: UUID
    stage: ProcessingStage
    status: ProcessingJobStatus = ProcessingJobStatus.READY
    attempt_count: int = Field(default=0, ge=0)
    max_attempts: int = Field(gt=0)
    next_attempt_at: AwareDatetime | None = None
    lease_owner: ShortText | None = None
    lease_expires_at: AwareDatetime | None = None
    last_failure: WorkflowFailure | None = None
    created_at: AwareDatetime
    updated_at: AwareDatetime

    @model_validator(mode="after")
    def validate_limits(self) -> ProcessingJob:
        if self.updated_at < self.created_at:
            raise DomainInvariantError(InvariantCode.JOB_TIMESTAMPS)
        if self.attempt_count > self.max_attempts:
            raise DomainInvariantError(InvariantCode.JOB_ATTEMPTS)
        if self.max_attempts != PROCESSING_MAX_ATTEMPTS[self.stage]:
            raise DomainInvariantError(InvariantCode.JOB_MAX_ATTEMPTS)
        return self

    @model_validator(mode="after")
    def validate_lease(self) -> ProcessingJob:
        if self.status is ProcessingJobStatus.RUNNING:
            if self.lease_owner is None or self.lease_expires_at is None:
                raise DomainInvariantError(InvariantCode.JOB_LEASE_REQUIRED)
            if self.lease_expires_at <= self.updated_at:
                raise DomainInvariantError(InvariantCode.JOB_LEASE_EXPIRY)
        elif self.lease_owner is not None or self.lease_expires_at is not None:
            raise DomainInvariantError(InvariantCode.JOB_LEASE_FORBIDDEN)
        return self

    @model_validator(mode="after")
    def validate_retry(self) -> ProcessingJob:
        if self.status is ProcessingJobStatus.RETRY_WAIT:
            if self.next_attempt_at is None:
                raise DomainInvariantError(InvariantCode.JOB_RETRY_REQUIRED)
            if self.next_attempt_at < self.updated_at:
                raise DomainInvariantError(InvariantCode.JOB_RETRY_EXPIRY)
            if (
                self.last_failure is not None
                and self.last_failure.disposition is not FailureDisposition.RETRYABLE
            ):
                raise DomainInvariantError(InvariantCode.JOB_RETRY_DISPOSITION)
        elif self.next_attempt_at is not None:
            raise DomainInvariantError(InvariantCode.JOB_RETRY_FORBIDDEN)
        return self

    @model_validator(mode="after")
    def validate_failure(self) -> ProcessingJob:
        failed_statuses = {ProcessingJobStatus.RETRY_WAIT, ProcessingJobStatus.FAILED}
        if self.status in failed_statuses and self.last_failure is None:
            raise DomainInvariantError(InvariantCode.JOB_FAILURE_REQUIRED)
        failure_forbidden = {
            ProcessingJobStatus.READY,
            ProcessingJobStatus.RUNNING,
            ProcessingJobStatus.SUCCEEDED,
        }
        if self.status in failure_forbidden and self.last_failure is not None:
            raise DomainInvariantError(InvariantCode.JOB_FAILURE_FORBIDDEN)
        return self


class AudioAsset(DomainModel):
    id: UUID
    storage_key: MediumText
    original_name: ShortText
    detected_media_type: AudioMediaType
    size_bytes: int = Field(gt=0)
    duration_ms: int = Field(gt=0)
    sha256: Sha256Digest
    created_at: AwareDatetime

    @field_validator("original_name")
    @classmethod
    def validate_original_name(cls, value: str) -> str:
        if "\x00" in value or "/" in value or "\\" in value or value in {".", ".."}:
            raise InvalidDomainValueError(DomainValueCode.ORIGINAL_NAME)
        return value


class PersonRef(DomainModel):
    display_name: ShortText
    email: Annotated[str, StringConstraints(min_length=3, max_length=320)] | None = None

    @field_validator("email")
    @classmethod
    def validate_email(cls, value: str | None) -> str | None:
        if value is not None and re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", value) is None:
            raise InvalidDomainValueError(DomainValueCode.EMAIL)
        return value


class Meeting(DomainModel):
    id: UUID
    ingest_key: ShortText
    title: ShortText
    audio_asset_id: UUID
    occurred_at: AwareDatetime | None = None
    timezone: TimezoneName
    participants: tuple[PersonRef, ...] = ()
    status: MeetingStatus = MeetingStatus.INGESTED
    current_transcript_id: UUID | None = None
    current_review_id: UUID | None = None
    approved_review_id: UUID | None = None
    failure: WorkflowFailure | None = None
    version: int = Field(default=0, ge=0)
    created_at: AwareDatetime
    updated_at: AwareDatetime

    @model_validator(mode="after")
    def validate_lifecycle(self) -> Meeting:
        if self.updated_at < self.created_at:
            raise DomainInvariantError(InvariantCode.MEETING_TIMESTAMPS)
        transcript_statuses = {
            MeetingStatus.TRANSCRIBED,
            MeetingStatus.EXTRACTING,
            MeetingStatus.EXTRACTION_FAILED,
            MeetingStatus.AWAITING_APPROVAL,
            MeetingStatus.APPROVED,
            MeetingStatus.FILING,
            MeetingStatus.PARTIALLY_FILED,
            MeetingStatus.FILING_FAILED,
            MeetingStatus.COMPLETED,
        }
        review_statuses = {
            MeetingStatus.AWAITING_APPROVAL,
            MeetingStatus.APPROVED,
            MeetingStatus.FILING,
            MeetingStatus.PARTIALLY_FILED,
            MeetingStatus.FILING_FAILED,
            MeetingStatus.COMPLETED,
        }
        approved_statuses = {
            MeetingStatus.APPROVED,
            MeetingStatus.FILING,
            MeetingStatus.PARTIALLY_FILED,
            MeetingStatus.FILING_FAILED,
            MeetingStatus.COMPLETED,
        }
        failed_statuses = {
            MeetingStatus.TRANSCRIPTION_FAILED,
            MeetingStatus.EXTRACTION_FAILED,
            MeetingStatus.PARTIALLY_FILED,
            MeetingStatus.FILING_FAILED,
        }
        if self.status in transcript_statuses and self.current_transcript_id is None:
            raise DomainInvariantError(InvariantCode.MEETING_TRANSCRIPT)
        if self.status in review_statuses and self.current_review_id is None:
            raise DomainInvariantError(InvariantCode.MEETING_REVIEW)
        if self.status in approved_statuses and self.approved_review_id is None:
            raise DomainInvariantError(InvariantCode.MEETING_APPROVAL)
        if self.status in failed_statuses and self.failure is None:
            raise DomainInvariantError(InvariantCode.MEETING_FAILURE)
        return self


class TranscriptSegment(DomainModel):
    id: UUID
    ordinal: int = Field(ge=0)
    start_ms: int = Field(ge=0)
    end_ms: int = Field(gt=0)
    speaker: ShortText | None = None
    text: LongText

    @model_validator(mode="after")
    def validate_range(self) -> TranscriptSegment:
        if self.end_ms < self.start_ms:
            raise DomainInvariantError(InvariantCode.SEGMENT_RANGE)
        return self


class Transcript(DomainModel):
    id: UUID
    meeting_id: UUID
    audio_asset_id: UUID
    provider: ShortText
    model: ShortText
    language: ShortText
    text: Annotated[str, StringConstraints(min_length=1, max_length=250_000)]
    segments: tuple[TranscriptSegment, ...] = Field(min_length=1, max_length=5_000)
    sha256: str = ""
    provider_request_id: ShortText | None = None
    created_at: AwareDatetime

    @model_validator(mode="after")
    def validate_segments_and_hash(self) -> Transcript:
        ordinals = tuple(segment.ordinal for segment in self.segments)
        if ordinals != tuple(range(len(self.segments))):
            raise DomainInvariantError(InvariantCode.SEGMENT_ORDINALS)
        starts = tuple(segment.start_ms for segment in self.segments)
        if starts != tuple(sorted(starts)):
            raise DomainInvariantError(InvariantCode.SEGMENT_ORDER)
        if sum(len(segment.text) for segment in self.segments) > 300_000:
            raise DomainInvariantError(InvariantCode.TRANSCRIPT_SIZE)
        expected = text_sha256(self.text)
        if self.sha256 and self.sha256 != expected:
            raise DomainInvariantError(InvariantCode.TRANSCRIPT_HASH)
        object.__setattr__(self, "sha256", expected)
        return self


class EvidenceRef(DomainModel):
    segment_ids: tuple[UUID, ...] = Field(min_length=1)
    quote: MediumText

    @field_validator("segment_ids")
    @classmethod
    def validate_segment_ids(cls, value: tuple[UUID, ...]) -> tuple[UUID, ...]:
        if len(value) != len(set(value)):
            raise InvalidDomainValueError(DomainValueCode.SEGMENT_IDS)
        return value


class Decision(DomainModel):
    id: UUID
    summary: MediumText
    detail: LongText | None = None
    evidence: tuple[EvidenceRef, ...] = ()
    confidence: Confidence
    origin: ReviewOrigin = ReviewOrigin.MODEL

    @model_validator(mode="after")
    def validate_evidence(self) -> Decision:
        if self.origin is ReviewOrigin.MODEL and not self.evidence:
            raise DomainInvariantError(InvariantCode.DECISION_EVIDENCE)
        return self


class DateDeadline(DomainModel):
    kind: Literal[DeadlineKind.DATE] = DeadlineKind.DATE
    value: date
    timezone: TimezoneName
    source_text: MediumText
    resolution: DeadlineResolution


class DateTimeDeadline(DomainModel):
    kind: Literal[DeadlineKind.DATETIME] = DeadlineKind.DATETIME
    at: AwareDatetime
    timezone: TimezoneName
    source_text: MediumText
    resolution: DeadlineResolution


Deadline = Annotated[DateDeadline | DateTimeDeadline, Field(discriminator="kind")]


class ActionItem(DomainModel):
    id: UUID
    title: ShortText
    description: LongText | None = None
    assignee: PersonRef | None = None
    deadline: Deadline | None = None
    priority: Priority = Priority.NORMAL
    evidence: tuple[EvidenceRef, ...] = ()
    confidence: Confidence
    origin: ReviewOrigin = ReviewOrigin.MODEL

    @model_validator(mode="after")
    def validate_evidence(self) -> ActionItem:
        if self.origin is ReviewOrigin.MODEL and not self.evidence:
            raise DomainInvariantError(InvariantCode.ACTION_EVIDENCE)
        return self


class OpenQuestion(DomainModel):
    id: UUID
    question: LongText
    owner: PersonRef | None = None
    evidence: tuple[EvidenceRef, ...] = ()
    origin: ReviewOrigin = ReviewOrigin.MODEL

    @model_validator(mode="after")
    def validate_evidence(self) -> OpenQuestion:
        if self.origin is ReviewOrigin.MODEL and not self.evidence:
            raise DomainInvariantError(InvariantCode.QUESTION_EVIDENCE)
        return self


class Risk(DomainModel):
    id: UUID
    description: LongText
    owner: PersonRef | None = None
    evidence: tuple[EvidenceRef, ...] = ()
    origin: ReviewOrigin = ReviewOrigin.MODEL

    @model_validator(mode="after")
    def validate_evidence(self) -> Risk:
        if self.origin is ReviewOrigin.MODEL and not self.evidence:
            raise DomainInvariantError(InvariantCode.RISK_EVIDENCE)
        return self


class ReviewIssue(DomainModel):
    id: UUID
    item_id: UUID | None = None
    field: ShortText
    severity: IssueSeverity
    status: IssueStatus = IssueStatus.OPEN
    message: DetailedText
    resolution_note: MediumText | None = None

    @model_validator(mode="after")
    def validate_resolution(self) -> ReviewIssue:
        if self.status is IssueStatus.OPEN and self.resolution_note is not None:
            raise DomainInvariantError(InvariantCode.ISSUE_OPEN_RESOLUTION)
        if self.status is not IssueStatus.OPEN and self.resolution_note is None:
            raise DomainInvariantError(InvariantCode.ISSUE_CLOSED_RESOLUTION)
        return self


class ConnectorTarget(DomainModel):
    connector_id: ShortText
    resource_id: ShortText


class DeliveryDirective(DomainModel):
    action_item_id: UUID
    create_task: bool = True
    task_target: ConnectorTarget | None = None
    create_calendar_event: bool = False
    calendar_target: ConnectorTarget | None = None
    calendar_event_duration_minutes: int = Field(default=15, ge=5, le=1_440)

    @model_validator(mode="after")
    def validate_targets(self) -> DeliveryDirective:
        if self.create_task and self.task_target is None:
            raise DomainInvariantError(InvariantCode.TASK_TARGET_REQUIRED)
        if not self.create_task and self.task_target is not None:
            raise DomainInvariantError(InvariantCode.TASK_TARGET_DISABLED)
        if self.create_calendar_event and self.calendar_target is None:
            raise DomainInvariantError(InvariantCode.CALENDAR_TARGET_REQUIRED)
        if not self.create_calendar_event and self.calendar_target is not None:
            raise DomainInvariantError(InvariantCode.CALENDAR_TARGET_DISABLED)
        return self


class ReviewRevision(DomainModel):
    id: UUID
    meeting_id: UUID
    transcript_id: UUID
    revision_number: int = Field(ge=1)
    origin: ReviewOrigin
    purpose: DetailedText | None = None
    recap_markdown: Annotated[str, StringConstraints(min_length=1, max_length=100_000)]
    decisions: tuple[Decision, ...] = ()
    action_items: tuple[ActionItem, ...] = ()
    open_questions: tuple[OpenQuestion, ...] = ()
    risks: tuple[Risk, ...] = ()
    issues: tuple[ReviewIssue, ...] = ()
    directives: tuple[DeliveryDirective, ...] = ()
    content_digest: str = ""
    actor_id: ShortText | None = None
    created_at: AwareDatetime

    @model_validator(mode="after")
    def validate_revision(self) -> ReviewRevision:
        decision_ids = tuple(item.id for item in self.decisions)
        action_ids = tuple(item.id for item in self.action_items)
        question_ids = tuple(item.id for item in self.open_questions)
        risk_ids = tuple(item.id for item in self.risks)
        issue_ids = tuple(item.id for item in self.issues)
        directive_ids = tuple(item.action_item_id for item in self.directives)
        if len(decision_ids) != len(set(decision_ids)):
            raise DomainInvariantError(InvariantCode.DECISION_IDS)
        if len(action_ids) != len(set(action_ids)):
            raise DomainInvariantError(InvariantCode.ACTION_IDS)
        if len(question_ids) != len(set(question_ids)):
            raise DomainInvariantError(InvariantCode.QUESTION_IDS)
        if len(risk_ids) != len(set(risk_ids)):
            raise DomainInvariantError(InvariantCode.RISK_IDS)
        all_item_ids = (*decision_ids, *action_ids, *question_ids, *risk_ids)
        if len(all_item_ids) != len(set(all_item_ids)):
            raise DomainInvariantError(InvariantCode.ITEM_IDS)
        if len(issue_ids) != len(set(issue_ids)):
            raise DomainInvariantError(InvariantCode.ISSUE_IDS)
        if len(directive_ids) != len(set(directive_ids)):
            raise DomainInvariantError(InvariantCode.DIRECTIVE_IDS)
        if set(directive_ids) != set(action_ids):
            raise DomainInvariantError(InvariantCode.DIRECTIVE_COVERAGE)
        item_ids = set(decision_ids) | set(action_ids) | set(question_ids) | set(risk_ids)
        has_unknown_issue_item = any(
            issue.item_id is not None and issue.item_id not in item_ids for issue in self.issues
        )
        if has_unknown_issue_item:
            raise DomainInvariantError(InvariantCode.ISSUE_ITEM)
        actions = {item.id: item for item in self.action_items}
        for directive in self.directives:
            action = actions[directive.action_item_id]
            if directive.create_calendar_event and action.deadline is None:
                raise DomainInvariantError(InvariantCode.CALENDAR_DEADLINE)
        digest_payload = {
            "meeting_id": self.meeting_id,
            "transcript_id": self.transcript_id,
            "purpose": self.purpose,
            "recap_markdown": self.recap_markdown,
            "decisions": self.decisions,
            "action_items": self.action_items,
            "open_questions": self.open_questions,
            "risks": self.risks,
            "issues": self.issues,
            "directives": self.directives,
        }
        expected = canonical_sha256(digest_payload)
        if self.content_digest and self.content_digest != expected:
            raise DomainInvariantError(InvariantCode.REVIEW_DIGEST)
        object.__setattr__(self, "content_digest", expected)
        return self


class Approval(DomainModel):
    id: UUID
    meeting_id: UUID
    review_revision_id: UUID
    review_digest: Sha256Digest
    request_key: ShortText
    actor_id: ShortText
    approved_at: AwareDatetime


class RecapArtifact(DomainModel):
    id: UUID
    meeting_id: UUID
    approval_id: UUID
    content: Annotated[str, StringConstraints(min_length=1, max_length=100_000)]
    sha256: str = ""
    created_at: AwareDatetime

    @model_validator(mode="after")
    def validate_hash(self) -> RecapArtifact:
        expected = text_sha256(self.content)
        if self.sha256 and self.sha256 != expected:
            raise DomainInvariantError(InvariantCode.RECAP_HASH)
        object.__setattr__(self, "sha256", expected)
        return self


class TaskProposal(DomainModel):
    kind: Literal[WriteKind.TASK] = WriteKind.TASK
    source_action_id: UUID
    target: ConnectorTarget
    title: ShortText
    description: Annotated[str, StringConstraints(max_length=10_000)] = ""
    assignee: PersonRef | None = None
    deadline: Deadline | None = None
    priority: Priority = Priority.NORMAL


class CalendarEventProposal(DomainModel):
    kind: Literal[WriteKind.CALENDAR_EVENT] = WriteKind.CALENDAR_EVENT
    source_action_id: UUID
    target: ConnectorTarget
    title: ShortText
    description: Annotated[str, StringConstraints(max_length=10_000)] = ""
    deadline: Deadline
    duration_minutes: int = Field(ge=5, le=1_440)


WriteProposal = Annotated[
    TaskProposal | CalendarEventProposal,
    Field(discriminator="kind"),
]


class WriteIntent(DomainModel):
    id: UUID
    meeting_id: UUID
    approval_id: UUID
    idempotency_key: Annotated[
        str,
        StringConstraints(pattern=r"^mao_v1_[0-9a-f]{64}$"),
    ]
    proposal: WriteProposal
    payload_digest: str = ""
    status: WriteStatus = WriteStatus.PENDING
    attempt_count: int = Field(default=0, ge=0)
    next_attempt_at: AwareDatetime | None = None
    lease_owner: ShortText | None = None
    lease_expires_at: AwareDatetime | None = None
    last_failure: WorkflowFailure | None = None
    version: int = Field(default=0, ge=0)
    created_at: AwareDatetime
    updated_at: AwareDatetime

    @model_validator(mode="after")
    def validate_intent(self) -> WriteIntent:
        if self.updated_at < self.created_at:
            raise DomainInvariantError(InvariantCode.WRITE_TIMESTAMPS)
        expected = canonical_sha256(self.proposal)
        if self.payload_digest and self.payload_digest != expected:
            raise DomainInvariantError(InvariantCode.WRITE_PAYLOAD_HASH)
        object.__setattr__(self, "payload_digest", expected)
        if self.status is WriteStatus.IN_FLIGHT:
            if self.lease_owner is None or self.lease_expires_at is None:
                raise DomainInvariantError(InvariantCode.WRITE_LEASE_REQUIRED)
            if self.lease_expires_at <= self.updated_at:
                raise DomainInvariantError(InvariantCode.WRITE_LEASE_EXPIRY)
        elif self.lease_owner is not None or self.lease_expires_at is not None:
            raise DomainInvariantError(InvariantCode.WRITE_LEASE_FORBIDDEN)
        if self.status is WriteStatus.RETRY_WAIT and self.next_attempt_at is None:
            raise DomainInvariantError(InvariantCode.WRITE_RETRY_REQUIRED)
        if self.next_attempt_at is not None and self.next_attempt_at <= self.updated_at:
            raise DomainInvariantError(InvariantCode.WRITE_RETRY_EXPIRY)
        if self.status is not WriteStatus.RETRY_WAIT and self.next_attempt_at is not None:
            raise DomainInvariantError(InvariantCode.WRITE_RETRY_FORBIDDEN)
        failed_statuses = {
            WriteStatus.RETRY_WAIT,
            WriteStatus.UNKNOWN,
            WriteStatus.PERMANENT_FAILED,
        }
        if self.status in failed_statuses and self.last_failure is None:
            raise DomainInvariantError(InvariantCode.WRITE_FAILURE)
        return self


class WriteReceipt(DomainModel):
    id: UUID
    intent_id: UUID
    idempotency_key: Annotated[
        str,
        StringConstraints(pattern=r"^mao_v1_[0-9a-f]{64}$"),
    ]
    payload_digest: Sha256Digest
    provider: ShortText
    external_id: ShortText
    external_url: Annotated[str, StringConstraints(min_length=1, max_length=2_000)] | None = None
    reconciled: bool = False
    recorded_at: AwareDatetime
