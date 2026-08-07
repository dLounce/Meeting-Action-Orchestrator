from __future__ import annotations

from typing import Annotated, Literal, TypeAlias
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, StringConstraints

from meeting_action_orchestrator.api.contracts import WorkflowEventPageResult
from meeting_action_orchestrator.domain.enums import (
    AudioMediaType,
    FailureCode,
    FailureDisposition,
    MeetingStatus,
    ProcessingStage,
    ReviewOrigin,
    WriteKind,
    WriteStatus,
)
from meeting_action_orchestrator.domain.workflow_events import (
    AUDIT_COUNTER_MAX,
    WORKFLOW_RETRY_DELAY_MAX_MS,
    WORKFLOW_SEQUENCE_MAX,
    DeliveryChangeKind,
    DeliveryTransitionMetadata,
    MeetingIngestedMetadata,
    MeetingTransitionMetadata,
    ProcessingAttemptMetadata,
    ProcessingAuditOutcome,
    ProcessingRetryRequestedMetadata,
    ReviewApprovedMetadata,
    ReviewChangeKind,
    ReviewRevisionMetadata,
    SpecialistHandoffMetadata,
    SpecialistRole,
    WorkflowEvent,
    WorkflowEventType,
)

AuditDigest = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$", strict=True)]
AuditActorId = Annotated[
    str,
    StringConstraints(min_length=1, max_length=200, strip_whitespace=True),
]
AuditModelIdentifier = Annotated[
    str,
    StringConstraints(
        min_length=1,
        max_length=128,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$",
        strict=True,
    ),
]


class WorkflowEventApiModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class MeetingIngestedEventMetadata(WorkflowEventApiModel):
    kind: Literal["meeting-ingested/v1"] = "meeting-ingested/v1"
    recording_digest: AuditDigest
    media_type: AudioMediaType
    size_bytes: int = Field(gt=0, le=WORKFLOW_SEQUENCE_MAX)
    duration_ms: int = Field(gt=0, le=WORKFLOW_SEQUENCE_MAX)

    @classmethod
    def from_domain(cls, metadata: MeetingIngestedMetadata) -> MeetingIngestedEventMetadata:
        return cls(
            recording_digest=metadata.recording_digest,
            media_type=metadata.media_type,
            size_bytes=metadata.size_bytes,
            duration_ms=metadata.duration_ms,
        )


class MeetingTransitionEventMetadata(WorkflowEventApiModel):
    kind: Literal["meeting-transition/v1"] = "meeting-transition/v1"
    previous_status: MeetingStatus
    current_status: MeetingStatus
    meeting_version: int = Field(gt=0, le=WORKFLOW_SEQUENCE_MAX)

    @classmethod
    def from_domain(cls, metadata: MeetingTransitionMetadata) -> MeetingTransitionEventMetadata:
        return cls(
            previous_status=metadata.previous_status,
            current_status=metadata.current_status,
            meeting_version=metadata.meeting_version,
        )


class ProcessingAttemptEventMetadata(WorkflowEventApiModel):
    kind: Literal["processing-attempt/v1"] = "processing-attempt/v1"
    stage: ProcessingStage
    attempt_number: int = Field(gt=0, le=AUDIT_COUNTER_MAX)
    outcome: ProcessingAuditOutcome
    input_digest: AuditDigest | None
    output_digest: AuditDigest | None
    failure_code: FailureCode | None
    failure_disposition: FailureDisposition | None
    retry_delay_ms: int | None = Field(default=None, ge=0, le=WORKFLOW_RETRY_DELAY_MAX_MS)
    retry_exhausted: bool

    @classmethod
    def from_domain(cls, metadata: ProcessingAttemptMetadata) -> ProcessingAttemptEventMetadata:
        return cls(
            stage=metadata.stage,
            attempt_number=metadata.attempt_number,
            outcome=metadata.outcome,
            input_digest=metadata.input_digest,
            output_digest=metadata.output_digest,
            failure_code=metadata.failure_code,
            failure_disposition=metadata.failure_disposition,
            retry_delay_ms=metadata.retry_delay_ms,
            retry_exhausted=metadata.retry_exhausted,
        )


class ProcessingRetryRequestedEventMetadata(WorkflowEventApiModel):
    kind: Literal["processing-retry-requested/v1"] = "processing-retry-requested/v1"
    stage: ProcessingStage
    previous_attempt_count: int = Field(gt=0, le=AUDIT_COUNTER_MAX)
    meeting_version: int = Field(gt=0, le=WORKFLOW_SEQUENCE_MAX)

    @classmethod
    def from_domain(
        cls,
        metadata: ProcessingRetryRequestedMetadata,
    ) -> ProcessingRetryRequestedEventMetadata:
        return cls(
            stage=metadata.stage,
            previous_attempt_count=metadata.previous_attempt_count,
            meeting_version=metadata.meeting_version,
        )


class SpecialistHandoffEventMetadata(WorkflowEventApiModel):
    kind: Literal["specialist-handoff/v1"] = "specialist-handoff/v1"
    specialist: SpecialistRole
    processing_attempt_number: int = Field(gt=0, le=AUDIT_COUNTER_MAX)
    model_identifier: AuditModelIdentifier
    input_digest: AuditDigest
    output_digest: AuditDigest
    request_ids_digest: AuditDigest
    request_count: int = Field(gt=0, le=AUDIT_COUNTER_MAX)
    input_tokens: int = Field(ge=0, le=WORKFLOW_SEQUENCE_MAX)
    output_tokens: int = Field(ge=0, le=WORKFLOW_SEQUENCE_MAX)
    cached_input_tokens: int = Field(ge=0, le=WORKFLOW_SEQUENCE_MAX)
    reasoning_tokens: int = Field(ge=0, le=WORKFLOW_SEQUENCE_MAX)

    @classmethod
    def from_domain(cls, metadata: SpecialistHandoffMetadata) -> SpecialistHandoffEventMetadata:
        return cls(
            specialist=metadata.specialist,
            processing_attempt_number=metadata.processing_attempt_number,
            model_identifier=metadata.model_identifier,
            input_digest=metadata.input_digest,
            output_digest=metadata.output_digest,
            request_ids_digest=metadata.request_ids_digest,
            request_count=metadata.request_count,
            input_tokens=metadata.input_tokens,
            output_tokens=metadata.output_tokens,
            cached_input_tokens=metadata.cached_input_tokens,
            reasoning_tokens=metadata.reasoning_tokens,
        )


class ReviewRevisionEventMetadata(WorkflowEventApiModel):
    kind: Literal["review-revision/v1"] = "review-revision/v1"
    revision_number: int = Field(gt=0, le=AUDIT_COUNTER_MAX)
    review_digest: AuditDigest
    origin: ReviewOrigin
    change_kind: ReviewChangeKind
    decision_count: int = Field(ge=0, le=AUDIT_COUNTER_MAX)
    action_count: int = Field(ge=0, le=AUDIT_COUNTER_MAX)
    question_count: int = Field(ge=0, le=AUDIT_COUNTER_MAX)
    risk_count: int = Field(ge=0, le=AUDIT_COUNTER_MAX)
    issue_count: int = Field(ge=0, le=AUDIT_COUNTER_MAX)
    blocking_issue_count: int = Field(ge=0, le=AUDIT_COUNTER_MAX)

    @classmethod
    def from_domain(cls, metadata: ReviewRevisionMetadata) -> ReviewRevisionEventMetadata:
        return cls(
            revision_number=metadata.revision_number,
            review_digest=metadata.review_digest,
            origin=metadata.origin,
            change_kind=metadata.change_kind,
            decision_count=metadata.decision_count,
            action_count=metadata.action_count,
            question_count=metadata.question_count,
            risk_count=metadata.risk_count,
            issue_count=metadata.issue_count,
            blocking_issue_count=metadata.blocking_issue_count,
        )


class ReviewApprovedEventMetadata(WorkflowEventApiModel):
    kind: Literal["review-approved/v1"] = "review-approved/v1"
    revision_number: int = Field(gt=0, le=AUDIT_COUNTER_MAX)
    review_digest: AuditDigest
    write_intent_count: int = Field(ge=0, le=AUDIT_COUNTER_MAX)

    @classmethod
    def from_domain(cls, metadata: ReviewApprovedMetadata) -> ReviewApprovedEventMetadata:
        return cls(
            revision_number=metadata.revision_number,
            review_digest=metadata.review_digest,
            write_intent_count=metadata.write_intent_count,
        )


class DeliveryTransitionEventMetadata(WorkflowEventApiModel):
    kind: Literal["delivery-transition/v1"] = "delivery-transition/v1"
    change_kind: DeliveryChangeKind
    write_kind: WriteKind
    write_intent_digest: AuditDigest
    previous_status: WriteStatus | None
    current_status: WriteStatus
    attempt_count: int = Field(ge=0, le=AUDIT_COUNTER_MAX)
    reconciliation_count: int = Field(ge=0, le=AUDIT_COUNTER_MAX)
    failure_code: FailureCode | None
    failure_disposition: FailureDisposition | None

    @classmethod
    def from_domain(cls, metadata: DeliveryTransitionMetadata) -> DeliveryTransitionEventMetadata:
        return cls(
            change_kind=metadata.change_kind,
            write_kind=metadata.write_kind,
            write_intent_digest=metadata.write_intent_digest,
            previous_status=metadata.previous_status,
            current_status=metadata.current_status,
            attempt_count=metadata.attempt_count,
            reconciliation_count=metadata.reconciliation_count,
            failure_code=metadata.failure_code,
            failure_disposition=metadata.failure_disposition,
        )


WorkflowEventSafeMetadata: TypeAlias = Annotated[
    MeetingIngestedEventMetadata
    | MeetingTransitionEventMetadata
    | ProcessingAttemptEventMetadata
    | ProcessingRetryRequestedEventMetadata
    | SpecialistHandoffEventMetadata
    | ReviewRevisionEventMetadata
    | ReviewApprovedEventMetadata
    | DeliveryTransitionEventMetadata,
    Field(discriminator="kind"),
]


class WorkflowEventResponse(WorkflowEventApiModel):
    id: UUID
    meeting_id: UUID
    sequence: int = Field(gt=0, le=WORKFLOW_SEQUENCE_MAX)
    type: WorkflowEventType
    actor_id: AuditActorId | None
    safe_metadata: WorkflowEventSafeMetadata
    occurred_at: AwareDatetime

    @classmethod
    def from_domain(cls, event: WorkflowEvent) -> WorkflowEventResponse:
        return cls(
            id=event.id,
            meeting_id=event.meeting_id,
            sequence=event.sequence,
            type=event.type,
            actor_id=event.actor_id,
            safe_metadata=_metadata_from_domain(event),
            occurred_at=event.occurred_at,
        )


class WorkflowEventPageResponse(WorkflowEventApiModel):
    items: tuple[WorkflowEventResponse, ...]
    next_cursor: str | None

    @classmethod
    def from_result(
        cls,
        result: WorkflowEventPageResult,
        next_cursor: str | None,
    ) -> WorkflowEventPageResponse:
        return cls(
            items=tuple(WorkflowEventResponse.from_domain(item) for item in result.items),
            next_cursor=next_cursor,
        )


def _metadata_from_domain(event: WorkflowEvent) -> WorkflowEventSafeMetadata:
    metadata = event.safe_metadata
    if (
        event.type is WorkflowEventType.MEETING_INGESTED
        and type(metadata) is MeetingIngestedMetadata
    ):
        return MeetingIngestedEventMetadata.from_domain(metadata)
    if (
        event.type is WorkflowEventType.MEETING_TRANSITIONED
        and type(metadata) is MeetingTransitionMetadata
    ):
        return MeetingTransitionEventMetadata.from_domain(metadata)
    if (
        event.type is WorkflowEventType.PROCESSING_ATTEMPTED
        and type(metadata) is ProcessingAttemptMetadata
    ):
        return ProcessingAttemptEventMetadata.from_domain(metadata)
    if (
        event.type is WorkflowEventType.PROCESSING_RETRY_REQUESTED
        and type(metadata) is ProcessingRetryRequestedMetadata
    ):
        return ProcessingRetryRequestedEventMetadata.from_domain(metadata)
    if (
        event.type is WorkflowEventType.SPECIALIST_HANDOFF_COMPLETED
        and type(metadata) is SpecialistHandoffMetadata
    ):
        return SpecialistHandoffEventMetadata.from_domain(metadata)
    if event.type is WorkflowEventType.REVIEW_REVISED and type(metadata) is ReviewRevisionMetadata:
        return ReviewRevisionEventMetadata.from_domain(metadata)
    if event.type is WorkflowEventType.REVIEW_APPROVED and type(metadata) is ReviewApprovedMetadata:
        return ReviewApprovedEventMetadata.from_domain(metadata)
    if (
        event.type is WorkflowEventType.DELIVERY_TRANSITIONED
        and type(metadata) is DeliveryTransitionMetadata
    ):
        return DeliveryTransitionEventMetadata.from_domain(metadata)
    raise ValueError("Workflow event metadata does not match its public event type")
