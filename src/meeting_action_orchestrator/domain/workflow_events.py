from __future__ import annotations

import re
from collections.abc import Sequence
from datetime import timedelta
from enum import Enum
from math import ceil, isfinite
from typing import Annotated, Literal, TypeAlias, cast
from uuid import UUID

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    model_validator,
)

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
from meeting_action_orchestrator.domain.hashing import canonical_sha256
from meeting_action_orchestrator.domain.transition_rules import (
    can_transition_meeting,
    can_transition_write,
)

_AUDIT_INTEGER_MAX = 9_223_372_036_854_775_807
WORKFLOW_SEQUENCE_MAX = _AUDIT_INTEGER_MAX
AUDIT_COUNTER_MAX = 1_000_000_000
WORKFLOW_RETRY_DELAY_MAX_MS = 604_800_000
AuditActorId = Annotated[
    str,
    StringConstraints(min_length=1, max_length=200, strip_whitespace=True),
]
AuditDigest = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$", strict=True)]
AuditModelIdentifier = Annotated[
    str,
    StringConstraints(
        min_length=1,
        max_length=128,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$",
        strict=True,
    ),
]


class WorkflowEventType(str, Enum):
    MEETING_INGESTED = "meeting_ingested"
    MEETING_TRANSITIONED = "meeting_transitioned"
    PROCESSING_ATTEMPTED = "processing_attempted"
    PROCESSING_RETRY_REQUESTED = "processing_retry_requested"
    SPECIALIST_HANDOFF_COMPLETED = "specialist_handoff_completed"
    REVIEW_REVISED = "review_revised"
    REVIEW_APPROVED = "review_approved"
    DELIVERY_TRANSITIONED = "delivery_transitioned"


class ProcessingAuditOutcome(str, Enum):
    STARTED = "started"
    SUCCEEDED = "succeeded"
    RETRY_SCHEDULED = "retry_scheduled"
    FAILED = "failed"


class SpecialistRole(str, Enum):
    EXTRACT = "extract"
    RECAP = "recap"
    VERIFY = "verify"


class ReviewChangeKind(str, Enum):
    MODEL_CREATED = "model_created"
    ACTION_EDITED = "action_edited"
    ISSUE_UPDATED = "issue_updated"
    DELIVERY_UPDATED = "delivery_updated"


class DeliveryChangeKind(str, Enum):
    CREATED = "created"
    STATUS_TRANSITION = "status_transition"
    RECONCILIATION_REFRESH = "reconciliation_refresh"


class SafeWorkflowEventMetadata(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        hide_input_in_errors=True,
    )

    @model_validator(mode="after")
    def validate_scalar_projection(self) -> SafeWorkflowEventMetadata:
        values = self.model_dump(mode="python")
        if any(
            value is not None and not isinstance(value, (str, int, bool, Enum))
            for value in values.values()
        ):
            raise ValueError("Workflow event metadata must contain only safe scalar values")
        return self


class MeetingIngestedMetadata(SafeWorkflowEventMetadata):
    kind: Literal["meeting-ingested/v1"] = "meeting-ingested/v1"
    recording_digest: AuditDigest
    media_type: AudioMediaType
    size_bytes: int = Field(gt=0, le=_AUDIT_INTEGER_MAX)
    duration_ms: int = Field(gt=0, le=_AUDIT_INTEGER_MAX)


class MeetingTransitionMetadata(SafeWorkflowEventMetadata):
    kind: Literal["meeting-transition/v1"] = "meeting-transition/v1"
    previous_status: MeetingStatus
    current_status: MeetingStatus
    meeting_version: int = Field(gt=0, le=_AUDIT_INTEGER_MAX)

    @model_validator(mode="after")
    def validate_transition(self) -> MeetingTransitionMetadata:
        if not can_transition_meeting(self.previous_status, self.current_status):
            raise ValueError("Workflow event meeting transition is invalid")
        return self


class ProcessingAttemptMetadata(SafeWorkflowEventMetadata):
    kind: Literal["processing-attempt/v1"] = "processing-attempt/v1"
    stage: ProcessingStage
    attempt_number: int = Field(gt=0, le=AUDIT_COUNTER_MAX)
    outcome: ProcessingAuditOutcome
    input_digest: AuditDigest | None = None
    output_digest: AuditDigest | None = None
    failure_code: FailureCode | None = None
    failure_disposition: FailureDisposition | None = None
    retry_delay_ms: int | None = Field(default=None, ge=0, le=WORKFLOW_RETRY_DELAY_MAX_MS)
    retry_exhausted: bool = False

    @model_validator(mode="after")
    def validate_outcome(self) -> ProcessingAttemptMetadata:
        failed = self.outcome in {
            ProcessingAuditOutcome.RETRY_SCHEDULED,
            ProcessingAuditOutcome.FAILED,
        }
        if failed != (self.failure_code is not None and self.failure_disposition is not None):
            raise ValueError("Processing audit outcome has inconsistent failure metadata")
        if (self.failure_code is None) != (self.failure_disposition is None):
            raise ValueError("Processing audit failure metadata must be paired")
        if self.outcome is not ProcessingAuditOutcome.SUCCEEDED and self.output_digest is not None:
            raise ValueError("Only successful processing audit events carry an output digest")
        retry = self.outcome is ProcessingAuditOutcome.RETRY_SCHEDULED
        if retry != (self.retry_delay_ms is not None):
            raise ValueError("Retry audit events require a bounded delay")
        if retry and self.failure_disposition is not FailureDisposition.RETRYABLE:
            raise ValueError("Retry audit events require a retryable failure")
        exhausted = (
            self.outcome is ProcessingAuditOutcome.FAILED
            and self.failure_disposition is FailureDisposition.RETRYABLE
        )
        if self.retry_exhausted != exhausted:
            raise ValueError("Retry exhaustion must identify a terminal retryable failure")
        if (
            self.outcome is ProcessingAuditOutcome.FAILED
            and not self.retry_exhausted
            and self.failure_disposition
            not in {
                FailureDisposition.PERMANENT,
                FailureDisposition.UNKNOWN_OUTCOME,
            }
        ):
            raise ValueError("Terminal processing audit events require a terminal failure")
        return self


class ProcessingRetryRequestedMetadata(SafeWorkflowEventMetadata):
    kind: Literal["processing-retry-requested/v1"] = "processing-retry-requested/v1"
    stage: ProcessingStage
    previous_attempt_count: int = Field(gt=0, le=AUDIT_COUNTER_MAX)
    meeting_version: int = Field(gt=0, le=_AUDIT_INTEGER_MAX)


class SpecialistHandoffMetadata(SafeWorkflowEventMetadata):
    kind: Literal["specialist-handoff/v1"] = "specialist-handoff/v1"
    specialist: SpecialistRole
    processing_attempt_number: int = Field(gt=0, le=AUDIT_COUNTER_MAX)
    model_identifier: AuditModelIdentifier
    input_digest: AuditDigest
    output_digest: AuditDigest
    request_ids_digest: AuditDigest
    request_count: int = Field(gt=0, le=AUDIT_COUNTER_MAX)
    input_tokens: int = Field(ge=0, le=_AUDIT_INTEGER_MAX)
    output_tokens: int = Field(ge=0, le=_AUDIT_INTEGER_MAX)
    cached_input_tokens: int = Field(default=0, ge=0, le=_AUDIT_INTEGER_MAX)
    reasoning_tokens: int = Field(default=0, ge=0, le=_AUDIT_INTEGER_MAX)

    @model_validator(mode="after")
    def validate_usage(self) -> SpecialistHandoffMetadata:
        if self.cached_input_tokens > self.input_tokens:
            raise ValueError("Cached input tokens cannot exceed input tokens")
        if self.reasoning_tokens > self.output_tokens:
            raise ValueError("Reasoning tokens cannot exceed output tokens")
        return self


class ReviewRevisionMetadata(SafeWorkflowEventMetadata):
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

    @model_validator(mode="after")
    def validate_issue_counts(self) -> ReviewRevisionMetadata:
        if self.blocking_issue_count > self.issue_count:
            raise ValueError("Blocking review issues cannot exceed all review issues")
        model_created = self.change_kind is ReviewChangeKind.MODEL_CREATED
        if model_created != (self.origin is ReviewOrigin.MODEL):
            raise ValueError("Model-created review audit metadata requires model origin")
        return self


class ReviewApprovedMetadata(SafeWorkflowEventMetadata):
    kind: Literal["review-approved/v1"] = "review-approved/v1"
    revision_number: int = Field(gt=0, le=AUDIT_COUNTER_MAX)
    review_digest: AuditDigest
    write_intent_count: int = Field(ge=0, le=AUDIT_COUNTER_MAX)


class DeliveryTransitionMetadata(SafeWorkflowEventMetadata):
    kind: Literal["delivery-transition/v1"] = "delivery-transition/v1"
    change_kind: DeliveryChangeKind
    write_kind: WriteKind
    write_intent_digest: AuditDigest
    previous_status: WriteStatus | None = None
    current_status: WriteStatus
    attempt_count: int = Field(ge=0, le=AUDIT_COUNTER_MAX)
    reconciliation_count: int = Field(ge=0, le=AUDIT_COUNTER_MAX)
    failure_code: FailureCode | None = None
    failure_disposition: FailureDisposition | None = None

    @model_validator(mode="after")
    def validate_transition(self) -> DeliveryTransitionMetadata:
        created = (
            self.previous_status is None
            and self.current_status is WriteStatus.PENDING
            and self.attempt_count == 0
            and self.reconciliation_count == 0
            and self.failure_code is None
            and self.failure_disposition is None
        )
        transitioned = self.previous_status is not None and can_transition_write(
            self.previous_status,
            self.current_status,
        )
        refreshed = (
            self.previous_status is WriteStatus.UNKNOWN
            and self.current_status is WriteStatus.UNKNOWN
            and self.reconciliation_count > 0
            and self.failure_code is not None
            and self.failure_disposition is FailureDisposition.UNKNOWN_OUTCOME
        )
        expected = {
            DeliveryChangeKind.CREATED: created,
            DeliveryChangeKind.STATUS_TRANSITION: transitioned,
            DeliveryChangeKind.RECONCILIATION_REFRESH: refreshed,
        }
        if not expected[self.change_kind]:
            raise ValueError("Workflow event delivery transition is invalid")
        if self.change_kind is not DeliveryChangeKind.CREATED and self.attempt_count == 0:
            raise ValueError("Workflow event delivery attempt count is invalid")
        if (self.reconciliation_count > 0) != (
            self.change_kind is DeliveryChangeKind.RECONCILIATION_REFRESH
        ):
            raise ValueError("Workflow event delivery reconciliation count is invalid")
        if (self.failure_code is None) != (self.failure_disposition is None):
            raise ValueError("Delivery audit failure metadata must be paired")
        expected = {
            WriteStatus.RETRY_WAIT: FailureDisposition.RETRYABLE,
            WriteStatus.UNKNOWN: FailureDisposition.UNKNOWN_OUTCOME,
            WriteStatus.PERMANENT_FAILED: FailureDisposition.PERMANENT,
        }.get(self.current_status)
        if expected is None and self.failure_disposition is not None:
            raise ValueError("Successful delivery audit transitions cannot carry failures")
        if expected is not None and self.failure_disposition is not expected:
            raise ValueError("Delivery audit transition has an invalid failure disposition")
        return self


WorkflowEventMetadata: TypeAlias = Annotated[
    MeetingIngestedMetadata
    | MeetingTransitionMetadata
    | ProcessingAttemptMetadata
    | ProcessingRetryRequestedMetadata
    | SpecialistHandoffMetadata
    | ReviewRevisionMetadata
    | ReviewApprovedMetadata
    | DeliveryTransitionMetadata,
    Field(discriminator="kind"),
]

_METADATA_TYPES: dict[WorkflowEventType, type[SafeWorkflowEventMetadata]] = {
    WorkflowEventType.MEETING_INGESTED: MeetingIngestedMetadata,
    WorkflowEventType.MEETING_TRANSITIONED: MeetingTransitionMetadata,
    WorkflowEventType.PROCESSING_ATTEMPTED: ProcessingAttemptMetadata,
    WorkflowEventType.PROCESSING_RETRY_REQUESTED: ProcessingRetryRequestedMetadata,
    WorkflowEventType.SPECIALIST_HANDOFF_COMPLETED: SpecialistHandoffMetadata,
    WorkflowEventType.REVIEW_REVISED: ReviewRevisionMetadata,
    WorkflowEventType.REVIEW_APPROVED: ReviewApprovedMetadata,
    WorkflowEventType.DELIVERY_TRANSITIONED: DeliveryTransitionMetadata,
}


class WorkflowEventDraft(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        hide_input_in_errors=True,
    )

    meeting_id: UUID
    type: WorkflowEventType
    actor_id: AuditActorId | None = Field(default=None, exclude=True, repr=False)
    safe_metadata: WorkflowEventMetadata
    occurred_at: AwareDatetime

    @model_validator(mode="after")
    def validate_metadata_type(self) -> WorkflowEventDraft:
        if type(self.safe_metadata) is not _METADATA_TYPES[self.type]:
            raise ValueError("Workflow event metadata does not match its event type")
        return self


class WorkflowEvent(WorkflowEventDraft):
    id: UUID
    sequence: int = Field(gt=0, le=WORKFLOW_SEQUENCE_MAX)


def parse_workflow_event_metadata(
    event_type: WorkflowEventType,
    payload: str,
) -> WorkflowEventMetadata:
    return cast(
        WorkflowEventMetadata,
        _METADATA_TYPES[event_type].model_validate_json(payload),
    )


def workflow_request_ids_digest(request_ids: Sequence[str]) -> str:
    if isinstance(request_ids, (str, bytes, bytearray)) or not 1 <= len(request_ids) <= 100:
        raise ValueError("Workflow request ID count must be between 1 and 100")
    normalized: list[str] = []
    for request_id in request_ids:
        if (
            not isinstance(request_id, str)
            or not 1 <= len(request_id) <= 200
            or re.fullmatch(r"[!-~]+", request_id) is None
        ):
            raise ValueError("Workflow request IDs must be bounded printable ASCII")
        normalized.append(request_id)
    return canonical_sha256(
        {
            "schema": "workflow-request-ids/v1",
            "request_ids": normalized,
        }
    )


def workflow_write_intent_digest(write_intent_id: UUID) -> str:
    if not isinstance(write_intent_id, UUID):
        raise ValueError("Workflow write intent identity must be a UUID")
    return canonical_sha256(
        {
            "schema": "workflow-write-intent/v1",
            "write_intent_id": write_intent_id,
        }
    )


def workflow_retry_delay_ms(delta: timedelta) -> int:
    if isinstance(delta, bool) or not isinstance(delta, timedelta):
        raise ValueError("Workflow retry delay must be a finite bounded duration")
    seconds = delta.total_seconds()
    if not isfinite(seconds) or seconds < 0:
        raise ValueError("Workflow retry delay must be a finite bounded duration")
    milliseconds = ceil(seconds * 1_000)
    if milliseconds > WORKFLOW_RETRY_DELAY_MAX_MS:
        raise ValueError("Workflow retry delay must be a finite bounded duration")
    return milliseconds
