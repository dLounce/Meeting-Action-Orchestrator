from __future__ import annotations

from datetime import datetime
from typing import Protocol, TypeVar
from uuid import UUID

from meeting_action_orchestrator.agents.contracts import AgentResult, StrictModel
from meeting_action_orchestrator.domain.enums import (
    FailureDisposition,
    IssueSeverity,
    IssueStatus,
    WriteStatus,
)
from meeting_action_orchestrator.domain.hashing import canonical_sha256
from meeting_action_orchestrator.domain.models import (
    Meeting,
    ProcessingJob,
    ReviewRevision,
    WorkflowFailure,
    WriteIntent,
)
from meeting_action_orchestrator.domain.workflow_events import (
    DeliveryChangeKind,
    DeliveryTransitionMetadata,
    MeetingTransitionMetadata,
    ProcessingAttemptMetadata,
    ProcessingAuditOutcome,
    ProcessingRetryRequestedMetadata,
    ReviewChangeKind,
    ReviewRevisionMetadata,
    SpecialistHandoffMetadata,
    SpecialistRole,
    WorkflowEventDraft,
    WorkflowEventType,
    workflow_request_ids_digest,
    workflow_retry_delay_ms,
    workflow_write_intent_digest,
)


class WorkflowEventSink(Protocol):
    def append(self, draft: WorkflowEventDraft) -> object: ...


OutputT = TypeVar("OutputT", bound=StrictModel)


def append_meeting_transition(
    sink: WorkflowEventSink,
    previous: Meeting,
    current: Meeting,
    occurred_at: datetime,
    *,
    actor_id: str | None = None,
) -> None:
    sink.append(
        WorkflowEventDraft(
            meeting_id=current.id,
            type=WorkflowEventType.MEETING_TRANSITIONED,
            actor_id=actor_id,
            safe_metadata=MeetingTransitionMetadata(
                previous_status=previous.status,
                current_status=current.status,
                meeting_version=current.version,
            ),
            occurred_at=occurred_at,
        )
    )


def append_processing_attempt(
    sink: WorkflowEventSink,
    job: ProcessingJob,
    outcome: ProcessingAuditOutcome,
    input_digest: str | None,
    occurred_at: datetime,
    *,
    output_digest: str | None = None,
    failure: WorkflowFailure | None = None,
    retry_at: datetime | None = None,
) -> None:
    sink.append(
        WorkflowEventDraft(
            meeting_id=job.meeting_id,
            type=WorkflowEventType.PROCESSING_ATTEMPTED,
            safe_metadata=ProcessingAttemptMetadata(
                stage=job.stage,
                attempt_number=job.attempt_count,
                outcome=outcome,
                input_digest=input_digest,
                output_digest=output_digest,
                failure_code=failure.code if failure is not None else None,
                failure_disposition=(failure.disposition if failure is not None else None),
                retry_delay_ms=(
                    workflow_retry_delay_ms(retry_at - occurred_at)
                    if retry_at is not None
                    else None
                ),
                retry_exhausted=(
                    outcome is ProcessingAuditOutcome.FAILED
                    and failure is not None
                    and failure.disposition is FailureDisposition.RETRYABLE
                ),
            ),
            occurred_at=occurred_at,
        )
    )


def append_processing_retry_requested(
    sink: WorkflowEventSink,
    job: ProcessingJob,
    meeting: Meeting,
    occurred_at: datetime,
    actor_id: str,
) -> None:
    sink.append(
        WorkflowEventDraft(
            meeting_id=meeting.id,
            type=WorkflowEventType.PROCESSING_RETRY_REQUESTED,
            actor_id=actor_id,
            safe_metadata=ProcessingRetryRequestedMetadata(
                stage=job.stage,
                previous_attempt_count=job.attempt_count,
                meeting_version=meeting.version,
            ),
            occurred_at=occurred_at,
        )
    )


def append_review_revision(
    sink: WorkflowEventSink,
    review: ReviewRevision,
    change_kind: ReviewChangeKind,
    occurred_at: datetime,
    *,
    actor_id: str | None = None,
) -> None:
    sink.append(
        WorkflowEventDraft(
            meeting_id=review.meeting_id,
            type=WorkflowEventType.REVIEW_REVISED,
            actor_id=actor_id,
            safe_metadata=review_revision_metadata(review, change_kind),
            occurred_at=occurred_at,
        )
    )


def review_revision_metadata(
    review: ReviewRevision,
    change_kind: ReviewChangeKind,
) -> ReviewRevisionMetadata:
    return ReviewRevisionMetadata(
        revision_number=review.revision_number,
        review_digest=review.content_digest,
        origin=review.origin,
        change_kind=change_kind,
        decision_count=len(review.decisions),
        action_count=len(review.action_items),
        question_count=len(review.open_questions),
        risk_count=len(review.risks),
        issue_count=len(review.issues),
        blocking_issue_count=sum(
            issue.severity is IssueSeverity.BLOCKING and issue.status is IssueStatus.OPEN
            for issue in review.issues
        ),
    )


def specialist_handoff_draft(
    *,
    meeting_id: UUID,
    specialist: SpecialistRole,
    processing_attempt_number: int,
    request: StrictModel,
    result: AgentResult[OutputT],
    occurred_at: datetime,
) -> WorkflowEventDraft:
    return WorkflowEventDraft(
        meeting_id=meeting_id,
        type=WorkflowEventType.SPECIALIST_HANDOFF_COMPLETED,
        safe_metadata=SpecialistHandoffMetadata(
            specialist=specialist,
            processing_attempt_number=processing_attempt_number,
            model_identifier=result.model,
            input_digest=canonical_sha256(request),
            output_digest=canonical_sha256(result.output),
            request_ids_digest=workflow_request_ids_digest(result.workflow_request_ids),
            request_count=result.usage.requests,
            input_tokens=result.usage.input_tokens,
            output_tokens=result.usage.output_tokens,
            cached_input_tokens=result.usage.cached_input_tokens,
            reasoning_tokens=result.usage.reasoning_tokens,
        ),
        occurred_at=occurred_at,
    )


def append_delivery_transition(
    sink: WorkflowEventSink,
    previous: WriteIntent | WriteStatus | None,
    current: WriteIntent,
    occurred_at: datetime,
    *,
    change_kind: DeliveryChangeKind = DeliveryChangeKind.STATUS_TRANSITION,
    actor_id: str | None = None,
) -> None:
    failure = current.last_failure
    sink.append(
        WorkflowEventDraft(
            meeting_id=current.meeting_id,
            type=WorkflowEventType.DELIVERY_TRANSITIONED,
            actor_id=actor_id,
            safe_metadata=DeliveryTransitionMetadata(
                change_kind=change_kind,
                write_kind=current.proposal.kind,
                write_intent_digest=workflow_write_intent_digest(current.id),
                previous_status=(
                    previous.status if isinstance(previous, WriteIntent) else previous
                ),
                current_status=current.status,
                attempt_count=current.attempt_count,
                reconciliation_count=(
                    current.reconcile_attempt_count
                    if change_kind is DeliveryChangeKind.RECONCILIATION_REFRESH
                    else 0
                ),
                failure_code=failure.code if failure is not None else None,
                failure_disposition=(failure.disposition if failure is not None else None),
            ),
            occurred_at=occurred_at,
        )
    )
