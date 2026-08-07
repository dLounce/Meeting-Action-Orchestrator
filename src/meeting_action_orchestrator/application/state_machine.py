from __future__ import annotations

from datetime import datetime
from uuid import UUID

from meeting_action_orchestrator.domain.enums import (
    FailureDisposition,
    MeetingStatus,
    WriteStatus,
)
from meeting_action_orchestrator.domain.errors import (
    DomainInvariantError,
    InvalidMeetingTransitionError,
    InvalidWriteTransitionError,
    InvariantCode,
)
from meeting_action_orchestrator.domain.models import Meeting, WorkflowFailure, WriteIntent
from meeting_action_orchestrator.domain.transition_rules import (
    MEETING_TRANSITIONS,
    WRITE_TRANSITIONS,
    can_transition_meeting,
    can_transition_write,
)

__all__ = [
    "MEETING_TRANSITIONS",
    "WRITE_TRANSITIONS",
    "can_transition_meeting",
    "can_transition_write",
    "derive_filing_status",
    "transition_meeting",
    "transition_write_intent",
]


def transition_meeting(
    meeting: Meeting,
    target: MeetingStatus,
    at: datetime,
    *,
    failure: WorkflowFailure | None = None,
    transcript_id: UUID | None = None,
    review_id: UUID | None = None,
    approved_review_id: UUID | None = None,
) -> Meeting:
    if not can_transition_meeting(meeting.status, target):
        raise InvalidMeetingTransitionError(meeting.status, target)
    if at < meeting.updated_at:
        raise DomainInvariantError(InvariantCode.TRANSITION_TIMESTAMP)
    updates = {
        "status": target,
        "failure": failure,
        "version": meeting.version + 1,
        "updated_at": at,
        "current_transcript_id": transcript_id or meeting.current_transcript_id,
        "current_review_id": review_id or meeting.current_review_id,
        "approved_review_id": approved_review_id or meeting.approved_review_id,
    }
    payload = meeting.model_dump(mode="python") | updates
    return Meeting.model_validate(payload)


def transition_write_intent(
    intent: WriteIntent,
    target: WriteStatus,
    at: datetime,
    *,
    failure: WorkflowFailure | None = None,
    next_attempt_at: datetime | None = None,
    next_reconcile_at: datetime | None = None,
    lease_owner: str | None = None,
    lease_expires_at: datetime | None = None,
) -> WriteIntent:
    if not can_transition_write(intent.status, target):
        raise InvalidWriteTransitionError(intent.status, target)
    if at < intent.updated_at:
        raise DomainInvariantError(InvariantCode.TRANSITION_TIMESTAMP)
    if target is WriteStatus.RETRY_WAIT and (
        failure is None or failure.disposition is not FailureDisposition.RETRYABLE
    ):
        raise DomainInvariantError(InvariantCode.RETRY_DISPOSITION)
    if target is WriteStatus.UNKNOWN and (
        failure is None or failure.disposition is not FailureDisposition.UNKNOWN_OUTCOME
    ):
        raise DomainInvariantError(InvariantCode.UNKNOWN_DISPOSITION)
    if target is WriteStatus.PERMANENT_FAILED and (
        failure is None or failure.disposition is not FailureDisposition.PERMANENT
    ):
        raise DomainInvariantError(InvariantCode.PERMANENT_DISPOSITION)
    if target is WriteStatus.IN_FLIGHT and (lease_owner is None or lease_expires_at is None):
        raise DomainInvariantError(InvariantCode.TRANSITION_LEASE)
    updates = {
        "status": target,
        "last_failure": failure,
        "next_attempt_at": next_attempt_at if target is WriteStatus.RETRY_WAIT else None,
        "next_reconcile_at": (next_reconcile_at or at if target is WriteStatus.UNKNOWN else None),
        "reconcile_attempt_count": 0,
        "reconcile_lease_owner": None,
        "reconcile_lease_expires_at": None,
        "lease_owner": lease_owner if target is WriteStatus.IN_FLIGHT else None,
        "lease_expires_at": lease_expires_at if target is WriteStatus.IN_FLIGHT else None,
        "attempt_count": intent.attempt_count + (1 if target is WriteStatus.IN_FLIGHT else 0),
        "version": intent.version + 1,
        "updated_at": at,
    }
    payload = intent.model_dump(mode="python") | updates
    return WriteIntent.model_validate(payload)


def derive_filing_status(
    intents: tuple[WriteIntent, ...],
    *,
    recap_ready: bool,
) -> MeetingStatus:
    if not recap_ready:
        raise DomainInvariantError(InvariantCode.RECAP_MISSING)
    if not intents:
        return MeetingStatus.COMPLETED
    statuses = {intent.status for intent in intents}
    active = {
        WriteStatus.PENDING,
        WriteStatus.IN_FLIGHT,
        WriteStatus.RETRY_WAIT,
        WriteStatus.UNKNOWN,
    }
    if statuses & active:
        return MeetingStatus.FILING
    if statuses == {WriteStatus.SUCCEEDED}:
        return MeetingStatus.COMPLETED
    if WriteStatus.SUCCEEDED in statuses:
        return MeetingStatus.PARTIALLY_FILED
    return MeetingStatus.FILING_FAILED
