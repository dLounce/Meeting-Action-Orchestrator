from datetime import datetime, timedelta, timezone
from uuid import UUID

import pytest
from pydantic import ValidationError

from meeting_action_orchestrator.application.state_machine import (
    can_transition_meeting,
    derive_filing_status,
    transition_meeting,
    transition_write_intent,
)
from meeting_action_orchestrator.domain import (
    ConnectorTarget,
    FailureCode,
    FailureDisposition,
    InvalidMeetingTransitionError,
    InvalidWriteTransitionError,
    Meeting,
    MeetingStatus,
    TaskProposal,
    WorkflowFailure,
    WriteIntent,
    WriteStatus,
)

NOW = datetime(2026, 6, 7, 9, 0, tzinfo=timezone.utc)


def uid(value: int) -> UUID:
    return UUID(int=value)


def failure(disposition: FailureDisposition) -> WorkflowFailure:
    code = {
        FailureDisposition.RETRYABLE: FailureCode.PROVIDER_UNAVAILABLE,
        FailureDisposition.PERMANENT: FailureCode.CONNECTOR_REJECTED,
        FailureDisposition.UNKNOWN_OUTCOME: FailureCode.UNKNOWN_REMOTE_OUTCOME,
    }[disposition]
    return WorkflowFailure(
        code=code,
        disposition=disposition,
        safe_message="Provider operation failed",
        occurred_at=NOW,
    )


def make_meeting(status: MeetingStatus = MeetingStatus.INGESTED) -> Meeting:
    transcript_id = None
    review_id = None
    approved_review_id = None
    meeting_failure = None
    if status in {
        MeetingStatus.TRANSCRIBED,
        MeetingStatus.EXTRACTING,
        MeetingStatus.EXTRACTION_FAILED,
        MeetingStatus.AWAITING_APPROVAL,
        MeetingStatus.APPROVED,
        MeetingStatus.FILING,
        MeetingStatus.PARTIALLY_FILED,
        MeetingStatus.FILING_FAILED,
        MeetingStatus.COMPLETED,
    }:
        transcript_id = uid(10)
    if status in {
        MeetingStatus.AWAITING_APPROVAL,
        MeetingStatus.APPROVED,
        MeetingStatus.FILING,
        MeetingStatus.PARTIALLY_FILED,
        MeetingStatus.FILING_FAILED,
        MeetingStatus.COMPLETED,
    }:
        review_id = uid(30)
    if status in {
        MeetingStatus.APPROVED,
        MeetingStatus.FILING,
        MeetingStatus.PARTIALLY_FILED,
        MeetingStatus.FILING_FAILED,
        MeetingStatus.COMPLETED,
    }:
        approved_review_id = review_id
    if status in {
        MeetingStatus.TRANSCRIPTION_FAILED,
        MeetingStatus.EXTRACTION_FAILED,
        MeetingStatus.PARTIALLY_FILED,
        MeetingStatus.FILING_FAILED,
    }:
        meeting_failure = failure(FailureDisposition.PERMANENT)
    return Meeting(
        id=uid(1),
        ingest_key="upload-1",
        title="Launch planning",
        audio_asset_id=uid(2),
        timezone="UTC",
        status=status,
        current_transcript_id=transcript_id,
        current_review_id=review_id,
        approved_review_id=approved_review_id,
        failure=meeting_failure,
        created_at=NOW,
        updated_at=NOW,
    )


def make_intent(status: WriteStatus = WriteStatus.PENDING) -> WriteIntent:
    retry_at = NOW + timedelta(seconds=30) if status is WriteStatus.RETRY_WAIT else None
    lease_owner = "worker-1" if status is WriteStatus.IN_FLIGHT else None
    lease_until = NOW + timedelta(minutes=1) if status is WriteStatus.IN_FLIGHT else None
    last_failure = None
    if status is WriteStatus.RETRY_WAIT:
        last_failure = failure(FailureDisposition.RETRYABLE)
    if status is WriteStatus.UNKNOWN:
        last_failure = failure(FailureDisposition.UNKNOWN_OUTCOME)
    if status is WriteStatus.PERMANENT_FAILED:
        last_failure = failure(FailureDisposition.PERMANENT)
    return WriteIntent(
        id=uid(60),
        meeting_id=uid(1),
        approval_id=uid(40),
        idempotency_key=f"mao_v1_{'a' * 64}",
        proposal=TaskProposal(
            source_action_id=uid(20),
            target=ConnectorTarget(connector_id="tasks", resource_id="inbox"),
            title="Send launch brief",
        ),
        status=status,
        next_attempt_at=retry_at,
        lease_owner=lease_owner,
        lease_expires_at=lease_until,
        last_failure=last_failure,
        created_at=NOW,
        updated_at=NOW,
    )


def test_meeting_transition_updates_version_and_transcript() -> None:
    meeting = make_meeting(MeetingStatus.TRANSCRIBING)

    transitioned = transition_meeting(
        meeting,
        MeetingStatus.TRANSCRIBED,
        NOW + timedelta(seconds=1),
        transcript_id=uid(10),
    )

    assert transitioned.status is MeetingStatus.TRANSCRIBED
    assert transitioned.current_transcript_id == uid(10)
    assert transitioned.version == 1


def test_terminal_meeting_state_rejects_transition() -> None:
    meeting = make_meeting(MeetingStatus.COMPLETED)

    assert not can_transition_meeting(meeting.status, MeetingStatus.FILING)
    with pytest.raises(InvalidMeetingTransitionError):
        transition_meeting(meeting, MeetingStatus.FILING, NOW + timedelta(seconds=1))


def test_failed_meeting_transition_requires_failure() -> None:
    meeting = make_meeting(MeetingStatus.TRANSCRIBING)

    with pytest.raises(ValidationError, match="failed status requires a failure"):
        transition_meeting(
            meeting,
            MeetingStatus.TRANSCRIPTION_FAILED,
            NOW + timedelta(seconds=1),
        )


def test_write_claim_sets_lease_and_increments_attempt() -> None:
    intent = make_intent()

    claimed = transition_write_intent(
        intent,
        WriteStatus.IN_FLIGHT,
        NOW + timedelta(seconds=1),
        lease_owner="worker-1",
        lease_expires_at=NOW + timedelta(minutes=1),
    )

    assert claimed.attempt_count == 1
    assert claimed.lease_owner == "worker-1"


def test_unknown_write_requires_reconciliation_before_retry() -> None:
    intent = make_intent(WriteStatus.IN_FLIGHT)
    uncertain = transition_write_intent(
        intent,
        WriteStatus.UNKNOWN,
        NOW + timedelta(seconds=1),
        failure=failure(FailureDisposition.UNKNOWN_OUTCOME),
    )

    assert uncertain.status is WriteStatus.UNKNOWN
    with pytest.raises(InvalidWriteTransitionError):
        transition_write_intent(
            uncertain,
            WriteStatus.IN_FLIGHT,
            NOW + timedelta(seconds=2),
            lease_owner="worker-2",
            lease_expires_at=NOW + timedelta(minutes=2),
        )


@pytest.mark.parametrize(
    ("statuses", "expected"),
    [
        ((), MeetingStatus.COMPLETED),
        ((WriteStatus.PENDING,), MeetingStatus.FILING),
        ((WriteStatus.SUCCEEDED,), MeetingStatus.COMPLETED),
        (
            (WriteStatus.SUCCEEDED, WriteStatus.PERMANENT_FAILED),
            MeetingStatus.PARTIALLY_FILED,
        ),
        ((WriteStatus.PERMANENT_FAILED,), MeetingStatus.FILING_FAILED),
        ((WriteStatus.UNKNOWN,), MeetingStatus.FILING_FAILED),
    ],
)
def test_filing_status_is_derived_from_intents(
    statuses: tuple[WriteStatus, ...],
    expected: MeetingStatus,
) -> None:
    intents = tuple(
        make_intent(status).model_copy(update={"id": uid(60 + index)})
        for index, status in enumerate(statuses)
    )

    assert derive_filing_status(intents, recap_ready=True) is expected
