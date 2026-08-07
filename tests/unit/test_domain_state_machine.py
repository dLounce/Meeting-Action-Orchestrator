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
    DeliveryOperationBinding,
    DeliveryOperationKind,
    DeliveryOperationStatus,
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
    reconcile_at = NOW if status is WriteStatus.UNKNOWN else None
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
        next_reconcile_at=reconcile_at,
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
    assert uncertain.next_reconcile_at == NOW + timedelta(seconds=1)
    assert uncertain.reconcile_attempt_count == 0
    with pytest.raises(InvalidWriteTransitionError):
        transition_write_intent(
            uncertain,
            WriteStatus.IN_FLIGHT,
            NOW + timedelta(seconds=2),
            lease_owner="worker-2",
            lease_expires_at=NOW + timedelta(minutes=2),
        )


@pytest.mark.parametrize(
    "updates",
    [
        {"next_reconcile_at": None},
        {"next_reconcile_at": NOW - timedelta(microseconds=1)},
    ],
)
def test_unknown_write_requires_a_current_reconciliation_schedule(
    updates: dict[str, object],
) -> None:
    unknown = make_intent(WriteStatus.UNKNOWN)

    with pytest.raises(ValidationError, match="reconciliation"):
        WriteIntent.model_validate(unknown.model_dump(mode="python") | updates)


def test_reconciliation_state_is_forbidden_outside_unknown_status() -> None:
    pending = make_intent()

    with pytest.raises(ValidationError, match="only an unknown write"):
        WriteIntent.model_validate(
            pending.model_dump(mode="python")
            | {
                "next_reconcile_at": NOW + timedelta(seconds=1),
                "reconcile_attempt_count": 1,
            }
        )


@pytest.mark.parametrize(
    "updates",
    [
        {"reconcile_lease_owner": "reconciler"},
        {"reconcile_lease_expires_at": NOW + timedelta(minutes=1)},
        {
            "reconcile_lease_owner": "reconciler",
            "reconcile_lease_expires_at": NOW,
        },
    ],
)
def test_unknown_reconciliation_lease_requires_paired_live_fields(
    updates: dict[str, object],
) -> None:
    unknown = make_intent(WriteStatus.UNKNOWN)

    with pytest.raises(ValidationError, match="reconciliation lease"):
        WriteIntent.model_validate(unknown.model_dump(mode="python") | updates)


def test_leaving_unknown_clears_reconciliation_lease() -> None:
    unknown = WriteIntent.model_validate(
        make_intent(WriteStatus.UNKNOWN).model_dump(mode="python")
        | {
            "reconcile_lease_owner": "reconciler",
            "reconcile_lease_expires_at": NOW + timedelta(minutes=1),
        }
    )

    retrying = transition_write_intent(
        unknown,
        WriteStatus.RETRY_WAIT,
        NOW + timedelta(seconds=1),
        failure=failure(FailureDisposition.RETRYABLE),
        next_attempt_at=NOW + timedelta(seconds=31),
    )

    assert retrying.reconcile_lease_owner is None
    assert retrying.reconcile_lease_expires_at is None


@pytest.mark.parametrize(
    "updates",
    [
        {"status": DeliveryOperationStatus.RUNNING},
        {
            "lease_owner": "operation-worker",
            "lease_expires_at": NOW + timedelta(minutes=1),
        },
        {
            "status": DeliveryOperationStatus.COMPLETED,
            "completed_at": None,
        },
    ],
)
def test_delivery_operation_lifecycle_fields_are_consistent(
    updates: dict[str, object],
) -> None:
    completed = DeliveryOperationBinding(
        request_key="delivery-operation",
        meeting_id=uid(1),
        operation=DeliveryOperationKind.RECONCILE,
        actor_id="owner",
        selection_fingerprint="a" * 64,
        status=DeliveryOperationStatus.COMPLETED,
        completed_at=NOW,
        created_at=NOW,
        updated_at=NOW,
    )

    with pytest.raises(ValidationError, match="delivery operation"):
        DeliveryOperationBinding.model_validate(completed.model_dump(mode="python") | updates)


def test_leaving_unknown_clears_reconciliation_state() -> None:
    unknown = make_intent(WriteStatus.UNKNOWN).model_copy(update={"reconcile_attempt_count": 3})

    retrying = transition_write_intent(
        unknown,
        WriteStatus.RETRY_WAIT,
        NOW + timedelta(seconds=1),
        failure=failure(FailureDisposition.RETRYABLE),
        next_attempt_at=NOW + timedelta(seconds=31),
    )

    assert retrying.next_reconcile_at is None
    assert retrying.reconcile_attempt_count == 0


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
        ((WriteStatus.UNKNOWN,), MeetingStatus.FILING),
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
