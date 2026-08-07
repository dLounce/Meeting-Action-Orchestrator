from datetime import datetime, timedelta, timezone
from uuid import UUID

import pytest

from meeting_action_orchestrator.domain.enums import (
    FailureDisposition,
    MeetingErasureFailureCode,
    MeetingErasureOperation,
    MeetingErasureReason,
    MeetingErasureRecordingState,
    MeetingErasureStatus,
)
from meeting_action_orchestrator.domain.models import (
    ErasureToken,
    MeetingErasureFailure,
    MeetingErasureJob,
    MeetingErasureOperationBinding,
    MeetingErasureTombstone,
)

NOW = datetime(2026, 8, 7, 9, 0, tzinfo=timezone.utc)
JOB_ID = UUID("10000000-0000-4000-8000-000000000001")
AUDIO_ID = UUID("20000000-0000-4000-8000-000000000001")
CLEANUP_ID = UUID("30000000-0000-4000-8000-000000000001")


def erasure_failure(
    code: MeetingErasureFailureCode = MeetingErasureFailureCode.DATABASE_SANITATION_DEFERRED,
) -> MeetingErasureFailure:
    disposition = (
        FailureDisposition.RETRYABLE
        if code is MeetingErasureFailureCode.DATABASE_SANITATION_DEFERRED
        else FailureDisposition.PERMANENT
    )
    return MeetingErasureFailure(code=code, disposition=disposition, occurred_at=NOW)


def erasure_job(**updates: object) -> MeetingErasureJob:
    values: dict[str, object] = {
        "id": JOB_ID,
        "token_version": 1,
        "token_key_id": "current",
        "meeting_token": "a" * 64,
        "reason": MeetingErasureReason.USER_REQUEST,
        "erased_meeting_version": 4,
        "recording_state": MeetingErasureRecordingState.WAITING_SHARED,
        "pending_audio_asset_id": AUDIO_ID,
        "max_remediations": 3,
        "created_at": NOW,
        "updated_at": NOW,
    }
    return MeetingErasureJob.model_validate(values | updates)


def token(key_id: str, digest: str) -> ErasureToken:
    return ErasureToken(token_version=1, key_id=key_id, digest=digest * 64)


def test_erasure_failure_enforces_code_disposition_mapping() -> None:
    assert erasure_failure().disposition is FailureDisposition.RETRYABLE
    assert (
        erasure_failure(MeetingErasureFailureCode.RECORDING_CLEANUP_REJECTED).disposition
        is FailureDisposition.PERMANENT
    )
    with pytest.raises(ValueError, match="failure disposition is invalid"):
        MeetingErasureFailure(
            code=MeetingErasureFailureCode.ERASURE_INTEGRITY_FAILED,
            disposition=FailureDisposition.RETRYABLE,
            occurred_at=NOW,
        )


@pytest.mark.parametrize(
    "updates",
    [
        {"pending_audio_asset_id": None},
        {"cleanup_job_id": CLEANUP_ID},
        {
            "recording_state": MeetingErasureRecordingState.CLEANUP_PENDING,
            "pending_audio_asset_id": AUDIO_ID,
        },
        {
            "recording_state": MeetingErasureRecordingState.REMOVED,
            "pending_audio_asset_id": None,
            "cleanup_job_id": CLEANUP_ID,
        },
    ],
)
def test_erasure_job_enforces_recording_resource_identity(updates: dict[str, object]) -> None:
    with pytest.raises(ValueError, match="inconsistent resources"):
        erasure_job(**updates)


def test_checkpoint_and_terminal_states_are_exact() -> None:
    failure = erasure_failure(MeetingErasureFailureCode.RECORDING_CLEANUP_REJECTED)
    active_failure = erasure_job(
        recording_state=MeetingErasureRecordingState.FAILED,
        pending_audio_asset_id=None,
        cleanup_job_id=CLEANUP_ID,
        last_failure=failure,
    )
    assert active_failure.status is MeetingErasureStatus.ACTIVE
    with pytest.raises(ValueError, match="aggregate state"):
        erasure_job(
            recording_state=MeetingErasureRecordingState.FAILED,
            pending_audio_asset_id=None,
            cleanup_job_id=CLEANUP_ID,
            last_failure=failure,
            database_checkpointed_at=NOW,
        )
    failed = erasure_job(
        status=MeetingErasureStatus.FAILED,
        recording_state=MeetingErasureRecordingState.FAILED,
        pending_audio_asset_id=None,
        cleanup_job_id=CLEANUP_ID,
        database_checkpointed_at=NOW,
        last_failure=failure,
        completed_at=NOW,
    )
    assert failed.database_checkpointed_at == NOW
    completed = erasure_job(
        status=MeetingErasureStatus.COMPLETED,
        recording_state=MeetingErasureRecordingState.REMOVED,
        pending_audio_asset_id=None,
        database_checkpointed_at=NOW,
        completed_at=NOW,
    )
    assert completed.status is MeetingErasureStatus.COMPLETED


def test_checkpoint_retry_and_lease_are_mutually_exclusive() -> None:
    retry_at = NOW + timedelta(minutes=1)
    with pytest.raises(ValueError, match="retry state"):
        erasure_job(
            next_attempt_at=retry_at,
            lease_owner="worker",
            lease_expires_at=retry_at,
        )
    with pytest.raises(ValueError, match="retry state"):
        erasure_job(next_attempt_at=retry_at, database_checkpointed_at=NOW)
    with pytest.raises(ValueError, match="failure state"):
        erasure_job(
            database_checkpointed_at=NOW,
            last_failure=erasure_failure(),
        )


def test_checkpoint_retry_count_is_unbounded_but_remediation_is_bounded() -> None:
    assert erasure_job(retry_count=1_000_000).retry_count == 1_000_000
    with pytest.raises(ValueError, match="remediation count exceeds"):
        erasure_job(remediation_count=4)


def test_operation_factory_binds_actor_resource_and_expected_version() -> None:
    request = token("current", "a")
    actor = token("current", "b")
    resource = token("current", "c")
    binding = MeetingErasureOperationBinding.create(
        request,
        actor,
        resource,
        JOB_ID,
        MeetingErasureOperation.REQUEST,
        4,
        NOW,
    )

    assert binding.expected_version == 4
    assert "a" * 64 not in repr(binding)
    payload = binding.model_dump(mode="python")
    payload["actor_token"] = "d" * 64
    with pytest.raises(ValueError, match="fingerprint is invalid"):
        MeetingErasureOperationBinding.model_validate(payload)
    with pytest.raises(ValueError, match="key identities are inconsistent"):
        MeetingErasureOperationBinding.create(
            request,
            token("old", "b"),
            resource,
            JOB_ID,
            MeetingErasureOperation.REQUEST,
            4,
            NOW,
        )


def test_tombstone_factory_rejects_mixed_key_identities() -> None:
    meeting_token = token("current", "a")
    ingest_key_token = token("current", "b")
    tombstone = MeetingErasureTombstone.create(
        JOB_ID,
        meeting_token,
        ingest_key_token,
        NOW,
    )

    assert tombstone.token_key_id == meeting_token.key_id
    with pytest.raises(ValueError, match="key identities are inconsistent"):
        MeetingErasureTombstone.create(
            JOB_ID,
            token("current", "a"),
            token("old", "b"),
            NOW,
        )
