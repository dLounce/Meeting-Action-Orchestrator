from datetime import datetime, timedelta, timezone
from uuid import UUID

import pytest

from meeting_action_orchestrator.domain.enums import (
    FailureCode,
    FailureDisposition,
    RecordingCleanupReason,
    RecordingCleanupStatus,
)
from meeting_action_orchestrator.domain.models import RecordingCleanupJob, WorkflowFailure

NOW = datetime(2026, 8, 7, 9, 0, tzinfo=timezone.utc)


def failure(disposition: FailureDisposition = FailureDisposition.RETRYABLE) -> WorkflowFailure:
    return WorkflowFailure(
        code=FailureCode.INTERNAL,
        disposition=disposition,
        safe_message="Recording cleanup could not finish",
        occurred_at=NOW,
    )


def job(**updates: object) -> RecordingCleanupJob:
    values: dict[str, object] = {
        "id": UUID("10000000-0000-4000-8000-000000000001"),
        "storage_key": "1" * 32 + ".wav",
        "expected_sha256": "a" * 64,
        "expected_size_bytes": 0,
        "reason": RecordingCleanupReason.ABANDONED_INGEST,
        "max_attempts": 5,
        "created_at": NOW,
        "updated_at": NOW,
    }
    return RecordingCleanupJob.model_validate(values | updates)


@pytest.mark.parametrize(
    "storage_key",
    [
        "0" * 32 + ".wav",
        "1" * 32 + ".mp3",
        "2" * 32 + ".m4a",
        "." + "3" * 32 + ".part",
    ],
)
def test_cleanup_job_accepts_generated_storage_keys(storage_key: str) -> None:
    assert job(storage_key=storage_key).storage_key == storage_key


@pytest.mark.parametrize(
    "storage_key",
    [
        "../recording.wav",
        "folder/" + "1" * 32 + ".wav",
        "folder\\" + "1" * 32 + ".wav",
        "A" * 32 + ".wav",
        "1" * 31 + ".wav",
        "1" * 32 + ".flac",
        "legacy.wav",
        ".",
        "\x00" + "1" * 32 + ".wav",
    ],
)
def test_cleanup_job_rejects_unowned_storage_key_shapes(storage_key: str) -> None:
    with pytest.raises(ValueError, match="generated recording key"):
        job(storage_key=storage_key)


def test_cleanup_job_enforces_attempt_and_timestamp_limits() -> None:
    with pytest.raises(ValueError, match="attempt count exceeds"):
        job(attempt_count=6)
    with pytest.raises(ValueError, match="timestamps are inconsistent"):
        job(updated_at=NOW - timedelta(seconds=1))


def test_running_cleanup_requires_a_future_paired_lease() -> None:
    with pytest.raises(ValueError, match="requires a lease"):
        job(status=RecordingCleanupStatus.RUNNING)
    with pytest.raises(ValueError, match="expire after"):
        job(
            status=RecordingCleanupStatus.RUNNING,
            lease_owner="worker",
            lease_expires_at=NOW,
        )
    running = job(
        status=RecordingCleanupStatus.RUNNING,
        attempt_count=1,
        lease_owner="worker",
        lease_expires_at=NOW + timedelta(minutes=1),
    )
    assert running.attempt_count == 1


def test_retrying_cleanup_requires_schedule_and_retryable_failure() -> None:
    with pytest.raises(ValueError, match="requires a schedule"):
        job(status=RecordingCleanupStatus.RETRY_WAIT, last_failure=failure())
    with pytest.raises(ValueError, match="requires a retryable failure"):
        job(
            status=RecordingCleanupStatus.RETRY_WAIT,
            next_attempt_at=NOW + timedelta(minutes=1),
            last_failure=failure(FailureDisposition.PERMANENT),
        )
    retrying = job(
        status=RecordingCleanupStatus.RETRY_WAIT,
        attempt_count=1,
        next_attempt_at=NOW + timedelta(minutes=1),
        last_failure=failure(),
    )
    assert retrying.status is RecordingCleanupStatus.RETRY_WAIT


def test_active_cleanup_rejects_failure_retry_and_completion_state() -> None:
    with pytest.raises(ValueError, match="cannot retain a failure"):
        job(last_failure=failure())
    with pytest.raises(ValueError, match="can have a schedule"):
        job(next_attempt_at=NOW + timedelta(minutes=1))
    with pytest.raises(ValueError, match="cannot be completed"):
        job(completed_at=NOW)


def test_failed_cleanup_requires_failure_and_completion() -> None:
    with pytest.raises(ValueError, match="requires a failure"):
        job(status=RecordingCleanupStatus.FAILED, completed_at=NOW)
    with pytest.raises(ValueError, match="requires a completion time"):
        job(
            status=RecordingCleanupStatus.FAILED,
            last_failure=failure(FailureDisposition.PERMANENT),
        )
    failed = job(
        status=RecordingCleanupStatus.FAILED,
        attempt_count=5,
        last_failure=failure(FailureDisposition.PERMANENT),
        completed_at=NOW,
    )
    assert failed.completed_at == NOW


def test_successful_cleanup_requires_consistent_completion_time() -> None:
    with pytest.raises(ValueError, match="requires a completion time"):
        job(status=RecordingCleanupStatus.SUCCEEDED)
    with pytest.raises(ValueError, match="timestamps are inconsistent"):
        job(
            status=RecordingCleanupStatus.SUCCEEDED,
            updated_at=NOW + timedelta(seconds=1),
            completed_at=NOW,
        )
