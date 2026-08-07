from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID

import pytest

from meeting_action_orchestrator.application.recording_cleanup import RecordingCleanupScheduler
from meeting_action_orchestrator.domain.enums import (
    AudioMediaType,
    FailureCode,
    FailureDisposition,
    RecordingCleanupReason,
    RecordingCleanupStatus,
)
from meeting_action_orchestrator.domain.errors import RecordingCleanupConflictError
from meeting_action_orchestrator.domain.models import (
    AudioAsset,
    RecordingCleanupJob,
    WorkflowFailure,
)
from meeting_action_orchestrator.infrastructure.database import Database
from meeting_action_orchestrator.infrastructure.repositories import (
    PersistenceConflictError,
    SqliteUnitOfWork,
)

NOW = datetime(2026, 8, 7, 9, 0, tzinfo=timezone.utc)
JOB_ID = UUID("10000000-0000-4000-8000-000000000001")
ASSET_ID = UUID("20000000-0000-4000-8000-000000000001")
FIRST_KEY = "1" * 32 + ".wav"
SECOND_KEY = "2" * 32 + ".wav"
THIRD_KEY = "3" * 32 + ".wav"
FOURTH_KEY = "4" * 32 + ".wav"
FIFTH_KEY = "5" * 32 + ".wav"


class FrozenClock:
    def now(self) -> datetime:
        return NOW


class CommitThenRaiseUnitOfWork(SqliteUnitOfWork):
    def commit(self) -> None:
        super().commit()
        raise RuntimeError("cleanup commit outcome unavailable")


def failure(at: datetime, disposition: FailureDisposition) -> WorkflowFailure:
    return WorkflowFailure(
        code=FailureCode.INTERNAL,
        disposition=disposition,
        safe_message="Recording cleanup could not finish",
        occurred_at=at,
    )


def cleanup_job(
    *,
    job_id: UUID = JOB_ID,
    storage_key: str = FIRST_KEY,
    reason: RecordingCleanupReason = RecordingCleanupReason.ABANDONED_INGEST,
    status: RecordingCleanupStatus = RecordingCleanupStatus.READY,
    attempt_count: int = 0,
    max_attempts: int = 5,
    next_attempt_at: datetime | None = None,
    lease_owner: str | None = None,
    lease_expires_at: datetime | None = None,
    last_failure: WorkflowFailure | None = None,
    created_at: datetime = NOW,
    updated_at: datetime = NOW,
    completed_at: datetime | None = None,
) -> RecordingCleanupJob:
    return RecordingCleanupJob(
        id=job_id,
        storage_key=storage_key,
        expected_sha256="a" * 64,
        expected_size_bytes=16,
        reason=reason,
        status=status,
        attempt_count=attempt_count,
        max_attempts=max_attempts,
        next_attempt_at=next_attempt_at,
        lease_owner=lease_owner,
        lease_expires_at=lease_expires_at,
        last_failure=last_failure,
        created_at=created_at,
        updated_at=updated_at,
        completed_at=completed_at,
    )


def asset(storage_key: str) -> AudioAsset:
    return AudioAsset(
        id=ASSET_ID,
        storage_key=storage_key,
        original_name="recording.wav",
        detected_media_type=AudioMediaType.WAV,
        size_bytes=16,
        duration_ms=1000,
        sha256="b" * 64,
        created_at=NOW,
    )


def scheduler(
    database: Database,
    job_id: UUID = JOB_ID,
) -> RecordingCleanupScheduler:
    return RecordingCleanupScheduler(
        unit_of_work=lambda: SqliteUnitOfWork(database),
        clock=FrozenClock(),
        id_factory=lambda: job_id,
    )


def test_scheduler_binds_exact_recording_identity_and_preserves_first_reason(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "application.sqlite3")
    database.migrate()
    service = scheduler(database)

    first = service.schedule_if_unreferenced(
        storage_key=FIRST_KEY,
        expected_sha256="a" * 64,
        expected_size_bytes=16,
        reason=RecordingCleanupReason.ABANDONED_INGEST,
    )
    replay = service.schedule_if_unreferenced(
        storage_key=FIRST_KEY,
        expected_sha256="a" * 64,
        expected_size_bytes=16,
        reason=RecordingCleanupReason.ORPHAN_RECONCILIATION,
    )

    assert first is not None
    assert replay == first
    assert replay.reason is RecordingCleanupReason.ABANDONED_INGEST
    same_content_other_key = scheduler(database, UUID(int=2)).schedule_if_unreferenced(
        storage_key=SECOND_KEY,
        expected_sha256="a" * 64,
        expected_size_bytes=16,
        reason=RecordingCleanupReason.ABANDONED_INGEST,
    )
    assert same_content_other_key is not None
    assert same_content_other_key.storage_key == SECOND_KEY
    with pytest.raises(RecordingCleanupConflictError):
        service.schedule_if_unreferenced(
            storage_key=FIRST_KEY,
            expected_sha256="c" * 64,
            expected_size_bytes=16,
            reason=RecordingCleanupReason.ABANDONED_INGEST,
        )
    with pytest.raises(RecordingCleanupConflictError):
        service.schedule_if_unreferenced(
            storage_key=FIRST_KEY,
            expected_sha256="a" * 64,
            expected_size_bytes=17,
            reason=RecordingCleanupReason.ABANDONED_INGEST,
        )


def test_scheduler_does_not_bind_an_asset_owned_storage_key(tmp_path: Path) -> None:
    database = Database(tmp_path / "application.sqlite3")
    database.migrate()
    with SqliteUnitOfWork(database) as uow:
        uow.audio_assets.add(asset(FIRST_KEY))
        uow.commit()

    scheduled = scheduler(database).schedule_if_unreferenced(
        storage_key=FIRST_KEY,
        expected_sha256="a" * 64,
        expected_size_bytes=16,
        reason=RecordingCleanupReason.ABANDONED_INGEST,
    )

    assert scheduled is None
    with SqliteUnitOfWork(database, immediate=False) as uow:
        assert uow.recording_cleanups.find_by_storage_key(FIRST_KEY) is None


def test_scheduler_uses_exact_key_instead_of_content_identity(tmp_path: Path) -> None:
    database = Database(tmp_path / "application.sqlite3")
    database.migrate()
    owned = asset(FIRST_KEY).model_copy(update={"sha256": "a" * 64})
    with SqliteUnitOfWork(database) as uow:
        uow.audio_assets.add(owned)
        uow.commit()

    scheduled = scheduler(database).schedule_if_unreferenced(
        storage_key=SECOND_KEY,
        expected_sha256=owned.sha256,
        expected_size_bytes=owned.size_bytes,
        reason=RecordingCleanupReason.ABANDONED_INGEST,
    )

    assert scheduled is not None
    assert scheduled.storage_key == SECOND_KEY


def test_scheduler_commit_ambiguity_replays_the_persisted_cleanup(tmp_path: Path) -> None:
    database = Database(tmp_path / "application.sqlite3")
    database.migrate()
    ambiguous = RecordingCleanupScheduler(
        unit_of_work=lambda: CommitThenRaiseUnitOfWork(database),
        clock=FrozenClock(),
        id_factory=lambda: JOB_ID,
    )

    with pytest.raises(RuntimeError, match="outcome unavailable"):
        ambiguous.schedule_if_unreferenced(
            storage_key=FIRST_KEY,
            expected_sha256="a" * 64,
            expected_size_bytes=16,
            reason=RecordingCleanupReason.ABANDONED_INGEST,
        )

    replay = scheduler(database).schedule_if_unreferenced(
        storage_key=FIRST_KEY,
        expected_sha256="a" * 64,
        expected_size_bytes=16,
        reason=RecordingCleanupReason.ORPHAN_RECONCILIATION,
    )
    assert replay is not None
    assert replay.reason is RecordingCleanupReason.ABANDONED_INGEST


def test_storage_ownership_triggers_reject_all_insert_and_update_directions(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "application.sqlite3")
    database.migrate()
    with SqliteUnitOfWork(database) as uow:
        uow.audio_assets.add(asset(FIRST_KEY))
        uow.recording_cleanups.add(cleanup_job(storage_key=SECOND_KEY))
        uow.commit()

    with pytest.raises(sqlite3.IntegrityError), SqliteUnitOfWork(database) as uow:
        uow.recording_cleanups.add(cleanup_job(storage_key=FIRST_KEY))
    with pytest.raises(sqlite3.IntegrityError), SqliteUnitOfWork(database) as uow:
        uow.audio_assets.add(asset(SECOND_KEY).model_copy(update={"id": UUID(int=3)}))
    with pytest.raises(sqlite3.IntegrityError), database.transaction(immediate=True) as connection:
        connection.execute(
            "UPDATE recording_cleanup_jobs SET storage_key = ? WHERE storage_key = ?",
            (FIRST_KEY, SECOND_KEY),
        )
    with pytest.raises(sqlite3.IntegrityError), database.transaction(immediate=True) as connection:
        connection.execute(
            "UPDATE audio_assets SET storage_key = ? WHERE storage_key = ?",
            (SECOND_KEY, FIRST_KEY),
        )


def test_cleanup_schema_rejects_unmanaged_storage_keys(tmp_path: Path) -> None:
    database = Database(tmp_path / "application.sqlite3")
    database.migrate()
    with SqliteUnitOfWork(database) as uow:
        uow.recording_cleanups.add(cleanup_job())
        uow.commit()

    with pytest.raises(sqlite3.IntegrityError), database.transaction(immediate=True) as connection:
        connection.execute(
            "UPDATE recording_cleanup_jobs SET storage_key = ? WHERE id = ?",
            ("../recording.wav", str(JOB_ID)),
        )


def test_claim_due_handles_ready_retry_and_expired_final_attempts(tmp_path: Path) -> None:
    database = Database(tmp_path / "application.sqlite3")
    database.migrate()
    jobs = (
        cleanup_job(
            job_id=UUID(int=1),
            storage_key=FIRST_KEY,
            created_at=NOW - timedelta(minutes=4),
            updated_at=NOW - timedelta(minutes=4),
        ),
        cleanup_job(
            job_id=UUID(int=2),
            storage_key=SECOND_KEY,
            status=RecordingCleanupStatus.RETRY_WAIT,
            attempt_count=1,
            next_attempt_at=NOW,
            last_failure=failure(NOW - timedelta(minutes=2), FailureDisposition.RETRYABLE),
            created_at=NOW - timedelta(minutes=3),
            updated_at=NOW - timedelta(minutes=2),
        ),
        cleanup_job(
            job_id=UUID(int=3),
            storage_key=THIRD_KEY,
            status=RecordingCleanupStatus.RUNNING,
            attempt_count=5,
            lease_owner="crashed-worker",
            lease_expires_at=NOW,
            created_at=NOW - timedelta(minutes=5),
            updated_at=NOW - timedelta(minutes=1),
        ),
        cleanup_job(
            job_id=UUID(int=4),
            storage_key=FOURTH_KEY,
            status=RecordingCleanupStatus.RUNNING,
            attempt_count=1,
            lease_owner="live-worker",
            lease_expires_at=NOW + timedelta(minutes=1),
            created_at=NOW - timedelta(minutes=6),
            updated_at=NOW - timedelta(minutes=1),
        ),
        cleanup_job(
            job_id=UUID(int=5),
            storage_key=FIFTH_KEY,
            status=RecordingCleanupStatus.RETRY_WAIT,
            attempt_count=1,
            next_attempt_at=NOW + timedelta(minutes=1),
            last_failure=failure(NOW - timedelta(minutes=1), FailureDisposition.RETRYABLE),
            created_at=NOW - timedelta(minutes=7),
            updated_at=NOW - timedelta(minutes=1),
        ),
    )
    with SqliteUnitOfWork(database) as uow:
        for item in jobs:
            uow.recording_cleanups.add(item)
        claimed = uow.recording_cleanups.claim_due(
            " worker-a ",
            NOW,
            NOW + timedelta(minutes=2),
            3,
        )
        uow.commit()

    assert tuple(item.id for item in claimed) == (UUID(int=1), UUID(int=3), UUID(int=2))
    assert tuple(item.attempt_count for item in claimed) == (1, 5, 2)
    assert all(item.lease_owner == "worker-a" for item in claimed)
    with SqliteUnitOfWork(database, immediate=False) as uow:
        assert uow.recording_cleanups.get(UUID(int=4)) == jobs[3]
        assert uow.recording_cleanups.get(UUID(int=5)) == jobs[4]


def test_claim_due_preserves_attempt_for_expired_running_below_limit(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "application.sqlite3")
    database.migrate()
    expired = cleanup_job(
        status=RecordingCleanupStatus.RUNNING,
        attempt_count=2,
        lease_owner="crashed-worker",
        lease_expires_at=NOW,
        created_at=NOW - timedelta(minutes=2),
        updated_at=NOW - timedelta(minutes=1),
    )
    with SqliteUnitOfWork(database) as uow:
        uow.recording_cleanups.add(expired)
        reclaimed = uow.recording_cleanups.claim_due(
            "worker-b",
            NOW,
            NOW + timedelta(minutes=1),
            1,
        )[0]
        uow.commit()

    assert reclaimed.attempt_count == 2
    assert reclaimed.lease_owner == "worker-b"


def test_reclaimed_cleanup_rejects_the_stale_running_worker(tmp_path: Path) -> None:
    database = Database(tmp_path / "application.sqlite3")
    database.migrate()
    stale = cleanup_job(
        status=RecordingCleanupStatus.RUNNING,
        attempt_count=1,
        lease_owner="stale-worker",
        lease_expires_at=NOW,
        created_at=NOW - timedelta(minutes=2),
        updated_at=NOW - timedelta(minutes=1),
    )
    with SqliteUnitOfWork(database) as uow:
        uow.recording_cleanups.add(stale)
        reclaimed = uow.recording_cleanups.claim_due(
            "current-worker",
            NOW,
            NOW + timedelta(minutes=1),
            1,
        )[0]
        uow.commit()
    completed_at = NOW + timedelta(seconds=1)
    stale_completion = stale.model_copy(
        update={
            "status": RecordingCleanupStatus.SUCCEEDED,
            "lease_owner": None,
            "lease_expires_at": None,
            "updated_at": completed_at,
            "completed_at": completed_at,
        }
    )

    with pytest.raises(PersistenceConflictError), SqliteUnitOfWork(database) as uow:
        uow.recording_cleanups.save(
            stale_completion,
            stale.status,
            stale.lease_owner,
            stale.lease_expires_at,
        )
    with SqliteUnitOfWork(database, immediate=False) as uow:
        assert uow.recording_cleanups.get(JOB_ID) == reclaimed


def test_cleanup_save_rejects_stale_lease_and_preserves_immutable_identity(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "application.sqlite3")
    database.migrate()
    with SqliteUnitOfWork(database) as uow:
        original = cleanup_job()
        uow.recording_cleanups.add(original)
        claimed = uow.recording_cleanups.claim_due(
            "worker-a",
            NOW,
            NOW + timedelta(minutes=1),
            1,
        )[0]
        uow.commit()
    completed = claimed.model_copy(
        update={
            "status": RecordingCleanupStatus.SUCCEEDED,
            "lease_owner": None,
            "lease_expires_at": None,
            "updated_at": NOW + timedelta(seconds=30),
            "completed_at": NOW + timedelta(seconds=30),
        }
    )
    with SqliteUnitOfWork(database) as uow:
        uow.recording_cleanups.save(
            completed,
            claimed.status,
            claimed.lease_owner,
            claimed.lease_expires_at,
        )
        uow.commit()
    with pytest.raises(PersistenceConflictError), SqliteUnitOfWork(database) as uow:
        uow.recording_cleanups.save(
            completed,
            claimed.status,
            claimed.lease_owner,
            claimed.lease_expires_at,
        )
    with SqliteUnitOfWork(database, immediate=False) as uow:
        persisted = uow.recording_cleanups.get(JOB_ID)
    assert persisted == completed
    assert persisted.storage_key == original.storage_key
    assert persisted.expected_sha256 == original.expected_sha256


def test_expired_worker_cannot_save_after_lease_reclaim(tmp_path: Path) -> None:
    database = Database(tmp_path / "application.sqlite3")
    database.migrate()
    expired = cleanup_job(
        status=RecordingCleanupStatus.RUNNING,
        attempt_count=5,
        lease_owner="stale-worker",
        lease_expires_at=NOW,
        created_at=NOW - timedelta(minutes=2),
        updated_at=NOW - timedelta(minutes=1),
    )
    with SqliteUnitOfWork(database) as uow:
        uow.recording_cleanups.add(expired)
        reclaimed = uow.recording_cleanups.claim_due(
            "replacement-worker",
            NOW,
            NOW + timedelta(minutes=1),
            1,
        )[0]
        uow.commit()
    stale_completion = expired.model_copy(
        update={
            "status": RecordingCleanupStatus.SUCCEEDED,
            "lease_owner": None,
            "lease_expires_at": None,
            "updated_at": NOW + timedelta(seconds=1),
            "completed_at": NOW + timedelta(seconds=1),
        }
    )

    with pytest.raises(PersistenceConflictError), SqliteUnitOfWork(database) as uow:
        uow.recording_cleanups.save(
            stale_completion,
            expired.status,
            expired.lease_owner,
            expired.lease_expires_at,
        )

    assert reclaimed.attempt_count == expired.attempt_count
    assert reclaimed.lease_owner == "replacement-worker"


def test_cleanup_add_rolls_back_without_commit(tmp_path: Path) -> None:
    database = Database(tmp_path / "application.sqlite3")
    database.migrate()

    with SqliteUnitOfWork(database) as uow:
        uow.recording_cleanups.add(cleanup_job())

    with SqliteUnitOfWork(database, immediate=False) as uow:
        assert uow.recording_cleanups.get(JOB_ID) is None


def test_database_rejects_malformed_cleanup_storage_key(tmp_path: Path) -> None:
    database = Database(tmp_path / "application.sqlite3")
    database.migrate()

    with pytest.raises(sqlite3.IntegrityError), database.transaction() as connection:
        connection.execute(
            """
            INSERT INTO recording_cleanup_jobs (
                id, storage_key, expected_sha256, expected_size_bytes, reason,
                status, attempt_count, max_attempts, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(JOB_ID),
                "../recording.wav",
                "a" * 64,
                16,
                RecordingCleanupReason.ABANDONED_INGEST.value,
                RecordingCleanupStatus.READY.value,
                0,
                5,
                str(NOW),
                str(NOW),
            ),
        )
