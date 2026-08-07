from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID

import pytest

from meeting_action_orchestrator.domain.enums import (
    FailureCode,
    FailureDisposition,
    MeetingErasureFailureCode,
    MeetingErasureOperation,
    MeetingErasureReason,
    MeetingErasureRecordingState,
    MeetingErasureStatus,
    RecordingCleanupReason,
    RecordingCleanupStatus,
)
from meeting_action_orchestrator.domain.models import (
    ErasureTokenIdentity,
    MeetingErasureFailure,
    MeetingErasureJob,
    MeetingErasureOperationBinding,
    MeetingErasureTombstone,
    RecordingCleanupJob,
    WorkflowFailure,
)
from meeting_action_orchestrator.infrastructure.database import Database
from meeting_action_orchestrator.infrastructure.erasure_tokens import (
    ErasureKeyVerificationError,
    ErasureTokenKeyring,
)
from meeting_action_orchestrator.infrastructure.repositories import (
    PersistenceConflictError,
    PersistenceIntegrityError,
    SqliteUnitOfWork,
)

NOW = datetime(2026, 8, 7, 9, 0, tzinfo=timezone.utc)
OLD_SECRET = b"o" * 32
NEW_SECRET = b"n" * 32


def migrated_database(tmp_path: Path) -> Database:
    database = Database(tmp_path / "application.sqlite3")
    database.migrate()
    return database


def register(database: Database, keyring: ErasureTokenKeyring) -> None:
    with SqliteUnitOfWork(database) as uow:
        for verifier in keyring.verifiers(NOW):
            if uow.erasure_key_verifiers.get(verifier.key_id) is None:
                uow.erasure_key_verifiers.add(verifier)
        keyring.validate_verifiers(
            uow.erasure_key_verifiers.list_all(),
            uow.erasure_key_verifiers.list_referenced_tokens(),
        )
        uow.commit()


def test_verifier_repository_canonicalizes_year_one_timestamp(tmp_path: Path) -> None:
    database = migrated_database(tmp_path)
    keyring = ErasureTokenKeyring("current", {"current": NEW_SECRET})
    created_at = datetime(1, 1, 1, tzinfo=timezone.utc)
    verifier = keyring.verifier("current", created_at)
    with SqliteUnitOfWork(database) as uow:
        uow.erasure_key_verifiers.add(verifier)
        uow.commit()
    with database.connect() as connection:
        stored = connection.execute(
            "SELECT created_at FROM erasure_key_verifiers WHERE key_id = 'current'"
        ).fetchone()[0]
    with SqliteUnitOfWork(database, immediate=False) as uow:
        loaded = uow.erasure_key_verifiers.get("current")

    assert stored == "0001-01-01T00:00:00.000000+00:00"
    assert loaded == verifier


def erasure_job(
    keyring: ErasureTokenKeyring,
    number: int,
    **updates: object,
) -> MeetingErasureJob:
    meeting_id = UUID(int=number)
    token = keyring.meeting_token(meeting_id)
    values: dict[str, object] = {
        "id": UUID(int=10_000 + number),
        "token_version": token.token_version,
        "token_key_id": token.key_id,
        "meeting_token": token.digest,
        "reason": MeetingErasureReason.USER_REQUEST,
        "erased_meeting_version": number,
        "recording_state": MeetingErasureRecordingState.WAITING_SHARED,
        "pending_audio_asset_id": UUID(int=20_000 + number),
        "max_remediations": 3,
        "created_at": NOW - timedelta(minutes=10),
        "updated_at": NOW,
    }
    return MeetingErasureJob.model_validate(values | updates)


def cleanup_job(
    number: int,
    status: RecordingCleanupStatus = RecordingCleanupStatus.READY,
) -> RecordingCleanupJob:
    terminal = status in {RecordingCleanupStatus.SUCCEEDED, RecordingCleanupStatus.FAILED}
    failure = None
    if status is RecordingCleanupStatus.FAILED:
        failure = WorkflowFailure(
            code=FailureCode.INTERNAL,
            disposition=FailureDisposition.PERMANENT,
            safe_message="Recording cleanup could not finish",
            occurred_at=NOW,
        )
    return RecordingCleanupJob(
        id=UUID(int=30_000 + number),
        storage_key=f"{number:032x}.wav",
        expected_sha256=f"{number % 16:x}" * 64,
        expected_size_bytes=16,
        reason=RecordingCleanupReason.MEETING_ERASURE,
        status=status,
        attempt_count=5 if status is RecordingCleanupStatus.FAILED else int(terminal),
        max_attempts=5,
        last_failure=failure,
        created_at=NOW - timedelta(minutes=10),
        updated_at=NOW,
        completed_at=NOW if terminal else None,
    )


def retryable_failure(at: datetime = NOW) -> MeetingErasureFailure:
    return MeetingErasureFailure(
        code=MeetingErasureFailureCode.DATABASE_SANITATION_DEFERRED,
        disposition=FailureDisposition.RETRYABLE,
        occurred_at=at,
    )


def permanent_failure(at: datetime = NOW) -> MeetingErasureFailure:
    return MeetingErasureFailure(
        code=MeetingErasureFailureCode.RECORDING_CLEANUP_REJECTED,
        disposition=FailureDisposition.PERMANENT,
        occurred_at=at,
    )


def test_erasure_records_replay_after_maintenance_key_cutover(tmp_path: Path) -> None:
    database = migrated_database(tmp_path)
    old = ErasureTokenKeyring("old", {"old": OLD_SECRET})
    register(database, old)
    meeting_id = UUID(int=1)
    job = erasure_job(old, 1)
    meeting_token = old.meeting_token(meeting_id)
    ingest_token = old.ingest_key_token("ingest-1")
    request_token = old.request_key_token("request-1")
    binding = MeetingErasureOperationBinding.create(
        request_token,
        old.actor_token("actor-1"),
        meeting_token,
        job.id,
        MeetingErasureOperation.REQUEST,
        job.erased_meeting_version,
        NOW,
    )
    tombstone = MeetingErasureTombstone.create(job.id, meeting_token, ingest_token, NOW)
    with SqliteUnitOfWork(database) as uow:
        uow.meeting_erasures.add(job)
        uow.meeting_erasure_tombstones.add(tombstone)
        uow.meeting_erasure_operations.add(binding)
        uow.commit()

    rotated = ErasureTokenKeyring("new", {"new": NEW_SECRET, "old": OLD_SECRET})
    register(database, rotated)
    with SqliteUnitOfWork(database, immediate=False) as uow:
        references = uow.erasure_key_verifiers.list_referenced_tokens()
        rotated.validate_verifiers(uow.erasure_key_verifiers.list_all(), references)
        loaded_job = uow.meeting_erasures.find_by_meeting_tokens(rotated.meeting_tokens(meeting_id))
        loaded_tombstone = uow.meeting_erasure_tombstones.find_by_ingest_key_tokens(
            rotated.ingest_key_tokens("ingest-1")
        )
        loaded_binding = uow.meeting_erasure_operations.find_by_request_tokens(
            rotated.request_key_tokens("request-1")
        )

    assert loaded_job == job
    assert loaded_tombstone == tombstone
    assert loaded_binding == binding
    assert references == (
        ErasureTokenIdentity(
            token_version=meeting_token.token_version,
            key_id=meeting_token.key_id,
        ),
    )


def test_same_request_key_under_two_rotation_keys_fails_as_integrity_error(
    tmp_path: Path,
) -> None:
    database = migrated_database(tmp_path)
    keyring = ErasureTokenKeyring("new", {"new": NEW_SECRET, "old": OLD_SECRET})
    register(database, keyring)
    secrets = {"new": NEW_SECRET, "old": OLD_SECRET}
    with SqliteUnitOfWork(database) as uow:
        for number, key_id in ((1, "old"), (2, "new")):
            scoped = ErasureTokenKeyring(key_id, {key_id: secrets[key_id]})
            job = erasure_job(scoped, number)
            uow.meeting_erasures.add(job)
            uow.meeting_erasure_operations.add(
                MeetingErasureOperationBinding.create(
                    scoped.request_key_token("same-request"),
                    scoped.actor_token("actor"),
                    scoped.meeting_token(UUID(int=number)),
                    job.id,
                    MeetingErasureOperation.REQUEST,
                    job.erased_meeting_version,
                    NOW,
                )
            )
        uow.commit()

    with (
        SqliteUnitOfWork(database, immediate=False) as uow,
        pytest.raises(PersistenceIntegrityError, match="multiple bindings"),
    ):
        uow.meeting_erasure_operations.find_by_request_tokens(
            keyring.request_key_tokens("same-request")
        )


def test_claim_actionable_respects_checkpoint_cleanup_schedule_and_expired_lease(
    tmp_path: Path,
) -> None:
    database = migrated_database(tmp_path)
    keyring = ErasureTokenKeyring("current", {"current": NEW_SECRET})
    register(database, keyring)
    ready_cleanup = cleanup_job(1)
    succeeded_cleanup = cleanup_job(2, RecordingCleanupStatus.SUCCEEDED)
    jobs = (
        erasure_job(keyring, 1),
        erasure_job(keyring, 2, database_checkpointed_at=NOW),
        erasure_job(
            keyring,
            3,
            recording_state=MeetingErasureRecordingState.CLEANUP_PENDING,
            pending_audio_asset_id=None,
            cleanup_job_id=ready_cleanup.id,
            database_checkpointed_at=NOW,
        ),
        erasure_job(
            keyring,
            4,
            recording_state=MeetingErasureRecordingState.CLEANUP_PENDING,
            pending_audio_asset_id=None,
            cleanup_job_id=succeeded_cleanup.id,
            database_checkpointed_at=NOW,
        ),
        erasure_job(keyring, 5, next_attempt_at=NOW + timedelta(minutes=1)),
        erasure_job(
            keyring,
            6,
            created_at=NOW - timedelta(minutes=10),
            updated_at=NOW - timedelta(minutes=2),
            lease_owner="stopped-worker",
            lease_expires_at=NOW - timedelta(minutes=1),
        ),
    )
    with SqliteUnitOfWork(database) as uow:
        uow.recording_cleanups.add(ready_cleanup)
        uow.recording_cleanups.add(succeeded_cleanup)
        for job in jobs:
            uow.meeting_erasures.add(job)
        uow.commit()

    with SqliteUnitOfWork(database) as uow:
        claimed = uow.meeting_erasures.claim_actionable(
            "worker",
            NOW,
            NOW + timedelta(minutes=5),
            10,
        )
        uow.commit()

    assert {job.id for job in claimed} == {jobs[0].id, jobs[3].id, jobs[5].id}
    assert all(job.retry_count == 0 for job in claimed)
    assert all(job.version == 1 for job in claimed)
    assert all(job.lease_owner == "worker" for job in claimed)


def test_claim_actionable_honors_limit_and_rejects_backwards_clock(tmp_path: Path) -> None:
    database = migrated_database(tmp_path)
    keyring = ErasureTokenKeyring("current", {"current": NEW_SECRET})
    register(database, keyring)
    jobs = tuple(erasure_job(keyring, number) for number in range(1, 4))
    with SqliteUnitOfWork(database) as uow:
        for job in jobs:
            uow.meeting_erasures.add(job)
        uow.commit()

    with SqliteUnitOfWork(database) as uow:
        claimed = uow.meeting_erasures.claim_actionable(
            "worker",
            NOW,
            NOW + timedelta(minutes=1),
            2,
        )
        uow.commit()
    assert len(claimed) == 2
    with SqliteUnitOfWork(database) as uow:
        assert (
            uow.meeting_erasures.claim_actionable(
                "worker",
                NOW - timedelta(minutes=1),
                NOW + timedelta(minutes=1),
                10,
            )
            == ()
        )


def test_claim_actionable_compares_offset_timestamps_by_instant(tmp_path: Path) -> None:
    database = migrated_database(tmp_path)
    keyring = ErasureTokenKeyring("current", {"current": NEW_SECRET})
    register(database, keyring)
    offset = timezone(timedelta(hours=5, minutes=30))
    offset_updated = datetime(2026, 8, 7, 13, 0, tzinfo=offset)
    jobs = (
        erasure_job(
            keyring,
            1,
            created_at=offset_updated - timedelta(minutes=10),
            updated_at=offset_updated,
            lease_owner="stopped-worker",
            lease_expires_at=datetime(2026, 8, 7, 14, 0, tzinfo=offset),
        ),
        erasure_job(
            keyring,
            2,
            created_at=offset_updated - timedelta(minutes=10),
            updated_at=offset_updated,
            next_attempt_at=datetime(2026, 8, 7, 14, 0, tzinfo=offset),
            last_failure=retryable_failure(offset_updated),
        ),
    )
    with SqliteUnitOfWork(database) as uow:
        for job in jobs:
            uow.meeting_erasures.add(job)
        uow.commit()
    with database.connect() as connection:
        stored = connection.execute(
            """
            SELECT created_at, updated_at, next_attempt_at, lease_expires_at
            FROM meeting_erasure_jobs ORDER BY id
            """
        ).fetchall()

    assert {row["created_at"] for row in stored} == {"2026-08-07T07:20:00.000000+00:00"}
    assert {row["updated_at"] for row in stored} == {"2026-08-07T07:30:00.000000+00:00"}
    assert stored[0]["lease_expires_at"] == "2026-08-07T08:30:00.000000+00:00"
    assert stored[1]["next_attempt_at"] == "2026-08-07T08:30:00.000000+00:00"

    with SqliteUnitOfWork(database) as uow:
        claimed = uow.meeting_erasures.claim_actionable(
            "worker",
            NOW,
            NOW + timedelta(minutes=1),
            2,
        )
        uow.commit()

    assert tuple(job.id for job in claimed) == tuple(job.id for job in jobs)


def test_separate_connections_cannot_claim_the_same_job(tmp_path: Path) -> None:
    database = migrated_database(tmp_path)
    keyring = ErasureTokenKeyring("current", {"current": NEW_SECRET})
    register(database, keyring)
    job = erasure_job(keyring, 1)
    with SqliteUnitOfWork(database) as uow:
        uow.meeting_erasures.add(job)
        uow.commit()

    with SqliteUnitOfWork(database) as first:
        first_claim = first.meeting_erasures.claim_actionable(
            "first",
            NOW,
            NOW + timedelta(minutes=2),
            1,
        )
        first.commit()
    with SqliteUnitOfWork(database) as second:
        second_claim = second.meeting_erasures.claim_actionable(
            "second",
            NOW,
            NOW + timedelta(minutes=2),
            1,
        )

    assert len(first_claim) == 1
    assert second_claim == ()


def test_save_enforces_immutable_identity_counter_steps_and_timestamp_order(
    tmp_path: Path,
) -> None:
    database = migrated_database(tmp_path)
    keyring = ErasureTokenKeyring("current", {"current": NEW_SECRET})
    register(database, keyring)
    job = erasure_job(keyring, 1)
    with SqliteUnitOfWork(database) as uow:
        uow.meeting_erasures.add(job)
        uow.commit()

    invalid_updates = (
        {"reason": MeetingErasureReason.RETENTION, "version": 1},
        {"retry_count": 2, "version": 1},
        {"updated_at": NOW - timedelta(minutes=1), "version": 1},
        {"max_remediations": 4, "version": 1},
    )
    for updates in invalid_updates:
        candidate = MeetingErasureJob.model_validate(job.model_dump(mode="python") | updates)
        with pytest.raises(PersistenceConflictError), SqliteUnitOfWork(database) as uow:
            uow.meeting_erasures.save(candidate, 0, None, None)

    retrying = MeetingErasureJob.model_validate(
        job.model_dump(mode="python")
        | {
            "retry_count": 1,
            "next_attempt_at": NOW + timedelta(minutes=1),
            "last_failure": retryable_failure(),
            "version": 1,
        }
    )
    with SqliteUnitOfWork(database) as uow:
        uow.meeting_erasures.save(retrying, 0, None, None)
        uow.commit()
    with SqliteUnitOfWork(database, immediate=False) as uow:
        assert uow.meeting_erasures.get(job.id) == retrying


def test_save_rejects_stale_version_and_lease(tmp_path: Path) -> None:
    database = migrated_database(tmp_path)
    keyring = ErasureTokenKeyring("current", {"current": NEW_SECRET})
    register(database, keyring)
    job = erasure_job(keyring, 1)
    with SqliteUnitOfWork(database) as uow:
        uow.meeting_erasures.add(job)
        uow.commit()
    with SqliteUnitOfWork(database) as uow:
        claimed = uow.meeting_erasures.claim_actionable(
            "worker",
            NOW,
            NOW + timedelta(minutes=1),
            1,
        )[0]
        uow.commit()
    candidate = MeetingErasureJob.model_validate(
        claimed.model_dump(mode="python")
        | {
            "lease_owner": None,
            "lease_expires_at": None,
            "next_attempt_at": NOW + timedelta(minutes=2),
            "last_failure": retryable_failure(),
            "retry_count": 1,
            "version": 2,
        }
    )

    with pytest.raises(PersistenceConflictError), SqliteUnitOfWork(database) as uow:
        uow.meeting_erasures.save(candidate, 0, "worker", claimed.lease_expires_at)
    with pytest.raises(PersistenceConflictError), SqliteUnitOfWork(database) as uow:
        uow.meeting_erasures.save(candidate, 1, "other-worker", claimed.lease_expires_at)


def test_cleanup_fanout_and_permanent_failure_round_trip(tmp_path: Path) -> None:
    database = migrated_database(tmp_path)
    keyring = ErasureTokenKeyring("current", {"current": NEW_SECRET})
    register(database, keyring)
    cleanup = cleanup_job(1, RecordingCleanupStatus.FAILED)
    jobs = tuple(
        erasure_job(
            keyring,
            number,
            status=MeetingErasureStatus.FAILED,
            recording_state=MeetingErasureRecordingState.FAILED,
            pending_audio_asset_id=None,
            cleanup_job_id=cleanup.id,
            database_checkpointed_at=NOW,
            last_failure=permanent_failure(),
            completed_at=NOW,
        )
        for number in (1, 2)
    )
    with SqliteUnitOfWork(database) as uow:
        uow.recording_cleanups.add(cleanup)
        for job in jobs:
            uow.meeting_erasures.add(job)
        uow.commit()

    with SqliteUnitOfWork(database, immediate=False) as uow:
        linked = uow.meeting_erasures.list_by_cleanup_job_id(cleanup.id)

    assert linked == jobs


def test_active_recording_failure_is_sticky_until_checkpoint_and_group_remediation(
    tmp_path: Path,
) -> None:
    database = migrated_database(tmp_path)
    keyring = ErasureTokenKeyring("current", {"current": NEW_SECRET})
    register(database, keyring)
    cleanup = cleanup_job(1, RecordingCleanupStatus.FAILED)
    job = erasure_job(
        keyring,
        1,
        recording_state=MeetingErasureRecordingState.FAILED,
        pending_audio_asset_id=None,
        cleanup_job_id=cleanup.id,
        last_failure=permanent_failure(),
    )
    with SqliteUnitOfWork(database) as uow:
        uow.recording_cleanups.add(cleanup)
        uow.meeting_erasures.add(job)
        uow.commit()
    bypass = MeetingErasureJob.model_validate(
        job.model_dump(mode="python")
        | {
            "recording_state": MeetingErasureRecordingState.CLEANUP_PENDING,
            "last_failure": None,
            "version": 1,
        }
    )
    with pytest.raises(PersistenceConflictError), SqliteUnitOfWork(database) as uow:
        uow.meeting_erasures.save(bypass, 0, None, None)

    terminal = MeetingErasureJob.model_validate(
        job.model_dump(mode="python")
        | {
            "status": MeetingErasureStatus.FAILED,
            "database_checkpointed_at": NOW,
            "completed_at": NOW,
            "version": 1,
        }
    )
    with SqliteUnitOfWork(database) as uow:
        uow.meeting_erasures.save(terminal, 0, None, None)
        uow.commit()
    with SqliteUnitOfWork(database, immediate=False) as uow:
        assert uow.meeting_erasures.get(job.id) == terminal


def test_succeeded_cleanup_detail_is_deleted_only_after_all_links_are_cleared(
    tmp_path: Path,
) -> None:
    database = migrated_database(tmp_path)
    keyring = ErasureTokenKeyring("current", {"current": NEW_SECRET})
    register(database, keyring)
    cleanup = cleanup_job(1, RecordingCleanupStatus.SUCCEEDED)
    jobs = tuple(
        erasure_job(
            keyring,
            number,
            recording_state=MeetingErasureRecordingState.CLEANUP_PENDING,
            pending_audio_asset_id=None,
            cleanup_job_id=cleanup.id,
            database_checkpointed_at=NOW,
        )
        for number in (1, 2)
    )
    with SqliteUnitOfWork(database) as uow:
        uow.recording_cleanups.add(cleanup)
        for job in jobs:
            uow.meeting_erasures.add(job)
        uow.commit()
    with SqliteUnitOfWork(database) as uow:
        assert not uow.recording_cleanups.delete_succeeded(cleanup)

    with SqliteUnitOfWork(database) as uow:
        linked = uow.meeting_erasures.list_by_cleanup_job_id(cleanup.id)
        for current in linked:
            removed = MeetingErasureJob.model_validate(
                current.model_dump(mode="python")
                | {
                    "recording_state": MeetingErasureRecordingState.REMOVED,
                    "cleanup_job_id": None,
                    "database_checkpointed_at": None,
                    "version": current.version + 1,
                }
            )
            uow.meeting_erasures.save(removed, current.version, None, None)
        assert uow.recording_cleanups.delete_succeeded(cleanup)
        uow.commit()

    with SqliteUnitOfWork(database, immediate=False) as uow:
        assert uow.recording_cleanups.get(cleanup.id) is None
        assert uow.meeting_erasures.list_by_cleanup_job_id(cleanup.id) == ()
    with SqliteUnitOfWork(database) as uow:
        claimed = uow.meeting_erasures.claim_actionable(
            "checkpoint-worker",
            NOW,
            NOW + timedelta(minutes=1),
            10,
        )
        retrying = MeetingErasureJob.model_validate(
            claimed[0].model_dump(mode="python")
            | {
                "retry_count": 1,
                "next_attempt_at": NOW + timedelta(minutes=1),
                "lease_owner": None,
                "lease_expires_at": None,
                "last_failure": retryable_failure(),
                "version": claimed[0].version + 1,
            }
        )
        uow.meeting_erasures.save(
            retrying,
            claimed[0].version,
            "checkpoint-worker",
            claimed[0].lease_expires_at,
        )
        uow.commit()

    assert len(claimed) == 2
    assert all(job.database_checkpointed_at is None for job in claimed)


def test_succeeded_cleanup_history_can_be_listed_and_deleted_by_audio_identity(
    tmp_path: Path,
) -> None:
    database = migrated_database(tmp_path)
    first = RecordingCleanupJob.model_validate(
        cleanup_job(7, RecordingCleanupStatus.SUCCEEDED).model_dump(mode="python")
        | {"reason": RecordingCleanupReason.ABANDONED_INGEST}
    )
    second = RecordingCleanupJob.model_validate(
        cleanup_job(8, RecordingCleanupStatus.SUCCEEDED).model_dump(mode="python")
        | {
            "expected_sha256": first.expected_sha256,
            "reason": RecordingCleanupReason.ORPHAN_RECONCILIATION,
        }
    )
    failed = cleanup_job(9, RecordingCleanupStatus.FAILED)
    with SqliteUnitOfWork(database) as uow:
        uow.recording_cleanups.add(first)
        uow.recording_cleanups.add(second)
        uow.recording_cleanups.add(failed)
        uow.commit()

    with SqliteUnitOfWork(database) as uow:
        assert uow.recording_cleanups.list_by_expected_sha256(first.expected_sha256) == (
            first,
            second,
        )
        assert uow.recording_cleanups.delete_succeeded(first)
        assert uow.recording_cleanups.delete_succeeded(second)
        with pytest.raises(ValueError, match="Only successful"):
            uow.recording_cleanups.delete_succeeded(failed)
        uow.commit()

    with SqliteUnitOfWork(database, immediate=False) as uow:
        assert uow.recording_cleanups.list_by_expected_sha256(first.expected_sha256) == ()
        assert uow.recording_cleanups.get(failed.id) == failed


def test_all_erasure_recording_and_terminal_states_round_trip(tmp_path: Path) -> None:
    database = migrated_database(tmp_path)
    keyring = ErasureTokenKeyring("current", {"current": NEW_SECRET})
    register(database, keyring)
    ready = cleanup_job(1)
    failed_cleanup = cleanup_job(2, RecordingCleanupStatus.FAILED)
    jobs = (
        erasure_job(keyring, 1),
        erasure_job(
            keyring,
            2,
            recording_state=MeetingErasureRecordingState.CLEANUP_PENDING,
            pending_audio_asset_id=None,
            cleanup_job_id=ready.id,
        ),
        erasure_job(
            keyring,
            3,
            recording_state=MeetingErasureRecordingState.REMOVED,
            pending_audio_asset_id=None,
        ),
        erasure_job(
            keyring,
            4,
            recording_state=MeetingErasureRecordingState.FAILED,
            pending_audio_asset_id=None,
            cleanup_job_id=failed_cleanup.id,
            last_failure=permanent_failure(),
        ),
        erasure_job(
            keyring,
            5,
            status=MeetingErasureStatus.COMPLETED,
            recording_state=MeetingErasureRecordingState.REMOVED,
            pending_audio_asset_id=None,
            database_checkpointed_at=NOW,
            completed_at=NOW,
        ),
        erasure_job(
            keyring,
            6,
            status=MeetingErasureStatus.FAILED,
            recording_state=MeetingErasureRecordingState.FAILED,
            pending_audio_asset_id=None,
            cleanup_job_id=failed_cleanup.id,
            database_checkpointed_at=NOW,
            last_failure=permanent_failure(),
            completed_at=NOW,
        ),
    )
    with SqliteUnitOfWork(database) as uow:
        uow.recording_cleanups.add(ready)
        uow.recording_cleanups.add(failed_cleanup)
        for job in jobs:
            uow.meeting_erasures.add(job)
        uow.commit()

    with SqliteUnitOfWork(database, immediate=False) as uow:
        loaded = tuple(uow.meeting_erasures.get(job.id) for job in jobs)

    assert loaded == jobs


def test_failed_cleanup_remediation_is_group_only_and_bounded(tmp_path: Path) -> None:
    database = migrated_database(tmp_path)
    keyring = ErasureTokenKeyring("current", {"current": NEW_SECRET})
    register(database, keyring)
    cleanup = cleanup_job(1, RecordingCleanupStatus.FAILED)
    failed_jobs = tuple(
        erasure_job(
            keyring,
            number,
            status=MeetingErasureStatus.FAILED,
            recording_state=MeetingErasureRecordingState.FAILED,
            pending_audio_asset_id=None,
            cleanup_job_id=cleanup.id,
            database_checkpointed_at=NOW,
            last_failure=permanent_failure(),
            remediation_count=number - 1,
            max_remediations=2,
            completed_at=NOW,
        )
        for number in (1, 2)
    )
    completed = erasure_job(
        keyring,
        9,
        status=MeetingErasureStatus.COMPLETED,
        recording_state=MeetingErasureRecordingState.REMOVED,
        pending_audio_asset_id=None,
        database_checkpointed_at=NOW,
        completed_at=NOW,
    )
    with SqliteUnitOfWork(database) as uow:
        uow.recording_cleanups.add(cleanup)
        for failed in failed_jobs:
            uow.meeting_erasures.add(failed)
        uow.meeting_erasures.add(completed)
        uow.commit()

    remediation_values = failed_jobs[0].model_dump(mode="python") | {
        "status": MeetingErasureStatus.ACTIVE,
        "recording_state": MeetingErasureRecordingState.CLEANUP_PENDING,
        "last_failure": None,
        "completed_at": None,
        "version": 1,
        "remediation_count": 1,
    }
    bypass = MeetingErasureJob.model_validate(remediation_values)
    with pytest.raises(PersistenceConflictError), SqliteUnitOfWork(database) as uow:
        uow.meeting_erasures.save(bypass, 0, None, None)
    requeued = RecordingCleanupJob.model_validate(
        cleanup.model_dump(mode="python")
        | {
            "status": RecordingCleanupStatus.READY,
            "attempt_count": 0,
            "last_failure": None,
            "completed_at": None,
        }
    )
    with pytest.raises(PersistenceConflictError), SqliteUnitOfWork(database) as uow:
        uow.recording_cleanups.save(
            requeued,
            RecordingCleanupStatus.FAILED,
            None,
            None,
        )
    with SqliteUnitOfWork(database) as uow:
        remediated = uow.meeting_erasures.reactivate_failed_cleanup_group(cleanup.id, NOW)
        uow.commit()
    reopened_completed = MeetingErasureJob.model_validate(
        completed.model_dump(mode="python")
        | {
            "status": MeetingErasureStatus.ACTIVE,
            "database_checkpointed_at": None,
            "completed_at": None,
            "version": 1,
        }
    )
    with pytest.raises(PersistenceConflictError), SqliteUnitOfWork(database) as uow:
        uow.meeting_erasures.save(reopened_completed, 0, None, None)

    with SqliteUnitOfWork(database, immediate=False) as uow:
        assert tuple(job.remediation_count for job in remediated) == (1, 2)
        assert all(job.status is MeetingErasureStatus.ACTIVE for job in remediated)
        assert uow.recording_cleanups.get(cleanup.id) == requeued

    exhausted_cleanup = cleanup_job(2, RecordingCleanupStatus.FAILED)
    exhausted_jobs = (
        erasure_job(
            keyring,
            3,
            status=MeetingErasureStatus.FAILED,
            recording_state=MeetingErasureRecordingState.FAILED,
            pending_audio_asset_id=None,
            cleanup_job_id=exhausted_cleanup.id,
            database_checkpointed_at=NOW,
            last_failure=permanent_failure(),
            remediation_count=1,
            max_remediations=1,
            completed_at=NOW,
        ),
        erasure_job(
            keyring,
            4,
            status=MeetingErasureStatus.FAILED,
            recording_state=MeetingErasureRecordingState.FAILED,
            pending_audio_asset_id=None,
            cleanup_job_id=exhausted_cleanup.id,
            database_checkpointed_at=NOW,
            last_failure=permanent_failure(),
            max_remediations=2,
            completed_at=NOW,
        ),
    )
    with SqliteUnitOfWork(database) as uow:
        uow.recording_cleanups.add(exhausted_cleanup)
        for failed in exhausted_jobs:
            uow.meeting_erasures.add(failed)
        uow.commit()
    with pytest.raises(PersistenceConflictError), SqliteUnitOfWork(database) as uow:
        uow.meeting_erasures.reactivate_failed_cleanup_group(exhausted_cleanup.id, NOW)
    with SqliteUnitOfWork(database, immediate=False) as uow:
        assert uow.meeting_erasures.list_by_cleanup_job_id(exhausted_cleanup.id) == exhausted_jobs
        assert uow.recording_cleanups.get(exhausted_cleanup.id) == exhausted_cleanup


def test_pending_audio_lookup_and_verifier_mismatch_rollback(tmp_path: Path) -> None:
    database = migrated_database(tmp_path)
    keyring = ErasureTokenKeyring("current", {"current": NEW_SECRET})
    register(database, keyring)
    shared_audio_id = UUID(int=99_999)
    jobs = tuple(
        erasure_job(keyring, number, pending_audio_asset_id=shared_audio_id) for number in (1, 2)
    )
    with SqliteUnitOfWork(database) as uow:
        for job in jobs:
            uow.meeting_erasures.add(job)
        uow.commit()
    with SqliteUnitOfWork(database, immediate=False) as uow:
        assert uow.meeting_erasures.list_by_pending_audio_asset_id(shared_audio_id) == jobs

    mismatched = ErasureTokenKeyring(
        "current",
        {"current": b"z" * 32, "candidate": b"c" * 32},
    )
    with pytest.raises(ErasureKeyVerificationError), SqliteUnitOfWork(database) as uow:
        uow.erasure_key_verifiers.add(mismatched.verifier("candidate", NOW))
        mismatched.validate_verifiers(
            uow.erasure_key_verifiers.list_all(),
            uow.erasure_key_verifiers.list_referenced_tokens(),
        )
    with SqliteUnitOfWork(database, immediate=False) as uow:
        assert uow.erasure_key_verifiers.get("candidate") is None


def test_unit_of_work_rolls_back_erasure_graph_atomically(tmp_path: Path) -> None:
    database = migrated_database(tmp_path)
    keyring = ErasureTokenKeyring("current", {"current": NEW_SECRET})
    register(database, keyring)
    job = erasure_job(keyring, 1)
    meeting_token = keyring.meeting_token(UUID(int=1))

    with pytest.raises(RuntimeError, match="stop"), SqliteUnitOfWork(database) as uow:
        uow.meeting_erasures.add(job)
        uow.meeting_erasure_tombstones.add(
            MeetingErasureTombstone.create(
                job.id,
                meeting_token,
                keyring.ingest_key_token("ingest-1"),
                NOW,
            )
        )
        raise RuntimeError("stop")

    with SqliteUnitOfWork(database, immediate=False) as uow:
        assert uow.meeting_erasures.get(job.id) is None
        assert uow.meeting_erasure_tombstones.get_for_job(job.id) is None
