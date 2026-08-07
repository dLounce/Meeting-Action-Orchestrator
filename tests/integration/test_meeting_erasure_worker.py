from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Event, get_ident
from types import TracebackType
from uuid import UUID

import pytest

from meeting_action_orchestrator.application.errors import (
    MeetingErasureBlockedError,
    MeetingErasureIntegrityError,
)
from meeting_action_orchestrator.application.meeting_erasure import (
    ErasureKeyRegistry,
    MeetingErasureRemediationService,
)
from meeting_action_orchestrator.application.meeting_erasure_worker import (
    MeetingErasureWorker,
    MeetingErasureWorkerOutcome,
)
from meeting_action_orchestrator.application.ports import WalCheckpointResult
from meeting_action_orchestrator.application.recording_cleanup import RecordingCleanupWorker
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
    MeetingErasureFailure,
    MeetingErasureJob,
    MeetingErasureOperationBinding,
    RecordingCleanupJob,
    WorkflowFailure,
)
from meeting_action_orchestrator.infrastructure.database import Database
from meeting_action_orchestrator.infrastructure.erasure_tokens import ErasureTokenKeyring
from meeting_action_orchestrator.infrastructure.repositories import (
    PersistenceConflictError,
    SqliteUnitOfWork,
)

NOW = datetime(2026, 8, 7, 9, 0, tzinfo=timezone.utc)


class MutableClock:
    def __init__(self, current: datetime = NOW) -> None:
        self.current = current

    def now(self) -> datetime:
        return self.current


class FixedRetryScheduler:
    def schedule(self, now: datetime, attempt_count: int) -> datetime:
        return now + timedelta(minutes=attempt_count)


class Checkpoint:
    def __init__(self, *results: WalCheckpointResult | Exception) -> None:
        self.results = list(results)
        self.calls = 0

    def truncate_wal(self) -> WalCheckpointResult:
        self.calls += 1
        result = self.results.pop(0) if self.results else WalCheckpointResult(0, 0, 0)
        if isinstance(result, Exception):
            raise result
        return result


class NoopCleanupExecutor:
    def execute(self, job: RecordingCleanupJob) -> None:
        del job

    def healthcheck(self) -> bool:
        return True


class TrackingUnitOfWork(SqliteUnitOfWork):
    def __init__(self, database: Database, tracker: TrackingUnitOfWorkFactory) -> None:
        super().__init__(database)
        self._tracker = tracker

    def __enter__(self) -> TrackingUnitOfWork:
        entered = super().__enter__()
        self._tracker.active += 1
        self._tracker.thread_ids.append(get_ident())
        return entered

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        try:
            return super().__exit__(exc_type, exc_value, traceback)
        finally:
            self._tracker.active -= 1


class TrackingUnitOfWorkFactory:
    def __init__(self, database: Database) -> None:
        self.database = database
        self.active = 0
        self.thread_ids: list[int] = []

    def __call__(self) -> TrackingUnitOfWork:
        return TrackingUnitOfWork(self.database, self)


class BlockingCheckpoint:
    def __init__(self, tracker: TrackingUnitOfWorkFactory) -> None:
        self.tracker = tracker
        self.started = Event()
        self.release = Event()
        self.thread_ids: list[int] = []

    def truncate_wal(self) -> WalCheckpointResult:
        self.thread_ids.append(get_ident())
        assert self.tracker.active == 0
        self.started.set()
        assert self.release.wait(timeout=2)
        return WalCheckpointResult(0, 0, 0)


def migrated_database(tmp_path: Path) -> Database:
    database = Database(tmp_path / "application.sqlite3")
    database.migrate()
    return database


def keyring() -> ErasureTokenKeyring:
    return ErasureTokenKeyring("current", {"current": b"k" * 32})


def register(database: Database, tokens: ErasureTokenKeyring) -> None:
    ErasureKeyRegistry(
        unit_of_work=lambda: SqliteUnitOfWork(database),
        tokens=tokens,
        clock=MutableClock(),
    ).ensure_registered_sync()


def cleanup_job(
    number: int,
    status: RecordingCleanupStatus,
    *,
    reason: RecordingCleanupReason = RecordingCleanupReason.MEETING_ERASURE,
) -> RecordingCleanupJob:
    failure = None
    if status is RecordingCleanupStatus.FAILED:
        failure = WorkflowFailure(
            code=FailureCode.INTERNAL,
            disposition=FailureDisposition.PERMANENT,
            safe_message="Recording cleanup could not finish",
            occurred_at=NOW,
        )
    terminal = status in {RecordingCleanupStatus.SUCCEEDED, RecordingCleanupStatus.FAILED}
    running = status is RecordingCleanupStatus.RUNNING
    return RecordingCleanupJob(
        id=UUID(int=10_000 + number),
        storage_key=f"{10_000 + number:032x}.wav",
        expected_sha256=f"{number % 16:x}" * 64,
        expected_size_bytes=1_024,
        reason=reason,
        status=status,
        attempt_count=1 if terminal or running else 0,
        max_attempts=5,
        lease_owner="cleanup-worker" if running else None,
        lease_expires_at=NOW + timedelta(minutes=5) if running else None,
        last_failure=failure,
        created_at=NOW,
        updated_at=NOW,
        completed_at=NOW if terminal else None,
    )


def erasure_job(
    tokens: ErasureTokenKeyring,
    number: int,
    *,
    cleanup: RecordingCleanupJob | None = None,
    checkpointed: bool = False,
    failed: bool = False,
    remediation_count: int = 0,
    max_remediations: int = 3,
) -> MeetingErasureJob:
    token = tokens.meeting_token(UUID(int=20_000 + number))
    values: dict[str, object] = {
        "id": UUID(int=30_000 + number),
        "token_version": token.token_version,
        "token_key_id": token.key_id,
        "meeting_token": token.digest,
        "reason": MeetingErasureReason.USER_REQUEST,
        "erased_meeting_version": number,
        "recording_state": MeetingErasureRecordingState.CLEANUP_PENDING,
        "cleanup_job_id": cleanup.id if cleanup is not None else UUID(int=40_000 + number),
        "database_checkpointed_at": NOW if checkpointed else None,
        "remediation_count": remediation_count,
        "max_remediations": max_remediations,
        "created_at": NOW,
        "updated_at": NOW,
    }
    if failed:
        values |= {
            "status": MeetingErasureStatus.FAILED,
            "recording_state": MeetingErasureRecordingState.FAILED,
            "database_checkpointed_at": NOW,
            "last_failure": MeetingErasureFailure(
                code=MeetingErasureFailureCode.RECORDING_CLEANUP_REJECTED,
                disposition=FailureDisposition.PERMANENT,
                occurred_at=NOW,
            ),
            "completed_at": NOW,
        }
    return MeetingErasureJob.model_validate(values)


def add_group(
    database: Database,
    cleanup: RecordingCleanupJob,
    jobs: tuple[MeetingErasureJob, ...],
) -> None:
    with SqliteUnitOfWork(database) as uow:
        uow.recording_cleanups.add(cleanup)
        for job in jobs:
            uow.meeting_erasures.add(job)
        uow.commit()


def worker(
    database: Database,
    checkpoint: Checkpoint,
    clock: MutableClock | None = None,
    *,
    worker_id: str = "erasure-worker",
) -> MeetingErasureWorker:
    return MeetingErasureWorker(
        unit_of_work=lambda: SqliteUnitOfWork(database),
        checkpoint=checkpoint,
        clock=clock or MutableClock(),
        retry_scheduler=FixedRetryScheduler(),
        worker_id=worker_id,
        lease_duration=timedelta(minutes=5),
    )


def test_busy_and_exception_checkpoint_results_schedule_unbounded_retry(
    tmp_path: Path,
) -> None:
    database = migrated_database(tmp_path)
    tokens = keyring()
    register(database, tokens)
    cleanup = cleanup_job(1, RecordingCleanupStatus.READY)
    jobs = (erasure_job(tokens, 1, cleanup=cleanup),)
    add_group(database, cleanup, jobs)
    checkpoints = Checkpoint(WalCheckpointResult(1, 3, 2), RuntimeError("locked"))
    erasure_worker = worker(database, checkpoints)

    first = erasure_worker._claim_and_process()
    clock = MutableClock(NOW + timedelta(minutes=1))
    second = worker(database, checkpoints, clock)._claim_and_process()

    assert first is not None
    assert first.outcome is MeetingErasureWorkerOutcome.RETRY_SCHEDULED
    assert first.job is not None
    assert first.job.retry_count == 1
    assert second is not None
    assert second.outcome is MeetingErasureWorkerOutcome.RETRY_SCHEDULED
    assert second.job is not None
    assert second.job.retry_count == 2
    assert second.job.remediation_count == 0
    assert second.job.database_checkpointed_at is None


def test_database_truncate_checkpoint_returns_exact_success_state(tmp_path: Path) -> None:
    database = migrated_database(tmp_path)

    result = database.truncate_wal()

    assert result == WalCheckpointResult(0, 0, 0)
    assert result.truncated


async def test_run_once_offloads_and_drains_checkpoint_publication_on_cancellation(
    tmp_path: Path,
) -> None:
    database = migrated_database(tmp_path)
    tokens = keyring()
    register(database, tokens)
    cleanup = cleanup_job(1, RecordingCleanupStatus.READY)
    job = erasure_job(tokens, 1, cleanup=cleanup)
    add_group(database, cleanup, (job,))
    tracker = TrackingUnitOfWorkFactory(database)
    checkpoint = BlockingCheckpoint(tracker)
    erasure_worker = MeetingErasureWorker(
        unit_of_work=tracker,
        checkpoint=checkpoint,
        clock=MutableClock(),
        retry_scheduler=FixedRetryScheduler(),
        worker_id="erasure-worker",
    )
    loop_thread = get_ident()

    running = asyncio.create_task(erasure_worker.run_once(1))
    for _ in range(100):
        if checkpoint.started.is_set():
            break
        await asyncio.sleep(0.01)
    assert checkpoint.started.is_set()
    running.cancel()
    await asyncio.sleep(0.01)
    assert not running.done()
    checkpoint.release.set()
    with pytest.raises(asyncio.CancelledError):
        await running

    with SqliteUnitOfWork(database, immediate=False) as uow:
        persisted = uow.meeting_erasures.get(job.id)
    assert persisted is not None
    assert persisted.database_checkpointed_at == NOW
    assert persisted.lease_owner is None
    assert checkpoint.thread_ids
    assert tracker.thread_ids
    assert loop_thread not in checkpoint.thread_ids
    assert loop_thread not in tracker.thread_ids


def test_cleanup_success_before_initial_checkpoint_rearms_and_completes(
    tmp_path: Path,
) -> None:
    database = migrated_database(tmp_path)
    tokens = keyring()
    register(database, tokens)
    cleanup = cleanup_job(1, RecordingCleanupStatus.SUCCEEDED)
    job = erasure_job(tokens, 1, cleanup=cleanup)
    add_group(database, cleanup, (job,))
    erasure_worker = worker(database, Checkpoint())

    initial = erasure_worker._claim_and_process()
    removed = erasure_worker._claim_and_process()
    completed = erasure_worker._claim_and_process()

    assert initial is not None
    assert initial.outcome is MeetingErasureWorkerOutcome.CHECKPOINTED
    assert removed is not None
    assert removed.outcome is MeetingErasureWorkerOutcome.CLEANUP_REMOVED
    assert removed.job is not None
    assert removed.job.database_checkpointed_at is None
    assert completed is not None
    assert completed.outcome is MeetingErasureWorkerOutcome.COMPLETED
    with SqliteUnitOfWork(database, immediate=False) as uow:
        persisted = uow.meeting_erasures.get(job.id)
        assert uow.recording_cleanups.get(cleanup.id) is None
    assert persisted is not None
    assert persisted.status is MeetingErasureStatus.COMPLETED


def test_cleanup_success_fans_out_shared_group_and_requires_fresh_checkpoints(
    tmp_path: Path,
) -> None:
    database = migrated_database(tmp_path)
    tokens = keyring()
    register(database, tokens)
    cleanup = cleanup_job(1, RecordingCleanupStatus.SUCCEEDED)
    jobs = tuple(
        erasure_job(tokens, number, cleanup=cleanup, checkpointed=True) for number in (1, 2)
    )
    add_group(database, cleanup, jobs)
    erasure_worker = worker(database, Checkpoint())

    removed = erasure_worker._claim_and_process()
    with SqliteUnitOfWork(database, immediate=False) as uow:
        after_fanout = tuple(uow.meeting_erasures.get(job.id) for job in jobs)
        assert uow.recording_cleanups.get(cleanup.id) is None
    terminal = (erasure_worker._claim_and_process(), erasure_worker._claim_and_process())

    assert removed is not None
    assert removed.outcome is MeetingErasureWorkerOutcome.CLEANUP_REMOVED
    assert all(job is not None for job in after_fanout)
    assert all(job.recording_state is MeetingErasureRecordingState.REMOVED for job in after_fanout)
    assert all(job.database_checkpointed_at is None for job in after_fanout)
    assert all(result is not None for result in terminal)
    assert {result.outcome for result in terminal if result is not None} == {
        MeetingErasureWorkerOutcome.COMPLETED
    }


def test_same_worker_concurrent_group_leases_cannot_clear_each_other(tmp_path: Path) -> None:
    database = migrated_database(tmp_path)
    tokens = keyring()
    register(database, tokens)
    cleanup = cleanup_job(1, RecordingCleanupStatus.SUCCEEDED)
    jobs = tuple(
        erasure_job(tokens, number, cleanup=cleanup, checkpointed=True) for number in (1, 2)
    )
    add_group(database, cleanup, jobs)
    with SqliteUnitOfWork(database) as uow:
        claimed = tuple(
            uow.meeting_erasures.claim_actionable(
                "same-worker",
                NOW,
                NOW + timedelta(minutes=5),
                2,
            )
        )
        uow.commit()

    result = worker(database, Checkpoint(), worker_id="same-worker")._reconcile_cleanup(claimed[0])

    assert result.outcome is MeetingErasureWorkerOutcome.LEASE_LOST
    with SqliteUnitOfWork(database, immediate=False) as uow:
        persisted = tuple(uow.meeting_erasures.get(job.id) for job in jobs)
        assert uow.recording_cleanups.get(cleanup.id) == cleanup
    assert all(job is not None and job.cleanup_job_id == cleanup.id for job in persisted)


def test_cleanup_failure_fans_out_and_group_remediation_is_idempotent(
    tmp_path: Path,
) -> None:
    database = migrated_database(tmp_path)
    tokens = keyring()
    register(database, tokens)
    cleanup = cleanup_job(1, RecordingCleanupStatus.FAILED)
    jobs = tuple(
        erasure_job(tokens, number, cleanup=cleanup, checkpointed=True) for number in (1, 2)
    )
    add_group(database, cleanup, jobs)
    failed = worker(database, Checkpoint())._claim_and_process()
    remediation = MeetingErasureRemediationService(
        unit_of_work=lambda: SqliteUnitOfWork(database),
        tokens=tokens,
        key_registry=ErasureKeyRegistry(
            unit_of_work=lambda: SqliteUnitOfWork(database),
            tokens=tokens,
            clock=MutableClock(),
        ),
        clock=MutableClock(),
    )

    retried = remediation._retry(
        jobs[0].id,
        jobs[0].version + 2,
        "retry-request",
        "actor",
    )
    replay = remediation._retry(
        jobs[0].id,
        jobs[0].version + 2,
        "retry-request",
        "actor",
    )

    assert failed is not None
    assert failed.outcome is MeetingErasureWorkerOutcome.CLEANUP_FAILED
    assert retried.job.status is MeetingErasureStatus.ACTIVE
    assert retried.job.remediation_count == 1
    assert replay.replayed
    with SqliteUnitOfWork(database, immediate=False) as uow:
        linked = tuple(uow.meeting_erasures.list_by_cleanup_job_id(cleanup.id))
        reset = uow.recording_cleanups.get(cleanup.id)
    assert all(job.status is MeetingErasureStatus.ACTIVE for job in linked)
    assert all(job.remediation_count == 1 for job in linked)
    assert reset is not None
    assert reset.status is RecordingCleanupStatus.READY


def test_generic_repository_cannot_reset_terminal_erasure_cleanup(tmp_path: Path) -> None:
    database = migrated_database(tmp_path)
    tokens = keyring()
    register(database, tokens)
    cleanup = cleanup_job(1, RecordingCleanupStatus.FAILED)
    jobs = tuple(erasure_job(tokens, number, cleanup=cleanup) for number in (1, 2))
    add_group(database, cleanup, jobs)
    ready = RecordingCleanupJob.model_validate(
        cleanup.model_dump(mode="python")
        | {
            "status": RecordingCleanupStatus.READY,
            "attempt_count": 0,
            "last_failure": None,
            "completed_at": None,
        }
    )

    with pytest.raises(PersistenceConflictError), SqliteUnitOfWork(database) as uow:
        uow.recording_cleanups.save(ready, RecordingCleanupStatus.FAILED, None, None)

    with SqliteUnitOfWork(database, immediate=False) as uow:
        assert uow.recording_cleanups.get(cleanup.id) == cleanup
        persisted = tuple(uow.meeting_erasures.list_by_cleanup_job_id(cleanup.id))
    assert persisted == jobs


def test_cleanup_worker_deletes_unreferenced_success_and_retains_linked_success(
    tmp_path: Path,
) -> None:
    database = migrated_database(tmp_path)
    tokens = keyring()
    register(database, tokens)
    unlinked = cleanup_job(
        1,
        RecordingCleanupStatus.RUNNING,
        reason=RecordingCleanupReason.ABANDONED_INGEST,
    )
    linked = cleanup_job(2, RecordingCleanupStatus.RUNNING)
    job = erasure_job(tokens, 1, cleanup=linked)
    add_group(database, linked, (job,))
    with SqliteUnitOfWork(database) as uow:
        uow.recording_cleanups.add(unlinked)
        uow.commit()
    cleanup_worker = RecordingCleanupWorker(
        unit_of_work=lambda: SqliteUnitOfWork(database),
        executor=NoopCleanupExecutor(),
        clock=MutableClock(),
        retry_scheduler=FixedRetryScheduler(),
        worker_id="cleanup-worker",
    )

    first = cleanup_worker._finish_success(unlinked)
    second = cleanup_worker._finish_success(linked)

    assert first is not None
    assert first.status is RecordingCleanupStatus.SUCCEEDED
    assert second is not None
    assert second.status is RecordingCleanupStatus.SUCCEEDED
    with SqliteUnitOfWork(database, immediate=False) as uow:
        assert uow.recording_cleanups.get(unlinked.id) is None
        persisted = uow.recording_cleanups.get(linked.id)
    assert persisted is not None
    assert persisted.status is RecordingCleanupStatus.SUCCEEDED


def test_exhausted_shared_remediation_rolls_back_without_binding(tmp_path: Path) -> None:
    database = migrated_database(tmp_path)
    tokens = keyring()
    register(database, tokens)
    cleanup = cleanup_job(1, RecordingCleanupStatus.FAILED)
    jobs = (
        erasure_job(tokens, 1, cleanup=cleanup, failed=True),
        erasure_job(
            tokens,
            2,
            cleanup=cleanup,
            failed=True,
            remediation_count=1,
            max_remediations=1,
        ),
    )
    add_group(database, cleanup, jobs)
    remediation = MeetingErasureRemediationService(
        unit_of_work=lambda: SqliteUnitOfWork(database),
        tokens=tokens,
        key_registry=ErasureKeyRegistry(
            unit_of_work=lambda: SqliteUnitOfWork(database),
            tokens=tokens,
            clock=MutableClock(),
        ),
        clock=MutableClock(),
    )

    with pytest.raises(MeetingErasureBlockedError):
        remediation._retry(jobs[0].id, 0, "retry-request", "actor")

    with SqliteUnitOfWork(database, immediate=False) as uow:
        assert uow.meeting_erasure_operations.list_for_job(jobs[0].id) == ()
        assert uow.recording_cleanups.get(cleanup.id) == cleanup
        persisted = tuple(uow.meeting_erasures.list_by_cleanup_job_id(cleanup.id))
    assert persisted == jobs


def test_forged_retry_binding_cannot_return_another_erasure_job(tmp_path: Path) -> None:
    database = migrated_database(tmp_path)
    tokens = keyring()
    register(database, tokens)
    first_token = tokens.meeting_token(UUID(int=90_001))
    second_token = tokens.meeting_token(UUID(int=90_002))
    jobs = tuple(
        MeetingErasureJob(
            id=UUID(int=90_010 + number),
            token_version=token.token_version,
            token_key_id=token.key_id,
            meeting_token=token.digest,
            reason=MeetingErasureReason.USER_REQUEST,
            erased_meeting_version=0,
            recording_state=MeetingErasureRecordingState.WAITING_SHARED,
            pending_audio_asset_id=UUID(int=90_020 + number),
            created_at=NOW,
            updated_at=NOW,
        )
        for number, token in enumerate((first_token, second_token), start=1)
    )
    binding = MeetingErasureOperationBinding.create(
        tokens.request_key_token("forged-retry"),
        tokens.actor_token("actor"),
        tokens.erasure_job_token(jobs[0].id),
        jobs[1].id,
        MeetingErasureOperation.RETRY,
        0,
        NOW,
    )
    with SqliteUnitOfWork(database) as uow:
        for job in jobs:
            uow.meeting_erasures.add(job)
        uow.meeting_erasure_operations.add(binding)
        uow.commit()
    remediation = MeetingErasureRemediationService(
        unit_of_work=lambda: SqliteUnitOfWork(database),
        tokens=tokens,
        key_registry=ErasureKeyRegistry(
            unit_of_work=lambda: SqliteUnitOfWork(database),
            tokens=tokens,
            clock=MutableClock(),
        ),
        clock=MutableClock(),
    )

    with pytest.raises(MeetingErasureIntegrityError):
        remediation._retry(jobs[0].id, 0, "forged-retry", "actor")
