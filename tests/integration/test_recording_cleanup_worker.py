from __future__ import annotations

import asyncio
from collections.abc import Callable, Set
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Event, get_ident
from uuid import UUID

import pytest

from meeting_action_orchestrator.application.errors import (
    PermanentRecordingCleanupError,
    RetryableRecordingCleanupError,
)
from meeting_action_orchestrator.application.processing import FullJitterRetryScheduler
from meeting_action_orchestrator.application.recording_cleanup import (
    OrphanDiscoveryBatch,
    RecordingCleanupOutcome,
    RecordingCleanupScheduler,
    RecordingCleanupWorker,
    RecordingIdentity,
    RecordingOrphanDiscoverer,
    StaleRecordingCandidate,
)
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
from meeting_action_orchestrator.infrastructure.repositories import SqliteUnitOfWork

NOW = datetime(2026, 8, 7, 9, 0, tzinfo=timezone.utc)
JOB_ID = UUID(int=1)
FIRST_KEY = "1" * 32 + ".wav"
SECOND_KEY = "2" * 32 + ".wav"
THIRD_KEY = "3" * 32 + ".wav"


class MutableClock:
    def __init__(self, current: datetime = NOW) -> None:
        self.current = current

    def now(self) -> datetime:
        return self.current


class FixedRetryScheduler:
    def __init__(self) -> None:
        self.attempts: list[int] = []

    def schedule(self, now: datetime, attempt_count: int) -> datetime:
        self.attempts.append(attempt_count)
        return now + timedelta(seconds=37)


class FakeExecutor:
    def __init__(
        self,
        behavior: Callable[[RecordingCleanupJob], None] | None = None,
        *,
        ready: bool = True,
    ) -> None:
        self._behavior = behavior
        self._ready = ready
        self.jobs: list[RecordingCleanupJob] = []
        self.thread_ids: list[int] = []

    def execute(self, job: RecordingCleanupJob) -> None:
        self.jobs.append(job)
        self.thread_ids.append(get_ident())
        if self._behavior is not None:
            self._behavior(job)

    def healthcheck(self) -> bool:
        return self._ready


class BlockingExecutor(FakeExecutor):
    def __init__(self) -> None:
        super().__init__()
        self.started = Event()
        self.release = Event()

    def execute(self, job: RecordingCleanupJob) -> None:
        self.jobs.append(job)
        self.thread_ids.append(get_ident())
        self.started.set()
        if not self.release.wait(timeout=2):
            raise RuntimeError("executor test timed out")


class TrackingUnitOfWorkFactory:
    def __init__(self, database: Database) -> None:
        self.database = database
        self.thread_ids: list[int] = []

    def __call__(self) -> SqliteUnitOfWork:
        self.thread_ids.append(get_ident())
        return SqliteUnitOfWork(self.database)


class FailBeforeCommitUnitOfWork(SqliteUnitOfWork):
    def __init__(self, database: Database, factory: FailingUnitOfWorkFactory) -> None:
        super().__init__(database)
        self._factory = factory

    def commit(self) -> None:
        self._factory.commit_count += 1
        if self._factory.commit_count == self._factory.fail_on_commit:
            raise RuntimeError("publication unavailable")
        super().commit()


class FailingUnitOfWorkFactory:
    def __init__(self, database: Database, fail_on_commit: int) -> None:
        self.database = database
        self.fail_on_commit = fail_on_commit
        self.commit_count = 0

    def __call__(self) -> SqliteUnitOfWork:
        return FailBeforeCommitUnitOfWork(self.database, self)


class ActiveRecordings:
    def __init__(self, keys: frozenset[str] = frozenset()) -> None:
        self.keys = keys
        self.thread_ids: list[int] = []

    def active_temporary_keys(self) -> frozenset[str]:
        self.thread_ids.append(get_ident())
        return self.keys


class FakeScanner:
    def __init__(
        self,
        candidates: tuple[StaleRecordingCandidate, ...],
        identities: dict[str, RecordingIdentity | Exception | None],
    ) -> None:
        self.candidates = candidates
        self.identities = identities
        self.scan_calls: list[tuple[int, str | None, frozenset[str]]] = []
        self.identified: list[str] = []
        self.thread_ids: list[int] = []

    def scan_stale_candidates(
        self,
        *,
        now: datetime,
        grace_period: timedelta,
        limit: int,
        after_storage_key: str | None = None,
        active_temporary_keys: Set[str] = frozenset(),
    ) -> tuple[StaleRecordingCandidate, ...]:
        del now, grace_period
        self.thread_ids.append(get_ident())
        self.scan_calls.append((limit, after_storage_key, frozenset(active_temporary_keys)))
        eligible = tuple(
            candidate
            for candidate in self.candidates
            if after_storage_key is None or candidate.storage_key > after_storage_key
        )
        return eligible[:limit]

    def identify(self, candidate: StaleRecordingCandidate) -> RecordingIdentity | None:
        self.thread_ids.append(get_ident())
        self.identified.append(candidate.storage_key)
        identity = self.identities[candidate.storage_key]
        if isinstance(identity, Exception):
            raise identity
        return identity


def cleanup_job(
    *,
    job_id: UUID = JOB_ID,
    storage_key: str = FIRST_KEY,
    status: RecordingCleanupStatus = RecordingCleanupStatus.READY,
    attempt_count: int = 0,
    lease_owner: str | None = None,
    lease_expires_at: datetime | None = None,
    created_at: datetime = NOW,
    updated_at: datetime = NOW,
) -> RecordingCleanupJob:
    return RecordingCleanupJob(
        id=job_id,
        storage_key=storage_key,
        expected_sha256="a" * 64,
        expected_size_bytes=16,
        reason=RecordingCleanupReason.ABANDONED_INGEST,
        status=status,
        attempt_count=attempt_count,
        max_attempts=5,
        lease_owner=lease_owner,
        lease_expires_at=lease_expires_at,
        created_at=created_at,
        updated_at=updated_at,
    )


def candidate(storage_key: str, value: int) -> StaleRecordingCandidate:
    return StaleRecordingCandidate(
        storage_key=storage_key,
        size_bytes=16,
        modified_at=NOW - timedelta(days=2),
        stat_device=1,
        stat_inode=value,
        stat_modified_ns=value,
        stat_changed_ns=value,
    )


def identity(storage_key: str) -> RecordingIdentity:
    return RecordingIdentity(storage_key=storage_key, sha256="b" * 64, size_bytes=16)


def audio_asset(storage_key: str) -> AudioAsset:
    return AudioAsset(
        id=UUID(int=10),
        storage_key=storage_key,
        original_name="recording.wav",
        detected_media_type=AudioMediaType.WAV,
        size_bytes=16,
        duration_ms=1_000,
        sha256="c" * 64,
        created_at=NOW,
    )


def reject_cleanup(_job: RecordingCleanupJob) -> None:
    raise PermanentRecordingCleanupError


def defer_cleanup(_job: RecordingCleanupJob) -> None:
    raise RetryableRecordingCleanupError


def test_recording_identity_repr_omits_the_content_digest() -> None:
    value = identity(FIRST_KEY)

    assert value.sha256 not in repr(value)


def migrated_database(root: Path) -> Database:
    database = Database(root / "application.sqlite3")
    database.migrate()
    return database


def add_cleanup(database: Database, job: RecordingCleanupJob) -> None:
    with SqliteUnitOfWork(database) as uow:
        uow.recording_cleanups.add(job)
        uow.commit()


def find_cleanup(
    database: Database,
    job_id: UUID = JOB_ID,
) -> RecordingCleanupJob | None:
    with SqliteUnitOfWork(database, immediate=False) as uow:
        return uow.recording_cleanups.get(job_id)


def get_cleanup(database: Database, job_id: UUID = JOB_ID) -> RecordingCleanupJob:
    persisted = find_cleanup(database, job_id)
    assert persisted is not None
    return persisted


def cleanup_worker(
    database: Database,
    executor: FakeExecutor,
    clock: MutableClock,
    scheduler: FixedRetryScheduler | None = None,
    *,
    unit_of_work: Callable[[], SqliteUnitOfWork] | None = None,
    worker_id: str = "cleanup-one",
    heartbeat_interval: timedelta | None = None,
) -> RecordingCleanupWorker:
    return RecordingCleanupWorker(
        unit_of_work=unit_of_work or (lambda: SqliteUnitOfWork(database)),
        executor=executor,
        clock=clock,
        retry_scheduler=scheduler or FixedRetryScheduler(),
        worker_id=worker_id,
        lease_duration=timedelta(seconds=30),
        heartbeat_interval=heartbeat_interval,
    )


async def test_cleanup_worker_runs_storage_and_database_work_off_loop(
    tmp_path: Path,
) -> None:
    database = migrated_database(tmp_path)
    add_cleanup(database, cleanup_job())
    factory = TrackingUnitOfWorkFactory(database)
    executor = FakeExecutor()
    loop_thread = get_ident()

    results = await cleanup_worker(
        database,
        executor,
        MutableClock(),
        unit_of_work=factory,
    ).run_once(1)

    assert results[0].outcome is RecordingCleanupOutcome.SUCCEEDED
    assert results[0].job is not None
    assert results[0].job.status is RecordingCleanupStatus.SUCCEEDED
    assert executor.thread_ids
    assert factory.thread_ids
    assert loop_thread not in executor.thread_ids
    assert loop_thread not in factory.thread_ids


async def test_cleanup_worker_retries_then_succeeds(tmp_path: Path) -> None:
    database = migrated_database(tmp_path)
    add_cleanup(database, cleanup_job())
    clock = MutableClock()
    scheduler = FixedRetryScheduler()
    attempts = 0

    def execute(_job: RecordingCleanupJob) -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RetryableRecordingCleanupError

    worker = cleanup_worker(database, FakeExecutor(execute), clock, scheduler)

    first = await worker.run_once(1)
    retrying = get_cleanup(database)
    clock.current = retrying.next_attempt_at or NOW
    second = await worker.run_once(1)

    assert first[0].outcome is RecordingCleanupOutcome.RETRY_SCHEDULED
    assert retrying.status is RecordingCleanupStatus.RETRY_WAIT
    assert retrying.last_failure is not None
    assert retrying.last_failure.safe_message == "Recording cleanup could not finish"
    assert scheduler.attempts == [1]
    assert second[0].outcome is RecordingCleanupOutcome.SUCCEEDED
    assert second[0].job is not None
    assert second[0].job.attempt_count == 2
    assert find_cleanup(database) is None


async def test_unexpected_cleanup_error_is_persisted_without_private_details(
    tmp_path: Path,
) -> None:
    database = migrated_database(tmp_path)
    add_cleanup(database, cleanup_job())
    private_detail = "C:\\private\\recordings\\secret.wav"

    def fail(_job: RecordingCleanupJob) -> None:
        raise RuntimeError(private_detail)

    result = await cleanup_worker(
        database,
        FakeExecutor(fail),
        MutableClock(),
    ).run_once(1)

    assert result[0].outcome is RecordingCleanupOutcome.RETRY_SCHEDULED
    persisted = get_cleanup(database)
    assert persisted.last_failure is not None
    assert private_detail not in persisted.last_failure.safe_message


async def test_cleanup_worker_fails_permanent_and_exhausted_jobs(tmp_path: Path) -> None:
    database = migrated_database(tmp_path)
    permanent = cleanup_job()
    exhausted = cleanup_job(
        job_id=UUID(int=2),
        storage_key=SECOND_KEY,
        status=RecordingCleanupStatus.RUNNING,
        attempt_count=5,
        lease_owner="expired-worker",
        lease_expires_at=NOW - timedelta(seconds=1),
        created_at=NOW - timedelta(minutes=2),
        updated_at=NOW - timedelta(minutes=1),
    )
    add_cleanup(database, permanent)

    permanent_result = await cleanup_worker(
        database,
        FakeExecutor(reject_cleanup),
        MutableClock(),
    ).run_once(1)
    add_cleanup(database, exhausted)
    retry_result = await cleanup_worker(
        database,
        FakeExecutor(defer_cleanup),
        MutableClock(),
        worker_id="cleanup-two",
    ).run_once(1)

    assert permanent_result[0].outcome is RecordingCleanupOutcome.FAILED
    assert permanent_result[0].job is not None
    assert permanent_result[0].job.attempt_count == 1
    assert retry_result[0].outcome is RecordingCleanupOutcome.FAILED
    assert retry_result[0].job is not None
    assert retry_result[0].job.attempt_count == 5


async def test_cleanup_worker_renews_lease_during_storage_work(tmp_path: Path) -> None:
    database = migrated_database(tmp_path)
    add_cleanup(database, cleanup_job())
    clock = MutableClock()
    executor = BlockingExecutor()
    worker = cleanup_worker(
        database,
        executor,
        clock,
        heartbeat_interval=timedelta(milliseconds=5),
    )

    running = asyncio.create_task(worker.run_once(1))
    assert await asyncio.to_thread(executor.started.wait, 1)
    clock.current = NOW + timedelta(seconds=20)

    async def wait_for_renewal() -> RecordingCleanupJob:
        while True:
            renewed = get_cleanup(database)
            if renewed.lease_expires_at == NOW + timedelta(seconds=50):
                return renewed
            await asyncio.sleep(0.001)

    renewed = await asyncio.wait_for(wait_for_renewal(), timeout=1)
    clock.current = NOW + timedelta(seconds=35)
    executor.release.set()
    results = await asyncio.wait_for(running, timeout=1)

    assert renewed.lease_expires_at == NOW + timedelta(seconds=50)
    assert results[0].outcome is RecordingCleanupOutcome.SUCCEEDED


async def test_cleanup_worker_drains_execution_and_publication_on_cancellation(
    tmp_path: Path,
) -> None:
    database = migrated_database(tmp_path)
    add_cleanup(database, cleanup_job())
    executor = BlockingExecutor()
    worker = cleanup_worker(database, executor, MutableClock())

    running = asyncio.create_task(worker.run_once(1))
    assert await asyncio.to_thread(executor.started.wait, 1)
    running.cancel()
    await asyncio.sleep(0.01)
    assert not running.done()
    executor.release.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(running, timeout=1)

    assert find_cleanup(database) is None


async def test_reclaimed_cleanup_rejects_the_stale_worker_publication(
    tmp_path: Path,
) -> None:
    database = migrated_database(tmp_path)
    add_cleanup(database, cleanup_job())
    clock = MutableClock()
    blocked = BlockingExecutor()
    stale_worker = cleanup_worker(database, blocked, clock)

    stale_run = asyncio.create_task(stale_worker.run_once(1))
    assert await asyncio.to_thread(blocked.started.wait, 1)
    clock.current = NOW + timedelta(seconds=31)
    replacement = await cleanup_worker(
        database,
        FakeExecutor(),
        clock,
        worker_id="cleanup-two",
    ).run_once(1)
    blocked.release.set()
    stale = await asyncio.wait_for(stale_run, timeout=1)

    assert replacement[0].outcome is RecordingCleanupOutcome.SUCCEEDED
    assert stale[0].outcome is RecordingCleanupOutcome.LEASE_LOST
    assert stale[0].job is None
    assert find_cleanup(database) is None


async def test_heartbeat_database_failure_stops_publication(tmp_path: Path) -> None:
    database = migrated_database(tmp_path)
    add_cleanup(database, cleanup_job())
    executor = BlockingExecutor()
    failing_factory = FailingUnitOfWorkFactory(database, fail_on_commit=2)
    worker = cleanup_worker(
        database,
        executor,
        MutableClock(),
        unit_of_work=failing_factory,
        heartbeat_interval=timedelta(milliseconds=5),
    )

    running = asyncio.create_task(worker.run_once(1))
    assert await asyncio.to_thread(executor.started.wait, 1)
    await asyncio.sleep(0.02)
    executor.release.set()
    with pytest.raises(RuntimeError, match="publication unavailable"):
        await asyncio.wait_for(running, timeout=1)

    assert get_cleanup(database).status is RecordingCleanupStatus.RUNNING


async def test_cleanup_publication_failure_is_reclaimed_idempotently(tmp_path: Path) -> None:
    database = migrated_database(tmp_path)
    add_cleanup(database, cleanup_job())
    clock = MutableClock()
    present = True
    observed: list[bool] = []

    def delete_if_present(_job: RecordingCleanupJob) -> None:
        nonlocal present
        observed.append(present)
        present = False

    executor = FakeExecutor(delete_if_present)
    failing_factory = FailingUnitOfWorkFactory(database, fail_on_commit=2)
    first_worker = cleanup_worker(
        database,
        executor,
        clock,
        unit_of_work=failing_factory,
    )

    with pytest.raises(RuntimeError, match="publication unavailable"):
        await first_worker.run_once(1)
    running = get_cleanup(database)
    clock.current = (running.lease_expires_at or NOW) + timedelta(seconds=1)
    second = await cleanup_worker(
        database,
        executor,
        clock,
        worker_id="cleanup-two",
    ).run_once(1)

    assert observed == [True, False]
    assert second[0].outcome is RecordingCleanupOutcome.SUCCEEDED
    assert second[0].job is not None
    assert second[0].job.attempt_count == 1
    assert find_cleanup(database) is None


def test_cleanup_worker_validates_lease_and_batch_limits(tmp_path: Path) -> None:
    database = migrated_database(tmp_path)
    executor = FakeExecutor()
    clock = MutableClock()
    scheduler = FullJitterRetryScheduler(random_value=lambda: 0.5)

    with pytest.raises(ValueError, match="30 seconds"):
        RecordingCleanupWorker(
            unit_of_work=lambda: SqliteUnitOfWork(database),
            executor=executor,
            clock=clock,
            retry_scheduler=scheduler,
            worker_id="worker",
            lease_duration=timedelta(seconds=29),
        )
    with pytest.raises(ValueError, match="Heartbeat"):
        RecordingCleanupWorker(
            unit_of_work=lambda: SqliteUnitOfWork(database),
            executor=executor,
            clock=clock,
            retry_scheduler=scheduler,
            worker_id="worker",
            lease_duration=timedelta(seconds=30),
            heartbeat_interval=timedelta(seconds=30),
        )
    with pytest.raises(ValueError, match="Heartbeat"):
        RecordingCleanupWorker(
            unit_of_work=lambda: SqliteUnitOfWork(database),
            executor=executor,
            clock=clock,
            retry_scheduler=scheduler,
            worker_id="worker",
            lease_duration=timedelta(seconds=30),
            heartbeat_interval=timedelta(0),
        )
    with pytest.raises(ValueError, match="Worker ID"):
        RecordingCleanupWorker(
            unit_of_work=lambda: SqliteUnitOfWork(database),
            executor=executor,
            clock=clock,
            retry_scheduler=scheduler,
            worker_id=" worker ",
        )
    with pytest.raises(ValueError, match="one hour"):
        RecordingCleanupWorker(
            unit_of_work=lambda: SqliteUnitOfWork(database),
            executor=executor,
            clock=clock,
            retry_scheduler=scheduler,
            worker_id="worker",
            lease_duration=timedelta(hours=1, seconds=1),
        )
    with pytest.raises(ValueError, match="between one and five"):
        RecordingCleanupScheduler(
            unit_of_work=lambda: SqliteUnitOfWork(database),
            clock=clock,
            max_attempts=6,
        )


async def test_cleanup_worker_enforces_run_batch_bounds(tmp_path: Path) -> None:
    database = migrated_database(tmp_path)
    worker = cleanup_worker(database, FakeExecutor(), MutableClock())

    assert await worker.run_once(0) == ()
    assert await worker.run_once(-1) == ()
    with pytest.raises(ValueError, match="cannot exceed 100"):
        await worker.run_once(101)


async def test_orphan_discovery_checks_exact_ownership_before_hashing(
    tmp_path: Path,
) -> None:
    database = migrated_database(tmp_path)
    with SqliteUnitOfWork(database) as uow:
        uow.audio_assets.add(audio_asset(FIRST_KEY))
        uow.recording_cleanups.add(cleanup_job(storage_key=SECOND_KEY))
        uow.commit()
    candidates = (
        candidate(FIRST_KEY, 1),
        candidate(SECOND_KEY, 2),
        candidate(THIRD_KEY, 3),
    )
    scanner = FakeScanner(
        candidates,
        {
            FIRST_KEY: identity(FIRST_KEY),
            SECOND_KEY: identity(SECOND_KEY),
            THIRD_KEY: identity(THIRD_KEY),
        },
    )
    active = ActiveRecordings(frozenset({"." + "4" * 32 + ".part"}))
    scheduler = RecordingCleanupScheduler(
        unit_of_work=lambda: SqliteUnitOfWork(database),
        clock=MutableClock(),
        id_factory=lambda: UUID(int=30),
    )
    discoverer = RecordingOrphanDiscoverer(
        unit_of_work=lambda: SqliteUnitOfWork(database, immediate=False),
        scheduler=scheduler,
        scanner=scanner,
        active_recordings=active,
        clock=MutableClock(),
        grace_period=timedelta(days=1),
    )
    loop_thread = get_ident()

    first = await discoverer.run_once(2)
    second = await discoverer.run_once(2)

    assert first == OrphanDiscoveryBatch(scanned=2, owned=2)
    assert second == OrphanDiscoveryBatch(scanned=1, scheduled=1)
    assert scanner.identified == [THIRD_KEY]
    assert scanner.scan_calls[1][1] == SECOND_KEY
    assert scanner.scan_calls[0][2] == active.keys
    assert loop_thread not in scanner.thread_ids
    scheduled = get_cleanup(database, UUID(int=30))
    assert scheduled.storage_key == THIRD_KEY
    assert scheduled.reason is RecordingCleanupReason.ORPHAN_RECONCILIATION


async def test_orphan_discovery_classifies_changed_and_permanent_candidates(
    tmp_path: Path,
) -> None:
    database = migrated_database(tmp_path)
    scanner = FakeScanner(
        (candidate(FIRST_KEY, 1), candidate(SECOND_KEY, 2)),
        {
            FIRST_KEY: None,
            SECOND_KEY: PermanentRecordingCleanupError(),
        },
    )
    discoverer = RecordingOrphanDiscoverer(
        unit_of_work=lambda: SqliteUnitOfWork(database, immediate=False),
        scheduler=RecordingCleanupScheduler(
            unit_of_work=lambda: SqliteUnitOfWork(database),
            clock=MutableClock(),
        ),
        scanner=scanner,
        active_recordings=ActiveRecordings(),
        clock=MutableClock(),
        grace_period=timedelta(days=1),
    )

    batch = await discoverer.run_once(2)

    assert batch == OrphanDiscoveryBatch(scanned=2, changed=1, rejected=1)


async def test_orphan_discovery_propagates_retryable_identification_failure(
    tmp_path: Path,
) -> None:
    database = migrated_database(tmp_path)
    scanner = FakeScanner(
        (candidate(FIRST_KEY, 1),),
        {FIRST_KEY: RetryableRecordingCleanupError()},
    )
    discoverer = RecordingOrphanDiscoverer(
        unit_of_work=lambda: SqliteUnitOfWork(database, immediate=False),
        scheduler=RecordingCleanupScheduler(
            unit_of_work=lambda: SqliteUnitOfWork(database),
            clock=MutableClock(),
        ),
        scanner=scanner,
        active_recordings=ActiveRecordings(),
        clock=MutableClock(),
        grace_period=timedelta(days=1),
    )

    with pytest.raises(RetryableRecordingCleanupError):
        await discoverer.run_once(1)


async def test_orphan_discovery_retries_a_page_after_mid_page_failure(
    tmp_path: Path,
) -> None:
    database = migrated_database(tmp_path)
    scanner = FakeScanner(
        (
            candidate(FIRST_KEY, 1),
            candidate(SECOND_KEY, 2),
            candidate(THIRD_KEY, 3),
        ),
        {
            FIRST_KEY: identity(FIRST_KEY),
            SECOND_KEY: RetryableRecordingCleanupError(),
            THIRD_KEY: identity(THIRD_KEY),
        },
    )
    identifiers = iter((UUID(int=20), UUID(int=21), UUID(int=22)))
    discoverer = RecordingOrphanDiscoverer(
        unit_of_work=lambda: SqliteUnitOfWork(database, immediate=False),
        scheduler=RecordingCleanupScheduler(
            unit_of_work=lambda: SqliteUnitOfWork(database),
            clock=MutableClock(),
            id_factory=lambda: next(identifiers),
        ),
        scanner=scanner,
        active_recordings=ActiveRecordings(),
        clock=MutableClock(),
        grace_period=timedelta(days=1),
    )

    with pytest.raises(RetryableRecordingCleanupError):
        await discoverer.run_once(3)
    scanner.identities[SECOND_KEY] = identity(SECOND_KEY)
    recovered = await discoverer.run_once(3)

    assert scanner.scan_calls[0][1] is None
    assert scanner.scan_calls[1][1] is None
    assert scanner.identified == [FIRST_KEY, SECOND_KEY, SECOND_KEY, THIRD_KEY]
    assert recovered == OrphanDiscoveryBatch(scanned=3, owned=1, scheduled=2)


async def test_orphan_discovery_rejects_invalid_identity_and_scanner_order(
    tmp_path: Path,
) -> None:
    database = migrated_database(tmp_path)
    scanner = FakeScanner(
        (candidate(FIRST_KEY, 1),),
        {FIRST_KEY: identity(SECOND_KEY)},
    )
    discoverer = RecordingOrphanDiscoverer(
        unit_of_work=lambda: SqliteUnitOfWork(database, immediate=False),
        scheduler=RecordingCleanupScheduler(
            unit_of_work=lambda: SqliteUnitOfWork(database),
            clock=MutableClock(),
        ),
        scanner=scanner,
        active_recordings=ActiveRecordings(),
        clock=MutableClock(),
        grace_period=timedelta(days=1),
    )

    rejected = await discoverer.run_once(2)
    scanner.candidates = (candidate(SECOND_KEY, 2), candidate(FIRST_KEY, 1))
    with pytest.raises(ValueError, match="invalid candidate page"):
        await discoverer.run_once(2)

    assert rejected == OrphanDiscoveryBatch(scanned=1, rejected=1)


def test_stale_candidate_rejects_unsafe_file_descriptors() -> None:
    value = candidate(FIRST_KEY, 1)

    invalid_values: tuple[tuple[str, Callable[[], object]], ...] = (
        ("storage key", lambda: replace(value, storage_key="../recording.wav")),
        ("size cannot be negative", lambda: replace(value, size_bytes=-1)),
        ("UTC offset", lambda: replace(value, modified_at=NOW.replace(tzinfo=None))),
        ("stat identity", lambda: replace(value, stat_device=-1)),
        ("stat identity", lambda: replace(value, stat_inode=-1)),
        ("stat identity", lambda: replace(value, stat_modified_ns=-1)),
        ("stat identity", lambda: replace(value, stat_changed_ns=-1)),
    )

    for message, build in invalid_values:
        with pytest.raises(ValueError, match=message):
            build()


def test_recording_identity_rejects_unverifiable_descriptors() -> None:
    value = identity(FIRST_KEY)

    invalid_values: tuple[tuple[str, Callable[[], object]], ...] = (
        ("storage key", lambda: replace(value, storage_key="recording.wav")),
        ("lowercase SHA-256", lambda: replace(value, sha256="A" * 64)),
        ("lowercase SHA-256", lambda: replace(value, sha256="a" * 63)),
        ("size cannot be negative", lambda: replace(value, size_bytes=-1)),
    )

    for message, build in invalid_values:
        with pytest.raises(ValueError, match=message):
            build()


async def test_cleanup_worker_stops_heartbeat_after_lease_loss(tmp_path: Path) -> None:
    database = migrated_database(tmp_path)
    worker = cleanup_worker(
        database,
        FakeExecutor(),
        MutableClock(),
        heartbeat_interval=timedelta(milliseconds=1),
    )

    await asyncio.wait_for(worker._heartbeat(JOB_ID), timeout=0.1)


def test_cleanup_worker_rejects_publication_after_lease_expiry(tmp_path: Path) -> None:
    database = migrated_database(tmp_path)
    add_cleanup(database, cleanup_job())
    clock = MutableClock()
    worker = cleanup_worker(database, FakeExecutor(), clock)
    claimed = worker._claim(1)[0]
    clock.current = (claimed.lease_expires_at or NOW) + timedelta(microseconds=1)
    retryable = WorkflowFailure(
        code=FailureCode.INTERNAL,
        disposition=FailureDisposition.RETRYABLE,
        safe_message="Recording cleanup could not finish",
        occurred_at=clock.current,
    )

    assert worker._renew_lease(claimed.id) is None
    assert worker._finish_failure(claimed, retryable) is None
    assert get_cleanup(database).status is RecordingCleanupStatus.RUNNING


def test_orphan_discoverer_validates_grace_period(tmp_path: Path) -> None:
    database = migrated_database(tmp_path)
    scheduler = RecordingCleanupScheduler(
        unit_of_work=lambda: SqliteUnitOfWork(database),
        clock=MutableClock(),
    )

    for grace_period in (timedelta(minutes=4), timedelta(days=7, seconds=1)):
        with pytest.raises(ValueError, match="between five minutes and seven days"):
            RecordingOrphanDiscoverer(
                unit_of_work=lambda: SqliteUnitOfWork(database, immediate=False),
                scheduler=scheduler,
                scanner=FakeScanner((), {}),
                active_recordings=ActiveRecordings(),
                clock=MutableClock(),
                grace_period=grace_period,
            )


async def test_orphan_discoverer_enforces_batch_bounds(tmp_path: Path) -> None:
    database = migrated_database(tmp_path)
    discoverer = RecordingOrphanDiscoverer(
        unit_of_work=lambda: SqliteUnitOfWork(database, immediate=False),
        scheduler=RecordingCleanupScheduler(
            unit_of_work=lambda: SqliteUnitOfWork(database),
            clock=MutableClock(),
        ),
        scanner=FakeScanner((), {}),
        active_recordings=ActiveRecordings(),
        clock=MutableClock(),
        grace_period=timedelta(days=1),
    )

    assert await discoverer.run_once(0) == OrphanDiscoveryBatch()
    with pytest.raises(ValueError, match="cannot exceed 1000"):
        await discoverer.run_once(1_001)


async def test_orphan_discoverer_rejects_conflicting_cleanup_identity(
    tmp_path: Path,
) -> None:
    database = migrated_database(tmp_path)

    class ConflictingScheduler(RecordingCleanupScheduler):
        def schedule_if_unreferenced(
            self,
            *,
            storage_key: str,
            expected_sha256: str,
            expected_size_bytes: int,
            reason: RecordingCleanupReason,
        ) -> RecordingCleanupJob | None:
            del expected_sha256, expected_size_bytes, reason
            raise RecordingCleanupConflictError(storage_key)

    scanner = FakeScanner(
        (candidate(FIRST_KEY, 1),),
        {FIRST_KEY: identity(FIRST_KEY)},
    )
    discoverer = RecordingOrphanDiscoverer(
        unit_of_work=lambda: SqliteUnitOfWork(database, immediate=False),
        scheduler=ConflictingScheduler(
            unit_of_work=lambda: SqliteUnitOfWork(database),
            clock=MutableClock(),
        ),
        scanner=scanner,
        active_recordings=ActiveRecordings(),
        clock=MutableClock(),
        grace_period=timedelta(days=1),
    )

    assert await discoverer.run_once(1) == OrphanDiscoveryBatch(scanned=1, rejected=1)


async def test_orphan_discoverer_rejects_nonadvancing_page(tmp_path: Path) -> None:
    database = migrated_database(tmp_path)

    class NonAdvancingScanner(FakeScanner):
        def scan_stale_candidates(
            self,
            *,
            now: datetime,
            grace_period: timedelta,
            limit: int,
            after_storage_key: str | None = None,
            active_temporary_keys: Set[str] = frozenset(),
        ) -> tuple[StaleRecordingCandidate, ...]:
            del now, grace_period, after_storage_key, active_temporary_keys
            return self.candidates[:limit]

    scanner = NonAdvancingScanner(
        (candidate(FIRST_KEY, 1),),
        {FIRST_KEY: identity(FIRST_KEY)},
    )
    discoverer = RecordingOrphanDiscoverer(
        unit_of_work=lambda: SqliteUnitOfWork(database, immediate=False),
        scheduler=RecordingCleanupScheduler(
            unit_of_work=lambda: SqliteUnitOfWork(database),
            clock=MutableClock(),
        ),
        scanner=scanner,
        active_recordings=ActiveRecordings(),
        clock=MutableClock(),
        grace_period=timedelta(days=1),
    )

    await discoverer.run_once(1)
    with pytest.raises(ValueError, match="did not advance"):
        await discoverer.run_once(1)
