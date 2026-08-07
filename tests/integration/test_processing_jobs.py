from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import get_ident
from uuid import UUID

import pytest

from meeting_action_orchestrator.application.processing import (
    ProcessingOutcome,
    ProcessingScheduler,
    ProcessingWorker,
)
from meeting_action_orchestrator.domain.enums import (
    AudioMediaType,
    FailureCode,
    FailureDisposition,
    ProcessingJobStatus,
    ProcessingStage,
)
from meeting_action_orchestrator.domain.models import (
    AudioAsset,
    Meeting,
    ProcessingJob,
    WorkflowFailure,
)
from meeting_action_orchestrator.infrastructure.database import Database
from meeting_action_orchestrator.infrastructure.repositories import SqliteUnitOfWork

NOW = datetime(2026, 8, 7, 9, 0, tzinfo=timezone.utc)
ASSET_ID = UUID("0bb1da7c-589a-4e06-a69c-73c10cbec648")
MEETING_ID = UUID("1040ddb5-cf1d-4e17-9a7f-b988457ef460")
JOB_ID = UUID("7be498ba-698d-492a-9f45-2483a7966af7")
SECOND_MEETING_ID = UUID("45ad5a89-ce5f-48f0-b943-554b96a980b4")
SECOND_JOB_ID = UUID("d91c9f62-3314-4144-ac77-986388a65ab2")


@dataclass
class FrozenClock:
    current: datetime = NOW

    def now(self) -> datetime:
        return self.current


class FixedRetryScheduler:
    def __init__(self, delay: timedelta = timedelta(seconds=30)) -> None:
        self.delay = delay
        self.calls: list[int] = []
        self.bases: list[datetime] = []

    def schedule(self, now: datetime, attempt_count: int) -> datetime:
        self.calls.append(attempt_count)
        self.bases.append(now)
        return now + self.delay


def create_database(path: Path) -> Database:
    database = Database(path)
    database.migrate()
    with SqliteUnitOfWork(database) as uow:
        uow.audio_assets.add(
            AudioAsset(
                id=ASSET_ID,
                storage_key="recording.wav",
                original_name="recording.wav",
                detected_media_type=AudioMediaType.WAV,
                size_bytes=1_024,
                duration_ms=60_000,
                sha256="a" * 64,
                created_at=NOW,
            )
        )
        uow.meetings.add(
            Meeting(
                id=MEETING_ID,
                ingest_key="processing-test",
                title="Processing test",
                audio_asset_id=ASSET_ID,
                occurred_at=NOW,
                timezone="UTC",
                created_at=NOW,
                updated_at=NOW,
            )
        )
        uow.commit()
    return database


def create_scheduler(database: Database, clock: FrozenClock) -> ProcessingScheduler:
    return ProcessingScheduler(
        unit_of_work=lambda: SqliteUnitOfWork(database),
        clock=clock,
        id_factory=lambda: JOB_ID,
    )


def retryable_failure(at: datetime) -> WorkflowFailure:
    return WorkflowFailure(
        code=FailureCode.PROVIDER_UNAVAILABLE,
        disposition=FailureDisposition.RETRYABLE,
        safe_message="The provider is temporarily unavailable",
        occurred_at=at,
    )


def test_enqueue_is_idempotent_and_uses_stage_limit(tmp_path: Path) -> None:
    database = create_database(tmp_path / "application.sqlite3")
    clock = FrozenClock()
    scheduler = create_scheduler(database, clock)

    first = scheduler.enqueue(MEETING_ID, ProcessingStage.TRANSCRIPTION)
    replay = scheduler.enqueue(MEETING_ID, ProcessingStage.TRANSCRIPTION)

    assert replay == first
    assert first.max_attempts == 3
    with SqliteUnitOfWork(database) as uow:
        assert uow.processing_jobs.list_for_meeting(MEETING_ID) == (first,)


def test_expired_lease_is_reclaimed_until_attempts_are_exhausted(tmp_path: Path) -> None:
    database = create_database(tmp_path / "application.sqlite3")
    clock = FrozenClock()
    scheduler = create_scheduler(database, clock)
    scheduler.enqueue(MEETING_ID, ProcessingStage.EXTRACTION)

    with SqliteUnitOfWork(database) as uow:
        first = uow.processing_jobs.claim_due(
            ProcessingStage.EXTRACTION,
            "worker-a",
            NOW,
            NOW + timedelta(seconds=10),
            1,
        )
        uow.commit()
    with SqliteUnitOfWork(database) as uow:
        unavailable = uow.processing_jobs.claim_due(
            ProcessingStage.EXTRACTION,
            "worker-b",
            NOW + timedelta(seconds=5),
            NOW + timedelta(seconds=15),
            1,
        )
        uow.commit()
    with SqliteUnitOfWork(database) as uow:
        reclaimed = uow.processing_jobs.claim_due(
            ProcessingStage.EXTRACTION,
            "worker-b",
            NOW + timedelta(seconds=10),
            NOW + timedelta(seconds=20),
            1,
        )
        uow.commit()
    with SqliteUnitOfWork(database) as uow:
        exhausted = uow.processing_jobs.claim_due(
            ProcessingStage.EXTRACTION,
            "worker-c",
            NOW + timedelta(seconds=20),
            NOW + timedelta(seconds=30),
            1,
        )
        current = uow.processing_jobs.get(JOB_ID)
        expired = uow.processing_jobs.list_expired_exhausted(
            ProcessingStage.EXTRACTION,
            NOW + timedelta(seconds=20),
            1,
        )
        uow.commit()

    assert first[0].attempt_count == 1
    assert unavailable == ()
    assert reclaimed[0].attempt_count == 2
    assert exhausted == ()
    assert current is not None
    assert current.status is ProcessingJobStatus.RUNNING
    assert current.attempt_count == 2
    assert current.last_failure is None
    assert expired == (current,)


def test_expired_exhausted_lookup_filters_orders_and_limits(tmp_path: Path) -> None:
    database = create_database(tmp_path / "application.sqlite3")
    meeting_ids = tuple(UUID(f"20000000-0000-4000-8000-{index:012d}") for index in range(1, 7))
    job_ids = tuple(UUID(f"30000000-0000-4000-8000-{index:012d}") for index in range(1, 7))
    with SqliteUnitOfWork(database) as uow:
        for index, meeting_id in enumerate(meeting_ids, start=1):
            uow.meetings.add(
                Meeting(
                    id=meeting_id,
                    ingest_key=f"expired-{index}",
                    title=f"Expired {index}",
                    audio_asset_id=ASSET_ID,
                    occurred_at=NOW,
                    timezone="UTC",
                    created_at=NOW,
                    updated_at=NOW,
                )
            )
        jobs = (
            ProcessingJob(
                id=job_ids[3],
                meeting_id=meeting_ids[3],
                stage=ProcessingStage.EXTRACTION,
                status=ProcessingJobStatus.RUNNING,
                attempt_count=2,
                max_attempts=2,
                lease_owner="worker-a",
                lease_expires_at=NOW + timedelta(seconds=4),
                created_at=NOW,
                updated_at=NOW,
            ),
            ProcessingJob(
                id=job_ids[1],
                meeting_id=meeting_ids[1],
                stage=ProcessingStage.EXTRACTION,
                status=ProcessingJobStatus.RUNNING,
                attempt_count=2,
                max_attempts=2,
                lease_owner="worker-a",
                lease_expires_at=NOW + timedelta(seconds=5),
                created_at=NOW,
                updated_at=NOW,
            ),
            ProcessingJob(
                id=job_ids[0],
                meeting_id=meeting_ids[0],
                stage=ProcessingStage.EXTRACTION,
                status=ProcessingJobStatus.RUNNING,
                attempt_count=2,
                max_attempts=2,
                lease_owner="worker-a",
                lease_expires_at=NOW + timedelta(seconds=5),
                created_at=NOW,
                updated_at=NOW,
            ),
            ProcessingJob(
                id=job_ids[2],
                meeting_id=meeting_ids[2],
                stage=ProcessingStage.EXTRACTION,
                status=ProcessingJobStatus.RUNNING,
                attempt_count=2,
                max_attempts=2,
                lease_owner="worker-a",
                lease_expires_at=NOW + timedelta(seconds=5),
                created_at=NOW + timedelta(seconds=1),
                updated_at=NOW + timedelta(seconds=1),
            ),
            ProcessingJob(
                id=job_ids[4],
                meeting_id=meeting_ids[4],
                stage=ProcessingStage.EXTRACTION,
                status=ProcessingJobStatus.RUNNING,
                attempt_count=1,
                max_attempts=2,
                lease_owner="worker-a",
                lease_expires_at=NOW + timedelta(seconds=4),
                created_at=NOW,
                updated_at=NOW,
            ),
            ProcessingJob(
                id=job_ids[5],
                meeting_id=meeting_ids[5],
                stage=ProcessingStage.TRANSCRIPTION,
                status=ProcessingJobStatus.RUNNING,
                attempt_count=3,
                max_attempts=3,
                lease_owner="worker-a",
                lease_expires_at=NOW + timedelta(seconds=4),
                created_at=NOW,
                updated_at=NOW,
            ),
        )
        for job in jobs:
            uow.processing_jobs.add(job)
        exact_limited = uow.processing_jobs.list_expired_exhausted(
            ProcessingStage.EXTRACTION,
            NOW + timedelta(seconds=5),
            3,
        )
        exact_all = uow.processing_jobs.list_expired_exhausted(
            ProcessingStage.EXTRACTION,
            NOW + timedelta(seconds=5),
            10,
        )
        before = uow.processing_jobs.list_expired_exhausted(
            ProcessingStage.EXTRACTION,
            NOW + timedelta(seconds=3),
            10,
        )
        empty_limit = uow.processing_jobs.list_expired_exhausted(
            ProcessingStage.EXTRACTION,
            NOW + timedelta(seconds=5),
            0,
        )
        uow.commit()

    assert tuple(job.id for job in exact_limited) == (
        job_ids[3],
        job_ids[0],
        job_ids[1],
    )
    assert tuple(job.id for job in exact_all) == (
        job_ids[3],
        job_ids[0],
        job_ids[1],
        job_ids[2],
    )
    assert before == ()
    assert empty_limit == ()


async def test_worker_releases_claim_transaction_before_handler(tmp_path: Path) -> None:
    database = create_database(tmp_path / "application.sqlite3")
    clock = FrozenClock()
    retry_scheduler = FixedRetryScheduler()
    scheduler = create_scheduler(database, clock)
    scheduler.enqueue(MEETING_ID, ProcessingStage.TRANSCRIPTION)
    calls = 0

    async def handler(job: ProcessingJob) -> WorkflowFailure | None:
        nonlocal calls
        calls += 1
        with database.transaction(immediate=True) as connection:
            connection.execute("SELECT 1")
        return retryable_failure(clock.now()) if calls == 1 else None

    worker = ProcessingWorker(
        unit_of_work=lambda: SqliteUnitOfWork(database),
        handlers={ProcessingStage.TRANSCRIPTION: handler},
        clock=clock,
        retry_scheduler=retry_scheduler,
        worker_id="worker-a",
        lease_duration=timedelta(minutes=1),
    )

    first = await worker.run_once(ProcessingStage.TRANSCRIPTION)
    clock.current += timedelta(seconds=30)
    second = await worker.run_once(ProcessingStage.TRANSCRIPTION)

    assert first[0].outcome is ProcessingOutcome.RETRY_SCHEDULED
    assert first[0].job is not None
    assert first[0].job.status is ProcessingJobStatus.RETRY_WAIT
    assert first[0].job.next_attempt_at == NOW + timedelta(seconds=30)
    assert retry_scheduler.calls == [1]
    assert retry_scheduler.bases == [NOW]
    assert second[0].outcome is ProcessingOutcome.SUCCEEDED
    assert second[0].job is not None
    assert second[0].job.attempt_count == 2


@pytest.mark.parametrize(
    ("provider_delay", "local_delay", "expected_delay"),
    [(60.0, 30.0, 90.0), (10.0, 30.0, 40.0)],
)
async def test_worker_respects_local_and_provider_retry_minimums(
    tmp_path: Path,
    provider_delay: float,
    local_delay: float,
    expected_delay: float,
) -> None:
    database = create_database(tmp_path / "application.sqlite3")
    clock = FrozenClock()
    create_scheduler(database, clock).enqueue(MEETING_ID, ProcessingStage.TRANSCRIPTION)
    failure = WorkflowFailure(
        code=FailureCode.RATE_LIMITED,
        disposition=FailureDisposition.RETRYABLE,
        safe_message="The provider is temporarily unavailable",
        retry_after_seconds=provider_delay,
        occurred_at=clock.now(),
    )

    async def handler(job: ProcessingJob) -> WorkflowFailure:
        del job
        return failure

    retry_scheduler = FixedRetryScheduler(timedelta(seconds=local_delay))
    worker = ProcessingWorker(
        unit_of_work=lambda: SqliteUnitOfWork(database),
        handlers={ProcessingStage.TRANSCRIPTION: handler},
        clock=clock,
        retry_scheduler=retry_scheduler,
        worker_id="worker-a",
        lease_duration=timedelta(minutes=1),
    )

    result = await worker.run_once(ProcessingStage.TRANSCRIPTION)

    assert result[0].outcome is ProcessingOutcome.RETRY_SCHEDULED
    assert result[0].job is not None
    assert result[0].job.next_attempt_at == NOW + timedelta(seconds=expected_delay)
    assert result[0].job.next_attempt_at > NOW + timedelta(seconds=provider_delay)
    assert retry_scheduler.bases == [NOW + timedelta(seconds=provider_delay)]
    assert result[0].job.last_failure == failure
    with SqliteUnitOfWork(database) as restarted:
        persisted = restarted.processing_jobs.get(JOB_ID)
    assert persisted is not None
    assert persisted.last_failure == failure


async def test_worker_claims_each_job_with_a_fresh_lease(tmp_path: Path) -> None:
    database = create_database(tmp_path / "application.sqlite3")
    clock = FrozenClock()
    with SqliteUnitOfWork(database) as uow:
        uow.meetings.add(
            Meeting(
                id=SECOND_MEETING_ID,
                ingest_key="processing-test-two",
                title="Second processing test",
                audio_asset_id=ASSET_ID,
                occurred_at=NOW,
                timezone="UTC",
                created_at=NOW,
                updated_at=NOW,
            )
        )
        uow.commit()
    create_scheduler(database, clock).enqueue(MEETING_ID, ProcessingStage.TRANSCRIPTION)
    ProcessingScheduler(
        unit_of_work=lambda: SqliteUnitOfWork(database),
        clock=clock,
        id_factory=lambda: SECOND_JOB_ID,
    ).enqueue(SECOND_MEETING_ID, ProcessingStage.TRANSCRIPTION)
    lease_expirations: list[datetime] = []
    provider_threads: list[int] = []
    persistence_threads: set[int] = set()
    event_loop_thread = get_ident()

    async def handler(job: ProcessingJob) -> WorkflowFailure | None:
        assert job.lease_expires_at is not None
        lease_expirations.append(job.lease_expires_at)
        provider_threads.append(get_ident())
        clock.current += timedelta(seconds=9)
        return None

    def unit_of_work() -> SqliteUnitOfWork:
        persistence_threads.add(get_ident())
        return SqliteUnitOfWork(database)

    worker = ProcessingWorker(
        unit_of_work=unit_of_work,
        handlers={ProcessingStage.TRANSCRIPTION: handler},
        clock=clock,
        retry_scheduler=FixedRetryScheduler(),
        worker_id="worker-a",
        lease_duration=timedelta(seconds=10),
    )

    results = await worker.run_once(ProcessingStage.TRANSCRIPTION, limit=2)

    assert [result.outcome for result in results] == [
        ProcessingOutcome.SUCCEEDED,
        ProcessingOutcome.SUCCEEDED,
    ]
    assert lease_expirations == [
        NOW + timedelta(seconds=10),
        NOW + timedelta(seconds=19),
    ]
    assert provider_threads == [event_loop_thread, event_loop_thread]
    assert persistence_threads
    assert event_loop_thread not in persistence_threads


async def test_worker_does_not_retry_permanent_failure(tmp_path: Path) -> None:
    database = create_database(tmp_path / "application.sqlite3")
    clock = FrozenClock()
    scheduler = create_scheduler(database, clock)
    scheduler.enqueue(MEETING_ID, ProcessingStage.TRANSCRIPTION)
    retry_scheduler = FixedRetryScheduler()

    async def handler(job: ProcessingJob) -> WorkflowFailure:
        return WorkflowFailure(
            code=FailureCode.INVALID_INPUT,
            disposition=FailureDisposition.PERMANENT,
            safe_message="The recording is invalid",
            occurred_at=clock.now(),
        )

    worker = ProcessingWorker(
        unit_of_work=lambda: SqliteUnitOfWork(database),
        handlers={ProcessingStage.TRANSCRIPTION: handler},
        clock=clock,
        retry_scheduler=retry_scheduler,
        worker_id="worker-a",
    )

    result = await worker.run_once(ProcessingStage.TRANSCRIPTION)

    assert result[0].outcome is ProcessingOutcome.FAILED
    assert result[0].job is not None
    assert result[0].job.status is ProcessingJobStatus.FAILED
    assert retry_scheduler.calls == []


async def test_worker_rejects_completion_after_lease_expiry(tmp_path: Path) -> None:
    database = create_database(tmp_path / "application.sqlite3")
    clock = FrozenClock()
    scheduler = create_scheduler(database, clock)
    scheduler.enqueue(MEETING_ID, ProcessingStage.TRANSCRIPTION)

    async def handler(job: ProcessingJob) -> None:
        clock.current += timedelta(seconds=11)

    worker = ProcessingWorker(
        unit_of_work=lambda: SqliteUnitOfWork(database),
        handlers={ProcessingStage.TRANSCRIPTION: handler},
        clock=clock,
        retry_scheduler=FixedRetryScheduler(),
        worker_id="worker-a",
        lease_duration=timedelta(seconds=10),
    )

    result = await worker.run_once(ProcessingStage.TRANSCRIPTION)

    assert result[0].outcome is ProcessingOutcome.LEASE_LOST
    with SqliteUnitOfWork(database) as uow:
        current = uow.processing_jobs.get(JOB_ID)
    assert current is not None
    assert current.status is ProcessingJobStatus.RUNNING
