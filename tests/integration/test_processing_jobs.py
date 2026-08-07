from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID

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


@dataclass
class FrozenClock:
    current: datetime = NOW

    def now(self) -> datetime:
        return self.current


class FixedRetryScheduler:
    def __init__(self, delay: timedelta = timedelta(seconds=30)) -> None:
        self.delay = delay
        self.calls: list[int] = []

    def schedule(self, now: datetime, attempt_count: int) -> datetime:
        self.calls.append(attempt_count)
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
            retryable_failure(NOW),
        )
        uow.commit()
    with SqliteUnitOfWork(database) as uow:
        unavailable = uow.processing_jobs.claim_due(
            ProcessingStage.EXTRACTION,
            "worker-b",
            NOW + timedelta(seconds=5),
            NOW + timedelta(seconds=15),
            1,
            retryable_failure(NOW + timedelta(seconds=5)),
        )
        uow.commit()
    with SqliteUnitOfWork(database) as uow:
        reclaimed = uow.processing_jobs.claim_due(
            ProcessingStage.EXTRACTION,
            "worker-b",
            NOW + timedelta(seconds=10),
            NOW + timedelta(seconds=20),
            1,
            retryable_failure(NOW + timedelta(seconds=10)),
        )
        uow.commit()
    with SqliteUnitOfWork(database) as uow:
        exhausted = uow.processing_jobs.claim_due(
            ProcessingStage.EXTRACTION,
            "worker-c",
            NOW + timedelta(seconds=20),
            NOW + timedelta(seconds=30),
            1,
            retryable_failure(NOW + timedelta(seconds=20)),
        )
        current = uow.processing_jobs.get(JOB_ID)
        uow.commit()

    assert first[0].attempt_count == 1
    assert unavailable == ()
    assert reclaimed[0].attempt_count == 2
    assert exhausted == ()
    assert current is not None
    assert current.status is ProcessingJobStatus.FAILED
    assert current.attempt_count == 2
    assert current.last_failure is not None
    assert current.last_failure.disposition is FailureDisposition.RETRYABLE


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
    assert retry_scheduler.calls == [1]
    assert second[0].outcome is ProcessingOutcome.SUCCEEDED
    assert second[0].job is not None
    assert second[0].job.attempt_count == 2


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
