from __future__ import annotations

import asyncio
import sqlite3
from datetime import timedelta
from pathlib import Path
from uuid import UUID

import pytest

from meeting_action_orchestrator.application.processing import (
    ProcessingOutcome,
    ProcessingWorker,
)
from meeting_action_orchestrator.application.processing_control import ProcessingControlService
from meeting_action_orchestrator.application.state_machine import transition_meeting
from meeting_action_orchestrator.domain.enums import (
    FailureCode,
    FailureDisposition,
    MeetingStatus,
    ProcessingJobStatus,
    ProcessingStage,
)
from meeting_action_orchestrator.domain.models import Meeting, ProcessingJob, WorkflowFailure
from meeting_action_orchestrator.infrastructure.database import Database
from meeting_action_orchestrator.infrastructure.repositories import SqliteUnitOfWork
from tests.integration.test_workflow_processing import (
    FixedRetryScheduler,
    MutableClock,
    create_worker,
    create_workflow,
    ingest,
)


def classified_failure(clock: MutableClock) -> WorkflowFailure:
    return WorkflowFailure(
        code=FailureCode.INVALID_MODEL_OUTPUT,
        disposition=FailureDisposition.PERMANENT,
        safe_message="The provider response could not be validated",
        occurred_at=clock.now(),
    )


def recovery_worker(
    database: Database,
    clock: MutableClock,
    stage: ProcessingStage,
    calls: list[UUID],
    *,
    worker_id: str = "recovery-worker",
) -> ProcessingWorker:
    async def handler(job: ProcessingJob) -> None:
        calls.append(job.id)

    return ProcessingWorker(
        unit_of_work=lambda: SqliteUnitOfWork(database),
        handlers={stage: handler},
        clock=clock,
        retry_scheduler=FixedRetryScheduler(),
        worker_id=worker_id,
        lease_duration=timedelta(seconds=10),
    )


def expire_job(
    database: Database,
    meeting_id: UUID,
    stage: ProcessingStage,
    clock: MutableClock,
    *,
    lease_seconds: int = 10,
) -> ProcessingJob:
    with SqliteUnitOfWork(database) as uow:
        current = uow.processing_jobs.find_for_stage(meeting_id, stage)
        assert current is not None
        expired = ProcessingJob.model_validate(
            current.model_dump(mode="python")
            | {
                "status": ProcessingJobStatus.RUNNING,
                "attempt_count": current.max_attempts,
                "next_attempt_at": None,
                "lease_owner": "crashed-worker",
                "lease_expires_at": clock.now() + timedelta(seconds=lease_seconds),
                "last_failure": None,
                "updated_at": clock.now(),
            }
        )
        uow.processing_jobs.save(
            expired,
            current.status,
            current.lease_owner,
            current.lease_expires_at,
        )
        uow.commit()
    return expired


def prepare_meeting_status(
    database: Database,
    meeting_id: UUID,
    stage: ProcessingStage,
    target: MeetingStatus,
    clock: MutableClock,
) -> Meeting:
    states = {
        ProcessingStage.TRANSCRIPTION: (
            MeetingStatus.INGESTED,
            MeetingStatus.TRANSCRIBING,
            MeetingStatus.TRANSCRIPTION_FAILED,
        ),
        ProcessingStage.EXTRACTION: (
            MeetingStatus.TRANSCRIBED,
            MeetingStatus.EXTRACTING,
            MeetingStatus.EXTRACTION_FAILED,
        ),
    }
    pending, active, failed = states[stage]
    with SqliteUnitOfWork(database) as uow:
        current = uow.meetings.get(meeting_id)
        assert current is not None
        assert current.status is pending
        if target is pending:
            return current
        updated = transition_meeting(current, active, clock.now())
        if target is failed:
            updated = transition_meeting(
                updated,
                failed,
                clock.now(),
                failure=classified_failure(clock),
            )
        uow.meetings.save(updated, current.version)
        uow.commit()
    return updated


async def prepare_stage(
    tmp_path: Path,
    stage: ProcessingStage,
) -> tuple[MutableClock, object, Database, UUID]:
    clock = MutableClock()
    service, database = create_workflow(tmp_path, clock)
    meeting_id = ingest(service)
    if stage is ProcessingStage.EXTRACTION:
        result = await create_worker(
            service,
            database,
            clock,
            "transcription-worker",
        ).run_once(ProcessingStage.TRANSCRIPTION)
        assert result[0].outcome is ProcessingOutcome.SUCCEEDED
    return clock, service, database, meeting_id


@pytest.mark.parametrize(
    ("stage", "prepared_status", "version_increment"),
    [
        (ProcessingStage.TRANSCRIPTION, MeetingStatus.INGESTED, 2),
        (ProcessingStage.TRANSCRIPTION, MeetingStatus.TRANSCRIBING, 1),
        (ProcessingStage.TRANSCRIPTION, MeetingStatus.TRANSCRIPTION_FAILED, 0),
        (ProcessingStage.EXTRACTION, MeetingStatus.TRANSCRIBED, 2),
        (ProcessingStage.EXTRACTION, MeetingStatus.EXTRACTING, 1),
        (ProcessingStage.EXTRACTION, MeetingStatus.EXTRACTION_FAILED, 0),
    ],
)
async def test_expired_exhausted_jobs_repair_stage_state(
    tmp_path: Path,
    stage: ProcessingStage,
    prepared_status: MeetingStatus,
    version_increment: int,
) -> None:
    clock, _, database, meeting_id = await prepare_stage(tmp_path, stage)
    prepared = prepare_meeting_status(
        database,
        meeting_id,
        stage,
        prepared_status,
        clock,
    )
    expired = expire_job(database, meeting_id, stage, clock)
    clock.current += timedelta(seconds=10)
    calls: list[UUID] = []

    result = await recovery_worker(database, clock, stage, calls).run_once(stage)

    with SqliteUnitOfWork(database) as uow:
        meeting = uow.meetings.get(meeting_id)
        job = uow.processing_jobs.get(expired.id)
    assert result == ()
    assert calls == []
    assert meeting is not None
    assert job is not None
    assert meeting.status is (
        MeetingStatus.TRANSCRIPTION_FAILED
        if stage is ProcessingStage.TRANSCRIPTION
        else MeetingStatus.EXTRACTION_FAILED
    )
    assert meeting.version == prepared.version + version_increment
    assert job.status is ProcessingJobStatus.FAILED
    assert job.attempt_count == job.max_attempts
    assert job.lease_owner is None
    assert job.lease_expires_at is None
    if version_increment == 0:
        assert meeting == prepared
        assert job.last_failure == prepared.failure
    else:
        assert meeting.failure is not None
        assert meeting.failure.code is FailureCode.PROVIDER_TIMEOUT
        assert meeting.failure.disposition is FailureDisposition.RETRYABLE
        assert job.last_failure == meeting.failure


@pytest.mark.parametrize(
    "stage",
    [ProcessingStage.TRANSCRIPTION, ProcessingStage.EXTRACTION],
)
async def test_committed_artifact_repairs_job_without_provider_execution(
    tmp_path: Path,
    stage: ProcessingStage,
) -> None:
    clock = MutableClock()
    service, database = create_workflow(tmp_path, clock)
    meeting_id = ingest(service)
    stage_worker = create_worker(service, database, clock, "stage-worker")
    transcription = await stage_worker.run_once(ProcessingStage.TRANSCRIPTION)
    assert transcription[0].outcome is ProcessingOutcome.SUCCEEDED
    if stage is ProcessingStage.EXTRACTION:
        extraction = await stage_worker.run_once(ProcessingStage.EXTRACTION)
        assert extraction[0].outcome is ProcessingOutcome.SUCCEEDED
    with SqliteUnitOfWork(database) as uow:
        original_meeting = uow.meetings.get(meeting_id)
        original_jobs = uow.processing_jobs.list_for_meeting(meeting_id)
    assert original_meeting is not None
    expired = expire_job(database, meeting_id, stage, clock)
    clock.current += timedelta(seconds=10)
    calls: list[UUID] = []

    result = await recovery_worker(database, clock, stage, calls).run_once(stage)

    with SqliteUnitOfWork(database) as uow:
        meeting = uow.meetings.get(meeting_id)
        job = uow.processing_jobs.get(expired.id)
        current_jobs = uow.processing_jobs.list_for_meeting(meeting_id)
    assert result == ()
    assert calls == []
    assert meeting == original_meeting
    assert job is not None
    assert job.status is ProcessingJobStatus.SUCCEEDED
    assert job.attempt_count == job.max_attempts
    assert job.last_failure is None
    assert tuple(item.id for item in current_jobs) == tuple(item.id for item in original_jobs)


@pytest.mark.parametrize(
    "stage",
    [ProcessingStage.TRANSCRIPTION, ProcessingStage.EXTRACTION],
)
async def test_cancelled_meeting_cancels_expired_exhausted_job(
    tmp_path: Path,
    stage: ProcessingStage,
) -> None:
    clock, _, database, meeting_id = await prepare_stage(tmp_path, stage)
    with SqliteUnitOfWork(database) as uow:
        current = uow.meetings.get(meeting_id)
        assert current is not None
        cancelled = transition_meeting(current, MeetingStatus.CANCELLED, clock.now())
        uow.meetings.save(cancelled, current.version)
        uow.commit()
    expired = expire_job(database, meeting_id, stage, clock)
    clock.current += timedelta(seconds=10)
    calls: list[UUID] = []

    result = await recovery_worker(
        database,
        clock,
        stage,
        calls,
    ).run_once(stage)

    with SqliteUnitOfWork(database) as uow:
        meeting = uow.meetings.get(meeting_id)
        job = uow.processing_jobs.get(expired.id)
    assert result == ()
    assert calls == []
    assert meeting == cancelled
    assert job is not None
    assert job.status is ProcessingJobStatus.CANCELLED
    assert job.last_failure is None


@pytest.mark.parametrize(
    "stage",
    [ProcessingStage.TRANSCRIPTION, ProcessingStage.EXTRACTION],
)
async def test_inconsistent_meeting_fails_job_without_mutating_meeting(
    tmp_path: Path,
    stage: ProcessingStage,
) -> None:
    clock, _, database, meeting_id = await prepare_stage(tmp_path, stage)
    with SqliteUnitOfWork(database) as uow:
        current = uow.meetings.get(meeting_id)
        assert current is not None
        updates = (
            {
                "status": MeetingStatus.TRANSCRIBED,
                "current_transcript_id": UUID("40000000-0000-4000-8000-000000000001"),
                "version": current.version + 1,
            }
            if stage is ProcessingStage.TRANSCRIPTION
            else {
                "status": MeetingStatus.AWAITING_APPROVAL,
                "current_review_id": UUID("40000000-0000-4000-8000-000000000002"),
                "version": current.version + 1,
            }
        )
        inconsistent = Meeting.model_validate(current.model_dump(mode="python") | updates)
        uow.meetings.save(inconsistent, current.version)
        uow.commit()
    expired = expire_job(
        database,
        meeting_id,
        stage,
        clock,
    )
    clock.current += timedelta(seconds=10)
    calls: list[UUID] = []

    result = await recovery_worker(
        database,
        clock,
        stage,
        calls,
    ).run_once(stage)

    with SqliteUnitOfWork(database) as uow:
        meeting = uow.meetings.get(meeting_id)
        job = uow.processing_jobs.get(expired.id)
    assert result == ()
    assert calls == []
    assert meeting == inconsistent
    assert job is not None
    assert job.status is ProcessingJobStatus.FAILED
    assert job.last_failure is not None
    assert job.last_failure.code is FailureCode.INTERNAL
    assert job.last_failure.disposition is FailureDisposition.PERMANENT


async def test_repair_only_run_honors_limit_and_returns_no_results(tmp_path: Path) -> None:
    clock = MutableClock()
    service, database = create_workflow(tmp_path, clock)
    meeting_ids = tuple(ingest(service, f"recovery-batch-{index}") for index in range(3))
    expirations = (1, 2, 3)
    expired_jobs = tuple(
        expire_job(
            database,
            meeting_id,
            ProcessingStage.TRANSCRIPTION,
            clock,
            lease_seconds=lease_seconds,
        )
        for meeting_id, lease_seconds in zip(meeting_ids, expirations, strict=True)
    )
    clock.current += timedelta(seconds=3)
    calls: list[UUID] = []

    result = await recovery_worker(
        database,
        clock,
        ProcessingStage.TRANSCRIPTION,
        calls,
    ).run_once(ProcessingStage.TRANSCRIPTION, limit=2)

    with SqliteUnitOfWork(database) as uow:
        jobs = tuple(uow.processing_jobs.get(job.id) for job in expired_jobs)
        meetings = tuple(uow.meetings.get(meeting_id) for meeting_id in meeting_ids)
    assert result == ()
    assert calls == []
    assert all(job is not None for job in jobs)
    assert tuple(job.status for job in jobs if job is not None) == (
        ProcessingJobStatus.FAILED,
        ProcessingJobStatus.FAILED,
        ProcessingJobStatus.RUNNING,
    )
    assert all(meeting is not None for meeting in meetings)
    assert tuple(meeting.status for meeting in meetings if meeting is not None) == (
        MeetingStatus.TRANSCRIPTION_FAILED,
        MeetingStatus.TRANSCRIPTION_FAILED,
        MeetingStatus.INGESTED,
    )


async def test_repair_and_claim_roll_back_together(tmp_path: Path) -> None:
    clock = MutableClock()
    service, database = create_workflow(tmp_path, clock)
    expired_meeting_id = ingest(service, "recovery-rollback-expired")
    due_meeting_id = ingest(service, "recovery-rollback-due")
    expired = expire_job(
        database,
        expired_meeting_id,
        ProcessingStage.TRANSCRIPTION,
        clock,
    )
    with SqliteUnitOfWork(database) as uow:
        due = uow.processing_jobs.find_for_stage(
            due_meeting_id,
            ProcessingStage.TRANSCRIPTION,
        )
        original_meeting = uow.meetings.get(expired_meeting_id)
    assert due is not None
    assert original_meeting is not None
    with database.transaction(immediate=True) as connection:
        connection.execute(
            """
            CREATE TRIGGER reject_due_claim
            BEFORE UPDATE ON processing_jobs
            WHEN OLD.status = 'ready' AND NEW.status = 'running'
            BEGIN
                SELECT RAISE(ABORT, 'rejected');
            END
            """
        )
    clock.current += timedelta(seconds=10)
    calls: list[UUID] = []

    with pytest.raises(sqlite3.IntegrityError):
        await recovery_worker(
            database,
            clock,
            ProcessingStage.TRANSCRIPTION,
            calls,
        ).run_once(ProcessingStage.TRANSCRIPTION)

    with SqliteUnitOfWork(database) as uow:
        persisted_meeting = uow.meetings.get(expired_meeting_id)
        persisted_expired = uow.processing_jobs.get(expired.id)
        persisted_due = uow.processing_jobs.get(due.id)
    assert calls == []
    assert persisted_meeting == original_meeting
    assert persisted_expired == expired
    assert persisted_due == due


async def test_failed_repair_can_be_requeued_explicitly(tmp_path: Path) -> None:
    clock, _, database, meeting_id = await prepare_stage(
        tmp_path,
        ProcessingStage.TRANSCRIPTION,
    )
    expire_job(database, meeting_id, ProcessingStage.TRANSCRIPTION, clock)
    clock.current += timedelta(seconds=10)
    calls: list[UUID] = []
    await recovery_worker(
        database,
        clock,
        ProcessingStage.TRANSCRIPTION,
        calls,
    ).run_once(ProcessingStage.TRANSCRIPTION)
    with SqliteUnitOfWork(database) as uow:
        failed_meeting = uow.meetings.get(meeting_id)
    assert failed_meeting is not None

    result = await ProcessingControlService(
        unit_of_work=lambda: SqliteUnitOfWork(database),
        clock=clock,
    ).retry(
        meeting_id,
        expected_version=failed_meeting.version,
        request_key="retry-recovered-processing",
        actor_id="owner",
    )

    assert result.meeting.status is MeetingStatus.TRANSCRIPTION_FAILED
    assert result.meeting.version == failed_meeting.version + 1
    assert result.jobs[0].status is ProcessingJobStatus.READY
    assert result.jobs[0].attempt_count == 0
    assert result.jobs[0].last_failure is None


async def test_stale_worker_cannot_overwrite_repaired_job(tmp_path: Path) -> None:
    clock = MutableClock()
    service, database = create_workflow(tmp_path, clock)
    meeting_id = ingest(service)
    with SqliteUnitOfWork(database) as uow:
        current = uow.processing_jobs.find_for_stage(
            meeting_id,
            ProcessingStage.TRANSCRIPTION,
        )
        assert current is not None
        nearly_exhausted = ProcessingJob.model_validate(
            current.model_dump(mode="python") | {"attempt_count": current.max_attempts - 1}
        )
        uow.processing_jobs.save(
            nearly_exhausted,
            current.status,
            current.lease_owner,
            current.lease_expires_at,
        )
        uow.commit()
    started = asyncio.Event()
    released = asyncio.Event()

    async def delayed_handler(job: ProcessingJob) -> None:
        del job
        started.set()
        await released.wait()

    stale_worker = ProcessingWorker(
        unit_of_work=lambda: SqliteUnitOfWork(database),
        handlers={ProcessingStage.TRANSCRIPTION: delayed_handler},
        clock=clock,
        retry_scheduler=FixedRetryScheduler(),
        worker_id="stale-worker",
        lease_duration=timedelta(seconds=10),
    )
    stale_task = asyncio.create_task(stale_worker.run_once(ProcessingStage.TRANSCRIPTION))
    await started.wait()
    clock.current += timedelta(seconds=10)
    recovery_calls: list[UUID] = []

    recovered = await recovery_worker(
        database,
        clock,
        ProcessingStage.TRANSCRIPTION,
        recovery_calls,
        worker_id="replacement-worker",
    ).run_once(ProcessingStage.TRANSCRIPTION)
    released.set()
    stale_result = await stale_task

    with SqliteUnitOfWork(database) as uow:
        job = uow.processing_jobs.find_for_stage(
            meeting_id,
            ProcessingStage.TRANSCRIPTION,
        )
    assert recovered == ()
    assert recovery_calls == []
    assert stale_result[0].outcome is ProcessingOutcome.LEASE_LOST
    assert job is not None
    assert job.status is ProcessingJobStatus.FAILED
    assert job.last_failure is not None
    assert job.last_failure.code is FailureCode.PROVIDER_TIMEOUT
