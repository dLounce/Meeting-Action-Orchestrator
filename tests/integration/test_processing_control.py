from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from uuid import UUID

import pytest

from meeting_action_orchestrator.application.errors import (
    OperationConflictError,
    StaleWorkflowVersionError,
    WorkflowBusyError,
)
from meeting_action_orchestrator.application.processing_control import ProcessingControlService
from meeting_action_orchestrator.domain.enums import (
    FailureCode,
    FailureDisposition,
    MeetingStatus,
    ProcessingJobStatus,
    ProcessingStage,
)
from meeting_action_orchestrator.domain.errors import IdempotencyConflictError
from meeting_action_orchestrator.domain.models import Meeting, ProcessingJob, WorkflowFailure
from meeting_action_orchestrator.infrastructure.database import Database
from meeting_action_orchestrator.infrastructure.repositories import SqliteUnitOfWork
from tests.integration.test_processing_jobs import (
    MEETING_ID,
    NOW,
    create_database,
)

JOB_ID = UUID("70000000-0000-4000-8000-000000000001")
EXTRACTION_JOB_ID = UUID("70000000-0000-4000-8000-000000000002")
CONTROL_NOW = NOW + timedelta(minutes=1)


@dataclass
class FrozenClock:
    current: datetime = CONTROL_NOW

    def now(self) -> datetime:
        return self.current


def failure() -> WorkflowFailure:
    return WorkflowFailure(
        code=FailureCode.PROVIDER_UNAVAILABLE,
        disposition=FailureDisposition.PERMANENT,
        safe_message="The provider rejected this processing attempt",
        occurred_at=NOW,
    )


def retry_failure() -> WorkflowFailure:
    return WorkflowFailure(
        code=FailureCode.PROVIDER_UNAVAILABLE,
        disposition=FailureDisposition.RETRYABLE,
        safe_message="The provider is temporarily unavailable",
        occurred_at=NOW,
    )


def service(database: Database) -> ProcessingControlService:
    return ProcessingControlService(
        unit_of_work=lambda: SqliteUnitOfWork(database),
        clock=FrozenClock(),
    )


def set_meeting(
    database: Database,
    *,
    status: MeetingStatus,
    version: int = 0,
) -> Meeting:
    with SqliteUnitOfWork(database) as uow:
        current = uow.meetings.get(MEETING_ID)
        assert current is not None
        failed = status in {
            MeetingStatus.TRANSCRIPTION_FAILED,
            MeetingStatus.EXTRACTION_FAILED,
        }
        updated = Meeting.model_validate(
            current.model_dump(mode="python")
            | {
                "status": status,
                "current_transcript_id": (
                    UUID("30000000-0000-4000-8000-000000000001")
                    if status is MeetingStatus.EXTRACTION_FAILED
                    else None
                ),
                "failure": failure() if failed else None,
                "version": version,
            }
        )
        uow.meetings.save(updated, current.version)
        uow.commit()
    return updated


def add_failed_job(database: Database, stage: ProcessingStage) -> ProcessingJob:
    job = ProcessingJob(
        id=JOB_ID,
        meeting_id=MEETING_ID,
        stage=stage,
        status=ProcessingJobStatus.FAILED,
        attempt_count=3 if stage is ProcessingStage.TRANSCRIPTION else 2,
        max_attempts=3 if stage is ProcessingStage.TRANSCRIPTION else 2,
        last_failure=failure(),
        created_at=NOW,
        updated_at=NOW,
    )
    with SqliteUnitOfWork(database) as uow:
        uow.processing_jobs.add(job)
        uow.commit()
    return job


async def test_retry_resets_failed_job_once_and_returns_authoritative_replay(
    tmp_path: Path,
) -> None:
    database = create_database(tmp_path / "application.sqlite3")
    set_meeting(database, status=MeetingStatus.TRANSCRIPTION_FAILED, version=4)
    add_failed_job(database, ProcessingStage.TRANSCRIPTION)
    controls = service(database)

    first = await controls.retry(
        MEETING_ID,
        expected_version=4,
        request_key="retry-one",
        actor_id="owner",
    )

    assert first.replayed is False
    assert first.meeting.status is MeetingStatus.TRANSCRIPTION_FAILED
    assert first.meeting.version == 5
    assert first.meeting.failure == failure()
    assert first.jobs[0].status is ProcessingJobStatus.READY
    assert first.jobs[0].attempt_count == 0
    assert first.jobs[0].last_failure is None

    with SqliteUnitOfWork(database) as uow:
        claimed = uow.processing_jobs.claim_due(
            ProcessingStage.TRANSCRIPTION,
            "worker-one",
            CONTROL_NOW,
            CONTROL_NOW + timedelta(minutes=5),
            1,
            retry_failure(),
        )
        uow.commit()
    assert len(claimed) == 1

    replay = await controls.retry(
        MEETING_ID,
        expected_version=4,
        request_key="retry-one",
        actor_id="owner",
    )

    assert replay.replayed is True
    assert replay.meeting.version == 5
    assert replay.jobs[0].status is ProcessingJobStatus.RUNNING
    assert replay.jobs[0].attempt_count == 1


async def test_retry_rejects_stale_versions_and_idempotency_key_rebinding(
    tmp_path: Path,
) -> None:
    database = create_database(tmp_path / "application.sqlite3")
    set_meeting(database, status=MeetingStatus.EXTRACTION_FAILED, version=7)
    add_failed_job(database, ProcessingStage.EXTRACTION)
    controls = service(database)

    with pytest.raises(StaleWorkflowVersionError):
        await controls.retry(
            MEETING_ID,
            expected_version=6,
            request_key="stale-retry",
            actor_id="owner",
        )

    await controls.retry(
        MEETING_ID,
        expected_version=7,
        request_key="retry-two",
        actor_id="owner",
    )

    with pytest.raises(IdempotencyConflictError):
        await controls.retry(
            MEETING_ID,
            expected_version=7,
            request_key="retry-two",
            actor_id="another-owner",
        )
    with pytest.raises(IdempotencyConflictError):
        await controls.retry(
            MEETING_ID,
            expected_version=8,
            request_key="retry-two",
            actor_id="owner",
        )
    with pytest.raises(IdempotencyConflictError):
        await controls.cancel(
            MEETING_ID,
            expected_version=7,
            request_key="retry-two",
            actor_id="owner",
        )


@pytest.mark.parametrize(
    "status",
    [
        ProcessingJobStatus.READY,
        ProcessingJobStatus.RETRY_WAIT,
        ProcessingJobStatus.RUNNING,
    ],
)
async def test_retry_rejects_nonterminal_jobs(
    tmp_path: Path,
    status: ProcessingJobStatus,
) -> None:
    database = create_database(tmp_path / f"{status.value}.sqlite3")
    set_meeting(database, status=MeetingStatus.TRANSCRIPTION_FAILED, version=4)
    job = ProcessingJob(
        id=JOB_ID,
        meeting_id=MEETING_ID,
        stage=ProcessingStage.TRANSCRIPTION,
        status=status,
        attempt_count=1 if status is not ProcessingJobStatus.READY else 0,
        max_attempts=3,
        next_attempt_at=(
            CONTROL_NOW + timedelta(minutes=5) if status is ProcessingJobStatus.RETRY_WAIT else None
        ),
        lease_owner="worker-one" if status is ProcessingJobStatus.RUNNING else None,
        lease_expires_at=(
            CONTROL_NOW + timedelta(minutes=5) if status is ProcessingJobStatus.RUNNING else None
        ),
        last_failure=retry_failure() if status is ProcessingJobStatus.RETRY_WAIT else None,
        created_at=NOW,
        updated_at=NOW,
    )
    with SqliteUnitOfWork(database) as uow:
        uow.processing_jobs.add(job)
        uow.commit()

    error = WorkflowBusyError if status is ProcessingJobStatus.RUNNING else OperationConflictError
    with pytest.raises(error):
        await service(database).retry(
            MEETING_ID,
            expected_version=4,
            request_key=f"retry-{status.value}",
            actor_id="owner",
        )

    with SqliteUnitOfWork(database) as uow:
        assert uow.meetings.get(MEETING_ID).version == 4
        assert uow.processing_jobs.get(JOB_ID) == job
        assert uow.meeting_operations.get(f"retry-{status.value}") is None


async def test_retry_rolls_back_job_and_meeting_when_binding_insert_fails(
    tmp_path: Path,
) -> None:
    database = create_database(tmp_path / "rollback.sqlite3")
    original_meeting = set_meeting(
        database,
        status=MeetingStatus.TRANSCRIPTION_FAILED,
        version=4,
    )
    original_job = add_failed_job(database, ProcessingStage.TRANSCRIPTION)
    with database.transaction(immediate=True) as connection:
        connection.execute(
            """
            CREATE TRIGGER reject_meeting_operation
            BEFORE INSERT ON meeting_operation_bindings
            BEGIN
                SELECT RAISE(ABORT, 'rejected');
            END
            """
        )

    with pytest.raises(sqlite3.IntegrityError):
        await service(database).retry(
            MEETING_ID,
            expected_version=4,
            request_key="retry-rollback",
            actor_id="owner",
        )

    with SqliteUnitOfWork(database) as uow:
        assert uow.meetings.get(MEETING_ID) == original_meeting
        assert uow.processing_jobs.get(JOB_ID) == original_job
        assert uow.meeting_operations.get("retry-rollback") is None


async def test_cancellation_is_atomic_and_idempotent(tmp_path: Path) -> None:
    database = create_database(tmp_path / "application.sqlite3")
    ready = ProcessingJob(
        id=JOB_ID,
        meeting_id=MEETING_ID,
        stage=ProcessingStage.TRANSCRIPTION,
        max_attempts=3,
        created_at=NOW,
        updated_at=NOW,
    )
    waiting = ProcessingJob(
        id=EXTRACTION_JOB_ID,
        meeting_id=MEETING_ID,
        stage=ProcessingStage.EXTRACTION,
        status=ProcessingJobStatus.RETRY_WAIT,
        attempt_count=1,
        max_attempts=2,
        next_attempt_at=NOW + timedelta(minutes=10),
        last_failure=retry_failure(),
        created_at=NOW,
        updated_at=NOW,
    )
    with SqliteUnitOfWork(database) as uow:
        uow.processing_jobs.add(ready)
        uow.processing_jobs.add(waiting)
        uow.commit()
    controls = service(database)

    first = await controls.cancel(
        MEETING_ID,
        expected_version=0,
        request_key="cancel-one",
        actor_id="owner",
    )
    replay = await controls.cancel(
        MEETING_ID,
        expected_version=0,
        request_key="cancel-one",
        actor_id="owner",
    )

    assert first.replayed is False
    assert first.meeting.status is MeetingStatus.CANCELLED
    assert first.meeting.version == 1
    assert {job.status for job in first.jobs} == {ProcessingJobStatus.CANCELLED}
    assert replay.replayed is True
    assert replay.meeting == first.meeting
    assert replay.jobs == first.jobs
    with SqliteUnitOfWork(database) as uow:
        assert uow.meeting_operations.get("cancel-one") is not None
    with pytest.raises(IdempotencyConflictError):
        await controls.cancel(
            MEETING_ID,
            expected_version=0,
            request_key="cancel-one",
            actor_id="another-owner",
        )
    with pytest.raises(StaleWorkflowVersionError):
        await controls.cancel(
            MEETING_ID,
            expected_version=0,
            request_key="cancel-stale",
            actor_id="owner",
        )


async def test_cancellation_preserves_terminal_processing_jobs(tmp_path: Path) -> None:
    database = create_database(tmp_path / "application.sqlite3")
    failed = ProcessingJob(
        id=JOB_ID,
        meeting_id=MEETING_ID,
        stage=ProcessingStage.TRANSCRIPTION,
        status=ProcessingJobStatus.FAILED,
        attempt_count=3,
        max_attempts=3,
        last_failure=failure(),
        created_at=NOW,
        updated_at=NOW,
    )
    succeeded = ProcessingJob(
        id=EXTRACTION_JOB_ID,
        meeting_id=MEETING_ID,
        stage=ProcessingStage.EXTRACTION,
        status=ProcessingJobStatus.SUCCEEDED,
        attempt_count=1,
        max_attempts=2,
        created_at=NOW,
        updated_at=NOW,
    )
    with SqliteUnitOfWork(database) as uow:
        uow.processing_jobs.add(failed)
        uow.processing_jobs.add(succeeded)
        uow.commit()

    result = await service(database).cancel(
        MEETING_ID,
        expected_version=0,
        request_key="cancel-terminal",
        actor_id="owner",
    )

    assert tuple(job.status for job in result.jobs) == (
        ProcessingJobStatus.FAILED,
        ProcessingJobStatus.SUCCEEDED,
    )
    assert result.jobs[0] == failed
    assert result.jobs[1] == succeeded


async def test_cancellation_rejects_running_jobs_and_late_states(tmp_path: Path) -> None:
    running_database = create_database(tmp_path / "running.sqlite3")
    ready = ProcessingJob(
        id=JOB_ID,
        meeting_id=MEETING_ID,
        stage=ProcessingStage.TRANSCRIPTION,
        max_attempts=3,
        created_at=NOW,
        updated_at=NOW,
    )
    with SqliteUnitOfWork(running_database) as uow:
        uow.processing_jobs.add(ready)
        uow.commit()
    with SqliteUnitOfWork(running_database) as uow:
        uow.processing_jobs.claim_due(
            ProcessingStage.TRANSCRIPTION,
            "worker-one",
            CONTROL_NOW,
            CONTROL_NOW + timedelta(minutes=5),
            1,
            retry_failure(),
        )
        uow.commit()

    with pytest.raises(WorkflowBusyError):
        await service(running_database).cancel(
            MEETING_ID,
            expected_version=0,
            request_key="cancel-running",
            actor_id="owner",
        )

    completed_database = create_database(tmp_path / "completed.sqlite3")
    set_meeting(completed_database, status=MeetingStatus.CANCELLED)
    with pytest.raises(OperationConflictError):
        await service(completed_database).cancel(
            MEETING_ID,
            expected_version=0,
            request_key="cancel-late",
            actor_id="owner",
        )
