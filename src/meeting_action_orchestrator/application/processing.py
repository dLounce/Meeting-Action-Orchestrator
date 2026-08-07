from __future__ import annotations

import asyncio
import secrets
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Protocol, TypeGuard
from uuid import UUID, uuid4

from meeting_action_orchestrator.application.errors import ResourceNotFoundError
from meeting_action_orchestrator.application.ports import Clock, UnitOfWork
from meeting_action_orchestrator.application.state_machine import transition_meeting
from meeting_action_orchestrator.domain.enums import (
    FailureCode,
    FailureDisposition,
    MeetingStatus,
    ProcessingJobStatus,
    ProcessingStage,
)
from meeting_action_orchestrator.domain.models import (
    PROCESSING_MAX_ATTEMPTS,
    Meeting,
    ProcessingJob,
    WorkflowFailure,
)

UnitOfWorkFactory = Callable[[], UnitOfWork]
ProcessingHandler = Callable[[ProcessingJob], Awaitable[WorkflowFailure | None]]


class RetryScheduler(Protocol):
    def schedule(self, now: datetime, attempt_count: int) -> datetime: ...


class ProcessingOutcome(str, Enum):
    SUCCEEDED = "succeeded"
    RETRY_SCHEDULED = "retry_scheduled"
    FAILED = "failed"
    LEASE_LOST = "lease_lost"


@dataclass(frozen=True, slots=True)
class ProcessingResult:
    job_id: UUID
    outcome: ProcessingOutcome
    job: ProcessingJob | None


class FullJitterRetryScheduler:
    def __init__(
        self,
        *,
        base_delay: timedelta = timedelta(seconds=2),
        maximum_delay: timedelta = timedelta(minutes=2),
        random_value: Callable[[], float] | None = None,
    ) -> None:
        if base_delay <= timedelta(0) or maximum_delay < base_delay:
            raise ValueError("Backoff delays are invalid")
        self._base_seconds = base_delay.total_seconds()
        self._maximum_seconds = maximum_delay.total_seconds()
        self._random_value = random_value or _secure_fraction

    def schedule(self, now: datetime, attempt_count: int) -> datetime:
        if attempt_count < 1:
            raise ValueError("Attempt count must be positive")
        random_value = self._random_value()
        if not 0.0 <= random_value <= 1.0:
            raise ValueError("Random value must be between zero and one")
        ceiling = min(
            self._maximum_seconds,
            self._base_seconds * (2 ** (attempt_count - 1)),
        )
        return now + timedelta(seconds=ceiling * random_value)


class ProcessingScheduler:
    def __init__(
        self,
        *,
        unit_of_work: UnitOfWorkFactory,
        clock: Clock,
        id_factory: Callable[[], UUID] = uuid4,
    ) -> None:
        self._unit_of_work = unit_of_work
        self._clock = clock
        self._id_factory = id_factory

    def enqueue(self, meeting_id: UUID, stage: ProcessingStage) -> ProcessingJob:
        now = self._clock.now()
        with self._unit_of_work() as uow:
            job = self.enqueue_in(uow, meeting_id, stage, scheduled_at=now)
            uow.commit()
        return job

    def enqueue_in(
        self,
        uow: UnitOfWork,
        meeting_id: UUID,
        stage: ProcessingStage,
        *,
        scheduled_at: datetime | None = None,
    ) -> ProcessingJob:
        if uow.meetings.get(meeting_id) is None:
            raise ResourceNotFoundError("Meeting")
        existing = uow.processing_jobs.find_for_stage(meeting_id, stage)
        if existing is not None:
            return existing
        now = scheduled_at or self._clock.now()
        job = ProcessingJob(
            id=self._id_factory(),
            meeting_id=meeting_id,
            stage=stage,
            max_attempts=PROCESSING_MAX_ATTEMPTS[stage],
            created_at=now,
            updated_at=now,
        )
        uow.processing_jobs.add(job)
        return job

    def cancel(self, job_id: UUID) -> ProcessingJob:
        now = self._clock.now()
        with self._unit_of_work() as uow:
            current = uow.processing_jobs.get(job_id)
            if current is None:
                raise ResourceNotFoundError("Processing job")
            terminal = {
                ProcessingJobStatus.SUCCEEDED,
                ProcessingJobStatus.FAILED,
                ProcessingJobStatus.CANCELLED,
            }
            if current.status in terminal:
                return current
            cancelled = _replace_job(
                current,
                status=ProcessingJobStatus.CANCELLED,
                updated_at=now,
                next_attempt_at=None,
                lease_owner=None,
                lease_expires_at=None,
            )
            uow.processing_jobs.save(
                cancelled,
                current.status,
                current.lease_owner,
                current.lease_expires_at,
            )
            uow.commit()
        return cancelled


class ProcessingWorker:
    def __init__(
        self,
        *,
        unit_of_work: UnitOfWorkFactory,
        handlers: Mapping[ProcessingStage, ProcessingHandler],
        clock: Clock,
        retry_scheduler: RetryScheduler,
        worker_id: str,
        lease_duration: timedelta = timedelta(minutes=15),
    ) -> None:
        if not worker_id.strip():
            raise ValueError("Worker ID cannot be empty")
        if lease_duration <= timedelta(0):
            raise ValueError("Lease duration must be positive")
        self._unit_of_work = unit_of_work
        self._handlers = dict(handlers)
        self._clock = clock
        self._retry_scheduler = retry_scheduler
        self._worker_id = worker_id
        self._lease_duration = lease_duration

    async def run_once(
        self,
        stage: ProcessingStage,
        *,
        limit: int = 1,
    ) -> tuple[ProcessingResult, ...]:
        if stage not in self._handlers:
            raise ValueError(f"No handler is registered for {stage.value}")
        batch_limit = max(0, limit)
        if batch_limit == 0:
            return ()
        results = []
        for index in range(batch_limit):
            claimed = await asyncio.to_thread(
                self._claim,
                stage,
                1,
                batch_limit if index == 0 else 0,
            )
            if not claimed:
                break
            job = claimed[0]
            failure = await self._execute(job)
            if failure is None:
                persisted = await asyncio.to_thread(self._finish_success, job)
            else:
                persisted = await asyncio.to_thread(self._finish_failure, job, failure)
            if persisted is None:
                results.append(
                    ProcessingResult(
                        job_id=job.id,
                        outcome=ProcessingOutcome.LEASE_LOST,
                        job=None,
                    )
                )
                continue
            outcome = _outcome_for(persisted.status)
            results.append(ProcessingResult(job_id=job.id, outcome=outcome, job=persisted))
        return tuple(results)

    def renew_lease(self, job_id: UUID) -> ProcessingJob | None:
        now = self._clock.now()
        with self._unit_of_work() as uow:
            current = uow.processing_jobs.get(job_id)
            if not self._owns_live_lease(current, now):
                return None
            renewed = _replace_job(
                current,
                updated_at=now,
                lease_expires_at=now + self._lease_duration,
            )
            uow.processing_jobs.save(
                renewed,
                current.status,
                current.lease_owner,
                current.lease_expires_at,
            )
            uow.commit()
        return renewed

    def _claim(
        self,
        stage: ProcessingStage,
        limit: int,
        repair_limit: int = 0,
    ) -> tuple[ProcessingJob, ...]:
        if limit <= 0 and repair_limit <= 0:
            return ()
        now = self._clock.now()
        expired_failure = WorkflowFailure(
            code=FailureCode.PROVIDER_TIMEOUT,
            disposition=FailureDisposition.RETRYABLE,
            safe_message="The processing lease expired before completion",
            occurred_at=now,
        )
        inconsistent_failure = WorkflowFailure(
            code=FailureCode.INTERNAL,
            disposition=FailureDisposition.PERMANENT,
            safe_message="The processing job state is inconsistent",
            occurred_at=now,
        )
        with self._unit_of_work() as uow:
            expired_jobs = uow.processing_jobs.list_expired_exhausted(
                stage,
                now,
                max(0, repair_limit),
            )
            for expired in expired_jobs:
                self._repair_expired_exhausted(
                    uow,
                    expired,
                    expired_failure,
                    inconsistent_failure,
                    now,
                )
            claimed = tuple(
                uow.processing_jobs.claim_due(
                    stage,
                    self._worker_id,
                    now,
                    now + self._lease_duration,
                    limit,
                )
            )
            uow.commit()
        return claimed

    @staticmethod
    def _repair_expired_exhausted(
        uow: UnitOfWork,
        job: ProcessingJob,
        expired_failure: WorkflowFailure,
        inconsistent_failure: WorkflowFailure,
        now: datetime,
    ) -> None:
        meeting = uow.meetings.get(job.meeting_id)
        if meeting is not None and _has_committed_artifact(uow, job, meeting):
            status = ProcessingJobStatus.SUCCEEDED
            job_failure = None
            repaired_meeting = None
        elif meeting is not None and meeting.status is MeetingStatus.CANCELLED:
            status = ProcessingJobStatus.CANCELLED
            job_failure = None
            repaired_meeting = None
        else:
            status = ProcessingJobStatus.FAILED
            repaired_meeting = (
                _fail_meeting_for_expired_job(
                    meeting,
                    job.stage,
                    expired_failure,
                    now,
                )
                if meeting is not None
                else None
            )
            failed_status = _stage_states(job.stage)[2]
            if repaired_meeting is not None:
                job_failure = expired_failure
            elif meeting is not None and meeting.status is failed_status:
                job_failure = meeting.failure or inconsistent_failure
            else:
                job_failure = inconsistent_failure
        repaired_job = _replace_job(
            job,
            status=status,
            updated_at=now,
            next_attempt_at=None,
            lease_owner=None,
            lease_expires_at=None,
            last_failure=job_failure,
        )
        if repaired_meeting is not None and meeting is not None:
            uow.meetings.save(repaired_meeting, meeting.version)
        uow.processing_jobs.save(
            repaired_job,
            job.status,
            job.lease_owner,
            job.lease_expires_at,
        )

    async def _execute(self, job: ProcessingJob) -> WorkflowFailure | None:
        try:
            return await self._handlers[job.stage](job)
        except Exception:
            return WorkflowFailure(
                code=FailureCode.INTERNAL,
                disposition=FailureDisposition.RETRYABLE,
                safe_message="The processing stage failed unexpectedly",
                occurred_at=self._clock.now(),
            )

    def _finish_success(self, claimed: ProcessingJob) -> ProcessingJob | None:
        now = self._clock.now()
        with self._unit_of_work() as uow:
            current = uow.processing_jobs.get(claimed.id)
            if not self._owns_live_lease(current, now):
                return None
            completed = _replace_job(
                current,
                status=ProcessingJobStatus.SUCCEEDED,
                updated_at=now,
                lease_owner=None,
                lease_expires_at=None,
                last_failure=None,
            )
            uow.processing_jobs.save(
                completed,
                current.status,
                current.lease_owner,
                current.lease_expires_at,
            )
            uow.commit()
        return completed

    def _finish_failure(
        self,
        claimed: ProcessingJob,
        failure: WorkflowFailure,
    ) -> ProcessingJob | None:
        now = self._clock.now()
        with self._unit_of_work() as uow:
            current = uow.processing_jobs.get(claimed.id)
            if not self._owns_live_lease(current, now):
                return None
            retryable = (
                failure.disposition is FailureDisposition.RETRYABLE
                and current.attempt_count < current.max_attempts
            )
            status = ProcessingJobStatus.RETRY_WAIT if retryable else ProcessingJobStatus.FAILED
            retry_at = (
                self._retry_scheduler.schedule(now, current.attempt_count) if retryable else None
            )
            failed = _replace_job(
                current,
                status=status,
                updated_at=now,
                next_attempt_at=retry_at,
                lease_owner=None,
                lease_expires_at=None,
                last_failure=failure,
            )
            uow.processing_jobs.save(
                failed,
                current.status,
                current.lease_owner,
                current.lease_expires_at,
            )
            uow.commit()
        return failed

    def _owns_live_lease(
        self,
        job: ProcessingJob | None,
        now: datetime,
    ) -> TypeGuard[ProcessingJob]:
        return (
            job is not None
            and job.status is ProcessingJobStatus.RUNNING
            and job.lease_owner == self._worker_id
            and job.lease_expires_at is not None
            and job.lease_expires_at > now
        )


def _replace_job(job: ProcessingJob, **updates: object) -> ProcessingJob:
    return ProcessingJob.model_validate(job.model_dump(mode="python") | updates)


def _has_committed_artifact(
    uow: UnitOfWork,
    job: ProcessingJob,
    meeting: Meeting,
) -> bool:
    if job.stage is ProcessingStage.TRANSCRIPTION:
        if meeting.current_transcript_id is None:
            return False
        transcript = uow.transcripts.get(meeting.current_transcript_id)
        return (
            transcript is not None
            and transcript.meeting_id == meeting.id
            and transcript.audio_asset_id == meeting.audio_asset_id
        )
    if meeting.current_review_id is None or meeting.current_transcript_id is None:
        return False
    review = uow.reviews.get(meeting.current_review_id)
    return (
        review is not None
        and review.meeting_id == meeting.id
        and review.transcript_id == meeting.current_transcript_id
    )


def _fail_meeting_for_expired_job(
    meeting: Meeting,
    stage: ProcessingStage,
    failure: WorkflowFailure,
    now: datetime,
) -> Meeting | None:
    pending, active, failed = _stage_states(stage)
    if meeting.status is failed:
        return None
    if meeting.status is pending:
        meeting = transition_meeting(meeting, active, now)
    if meeting.status is active:
        return transition_meeting(meeting, failed, now, failure=failure)
    return None


def _stage_states(
    stage: ProcessingStage,
) -> tuple[MeetingStatus, MeetingStatus, MeetingStatus]:
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
    return states[stage]


def _outcome_for(status: ProcessingJobStatus) -> ProcessingOutcome:
    outcomes = {
        ProcessingJobStatus.SUCCEEDED: ProcessingOutcome.SUCCEEDED,
        ProcessingJobStatus.RETRY_WAIT: ProcessingOutcome.RETRY_SCHEDULED,
        ProcessingJobStatus.FAILED: ProcessingOutcome.FAILED,
    }
    return outcomes[status]


def _secure_fraction() -> float:
    denominator = 1 << 53
    return secrets.randbelow(denominator + 1) / denominator
