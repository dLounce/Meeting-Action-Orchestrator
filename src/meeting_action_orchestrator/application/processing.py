from __future__ import annotations

import secrets
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Protocol, TypeGuard
from uuid import UUID, uuid4

from meeting_action_orchestrator.application.errors import ResourceNotFoundError
from meeting_action_orchestrator.application.ports import Clock, UnitOfWork
from meeting_action_orchestrator.domain.enums import (
    FailureCode,
    FailureDisposition,
    ProcessingJobStatus,
    ProcessingStage,
)
from meeting_action_orchestrator.domain.models import (
    PROCESSING_MAX_ATTEMPTS,
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
        claimed = self._claim(stage, limit)
        results = []
        for job in claimed:
            failure = await self._execute(job)
            persisted = (
                self._finish_success(job) if failure is None else self._finish_failure(job, failure)
            )
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

    def _claim(self, stage: ProcessingStage, limit: int) -> tuple[ProcessingJob, ...]:
        if limit <= 0:
            return ()
        now = self._clock.now()
        expired_failure = WorkflowFailure(
            code=FailureCode.PROVIDER_TIMEOUT,
            disposition=FailureDisposition.RETRYABLE,
            safe_message="The processing lease expired before completion",
            occurred_at=now,
        )
        with self._unit_of_work() as uow:
            claimed = tuple(
                uow.processing_jobs.claim_due(
                    stage,
                    self._worker_id,
                    now,
                    now + self._lease_duration,
                    limit,
                    expired_failure,
                )
            )
            uow.commit()
        return claimed

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
