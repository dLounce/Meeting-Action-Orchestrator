from __future__ import annotations

import asyncio
import secrets
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Protocol, TypeGuard
from uuid import UUID, uuid4

from meeting_action_orchestrator.application.auditing import (
    append_meeting_transition,
    append_processing_attempt,
)
from meeting_action_orchestrator.application.errors import (
    ProviderBudgetIntegrityError,
    ResourceNotFoundError,
)
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
from meeting_action_orchestrator.domain.provider_budget import (
    DEFAULT_PROVIDER_BUDGET_LIMITS,
    ProviderBudgetAccount,
    ProviderBudgetLimits,
)
from meeting_action_orchestrator.domain.workflow_events import (
    ProcessingAttemptMetadata,
    ProcessingAuditOutcome,
    WorkflowEventType,
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
        budget_limits: Mapping[ProcessingStage, ProviderBudgetLimits] | None = None,
        budget_policy_version: int = 1,
    ) -> None:
        self._unit_of_work = unit_of_work
        self._clock = clock
        self._id_factory = id_factory
        configured = DEFAULT_PROVIDER_BUDGET_LIMITS if budget_limits is None else budget_limits
        if set(configured) != set(ProcessingStage):
            raise ValueError("Provider budget limits must cover every processing stage")
        self._budget_limits = dict(configured)
        if not 1 <= budget_policy_version <= 1_000:
            raise ValueError("Provider budget policy version is invalid")
        self._budget_policy_version = budget_policy_version

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
            if uow.provider_budget_accounts.get(existing.id) is None:
                raise ProviderBudgetIntegrityError
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
        uow.provider_budget_accounts.add(
            ProviderBudgetAccount(
                processing_job_id=job.id,
                stage=stage,
                policy_version=self._budget_policy_version,
                limits=self._budget_limits[stage],
                created_at=now,
            )
        )
        return job


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
        normalized_worker_id = worker_id.strip()
        if not normalized_worker_id or len(normalized_worker_id) > 200:
            raise ValueError("Worker ID is invalid")
        if lease_duration <= timedelta(0):
            raise ValueError("Lease duration must be positive")
        self._unit_of_work = unit_of_work
        self._handlers = dict(handlers)
        self._clock = clock
        self._retry_scheduler = retry_scheduler
        self._worker_id = normalized_worker_id
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

    def renew_lease(self, job_id: UUID, claim_token: UUID) -> ProcessingJob | None:
        with self._unit_of_work() as uow:
            now = self._clock.now()
            current = uow.processing_jobs.get(job_id)
            if not self._owns_live_lease(current, claim_token, now):
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
                current.claim_token,
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
        with self._unit_of_work() as uow:
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
            selected = tuple(
                uow.processing_jobs.claim_due(
                    stage,
                    self._worker_id,
                    now,
                    now + self._lease_duration,
                    limit,
                )
            )
            claimed = []
            for job in selected:
                if not _prepare_claimed_attempt(uow, job, now):
                    continue
                append_processing_attempt(
                    uow.workflow_events,
                    job,
                    ProcessingAuditOutcome.STARTED,
                    _processing_input_digest(uow, job),
                    now,
                )
                claimed.append(job)
            uow.commit()
        return tuple(claimed)

    @staticmethod
    def _repair_expired_exhausted(
        uow: UnitOfWork,
        job: ProcessingJob,
        expired_failure: WorkflowFailure,
        inconsistent_failure: WorkflowFailure,
        now: datetime,
    ) -> None:
        meeting = uow.meetings.get(job.meeting_id)
        has_open_attempt = (
            _latest_processing_outcome(uow, job, job.attempt_count)
            is ProcessingAuditOutcome.STARTED
        )
        transitions: tuple[tuple[Meeting, Meeting], ...] = ()
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
            repair = (
                _fail_meeting_for_expired_job(
                    meeting,
                    job.stage,
                    expired_failure,
                    now,
                )
                if meeting is not None
                else (None, ())
            )
            repaired_meeting, transitions = repair
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
            claim_token=None,
            last_failure=job_failure,
        )
        if repaired_meeting is not None and meeting is not None:
            uow.meetings.save(repaired_meeting, meeting.version)
            for previous, current in transitions:
                append_meeting_transition(uow.workflow_events, previous, current, now)
        uow.processing_jobs.save(
            repaired_job,
            job.status,
            job.lease_owner,
            job.lease_expires_at,
            job.claim_token,
        )
        if has_open_attempt and meeting is not None and status is ProcessingJobStatus.SUCCEEDED:
            append_processing_attempt(
                uow.workflow_events,
                repaired_job,
                ProcessingAuditOutcome.SUCCEEDED,
                _processing_input_digest(uow, repaired_job),
                now,
                output_digest=_processing_output_digest(uow, repaired_job),
            )
        elif has_open_attempt and meeting is not None and status is ProcessingJobStatus.FAILED:
            append_processing_attempt(
                uow.workflow_events,
                repaired_job,
                ProcessingAuditOutcome.FAILED,
                _processing_input_digest(uow, repaired_job),
                now,
                failure=job_failure,
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
        with self._unit_of_work() as uow:
            now = self._clock.now()
            current = uow.processing_jobs.get(claimed.id)
            if claimed.claim_token is None or not self._owns_live_lease(
                current, claimed.claim_token, now
            ):
                return None
            completed = _replace_job(
                current,
                status=ProcessingJobStatus.SUCCEEDED,
                updated_at=now,
                lease_owner=None,
                lease_expires_at=None,
                claim_token=None,
                last_failure=None,
            )
            uow.processing_jobs.save(
                completed,
                current.status,
                current.lease_owner,
                current.lease_expires_at,
                current.claim_token,
            )
            append_processing_attempt(
                uow.workflow_events,
                completed,
                ProcessingAuditOutcome.SUCCEEDED,
                _processing_input_digest(uow, completed),
                now,
                output_digest=_processing_output_digest(uow, completed),
            )
            uow.commit()
        return completed

    def _finish_failure(
        self,
        claimed: ProcessingJob,
        failure: WorkflowFailure,
    ) -> ProcessingJob | None:
        with self._unit_of_work() as uow:
            now = self._clock.now()
            current = uow.processing_jobs.get(claimed.id)
            if claimed.claim_token is None or not self._owns_live_lease(
                current, claimed.claim_token, now
            ):
                return None
            meeting = uow.meetings.get(current.meeting_id)
            if meeting is not None and _has_committed_artifact(uow, current, meeting):
                completed = _replace_job(
                    current,
                    status=ProcessingJobStatus.SUCCEEDED,
                    updated_at=now,
                    next_attempt_at=None,
                    lease_owner=None,
                    lease_expires_at=None,
                    claim_token=None,
                    last_failure=None,
                )
                uow.processing_jobs.save(
                    completed,
                    current.status,
                    current.lease_owner,
                    current.lease_expires_at,
                    current.claim_token,
                )
                append_processing_attempt(
                    uow.workflow_events,
                    completed,
                    ProcessingAuditOutcome.SUCCEEDED,
                    _processing_input_digest(uow, completed),
                    now,
                    output_digest=_processing_output_digest(uow, completed),
                )
                uow.commit()
                return completed
            effective_failure = failure
            if (
                meeting is not None
                and meeting.status is _stage_states(current.stage)[2]
                and meeting.failure is not None
            ):
                effective_failure = meeting.failure
            retryable = (
                effective_failure.disposition is FailureDisposition.RETRYABLE
                and current.attempt_count < current.max_attempts
            )
            status = ProcessingJobStatus.RETRY_WAIT if retryable else ProcessingJobStatus.FAILED
            retry_at = None
            if retryable:
                retry_base = now
                provider_retry_at = None
                provider_retry_delay = effective_failure.retry_after_seconds
                if provider_retry_delay is not None:
                    provider_retry_at = now + timedelta(seconds=provider_retry_delay)
                    retry_base = provider_retry_at
                retry_at = self._retry_scheduler.schedule(retry_base, current.attempt_count)
                if provider_retry_at is not None:
                    minimum_retry_at = provider_retry_at
                    if provider_retry_delay is not None and provider_retry_delay > 0:
                        minimum_retry_at += timedelta(microseconds=1)
                    retry_at = max(retry_at, minimum_retry_at)
            failed = _replace_job(
                current,
                status=status,
                updated_at=now,
                next_attempt_at=retry_at,
                lease_owner=None,
                lease_expires_at=None,
                claim_token=None,
                last_failure=effective_failure,
            )
            uow.processing_jobs.save(
                failed,
                current.status,
                current.lease_owner,
                current.lease_expires_at,
                current.claim_token,
            )
            append_processing_attempt(
                uow.workflow_events,
                failed,
                (
                    ProcessingAuditOutcome.RETRY_SCHEDULED
                    if retryable
                    else ProcessingAuditOutcome.FAILED
                ),
                _processing_input_digest(uow, failed),
                now,
                failure=effective_failure,
                retry_at=retry_at,
            )
            uow.commit()
        return failed

    def _owns_live_lease(
        self,
        job: ProcessingJob | None,
        claim_token: UUID,
        now: datetime,
    ) -> TypeGuard[ProcessingJob]:
        return (
            job is not None
            and job.status is ProcessingJobStatus.RUNNING
            and job.lease_owner == self._worker_id
            and job.claim_token == claim_token
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
) -> tuple[Meeting | None, tuple[tuple[Meeting, Meeting], ...]]:
    pending, active, failed = _stage_states(stage)
    if meeting.status is failed:
        return None, ()
    transitions: list[tuple[Meeting, Meeting]] = []
    if meeting.status is pending:
        started = transition_meeting(meeting, active, now)
        transitions.append((meeting, started))
        meeting = started
    if meeting.status is active:
        completed = transition_meeting(meeting, failed, now, failure=failure)
        transitions.append((meeting, completed))
        return completed, tuple(transitions)
    return None, ()


def _processing_input_digest(uow: UnitOfWork, job: ProcessingJob) -> str | None:
    meeting = uow.meetings.get(job.meeting_id)
    if meeting is not None and job.stage is ProcessingStage.TRANSCRIPTION:
        asset = uow.audio_assets.get(meeting.audio_asset_id)
        if asset is not None:
            return asset.sha256
    if meeting is not None and job.stage is ProcessingStage.EXTRACTION:
        transcript = (
            uow.transcripts.get(meeting.current_transcript_id)
            if meeting.current_transcript_id is not None
            else uow.transcripts.latest_for_meeting(meeting.id)
        )
        if transcript is not None:
            return transcript.sha256
    return None


def _prepare_claimed_attempt(
    uow: UnitOfWork,
    claimed: ProcessingJob,
    now: datetime,
) -> bool:
    previous_attempt = claimed.attempt_count - 1
    if previous_attempt < 1:
        return True
    latest_outcome = _latest_processing_outcome(uow, claimed, previous_attempt)
    has_open_attempt = latest_outcome is ProcessingAuditOutcome.STARTED
    previous = _replace_job(claimed, attempt_count=previous_attempt)
    meeting = uow.meetings.get(claimed.meeting_id)
    if meeting is not None and _has_committed_artifact(uow, previous, meeting):
        succeeded = _replace_job(
            previous,
            status=ProcessingJobStatus.SUCCEEDED,
            lease_owner=None,
            lease_expires_at=None,
            claim_token=None,
            last_failure=None,
        )
        uow.processing_jobs.save(
            succeeded,
            claimed.status,
            claimed.lease_owner,
            claimed.lease_expires_at,
            claimed.claim_token,
        )
        if has_open_attempt:
            append_processing_attempt(
                uow.workflow_events,
                succeeded,
                ProcessingAuditOutcome.SUCCEEDED,
                _processing_input_digest(uow, succeeded),
                now,
                output_digest=_processing_output_digest(uow, succeeded),
            )
        return False
    if meeting is not None and meeting.status is MeetingStatus.CANCELLED:
        cancelled = _replace_job(
            previous,
            status=ProcessingJobStatus.CANCELLED,
            lease_owner=None,
            lease_expires_at=None,
            claim_token=None,
            last_failure=None,
        )
        uow.processing_jobs.save(
            cancelled,
            claimed.status,
            claimed.lease_owner,
            claimed.lease_expires_at,
            claimed.claim_token,
        )
        return False
    failure = _reclaimed_attempt_failure(meeting, claimed.stage, now)
    if failure.disposition is not FailureDisposition.RETRYABLE:
        failed = _replace_job(
            previous,
            status=ProcessingJobStatus.FAILED,
            lease_owner=None,
            lease_expires_at=None,
            claim_token=None,
            last_failure=failure,
        )
        uow.processing_jobs.save(
            failed,
            claimed.status,
            claimed.lease_owner,
            claimed.lease_expires_at,
            claimed.claim_token,
        )
        if has_open_attempt:
            append_processing_attempt(
                uow.workflow_events,
                failed,
                ProcessingAuditOutcome.FAILED,
                _processing_input_digest(uow, failed),
                now,
                failure=failure,
            )
        return False
    if has_open_attempt:
        append_processing_attempt(
            uow.workflow_events,
            previous,
            ProcessingAuditOutcome.RETRY_SCHEDULED,
            _processing_input_digest(uow, previous),
            now,
            failure=failure,
            retry_at=now,
        )
    return True


def _latest_processing_outcome(
    uow: UnitOfWork,
    claimed: ProcessingJob,
    attempt_number: int,
) -> ProcessingAuditOutcome | None:
    event = uow.workflow_events.latest_processing_event(
        claimed.meeting_id,
        claimed.stage,
    )
    if event is None or event.type is WorkflowEventType.PROCESSING_RETRY_REQUESTED:
        return None
    metadata = event.safe_metadata
    if not isinstance(metadata, ProcessingAttemptMetadata):
        return None
    if metadata.attempt_number != attempt_number:
        return None
    return metadata.outcome


def _reclaimed_attempt_failure(
    meeting: Meeting | None,
    stage: ProcessingStage,
    now: datetime,
) -> WorkflowFailure:
    if meeting is not None and meeting.status is _stage_states(stage)[2]:
        if meeting.failure is not None:
            return meeting.failure
        return WorkflowFailure(
            code=FailureCode.INTERNAL,
            disposition=FailureDisposition.PERMANENT,
            safe_message="The processing job state is inconsistent",
            occurred_at=now,
        )
    return WorkflowFailure(
        code=FailureCode.PROVIDER_TIMEOUT,
        disposition=FailureDisposition.RETRYABLE,
        safe_message="The processing lease expired before completion",
        occurred_at=now,
    )


def _processing_output_digest(uow: UnitOfWork, job: ProcessingJob) -> str | None:
    meeting = uow.meetings.get(job.meeting_id)
    if meeting is not None and job.stage is ProcessingStage.TRANSCRIPTION:
        transcript = (
            uow.transcripts.get(meeting.current_transcript_id)
            if meeting.current_transcript_id is not None
            else None
        )
        if transcript is not None:
            return transcript.sha256
    if meeting is not None and job.stage is ProcessingStage.EXTRACTION:
        review = (
            uow.reviews.get(meeting.current_review_id)
            if meeting.current_review_id is not None
            else None
        )
        if review is not None:
            return review.content_digest
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
