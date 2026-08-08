from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Protocol, TypeGuard
from uuid import UUID

from meeting_action_orchestrator.application.erasure_support import (
    UnitOfWorkFactory,
    aware_now,
    replace_erasure_job,
    shielded_thread,
)
from meeting_action_orchestrator.application.errors import MeetingErasureIntegrityError
from meeting_action_orchestrator.application.ports import Clock, DatabaseCheckpoint, UnitOfWork
from meeting_action_orchestrator.domain.enums import (
    FailureDisposition,
    MeetingErasureFailureCode,
    MeetingErasureRecordingState,
    MeetingErasureStatus,
    RecordingCleanupReason,
    RecordingCleanupStatus,
)
from meeting_action_orchestrator.domain.models import (
    MeetingErasureFailure,
    MeetingErasureJob,
    RecordingCleanupJob,
)

_MAX_BATCH_SIZE = 100


class ErasureRetryScheduler(Protocol):
    def schedule(self, now: datetime, attempt_count: int) -> datetime: ...


class MeetingErasureWorkerOutcome(str, Enum):
    CHECKPOINTED = "checkpointed"
    RETRY_SCHEDULED = "retry_scheduled"
    CLEANUP_REMOVED = "cleanup_removed"
    CLEANUP_FAILED = "cleanup_failed"
    COMPLETED = "completed"
    FAILED = "failed"
    LEASE_LOST = "lease_lost"


@dataclass(frozen=True, slots=True)
class MeetingErasureWorkerResult:
    job_id: UUID
    outcome: MeetingErasureWorkerOutcome
    job: MeetingErasureJob | None


class _CleanupGroupLeaseConflictError(RuntimeError):
    pass


class MeetingErasureWorker:
    def __init__(
        self,
        *,
        unit_of_work: UnitOfWorkFactory,
        checkpoint: DatabaseCheckpoint,
        clock: Clock,
        retry_scheduler: ErasureRetryScheduler,
        worker_id: str,
        lease_duration: timedelta = timedelta(minutes=5),
    ) -> None:
        if not worker_id or worker_id != worker_id.strip() or len(worker_id) > 200:
            raise ValueError("Worker ID must be between 1 and 200 trimmed characters")
        if not timedelta(seconds=30) <= lease_duration <= timedelta(hours=1):
            raise ValueError("Lease duration must be between 30 seconds and one hour")
        self._unit_of_work = unit_of_work
        self._checkpoint = checkpoint
        self._clock = clock
        self._retry_scheduler = retry_scheduler
        self._worker_id = worker_id
        self._lease_duration = lease_duration

    async def run_once(self, limit: int = 20) -> tuple[MeetingErasureWorkerResult, ...]:
        if limit <= 0:
            return ()
        if limit > _MAX_BATCH_SIZE:
            raise ValueError("Meeting erasure batch size cannot exceed 100")
        results: list[MeetingErasureWorkerResult] = []
        for _ in range(limit):
            result = await shielded_thread(self._claim_and_process)
            if result is None:
                break
            results.append(result)
        return tuple(results)

    def _claim_and_process(self) -> MeetingErasureWorkerResult | None:
        now = aware_now(self._clock)
        with self._unit_of_work() as uow:
            claimed = tuple(
                uow.meeting_erasures.claim_actionable(
                    self._worker_id,
                    now,
                    now + self._lease_duration,
                    1,
                )
            )
            uow.commit()
        if not claimed:
            return None
        job = claimed[0]
        if job.database_checkpointed_at is None:
            return self._checkpoint_job(job)
        if job.recording_state is MeetingErasureRecordingState.CLEANUP_PENDING:
            return self._reconcile_cleanup(job)
        raise MeetingErasureIntegrityError

    def _checkpoint_job(self, claimed: MeetingErasureJob) -> MeetingErasureWorkerResult:
        try:
            result = self._checkpoint.truncate_wal()
        except Exception:
            result = None
        if result is None or not result.truncated:
            persisted = self._record_checkpoint_retry(claimed)
            return MeetingErasureWorkerResult(
                job_id=claimed.id,
                outcome=(
                    MeetingErasureWorkerOutcome.RETRY_SCHEDULED
                    if persisted is not None
                    else MeetingErasureWorkerOutcome.LEASE_LOST
                ),
                job=persisted,
            )
        persisted = self._record_checkpoint_success(claimed)
        if persisted is None:
            outcome = MeetingErasureWorkerOutcome.LEASE_LOST
        elif persisted.status is MeetingErasureStatus.COMPLETED:
            outcome = MeetingErasureWorkerOutcome.COMPLETED
        elif persisted.status is MeetingErasureStatus.FAILED:
            outcome = MeetingErasureWorkerOutcome.FAILED
        else:
            outcome = MeetingErasureWorkerOutcome.CHECKPOINTED
        return MeetingErasureWorkerResult(job_id=claimed.id, outcome=outcome, job=persisted)

    def _record_checkpoint_retry(
        self,
        claimed: MeetingErasureJob,
    ) -> MeetingErasureJob | None:
        now = aware_now(self._clock)
        with self._unit_of_work() as uow:
            current = uow.meeting_erasures.get(claimed.id)
            if not self._owns_live_lease(current, now):
                return None
            attempt = current.retry_count + 1
            failure = (
                current.last_failure
                if current.recording_state is MeetingErasureRecordingState.FAILED
                else MeetingErasureFailure(
                    code=MeetingErasureFailureCode.DATABASE_SANITATION_DEFERRED,
                    disposition=FailureDisposition.RETRYABLE,
                    occurred_at=now,
                )
            )
            retrying = replace_erasure_job(
                current,
                retry_count=attempt,
                next_attempt_at=self._retry_scheduler.schedule(now, attempt),
                lease_owner=None,
                lease_expires_at=None,
                last_failure=failure,
                version=current.version + 1,
                updated_at=now,
            )
            uow.meeting_erasures.save(
                retrying,
                current.version,
                current.lease_owner,
                current.lease_expires_at,
            )
            uow.commit()
        return retrying

    def _record_checkpoint_success(
        self,
        claimed: MeetingErasureJob,
    ) -> MeetingErasureJob | None:
        now = aware_now(self._clock)
        with self._unit_of_work() as uow:
            current = uow.meeting_erasures.get(claimed.id)
            if not self._owns_live_lease(current, now):
                return None
            values: dict[str, object] = {
                "database_checkpointed_at": now,
                "next_attempt_at": None,
                "lease_owner": None,
                "lease_expires_at": None,
                "version": current.version + 1,
                "updated_at": now,
            }
            if current.recording_state is MeetingErasureRecordingState.REMOVED:
                values |= {
                    "status": MeetingErasureStatus.COMPLETED,
                    "last_failure": None,
                    "completed_at": now,
                }
            elif current.recording_state is MeetingErasureRecordingState.FAILED:
                values |= {
                    "status": MeetingErasureStatus.FAILED,
                    "completed_at": now,
                }
            else:
                values["last_failure"] = None
            checkpointed = replace_erasure_job(current, **values)
            uow.meeting_erasures.save(
                checkpointed,
                current.version,
                current.lease_owner,
                current.lease_expires_at,
            )
            uow.commit()
        return checkpointed

    def _reconcile_cleanup(
        self,
        claimed: MeetingErasureJob,
    ) -> MeetingErasureWorkerResult:
        now = aware_now(self._clock)
        with self._unit_of_work() as uow:
            current = uow.meeting_erasures.get(claimed.id)
            if not self._owns_live_lease(current, now):
                return _lease_lost(claimed.id)
            if current.cleanup_job_id is None:
                raise MeetingErasureIntegrityError
            cleanup = uow.recording_cleanups.get(current.cleanup_job_id)
            if cleanup is None or cleanup.reason is not RecordingCleanupReason.MEETING_ERASURE:
                raise MeetingErasureIntegrityError
            try:
                if cleanup.status is RecordingCleanupStatus.SUCCEEDED:
                    jobs = _finalize_cleanup_success(uow, cleanup, now, current)
                    outcome = MeetingErasureWorkerOutcome.CLEANUP_REMOVED
                elif cleanup.status is RecordingCleanupStatus.FAILED:
                    jobs = _finalize_cleanup_failure(uow, cleanup, now, current)
                    outcome = MeetingErasureWorkerOutcome.CLEANUP_FAILED
                else:
                    return _lease_lost(claimed.id)
            except _CleanupGroupLeaseConflictError:
                return _lease_lost(claimed.id)
            target = next((job for job in jobs if job.id == current.id), None)
            if target is None:
                raise MeetingErasureIntegrityError
            uow.commit()
        return MeetingErasureWorkerResult(job_id=claimed.id, outcome=outcome, job=target)

    def _owns_live_lease(
        self,
        job: MeetingErasureJob | None,
        now: datetime,
    ) -> TypeGuard[MeetingErasureJob]:
        return (
            job is not None
            and job.status is MeetingErasureStatus.ACTIVE
            and job.lease_owner == self._worker_id
            and job.lease_expires_at is not None
            and job.lease_expires_at > now
        )


def _lease_lost(job_id: UUID) -> MeetingErasureWorkerResult:
    return MeetingErasureWorkerResult(
        job_id=job_id,
        outcome=MeetingErasureWorkerOutcome.LEASE_LOST,
        job=None,
    )


def _finalize_cleanup_success(
    uow: UnitOfWork,
    cleanup: RecordingCleanupJob,
    now: datetime,
    claimed: MeetingErasureJob,
) -> tuple[MeetingErasureJob, ...]:
    linked = tuple(uow.meeting_erasures.list_by_cleanup_job_id(cleanup.id))
    _validate_cleanup_group(linked, claimed, now)
    removed: list[MeetingErasureJob] = []
    for current in linked:
        candidate = replace_erasure_job(
            current,
            recording_state=MeetingErasureRecordingState.REMOVED,
            cleanup_job_id=None,
            database_checkpointed_at=None,
            next_attempt_at=None,
            lease_owner=None,
            lease_expires_at=None,
            last_failure=None,
            version=current.version + 1,
            updated_at=now,
        )
        uow.meeting_erasures.save(
            candidate,
            current.version,
            current.lease_owner,
            current.lease_expires_at,
        )
        removed.append(candidate)
    if not uow.recording_cleanups.delete_succeeded(cleanup):
        raise MeetingErasureIntegrityError
    return tuple(removed)


def _finalize_cleanup_failure(
    uow: UnitOfWork,
    cleanup: RecordingCleanupJob,
    now: datetime,
    claimed: MeetingErasureJob,
) -> tuple[MeetingErasureJob, ...]:
    linked = tuple(uow.meeting_erasures.list_by_cleanup_job_id(cleanup.id))
    _validate_cleanup_group(linked, claimed, now)
    failure = MeetingErasureFailure(
        code=MeetingErasureFailureCode.RECORDING_CLEANUP_REJECTED,
        disposition=FailureDisposition.PERMANENT,
        occurred_at=now,
    )
    failed: list[MeetingErasureJob] = []
    for current in linked:
        values: dict[str, object] = {
            "recording_state": MeetingErasureRecordingState.FAILED,
            "next_attempt_at": None,
            "lease_owner": None,
            "lease_expires_at": None,
            "last_failure": failure,
            "version": current.version + 1,
            "updated_at": now,
        }
        if current.database_checkpointed_at is not None:
            values |= {
                "status": MeetingErasureStatus.FAILED,
                "completed_at": now,
            }
        candidate = replace_erasure_job(current, **values)
        uow.meeting_erasures.save(
            candidate,
            current.version,
            current.lease_owner,
            current.lease_expires_at,
        )
        failed.append(candidate)
    return tuple(failed)


def _validate_cleanup_group(
    linked: Sequence[MeetingErasureJob],
    claimed: MeetingErasureJob,
    now: datetime,
) -> None:
    if not linked or sum(job.id == claimed.id for job in linked) != 1:
        raise MeetingErasureIntegrityError
    for job in linked:
        if (
            job.status is not MeetingErasureStatus.ACTIVE
            or job.recording_state is not MeetingErasureRecordingState.CLEANUP_PENDING
            or job.updated_at > now
        ):
            raise MeetingErasureIntegrityError
        live_lease = (
            job.lease_owner is not None
            and job.lease_expires_at is not None
            and job.lease_expires_at > now
        )
        claimed_lease_changed = job.id == claimed.id and (
            job.version != claimed.version
            or job.lease_owner != claimed.lease_owner
            or job.lease_expires_at != claimed.lease_expires_at
            or not live_lease
        )
        if claimed_lease_changed or (job.id != claimed.id and live_lease):
            raise _CleanupGroupLeaseConflictError
