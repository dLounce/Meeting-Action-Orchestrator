from __future__ import annotations

import asyncio
import re
from collections.abc import Callable, Set
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Protocol, TypeGuard
from uuid import UUID, uuid4

from meeting_action_orchestrator.application.errors import (
    PermanentRecordingCleanupError,
    RetryableRecordingCleanupError,
)
from meeting_action_orchestrator.application.ports import Clock, UnitOfWork
from meeting_action_orchestrator.domain.enums import (
    FailureCode,
    FailureDisposition,
    RecordingCleanupReason,
    RecordingCleanupStatus,
)
from meeting_action_orchestrator.domain.errors import RecordingCleanupConflictError
from meeting_action_orchestrator.domain.models import RecordingCleanupJob, WorkflowFailure

UnitOfWorkFactory = Callable[[], UnitOfWork]
_STORAGE_KEY_PATTERN = re.compile(r"(?:[0-9a-f]{32}\.(?:wav|mp3|m4a)|\.[0-9a-f]{32}\.part)")
_MAX_ATTEMPTS = 5
_MAX_CLEANUP_BATCH_SIZE = 100
_MAX_SCAN_BATCH_SIZE = 1_000


class RetryScheduler(Protocol):
    def schedule(self, now: datetime, attempt_count: int) -> datetime: ...


@dataclass(frozen=True, slots=True)
class StaleRecordingCandidate:
    storage_key: str
    size_bytes: int
    modified_at: datetime
    stat_device: int = field(repr=False)
    stat_inode: int = field(repr=False)
    stat_modified_ns: int = field(repr=False)
    stat_changed_ns: int = field(repr=False)

    def __post_init__(self) -> None:
        _validate_storage_key(self.storage_key)
        if self.size_bytes < 0:
            raise ValueError("Recording size cannot be negative")
        if self.modified_at.tzinfo is None or self.modified_at.utcoffset() is None:
            raise ValueError("Recording modification time must include a UTC offset")
        if (
            self.stat_device < 0
            or self.stat_inode < 0
            or self.stat_modified_ns < 0
            or self.stat_changed_ns < 0
        ):
            raise ValueError("Recording stat identity cannot be negative")


@dataclass(frozen=True, slots=True)
class RecordingIdentity:
    storage_key: str
    sha256: str = field(repr=False)
    size_bytes: int

    def __post_init__(self) -> None:
        _validate_storage_key(self.storage_key)
        if re.fullmatch(r"[0-9a-f]{64}", self.sha256) is None:
            raise ValueError("Recording digest must be lowercase SHA-256")
        if self.size_bytes < 0:
            raise ValueError("Recording size cannot be negative")


class RecordingCleanupExecutor(Protocol):
    def execute(self, job: RecordingCleanupJob) -> None: ...

    def healthcheck(self) -> bool: ...


class StaleRecordingScanner(Protocol):
    def scan_stale_candidates(
        self,
        *,
        now: datetime,
        grace_period: timedelta,
        limit: int,
        after_storage_key: str | None = None,
        active_temporary_keys: Set[str] = frozenset(),
    ) -> tuple[StaleRecordingCandidate, ...]: ...

    def identify(self, candidate: StaleRecordingCandidate) -> RecordingIdentity | None: ...


class ActiveRecordingTracker(Protocol):
    def active_temporary_keys(self) -> frozenset[str]: ...


class RecordingCleanupOutcome(str, Enum):
    SUCCEEDED = "succeeded"
    RETRY_SCHEDULED = "retry_scheduled"
    FAILED = "failed"
    LEASE_LOST = "lease_lost"


@dataclass(frozen=True, slots=True)
class RecordingCleanupResult:
    job_id: UUID
    outcome: RecordingCleanupOutcome
    job: RecordingCleanupJob | None


@dataclass(frozen=True, slots=True)
class OrphanDiscoveryBatch:
    scanned: int = 0
    owned: int = 0
    changed: int = 0
    scheduled: int = 0
    rejected: int = 0


class RecordingCleanupScheduler:
    def __init__(
        self,
        *,
        unit_of_work: UnitOfWorkFactory,
        clock: Clock,
        max_attempts: int = 5,
        id_factory: Callable[[], UUID] = uuid4,
    ) -> None:
        if not 1 <= max_attempts <= _MAX_ATTEMPTS:
            raise ValueError("Maximum attempts must be between one and five")
        self._unit_of_work = unit_of_work
        self._clock = clock
        self._max_attempts = max_attempts
        self._id_factory = id_factory

    def schedule_if_unreferenced(
        self,
        *,
        storage_key: str,
        expected_sha256: str,
        expected_size_bytes: int,
        reason: RecordingCleanupReason,
    ) -> RecordingCleanupJob | None:
        now = self._clock.now()
        with self._unit_of_work() as uow:
            if uow.audio_assets.find_by_storage_key(storage_key) is not None:
                return None
            existing = uow.recording_cleanups.find_by_storage_key(storage_key)
            if existing is not None:
                if (
                    existing.expected_sha256 != expected_sha256
                    or existing.expected_size_bytes != expected_size_bytes
                ):
                    raise RecordingCleanupConflictError(storage_key)
                return existing
            job = RecordingCleanupJob(
                id=self._id_factory(),
                storage_key=storage_key,
                expected_sha256=expected_sha256,
                expected_size_bytes=expected_size_bytes,
                reason=reason,
                max_attempts=self._max_attempts,
                created_at=now,
                updated_at=now,
            )
            uow.recording_cleanups.add(job)
            uow.commit()
        return job


class RecordingCleanupWorker:
    def __init__(
        self,
        *,
        unit_of_work: UnitOfWorkFactory,
        executor: RecordingCleanupExecutor,
        clock: Clock,
        retry_scheduler: RetryScheduler,
        worker_id: str,
        lease_duration: timedelta = timedelta(minutes=5),
        heartbeat_interval: timedelta | None = None,
    ) -> None:
        if not worker_id or worker_id != worker_id.strip() or len(worker_id) > 200:
            raise ValueError("Worker ID must be between 1 and 200 trimmed characters")
        if not timedelta(seconds=30) <= lease_duration <= timedelta(hours=1):
            raise ValueError("Lease duration must be between 30 seconds and one hour")
        heartbeat = heartbeat_interval if heartbeat_interval is not None else lease_duration / 3
        if heartbeat <= timedelta(0) or heartbeat >= lease_duration:
            raise ValueError("Heartbeat interval must be positive and shorter than the lease")
        self._unit_of_work = unit_of_work
        self._executor = executor
        self._clock = clock
        self._retry_scheduler = retry_scheduler
        self._worker_id = worker_id
        self._lease_duration = lease_duration
        self._heartbeat_interval = heartbeat

    async def run_once(self, limit: int = 20) -> tuple[RecordingCleanupResult, ...]:
        if limit <= 0:
            return ()
        if limit > _MAX_CLEANUP_BATCH_SIZE:
            raise ValueError("Cleanup batch size cannot exceed 100")
        results: list[RecordingCleanupResult] = []
        for _ in range(limit):
            processing = asyncio.create_task(self._claim_and_process())
            try:
                result = await asyncio.shield(processing)
            except asyncio.CancelledError:
                await processing
                raise
            if result is None:
                break
            results.append(result)
        return tuple(results)

    async def _claim_and_process(self) -> RecordingCleanupResult | None:
        claimed = await asyncio.to_thread(self._claim, 1)
        if not claimed:
            return None
        return await self._process(claimed[0])

    async def _process(self, job: RecordingCleanupJob) -> RecordingCleanupResult:
        failure = await self._execute(job)
        if failure is None:
            persisted = await asyncio.to_thread(self._finish_success, job)
        else:
            persisted = await asyncio.to_thread(self._finish_failure, job, failure)
        outcome = (
            RecordingCleanupOutcome.LEASE_LOST
            if persisted is None
            else _cleanup_outcome(persisted.status)
        )
        return RecordingCleanupResult(
            job_id=job.id,
            outcome=outcome,
            job=persisted,
        )

    def _claim(self, limit: int) -> tuple[RecordingCleanupJob, ...]:
        now = self._clock.now()
        with self._unit_of_work() as uow:
            claimed = tuple(
                uow.recording_cleanups.claim_due(
                    self._worker_id,
                    now,
                    now + self._lease_duration,
                    limit,
                )
            )
            uow.commit()
        return claimed

    async def _execute(self, job: RecordingCleanupJob) -> WorkflowFailure | None:
        execution = asyncio.create_task(asyncio.to_thread(self._executor.execute, job))
        heartbeat = asyncio.create_task(self._heartbeat(job.id))
        failure: WorkflowFailure | None = None
        try:
            await execution
        except PermanentRecordingCleanupError:
            failure = WorkflowFailure(
                code=FailureCode.INVALID_INPUT,
                disposition=FailureDisposition.PERMANENT,
                safe_message="Recording cleanup was rejected for safety",
                occurred_at=self._clock.now(),
            )
        except RetryableRecordingCleanupError:
            failure = _retryable_cleanup_failure(self._clock.now())
        except Exception:
            failure = _retryable_cleanup_failure(self._clock.now())
        finally:
            heartbeat.cancel()
            heartbeat_result = (await asyncio.gather(heartbeat, return_exceptions=True))[0]
        if isinstance(heartbeat_result, Exception):
            raise heartbeat_result
        return failure

    async def _heartbeat(self, job_id: UUID) -> None:
        while True:
            await asyncio.sleep(self._heartbeat_interval.total_seconds())
            renewed = await asyncio.to_thread(self._renew_lease, job_id)
            if renewed is None:
                return

    def _renew_lease(self, job_id: UUID) -> RecordingCleanupJob | None:
        now = self._clock.now()
        with self._unit_of_work() as uow:
            current = uow.recording_cleanups.get(job_id)
            if not self._owns_live_lease(current, now):
                return None
            renewed = _replace_cleanup_job(
                current,
                updated_at=now,
                lease_expires_at=now + self._lease_duration,
            )
            uow.recording_cleanups.save(
                renewed,
                current.status,
                current.lease_owner,
                current.lease_expires_at,
            )
            uow.commit()
        return renewed

    def _finish_success(self, claimed: RecordingCleanupJob) -> RecordingCleanupJob | None:
        now = self._clock.now()
        with self._unit_of_work() as uow:
            current = uow.recording_cleanups.get(claimed.id)
            if not self._owns_live_lease(current, now):
                return None
            completed = _replace_cleanup_job(
                current,
                status=RecordingCleanupStatus.SUCCEEDED,
                next_attempt_at=None,
                lease_owner=None,
                lease_expires_at=None,
                last_failure=None,
                updated_at=now,
                completed_at=now,
            )
            uow.recording_cleanups.save(
                completed,
                current.status,
                current.lease_owner,
                current.lease_expires_at,
            )
            uow.commit()
        return completed

    def _finish_failure(
        self,
        claimed: RecordingCleanupJob,
        failure: WorkflowFailure,
    ) -> RecordingCleanupJob | None:
        now = self._clock.now()
        with self._unit_of_work() as uow:
            current = uow.recording_cleanups.get(claimed.id)
            if not self._owns_live_lease(current, now):
                return None
            retryable = (
                failure.disposition is FailureDisposition.RETRYABLE
                and current.attempt_count < min(current.max_attempts, _MAX_ATTEMPTS)
            )
            status = (
                RecordingCleanupStatus.RETRY_WAIT if retryable else RecordingCleanupStatus.FAILED
            )
            retry_at = (
                self._retry_scheduler.schedule(now, max(1, current.attempt_count))
                if retryable
                else None
            )
            failed = _replace_cleanup_job(
                current,
                status=status,
                next_attempt_at=retry_at,
                lease_owner=None,
                lease_expires_at=None,
                last_failure=failure,
                updated_at=now,
                completed_at=None if retryable else now,
            )
            uow.recording_cleanups.save(
                failed,
                current.status,
                current.lease_owner,
                current.lease_expires_at,
            )
            uow.commit()
        return failed

    def _owns_live_lease(
        self,
        job: RecordingCleanupJob | None,
        now: datetime,
    ) -> TypeGuard[RecordingCleanupJob]:
        return (
            job is not None
            and job.status is RecordingCleanupStatus.RUNNING
            and job.lease_owner == self._worker_id
            and job.lease_expires_at is not None
            and job.lease_expires_at > now
        )


class RecordingOrphanDiscoverer:
    def __init__(
        self,
        *,
        unit_of_work: UnitOfWorkFactory,
        scheduler: RecordingCleanupScheduler,
        scanner: StaleRecordingScanner,
        active_recordings: ActiveRecordingTracker,
        clock: Clock,
        grace_period: timedelta,
    ) -> None:
        if not timedelta(minutes=5) <= grace_period <= timedelta(days=7):
            raise ValueError("Orphan grace period must be between five minutes and seven days")
        self._unit_of_work = unit_of_work
        self._scheduler = scheduler
        self._scanner = scanner
        self._active_recordings = active_recordings
        self._clock = clock
        self._grace_period = grace_period
        self._after_storage_key: str | None = None

    async def run_once(self, limit: int = 100) -> OrphanDiscoveryBatch:
        if limit <= 0:
            return OrphanDiscoveryBatch()
        if limit > _MAX_SCAN_BATCH_SIZE:
            raise ValueError("Orphan scan batch size cannot exceed 1000")
        now = self._clock.now()
        active_keys = await asyncio.to_thread(self._active_recordings.active_temporary_keys)
        candidates = await asyncio.to_thread(
            self._scanner.scan_stale_candidates,
            now=now,
            grace_period=self._grace_period,
            limit=limit,
            after_storage_key=self._after_storage_key,
            active_temporary_keys=active_keys,
        )
        self._validate_candidate_page(candidates, limit)
        owned = 0
        changed = 0
        scheduled = 0
        rejected = 0
        for candidate in candidates:
            if await asyncio.to_thread(self._is_owned, candidate.storage_key):
                owned += 1
                continue
            try:
                identity = await asyncio.to_thread(self._scanner.identify, candidate)
            except PermanentRecordingCleanupError:
                rejected += 1
                continue
            if identity is None:
                changed += 1
                continue
            if identity.storage_key != candidate.storage_key:
                rejected += 1
                continue
            try:
                job = await asyncio.to_thread(
                    self._scheduler.schedule_if_unreferenced,
                    storage_key=identity.storage_key,
                    expected_sha256=identity.sha256,
                    expected_size_bytes=identity.size_bytes,
                    reason=RecordingCleanupReason.ORPHAN_RECONCILIATION,
                )
            except RecordingCleanupConflictError:
                rejected += 1
                continue
            if job is None:
                owned += 1
            else:
                scheduled += 1
        self._advance_cursor(candidates, limit)
        return OrphanDiscoveryBatch(
            scanned=len(candidates),
            owned=owned,
            changed=changed,
            scheduled=scheduled,
            rejected=rejected,
        )

    def _is_owned(self, storage_key: str) -> bool:
        with self._unit_of_work() as uow:
            return (
                uow.audio_assets.find_by_storage_key(storage_key) is not None
                or uow.recording_cleanups.find_by_storage_key(storage_key) is not None
            )

    def _advance_cursor(
        self,
        candidates: tuple[StaleRecordingCandidate, ...],
        limit: int,
    ) -> None:
        if not candidates or len(candidates) < limit:
            self._after_storage_key = None
        else:
            self._after_storage_key = candidates[-1].storage_key

    def _validate_candidate_page(
        self,
        candidates: tuple[StaleRecordingCandidate, ...],
        limit: int,
    ) -> None:
        keys = tuple(candidate.storage_key for candidate in candidates)
        if len(candidates) > limit or keys != tuple(sorted(set(keys))):
            raise ValueError("Orphan scanner returned an invalid candidate page")
        if self._after_storage_key is not None and any(
            key <= self._after_storage_key for key in keys
        ):
            raise ValueError("Orphan scanner did not advance its candidate page")


def _retryable_cleanup_failure(now: datetime) -> WorkflowFailure:
    return WorkflowFailure(
        code=FailureCode.INTERNAL,
        disposition=FailureDisposition.RETRYABLE,
        safe_message="Recording cleanup could not finish",
        occurred_at=now,
    )


def _replace_cleanup_job(
    job: RecordingCleanupJob,
    **updates: object,
) -> RecordingCleanupJob:
    return RecordingCleanupJob.model_validate(job.model_dump(mode="python") | updates)


def _cleanup_outcome(status: RecordingCleanupStatus) -> RecordingCleanupOutcome:
    if status is RecordingCleanupStatus.SUCCEEDED:
        return RecordingCleanupOutcome.SUCCEEDED
    if status is RecordingCleanupStatus.RETRY_WAIT:
        return RecordingCleanupOutcome.RETRY_SCHEDULED
    return RecordingCleanupOutcome.FAILED


def _validate_storage_key(storage_key: str) -> None:
    if _STORAGE_KEY_PATTERN.fullmatch(storage_key) is None:
        raise ValueError("Recording storage key is invalid")
