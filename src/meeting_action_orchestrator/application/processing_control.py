from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import TypeVar
from uuid import UUID

from meeting_action_orchestrator.application.errors import (
    OperationConflictError,
    ResourceNotFoundError,
    StaleWorkflowVersionError,
    WorkflowBusyError,
)
from meeting_action_orchestrator.application.ports import Clock, UnitOfWork
from meeting_action_orchestrator.application.state_machine import transition_meeting
from meeting_action_orchestrator.domain.enums import (
    MeetingOperationKind,
    MeetingStatus,
    ProcessingJobStatus,
    ProcessingStage,
)
from meeting_action_orchestrator.domain.errors import IdempotencyConflictError
from meeting_action_orchestrator.domain.hashing import canonical_sha256
from meeting_action_orchestrator.domain.models import (
    Meeting,
    MeetingOperationBinding,
    ProcessingJob,
)

UnitOfWorkFactory = Callable[[], UnitOfWork]

_CANCELLABLE_STATUSES = frozenset(
    {
        MeetingStatus.INGESTED,
        MeetingStatus.TRANSCRIPTION_FAILED,
        MeetingStatus.TRANSCRIBED,
        MeetingStatus.EXTRACTION_FAILED,
        MeetingStatus.AWAITING_APPROVAL,
    }
)


@dataclass(frozen=True, slots=True)
class ProcessingControlResult:
    meeting: Meeting
    jobs: tuple[ProcessingJob, ...]
    replayed: bool = False


class ProcessingControlService:
    def __init__(
        self,
        *,
        unit_of_work: UnitOfWorkFactory,
        clock: Clock,
    ) -> None:
        self._unit_of_work = unit_of_work
        self._clock = clock

    async def retry(
        self,
        meeting_id: UUID,
        *,
        expected_version: int,
        request_key: str,
        actor_id: str,
    ) -> ProcessingControlResult:
        _validate_operation_identity(request_key, actor_id, expected_version)
        return await asyncio.to_thread(
            self._retry,
            meeting_id,
            expected_version,
            request_key,
            actor_id,
        )

    async def cancel(
        self,
        meeting_id: UUID,
        *,
        expected_version: int,
        request_key: str,
        actor_id: str,
    ) -> ProcessingControlResult:
        _validate_operation_identity(request_key, actor_id, expected_version)
        return await asyncio.to_thread(
            self._cancel,
            meeting_id,
            expected_version,
            request_key,
            actor_id,
        )

    def _retry(
        self,
        meeting_id: UUID,
        expected_version: int,
        request_key: str,
        actor_id: str,
    ) -> ProcessingControlResult:
        with self._unit_of_work() as uow:
            existing = uow.meeting_operations.get(request_key)
            if existing is not None:
                _validate_replay(
                    existing,
                    meeting_id=meeting_id,
                    operation=MeetingOperationKind.PROCESSING_RETRY,
                    actor_id=actor_id,
                    expected_version=expected_version,
                )
                return _snapshot(uow, meeting_id, existing.stage, replayed=True)
            now = self._now()
            meeting = _required(uow.meetings.get(meeting_id), "Meeting")
            _validate_version(meeting, expected_version)
            stage = _failed_stage(meeting.status)
            job = uow.processing_jobs.find_for_stage(meeting_id, stage)
            if job is None:
                raise ResourceNotFoundError("Processing job")
            if job.meeting_id != meeting.id or job.stage is not stage:
                raise OperationConflictError("The processing job does not belong to the meeting")
            if job.status is ProcessingJobStatus.RUNNING:
                raise WorkflowBusyError
            if job.status is not ProcessingJobStatus.FAILED:
                raise OperationConflictError("Only a failed processing job can be retried")
            queued = ProcessingJob.model_validate(
                job.model_dump(mode="python")
                | {
                    "status": ProcessingJobStatus.READY,
                    "attempt_count": 0,
                    "next_attempt_at": None,
                    "lease_owner": None,
                    "lease_expires_at": None,
                    "last_failure": None,
                    "updated_at": now,
                }
            )
            updated = Meeting.model_validate(
                meeting.model_dump(mode="python")
                | {
                    "version": meeting.version + 1,
                    "updated_at": now,
                }
            )
            binding = _create_binding(
                request_key=request_key,
                meeting_id=meeting_id,
                operation=MeetingOperationKind.PROCESSING_RETRY,
                actor_id=actor_id,
                stage=stage,
                expected_version=expected_version,
                created_at=now,
            )
            uow.processing_jobs.save(
                queued,
                job.status,
                job.lease_owner,
                job.lease_expires_at,
            )
            uow.meetings.save(updated, meeting.version)
            uow.meeting_operations.add(binding)
            result = _snapshot(uow, meeting_id, stage, replayed=False)
            uow.commit()
            return result

    def _cancel(
        self,
        meeting_id: UUID,
        expected_version: int,
        request_key: str,
        actor_id: str,
    ) -> ProcessingControlResult:
        with self._unit_of_work() as uow:
            existing = uow.meeting_operations.get(request_key)
            if existing is not None:
                _validate_replay(
                    existing,
                    meeting_id=meeting_id,
                    operation=MeetingOperationKind.CANCELLATION,
                    actor_id=actor_id,
                    expected_version=expected_version,
                )
                return _snapshot(uow, meeting_id, None, replayed=True)
            now = self._now()
            meeting = _required(uow.meetings.get(meeting_id), "Meeting")
            _validate_version(meeting, expected_version)
            jobs = tuple(uow.processing_jobs.list_for_meeting(meeting_id))
            if any(job.meeting_id != meeting.id for job in jobs):
                raise OperationConflictError("A processing job does not belong to the meeting")
            if any(job.status is ProcessingJobStatus.RUNNING for job in jobs):
                raise WorkflowBusyError
            if meeting.status not in _CANCELLABLE_STATUSES:
                raise OperationConflictError("The meeting can no longer be cancelled")
            cancelled_jobs = tuple(_cancel_job(job, now) for job in jobs)
            cancelled = transition_meeting(meeting, MeetingStatus.CANCELLED, now)
            binding = _create_binding(
                request_key=request_key,
                meeting_id=meeting_id,
                operation=MeetingOperationKind.CANCELLATION,
                actor_id=actor_id,
                stage=None,
                expected_version=expected_version,
                created_at=now,
            )
            for original, updated in zip(jobs, cancelled_jobs, strict=True):
                if updated is original:
                    continue
                uow.processing_jobs.save(
                    updated,
                    original.status,
                    original.lease_owner,
                    original.lease_expires_at,
                )
            uow.meetings.save(cancelled, meeting.version)
            uow.meeting_operations.add(binding)
            result = _snapshot(uow, meeting_id, None, replayed=False)
            uow.commit()
            return result

    def _now(self) -> datetime:
        now = self._clock.now()
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("clock must return an aware datetime")
        return now


def _create_binding(
    *,
    request_key: str,
    meeting_id: UUID,
    operation: MeetingOperationKind,
    actor_id: str,
    stage: ProcessingStage | None,
    expected_version: int,
    created_at: datetime,
) -> MeetingOperationBinding:
    identity = {
        "actor_id": actor_id,
        "expected_version": expected_version,
        "meeting_id": meeting_id,
        "operation": operation,
        "request_key": request_key,
        "stage": stage,
    }
    return MeetingOperationBinding(
        request_key=request_key,
        meeting_id=meeting_id,
        operation=operation,
        actor_id=actor_id,
        stage=stage,
        expected_version=expected_version,
        request_fingerprint=canonical_sha256(identity),
        created_at=created_at,
    )


def _validate_replay(
    binding: MeetingOperationBinding,
    *,
    meeting_id: UUID,
    operation: MeetingOperationKind,
    actor_id: str,
    expected_version: int,
) -> None:
    if (
        binding.meeting_id != meeting_id
        or binding.operation is not operation
        or binding.actor_id != actor_id
        or binding.expected_version != expected_version
    ):
        raise IdempotencyConflictError(binding.request_key)


def _snapshot(
    uow: UnitOfWork,
    meeting_id: UUID,
    stage: ProcessingStage | None,
    *,
    replayed: bool,
) -> ProcessingControlResult:
    meeting = _required(uow.meetings.get(meeting_id), "Meeting")
    jobs = tuple(uow.processing_jobs.list_for_meeting(meeting_id))
    if any(job.meeting_id != meeting.id for job in jobs):
        raise OperationConflictError("A processing job does not belong to the meeting")
    if stage is not None and not any(job.stage is stage for job in jobs):
        raise OperationConflictError("The bound processing job no longer exists")
    return ProcessingControlResult(meeting=meeting, jobs=jobs, replayed=replayed)


def _cancel_job(job: ProcessingJob, now: datetime) -> ProcessingJob:
    if job.status not in {ProcessingJobStatus.READY, ProcessingJobStatus.RETRY_WAIT}:
        return job
    return ProcessingJob.model_validate(
        job.model_dump(mode="python")
        | {
            "status": ProcessingJobStatus.CANCELLED,
            "next_attempt_at": None,
            "lease_owner": None,
            "lease_expires_at": None,
            "updated_at": now,
        }
    )


def _failed_stage(status: MeetingStatus) -> ProcessingStage:
    if status is MeetingStatus.TRANSCRIPTION_FAILED:
        return ProcessingStage.TRANSCRIPTION
    if status is MeetingStatus.EXTRACTION_FAILED:
        return ProcessingStage.EXTRACTION
    raise OperationConflictError("Only failed meeting processing can be retried")


def _validate_version(meeting: Meeting, expected_version: int) -> None:
    if meeting.version != expected_version:
        raise StaleWorkflowVersionError


def _validate_operation_identity(
    request_key: str,
    actor_id: str,
    expected_version: int,
) -> None:
    for name, value in (("request_key", request_key), ("actor_id", actor_id)):
        if not value or len(value) > 200 or value != value.strip():
            raise ValueError(f"{name} must be between 1 and 200 characters")
    if expected_version < 0:
        raise ValueError("expected_version cannot be negative")


ValueT = TypeVar("ValueT")


def _required(value: ValueT | None, resource: str) -> ValueT:
    if value is None:
        raise ResourceNotFoundError(resource)
    return value
