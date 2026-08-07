from __future__ import annotations

from collections.abc import Callable
from uuid import UUID, uuid4

from meeting_action_orchestrator.application.ports import Clock, UnitOfWork
from meeting_action_orchestrator.domain.enums import RecordingCleanupReason
from meeting_action_orchestrator.domain.errors import RecordingCleanupConflictError
from meeting_action_orchestrator.domain.models import RecordingCleanupJob

UnitOfWorkFactory = Callable[[], UnitOfWork]


class RecordingCleanupScheduler:
    def __init__(
        self,
        *,
        unit_of_work: UnitOfWorkFactory,
        clock: Clock,
        max_attempts: int = 5,
        id_factory: Callable[[], UUID] = uuid4,
    ) -> None:
        if max_attempts <= 0:
            raise ValueError("Maximum attempts must be positive")
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
