from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID, uuid4

from meeting_action_orchestrator.application.erasure_support import (
    UnitOfWorkFactory,
    aware_now,
    matches_token,
    replace_erasure_job,
    shielded_thread,
)
from meeting_action_orchestrator.application.errors import (
    MeetingErasureBlockedError,
    MeetingErasureIntegrityError,
    MeetingErasureRequestConflictError,
    OperationConflictError,
    ResourceNotFoundError,
    StaleWorkflowVersionError,
)
from meeting_action_orchestrator.application.ports import (
    Clock,
    ErasureTokenCodec,
    UnitOfWork,
)
from meeting_action_orchestrator.domain.enums import (
    MeetingErasureOperation,
    MeetingErasureReason,
    MeetingErasureRecordingState,
    MeetingErasureStatus,
    RecordingCleanupReason,
    RecordingCleanupStatus,
)
from meeting_action_orchestrator.domain.models import (
    AudioAsset,
    ErasureToken,
    MeetingErasureJob,
    MeetingErasureOperationBinding,
    MeetingErasureTombstone,
    RecordingCleanupJob,
)

_MAX_CLEANUP_ATTEMPTS = 5


@dataclass(frozen=True, slots=True)
class MeetingErasureResult:
    job: MeetingErasureJob
    replayed: bool = False


class ErasureKeyRegistry:
    def __init__(
        self,
        *,
        unit_of_work: UnitOfWorkFactory,
        tokens: ErasureTokenCodec,
        clock: Clock,
        validation_unit_of_work: UnitOfWorkFactory | None = None,
    ) -> None:
        self._unit_of_work = unit_of_work
        self._validation_unit_of_work = validation_unit_of_work or unit_of_work
        self._tokens = tokens
        self._clock = clock

    async def ensure_registered(self) -> tuple[str, ...]:
        return await shielded_thread(self.ensure_registered_sync)

    def ensure_registered_sync(self) -> tuple[str, ...]:
        now = aware_now(self._clock)
        with self._unit_of_work() as uow:
            for verifier in self._tokens.verifiers(now):
                if uow.erasure_key_verifiers.get(verifier.key_id) is None:
                    uow.erasure_key_verifiers.add(verifier)
            persisted = tuple(uow.erasure_key_verifiers.list_all())
            referenced = tuple(uow.erasure_key_verifiers.list_referenced_tokens())
            self._tokens.validate_verifiers(persisted, referenced)
            uow.commit()
        return self._tokens.key_ids

    async def validate_registered(self) -> tuple[str, ...]:
        return await shielded_thread(self.validate_registered_sync)

    def validate_registered_sync(self) -> tuple[str, ...]:
        with self._validation_unit_of_work() as uow:
            persisted = tuple(uow.erasure_key_verifiers.list_all())
            referenced = tuple(uow.erasure_key_verifiers.list_referenced_tokens())
            self._tokens.validate_verifiers(persisted, referenced)
        return self._tokens.key_ids


class MeetingErasureService:
    def __init__(
        self,
        *,
        unit_of_work: UnitOfWorkFactory,
        tokens: ErasureTokenCodec,
        key_registry: ErasureKeyRegistry,
        clock: Clock,
        max_remediations: int = 3,
        cleanup_max_attempts: int = 5,
        id_factory: Callable[[], UUID] = uuid4,
    ) -> None:
        if not 1 <= max_remediations <= 10:
            raise ValueError("Maximum erasure remediations must be between one and ten")
        if not 1 <= cleanup_max_attempts <= _MAX_CLEANUP_ATTEMPTS:
            raise ValueError("Maximum cleanup attempts must be between one and five")
        self._unit_of_work = unit_of_work
        self._tokens = tokens
        self._key_registry = key_registry
        self._clock = clock
        self._max_remediations = max_remediations
        self._cleanup_max_attempts = cleanup_max_attempts
        self._id_factory = id_factory

    async def request(
        self,
        meeting_id: UUID,
        *,
        expected_version: int,
        request_key: str,
        actor_id: str,
    ) -> MeetingErasureResult:
        return await shielded_thread(
            lambda: self._request(
                meeting_id,
                expected_version,
                request_key,
                actor_id,
            )
        )

    def _request(
        self,
        meeting_id: UUID,
        expected_version: int,
        request_key: str,
        actor_id: str,
    ) -> MeetingErasureResult:
        _validate_expected_version(expected_version)
        self._key_registry.ensure_registered_sync()
        request_tokens = self._tokens.request_key_tokens(request_key)
        actor_tokens = self._tokens.actor_tokens(actor_id)
        meeting_tokens = self._tokens.meeting_tokens(meeting_id)
        with self._unit_of_work() as uow:
            existing = uow.meeting_erasure_operations.find_by_request_tokens(request_tokens)
            if existing is not None:
                job = _validate_replay(
                    uow,
                    existing,
                    operation=MeetingErasureOperation.REQUEST,
                    resource_id=meeting_id,
                    expected_version=expected_version,
                    actor_tokens=actor_tokens,
                    resource_tokens=meeting_tokens,
                )
                return MeetingErasureResult(job=job, replayed=True)
            now = aware_now(self._clock)
            replay = self._converge_tombstone(
                uow,
                meeting_id,
                expected_version,
                request_key=request_key,
                actor_id=actor_id,
                meeting_tokens=meeting_tokens,
                now=now,
            )
            if replay is not None:
                return replay
            if uow.meeting_erasures.find_by_meeting_tokens(meeting_tokens) is not None:
                raise MeetingErasureIntegrityError
            job = self._purge_meeting(
                uow,
                meeting_id,
                expected_version,
                request_key=request_key,
                actor_id=actor_id,
                now=now,
            )
            uow.commit()
        return MeetingErasureResult(job=job)

    def _converge_tombstone(
        self,
        uow: UnitOfWork,
        meeting_id: UUID,
        expected_version: int,
        *,
        request_key: str,
        actor_id: str,
        meeting_tokens: Sequence[ErasureToken],
        now: datetime,
    ) -> MeetingErasureResult | None:
        tombstone = uow.meeting_erasure_tombstones.find_by_meeting_tokens(meeting_tokens)
        if tombstone is None:
            return None
        if uow.meetings.get(meeting_id) is not None:
            raise MeetingErasureIntegrityError
        job = uow.meeting_erasures.get(tombstone.erasure_job_id)
        if job is None or not _job_matches_meeting_tokens(job, meeting_tokens):
            raise MeetingErasureIntegrityError
        if expected_version != job.erased_meeting_version:
            raise StaleWorkflowVersionError
        binding = _create_operation_binding(
            self._tokens,
            request_key=request_key,
            actor_id=actor_id,
            resource_id=meeting_id,
            erasure_job_id=job.id,
            operation=MeetingErasureOperation.REQUEST,
            expected_version=expected_version,
            created_at=now,
        )
        uow.meeting_erasure_operations.add(binding)
        uow.commit()
        return MeetingErasureResult(job=job, replayed=True)

    def _purge_meeting(
        self,
        uow: UnitOfWork,
        meeting_id: UUID,
        expected_version: int,
        *,
        request_key: str,
        actor_id: str,
        now: datetime,
    ) -> MeetingErasureJob:
        meeting = uow.meetings.get(meeting_id)
        if meeting is None:
            raise ResourceNotFoundError("Meeting")
        if meeting.version != expected_version:
            raise StaleWorkflowVersionError
        if uow.meeting_erasure_purge.has_active_work(meeting.id, now):
            raise MeetingErasureBlockedError
        ingest_tokens = self._tokens.ingest_key_tokens(meeting.ingest_key)
        if uow.meeting_erasure_tombstones.find_by_ingest_key_tokens(ingest_tokens) is not None:
            raise MeetingErasureIntegrityError
        audio = uow.audio_assets.get(meeting.audio_asset_id)
        if audio is None or not uow.meeting_erasure_purge.meeting_graph_is_consistent(
            meeting.id,
            audio.id,
        ):
            raise MeetingErasureIntegrityError
        _remove_known_cleanup_history(uow, audio.sha256)
        waiters = tuple(uow.meeting_erasures.list_by_pending_audio_asset_id(audio.id))
        if not uow.meeting_erasure_purge.delete_meeting_graph(meeting.id):
            raise MeetingErasureIntegrityError
        cleanup = self._schedule_last_owner_cleanup(uow, meeting.id, audio, waiters, now)
        meeting_token = self._tokens.meeting_token(meeting.id)
        job = _new_erasure_job(
            self._id_factory(),
            meeting_token,
            meeting.version,
            audio.id,
            cleanup=cleanup,
            max_remediations=self._max_remediations,
            now=now,
        )
        tombstone = MeetingErasureTombstone.create(
            job.id,
            meeting_token,
            self._tokens.ingest_key_token(meeting.ingest_key),
            now,
        )
        binding = _create_operation_binding(
            self._tokens,
            request_key=request_key,
            actor_id=actor_id,
            resource_id=meeting.id,
            erasure_job_id=job.id,
            operation=MeetingErasureOperation.REQUEST,
            expected_version=expected_version,
            created_at=now,
        )
        uow.meeting_erasures.add(job)
        uow.meeting_erasure_tombstones.add(tombstone)
        uow.meeting_erasure_operations.add(binding)
        return job

    def _schedule_last_owner_cleanup(
        self,
        uow: UnitOfWork,
        meeting_id: UUID,
        audio: AudioAsset,
        waiters: Sequence[MeetingErasureJob],
        now: datetime,
    ) -> RecordingCleanupJob | None:
        if uow.meeting_erasure_purge.audio_has_other_references(audio.id, meeting_id):
            return None
        if not uow.meeting_erasure_purge.delete_audio_asset(audio.id):
            raise MeetingErasureIntegrityError
        cleanup = RecordingCleanupJob(
            id=self._id_factory(),
            storage_key=audio.storage_key,
            expected_sha256=audio.sha256,
            expected_size_bytes=audio.size_bytes,
            reason=RecordingCleanupReason.MEETING_ERASURE,
            max_attempts=self._cleanup_max_attempts,
            created_at=now,
            updated_at=now,
        )
        uow.recording_cleanups.add(cleanup)
        for waiter in waiters:
            _promote_waiter(uow, waiter, cleanup.id, now)
        return cleanup


class MeetingErasureRemediationService:
    def __init__(
        self,
        *,
        unit_of_work: UnitOfWorkFactory,
        tokens: ErasureTokenCodec,
        key_registry: ErasureKeyRegistry,
        clock: Clock,
    ) -> None:
        self._unit_of_work = unit_of_work
        self._tokens = tokens
        self._key_registry = key_registry
        self._clock = clock

    async def retry(
        self,
        erasure_job_id: UUID,
        *,
        expected_version: int,
        request_key: str,
        actor_id: str,
    ) -> MeetingErasureResult:
        return await shielded_thread(
            lambda: self._retry(
                erasure_job_id,
                expected_version,
                request_key,
                actor_id,
            )
        )

    def _retry(
        self,
        erasure_job_id: UUID,
        expected_version: int,
        request_key: str,
        actor_id: str,
    ) -> MeetingErasureResult:
        _validate_expected_version(expected_version)
        self._key_registry.ensure_registered_sync()
        request_tokens = self._tokens.request_key_tokens(request_key)
        actor_tokens = self._tokens.actor_tokens(actor_id)
        resource_tokens = self._tokens.erasure_job_tokens(erasure_job_id)
        with self._unit_of_work() as uow:
            existing = uow.meeting_erasure_operations.find_by_request_tokens(request_tokens)
            if existing is not None:
                job = _validate_replay(
                    uow,
                    existing,
                    operation=MeetingErasureOperation.RETRY,
                    resource_id=erasure_job_id,
                    expected_version=expected_version,
                    actor_tokens=actor_tokens,
                    resource_tokens=resource_tokens,
                )
                return MeetingErasureResult(job=job, replayed=True)
            current = uow.meeting_erasures.get(erasure_job_id)
            if current is None:
                raise ResourceNotFoundError("Meeting erasure job")
            if current.version != expected_version:
                raise StaleWorkflowVersionError
            if current.status is not MeetingErasureStatus.FAILED or current.cleanup_job_id is None:
                raise OperationConflictError("Only failed recording cleanup can be retried")
            cleanup = uow.recording_cleanups.get(current.cleanup_job_id)
            linked = tuple(uow.meeting_erasures.list_by_cleanup_job_id(current.cleanup_job_id))
            if (
                cleanup is None
                or cleanup.reason is not RecordingCleanupReason.MEETING_ERASURE
                or cleanup.status is not RecordingCleanupStatus.FAILED
                or not linked
                or all(job.id != current.id for job in linked)
                or any(
                    job.status is not MeetingErasureStatus.FAILED
                    or job.recording_state is not MeetingErasureRecordingState.FAILED
                    or job.remediation_count >= job.max_remediations
                    for job in linked
                )
            ):
                raise MeetingErasureBlockedError
            now = aware_now(self._clock)
            binding = _create_operation_binding(
                self._tokens,
                request_key=request_key,
                actor_id=actor_id,
                resource_id=erasure_job_id,
                erasure_job_id=erasure_job_id,
                operation=MeetingErasureOperation.RETRY,
                expected_version=expected_version,
                created_at=now,
            )
            remediated = tuple(
                uow.meeting_erasures.reactivate_failed_cleanup_group(
                    current.cleanup_job_id,
                    now,
                )
            )
            target = next((job for job in remediated if job.id == current.id), None)
            if target is None:
                raise MeetingErasureIntegrityError
            uow.meeting_erasure_operations.add(binding)
            uow.commit()
        return MeetingErasureResult(job=target)


def _remove_known_cleanup_history(uow: UnitOfWork, digest: str) -> None:
    history = tuple(uow.recording_cleanups.list_by_expected_sha256(digest))
    if any(job.status is not RecordingCleanupStatus.SUCCEEDED for job in history):
        raise MeetingErasureBlockedError
    for job in history:
        if not uow.recording_cleanups.delete_succeeded(job):
            raise MeetingErasureBlockedError


def _new_erasure_job(
    job_id: UUID,
    meeting_token: ErasureToken,
    erased_meeting_version: int,
    audio_asset_id: UUID,
    *,
    cleanup: RecordingCleanupJob | None,
    max_remediations: int,
    now: datetime,
) -> MeetingErasureJob:
    recording = (
        {
            "recording_state": MeetingErasureRecordingState.WAITING_SHARED,
            "pending_audio_asset_id": audio_asset_id,
        }
        if cleanup is None
        else {
            "recording_state": MeetingErasureRecordingState.CLEANUP_PENDING,
            "cleanup_job_id": cleanup.id,
        }
    )
    return MeetingErasureJob(
        id=job_id,
        token_version=meeting_token.token_version,
        token_key_id=meeting_token.key_id,
        meeting_token=meeting_token.digest,
        reason=MeetingErasureReason.USER_REQUEST,
        erased_meeting_version=erased_meeting_version,
        max_remediations=max_remediations,
        created_at=now,
        updated_at=now,
        **recording,
    )


def _promote_waiter(
    uow: UnitOfWork,
    waiter: MeetingErasureJob,
    cleanup_job_id: UUID,
    now: datetime,
) -> MeetingErasureJob:
    if (
        waiter.status is not MeetingErasureStatus.ACTIVE
        or waiter.recording_state is not MeetingErasureRecordingState.WAITING_SHARED
        or waiter.pending_audio_asset_id is None
        or waiter.updated_at > now
    ):
        raise MeetingErasureIntegrityError
    promoted = replace_erasure_job(
        waiter,
        recording_state=MeetingErasureRecordingState.CLEANUP_PENDING,
        pending_audio_asset_id=None,
        cleanup_job_id=cleanup_job_id,
        database_checkpointed_at=None,
        next_attempt_at=None,
        lease_owner=None,
        lease_expires_at=None,
        last_failure=None,
        version=waiter.version + 1,
        updated_at=now,
    )
    uow.meeting_erasures.save(
        promoted,
        waiter.version,
        waiter.lease_owner,
        waiter.lease_expires_at,
    )
    return promoted


def _validate_replay(
    uow: UnitOfWork,
    binding: MeetingErasureOperationBinding,
    *,
    operation: MeetingErasureOperation,
    resource_id: UUID,
    expected_version: int,
    actor_tokens: Sequence[ErasureToken],
    resource_tokens: Sequence[ErasureToken],
) -> MeetingErasureJob:
    if (
        binding.operation is not operation
        or binding.expected_version != expected_version
        or not matches_token(
            actor_tokens,
            binding.token_version,
            binding.token_key_id,
            binding.actor_token,
        )
        or not matches_token(
            resource_tokens,
            binding.token_version,
            binding.token_key_id,
            binding.resource_token,
        )
    ):
        raise MeetingErasureRequestConflictError
    job = uow.meeting_erasures.get(binding.erasure_job_id)
    if job is None:
        raise MeetingErasureIntegrityError
    if operation is MeetingErasureOperation.REQUEST:
        if not _job_matches_meeting_tokens(job, resource_tokens):
            raise MeetingErasureIntegrityError
    elif job.id != resource_id:
        raise MeetingErasureIntegrityError
    return job


def _job_matches_meeting_tokens(
    job: MeetingErasureJob,
    tokens: Sequence[ErasureToken],
) -> bool:
    return matches_token(
        tokens,
        job.token_version,
        job.token_key_id,
        job.meeting_token,
    )


def _create_operation_binding(
    tokens: ErasureTokenCodec,
    *,
    request_key: str,
    actor_id: str,
    resource_id: UUID,
    erasure_job_id: UUID,
    operation: MeetingErasureOperation,
    expected_version: int,
    created_at: datetime,
) -> MeetingErasureOperationBinding:
    resource_token = (
        tokens.meeting_token(resource_id)
        if operation is MeetingErasureOperation.REQUEST
        else tokens.erasure_job_token(resource_id)
    )
    return MeetingErasureOperationBinding.create(
        tokens.request_key_token(request_key),
        tokens.actor_token(actor_id),
        resource_token,
        erasure_job_id,
        operation,
        expected_version=expected_version,
        created_at=created_at,
    )


def _validate_expected_version(expected_version: int) -> None:
    if (
        isinstance(expected_version, bool)
        or not isinstance(expected_version, int)
        or expected_version < 0
    ):
        raise ValueError("Expected version cannot be negative")
