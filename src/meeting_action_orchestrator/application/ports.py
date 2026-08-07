from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from types import TracebackType
from typing import BinaryIO, Protocol
from uuid import UUID

from meeting_action_orchestrator.agents.contracts import (
    AgentResult,
    AgentRunContext,
    ExtractionRequest,
    MeetingExtraction,
    RecapDraft,
    RecapRequest,
    VerificationReport,
    VerificationRequest,
)
from meeting_action_orchestrator.domain.enums import (
    MeetingStatus,
    ProcessingJobStatus,
    ProcessingStage,
    RecordingCleanupStatus,
)
from meeting_action_orchestrator.domain.models import (
    Approval,
    AudioAsset,
    DeliveryOperationBinding,
    ErasureKeyVerifier,
    ErasureToken,
    ErasureTokenIdentity,
    IngestRequestBinding,
    Meeting,
    MeetingErasureJob,
    MeetingErasureOperationBinding,
    MeetingErasureTombstone,
    MeetingOperationBinding,
    ProcessingJob,
    RecapArtifact,
    RecordingCleanupJob,
    ReviewRevision,
    Transcript,
    WorkflowFailure,
    WriteIntent,
    WriteReceipt,
)


@dataclass(frozen=True, slots=True)
class AudioMetadata:
    media_type: str
    duration_ms: int
    codec: str
    sample_rate_hz: int
    channels: int


@dataclass(frozen=True, slots=True)
class StoredAudio:
    storage_key: str
    original_name: str
    path: Path
    size_bytes: int
    sha256: str
    metadata: AudioMetadata


@dataclass(frozen=True, slots=True)
class WalCheckpointResult:
    busy: int
    log_frames: int
    checkpointed_frames: int

    def __post_init__(self) -> None:
        if self.busy < 0 or self.log_frames < 0 or self.checkpointed_frames < 0:
            raise ValueError("WAL checkpoint counters cannot be negative")

    @property
    def truncated(self) -> bool:
        return self.busy == 0 and self.log_frames == 0 and self.checkpointed_frames == 0


class TranscriptionSegmentLike(Protocol):
    id: str
    start_ms: int
    end_ms: int | None
    speaker: str | None
    text: str


class TranscriptionOutputLike(Protocol):
    model: str
    provider_request_id: str | None
    language: str | None
    text: str
    duration_seconds: float | None
    segments: tuple[TranscriptionSegmentLike, ...]


class RecordingStore(Protocol):
    def put(self, stream: BinaryIO, original_name: str) -> StoredAudio: ...

    def path(self, storage_key: str) -> Path: ...


class DatabaseCheckpoint(Protocol):
    def truncate_wal(self) -> WalCheckpointResult: ...


class ErasureTokenCodec(Protocol):
    @property
    def key_ids(self) -> tuple[str, ...]: ...

    def meeting_token(self, meeting_id: UUID) -> ErasureToken: ...

    def meeting_tokens(self, meeting_id: UUID) -> tuple[ErasureToken, ...]: ...

    def ingest_key_token(self, ingest_key: str) -> ErasureToken: ...

    def ingest_key_tokens(self, ingest_key: str) -> tuple[ErasureToken, ...]: ...

    def request_key_token(self, request_key: str) -> ErasureToken: ...

    def request_key_tokens(self, request_key: str) -> tuple[ErasureToken, ...]: ...

    def actor_token(self, actor_id: str) -> ErasureToken: ...

    def actor_tokens(self, actor_id: str) -> tuple[ErasureToken, ...]: ...

    def erasure_job_token(self, erasure_job_id: UUID) -> ErasureToken: ...

    def erasure_job_tokens(self, erasure_job_id: UUID) -> tuple[ErasureToken, ...]: ...

    def verifiers(self, created_at: datetime) -> tuple[ErasureKeyVerifier, ...]: ...

    def validate_verifiers(
        self,
        persisted: Sequence[ErasureKeyVerifier],
        referenced_tokens: Sequence[ErasureTokenIdentity] = (),
    ) -> None: ...


class TranscriptionProvider(Protocol):
    async def transcribe(
        self,
        audio_path: Path,
        language: str | None = None,
    ) -> TranscriptionOutputLike: ...


class SpecialistProvider(Protocol):
    async def extract(
        self,
        request: ExtractionRequest,
        context: AgentRunContext,
    ) -> AgentResult[MeetingExtraction]: ...

    async def write_recap(
        self,
        request: RecapRequest,
        context: AgentRunContext,
    ) -> AgentResult[RecapDraft]: ...

    async def verify(
        self,
        request: VerificationRequest,
        context: AgentRunContext,
    ) -> AgentResult[VerificationReport]: ...


@dataclass(frozen=True, slots=True)
class MeetingListCursor:
    created_at: datetime
    id: UUID

    def __post_init__(self) -> None:
        if self.created_at.utcoffset() is None:
            raise ValueError("created_at must include a UTC offset")


class MeetingRepository(Protocol):
    def add(self, meeting: Meeting) -> None: ...

    def get(self, meeting_id: UUID) -> Meeting | None: ...

    def find_by_ingest_key(self, ingest_key: str) -> Meeting | None: ...

    def list_page(
        self,
        *,
        status: MeetingStatus | None,
        cursor: MeetingListCursor | None,
        limit: int,
    ) -> Sequence[Meeting]: ...

    def save(self, meeting: Meeting, expected_version: int) -> None: ...


class IngestRequestBindingRepository(Protocol):
    def add(self, binding: IngestRequestBinding) -> None: ...

    def get(self, ingest_key: str) -> IngestRequestBinding | None: ...


class AudioAssetRepository(Protocol):
    def add(self, asset: AudioAsset) -> None: ...

    def get(self, asset_id: UUID) -> AudioAsset | None: ...

    def find_by_sha256(self, digest: str) -> AudioAsset | None: ...

    def find_by_storage_key(self, storage_key: str) -> AudioAsset | None: ...


class RecordingCleanupRepository(Protocol):
    def add(self, job: RecordingCleanupJob) -> None: ...

    def get(self, job_id: UUID) -> RecordingCleanupJob | None: ...

    def find_by_storage_key(self, storage_key: str) -> RecordingCleanupJob | None: ...

    def list_by_expected_sha256(self, digest: str) -> Sequence[RecordingCleanupJob]: ...

    def delete_succeeded(self, job: RecordingCleanupJob) -> bool: ...

    def claim_due(
        self,
        worker_id: str,
        now: datetime,
        lease_until: datetime,
        limit: int,
    ) -> Sequence[RecordingCleanupJob]: ...

    def save(
        self,
        job: RecordingCleanupJob,
        expected_status: RecordingCleanupStatus,
        expected_lease_owner: str | None,
        expected_lease_expires_at: datetime | None,
    ) -> None: ...


class ErasureKeyVerifierRepository(Protocol):
    def add(self, verifier: ErasureKeyVerifier) -> None: ...

    def get(self, key_id: str) -> ErasureKeyVerifier | None: ...

    def list_all(self) -> Sequence[ErasureKeyVerifier]: ...

    def list_referenced_tokens(self) -> Sequence[ErasureTokenIdentity]: ...


class MeetingErasureRepository(Protocol):
    def add(self, job: MeetingErasureJob) -> None: ...

    def get(self, job_id: UUID) -> MeetingErasureJob | None: ...

    def find_by_meeting_tokens(
        self,
        tokens: Sequence[ErasureToken],
    ) -> MeetingErasureJob | None: ...

    def list_by_pending_audio_asset_id(
        self,
        audio_asset_id: UUID,
    ) -> Sequence[MeetingErasureJob]: ...

    def list_by_cleanup_job_id(self, cleanup_job_id: UUID) -> Sequence[MeetingErasureJob]: ...

    def reactivate_failed_cleanup_group(
        self,
        cleanup_job_id: UUID,
        now: datetime,
    ) -> Sequence[MeetingErasureJob]: ...

    def claim_actionable(
        self,
        worker_id: str,
        now: datetime,
        lease_until: datetime,
        limit: int,
    ) -> Sequence[MeetingErasureJob]: ...

    def save(
        self,
        job: MeetingErasureJob,
        expected_version: int,
        expected_lease_owner: str | None,
        expected_lease_expires_at: datetime | None,
    ) -> None: ...


class MeetingErasureOperationRepository(Protocol):
    def add(self, binding: MeetingErasureOperationBinding) -> None: ...

    def find_by_request_tokens(
        self,
        tokens: Sequence[ErasureToken],
    ) -> MeetingErasureOperationBinding | None: ...

    def list_for_job(self, job_id: UUID) -> Sequence[MeetingErasureOperationBinding]: ...


class MeetingErasureTombstoneRepository(Protocol):
    def add(self, tombstone: MeetingErasureTombstone) -> None: ...

    def get_for_job(self, job_id: UUID) -> MeetingErasureTombstone | None: ...

    def find_by_meeting_tokens(
        self,
        tokens: Sequence[ErasureToken],
    ) -> MeetingErasureTombstone | None: ...

    def find_by_ingest_key_tokens(
        self,
        tokens: Sequence[ErasureToken],
    ) -> MeetingErasureTombstone | None: ...


class MeetingErasurePurgeRepository(Protocol):
    def has_active_work(self, meeting_id: UUID, now: datetime) -> bool: ...

    def meeting_graph_is_consistent(
        self,
        meeting_id: UUID,
        audio_asset_id: UUID,
    ) -> bool: ...

    def audio_has_other_references(
        self,
        audio_asset_id: UUID,
        meeting_id: UUID,
    ) -> bool: ...

    def delete_meeting_graph(self, meeting_id: UUID) -> bool: ...

    def delete_audio_asset(self, audio_asset_id: UUID) -> bool: ...


class TranscriptRepository(Protocol):
    def add(self, transcript: Transcript) -> None: ...

    def get(self, transcript_id: UUID) -> Transcript | None: ...

    def latest_for_meeting(self, meeting_id: UUID) -> Transcript | None: ...


class ReviewRepository(Protocol):
    def add(self, review: ReviewRevision) -> None: ...

    def get(self, review_id: UUID) -> ReviewRevision | None: ...

    def latest_for_meeting(self, meeting_id: UUID) -> ReviewRevision | None: ...

    def list_for_meeting(self, meeting_id: UUID) -> Sequence[ReviewRevision]: ...


class ApprovalRepository(Protocol):
    def add(self, approval: Approval) -> None: ...

    def get(self, approval_id: UUID) -> Approval | None: ...

    def for_meeting(self, meeting_id: UUID) -> Approval | None: ...

    def find_by_request_key(self, request_key: str) -> Approval | None: ...


class RecapRepository(Protocol):
    def add(self, recap: RecapArtifact) -> None: ...

    def for_approval(self, approval_id: UUID) -> RecapArtifact | None: ...


class DeliveryOperationRepository(Protocol):
    def add(self, binding: DeliveryOperationBinding) -> None: ...

    def get(self, request_key: str) -> DeliveryOperationBinding | None: ...

    def claim(
        self,
        request_key: str,
        owner: str,
        now: datetime,
        lease_until: datetime,
    ) -> DeliveryOperationBinding | None: ...

    def release(
        self,
        request_key: str,
        owner: str,
        expected_version: int,
        now: datetime,
    ) -> bool: ...

    def renew(
        self,
        request_key: str,
        owner: str,
        expected_version: int,
        now: datetime,
        lease_until: datetime,
    ) -> DeliveryOperationBinding | None: ...

    def complete(
        self,
        request_key: str,
        owner: str,
        expected_version: int,
        now: datetime,
    ) -> bool: ...


class MeetingOperationRepository(Protocol):
    def add(self, binding: MeetingOperationBinding) -> None: ...

    def get(self, request_key: str) -> MeetingOperationBinding | None: ...


class ProcessingJobRepository(Protocol):
    def add(self, job: ProcessingJob) -> None: ...

    def get(self, job_id: UUID) -> ProcessingJob | None: ...

    def find_for_stage(
        self,
        meeting_id: UUID,
        stage: ProcessingStage,
    ) -> ProcessingJob | None: ...

    def list_for_meeting(self, meeting_id: UUID) -> Sequence[ProcessingJob]: ...

    def list_expired_exhausted(
        self,
        stage: ProcessingStage,
        now: datetime,
        limit: int,
    ) -> Sequence[ProcessingJob]: ...

    def claim_due(
        self,
        stage: ProcessingStage,
        worker_id: str,
        now: datetime,
        lease_until: datetime,
        limit: int,
    ) -> Sequence[ProcessingJob]: ...

    def save(
        self,
        job: ProcessingJob,
        expected_status: ProcessingJobStatus,
        expected_lease_owner: str | None,
        expected_lease_expires_at: datetime | None,
    ) -> None: ...


class WriteIntentRepository(Protocol):
    def add_many(self, intents: Sequence[WriteIntent]) -> None: ...

    def get(self, intent_id: UUID) -> WriteIntent | None: ...

    def list_for_approval(self, approval_id: UUID) -> Sequence[WriteIntent]: ...

    def claim_due(
        self,
        worker_id: str,
        now: datetime,
        lease_until: datetime,
        limit: int,
    ) -> Sequence[WriteIntent]: ...

    def claim_due_ids(
        self,
        worker_id: str,
        now: datetime,
        lease_until: datetime,
        limit: int,
    ) -> Sequence[UUID]: ...

    def recover_expired_ids(
        self,
        now: datetime,
        failure: WorkflowFailure,
        limit: int,
    ) -> Sequence[UUID]: ...

    def list_unknown_ids(self, now: datetime, limit: int) -> Sequence[UUID]: ...

    def claim_due_unknown_ids(
        self,
        owner: str,
        now: datetime,
        lease_until: datetime,
        limit: int,
    ) -> Sequence[UUID]: ...

    def claim_unknown(
        self,
        intent_id: UUID,
        owner: str,
        now: datetime,
        lease_until: datetime,
        *,
        force: bool,
    ) -> WriteIntent | None: ...

    def save(self, intent: WriteIntent, expected_version: int) -> None: ...


class WriteReceiptRepository(Protocol):
    def add(self, receipt: WriteReceipt) -> None: ...

    def for_intent(self, intent_id: UUID) -> WriteReceipt | None: ...

    def find_by_idempotency_key(self, idempotency_key: str) -> WriteReceipt | None: ...


class UnitOfWork(Protocol):
    meetings: MeetingRepository
    ingest_requests: IngestRequestBindingRepository
    audio_assets: AudioAssetRepository
    recording_cleanups: RecordingCleanupRepository
    erasure_key_verifiers: ErasureKeyVerifierRepository
    meeting_erasures: MeetingErasureRepository
    meeting_erasure_operations: MeetingErasureOperationRepository
    meeting_erasure_tombstones: MeetingErasureTombstoneRepository
    meeting_erasure_purge: MeetingErasurePurgeRepository
    transcripts: TranscriptRepository
    reviews: ReviewRepository
    approvals: ApprovalRepository
    recaps: RecapRepository
    delivery_operations: DeliveryOperationRepository
    meeting_operations: MeetingOperationRepository
    processing_jobs: ProcessingJobRepository
    write_intents: WriteIntentRepository
    write_receipts: WriteReceiptRepository

    def __enter__(self) -> UnitOfWork: ...

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None: ...

    def commit(self) -> None: ...

    def rollback(self) -> None: ...


class Clock(Protocol):
    def now(self) -> datetime: ...
