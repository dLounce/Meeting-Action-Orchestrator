from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from types import TracebackType
from typing import Protocol
from uuid import UUID

from meeting_action_orchestrator.domain.models import (
    Approval,
    AudioAsset,
    Meeting,
    RecapArtifact,
    ReviewRevision,
    Transcript,
    WriteIntent,
    WriteReceipt,
)


class AudioStore(Protocol):
    def put(self, key: str, content: bytes) -> None: ...

    def read(self, key: str) -> bytes: ...

    def delete(self, key: str) -> None: ...


class Transcriber(Protocol):
    async def transcribe(self, meeting: Meeting, asset: AudioAsset) -> Transcript: ...


class Extractor(Protocol):
    async def extract(self, meeting: Meeting, transcript: Transcript) -> ReviewRevision: ...


class RecapWriter(Protocol):
    async def write(self, meeting: Meeting, review: ReviewRevision) -> str: ...


class TaskGateway(Protocol):
    async def ensure_task(self, intent: WriteIntent) -> WriteReceipt: ...

    async def find_task(self, idempotency_key: str) -> WriteReceipt | None: ...


class CalendarGateway(Protocol):
    async def ensure_event(self, intent: WriteIntent) -> WriteReceipt: ...

    async def find_event(self, idempotency_key: str) -> WriteReceipt | None: ...


class MeetingRepository(Protocol):
    def add(self, meeting: Meeting) -> None: ...

    def get(self, meeting_id: UUID) -> Meeting | None: ...

    def find_by_ingest_key(self, ingest_key: str) -> Meeting | None: ...

    def save(self, meeting: Meeting, expected_version: int) -> None: ...


class AudioAssetRepository(Protocol):
    def add(self, asset: AudioAsset) -> None: ...

    def get(self, asset_id: UUID) -> AudioAsset | None: ...

    def find_by_sha256(self, digest: str) -> AudioAsset | None: ...


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

    def save(self, intent: WriteIntent, expected_version: int) -> None: ...


class WriteReceiptRepository(Protocol):
    def add(self, receipt: WriteReceipt) -> None: ...

    def for_intent(self, intent_id: UUID) -> WriteReceipt | None: ...

    def find_by_idempotency_key(self, idempotency_key: str) -> WriteReceipt | None: ...


class UnitOfWork(Protocol):
    meetings: MeetingRepository
    audio_assets: AudioAssetRepository
    transcripts: TranscriptRepository
    reviews: ReviewRepository
    approvals: ApprovalRepository
    recaps: RecapRepository
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
