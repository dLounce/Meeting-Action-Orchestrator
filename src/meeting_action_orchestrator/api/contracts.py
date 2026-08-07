from __future__ import annotations

from dataclasses import dataclass
from typing import BinaryIO, Protocol
from uuid import UUID

from meeting_action_orchestrator.application.ports import MeetingListCursor
from meeting_action_orchestrator.application.processing_control import ProcessingControlResult
from meeting_action_orchestrator.application.reviewing import ActionEdit, IssueResolutionEdit
from meeting_action_orchestrator.application.workflow import (
    ApprovalResult,
    IngestMeeting,
)
from meeting_action_orchestrator.domain.enums import MeetingStatus, WriteKind
from meeting_action_orchestrator.domain.models import (
    Meeting,
    ProcessingJob,
    RecapArtifact,
    ReviewRevision,
    Transcript,
    WriteIntent,
    WriteReceipt,
)


@dataclass(frozen=True, slots=True)
class Principal:
    subject: str


@dataclass(frozen=True, slots=True)
class ReadinessCheck:
    name: str
    ready: bool


@dataclass(frozen=True, slots=True)
class ReadinessResult:
    checks: tuple[ReadinessCheck, ...]

    @property
    def ready(self) -> bool:
        return all(check.ready for check in self.checks)


@dataclass(frozen=True, slots=True)
class DeliveryResult:
    meeting: Meeting
    intents: tuple[WriteIntent, ...]
    receipts: tuple[WriteReceipt, ...] = ()
    replayed: bool = False


@dataclass(frozen=True, slots=True)
class MeetingPageResult:
    items: tuple[Meeting, ...]
    next_cursor: MeetingListCursor | None


@dataclass(frozen=True, slots=True)
class ProcessingResult:
    meeting_id: UUID
    jobs: tuple[ProcessingJob, ...]


class Authenticator(Protocol):
    async def authenticate(self, token: str) -> Principal | None: ...


class ReadinessProbe(Protocol):
    async def check(self) -> ReadinessResult: ...


class MeetingWorkflowService(Protocol):
    async def ingest(self, command: IngestMeeting, stream: BinaryIO) -> Meeting: ...

    async def get_meeting(self, meeting_id: UUID) -> Meeting: ...

    async def approve(
        self,
        meeting_id: UUID,
        *,
        expected_digest: str,
        request_key: str,
        actor_id: str,
    ) -> ApprovalResult: ...


class MeetingQueryService(Protocol):
    async def list_meetings(
        self,
        *,
        status: MeetingStatus | None,
        cursor: MeetingListCursor | None,
        limit: int,
    ) -> MeetingPageResult: ...

    async def get_processing(self, meeting_id: UUID) -> ProcessingResult: ...

    async def get_transcript(self, meeting_id: UUID) -> Transcript: ...

    async def get_review(self, meeting_id: UUID) -> ReviewRevision: ...

    async def get_recap(self, meeting_id: UUID) -> RecapArtifact: ...

    async def get_delivery(self, meeting_id: UUID) -> DeliveryResult: ...


class ProcessingController(Protocol):
    async def retry(
        self,
        meeting_id: UUID,
        *,
        expected_version: int,
        request_key: str,
        actor_id: str,
    ) -> ProcessingControlResult: ...

    async def cancel(
        self,
        meeting_id: UUID,
        *,
        expected_version: int,
        request_key: str,
        actor_id: str,
    ) -> ProcessingControlResult: ...


class ReviewEditor(Protocol):
    async def revise_action(
        self,
        meeting_id: UUID,
        *,
        expected_digest: str,
        edit: ActionEdit,
        actor_id: str,
    ) -> ReviewRevision: ...

    async def revise_delivery(
        self,
        meeting_id: UUID,
        *,
        expected_digest: str,
        action_id: UUID,
        kind: WriteKind,
        enabled: bool,
        actor_id: str,
    ) -> ReviewRevision: ...

    async def revise_issue(
        self,
        meeting_id: UUID,
        *,
        expected_digest: str,
        edit: IssueResolutionEdit,
        actor_id: str,
    ) -> ReviewRevision: ...


class DeliveryService(Protocol):
    async def retry(
        self,
        meeting_id: UUID,
        *,
        intent_ids: tuple[UUID, ...],
        request_key: str,
        actor_id: str,
    ) -> DeliveryResult: ...

    async def reconcile(
        self,
        meeting_id: UUID,
        *,
        intent_ids: tuple[UUID, ...],
        request_key: str,
        actor_id: str,
    ) -> DeliveryResult: ...


@dataclass(frozen=True, slots=True)
class ApiDependencies:
    workflow: MeetingWorkflowService
    queries: MeetingQueryService
    processing_controls: ProcessingController
    reviews: ReviewEditor
    deliveries: DeliveryService
    authenticator: Authenticator
    readiness: ReadinessProbe
    max_upload_bytes: int
    service_version: str = "0.1.0"

    def __post_init__(self) -> None:
        if self.max_upload_bytes <= 0:
            raise ValueError("max_upload_bytes must be positive")
