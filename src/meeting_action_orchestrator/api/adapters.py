from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import BinaryIO, Protocol, TypeVar
from uuid import UUID

from meeting_action_orchestrator.api.contracts import (
    DeliveryResult,
    MeetingPageResult,
    ProcessingResult,
    ReadinessCheck,
    ReadinessResult,
)
from meeting_action_orchestrator.application.errors import (
    OperationConflictError,
    ResourceNotFoundError,
)
from meeting_action_orchestrator.application.ports import MeetingListCursor, UnitOfWork
from meeting_action_orchestrator.application.reviewing import ActionEdit, IssueResolutionEdit
from meeting_action_orchestrator.application.workflow import (
    ApprovalResult,
    IngestMeeting,
    ReviewUpdateResult,
)
from meeting_action_orchestrator.domain.enums import MeetingStatus, WriteKind
from meeting_action_orchestrator.domain.models import (
    Meeting,
    RecapArtifact,
    ReviewRevision,
    Transcript,
    WriteIntent,
    WriteReceipt,
)
from meeting_action_orchestrator.domain.services import validate_write_receipt

UnitOfWorkFactory = Callable[[], UnitOfWork]


class WorkflowBackend(Protocol):
    def ingest(self, command: IngestMeeting, stream: BinaryIO) -> Meeting: ...

    def get_meeting(self, meeting_id: UUID) -> Meeting: ...

    def approve(
        self,
        meeting_id: UUID,
        *,
        expected_digest: str,
        expected_version: int,
        request_key: str,
        actor_id: str,
    ) -> ApprovalResult: ...

    def revise_action(
        self,
        meeting_id: UUID,
        *,
        edit: ActionEdit,
        expected_digest: str,
        expected_version: int,
        actor_id: str,
    ) -> ReviewUpdateResult: ...

    def revise_delivery(
        self,
        meeting_id: UUID,
        *,
        action_id: UUID,
        kind: WriteKind,
        enabled: bool,
        expected_digest: str,
        expected_version: int,
        actor_id: str,
    ) -> ReviewUpdateResult: ...

    def revise_issue(
        self,
        meeting_id: UUID,
        *,
        edit: IssueResolutionEdit,
        expected_digest: str,
        expected_version: int,
        actor_id: str,
    ) -> ReviewUpdateResult: ...


class DeliveryControlOutput(Protocol):
    @property
    def meeting(self) -> Meeting: ...

    @property
    def intents(self) -> tuple[WriteIntent, ...]: ...

    @property
    def receipts(self) -> tuple[WriteReceipt, ...]: ...

    @property
    def replayed(self) -> bool: ...


class DeliveryController(Protocol):
    async def retry(
        self,
        meeting_id: UUID,
        *,
        intent_ids: tuple[UUID, ...],
        request_key: str,
        actor_id: str,
    ) -> DeliveryControlOutput: ...

    async def reconcile(
        self,
        meeting_id: UUID,
        *,
        intent_ids: tuple[UUID, ...],
        request_key: str,
        actor_id: str,
    ) -> DeliveryControlOutput: ...


class DatabaseHealthcheck(Protocol):
    def healthcheck(self) -> bool: ...


class AsyncWorkflowFacade:
    def __init__(self, workflow: WorkflowBackend) -> None:
        self._workflow = workflow

    async def ingest(self, command: IngestMeeting, stream: BinaryIO) -> Meeting:
        return await asyncio.to_thread(self._workflow.ingest, command, stream)

    async def get_meeting(self, meeting_id: UUID) -> Meeting:
        return await asyncio.to_thread(self._workflow.get_meeting, meeting_id)

    async def approve(
        self,
        meeting_id: UUID,
        *,
        expected_digest: str,
        request_key: str,
        actor_id: str,
    ) -> ApprovalResult:
        def execute() -> ApprovalResult:
            version = self._workflow.get_meeting(meeting_id).version
            return self._workflow.approve(
                meeting_id,
                expected_digest=expected_digest,
                expected_version=version,
                request_key=request_key,
                actor_id=actor_id,
            )

        return await asyncio.to_thread(execute)

    async def revise_action(
        self,
        meeting_id: UUID,
        *,
        expected_digest: str,
        edit: ActionEdit,
        actor_id: str,
    ) -> ReviewRevision:
        def execute() -> ReviewUpdateResult:
            version = self._workflow.get_meeting(meeting_id).version
            return self._workflow.revise_action(
                meeting_id,
                edit=edit,
                expected_digest=expected_digest,
                expected_version=version,
                actor_id=actor_id,
            )

        return (await asyncio.to_thread(execute)).review

    async def revise_delivery(
        self,
        meeting_id: UUID,
        *,
        expected_digest: str,
        action_id: UUID,
        kind: WriteKind,
        enabled: bool,
        actor_id: str,
    ) -> ReviewRevision:
        def execute() -> ReviewUpdateResult:
            version = self._workflow.get_meeting(meeting_id).version
            return self._workflow.revise_delivery(
                meeting_id,
                action_id=action_id,
                kind=kind,
                enabled=enabled,
                expected_digest=expected_digest,
                expected_version=version,
                actor_id=actor_id,
            )

        return (await asyncio.to_thread(execute)).review

    async def revise_issue(
        self,
        meeting_id: UUID,
        *,
        expected_digest: str,
        edit: IssueResolutionEdit,
        actor_id: str,
    ) -> ReviewRevision:
        def execute() -> ReviewUpdateResult:
            version = self._workflow.get_meeting(meeting_id).version
            return self._workflow.revise_issue(
                meeting_id,
                edit=edit,
                expected_digest=expected_digest,
                expected_version=version,
                actor_id=actor_id,
            )

        return (await asyncio.to_thread(execute)).review


class UnitOfWorkQueryFacade:
    def __init__(self, unit_of_work: UnitOfWorkFactory) -> None:
        self._unit_of_work = unit_of_work

    async def list_meetings(
        self,
        *,
        status: MeetingStatus | None,
        cursor: MeetingListCursor | None,
        limit: int,
    ) -> MeetingPageResult:
        return await asyncio.to_thread(
            self._list_meetings,
            status=status,
            cursor=cursor,
            limit=limit,
        )

    async def get_processing(self, meeting_id: UUID) -> ProcessingResult:
        return await asyncio.to_thread(self._get_processing, meeting_id)

    async def get_transcript(self, meeting_id: UUID) -> Transcript:
        return await asyncio.to_thread(self._get_transcript, meeting_id)

    async def get_review(self, meeting_id: UUID) -> ReviewRevision:
        return await asyncio.to_thread(self._get_review, meeting_id)

    async def get_delivery(self, meeting_id: UUID) -> DeliveryResult:
        return await asyncio.to_thread(self._get_delivery, meeting_id)

    async def get_recap(self, meeting_id: UUID) -> RecapArtifact:
        return await asyncio.to_thread(self._get_recap, meeting_id)

    def _list_meetings(
        self,
        *,
        status: MeetingStatus | None,
        cursor: MeetingListCursor | None,
        limit: int,
    ) -> MeetingPageResult:
        if not 1 <= limit <= 100:
            raise ValueError("limit must be between one and 100")
        with self._unit_of_work() as uow:
            records = tuple(
                uow.meetings.list_page(
                    status=status,
                    cursor=cursor,
                    limit=limit + 1,
                )
            )
        items = records[:limit]
        next_cursor = (
            MeetingListCursor(created_at=items[-1].created_at, id=items[-1].id)
            if len(records) > limit
            else None
        )
        return MeetingPageResult(items=items, next_cursor=next_cursor)

    def _get_processing(self, meeting_id: UUID) -> ProcessingResult:
        with self._unit_of_work() as uow:
            meeting = _required(uow.meetings.get(meeting_id), "Meeting")
            jobs = tuple(uow.processing_jobs.list_for_meeting(meeting_id))
        if any(job.meeting_id != meeting.id for job in jobs):
            raise OperationConflictError("A processing job does not belong to the meeting")
        return ProcessingResult(meeting_id=meeting.id, jobs=jobs)

    def _get_transcript(self, meeting_id: UUID) -> Transcript:
        with self._unit_of_work() as uow:
            meeting = _required(uow.meetings.get(meeting_id), "Meeting")
            if meeting.current_transcript_id is None:
                raise ResourceNotFoundError("Transcript")
            transcript = _required(
                uow.transcripts.get(meeting.current_transcript_id),
                "Transcript",
            )
        if transcript.meeting_id != meeting.id or transcript.id != meeting.current_transcript_id:
            raise OperationConflictError("The current transcript does not belong to the meeting")
        return transcript

    def _get_review(self, meeting_id: UUID) -> ReviewRevision:
        with self._unit_of_work() as uow:
            meeting = _required(uow.meetings.get(meeting_id), "Meeting")
            if meeting.current_review_id is None:
                raise ResourceNotFoundError("Review")
            review = _required(uow.reviews.get(meeting.current_review_id), "Review")
        if review.meeting_id != meeting.id or review.id != meeting.current_review_id:
            raise OperationConflictError("The current review does not belong to the meeting")
        return review

    def _get_recap(self, meeting_id: UUID) -> RecapArtifact:
        with self._unit_of_work() as uow:
            meeting = _required(uow.meetings.get(meeting_id), "Meeting")
            approval = uow.approvals.for_meeting(meeting_id)
            if approval is None:
                raise ResourceNotFoundError("Recap")
            recap = uow.recaps.for_approval(approval.id)
            if recap is None:
                raise ResourceNotFoundError("Recap")
        if (
            approval.meeting_id != meeting.id
            or meeting.approved_review_id != approval.review_revision_id
            or recap.meeting_id != meeting.id
            or recap.approval_id != approval.id
        ):
            raise OperationConflictError("The recap does not belong to the meeting approval")
        return recap

    def _get_delivery(self, meeting_id: UUID) -> DeliveryResult:
        with self._unit_of_work() as uow:
            meeting = _required(uow.meetings.get(meeting_id), "Meeting")
            approval = _required(uow.approvals.for_meeting(meeting_id), "Approval")
            intents = tuple(uow.write_intents.list_for_approval(approval.id))
            receipts = tuple(
                receipt
                for intent in intents
                if (receipt := uow.write_receipts.for_intent(intent.id)) is not None
            )
        if (
            approval.meeting_id != meeting.id
            or meeting.approved_review_id != approval.review_revision_id
        ):
            raise OperationConflictError("The approval does not belong to the meeting")
        for intent in intents:
            if intent.meeting_id != meeting.id or intent.approval_id != approval.id:
                raise OperationConflictError("A write intent does not belong to the meeting")
        indexed = {intent.id: intent for intent in intents}
        for receipt in receipts:
            intent = indexed.get(receipt.intent_id)
            if intent is None:
                raise OperationConflictError("A write receipt does not belong to the meeting")
            validate_write_receipt(intent, receipt)
        return DeliveryResult(meeting=meeting, intents=intents, receipts=receipts)


class AsyncDeliveryFacade:
    def __init__(self, controller: DeliveryController) -> None:
        self._controller = controller

    async def retry(
        self,
        meeting_id: UUID,
        *,
        intent_ids: tuple[UUID, ...],
        request_key: str,
        actor_id: str,
    ) -> DeliveryResult:
        result = await self._controller.retry(
            meeting_id,
            intent_ids=intent_ids,
            request_key=request_key,
            actor_id=actor_id,
        )
        return _delivery_result(result)

    async def reconcile(
        self,
        meeting_id: UUID,
        *,
        intent_ids: tuple[UUID, ...],
        request_key: str,
        actor_id: str,
    ) -> DeliveryResult:
        result = await self._controller.reconcile(
            meeting_id,
            intent_ids=intent_ids,
            request_key=request_key,
            actor_id=actor_id,
        )
        return _delivery_result(result)


class DatabaseConfigReadinessProbe:
    def __init__(
        self,
        database: DatabaseHealthcheck,
        configuration_check: Callable[[], bool],
    ) -> None:
        self._database = database
        self._configuration_check = configuration_check

    async def check(self) -> ReadinessResult:
        return await asyncio.to_thread(self._check)

    def _check(self) -> ReadinessResult:
        return ReadinessResult(
            (
                ReadinessCheck("database", _safe_check(self._database.healthcheck)),
                ReadinessCheck("configuration", _safe_check(self._configuration_check)),
            )
        )


ValueT = TypeVar("ValueT")


def _required(value: ValueT | None, resource: str) -> ValueT:
    if value is None:
        raise ResourceNotFoundError(resource)
    return value


def _delivery_result(result: DeliveryControlOutput) -> DeliveryResult:
    return DeliveryResult(
        meeting=result.meeting,
        intents=tuple(result.intents),
        receipts=tuple(result.receipts),
        replayed=result.replayed,
    )


def _safe_check(check: Callable[[], bool]) -> bool:
    try:
        return check() is True
    except Exception:
        return False
