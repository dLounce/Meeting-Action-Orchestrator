from __future__ import annotations

import io
import threading
from datetime import datetime, timezone
from typing import BinaryIO, cast
from uuid import UUID

import pytest

from meeting_action_orchestrator.api.adapters import (
    AsyncDeliveryFacade,
    AsyncWorkflowFacade,
    DatabaseConfigReadinessProbe,
    UnitOfWorkFactory,
    UnitOfWorkQueryFacade,
)
from meeting_action_orchestrator.application.delivery_control import DeliveryControlResult
from meeting_action_orchestrator.application.errors import OperationConflictError
from meeting_action_orchestrator.application.reviewing import ActionEdit, IssueResolutionEdit
from meeting_action_orchestrator.application.workflow import (
    ApprovalResult,
    IngestMeeting,
    ReviewUpdateResult,
)
from meeting_action_orchestrator.domain.enums import (
    IssueStatus,
    MeetingStatus,
    WriteKind,
)
from meeting_action_orchestrator.domain.models import Approval, Meeting, ReviewRevision, Transcript
from tests.unit.api.test_app import (
    ACTION_ID,
    APPROVAL_ID,
    ISSUE_ID,
    MEETING_ID,
    REVIEW_ID,
    meeting,
    review,
    transcript,
)

NOW = datetime(2026, 8, 7, 10, 0, tzinfo=timezone.utc)
OTHER_MEETING_ID = UUID("10000000-0000-4000-8000-000000000099")


def versioned_meeting(version: int = 7) -> Meeting:
    return Meeting.model_validate(meeting().model_dump(mode="python") | {"version": version})


class ThreadAwareWorkflow:
    def __init__(self) -> None:
        self.sync_thread_ids: list[int] = []
        self.expected_versions: list[int] = []
        self.approval_result = cast(ApprovalResult, object())

    def ingest(self, command: IngestMeeting, stream: BinaryIO) -> Meeting:
        self.sync_thread_ids.append(threading.get_ident())
        assert command.ingest_key == "upload-one"
        assert stream.read() == b"audio"
        return versioned_meeting()

    def get_meeting(self, meeting_id: UUID) -> Meeting:
        self.sync_thread_ids.append(threading.get_ident())
        assert meeting_id == MEETING_ID
        return versioned_meeting()

    def approve(
        self,
        meeting_id: UUID,
        *,
        expected_digest: str,
        expected_version: int,
        request_key: str,
        actor_id: str,
    ) -> ApprovalResult:
        self.sync_thread_ids.append(threading.get_ident())
        self.expected_versions.append(expected_version)
        assert meeting_id == MEETING_ID
        assert expected_digest == review().content_digest
        assert request_key == "approval-one"
        assert actor_id == "portfolio-owner"
        return self.approval_result

    def revise_action(
        self,
        meeting_id: UUID,
        *,
        edit: ActionEdit,
        expected_digest: str,
        expected_version: int,
        actor_id: str,
    ) -> ReviewUpdateResult:
        self.sync_thread_ids.append(threading.get_ident())
        self.expected_versions.append(expected_version)
        assert meeting_id == MEETING_ID
        assert edit.action_id == ACTION_ID
        assert expected_digest == review().content_digest
        assert actor_id == "portfolio-owner"
        return ReviewUpdateResult(versioned_meeting(), review())

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
    ) -> ReviewUpdateResult:
        self.sync_thread_ids.append(threading.get_ident())
        self.expected_versions.append(expected_version)
        assert meeting_id == MEETING_ID
        assert action_id == ACTION_ID
        assert kind is WriteKind.TASK
        assert enabled
        assert expected_digest == review().content_digest
        assert actor_id == "portfolio-owner"
        return ReviewUpdateResult(versioned_meeting(), review())

    def revise_issue(
        self,
        meeting_id: UUID,
        *,
        edit: IssueResolutionEdit,
        expected_digest: str,
        expected_version: int,
        actor_id: str,
    ) -> ReviewUpdateResult:
        self.sync_thread_ids.append(threading.get_ident())
        self.expected_versions.append(expected_version)
        assert meeting_id == MEETING_ID
        assert edit.issue_id == ISSUE_ID
        assert expected_digest == review().content_digest
        assert actor_id == "portfolio-owner"
        return ReviewUpdateResult(versioned_meeting(), review())


async def test_workflow_facade_offloads_ingest_and_status_reads() -> None:
    backend = ThreadAwareWorkflow()
    facade = AsyncWorkflowFacade(backend)
    main_thread = threading.get_ident()
    command = IngestMeeting(
        title="Release planning",
        occurred_at=NOW,
        timezone="UTC",
        original_name="meeting.wav",
        ingest_key="upload-one",
    )

    await facade.ingest(command, io.BytesIO(b"audio"))
    await facade.get_meeting(MEETING_ID)
    processing_status = await facade.process(MEETING_ID)

    assert processing_status == versioned_meeting()
    assert backend.sync_thread_ids
    assert all(thread_id != main_thread for thread_id in backend.sync_thread_ids)


async def test_workflow_facade_supplies_fresh_versions_to_mutations() -> None:
    backend = ThreadAwareWorkflow()
    facade = AsyncWorkflowFacade(backend)
    main_thread = threading.get_ident()

    approved = await facade.approve(
        MEETING_ID,
        expected_digest=review().content_digest,
        request_key="approval-one",
        actor_id="portfolio-owner",
    )
    revised_action = await facade.revise_action(
        MEETING_ID,
        expected_digest=review().content_digest,
        edit=ActionEdit(ACTION_ID, "Publish", None, None, None, "UTC", None),
        actor_id="portfolio-owner",
    )
    revised_delivery = await facade.revise_delivery(
        MEETING_ID,
        expected_digest=review().content_digest,
        action_id=ACTION_ID,
        kind=WriteKind.TASK,
        enabled=True,
        actor_id="portfolio-owner",
    )
    revised_issue = await facade.revise_issue(
        MEETING_ID,
        expected_digest=review().content_digest,
        edit=IssueResolutionEdit(ISSUE_ID, IssueStatus.RESOLVED, "Resolved"),
        actor_id="portfolio-owner",
    )

    assert approved is backend.approval_result
    assert revised_action == review()
    assert revised_delivery == review()
    assert revised_issue == review()
    assert backend.expected_versions == [7, 7, 7, 7]
    assert backend.sync_thread_ids
    assert all(thread_id != main_thread for thread_id in backend.sync_thread_ids)


class ItemRepository:
    def __init__(self, value: object | None) -> None:
        self.value = value

    def get(self, _item_id: UUID) -> object | None:
        return self.value


class ApprovalRepository:
    def __init__(self, value: Approval | None) -> None:
        self.value = value

    def for_meeting(self, _meeting_id: UUID) -> Approval | None:
        return self.value


class IntentRepository:
    def __init__(self, values: tuple[object, ...] = ()) -> None:
        self.values = values

    def list_for_approval(self, _approval_id: UUID) -> tuple[object, ...]:
        return self.values


class ReceiptRepository:
    def for_intent(self, _intent_id: UUID) -> None:
        return None


class QueryUnitOfWork:
    def __init__(
        self,
        current_meeting: Meeting,
        *,
        current_transcript: Transcript | None = None,
        current_review: ReviewRevision | None = None,
        approval: Approval | None = None,
    ) -> None:
        self.meetings = ItemRepository(current_meeting)
        self.transcripts = ItemRepository(current_transcript)
        self.reviews = ItemRepository(current_review)
        self.approvals = ApprovalRepository(approval)
        self.write_intents = IntentRepository()
        self.write_receipts = ReceiptRepository()

    def __enter__(self) -> QueryUnitOfWork:
        return self

    def __exit__(self, *_args: object) -> bool:
        return False


def current_review_meeting() -> Meeting:
    return Meeting.model_validate(
        meeting().model_dump(mode="python")
        | {
            "status": MeetingStatus.AWAITING_APPROVAL,
            "current_transcript_id": transcript().id,
            "current_review_id": review().id,
        }
    )


def approved_meeting() -> Meeting:
    return Meeting.model_validate(
        current_review_meeting().model_dump(mode="python")
        | {
            "status": MeetingStatus.COMPLETED,
            "approved_review_id": review().id,
        }
    )


def approval(meeting_id: UUID = MEETING_ID) -> Approval:
    return Approval(
        id=APPROVAL_ID,
        meeting_id=meeting_id,
        review_revision_id=REVIEW_ID,
        review_digest=review().content_digest,
        request_key="approval-one",
        actor_id="portfolio-owner",
        approved_at=NOW,
    )


async def test_query_facade_loads_current_records_and_approved_delivery_state() -> None:
    reviewed = current_review_meeting()
    review_uow = QueryUnitOfWork(
        reviewed,
        current_transcript=transcript(),
        current_review=review(),
    )
    query = UnitOfWorkQueryFacade(cast(UnitOfWorkFactory, lambda: review_uow))

    loaded_transcript = await query.get_transcript(MEETING_ID)
    loaded_review = await query.get_review(MEETING_ID)

    delivery_uow = QueryUnitOfWork(approved_meeting(), approval=approval())
    delivery_query = UnitOfWorkQueryFacade(cast(UnitOfWorkFactory, lambda: delivery_uow))
    delivery = await delivery_query.get_delivery(MEETING_ID)

    assert loaded_transcript == transcript()
    assert loaded_review == review()
    assert delivery.meeting == approved_meeting()
    assert delivery.intents == ()
    assert delivery.receipts == ()


async def test_query_facade_rejects_cross_meeting_records() -> None:
    wrong_transcript = transcript().model_copy(update={"meeting_id": OTHER_MEETING_ID})
    transcript_uow = QueryUnitOfWork(
        current_review_meeting(),
        current_transcript=wrong_transcript,
    )
    transcript_query = UnitOfWorkQueryFacade(cast(UnitOfWorkFactory, lambda: transcript_uow))
    delivery_uow = QueryUnitOfWork(
        approved_meeting(),
        approval=approval(OTHER_MEETING_ID),
    )
    delivery_query = UnitOfWorkQueryFacade(cast(UnitOfWorkFactory, lambda: delivery_uow))

    with pytest.raises(OperationConflictError, match="does not belong"):
        await transcript_query.get_transcript(MEETING_ID)
    with pytest.raises(OperationConflictError, match="does not belong"):
        await delivery_query.get_delivery(MEETING_ID)


class FakeDeliveryController:
    def __init__(self) -> None:
        self.calls: list[tuple[str, UUID, tuple[UUID, ...], str, str]] = []

    async def retry(
        self,
        meeting_id: UUID,
        *,
        intent_ids: tuple[UUID, ...],
        request_key: str,
        actor_id: str,
    ) -> DeliveryControlResult:
        self.calls.append(("retry", meeting_id, intent_ids, request_key, actor_id))
        return DeliveryControlResult(meeting(), (), replayed=True)

    async def reconcile(
        self,
        meeting_id: UUID,
        *,
        intent_ids: tuple[UUID, ...],
        request_key: str,
        actor_id: str,
    ) -> DeliveryControlResult:
        self.calls.append(("reconcile", meeting_id, intent_ids, request_key, actor_id))
        return DeliveryControlResult(meeting(), ())


async def test_delivery_facade_preserves_control_results_and_identity() -> None:
    controller = FakeDeliveryController()
    facade = AsyncDeliveryFacade(controller)

    retried = await facade.retry(
        MEETING_ID,
        intent_ids=(),
        request_key="retry-one",
        actor_id="portfolio-owner",
    )
    reconciled = await facade.reconcile(
        MEETING_ID,
        intent_ids=(),
        request_key="reconcile-one",
        actor_id="portfolio-owner",
    )

    assert retried.replayed
    assert not reconciled.replayed
    assert controller.calls == [
        ("retry", MEETING_ID, (), "retry-one", "portfolio-owner"),
        ("reconcile", MEETING_ID, (), "reconcile-one", "portfolio-owner"),
    ]


class FakeDatabase:
    def __init__(self, result: bool | Exception) -> None:
        self.result = result
        self.thread_id: int | None = None

    def healthcheck(self) -> bool:
        self.thread_id = threading.get_ident()
        if isinstance(self.result, Exception):
            raise self.result
        return self.result


async def test_readiness_probe_is_safe_and_runs_database_check_off_loop() -> None:
    database = FakeDatabase(True)
    main_thread = threading.get_ident()
    probe = DatabaseConfigReadinessProbe(database, lambda: True)
    failed_probe = DatabaseConfigReadinessProbe(
        FakeDatabase(RuntimeError("private database detail")),
        lambda: False,
    )

    ready = await probe.check()
    failed = await failed_probe.check()

    assert ready.ready
    assert database.thread_id != main_thread
    assert [(item.name, item.ready) for item in failed.checks] == [
        ("database", False),
        ("configuration", False),
    ]
