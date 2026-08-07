from __future__ import annotations

import io
from datetime import date
from pathlib import Path
from typing import cast
from uuid import UUID

import pytest

from meeting_action_orchestrator.application.errors import StaleWorkflowVersionError
from meeting_action_orchestrator.application.reviewing import ActionEdit
from meeting_action_orchestrator.application.workflow import IngestMeeting
from meeting_action_orchestrator.domain.enums import MeetingStatus
from meeting_action_orchestrator.domain.workflow_events import (
    DeliveryTransitionMetadata,
    MeetingTransitionMetadata,
    ReviewApprovedMetadata,
    ReviewChangeKind,
    ReviewRevisionMetadata,
    SpecialistHandoffMetadata,
    SpecialistRole,
    WorkflowEvent,
    WorkflowEventDraft,
    WorkflowEventType,
)
from meeting_action_orchestrator.infrastructure.database import Database
from meeting_action_orchestrator.infrastructure.repositories import SqliteUnitOfWork
from meeting_action_orchestrator.infrastructure.workflow_events import (
    SqliteWorkflowEventRepository,
)
from tests.integration.test_workflow import NOW, process_meeting, workflow


class RejectingEventRepository:
    def __init__(self, delegate: object) -> None:
        self._delegate = delegate

    def append(self, draft: WorkflowEventDraft) -> object:
        del draft
        raise RuntimeError("workflow event append rejected")

    def __getattr__(self, name: str) -> object:
        return getattr(self._delegate, name)


class RejectingEventUnitOfWork(SqliteUnitOfWork):
    def __enter__(self) -> RejectingEventUnitOfWork:
        super().__enter__()
        self.workflow_events = cast(
            SqliteWorkflowEventRepository,
            RejectingEventRepository(self.workflow_events),
        )
        return self


def events(database: Database, meeting_id: UUID) -> tuple[WorkflowEvent, ...]:
    with SqliteUnitOfWork(database, immediate=False) as uow:
        return tuple(uow.workflow_events.list_page(meeting_id, cursor=None, limit=100))


def command(actor_id: str = "portfolio-owner") -> IngestMeeting:
    return IngestMeeting(
        title="Release planning",
        occurred_at=NOW,
        timezone="UTC",
        original_name="private-recording.wav",
        ingest_key="workflow-event-upload",
        actor_id=actor_id,
    )


def test_ingest_event_is_actor_attributed_and_exact_replay_emits_nothing(
    tmp_path: Path,
) -> None:
    service, database = workflow(tmp_path)
    audio = b"RIFF\x00\x00\x00\x00WAVEaudit"

    meeting = service.ingest(command(), io.BytesIO(audio))
    replay = service.ingest(command("second-authenticated-owner"), io.BytesIO(audio))

    audit = events(database, meeting.id)
    assert replay == meeting
    assert len(audit) == 1
    assert audit[0].type is WorkflowEventType.MEETING_INGESTED
    assert audit[0].actor_id == "portfolio-owner"
    assert "private-recording.wav" not in audit[0].safe_metadata.model_dump_json()


def test_ingest_rolls_back_when_event_append_fails(tmp_path: Path) -> None:
    service, database = workflow(
        tmp_path,
        unit_of_work_type=RejectingEventUnitOfWork,
    )

    with pytest.raises(RuntimeError, match="append rejected"):
        service.ingest(command(), io.BytesIO(b"RIFF\x00\x00\x00\x00WAVEaudit"))

    with SqliteUnitOfWork(database, immediate=False) as uow:
        assert uow.meetings.find_by_ingest_key("workflow-event-upload") is None


@pytest.mark.asyncio
async def test_processing_review_and_approval_events_have_exact_order_and_metadata(
    tmp_path: Path,
) -> None:
    service, database = workflow(tmp_path)
    meeting = service.ingest(
        command(),
        io.BytesIO(b"RIFF\x00\x00\x00\x00WAVEaudit"),
    )
    reviewed = await process_meeting(service, database, meeting.id)

    initial = events(database, meeting.id)
    assert [event.type for event in initial] == [
        WorkflowEventType.MEETING_INGESTED,
        WorkflowEventType.PROCESSING_ATTEMPTED,
        WorkflowEventType.MEETING_TRANSITIONED,
        WorkflowEventType.MEETING_TRANSITIONED,
        WorkflowEventType.PROCESSING_ATTEMPTED,
        WorkflowEventType.PROCESSING_ATTEMPTED,
        WorkflowEventType.MEETING_TRANSITIONED,
        WorkflowEventType.SPECIALIST_HANDOFF_COMPLETED,
        WorkflowEventType.SPECIALIST_HANDOFF_COMPLETED,
        WorkflowEventType.SPECIALIST_HANDOFF_COMPLETED,
        WorkflowEventType.REVIEW_REVISED,
        WorkflowEventType.MEETING_TRANSITIONED,
        WorkflowEventType.PROCESSING_ATTEMPTED,
    ]
    handoffs = tuple(
        event.safe_metadata
        for event in initial
        if isinstance(event.safe_metadata, SpecialistHandoffMetadata)
    )
    assert [handoff.specialist for handoff in handoffs] == [
        SpecialistRole.EXTRACT,
        SpecialistRole.RECAP,
        SpecialistRole.VERIFY,
    ]
    assert all(handoff.processing_attempt_number == 1 for handoff in handoffs)
    model_review = next(
        event.safe_metadata
        for event in initial
        if isinstance(event.safe_metadata, ReviewRevisionMetadata)
    )
    assert model_review.change_kind is ReviewChangeKind.MODEL_CREATED

    with SqliteUnitOfWork(database, immediate=False) as uow:
        review = uow.reviews.latest_for_meeting(meeting.id)
    assert review is not None
    revised = service.revise_action(
        meeting.id,
        edit=ActionEdit(
            action_id=review.action_items[0].id,
            title="Publish the approved brief",
            owner="Dev",
            due_date=date(2026, 8, 15),
            due_time=None,
            timezone="UTC",
            notes="Publish after final approval.",
        ),
        expected_digest=review.content_digest,
        expected_version=reviewed.version,
        actor_id="human-reviewer",
    )
    after_revision = events(database, meeting.id)
    revision_events = after_revision[len(initial) :]
    assert [event.type for event in revision_events] == [
        WorkflowEventType.REVIEW_REVISED,
        WorkflowEventType.MEETING_TRANSITIONED,
    ]
    assert all(event.actor_id == "human-reviewer" for event in revision_events)
    revision_metadata = revision_events[0].safe_metadata
    transition_metadata = revision_events[1].safe_metadata
    assert isinstance(revision_metadata, ReviewRevisionMetadata)
    assert revision_metadata.change_kind is ReviewChangeKind.ACTION_EDITED
    assert isinstance(transition_metadata, MeetingTransitionMetadata)
    assert transition_metadata.previous_status is MeetingStatus.AWAITING_APPROVAL
    assert transition_metadata.current_status is MeetingStatus.AWAITING_APPROVAL

    before_stale = len(after_revision)
    with pytest.raises(StaleWorkflowVersionError):
        service.revise_action(
            meeting.id,
            edit=ActionEdit(
                action_id=revised.review.action_items[0].id,
                title="Stale edit",
                owner="Dev",
                due_date=date(2026, 8, 16),
                due_time=None,
                timezone="UTC",
                notes=None,
            ),
            expected_digest=revised.review.content_digest,
            expected_version=reviewed.version,
            actor_id="human-reviewer",
        )
    assert len(events(database, meeting.id)) == before_stale

    approved = service.approve(
        meeting.id,
        expected_digest=revised.review.content_digest,
        expected_version=revised.meeting.version,
        request_key="approval-audit-one",
        actor_id="approver",
    )
    after_approval = events(database, meeting.id)
    approval_events = after_approval[before_stale:]
    assert [event.type for event in approval_events] == [
        WorkflowEventType.MEETING_TRANSITIONED,
        WorkflowEventType.REVIEW_APPROVED,
        WorkflowEventType.DELIVERY_TRANSITIONED,
        WorkflowEventType.DELIVERY_TRANSITIONED,
        WorkflowEventType.MEETING_TRANSITIONED,
    ]
    assert all(event.actor_id == "approver" for event in approval_events)
    approval_metadata = approval_events[1].safe_metadata
    assert isinstance(approval_metadata, ReviewApprovedMetadata)
    assert approval_metadata.review_digest == revised.review.content_digest
    assert approval_metadata.write_intent_count == 2
    deliveries = tuple(
        event.safe_metadata
        for event in approval_events
        if isinstance(event.safe_metadata, DeliveryTransitionMetadata)
    )
    assert len({delivery.write_intent_digest for delivery in deliveries}) == 2
    serialized = "\n".join(delivery.model_dump_json() for delivery in deliveries)
    assert all(str(intent.id) not in serialized for intent in approved.intents)

    replay_count = len(after_approval)
    replay = service.approve(
        meeting.id,
        expected_digest=revised.review.content_digest,
        expected_version=revised.meeting.version,
        request_key="approval-audit-one",
        actor_id="approver",
    )
    assert replay.replayed is True
    assert len(events(database, meeting.id)) == replay_count


@pytest.mark.asyncio
async def test_review_write_rolls_back_when_event_append_fails(tmp_path: Path) -> None:
    service, database = workflow(tmp_path)
    meeting = service.ingest(
        command(),
        io.BytesIO(b"RIFF\x00\x00\x00\x00WAVEaudit"),
    )
    reviewed = await process_meeting(service, database, meeting.id)
    with SqliteUnitOfWork(database, immediate=False) as uow:
        review = uow.reviews.latest_for_meeting(meeting.id)
        revision_count = len(uow.reviews.list_for_meeting(meeting.id))
    assert review is not None
    before = len(events(database, meeting.id))
    service._unit_of_work = lambda: RejectingEventUnitOfWork(database)

    with pytest.raises(RuntimeError, match="append rejected"):
        service.revise_action(
            meeting.id,
            edit=ActionEdit(
                action_id=review.action_items[0].id,
                title="Rolled back edit",
                owner="Dev",
                due_date=date(2026, 8, 16),
                due_time=None,
                timezone="UTC",
                notes=None,
            ),
            expected_digest=review.content_digest,
            expected_version=reviewed.version,
            actor_id="human-reviewer",
        )

    with SqliteUnitOfWork(database, immediate=False) as uow:
        persisted = uow.meetings.get(meeting.id)
        assert len(uow.reviews.list_for_meeting(meeting.id)) == revision_count
    assert persisted is not None
    assert persisted.version == reviewed.version
    assert len(events(database, meeting.id)) == before
