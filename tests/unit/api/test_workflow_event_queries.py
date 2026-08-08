from __future__ import annotations

from datetime import datetime, timezone
from typing import Literal, cast
from uuid import UUID

import pytest

from meeting_action_orchestrator.api.adapters import UnitOfWorkFactory, UnitOfWorkQueryFacade
from meeting_action_orchestrator.application.errors import (
    OperationConflictError,
    ResourceNotFoundError,
)
from meeting_action_orchestrator.application.ports import WorkflowEventCursor
from meeting_action_orchestrator.domain.enums import AudioMediaType
from meeting_action_orchestrator.domain.models import Meeting
from meeting_action_orchestrator.domain.workflow_events import (
    MeetingIngestedMetadata,
    WorkflowEvent,
    WorkflowEventType,
)

NOW = datetime(2026, 8, 7, 12, 0, tzinfo=timezone.utc)
MEETING_ID = UUID("10000000-0000-4000-8000-000000000001")
OTHER_MEETING_ID = UUID("10000000-0000-4000-8000-000000000002")


def meeting() -> Meeting:
    return Meeting(
        id=MEETING_ID,
        ingest_key="upload-one",
        title="Planning",
        audio_asset_id=UUID("30000000-0000-4000-8000-000000000001"),
        occurred_at=NOW,
        timezone="UTC",
        created_at=NOW,
        updated_at=NOW,
    )


def event(sequence: int, meeting_id: UUID = MEETING_ID) -> WorkflowEvent:
    return WorkflowEvent(
        id=UUID(f"20000000-0000-4000-8000-{sequence:012d}"),
        meeting_id=meeting_id,
        sequence=sequence,
        type=WorkflowEventType.MEETING_INGESTED,
        actor_id="portfolio-owner",
        safe_metadata=MeetingIngestedMetadata(
            recording_digest="a" * 64,
            media_type=AudioMediaType.WAV,
            size_bytes=1_024,
            duration_ms=60_000,
        ),
        occurred_at=NOW,
    )


class MeetingRepository:
    def __init__(self, unit_of_work: QueryUnitOfWork, value: Meeting | None) -> None:
        self.unit_of_work = unit_of_work
        self.value = value
        self.calls: list[UUID] = []

    def get(self, meeting_id: UUID) -> Meeting | None:
        assert self.unit_of_work.active
        self.calls.append(meeting_id)
        return self.value


class EventRepository:
    def __init__(self, unit_of_work: QueryUnitOfWork, values: tuple[WorkflowEvent, ...]) -> None:
        self.unit_of_work = unit_of_work
        self.values = values
        self.calls: list[tuple[UUID, WorkflowEventCursor | None, int]] = []

    def list_page(
        self,
        meeting_id: UUID,
        *,
        cursor: WorkflowEventCursor | None,
        limit: int,
    ) -> tuple[WorkflowEvent, ...]:
        assert self.unit_of_work.active
        self.calls.append((meeting_id, cursor, limit))
        minimum_sequence = cursor.sequence if cursor is not None else 0
        return tuple(item for item in self.values if item.sequence > minimum_sequence)[:limit]


class QueryUnitOfWork:
    def __init__(
        self,
        current_meeting: Meeting | None,
        values: tuple[WorkflowEvent, ...],
    ) -> None:
        self.active = False
        self.entries = 0
        self.exits = 0
        self.meetings = MeetingRepository(self, current_meeting)
        self.workflow_events = EventRepository(self, values)

    def __enter__(self) -> QueryUnitOfWork:
        assert not self.active
        self.active = True
        self.entries += 1
        return self

    def __exit__(self, *_args: object) -> Literal[False]:
        assert self.active
        self.active = False
        self.exits += 1
        return False


async def test_event_query_verifies_meeting_and_probes_inside_one_unit_of_work() -> None:
    events = (event(1), event(2), event(3))
    unit_of_work = QueryUnitOfWork(meeting(), events)
    facade = UnitOfWorkQueryFacade(cast(UnitOfWorkFactory, lambda: unit_of_work))

    result = await facade.list_workflow_events(MEETING_ID, cursor=None, limit=2)

    anchor = WorkflowEventCursor(meeting_id=MEETING_ID, sequence=2)
    assert result.items == events[:2]
    assert result.next_cursor == anchor
    assert unit_of_work.entries == unit_of_work.exits == 1
    assert unit_of_work.meetings.calls == [MEETING_ID]
    assert unit_of_work.workflow_events.calls == [
        (MEETING_ID, None, 2),
        (MEETING_ID, anchor, 1),
    ]


async def test_event_query_omits_cursor_without_a_following_row() -> None:
    events = (event(1), event(2))
    unit_of_work = QueryUnitOfWork(meeting(), events)
    facade = UnitOfWorkQueryFacade(cast(UnitOfWorkFactory, lambda: unit_of_work))

    result = await facade.list_workflow_events(MEETING_ID, cursor=None, limit=2)

    assert result.items == events
    assert result.next_cursor is None
    assert unit_of_work.workflow_events.calls[-1][2] == 1


async def test_event_query_returns_generic_missing_meeting_before_reading_events() -> None:
    unit_of_work = QueryUnitOfWork(None, (event(1),))
    facade = UnitOfWorkQueryFacade(cast(UnitOfWorkFactory, lambda: unit_of_work))

    with pytest.raises(ResourceNotFoundError):
        await facade.list_workflow_events(MEETING_ID, cursor=None, limit=20)

    assert unit_of_work.workflow_events.calls == []
    assert unit_of_work.entries == unit_of_work.exits == 1


async def test_event_query_rejects_cross_meeting_records_and_inputs() -> None:
    wrong_event_unit = QueryUnitOfWork(meeting(), (event(1, OTHER_MEETING_ID),))
    wrong_event = UnitOfWorkQueryFacade(cast(UnitOfWorkFactory, lambda: wrong_event_unit))
    cursor_unit = QueryUnitOfWork(meeting(), ())
    cross_cursor = UnitOfWorkQueryFacade(cast(UnitOfWorkFactory, lambda: cursor_unit))

    with pytest.raises(OperationConflictError, match="workflow event does not belong"):
        await wrong_event.list_workflow_events(MEETING_ID, cursor=None, limit=20)
    with pytest.raises(ValueError, match="another meeting"):
        await cross_cursor.list_workflow_events(
            MEETING_ID,
            cursor=WorkflowEventCursor(meeting_id=OTHER_MEETING_ID, sequence=1),
            limit=20,
        )

    assert cursor_unit.entries == 0


@pytest.mark.parametrize("limit", [True, 0, 101])
async def test_event_query_rejects_invalid_limits_before_opening_a_unit_of_work(
    limit: int,
) -> None:
    unit_of_work = QueryUnitOfWork(meeting(), ())
    facade = UnitOfWorkQueryFacade(cast(UnitOfWorkFactory, lambda: unit_of_work))

    with pytest.raises(ValueError, match="between one and 100"):
        await facade.list_workflow_events(MEETING_ID, cursor=None, limit=limit)

    assert unit_of_work.entries == 0
