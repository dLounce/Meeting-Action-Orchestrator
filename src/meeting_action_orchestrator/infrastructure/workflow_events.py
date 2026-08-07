from __future__ import annotations

import sqlite3
from collections.abc import Sequence
from datetime import datetime, timezone
from uuid import UUID, uuid4

from meeting_action_orchestrator.application.ports import WorkflowEventCursor
from meeting_action_orchestrator.domain.hashing import canonical_json
from meeting_action_orchestrator.domain.workflow_events import (
    WORKFLOW_SEQUENCE_MAX,
    WorkflowEvent,
    WorkflowEventDraft,
    WorkflowEventType,
    parse_workflow_event_metadata,
)

WORKFLOW_EVENT_PAGE_LIMIT = 100


class WorkflowEventIntegrityError(RuntimeError):
    pass


class WorkflowEventWriteModeError(RuntimeError):
    pass


class SqliteWorkflowEventRepository:
    def __init__(self, connection: sqlite3.Connection, *, writable: bool) -> None:
        self._connection = connection
        self._writable = writable

    def append(self, draft: WorkflowEventDraft) -> WorkflowEvent:
        if not self._writable or not self._connection.in_transaction:
            raise WorkflowEventWriteModeError("Workflow events require an immediate unit of work")
        row = self._connection.execute(
            "SELECT COALESCE(MAX(sequence), 0) FROM workflow_events WHERE meeting_id = ?",
            (str(draft.meeting_id),),
        ).fetchone()
        if row is None or isinstance(row[0], bool) or not isinstance(row[0], int):
            raise WorkflowEventIntegrityError("Workflow event sequence state is invalid")
        if row[0] >= WORKFLOW_SEQUENCE_MAX:
            raise WorkflowEventIntegrityError("Workflow event sequence is exhausted")
        occurred_at = draft.occurred_at.astimezone(timezone.utc)
        event = WorkflowEvent(
            id=uuid4(),
            meeting_id=draft.meeting_id,
            sequence=row[0] + 1,
            type=draft.type,
            actor_id=draft.actor_id,
            safe_metadata=draft.safe_metadata,
            occurred_at=occurred_at,
        )
        self._connection.execute(
            """
            INSERT INTO workflow_events (
                id, meeting_id, sequence, type, actor_id, safe_metadata_json, occurred_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(event.id),
                str(event.meeting_id),
                event.sequence,
                event.type.value,
                event.actor_id,
                canonical_json(event.safe_metadata),
                _as_utc_text(event.occurred_at),
            ),
        )
        return event

    def list_page(
        self,
        meeting_id: UUID,
        *,
        cursor: WorkflowEventCursor | None,
        limit: int,
    ) -> Sequence[WorkflowEvent]:
        if (
            isinstance(limit, bool)
            or not isinstance(limit, int)
            or limit < 1
            or limit > WORKFLOW_EVENT_PAGE_LIMIT
        ):
            raise ValueError(
                f"Workflow event page limit must be between 1 and {WORKFLOW_EVENT_PAGE_LIMIT}"
            )
        if cursor is not None and cursor.meeting_id != meeting_id:
            raise ValueError("Workflow event cursor belongs to another meeting")
        if cursor is None:
            rows = self._connection.execute(
                """
                SELECT * FROM workflow_events
                WHERE meeting_id = ?
                ORDER BY sequence ASC LIMIT ?
                """,
                (str(meeting_id), limit),
            ).fetchall()
        else:
            rows = self._connection.execute(
                """
                SELECT * FROM workflow_events
                WHERE meeting_id = ? AND sequence > ?
                ORDER BY sequence ASC LIMIT ?
                """,
                (str(meeting_id), cursor.sequence, limit),
            ).fetchall()
        events = tuple(_event_from_row(row) for row in rows)
        expected_sequence = cursor.sequence + 1 if cursor is not None else 1
        if any(event.sequence != expected_sequence + offset for offset, event in enumerate(events)):
            raise WorkflowEventIntegrityError("Stored workflow event sequence is not contiguous")
        return events


def _as_utc_text(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Workflow event timestamps must include a UTC offset")
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds")


def _event_from_row(row: sqlite3.Row) -> WorkflowEvent:
    event: WorkflowEvent | None = None
    try:
        raw_id = row["id"]
        raw_meeting_id = row["meeting_id"]
        raw_sequence = row["sequence"]
        raw_type = row["type"]
        raw_actor_id = row["actor_id"]
        raw_occurred_at = row["occurred_at"]
        if (
            type(raw_id) is not str
            or type(raw_meeting_id) is not str
            or isinstance(raw_sequence, bool)
            or type(raw_sequence) is not int
            or type(raw_type) is not str
            or (raw_actor_id is not None and type(raw_actor_id) is not str)
            or type(raw_occurred_at) is not str
        ):
            raise ValueError("Workflow event scalar storage type is invalid")
        event_id = UUID(raw_id)
        meeting_id = UUID(raw_meeting_id)
        if str(event_id) != raw_id or str(meeting_id) != raw_meeting_id:
            raise ValueError("Workflow event UUID storage is not canonical")
        event_type = WorkflowEventType(raw_type)
        if event_type.value != raw_type:
            raise ValueError("Workflow event type storage is not canonical")
        occurred_at = datetime.fromisoformat(raw_occurred_at)
        if _as_utc_text(occurred_at) != raw_occurred_at:
            raise ValueError("Workflow event timestamp storage is not canonical")
        if raw_actor_id is not None and raw_actor_id.strip() != raw_actor_id:
            raise ValueError("Workflow event actor storage is not canonical")
        raw_metadata = row["safe_metadata_json"]
        if type(raw_metadata) is not str:
            raise ValueError("Workflow event metadata storage type is invalid")
        metadata = parse_workflow_event_metadata(event_type, raw_metadata)
        if raw_metadata != canonical_json(metadata):
            raise ValueError("Workflow event metadata is not canonical")
        event = WorkflowEvent(
            id=event_id,
            meeting_id=meeting_id,
            sequence=raw_sequence,
            type=event_type,
            actor_id=raw_actor_id,
            safe_metadata=metadata,
            occurred_at=occurred_at,
        )
        if event.actor_id != raw_actor_id:
            raise ValueError("Workflow event actor storage is not canonical")
    except Exception:
        event = None
    if event is None:
        raise WorkflowEventIntegrityError("Stored workflow event violates the audit contract")
    return event
