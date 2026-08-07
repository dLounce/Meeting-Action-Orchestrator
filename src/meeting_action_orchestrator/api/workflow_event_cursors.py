from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import re
from collections.abc import Sequence
from uuid import UUID

from meeting_action_orchestrator.api.problems import ProblemError, create_problem
from meeting_action_orchestrator.application.ports import WorkflowEventCursor
from meeting_action_orchestrator.domain.workflow_events import WORKFLOW_SEQUENCE_MAX

WORKFLOW_EVENT_CURSOR_PATTERN = re.compile(r"[A-Za-z0-9_-]{1,128}")
WORKFLOW_EVENT_CURSOR_VERSION = 1
WORKFLOW_EVENT_CURSOR_UUID_BYTES = 16
WORKFLOW_EVENT_CURSOR_SEQUENCE_BYTES = 8
WORKFLOW_EVENT_CURSOR_CHECKSUM_BYTES = 16
WORKFLOW_EVENT_CURSOR_CHECKSUM_CONTEXT = b"meeting-action-orchestrator:workflow-event-cursor:v1:"
WORKFLOW_EVENT_CURSOR_BODY_BYTES = (
    1 + WORKFLOW_EVENT_CURSOR_UUID_BYTES + WORKFLOW_EVENT_CURSOR_SEQUENCE_BYTES
)
WORKFLOW_EVENT_CURSOR_BYTES = (
    WORKFLOW_EVENT_CURSOR_BODY_BYTES + WORKFLOW_EVENT_CURSOR_CHECKSUM_BYTES
)


def parse_workflow_event_cursor(
    value: str | None,
    meeting_id: UUID,
) -> WorkflowEventCursor | None:
    if value is None:
        return None
    if type(value) is not str or WORKFLOW_EVENT_CURSOR_PATTERN.fullmatch(value) is None:
        raise _invalid_workflow_event_cursor()
    decoded: bytes | None
    try:
        decoded = base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))
    except (binascii.Error, ValueError):
        decoded = None
    if decoded is None or _encode_cursor_bytes(decoded) != value:
        raise _invalid_workflow_event_cursor()
    if len(decoded) != WORKFLOW_EVENT_CURSOR_BYTES:
        raise _invalid_workflow_event_cursor()
    body = decoded[:WORKFLOW_EVENT_CURSOR_BODY_BYTES]
    checksum = decoded[WORKFLOW_EVENT_CURSOR_BODY_BYTES:]
    if body[0] != WORKFLOW_EVENT_CURSOR_VERSION:
        raise _invalid_workflow_event_cursor()
    if not hmac.compare_digest(checksum, _workflow_event_cursor_checksum(body)):
        raise _invalid_workflow_event_cursor()
    cursor_meeting_id = UUID(bytes=body[1 : 1 + WORKFLOW_EVENT_CURSOR_UUID_BYTES])
    sequence = int.from_bytes(body[-WORKFLOW_EVENT_CURSOR_SEQUENCE_BYTES:], "big")
    if cursor_meeting_id != meeting_id or not 1 <= sequence <= WORKFLOW_SEQUENCE_MAX:
        raise _invalid_workflow_event_cursor()
    return WorkflowEventCursor(meeting_id=cursor_meeting_id, sequence=sequence)


def parse_workflow_event_cursor_values(
    values: Sequence[str],
    meeting_id: UUID,
) -> WorkflowEventCursor | None:
    if len(values) > 1:
        raise _invalid_workflow_event_cursor()
    return parse_workflow_event_cursor(values[0] if values else None, meeting_id)


def format_workflow_event_cursor(cursor: WorkflowEventCursor | None) -> str | None:
    if cursor is None:
        return None
    if not isinstance(cursor, WorkflowEventCursor):
        raise ValueError("workflow event cursor has an invalid type")
    body = b"".join(
        (
            bytes((WORKFLOW_EVENT_CURSOR_VERSION,)),
            cursor.meeting_id.bytes,
            cursor.sequence.to_bytes(WORKFLOW_EVENT_CURSOR_SEQUENCE_BYTES, "big"),
        )
    )
    return _encode_cursor_bytes(body + _workflow_event_cursor_checksum(body))


def _workflow_event_cursor_checksum(body: bytes) -> bytes:
    return hashlib.sha256(WORKFLOW_EVENT_CURSOR_CHECKSUM_CONTEXT + body).digest()[
        :WORKFLOW_EVENT_CURSOR_CHECKSUM_BYTES
    ]


def _encode_cursor_bytes(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _invalid_workflow_event_cursor() -> ProblemError:
    return ProblemError(
        create_problem(
            400,
            detail="The workflow event page cursor is invalid for this query.",
            type_uri="urn:meeting-action-orchestrator:problem:invalid-page-cursor",
        )
    )
