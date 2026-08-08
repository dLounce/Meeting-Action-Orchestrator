from __future__ import annotations

import base64
import hashlib
from typing import cast
from uuid import UUID

import pytest

from meeting_action_orchestrator.api.problems import ProblemError
from meeting_action_orchestrator.api.workflow_event_cursors import (
    WORKFLOW_EVENT_CURSOR_CHECKSUM_BYTES,
    WORKFLOW_EVENT_CURSOR_CHECKSUM_CONTEXT,
    format_workflow_event_cursor,
    parse_workflow_event_cursor,
    parse_workflow_event_cursor_values,
)
from meeting_action_orchestrator.application.ports import WorkflowEventCursor

MEETING_ID = UUID("10000000-0000-4000-8000-000000000001")
OTHER_MEETING_ID = UUID("10000000-0000-4000-8000-000000000002")


def encode_raw(body: bytes) -> str:
    checksum = hashlib.sha256(WORKFLOW_EVENT_CURSOR_CHECKSUM_CONTEXT + body).digest()[
        :WORKFLOW_EVENT_CURSOR_CHECKSUM_BYTES
    ]
    return base64.urlsafe_b64encode(body + checksum).rstrip(b"=").decode("ascii")


def test_workflow_event_cursor_round_trips_meeting_and_sequence() -> None:
    cursor = WorkflowEventCursor(meeting_id=MEETING_ID, sequence=42)

    encoded = format_workflow_event_cursor(cursor)

    assert encoded is not None
    assert "=" not in encoded
    assert parse_workflow_event_cursor(encoded, MEETING_ID) == cursor
    assert format_workflow_event_cursor(None) is None
    assert parse_workflow_event_cursor(None, MEETING_ID) is None


def test_workflow_event_cursor_rejects_tampering_and_noncanonical_encodings() -> None:
    encoded = format_workflow_event_cursor(WorkflowEventCursor(meeting_id=MEETING_ID, sequence=42))
    assert encoded is not None
    replacement = "A" if encoded[-1] != "A" else "B"

    for malformed in (f"{encoded[:-1]}{replacement}", f"{encoded}=", "", "x" * 129):
        with pytest.raises(ProblemError) as failure:
            parse_workflow_event_cursor(malformed, MEETING_ID)
        assert failure.value.problem.type_uri.endswith("invalid-page-cursor")
        if malformed:
            assert malformed not in str(failure.value)


def test_workflow_event_cursor_rejects_cross_meeting_and_duplicate_values() -> None:
    encoded = format_workflow_event_cursor(WorkflowEventCursor(meeting_id=MEETING_ID, sequence=42))
    assert encoded is not None

    with pytest.raises(ProblemError):
        parse_workflow_event_cursor(encoded, OTHER_MEETING_ID)
    with pytest.raises(ProblemError):
        parse_workflow_event_cursor_values((encoded, encoded), MEETING_ID)


def test_workflow_event_cursor_rejects_zero_overflow_bool_and_invalid_types() -> None:
    zero = encode_raw(bytes((1,)) + MEETING_ID.bytes + (0).to_bytes(8, "big"))
    overflow = encode_raw(bytes((1,)) + MEETING_ID.bytes + (2**63).to_bytes(8, "big"))

    for malformed in (zero, overflow, cast(str, True), cast(str, b"bytes")):
        with pytest.raises(ProblemError):
            parse_workflow_event_cursor(malformed, MEETING_ID)
    with pytest.raises(ValueError, match="invalid type"):
        format_workflow_event_cursor(cast(WorkflowEventCursor, True))
