from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import UUID

import pytest

from meeting_action_orchestrator.api.dependencies import (
    format_etag,
    format_meeting_cursor,
    parse_idempotency_key,
    parse_meeting_cursor,
    parse_meeting_precondition,
    parse_review_precondition,
)
from meeting_action_orchestrator.api.problems import ProblemError
from meeting_action_orchestrator.application.ports import MeetingListCursor
from meeting_action_orchestrator.domain.enums import MeetingStatus

DIGEST = "a" * 64


def test_review_precondition_accepts_one_strong_digest_etag() -> None:
    assert parse_review_precondition(f'"{DIGEST}"') == DIGEST
    assert format_etag(DIGEST) == f'"{DIGEST}"'


def test_meeting_precondition_accepts_one_strong_version_etag() -> None:
    assert parse_meeting_precondition('"meeting-0"') == 0
    assert parse_meeting_precondition('"meeting-42"') == 42


@pytest.mark.parametrize(
    "value",
    [
        None,
        "meeting-1",
        'W/"meeting-1"',
        "*",
        '"meeting-01"',
        '"meeting-1", "meeting-2"',
        '"meeting-9999999999999999999"',
    ],
)
def test_meeting_precondition_rejects_missing_or_ambiguous_values(value: str | None) -> None:
    with pytest.raises(ProblemError):
        parse_meeting_precondition(value)


@pytest.mark.parametrize(
    "value",
    [None, DIGEST, f'W/"{DIGEST}"', "*", f'"{DIGEST}", "{DIGEST}"'],
)
def test_review_precondition_rejects_missing_or_ambiguous_values(value: str | None) -> None:
    with pytest.raises(ProblemError):
        parse_review_precondition(value)


@pytest.mark.parametrize("value", [None, "", "with space", "a" * 201, "line\nbreak"])
def test_idempotency_key_rejects_unsafe_values(value: str | None) -> None:
    with pytest.raises(ProblemError):
        parse_idempotency_key(value)


def test_idempotency_key_accepts_visible_portable_characters() -> None:
    assert parse_idempotency_key("upload:2026-08-07/one") == "upload:2026-08-07/one"


def test_meeting_cursor_round_trips_an_exact_status_bound_anchor() -> None:
    anchor = MeetingListCursor(
        created_at=datetime(
            2026,
            8,
            7,
            15,
            30,
            0,
            123456,
            tzinfo=timezone(timedelta(hours=5, minutes=30)),
        ),
        id=UUID("10000000-0000-4000-8000-000000000001"),
    )

    encoded = format_meeting_cursor(anchor, MeetingStatus.INGESTED)

    assert encoded is not None
    assert parse_meeting_cursor(encoded, MeetingStatus.INGESTED) == anchor


def test_meeting_cursor_rejects_tampering_and_filter_changes() -> None:
    anchor = MeetingListCursor(
        created_at=datetime(2026, 8, 7, 10, 0, tzinfo=timezone.utc),
        id=UUID("10000000-0000-4000-8000-000000000001"),
    )
    encoded = format_meeting_cursor(anchor, MeetingStatus.INGESTED)
    assert encoded is not None
    replacement = "A" if encoded[-1] != "A" else "B"

    with pytest.raises(ProblemError):
        parse_meeting_cursor(f"{encoded[:-1]}{replacement}", MeetingStatus.INGESTED)
    with pytest.raises(ProblemError):
        parse_meeting_cursor(encoded, MeetingStatus.COMPLETED)
    with pytest.raises(ProblemError):
        parse_meeting_cursor(f"{encoded}=", MeetingStatus.INGESTED)
