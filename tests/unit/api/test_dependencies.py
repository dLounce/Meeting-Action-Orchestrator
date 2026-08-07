from __future__ import annotations

import pytest

from meeting_action_orchestrator.api.dependencies import (
    format_etag,
    parse_idempotency_key,
    parse_review_precondition,
)
from meeting_action_orchestrator.api.problems import ProblemError

DIGEST = "a" * 64


def test_review_precondition_accepts_one_strong_digest_etag() -> None:
    assert parse_review_precondition(f'"{DIGEST}"') == DIGEST
    assert format_etag(DIGEST) == f'"{DIGEST}"'


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
