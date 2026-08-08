from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone

from meeting_action_orchestrator.observability import (
    REDACTED,
    JsonFormatter,
    configure_logging,
    sanitize,
)


def test_sanitize_redacts_nested_sensitive_values() -> None:
    value = {
        "job_id": "job-1",
        "request_id": "request-1",
        "meeting_id": "meeting-1",
        "actor_id": "actor-1",
        "email": "person@example.com",
        "provider_request_id": "provider-1",
        "digest": "digest-1",
        "verifier_digest": "digest-2",
        "erasure_hmac_keys": "encoded-keyring",
        "request_fingerprint": "fingerprint-1",
        "sha256": "audio-hash",
        "authorization": "Bearer secret",
        "nested": {
            "transcript": "private meeting",
            "service_token": "secret",
        },
    }

    result = sanitize(value)

    assert result == {
        "job_id": "job-1",
        "request_id": "request-1",
        "meeting_id": REDACTED,
        "actor_id": REDACTED,
        "email": REDACTED,
        "provider_request_id": REDACTED,
        "digest": REDACTED,
        "verifier_digest": REDACTED,
        "erasure_hmac_keys": REDACTED,
        "request_fingerprint": REDACTED,
        "sha256": REDACTED,
        "authorization": REDACTED,
        "nested": {"transcript": REDACTED, "service_token": REDACTED},
    }


def test_json_formatter_emits_safe_structured_record() -> None:
    record = logging.LogRecord("test", logging.INFO, __file__, 1, "processed", (), None)
    record.fields = {"meeting_id": "meeting-1", "api_key": "secret"}

    result = json.loads(JsonFormatter().format(record))

    assert result["level"] == "info"
    assert result["message"] == "processed"
    assert result["fields"] == {"meeting_id": REDACTED, "api_key": REDACTED}


def test_json_formatter_does_not_interpolate_log_arguments() -> None:
    record = logging.LogRecord(
        "test",
        logging.INFO,
        __file__,
        1,
        "provider failed: %s",
        ("private transcript",),
        None,
    )

    result = json.loads(JsonFormatter().format(record))

    assert result["message"] == "provider failed: %s"
    assert "private transcript" not in json.dumps(result)


def test_sanitize_normalizes_structured_safe_values() -> None:
    occurred_at = datetime(2026, 8, 7, 15, 30, tzinfo=timezone(timedelta(hours=5, minutes=30)))

    result = sanitize(
        {
            "items": ("ready", 3, None),
            "occurred_at": occurred_at,
            "description": "x" * 501,
            "opaque": object(),
        }
    )

    assert result["items"] == ["ready", 3, None]
    assert result["occurred_at"] == "2026-08-07T10:00:00+00:00"
    assert result["description"] == f"{'x' * 497}..."
    assert result["opaque"].startswith("<object object at ")


def test_json_formatter_reports_exception_type_without_exception_text() -> None:
    exception = ValueError("private transcript")
    record = logging.LogRecord(
        "test",
        logging.ERROR,
        __file__,
        1,
        {"unsafe": "message"},
        (),
        (ValueError, exception, None),
    )

    result = json.loads(JsonFormatter().format(record))

    assert result["message"] == "dict"
    assert result["exception"] == "ValueError"
    assert "private transcript" not in json.dumps(result)


def test_configure_logging_replaces_root_handlers_and_normalizes_level() -> None:
    root = logging.getLogger()
    original_handlers = root.handlers[:]
    original_level = root.level
    stale_handler = logging.NullHandler()
    root.handlers[:] = [stale_handler]
    try:
        configure_logging("warning")

        assert len(root.handlers) == 1
        assert root.handlers[0] is not stale_handler
        assert isinstance(root.handlers[0].formatter, JsonFormatter)
        assert root.level == logging.WARNING
    finally:
        root.handlers[:] = original_handlers
        root.setLevel(original_level)
