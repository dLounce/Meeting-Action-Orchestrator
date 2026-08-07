from __future__ import annotations

import json
import logging

from meeting_action_orchestrator.observability import REDACTED, JsonFormatter, sanitize


def test_sanitize_redacts_nested_sensitive_values() -> None:
    value = {
        "job_id": "job-1",
        "request_id": "request-1",
        "meeting_id": "meeting-1",
        "actor_id": "actor-1",
        "email": "person@example.com",
        "provider_request_id": "provider-1",
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
