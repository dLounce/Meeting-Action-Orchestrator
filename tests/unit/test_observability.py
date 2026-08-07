from __future__ import annotations

import json
import logging

from meeting_action_orchestrator.observability import REDACTED, JsonFormatter, sanitize


def test_sanitize_redacts_nested_sensitive_values() -> None:
    value = {
        "job_id": "job-1",
        "authorization": "Bearer secret",
        "nested": {"transcript": "private meeting"},
    }

    result = sanitize(value)

    assert result == {
        "job_id": "job-1",
        "authorization": REDACTED,
        "nested": {"transcript": REDACTED},
    }


def test_json_formatter_emits_safe_structured_record() -> None:
    record = logging.LogRecord("test", logging.INFO, __file__, 1, "processed", (), None)
    record.fields = {"meeting_id": "meeting-1", "api_key": "secret"}

    result = json.loads(JsonFormatter().format(record))

    assert result["level"] == "info"
    assert result["message"] == "processed"
    assert result["fields"] == {"meeting_id": "meeting-1", "api_key": REDACTED}
