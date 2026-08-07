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
