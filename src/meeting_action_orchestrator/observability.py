from __future__ import annotations

import json
import logging
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from typing import Any

REDACTED = "[redacted]"
SENSITIVE_KEYS = frozenset(
    {
        "actor_id",
        "api_key",
        "authorization",
        "audio",
        "audio_asset_id",
        "content",
        "cookie",
        "credentials",
        "email",
        "external_id",
        "external_url",
        "filename",
        "mcp_auth_token",
        "meeting_id",
        "openai_api_key",
        "original_name",
        "password",
        "path",
        "prompt",
        "provider_request_id",
        "secret",
        "text",
        "token",
        "transcript",
        "url",
    }
)


def sanitize(value: Any, *, key: str | None = None) -> Any:
    normalized_key = key.lower().replace("-", "_") if key is not None else None
    if normalized_key is not None and (
        normalized_key in SENSITIVE_KEYS
        or normalized_key.endswith(("_key", "_password", "_secret", "_token"))
    ):
        return REDACTED
    if isinstance(value, Mapping):
        return {
            str(item_key): sanitize(item, key=str(item_key)) for item_key, item in value.items()
        }
    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        return [sanitize(item) for item in value]
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, str) and len(value) > 500:
        return f"{value[:497]}..."
    if isinstance(value, str | int | float | bool) or value is None:
        return value
    return str(value)


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname.lower(),
            "logger": record.name,
            "message": str(record.msg)
            if isinstance(record.msg, str)
            else type(record.msg).__name__,
        }
        fields = getattr(record, "fields", None)
        if isinstance(fields, Mapping):
            payload["fields"] = sanitize(fields)
        if record.exc_info is not None:
            payload["exception"] = record.exc_info[0].__name__
        return json.dumps(payload, ensure_ascii=True, separators=(",", ":"))


def configure_logging(level: str = "INFO") -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level.upper())
