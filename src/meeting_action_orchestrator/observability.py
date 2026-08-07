from __future__ import annotations

import json
import logging
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from typing import Any

REDACTED = "[redacted]"
SENSITIVE_KEYS = frozenset(
    {
        "api_key",
        "authorization",
        "audio",
        "content",
        "cookie",
        "credentials",
        "mcp_auth_token",
        "openai_api_key",
        "password",
        "prompt",
        "secret",
        "text",
        "token",
        "transcript",
    }
)


def sanitize(value: Any, *, key: str | None = None) -> Any:
    if key is not None and key.lower() in SENSITIVE_KEYS:
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
            "message": record.getMessage(),
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
