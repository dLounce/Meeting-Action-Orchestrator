from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence, Set
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Any
from uuid import UUID

from pydantic import BaseModel

from meeting_action_orchestrator.domain.errors import DomainValueCode, InvalidDomainValueError


def _jsonable(value: object) -> Any:
    if isinstance(value, BaseModel):
        result = _jsonable(value.model_dump(mode="python", by_alias=True, exclude_none=False))
    elif isinstance(value, Enum):
        result = _jsonable(value.value)
    elif isinstance(value, UUID):
        result = str(value)
    elif isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise InvalidDomainValueError(DomainValueCode.CANONICAL_DATETIME)
        normalized = value.astimezone(timezone.utc).isoformat(timespec="microseconds")
        result = normalized.replace("+00:00", "Z")
    elif isinstance(value, date):
        result = value.isoformat()
    elif isinstance(value, Decimal):
        result = str(value)
    elif isinstance(value, Mapping):
        result = {str(key): _jsonable(item) for key, item in value.items()}
    elif isinstance(value, Set):
        items = [_jsonable(item) for item in value]
        result = sorted(items, key=lambda item: json.dumps(item, sort_keys=True))
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        result = [_jsonable(item) for item in value]
    elif value is None or isinstance(value, (str, int, float, bool)):
        result = value
    else:
        raise InvalidDomainValueError(DomainValueCode.CANONICAL_TYPE, type(value).__name__)
    return result


def canonical_json(value: object) -> str:
    return json.dumps(
        _jsonable(value),
        ensure_ascii=True,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def canonical_sha256(value: object) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def text_sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()
