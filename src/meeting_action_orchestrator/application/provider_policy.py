from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from math import isfinite

MAX_PROVIDER_RETRY_AFTER_SECONDS = 600.0
_SAFE_PROVIDER_VALUE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,199}$")
_PERMANENT_PROVIDER_MARKERS = (
    "action_required",
    "account_deactivated",
    "billing",
    "hard_limit",
    "insufficient_quota",
    "organization_deactivated",
    "payment",
    "project_archived",
    "quota_exceeded",
    "verification_required",
)


@dataclass(frozen=True, slots=True)
class ProviderErrorMetadata:
    http_status: int | None = None
    provider_code: str | None = None
    request_id: str | None = None
    response_id: str | None = None
    retry_after_seconds: float | None = None
    retry_after_exceeds_limit: bool = False
    provider_should_retry: bool | None = None
    retry_control_rejected: bool = False

    def __post_init__(self) -> None:
        retry_after = _parse_retry_after_value(self.retry_after_seconds)
        directive = self.provider_should_retry
        directive_rejected = directive is not None and not isinstance(directive, bool)
        object.__setattr__(self, "http_status", _http_status_value(self.http_status))
        object.__setattr__(self, "provider_code", sanitize_provider_identifier(self.provider_code))
        object.__setattr__(self, "request_id", sanitize_provider_identifier(self.request_id))
        object.__setattr__(self, "response_id", sanitize_provider_identifier(self.response_id))
        object.__setattr__(self, "retry_after_seconds", retry_after.seconds)
        object.__setattr__(
            self,
            "retry_after_exceeds_limit",
            self.retry_after_exceeds_limit is True or retry_after.exceeds_limit,
        )
        object.__setattr__(
            self,
            "provider_should_retry",
            directive if isinstance(directive, bool) else None,
        )
        object.__setattr__(
            self,
            "retry_control_rejected",
            self.retry_control_rejected is True
            or self.retry_after_exceeds_limit is True
            or retry_after.rejected
            or directive_rejected,
        )


def provider_error_metadata(
    error: Exception,
    request_id_fallback: object = None,
) -> ProviderErrorMetadata:
    response = getattr(error, "response", None)
    headers = getattr(response, "headers", None)
    retry_after = _retry_after(error, headers)
    provider_should_retry, directive_rejected = _provider_retry_directive(headers)
    return ProviderErrorMetadata(
        http_status=_http_status(error, response),
        provider_code=_provider_code(error),
        request_id=_first_safe_value(
            getattr(error, "request_id", None),
            getattr(error, "_request_id", None),
            _header(headers, "x-request-id"),
            request_id_fallback,
        ),
        response_id=_first_safe_value(getattr(error, "response_id", None)),
        retry_after_seconds=retry_after.seconds,
        retry_after_exceeds_limit=retry_after.exceeds_limit,
        provider_should_retry=provider_should_retry,
        retry_control_rejected=retry_after.rejected or directive_rejected,
    )


def provider_error_requires_action(error: Exception) -> bool:
    candidates = (
        getattr(error, "code", None),
        getattr(error, "type", None),
        _mapping_value(getattr(error, "body", None), "code"),
        _mapping_value(getattr(error, "body", None), "type"),
        _nested_mapping_value(getattr(error, "body", None), "error", "code"),
        _nested_mapping_value(getattr(error, "body", None), "error", "type"),
    )
    for candidate in candidates:
        normalized = sanitize_provider_identifier(candidate)
        if normalized is None:
            continue
        lowered = normalized.lower()
        if any(marker in lowered for marker in _PERMANENT_PROVIDER_MARKERS):
            return True
    return False


def sanitize_provider_identifier(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip()
    if _SAFE_PROVIDER_VALUE.fullmatch(normalized) is None:
        return None
    return normalized


def _http_status(error: Exception, response: object) -> int | None:
    for value in (getattr(error, "status_code", None), getattr(response, "status_code", None)):
        status = _http_status_value(value)
        if status is not None:
            return status
    return None


def _http_status_value(value: object) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value if 100 <= value <= 599 else None


def _provider_code(error: Exception) -> str | None:
    body = getattr(error, "body", None)
    return _first_safe_value(
        getattr(error, "code", None),
        _mapping_value(body, "code"),
        _nested_mapping_value(body, "error", "code"),
    )


def _mapping_value(value: object, key: str) -> object:
    return value.get(key) if isinstance(value, Mapping) else None


def _nested_mapping_value(value: object, parent: str, key: str) -> object:
    nested = _mapping_value(value, parent)
    return _mapping_value(nested, key)


def _first_safe_value(*values: object) -> str | None:
    for value in values:
        normalized = sanitize_provider_identifier(value)
        if normalized is not None:
            return normalized
    return None


def _header(headers: object, name: str) -> object:
    values = _header_values(headers, name)
    return values[0] if values is not None and len(values) == 1 else None


def _header_values(headers: object, name: str) -> tuple[object, ...] | None:
    get_list = getattr(headers, "get_list", None)
    if callable(get_list):
        try:
            values = get_list(name)
        except Exception:
            return None
        if isinstance(values, (list, tuple)):
            return tuple(values)
        return None
    if isinstance(headers, Mapping):
        values: list[object] = []
        try:
            for key, value in headers.items():
                if isinstance(key, str) and key.casefold() == name.casefold():
                    if isinstance(value, (list, tuple)):
                        values.extend(value)
                    else:
                        values.append(value)
        except Exception:
            return None
        return tuple(values)
    getter = getattr(headers, "get", None)
    if not callable(getter):
        return ()
    try:
        value = getter(name)
    except Exception:
        return None
    return () if value is None else (value,)


@dataclass(frozen=True, slots=True)
class _RetryAfter:
    seconds: float | None = None
    exceeds_limit: bool = False
    rejected: bool = False


def _retry_after(error: Exception, headers: object) -> _RetryAfter:
    milliseconds = _header_values(headers, "retry-after-ms")
    seconds = _header_values(headers, "retry-after")
    if milliseconds is None or seconds is None:
        return _RetryAfter(rejected=True)
    if len(milliseconds) > 1 or len(seconds) > 1:
        return _RetryAfter(rejected=True)
    candidates: list[tuple[object, float, bool]] = []
    direct = getattr(error, "retry_after", None)
    if direct is not None:
        candidates.append((direct, 1.0, False))
    if milliseconds:
        candidates.append((milliseconds[0], 1_000.0, False))
    if seconds:
        candidates.append((seconds[0], 1.0, True))
    parsed = tuple(
        _parse_retry_after_value(value, divisor=divisor, allow_http_date=allow_http_date)
        for value, divisor, allow_http_date in candidates
    )
    if any(value.rejected for value in parsed):
        return _RetryAfter(
            exceeds_limit=any(value.exceeds_limit for value in parsed),
            rejected=True,
        )
    values = tuple(value.seconds for value in parsed if value.seconds is not None)
    if not values:
        return _RetryAfter()
    if any(abs(value - values[0]) > 1e-6 for value in values[1:]):
        return _RetryAfter(rejected=True)
    return _RetryAfter(seconds=values[0])


def _parse_retry_after_value(
    value: object,
    *,
    divisor: float = 1.0,
    allow_http_date: bool = False,
) -> _RetryAfter:
    if value is None:
        return _RetryAfter()
    seconds = _numeric_seconds(value, divisor)
    if seconds is None and allow_http_date and isinstance(value, str):
        seconds = _http_date_seconds(value)
    if seconds is None:
        return _RetryAfter(rejected=True)
    if not isfinite(seconds):
        return _RetryAfter(exceeds_limit=seconds > 0, rejected=True)
    if seconds < 0:
        return _RetryAfter(rejected=True)
    if seconds > MAX_PROVIDER_RETRY_AFTER_SECONDS:
        return _RetryAfter(exceeds_limit=True, rejected=True)
    return _RetryAfter(seconds=seconds)


def _numeric_seconds(value: object, divisor: float) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value) / divisor
    if not isinstance(value, str) or len(value) > 64:
        return None
    try:
        return float(value.strip()) / divisor
    except (OverflowError, ValueError):
        return None


def _http_date_seconds(value: str) -> float | None:
    if len(value) > 128:
        return None
    try:
        retry_at = parsedate_to_datetime(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if retry_at.tzinfo is None or retry_at.utcoffset() is None:
        return None
    return max(0.0, (retry_at - datetime.now(timezone.utc)).total_seconds())


def _provider_retry_directive(headers: object) -> tuple[bool | None, bool]:
    values = _header_values(headers, "x-should-retry")
    if values is None:
        return None, True
    if not values:
        return None, False
    if len(values) != 1 or not isinstance(values[0], str):
        return None, True
    normalized = values[0].strip().casefold()
    if normalized == "true":
        return True, False
    if normalized == "false":
        return False, False
    return None, True
