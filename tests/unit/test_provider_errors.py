from __future__ import annotations

from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
from types import SimpleNamespace

import httpx

from meeting_action_orchestrator.application.errors import ProviderError
from meeting_action_orchestrator.application.provider_policy import (
    MAX_PROVIDER_RETRY_AFTER_SECONDS,
    ProviderErrorMetadata,
    provider_error_metadata,
    provider_error_requires_action,
    sanitize_provider_identifier,
)


class FakeProviderFailureError(Exception):
    def __init__(
        self,
        *,
        status_code: object = None,
        code: object = None,
        body: object = None,
        request_id: object = None,
        response_id: object = None,
        retry_after: object = None,
        response: object = None,
    ) -> None:
        self.status_code = status_code
        self.code = code
        self.body = body
        self.request_id = request_id
        self._request_id: object = None
        self.response_id = response_id
        self.retry_after = retry_after
        self.response = response
        super().__init__("provider payload must remain private")


class UnreadableHeaders:
    def get(self, _name: str) -> object:
        raise RuntimeError("private header failure")


def test_provider_metadata_extracts_only_allowlisted_values() -> None:
    private = "sk-private-value"
    error = FakeProviderFailureError(
        status_code=429,
        code="rate_limit_exceeded",
        body={"message": private, "details": {"authorization": private}},
        request_id="req_transport_1",
        response_id="resp_object_1",
        response=SimpleNamespace(
            status_code=429,
            headers={
                "retry-after": "12.5",
                "authorization": private,
            },
        ),
    )

    metadata = provider_error_metadata(error)
    translated = ProviderError("Provider request failed", metadata=metadata)

    assert metadata == ProviderErrorMetadata(
        http_status=429,
        provider_code="rate_limit_exceeded",
        request_id="req_transport_1",
        response_id="resp_object_1",
        retry_after_seconds=12.5,
    )
    assert str(translated) == "Provider request failed"
    assert translated.request_id == "req_transport_1"
    assert translated.response_id == "resp_object_1"
    assert private not in repr(metadata)
    assert private not in str(translated)


def test_provider_metadata_rejects_conflicting_retry_after_units() -> None:
    error = FakeProviderFailureError(
        response=SimpleNamespace(
            status_code=503,
            headers={"retry-after-ms": "2500", "retry-after": "10"},
        )
    )

    metadata = provider_error_metadata(error)

    assert metadata.http_status == 503
    assert metadata.retry_after_seconds is None
    assert metadata.retry_after_exceeds_limit is False
    assert metadata.retry_control_rejected is True


def test_provider_metadata_parses_retry_after_milliseconds() -> None:
    metadata = provider_error_metadata(
        FakeProviderFailureError(
            response=SimpleNamespace(
                status_code=503,
                headers={"retry-after-ms": "2500"},
            )
        )
    )

    assert metadata.retry_after_seconds == 2.5
    assert metadata.retry_control_rejected is False


def test_provider_metadata_accepts_direct_and_http_date_retry_hints() -> None:
    direct = provider_error_metadata(FakeProviderFailureError(retry_after=7))
    retry_at = datetime.now(timezone.utc) + timedelta(seconds=30)
    http_date = provider_error_metadata(
        FakeProviderFailureError(
            response=SimpleNamespace(
                status_code=429,
                headers={"retry-after": format_datetime(retry_at, usegmt=True)},
            )
        )
    )

    assert direct.retry_after_seconds == 7
    assert http_date.retry_after_seconds is not None
    assert 20 <= http_date.retry_after_seconds <= 30


def test_provider_metadata_uses_fallback_identifiers_and_nested_codes() -> None:
    error = FakeProviderFailureError(
        status_code=True,
        code="unsafe code",
        body={"error": {"code": "server_error"}},
        request_id="unsafe request",
        response_id=1,
        response=SimpleNamespace(
            status_code=500,
            headers={"x-request-id": "req_header_1"},
        ),
    )
    error._request_id = "req_private_1"

    metadata = provider_error_metadata(error)

    assert metadata.http_status == 500
    assert metadata.provider_code == "server_error"
    assert metadata.request_id == "req_private_1"
    assert metadata.response_id is None


def test_provider_metadata_rejects_invalid_retry_hints_and_unreadable_headers() -> None:
    invalid = provider_error_metadata(
        FakeProviderFailureError(
            retry_after=True,
            response=SimpleNamespace(
                status_code=429,
                headers={"retry-after-ms": "-1", "retry-after": "not-a-date"},
            ),
        )
    )
    unreadable = provider_error_metadata(
        FakeProviderFailureError(
            response=SimpleNamespace(status_code=429, headers=UnreadableHeaders())
        )
    )

    assert invalid.retry_after_seconds is None
    assert invalid.retry_after_exceeds_limit is False
    assert invalid.retry_control_rejected is True
    assert unreadable.request_id is None
    assert unreadable.retry_after_seconds is None
    assert unreadable.retry_control_rejected is True


def test_provider_metadata_rejects_oversized_retry_after_without_clamping() -> None:
    error = FakeProviderFailureError(
        response=SimpleNamespace(
            status_code=429,
            headers={"retry-after": str(MAX_PROVIDER_RETRY_AFTER_SECONDS + 1)},
        )
    )

    metadata = provider_error_metadata(error)

    assert metadata.retry_after_seconds is None
    assert metadata.retry_after_exceeds_limit is True
    assert metadata.retry_control_rejected is True


def test_manually_constructed_provider_metadata_is_sanitized() -> None:
    metadata = ProviderErrorMetadata(
        http_status=99,
        provider_code="invalid code with spaces",
        request_id="req_safe\nsecret",
        response_id="https://private.example/response",
        retry_after_seconds=MAX_PROVIDER_RETRY_AFTER_SECONDS + 1,
    )

    assert metadata.http_status is None
    assert metadata.provider_code is None
    assert metadata.request_id is None
    assert metadata.response_id is None
    assert metadata.retry_after_seconds is None
    assert metadata.retry_after_exceeds_limit is True
    assert metadata.retry_control_rejected is True

    explicit = ProviderErrorMetadata(retry_after_exceeds_limit=True)

    assert explicit.retry_after_exceeds_limit is True
    assert explicit.retry_control_rejected is True
    error = ProviderError(metadata=explicit)
    assert error.retry_after_exceeds_limit is True
    assert error.retry_control_rejected is True


def test_quota_billing_and_action_required_codes_are_permanent() -> None:
    for body in (
        {"code": "insufficient_quota"},
        {"type": "billing_hard_limit_reached"},
        {"error": {"code": "account_verification_required"}},
    ):
        assert provider_error_requires_action(FakeProviderFailureError(body=body))

    assert not provider_error_requires_action(
        FakeProviderFailureError(body={"code": "rate_limit_exceeded"})
    )


def test_provider_identifier_sanitizer_rejects_content_bearing_values() -> None:
    assert sanitize_provider_identifier(" req_123 ") == "req_123"
    assert sanitize_provider_identifier(123) is None
    assert sanitize_provider_identifier("") is None
    assert sanitize_provider_identifier("https://private.example") is None
    assert sanitize_provider_identifier("line-one\nline-two") is None
    assert sanitize_provider_identifier("x" * 201) is None


def test_provider_metadata_honors_case_insensitive_retry_directive() -> None:
    retry = provider_error_metadata(
        FakeProviderFailureError(
            response=SimpleNamespace(headers=httpx.Headers({"X-Should-Retry": "TRUE"}))
        )
    )
    no_retry = provider_error_metadata(
        FakeProviderFailureError(
            response=SimpleNamespace(headers=httpx.Headers({"x-SHOULD-retry": "false"}))
        )
    )

    assert retry.provider_should_retry is True
    assert retry.retry_control_rejected is False
    assert no_retry.provider_should_retry is False
    assert no_retry.retry_control_rejected is False


def test_provider_metadata_rejects_duplicate_and_conflicting_retry_headers() -> None:
    duplicate = provider_error_metadata(
        FakeProviderFailureError(
            response=SimpleNamespace(
                headers=httpx.Headers([("retry-after", "5"), ("Retry-After", "5")])
            )
        )
    )
    conflicting = provider_error_metadata(
        FakeProviderFailureError(
            response=SimpleNamespace(
                headers=httpx.Headers([("retry-after-ms", "1000"), ("retry-after", "2")])
            )
        )
    )
    directive = provider_error_metadata(
        FakeProviderFailureError(
            response=SimpleNamespace(
                headers=httpx.Headers([("x-should-retry", "true"), ("X-Should-Retry", "false")])
            )
        )
    )

    assert duplicate.retry_control_rejected is True
    assert conflicting.retry_control_rejected is True
    assert directive.provider_should_retry is None
    assert directive.retry_control_rejected is True


def test_provider_metadata_rejects_nonfinite_retry_hints() -> None:
    positive_infinity = provider_error_metadata(
        FakeProviderFailureError(
            response=SimpleNamespace(headers=httpx.Headers({"retry-after": "1e309"}))
        )
    )
    not_a_number = provider_error_metadata(
        FakeProviderFailureError(
            response=SimpleNamespace(headers=httpx.Headers({"retry-after": "NaN"}))
        )
    )

    assert positive_infinity.retry_after_exceeds_limit is True
    assert positive_infinity.retry_control_rejected is True
    assert not_a_number.retry_after_exceeds_limit is False
    assert not_a_number.retry_control_rejected is True
