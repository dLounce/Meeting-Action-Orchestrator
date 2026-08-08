from datetime import datetime, timezone
from uuid import UUID

import pytest
from pydantic import ValidationError

from meeting_action_orchestrator.domain.enums import (
    ProcessingStage,
    ProviderBudgetDimension,
    ProviderCallRole,
    ProviderOperation,
    ProviderSettlementOutcome,
    ProviderUsageKind,
)
from meeting_action_orchestrator.domain.provider_budget import (
    ProviderBudgetAccount,
    ProviderBudgetLimits,
    ProviderBudgetReservation,
    ProviderBudgetReservationRequest,
    ProviderBudgetSettlement,
    ProviderBudgetUsage,
    ProviderUsage,
    provider_dispatch_digest,
    provider_reservation_fingerprint,
)

NOW = datetime(2026, 8, 8, 9, 0, tzinfo=timezone.utc)
JOB_ID = UUID("30000000-0000-4000-8000-000000000001")
CLAIM_TOKEN = UUID("40000000-0000-4000-8000-000000000001")
RESERVATION_ID = UUID("50000000-0000-4000-8000-000000000001")


def account(
    stage: ProcessingStage,
    limits: ProviderBudgetLimits,
    *,
    legacy_locked: bool = False,
) -> ProviderBudgetAccount:
    return ProviderBudgetAccount(
        processing_job_id=JOB_ID,
        stage=stage,
        limits=limits,
        legacy_locked=legacy_locked,
        created_at=NOW,
    )


def test_provider_budget_accounts_enforce_stage_specific_limit_shapes() -> None:
    zero_limits = ProviderBudgetLimits(
        preflight_request_limit=0,
        provider_request_limit=0,
        input_token_limit=0,
        output_token_limit=0,
        audio_duration_ms_limit=0,
    )
    assert account(ProcessingStage.TRANSCRIPTION, zero_limits, legacy_locked=True).legacy_locked
    with pytest.raises(ValidationError, match="zero limits"):
        account(
            ProcessingStage.TRANSCRIPTION,
            ProviderBudgetLimits(provider_request_limit=1),
            legacy_locked=True,
        )

    transcription = account(
        ProcessingStage.TRANSCRIPTION,
        ProviderBudgetLimits(provider_request_limit=3, audio_duration_ms_limit=60_000),
    )
    assert transcription.limits.audio_duration_ms_limit == 60_000
    with pytest.raises(ValidationError, match="token limits"):
        account(
            ProcessingStage.TRANSCRIPTION,
            ProviderBudgetLimits(
                provider_request_limit=3,
                audio_duration_ms_limit=60_000,
                input_token_limit=1,
            ),
        )
    with pytest.raises(ValidationError, match="request and audio limits"):
        account(
            ProcessingStage.TRANSCRIPTION,
            ProviderBudgetLimits(provider_request_limit=3),
        )

    responses = account(
        ProcessingStage.EXTRACTION,
        ProviderBudgetLimits(
            preflight_request_limit=6,
            provider_request_limit=6,
            input_token_limit=800_000,
            output_token_limit=24_000,
        ),
    )
    assert responses.limits.output_token_limit == 24_000
    with pytest.raises(ValidationError, match="cannot include audio"):
        account(
            ProcessingStage.EXTRACTION,
            ProviderBudgetLimits(
                preflight_request_limit=6,
                provider_request_limit=6,
                input_token_limit=800_000,
                output_token_limit=24_000,
                audio_duration_ms_limit=1,
            ),
        )
    with pytest.raises(ValidationError, match="limits are required"):
        account(
            ProcessingStage.EXTRACTION,
            ProviderBudgetLimits(provider_request_limit=6),
        )


def reservation_request(**updates: object) -> ProviderBudgetReservationRequest:
    values: dict[str, object] = {
        "dispatch_key": "responses-create",
        "operation_digest": "a" * 64,
        "operation": ProviderOperation.RESPONSES_CREATE,
        "role": ProviderCallRole.EXTRACT,
        "model": "gpt-test",
        "reserved_input_tokens": 100,
        "reserved_output_tokens": 20,
    }
    return ProviderBudgetReservationRequest.model_validate(values | updates)


def test_provider_reservation_requests_accept_each_supported_operation_shape() -> None:
    transcription = reservation_request(
        dispatch_key="transcription",
        operation=ProviderOperation.TRANSCRIPTION_CREATE,
        role=ProviderCallRole.TRANSCRIPTION,
        reserved_input_tokens=0,
        reserved_output_tokens=0,
        reserved_audio_duration_ms=30_000,
    )
    preflight = reservation_request(
        dispatch_key="preflight",
        operation=ProviderOperation.RESPONSES_PREFLIGHT,
        reserved_input_tokens=0,
        reserved_output_tokens=0,
    )
    generated = reservation_request()

    assert transcription.reserved_audio_duration_ms == 30_000
    assert preflight.operation is ProviderOperation.RESPONSES_PREFLIGHT
    assert generated.reserved_output_tokens == 20


@pytest.mark.parametrize(
    ("updates", "message"),
    [
        (
            {
                "operation": ProviderOperation.TRANSCRIPTION_CREATE,
                "role": ProviderCallRole.EXTRACT,
                "reserved_input_tokens": 0,
                "reserved_output_tokens": 0,
                "reserved_audio_duration_ms": 1,
            },
            "transcription role",
        ),
        (
            {
                "operation": ProviderOperation.TRANSCRIPTION_CREATE,
                "role": ProviderCallRole.TRANSCRIPTION,
                "reserved_input_tokens": 0,
                "reserved_output_tokens": 0,
                "reserved_audio_duration_ms": 0,
            },
            "audio duration",
        ),
        ({"role": ProviderCallRole.TRANSCRIPTION}, "specialist role"),
        ({"reserved_audio_duration_ms": 1}, "cannot reserve audio"),
        (
            {
                "operation": ProviderOperation.RESPONSES_PREFLIGHT,
                "reserved_input_tokens": 1,
                "reserved_output_tokens": 0,
            },
            "cannot reserve generation tokens",
        ),
        ({"reserved_input_tokens": 0}, "require token limits"),
    ],
)
def test_provider_reservation_requests_reject_cross_operation_resources(
    updates: dict[str, object],
    message: str,
) -> None:
    with pytest.raises(ValidationError, match=message):
        reservation_request(**updates)


def test_provider_reservation_fingerprint_binds_the_complete_request() -> None:
    dispatch_digest = provider_dispatch_digest("dispatch-key")
    fingerprint = provider_reservation_fingerprint(
        JOB_ID,
        1,
        CLAIM_TOKEN,
        dispatch_digest,
        "b" * 64,
        ProviderOperation.RESPONSES_CREATE,
        ProviderCallRole.RECAP,
        "gpt-test",
        100,
        20,
        0,
    )
    reservation = ProviderBudgetReservation(
        id=RESERVATION_ID,
        processing_job_id=JOB_ID,
        sequence=1,
        attempt_number=1,
        claim_token=CLAIM_TOKEN,
        dispatch_digest=dispatch_digest,
        operation_digest="b" * 64,
        request_fingerprint=fingerprint,
        operation=ProviderOperation.RESPONSES_CREATE,
        role=ProviderCallRole.RECAP,
        model="gpt-test",
        reserved_input_tokens=100,
        reserved_output_tokens=20,
        created_at=NOW,
    )

    assert reservation.request_fingerprint == fingerprint
    assert dispatch_digest != provider_dispatch_digest("another-key")
    with pytest.raises(ValidationError, match="fingerprint is invalid"):
        ProviderBudgetReservation.model_validate(
            reservation.model_dump(mode="python") | {"request_fingerprint": "0" * 64}
        )


@pytest.mark.parametrize(
    ("values", "message"),
    [
        ({"kind": ProviderUsageKind.NONE, "input_tokens": 0}, "cannot include counters"),
        ({"kind": ProviderUsageKind.TOKENS, "input_tokens": 1}, "token counters only"),
        (
            {
                "kind": ProviderUsageKind.TOKENS,
                "input_tokens": 1,
                "output_tokens": 2,
                "audio_duration_ms": 3,
            },
            "token counters only",
        ),
        ({"kind": ProviderUsageKind.DURATION}, "audio duration only"),
        (
            {
                "kind": ProviderUsageKind.DURATION,
                "audio_duration_ms": 3,
                "output_tokens": 2,
            },
            "audio duration only",
        ),
    ],
)
def test_provider_usage_rejects_mixed_counter_shapes(
    values: dict[str, object],
    message: str,
) -> None:
    with pytest.raises(ValidationError, match=message):
        ProviderUsage.model_validate(values)


def test_provider_usage_settlement_and_aggregate_dimensions_are_explicit() -> None:
    token_usage = ProviderUsage(kind=ProviderUsageKind.TOKENS, input_tokens=10, output_tokens=4)
    duration_usage = ProviderUsage(kind=ProviderUsageKind.DURATION, audio_duration_ms=2_000)
    succeeded = ProviderBudgetSettlement(
        reservation_id=RESERVATION_ID,
        outcome=ProviderSettlementOutcome.SUCCEEDED,
        usage=token_usage,
        settled_at=NOW,
    )
    failed = ProviderBudgetSettlement(
        reservation_id=RESERVATION_ID,
        outcome=ProviderSettlementOutcome.FAILED,
        settled_at=NOW,
    )

    assert succeeded.usage == token_usage
    assert duration_usage.audio_duration_ms == 2_000
    assert failed.usage.kind is ProviderUsageKind.NONE
    with pytest.raises(ValidationError, match="Only successful"):
        ProviderBudgetSettlement(
            reservation_id=RESERVATION_ID,
            outcome=ProviderSettlementOutcome.ABANDONED,
            usage=duration_usage,
            settled_at=NOW,
        )

    aggregate = ProviderBudgetUsage(
        processing_job_id=JOB_ID,
        preflight_requests=1,
        provider_requests=2,
        input_tokens=3,
        output_tokens=4,
        audio_duration_ms=5,
    )
    expected = {
        ProviderBudgetDimension.PREFLIGHT_REQUESTS: 1,
        ProviderBudgetDimension.PROVIDER_REQUESTS: 2,
        ProviderBudgetDimension.INPUT_TOKENS: 3,
        ProviderBudgetDimension.OUTPUT_TOKENS: 4,
        ProviderBudgetDimension.AUDIO_DURATION_MS: 5,
    }
    assert {dimension: aggregate.charged(dimension) for dimension in expected} == expected
