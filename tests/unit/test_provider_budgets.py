from operator import setitem
from uuid import UUID

import pytest
from pydantic import ValidationError

from meeting_action_orchestrator.application.ports import TranscriptionRunContext
from meeting_action_orchestrator.domain.enums import (
    ProviderCallRole,
    ProviderOperation,
    ProviderUsageKind,
)
from meeting_action_orchestrator.domain.provider_budget import (
    DEFAULT_PROVIDER_BUDGET_LIMITS,
    PROVIDER_BUDGET_COUNTER_MAX,
    ProviderBudgetReservationRequest,
    ProviderDispatchContext,
    ProviderUsage,
)

JOB_ID = UUID("30000000-0000-4000-8000-000000000001")


def dispatch() -> ProviderDispatchContext:
    return ProviderDispatchContext(
        processing_job_id=JOB_ID,
        attempt_number=1,
        lease_owner="worker",
        claim_token=UUID(int=1),
    )


def test_default_budget_mapping_is_immutable() -> None:
    with pytest.raises(TypeError):
        setitem(
            DEFAULT_PROVIDER_BUDGET_LIMITS,
            next(iter(DEFAULT_PROVIDER_BUDGET_LIMITS)),
            next(iter(DEFAULT_PROVIDER_BUDGET_LIMITS.values())),
        )


def test_transcription_context_requires_exact_positive_audio_identity() -> None:
    context = TranscriptionRunContext(
        dispatch=dispatch(),
        audio_duration_ms=1,
        audio_size_bytes=1,
        audio_sha256="a" * 64,
    )

    assert context.audio_duration_ms == 1
    with pytest.raises(ValueError, match="audio_size_bytes"):
        TranscriptionRunContext(
            dispatch=dispatch(),
            audio_duration_ms=1,
            audio_size_bytes=0,
            audio_sha256="a" * 64,
        )
    with pytest.raises(ValueError, match="lowercase SHA-256"):
        TranscriptionRunContext(
            dispatch=dispatch(),
            audio_duration_ms=1,
            audio_size_bytes=1,
            audio_sha256="A" * 64,
        )


def test_transcription_reservation_rejects_token_envelopes() -> None:
    with pytest.raises(ValidationError, match="cannot be reserved"):
        ProviderBudgetReservationRequest(
            dispatch_key="transcription",
            operation_digest="b" * 64,
            operation=ProviderOperation.TRANSCRIPTION_CREATE,
            role=ProviderCallRole.TRANSCRIPTION,
            model="transcribe-test",
            reserved_input_tokens=1,
            reserved_audio_duration_ms=1,
        )


def test_usage_and_reservation_counters_are_bounded() -> None:
    with pytest.raises(ValidationError):
        ProviderUsage(
            kind=ProviderUsageKind.TOKENS,
            input_tokens=PROVIDER_BUDGET_COUNTER_MAX + 1,
            output_tokens=0,
        )
    with pytest.raises(ValidationError):
        ProviderBudgetReservationRequest(
            dispatch_key="responses",
            operation_digest="c" * 64,
            operation=ProviderOperation.RESPONSES_CREATE,
            role=ProviderCallRole.EXTRACT,
            model="gpt-test",
            reserved_input_tokens=PROVIDER_BUDGET_COUNTER_MAX + 1,
            reserved_output_tokens=1,
        )


def test_dispatch_context_requires_positive_attempt() -> None:
    with pytest.raises(ValidationError):
        ProviderDispatchContext(
            processing_job_id=JOB_ID,
            attempt_number=0,
            lease_owner="worker",
            claim_token=UUID(int=1),
        )
