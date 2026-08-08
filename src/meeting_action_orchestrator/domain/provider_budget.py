from __future__ import annotations

import hashlib
from types import MappingProxyType
from typing import Annotated
from uuid import UUID

from pydantic import AwareDatetime, Field, StringConstraints, model_validator

from meeting_action_orchestrator.domain.base import DomainModel, Sha256Digest
from meeting_action_orchestrator.domain.enums import (
    ProcessingStage,
    ProviderBudgetDimension,
    ProviderCallRole,
    ProviderOperation,
    ProviderSettlementOutcome,
    ProviderUsageKind,
)
from meeting_action_orchestrator.domain.hashing import canonical_sha256

DispatchKey = Annotated[
    str,
    StringConstraints(min_length=1, max_length=200, strip_whitespace=True),
]
ProviderModelName = Annotated[
    str,
    StringConstraints(min_length=1, max_length=200, strip_whitespace=True),
]
PROVIDER_REQUEST_LIMIT_MAX = 1_000
PROVIDER_BUDGET_COUNTER_MAX = 1_000_000_000_000
PROVIDER_AGGREGATE_COUNTER_MAX = 1_000_000_000_000_000
PROVIDER_RESERVATION_SEQUENCE_MAX = 10_000


class ProviderBudgetLimits(DomainModel):
    preflight_request_limit: int | None = Field(default=None, ge=0, le=PROVIDER_REQUEST_LIMIT_MAX)
    provider_request_limit: int | None = Field(default=None, ge=0, le=PROVIDER_REQUEST_LIMIT_MAX)
    input_token_limit: int | None = Field(default=None, ge=0, le=PROVIDER_BUDGET_COUNTER_MAX)
    output_token_limit: int | None = Field(default=None, ge=0, le=PROVIDER_BUDGET_COUNTER_MAX)
    audio_duration_ms_limit: int | None = Field(default=None, ge=0, le=PROVIDER_BUDGET_COUNTER_MAX)


DEFAULT_PROVIDER_BUDGET_LIMITS = MappingProxyType(
    {
        ProcessingStage.TRANSCRIPTION: ProviderBudgetLimits(
            provider_request_limit=3,
            audio_duration_ms_limit=21_600_000,
        ),
        ProcessingStage.EXTRACTION: ProviderBudgetLimits(
            preflight_request_limit=6,
            provider_request_limit=6,
            input_token_limit=800_000,
            output_token_limit=24_000,
        ),
    }
)


class ProviderBudgetAccount(DomainModel):
    processing_job_id: UUID
    stage: ProcessingStage
    policy_version: int = Field(default=1, gt=0, le=PROVIDER_REQUEST_LIMIT_MAX)
    limits: ProviderBudgetLimits
    legacy_locked: bool = False
    created_at: AwareDatetime

    @model_validator(mode="after")
    def validate_limits(self) -> ProviderBudgetAccount:
        values = tuple(self.limits.model_dump(mode="python").values())
        if self.legacy_locked:
            if any(value != 0 for value in values):
                raise ValueError("Legacy provider budget accounts must have zero limits")
            return self
        if self.stage is ProcessingStage.TRANSCRIPTION:
            expected_null = (
                self.limits.preflight_request_limit,
                self.limits.input_token_limit,
                self.limits.output_token_limit,
            )
            if any(value is not None for value in expected_null):
                raise ValueError("Transcription token limits must be non-preventative")
            if (
                self.limits.provider_request_limit is None
                or self.limits.audio_duration_ms_limit is None
            ):
                raise ValueError("Transcription request and audio limits are required")
            return self
        if self.limits.audio_duration_ms_limit is not None:
            raise ValueError("Responses provider budgets cannot include audio limits")
        required = (
            self.limits.preflight_request_limit,
            self.limits.provider_request_limit,
            self.limits.input_token_limit,
            self.limits.output_token_limit,
        )
        if any(value is None for value in required):
            raise ValueError("Responses provider budget limits are required")
        return self


class ProviderDispatchContext(DomainModel):
    processing_job_id: UUID
    attempt_number: int = Field(gt=0, le=PROVIDER_REQUEST_LIMIT_MAX)
    lease_owner: Annotated[str, StringConstraints(min_length=1, max_length=200)]
    claim_token: UUID


class ProviderBudgetReservationRequest(DomainModel):
    dispatch_key: DispatchKey
    operation_digest: Sha256Digest
    operation: ProviderOperation
    role: ProviderCallRole
    model: ProviderModelName
    reserved_input_tokens: int = Field(default=0, ge=0, le=PROVIDER_BUDGET_COUNTER_MAX)
    reserved_output_tokens: int = Field(default=0, ge=0, le=PROVIDER_BUDGET_COUNTER_MAX)
    reserved_audio_duration_ms: int = Field(default=0, ge=0, le=PROVIDER_BUDGET_COUNTER_MAX)

    @model_validator(mode="after")
    def validate_operation(self) -> ProviderBudgetReservationRequest:
        if self.operation is ProviderOperation.TRANSCRIPTION_CREATE:
            if self.role is not ProviderCallRole.TRANSCRIPTION:
                raise ValueError("Transcription reservations require the transcription role")
            if self.reserved_audio_duration_ms <= 0:
                raise ValueError("Transcription reservations require an audio duration")
            if self.reserved_input_tokens or self.reserved_output_tokens:
                raise ValueError("Transcription token usage cannot be reserved")
            return self
        if self.role is ProviderCallRole.TRANSCRIPTION:
            raise ValueError("Responses reservations require a specialist role")
        if self.reserved_audio_duration_ms != 0:
            raise ValueError("Responses reservations cannot reserve audio duration")
        if self.operation is ProviderOperation.RESPONSES_PREFLIGHT:
            if self.reserved_input_tokens or self.reserved_output_tokens:
                raise ValueError("Input-count reservations cannot reserve generation tokens")
            return self
        if self.reserved_input_tokens <= 0 or self.reserved_output_tokens <= 0:
            raise ValueError("Responses create reservations require token limits")
        return self


def provider_dispatch_digest(dispatch_key: str) -> str:
    return hashlib.sha256(dispatch_key.encode("utf-8")).hexdigest()


class ProviderBudgetReservation(DomainModel):
    id: UUID
    processing_job_id: UUID
    sequence: int = Field(gt=0, le=PROVIDER_RESERVATION_SEQUENCE_MAX)
    attempt_number: int = Field(gt=0, le=PROVIDER_REQUEST_LIMIT_MAX)
    claim_token: UUID
    dispatch_digest: Sha256Digest
    operation_digest: Sha256Digest
    request_fingerprint: Sha256Digest
    operation: ProviderOperation
    role: ProviderCallRole
    model: ProviderModelName
    reserved_input_tokens: int = Field(default=0, ge=0, le=PROVIDER_BUDGET_COUNTER_MAX)
    reserved_output_tokens: int = Field(default=0, ge=0, le=PROVIDER_BUDGET_COUNTER_MAX)
    reserved_audio_duration_ms: int = Field(default=0, ge=0, le=PROVIDER_BUDGET_COUNTER_MAX)
    created_at: AwareDatetime

    @model_validator(mode="after")
    def validate_fingerprint(self) -> ProviderBudgetReservation:
        expected = provider_reservation_fingerprint(
            self.processing_job_id,
            self.attempt_number,
            self.claim_token,
            self.dispatch_digest,
            self.operation_digest,
            operation=self.operation,
            role=self.role,
            model=self.model,
            reserved_input_tokens=self.reserved_input_tokens,
            reserved_output_tokens=self.reserved_output_tokens,
            reserved_audio_duration_ms=self.reserved_audio_duration_ms,
        )
        if self.request_fingerprint != expected:
            raise ValueError("Provider reservation fingerprint is invalid")
        return self


def provider_reservation_fingerprint(  # noqa: PLR0917
    processing_job_id: UUID,
    attempt_number: int,
    claim_token: UUID,
    dispatch_digest: str,
    operation_digest: str,
    operation: ProviderOperation,
    role: ProviderCallRole,
    model: str,
    reserved_input_tokens: int,
    reserved_output_tokens: int,
    reserved_audio_duration_ms: int,
) -> str:
    return canonical_sha256(
        {
            "dispatch_digest": dispatch_digest,
            "operation_digest": operation_digest,
            "model": model,
            "operation": operation,
            "processing_job_id": processing_job_id,
            "attempt_number": attempt_number,
            "claim_token": claim_token,
            "reserved_audio_duration_ms": reserved_audio_duration_ms,
            "reserved_input_tokens": reserved_input_tokens,
            "reserved_output_tokens": reserved_output_tokens,
            "role": role,
        }
    )


class ProviderUsage(DomainModel):
    kind: ProviderUsageKind
    input_tokens: int | None = Field(default=None, ge=0, le=PROVIDER_BUDGET_COUNTER_MAX)
    output_tokens: int | None = Field(default=None, ge=0, le=PROVIDER_BUDGET_COUNTER_MAX)
    audio_duration_ms: int | None = Field(default=None, gt=0, le=PROVIDER_BUDGET_COUNTER_MAX)

    @model_validator(mode="after")
    def validate_shape(self) -> ProviderUsage:
        if self.kind is ProviderUsageKind.NONE:
            if any(
                value is not None
                for value in (
                    self.input_tokens,
                    self.output_tokens,
                    self.audio_duration_ms,
                )
            ):
                raise ValueError("Empty provider usage cannot include counters")
            return self
        if self.kind is ProviderUsageKind.TOKENS:
            if (
                self.input_tokens is None
                or self.output_tokens is None
                or self.audio_duration_ms is not None
            ):
                raise ValueError("Token provider usage requires token counters only")
            return self
        if (
            self.audio_duration_ms is None
            or self.input_tokens is not None
            or self.output_tokens is not None
        ):
            raise ValueError("Duration provider usage requires an audio duration only")
        return self


EMPTY_PROVIDER_USAGE = ProviderUsage(kind=ProviderUsageKind.NONE)


class ProviderBudgetSettlement(DomainModel):
    reservation_id: UUID
    outcome: ProviderSettlementOutcome
    usage: ProviderUsage = EMPTY_PROVIDER_USAGE
    settled_at: AwareDatetime

    @model_validator(mode="after")
    def validate_outcome(self) -> ProviderBudgetSettlement:
        if (
            self.outcome is not ProviderSettlementOutcome.SUCCEEDED
            and self.usage.kind is not ProviderUsageKind.NONE
        ):
            raise ValueError("Only successful provider calls can record usage")
        return self


class ProviderBudgetUsage(DomainModel):
    processing_job_id: UUID
    preflight_requests: int = Field(ge=0, le=PROVIDER_REQUEST_LIMIT_MAX)
    provider_requests: int = Field(ge=0, le=PROVIDER_REQUEST_LIMIT_MAX)
    input_tokens: int = Field(ge=0, le=PROVIDER_AGGREGATE_COUNTER_MAX)
    output_tokens: int = Field(ge=0, le=PROVIDER_AGGREGATE_COUNTER_MAX)
    audio_duration_ms: int = Field(ge=0, le=PROVIDER_AGGREGATE_COUNTER_MAX)

    def charged(self, dimension: ProviderBudgetDimension) -> int:
        return int(getattr(self, dimension.value))
