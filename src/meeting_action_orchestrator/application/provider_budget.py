from __future__ import annotations

import asyncio
from collections.abc import Callable
from contextlib import suppress
from datetime import datetime
from typing import TypeVar, cast
from uuid import UUID, uuid4

from meeting_action_orchestrator.application.errors import (
    PersistenceIntegrityError,
    ProviderBudgetExhaustedError,
    ProviderBudgetIntegrityError,
    ProviderBudgetLeaseLostError,
)
from meeting_action_orchestrator.application.ports import Clock, UnitOfWork
from meeting_action_orchestrator.domain.enums import (
    ProcessingJobStatus,
    ProcessingStage,
    ProviderBudgetDimension,
    ProviderOperation,
    ProviderSettlementOutcome,
    ProviderUsageKind,
)
from meeting_action_orchestrator.domain.models import ProcessingJob
from meeting_action_orchestrator.domain.provider_budget import (
    ProviderBudgetAccount,
    ProviderBudgetReservation,
    ProviderBudgetReservationRequest,
    ProviderBudgetSettlement,
    ProviderBudgetUsage,
    ProviderDispatchContext,
    ProviderUsage,
    provider_dispatch_digest,
    provider_reservation_fingerprint,
)

UnitOfWorkFactory = Callable[[], UnitOfWork]
ReadT = TypeVar("ReadT")
_MISSING = object()


def _budget_read(call: Callable[[], ReadT]) -> ReadT:
    result: ReadT | object = _MISSING
    with suppress(PersistenceIntegrityError, TypeError, ValueError):
        result = call()
    if result is _MISSING:
        raise ProviderBudgetIntegrityError
    return cast(ReadT, result)


class ProviderBudgetService:
    def __init__(
        self,
        *,
        unit_of_work: UnitOfWorkFactory,
        clock: Clock,
        id_factory: Callable[[], UUID] = uuid4,
    ) -> None:
        self._unit_of_work = unit_of_work
        self._clock = clock
        self._id_factory = id_factory

    async def reserve(
        self,
        context: ProviderDispatchContext,
        request: ProviderBudgetReservationRequest,
    ) -> ProviderBudgetReservation:
        try:
            return await asyncio.to_thread(self._reserve, context, request)
        except (
            ProviderBudgetExhaustedError,
            ProviderBudgetIntegrityError,
            ProviderBudgetLeaseLostError,
        ):
            raise
        except Exception as error:
            try:
                recovered = await asyncio.to_thread(
                    self._reconcile_reservation,
                    context,
                    request,
                )
            except (ProviderBudgetIntegrityError, ProviderBudgetLeaseLostError):
                raise
            except Exception:
                raise error from None
            if recovered is not None:
                return recovered
            raise error

    def _reserve(
        self,
        context: ProviderDispatchContext,
        request: ProviderBudgetReservationRequest,
    ) -> ProviderBudgetReservation:
        dispatch_digest = provider_dispatch_digest(request.dispatch_key)
        with self._unit_of_work() as uow:
            now = self._now()
            job = self._require_active_job(
                _budget_read(lambda: uow.processing_jobs.get(context.processing_job_id)),
                context,
                now,
            )
            account = _budget_read(
                lambda: uow.provider_budget_accounts.get(context.processing_job_id)
            )
            if account is None or account.stage is not job.stage:
                raise ProviderBudgetIntegrityError
            self._validate_operation_stage(account, request)
            fingerprint = provider_reservation_fingerprint(
                context.processing_job_id,
                context.attempt_number,
                context.claim_token,
                dispatch_digest,
                request.operation_digest,
                operation=request.operation,
                role=request.role,
                model=request.model,
                reserved_input_tokens=request.reserved_input_tokens,
                reserved_output_tokens=request.reserved_output_tokens,
                reserved_audio_duration_ms=request.reserved_audio_duration_ms,
            )
            existing = _budget_read(
                lambda: uow.provider_budget_reservations.find_by_dispatch_digest(dispatch_digest)
            )
            if existing is not None:
                if (
                    existing.processing_job_id != context.processing_job_id
                    or existing.request_fingerprint != fingerprint
                ):
                    raise ProviderBudgetIntegrityError
                return existing
            usage = _budget_read(
                lambda: uow.provider_budget_reservations.usage_for_job(context.processing_job_id)
            )
            self._ensure_available(account, usage, request)
            reservation = ProviderBudgetReservation(
                id=self._id_factory(),
                processing_job_id=context.processing_job_id,
                sequence=uow.provider_budget_reservations.next_sequence(context.processing_job_id),
                attempt_number=context.attempt_number,
                claim_token=context.claim_token,
                dispatch_digest=dispatch_digest,
                operation_digest=request.operation_digest,
                request_fingerprint=fingerprint,
                operation=request.operation,
                role=request.role,
                model=request.model,
                reserved_input_tokens=request.reserved_input_tokens,
                reserved_output_tokens=request.reserved_output_tokens,
                reserved_audio_duration_ms=request.reserved_audio_duration_ms,
                created_at=now,
            )
            uow.provider_budget_reservations.add(reservation)
            uow.commit()
        return reservation

    def _reconcile_reservation(
        self,
        context: ProviderDispatchContext,
        request: ProviderBudgetReservationRequest,
    ) -> ProviderBudgetReservation | None:
        dispatch_digest = provider_dispatch_digest(request.dispatch_key)
        fingerprint = provider_reservation_fingerprint(
            context.processing_job_id,
            context.attempt_number,
            context.claim_token,
            dispatch_digest,
            request.operation_digest,
            operation=request.operation,
            role=request.role,
            model=request.model,
            reserved_input_tokens=request.reserved_input_tokens,
            reserved_output_tokens=request.reserved_output_tokens,
            reserved_audio_duration_ms=request.reserved_audio_duration_ms,
        )
        with self._unit_of_work() as uow:
            job = self._require_active_job(
                _budget_read(lambda: uow.processing_jobs.get(context.processing_job_id)),
                context,
                self._now(),
            )
            account = _budget_read(
                lambda: uow.provider_budget_accounts.get(context.processing_job_id)
            )
            if account is None or account.stage is not job.stage:
                raise ProviderBudgetIntegrityError
            self._validate_operation_stage(account, request)
            existing = _budget_read(
                lambda: uow.provider_budget_reservations.find_by_dispatch_digest(dispatch_digest)
            )
            if existing is None:
                return None
            if (
                existing.processing_job_id != context.processing_job_id
                or existing.request_fingerprint != fingerprint
            ):
                raise ProviderBudgetIntegrityError
            return existing

    async def settle(
        self,
        reservation_id: UUID,
        *,
        outcome: ProviderSettlementOutcome,
        usage: ProviderUsage,
    ) -> ProviderBudgetSettlement:
        try:
            return await asyncio.to_thread(
                self._settle,
                reservation_id,
                outcome=outcome,
                usage=usage,
            )
        except ProviderBudgetIntegrityError:
            raise
        except Exception as error:
            try:
                recovered = await asyncio.to_thread(
                    self._reconcile_settlement,
                    reservation_id,
                    outcome=outcome,
                    usage=usage,
                )
            except ProviderBudgetIntegrityError:
                raise
            except Exception:
                raise error from None
            if recovered is not None:
                return recovered
            raise error

    def _settle(
        self,
        reservation_id: UUID,
        *,
        outcome: ProviderSettlementOutcome,
        usage: ProviderUsage,
    ) -> ProviderBudgetSettlement:
        with self._unit_of_work() as uow:
            reservation = _budget_read(lambda: uow.provider_budget_reservations.get(reservation_id))
            if reservation is None:
                raise ProviderBudgetIntegrityError
            self._validate_settlement_usage(reservation.operation, outcome, usage)
            existing = _budget_read(lambda: uow.provider_budget_settlements.get(reservation_id))
            if existing is not None:
                if existing.outcome is not outcome or existing.usage != usage:
                    raise ProviderBudgetIntegrityError
                return existing
            settlement = ProviderBudgetSettlement(
                reservation_id=reservation_id,
                outcome=outcome,
                usage=usage,
                settled_at=self._now(),
            )
            uow.provider_budget_settlements.add(settlement)
            uow.commit()
        return settlement

    def _reconcile_settlement(
        self,
        reservation_id: UUID,
        *,
        outcome: ProviderSettlementOutcome,
        usage: ProviderUsage,
    ) -> ProviderBudgetSettlement | None:
        with self._unit_of_work() as uow:
            reservation = _budget_read(lambda: uow.provider_budget_reservations.get(reservation_id))
            if reservation is None:
                return None
            self._validate_settlement_usage(reservation.operation, outcome, usage)
            existing = _budget_read(lambda: uow.provider_budget_settlements.get(reservation_id))
            if existing is None:
                return None
            if existing.outcome is not outcome or existing.usage != usage:
                raise ProviderBudgetIntegrityError
            return existing

    @staticmethod
    def _require_active_job(
        job: ProcessingJob | None,
        context: ProviderDispatchContext,
        now: datetime,
    ) -> ProcessingJob:
        active = (
            job is not None
            and job.status is ProcessingJobStatus.RUNNING
            and job.attempt_count == context.attempt_number
            and job.lease_owner == context.lease_owner
            and job.claim_token == context.claim_token
            and job.lease_expires_at is not None
            and job.lease_expires_at > now
        )
        if not active:
            raise ProviderBudgetLeaseLostError
        if job is None:
            raise ProviderBudgetLeaseLostError
        return job

    @staticmethod
    def _validate_operation_stage(
        account: ProviderBudgetAccount,
        request: ProviderBudgetReservationRequest,
    ) -> None:
        transcription = request.operation is ProviderOperation.TRANSCRIPTION_CREATE
        if transcription != (account.stage is ProcessingStage.TRANSCRIPTION):
            raise ProviderBudgetIntegrityError

    @staticmethod
    def _ensure_available(
        account: ProviderBudgetAccount,
        usage: ProviderBudgetUsage,
        request: ProviderBudgetReservationRequest,
    ) -> None:
        if account.legacy_locked:
            raise ProviderBudgetExhaustedError(ProviderBudgetDimension.PROVIDER_REQUESTS)
        increments = {
            ProviderBudgetDimension.PREFLIGHT_REQUESTS: int(
                request.operation is ProviderOperation.RESPONSES_PREFLIGHT
            ),
            ProviderBudgetDimension.PROVIDER_REQUESTS: int(
                request.operation
                in {
                    ProviderOperation.RESPONSES_CREATE,
                    ProviderOperation.TRANSCRIPTION_CREATE,
                }
            ),
            ProviderBudgetDimension.INPUT_TOKENS: request.reserved_input_tokens,
            ProviderBudgetDimension.OUTPUT_TOKENS: request.reserved_output_tokens,
            ProviderBudgetDimension.AUDIO_DURATION_MS: (request.reserved_audio_duration_ms),
        }
        limits = {
            ProviderBudgetDimension.PREFLIGHT_REQUESTS: (account.limits.preflight_request_limit),
            ProviderBudgetDimension.PROVIDER_REQUESTS: (account.limits.provider_request_limit),
            ProviderBudgetDimension.INPUT_TOKENS: account.limits.input_token_limit,
            ProviderBudgetDimension.OUTPUT_TOKENS: account.limits.output_token_limit,
            ProviderBudgetDimension.AUDIO_DURATION_MS: (account.limits.audio_duration_ms_limit),
        }
        for dimension, increment in increments.items():
            limit = limits[dimension]
            if limit is not None and usage.charged(dimension) + increment > limit:
                raise ProviderBudgetExhaustedError(dimension)

    @staticmethod
    def _validate_settlement_usage(
        operation: ProviderOperation,
        outcome: ProviderSettlementOutcome,
        usage: ProviderUsage,
    ) -> None:
        if outcome is not ProviderSettlementOutcome.SUCCEEDED:
            if usage.kind is not ProviderUsageKind.NONE:
                raise ProviderBudgetIntegrityError
            return
        allowed = {
            ProviderOperation.RESPONSES_PREFLIGHT: {ProviderUsageKind.NONE},
            ProviderOperation.RESPONSES_CREATE: {ProviderUsageKind.TOKENS},
            ProviderOperation.TRANSCRIPTION_CREATE: {
                ProviderUsageKind.TOKENS,
                ProviderUsageKind.DURATION,
            },
        }
        if usage.kind not in allowed[operation]:
            raise ProviderBudgetIntegrityError

    def _now(self) -> datetime:
        value = self._clock.now()
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("Provider budget timestamps must be timezone-aware")
        return value
