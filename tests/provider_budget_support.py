from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import cast
from uuid import UUID, uuid4

from meeting_action_orchestrator.application.ports import TranscriptionRunContext
from meeting_action_orchestrator.domain.enums import ProviderSettlementOutcome
from meeting_action_orchestrator.domain.provider_budget import (
    ProviderBudgetReservation,
    ProviderBudgetReservationRequest,
    ProviderBudgetSettlement,
    ProviderDispatchContext,
    ProviderUsage,
)


@dataclass
class FakeBudgetController:
    reservations: list[tuple[ProviderDispatchContext, ProviderBudgetReservationRequest]] = field(
        default_factory=list
    )
    settlements: list[tuple[UUID, ProviderSettlementOutcome, ProviderUsage]] = field(
        default_factory=list
    )

    async def reserve(
        self,
        context: ProviderDispatchContext,
        request: ProviderBudgetReservationRequest,
    ) -> ProviderBudgetReservation:
        self.reservations.append((context, request))
        return cast(ProviderBudgetReservation, type("Reservation", (), {"id": uuid4()})())

    async def settle(
        self,
        reservation_id: UUID,
        *,
        outcome: ProviderSettlementOutcome,
        usage: ProviderUsage,
    ) -> ProviderBudgetSettlement:
        self.settlements.append((reservation_id, outcome, usage))
        return cast(ProviderBudgetSettlement, object())


def dispatch_context() -> ProviderDispatchContext:
    return ProviderDispatchContext(
        processing_job_id=UUID("11111111-1111-4111-8111-111111111111"),
        attempt_number=1,
        lease_owner="worker-test",
        claim_token=UUID("22222222-2222-4222-8222-222222222222"),
    )


def transcription_context(
    path: Path,
    *,
    audio_duration_ms: int = 1000,
) -> TranscriptionRunContext:
    content = path.read_bytes() if path.is_file() else b"missing"
    return TranscriptionRunContext(
        dispatch=dispatch_context(),
        audio_duration_ms=audio_duration_ms,
        audio_sha256=hashlib.sha256(content).hexdigest(),
        audio_size_bytes=len(content),
    )
