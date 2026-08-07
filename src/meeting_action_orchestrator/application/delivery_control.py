from __future__ import annotations

import asyncio
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from types import TracebackType
from typing import Protocol
from uuid import UUID, uuid4

from meeting_action_orchestrator.application.auditing import (
    WorkflowEventSink,
    append_delivery_transition,
    append_meeting_transition,
)
from meeting_action_orchestrator.application.errors import (
    OperationConflictError,
    ResourceNotFoundError,
)
from meeting_action_orchestrator.application.state_machine import (
    transition_meeting,
    transition_write_intent,
)
from meeting_action_orchestrator.domain.enums import (
    DeliveryOperationKind,
    DeliveryOperationStatus,
    FailureCode,
    FailureDisposition,
    MeetingStatus,
    WriteStatus,
)
from meeting_action_orchestrator.domain.errors import IdempotencyConflictError
from meeting_action_orchestrator.domain.hashing import canonical_sha256
from meeting_action_orchestrator.domain.models import (
    Approval,
    DeliveryOperationBinding,
    Meeting,
    WorkflowFailure,
    WriteIntent,
    WriteReceipt,
)

_RETRY_MESSAGE = "Delivery was queued for another approved attempt"
_RECONCILE_MESSAGE = "Delivery requires remote reconciliation before another attempt"
_AMBIGUOUS_FAILURE_CODES = frozenset(
    {
        FailureCode.IDEMPOTENCY_CONFLICT,
        FailureCode.INTERNAL,
        FailureCode.UNKNOWN_REMOTE_OUTCOME,
    }
)


class Clock(Protocol):
    def now(self) -> datetime: ...


class RetryScheduler(Protocol):
    def next_attempt_at(self, now: datetime, attempt_count: int) -> datetime: ...


class IntentReconciler(Protocol):
    async def reconcile_intent(
        self,
        intent_id: UUID,
        actor_id: str | None = None,
    ) -> object: ...


class ControlMeetingRepository(Protocol):
    def get(self, meeting_id: UUID) -> Meeting | None: ...

    def save(self, meeting: Meeting, expected_version: int) -> None: ...


class ControlApprovalRepository(Protocol):
    def for_meeting(self, meeting_id: UUID) -> Approval | None: ...


class ControlIntentRepository(Protocol):
    def get(self, intent_id: UUID) -> WriteIntent | None: ...

    def list_for_approval(self, approval_id: UUID) -> Sequence[WriteIntent]: ...

    def save(self, intent: WriteIntent, expected_version: int) -> None: ...


class ControlReceiptRepository(Protocol):
    def for_intent(self, intent_id: UUID) -> WriteReceipt | None: ...


class ControlDeliveryOperationRepository(Protocol):
    def add(self, binding: DeliveryOperationBinding) -> None: ...

    def get(self, request_key: str) -> DeliveryOperationBinding | None: ...

    def claim(
        self,
        request_key: str,
        owner: str,
        now: datetime,
        lease_until: datetime,
    ) -> DeliveryOperationBinding | None: ...

    def release(
        self,
        request_key: str,
        owner: str,
        expected_version: int,
        now: datetime,
    ) -> bool: ...

    def renew(
        self,
        request_key: str,
        owner: str,
        expected_version: int,
        now: datetime,
        lease_until: datetime,
    ) -> DeliveryOperationBinding | None: ...

    def complete(
        self,
        request_key: str,
        owner: str,
        expected_version: int,
        now: datetime,
    ) -> bool: ...


class ControlUnitOfWork(Protocol):
    meetings: ControlMeetingRepository
    approvals: ControlApprovalRepository
    write_intents: ControlIntentRepository
    write_receipts: ControlReceiptRepository
    delivery_operations: ControlDeliveryOperationRepository
    workflow_events: WorkflowEventSink

    def __enter__(self) -> ControlUnitOfWork: ...

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None: ...

    def commit(self) -> None: ...


@dataclass(frozen=True, slots=True)
class DeliveryControlResult:
    meeting: Meeting
    intents: tuple[WriteIntent, ...]
    receipts: tuple[WriteReceipt, ...] = ()
    replayed: bool = False


@dataclass(frozen=True, slots=True)
class _Selection:
    meeting: Meeting
    approval: Approval
    all_intents: tuple[WriteIntent, ...]
    selected: tuple[WriteIntent, ...]
    binding: DeliveryOperationBinding
    replayed: bool


class DeliveryControlService:
    def __init__(
        self,
        *,
        unit_of_work: Callable[[], ControlUnitOfWork],
        reconciler: IntentReconciler,
        clock: Clock,
        retry_scheduler: RetryScheduler,
        operation_lease_duration: timedelta = timedelta(minutes=2),
        max_attempts: int = 5,
    ) -> None:
        if operation_lease_duration <= timedelta(0):
            raise ValueError("operation_lease_duration must be positive")
        if not 1 <= max_attempts <= 5:
            raise ValueError("max_attempts must be between one and five")
        self._unit_of_work = unit_of_work
        self._reconciler = reconciler
        self._clock = clock
        self._retry_scheduler = retry_scheduler
        self._operation_lease_duration = operation_lease_duration
        self._max_attempts = max_attempts

    async def retry(
        self,
        meeting_id: UUID,
        *,
        intent_ids: tuple[UUID, ...],
        request_key: str,
        actor_id: str,
    ) -> DeliveryControlResult:
        _validate_operation_identity(request_key, actor_id)
        before = await asyncio.to_thread(
            self._select_and_claim,
            meeting_id,
            intent_ids,
            request_key,
            actor_id,
            DeliveryOperationKind.RETRY,
        )
        if before.replayed:
            return await asyncio.to_thread(self._snapshot, meeting_id, True)
        binding = before.binding
        try:
            for intent in before.selected:
                binding = await asyncio.to_thread(self._renew_operation, binding)
                if intent.status is WriteStatus.UNKNOWN:
                    await self._reconciler.reconcile_intent(intent.id, actor_id=actor_id)
                elif _requires_reconciliation(intent):
                    prepared = await asyncio.to_thread(
                        self._prepare_reconciliation,
                        meeting_id,
                        before.approval.id,
                        intent.id,
                        actor_id,
                    )
                    if prepared:
                        await self._reconciler.reconcile_intent(
                            intent.id,
                            actor_id=actor_id,
                        )
                elif _can_direct_retry(intent, self._max_attempts):
                    await asyncio.to_thread(
                        self._queue_retry,
                        meeting_id,
                        before.approval.id,
                        intent.id,
                        actor_id,
                    )
            await asyncio.to_thread(self._complete_operation, binding)
        except asyncio.CancelledError:
            raise
        except Exception:
            await asyncio.to_thread(self._release_operation, binding)
            raise
        return await asyncio.to_thread(self._snapshot, meeting_id, False)

    async def reconcile(
        self,
        meeting_id: UUID,
        *,
        intent_ids: tuple[UUID, ...],
        request_key: str,
        actor_id: str,
    ) -> DeliveryControlResult:
        _validate_operation_identity(request_key, actor_id)
        before = await asyncio.to_thread(
            self._select_and_claim,
            meeting_id,
            intent_ids,
            request_key,
            actor_id,
            DeliveryOperationKind.RECONCILE,
        )
        if before.replayed:
            return await asyncio.to_thread(self._snapshot, meeting_id, True)
        binding = before.binding
        try:
            for intent in before.selected:
                binding = await asyncio.to_thread(self._renew_operation, binding)
                if intent.status is WriteStatus.UNKNOWN:
                    await self._reconciler.reconcile_intent(intent.id, actor_id=actor_id)
            await asyncio.to_thread(self._complete_operation, binding)
        except asyncio.CancelledError:
            raise
        except Exception:
            await asyncio.to_thread(self._release_operation, binding)
            raise
        return await asyncio.to_thread(self._snapshot, meeting_id, False)

    def _select_and_claim(
        self,
        meeting_id: UUID,
        intent_ids: tuple[UUID, ...],
        request_key: str,
        actor_id: str,
        operation: DeliveryOperationKind,
    ) -> _Selection:
        now = self._now()
        with self._unit_of_work() as uow:
            if len(intent_ids) != len(set(intent_ids)):
                raise OperationConflictError("Each write intent may be selected only once")
            meeting = uow.meetings.get(meeting_id)
            if meeting is None:
                raise ResourceNotFoundError("Meeting")
            approval = uow.approvals.for_meeting(meeting_id)
            if approval is None:
                raise ResourceNotFoundError("Approval")
            all_intents = tuple(uow.write_intents.list_for_approval(approval.id))
            _validate_approval_binding(meeting, approval, all_intents)
            indexed = {intent.id: intent for intent in all_intents}
            selected_ids = intent_ids or tuple(indexed)
            try:
                selected = tuple(indexed[intent_id] for intent_id in selected_ids)
            except KeyError as error:
                raise ResourceNotFoundError("Write intent") from error
            binding = DeliveryOperationBinding(
                request_key=request_key,
                meeting_id=meeting_id,
                operation=operation,
                actor_id=actor_id,
                selection_fingerprint=_selection_fingerprint(selected),
                created_at=now,
                updated_at=now,
            )
            existing = uow.delivery_operations.get(request_key)
            if existing is not None and not _same_binding(existing, binding):
                raise IdempotencyConflictError(request_key)
            if existing is not None and existing.status is DeliveryOperationStatus.COMPLETED:
                return _Selection(
                    meeting=meeting,
                    approval=approval,
                    all_intents=all_intents,
                    selected=selected,
                    binding=existing,
                    replayed=True,
                )
            if existing is None:
                uow.delivery_operations.add(binding)
            owner = f"operation:{uuid4().hex}"
            claimed = uow.delivery_operations.claim(
                request_key,
                owner,
                now,
                now + self._operation_lease_duration,
            )
            if claimed is None:
                raise OperationConflictError("The delivery operation is already in progress")
            uow.commit()
        return _Selection(
            meeting=meeting,
            approval=approval,
            all_intents=all_intents,
            selected=selected,
            binding=claimed,
            replayed=False,
        )

    def _queue_retry(
        self,
        meeting_id: UUID,
        approval_id: UUID,
        intent_id: UUID,
        actor_id: str,
    ) -> None:
        now = self._now()
        with self._unit_of_work() as uow:
            meeting = uow.meetings.get(meeting_id)
            approval = uow.approvals.for_meeting(meeting_id)
            intent = uow.write_intents.get(intent_id)
            if meeting is None or approval is None or intent is None:
                raise ResourceNotFoundError("Delivery state")
            _validate_approval_binding(meeting, approval, (intent,))
            if approval.id != approval_id:
                raise OperationConflictError("The approved delivery set changed")
            if not _can_direct_retry(intent, self._max_attempts):
                return
            previous = intent.last_failure
            retry_failure = WorkflowFailure(
                code=previous.code if previous is not None else FailureCode.PROVIDER_UNAVAILABLE,
                disposition=FailureDisposition.RETRYABLE,
                safe_message=_RETRY_MESSAGE,
                occurred_at=now,
            )
            updated = transition_write_intent(
                intent,
                WriteStatus.RETRY_WAIT,
                now,
                failure=retry_failure,
                next_attempt_at=self._retry_scheduler.next_attempt_at(
                    now,
                    max(intent.attempt_count, 1),
                ),
            )
            uow.write_intents.save(updated, intent.version)
            append_delivery_transition(
                uow.workflow_events,
                intent,
                updated,
                now,
                actor_id=actor_id,
            )
            if meeting.status in {
                MeetingStatus.PARTIALLY_FILED,
                MeetingStatus.FILING_FAILED,
            }:
                filing = transition_meeting(meeting, MeetingStatus.FILING, now)
                uow.meetings.save(filing, meeting.version)
                append_meeting_transition(
                    uow.workflow_events,
                    meeting,
                    filing,
                    now,
                    actor_id=actor_id,
                )
            uow.commit()

    def _prepare_reconciliation(
        self,
        meeting_id: UUID,
        approval_id: UUID,
        intent_id: UUID,
        actor_id: str,
    ) -> bool:
        now = self._now()
        with self._unit_of_work() as uow:
            meeting = uow.meetings.get(meeting_id)
            approval = uow.approvals.for_meeting(meeting_id)
            intent = uow.write_intents.get(intent_id)
            if meeting is None or approval is None or intent is None:
                raise ResourceNotFoundError("Delivery state")
            _validate_approval_binding(meeting, approval, (intent,))
            if approval.id != approval_id:
                raise OperationConflictError("The approved delivery set changed")
            if intent.status is WriteStatus.UNKNOWN:
                return True
            if not _requires_reconciliation(intent):
                return False
            previous = intent.last_failure
            if previous is None:
                return False
            failure = WorkflowFailure(
                code=previous.code,
                disposition=FailureDisposition.UNKNOWN_OUTCOME,
                safe_message=_RECONCILE_MESSAGE,
                occurred_at=now,
            )
            updated = transition_write_intent(
                intent,
                WriteStatus.UNKNOWN,
                now,
                failure=failure,
                next_reconcile_at=now,
            )
            uow.write_intents.save(updated, intent.version)
            append_delivery_transition(
                uow.workflow_events,
                intent,
                updated,
                now,
                actor_id=actor_id,
            )
            if meeting.status in {
                MeetingStatus.PARTIALLY_FILED,
                MeetingStatus.FILING_FAILED,
            }:
                filing = transition_meeting(meeting, MeetingStatus.FILING, now)
                uow.meetings.save(filing, meeting.version)
                append_meeting_transition(
                    uow.workflow_events,
                    meeting,
                    filing,
                    now,
                    actor_id=actor_id,
                )
            uow.commit()
        return True

    def _complete_operation(self, binding: DeliveryOperationBinding) -> None:
        now = self._now()
        owner = binding.lease_owner
        if owner is None:
            raise OperationConflictError("The delivery operation has no execution owner")
        with self._unit_of_work() as uow:
            completed = uow.delivery_operations.complete(
                binding.request_key,
                owner,
                binding.version,
                now,
            )
            if not completed:
                raise OperationConflictError("The delivery operation lease is no longer current")
            uow.commit()

    def _renew_operation(
        self,
        binding: DeliveryOperationBinding,
    ) -> DeliveryOperationBinding:
        now = self._now()
        owner = binding.lease_owner
        if owner is None:
            raise OperationConflictError("The delivery operation has no execution owner")
        with self._unit_of_work() as uow:
            renewed = uow.delivery_operations.renew(
                binding.request_key,
                owner,
                binding.version,
                now,
                now + self._operation_lease_duration,
            )
            if renewed is None:
                raise OperationConflictError("The delivery operation lease is no longer current")
            uow.commit()
        return renewed

    def _release_operation(self, binding: DeliveryOperationBinding) -> None:
        now = self._now()
        owner = binding.lease_owner
        if owner is None:
            return
        with self._unit_of_work() as uow:
            uow.delivery_operations.release(
                binding.request_key,
                owner,
                binding.version,
                now,
            )
            uow.commit()

    def _snapshot(
        self,
        meeting_id: UUID,
        replayed: bool,
    ) -> DeliveryControlResult:
        with self._unit_of_work() as uow:
            meeting = uow.meetings.get(meeting_id)
            if meeting is None:
                raise ResourceNotFoundError("Meeting")
            approval = uow.approvals.for_meeting(meeting_id)
            if approval is None:
                raise ResourceNotFoundError("Approval")
            intents = tuple(uow.write_intents.list_for_approval(approval.id))
            receipts = tuple(
                receipt
                for intent in intents
                if (receipt := uow.write_receipts.for_intent(intent.id)) is not None
            )
        _validate_approval_binding(meeting, approval, intents)
        return DeliveryControlResult(
            meeting=meeting,
            intents=intents,
            receipts=receipts,
            replayed=replayed,
        )

    def _now(self) -> datetime:
        now = self._clock.now()
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("clock must return an aware datetime")
        return now


def _validate_approval_binding(
    meeting: Meeting,
    approval: Approval,
    intents: tuple[WriteIntent, ...],
) -> None:
    if (
        approval.meeting_id != meeting.id
        or meeting.approved_review_id != approval.review_revision_id
        or any(
            intent.meeting_id != meeting.id or intent.approval_id != approval.id
            for intent in intents
        )
    ):
        raise OperationConflictError("The delivery state does not match the approved meeting")


def _validate_operation_identity(request_key: str, actor_id: str) -> None:
    for name, value in (("request_key", request_key), ("actor_id", actor_id)):
        if not value or len(value) > 200 or value != value.strip():
            raise ValueError(f"{name} must be between 1 and 200 characters")


def _selection_fingerprint(intents: tuple[WriteIntent, ...]) -> str:
    return canonical_sha256({"intent_ids": sorted(str(intent.id) for intent in intents)})


def _same_binding(
    existing: DeliveryOperationBinding,
    requested: DeliveryOperationBinding,
) -> bool:
    return (
        existing.meeting_id == requested.meeting_id
        and existing.operation is requested.operation
        and existing.actor_id == requested.actor_id
        and existing.selection_fingerprint == requested.selection_fingerprint
    )


def _requires_reconciliation(intent: WriteIntent) -> bool:
    return (
        intent.status is WriteStatus.PERMANENT_FAILED
        and intent.last_failure is not None
        and intent.last_failure.code in _AMBIGUOUS_FAILURE_CODES
    )


def _can_direct_retry(intent: WriteIntent, max_attempts: int) -> bool:
    return (
        intent.status is WriteStatus.PERMANENT_FAILED
        and intent.attempt_count < max_attempts
        and not _requires_reconciliation(intent)
    )
