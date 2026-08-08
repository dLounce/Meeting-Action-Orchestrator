from __future__ import annotations

import asyncio
import random
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
    DeliveryGatewayError,
    OperationConflictError,
    PermanentDeliveryError,
    RetryableDeliveryError,
    UnknownDeliveryOutcomeError,
)
from meeting_action_orchestrator.application.state_machine import (
    derive_filing_status,
    transition_meeting,
    transition_write_intent,
)
from meeting_action_orchestrator.domain.enums import (
    FailureCode,
    FailureDisposition,
    MeetingStatus,
    WriteKind,
    WriteStatus,
)
from meeting_action_orchestrator.domain.errors import (
    DomainInvariantError,
    IdempotencyConflictError,
)
from meeting_action_orchestrator.domain.models import (
    Approval,
    Meeting,
    RecapArtifact,
    WorkflowFailure,
    WriteIntent,
    WriteReceipt,
)
from meeting_action_orchestrator.domain.services import validate_write_receipt
from meeting_action_orchestrator.domain.workflow_events import DeliveryChangeKind

_RETRYABLE_MESSAGE = "The connector is temporarily unavailable"
_PERMANENT_MESSAGE = "The connector rejected the approved action"
_UNKNOWN_MESSAGE = "The connector outcome is unknown and requires reconciliation"
_EXHAUSTED_MESSAGE = "The connector could not complete the approved action after repeated attempts"
_ABSENT_MESSAGE = "No prior connector result was found; the approved action will be retried"
_INVALID_RECEIPT_MESSAGE = "The connector returned a receipt that did not match the approved action"
_INVALID_INTENT_MESSAGE = "The approved action is no longer eligible for delivery"


class Clock(Protocol):
    def now(self) -> datetime: ...


class RetryScheduler(Protocol):
    def next_attempt_at(self, now: datetime, attempt_count: int) -> datetime: ...


class DeliveryGateway(Protocol):
    async def ensure_task(self, intent: WriteIntent) -> WriteReceipt: ...

    async def ensure_event(self, intent: WriteIntent) -> WriteReceipt: ...

    async def find_task(self, idempotency_key: str) -> WriteReceipt | None: ...

    async def find_event(self, idempotency_key: str) -> WriteReceipt | None: ...


class DeliveryMeetingRepository(Protocol):
    def get(self, meeting_id: UUID) -> Meeting | None: ...

    def save(self, meeting: Meeting, expected_version: int) -> None: ...


class DeliveryApprovalRepository(Protocol):
    def get(self, approval_id: UUID) -> Approval | None: ...


class DeliveryRecapRepository(Protocol):
    def for_approval(self, approval_id: UUID) -> RecapArtifact | None: ...


class DeliveryIntentRepository(Protocol):
    def get(self, intent_id: UUID) -> WriteIntent | None: ...

    def list_for_approval(self, approval_id: UUID) -> Sequence[WriteIntent]: ...

    def claim_due_ids(
        self,
        worker_id: str,
        now: datetime,
        lease_until: datetime,
        limit: int,
    ) -> Sequence[UUID]: ...

    def claim_due_with_previous_statuses(
        self,
        worker_id: str,
        now: datetime,
        lease_until: datetime,
        limit: int,
    ) -> Sequence[tuple[UUID, WriteStatus]]: ...

    def recover_expired_ids(
        self,
        now: datetime,
        failure: WorkflowFailure,
        limit: int,
    ) -> Sequence[UUID]: ...

    def list_unknown_ids(self, now: datetime, limit: int) -> Sequence[UUID]: ...

    def claim_due_unknown_ids(
        self,
        owner: str,
        now: datetime,
        lease_until: datetime,
        limit: int,
    ) -> Sequence[UUID]: ...

    def claim_unknown(
        self,
        intent_id: UUID,
        owner: str,
        now: datetime,
        lease_until: datetime,
        *,
        force: bool,
    ) -> WriteIntent | None: ...

    def save(self, intent: WriteIntent, expected_version: int) -> None: ...


class DeliveryReceiptRepository(Protocol):
    def add(self, receipt: WriteReceipt) -> None: ...

    def for_intent(self, intent_id: UUID) -> WriteReceipt | None: ...


class DeliveryUnitOfWork(Protocol):
    @property
    def meetings(self) -> DeliveryMeetingRepository: ...

    @property
    def approvals(self) -> DeliveryApprovalRepository: ...

    @property
    def recaps(self) -> DeliveryRecapRepository: ...

    @property
    def write_intents(self) -> DeliveryIntentRepository: ...

    @property
    def write_receipts(self) -> DeliveryReceiptRepository: ...

    @property
    def workflow_events(self) -> WorkflowEventSink: ...

    def __enter__(self) -> DeliveryUnitOfWork: ...

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None: ...

    def commit(self) -> None: ...


@dataclass(frozen=True, slots=True)
class DeliveryResult:
    intent_id: UUID
    status: WriteStatus
    replayed: bool = False


@dataclass(frozen=True, slots=True)
class DeliveryBatch:
    recovered: tuple[UUID, ...]
    results: tuple[DeliveryResult, ...]


@dataclass(frozen=True, slots=True)
class _ReconciliationClaim:
    intent_id: UUID
    owner: str
    actor_id: str | None = None


@dataclass(frozen=True, slots=True)
class FullJitterRetryScheduler:
    base_delay: timedelta = timedelta(seconds=2)
    maximum_delay: timedelta = timedelta(minutes=5)
    random_value: Callable[[], float] = random.random

    def __post_init__(self) -> None:
        if self.base_delay <= timedelta(0):
            raise ValueError("base_delay must be positive")
        if self.maximum_delay < self.base_delay:
            raise ValueError("maximum_delay must not be shorter than base_delay")

    def next_attempt_at(self, now: datetime, attempt_count: int) -> datetime:
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("now must include a UTC offset")
        if attempt_count < 1:
            raise ValueError("attempt_count must be positive")
        sample = self.random_value()
        if not 0 <= sample <= 1:
            raise ValueError("random_value must return a value between zero and one")
        ceiling = self.base_delay.total_seconds()
        maximum = self.maximum_delay.total_seconds()
        for _ in range(attempt_count - 1):
            if ceiling >= maximum:
                break
            ceiling = min(maximum, ceiling * 2)
        delay = max(ceiling * sample, 0.000001)
        return now + timedelta(seconds=delay)


class PersistedApprovalAuthorizer:
    def __init__(
        self,
        unit_of_work: Callable[[], DeliveryUnitOfWork],
        clock: Clock,
    ) -> None:
        self._unit_of_work = unit_of_work
        self._clock = clock

    async def permits(self, intent: WriteIntent) -> bool:
        return await asyncio.to_thread(self._permits, intent)

    def _permits(self, intent: WriteIntent) -> bool:
        now = self._clock.now()
        with self._unit_of_work() as uow:
            persisted = uow.write_intents.get(intent.id)
            if persisted != intent:
                return False
            return _is_executable(uow, persisted, now)


class ApprovedOutboxExecutor:
    def __init__(
        self,
        *,
        unit_of_work: Callable[[], DeliveryUnitOfWork],
        gateway: DeliveryGateway,
        clock: Clock,
        retry_scheduler: RetryScheduler,
        worker_id: str,
        lease_duration: timedelta = timedelta(minutes=2),
        reconciliation_lease_duration: timedelta = timedelta(minutes=2),
        max_attempts: int = 5,
    ) -> None:
        if not worker_id or len(worker_id) > 200 or worker_id != worker_id.strip():
            raise ValueError("worker_id must be between 1 and 200 characters")
        if lease_duration <= timedelta(0):
            raise ValueError("lease_duration must be positive")
        if reconciliation_lease_duration <= timedelta(0):
            raise ValueError("reconciliation_lease_duration must be positive")
        if not 1 <= max_attempts <= 5:
            raise ValueError("max_attempts must be between one and five")
        self._unit_of_work = unit_of_work
        self._gateway = gateway
        self._clock = clock
        self._retry_scheduler = retry_scheduler
        self._worker_id = worker_id
        self._lease_duration = lease_duration
        self._reconciliation_lease_duration = reconciliation_lease_duration
        self._max_attempts = max_attempts

    async def run_once(self, limit: int = 20) -> DeliveryBatch:
        if limit <= 0:
            return DeliveryBatch(recovered=(), results=())
        recovered = await asyncio.to_thread(self._recover_expired, limit)
        results: list[DeliveryResult] = []
        for _ in range(limit):
            claim = await asyncio.to_thread(self._claim_due_unknown)
            if claim is None:
                break
            results.append(await self._reconcile_claimed(claim))
        remaining = limit - len(results)
        for _ in range(remaining):
            claimed = await asyncio.to_thread(self._claim_due, 1)
            if not claimed:
                break
            results.append(await self.deliver_intent(claimed[0]))
        return DeliveryBatch(recovered=recovered, results=tuple(results))

    async def deliver_intent(self, intent_id: UUID) -> DeliveryResult:
        snapshot, receipt, executable = await asyncio.to_thread(
            self._load_execution_snapshot,
            intent_id,
        )
        if receipt is not None:
            return await asyncio.to_thread(
                self._record_success,
                snapshot,
                receipt,
                replayed=True,
            )
        if not executable:
            return DeliveryResult(snapshot.id, snapshot.status)
        try:
            if snapshot.proposal.kind is WriteKind.TASK:
                created = await self._gateway.ensure_task(snapshot)
            else:
                created = await self._gateway.ensure_event(snapshot)
            validate_write_receipt(snapshot, created)
        except RetryableDeliveryError as error:
            return await asyncio.to_thread(self._record_gateway_failure, snapshot, error)
        except PermanentDeliveryError as error:
            return await asyncio.to_thread(self._record_gateway_failure, snapshot, error)
        except UnknownDeliveryOutcomeError as error:
            return await asyncio.to_thread(self._record_gateway_failure, snapshot, error)
        except (DomainInvariantError, IdempotencyConflictError):
            return await asyncio.to_thread(
                self._record_failure,
                snapshot,
                FailureCode.IDEMPOTENCY_CONFLICT,
                FailureDisposition.UNKNOWN_OUTCOME,
                _INVALID_RECEIPT_MESSAGE,
            )
        except Exception:
            return await asyncio.to_thread(
                self._record_failure,
                snapshot,
                FailureCode.UNKNOWN_REMOTE_OUTCOME,
                FailureDisposition.UNKNOWN_OUTCOME,
                _UNKNOWN_MESSAGE,
            )
        return await asyncio.to_thread(self._record_success, snapshot, created)

    async def reconcile_intent(
        self,
        intent_id: UUID,
        actor_id: str | None = None,
    ) -> DeliveryResult:
        claim = await asyncio.to_thread(self._claim_unknown, intent_id, True, actor_id)
        if claim is None:
            snapshot = await asyncio.to_thread(self._load_intent, intent_id)
            if snapshot.status is WriteStatus.UNKNOWN:
                raise OperationConflictError("The write intent is already being reconciled")
            return DeliveryResult(snapshot.id, snapshot.status)
        return await self._reconcile_claimed(claim)

    async def _reconcile_claimed(self, claim: _ReconciliationClaim) -> DeliveryResult:
        snapshot, receipt = await asyncio.to_thread(
            self._load_reconciliation_snapshot,
            claim.intent_id,
            claim.owner,
        )
        if receipt is not None:
            return await asyncio.to_thread(
                self._record_success,
                snapshot,
                receipt,
                replayed=True,
                reconciliation_owner=claim.owner,
                actor_id=claim.actor_id,
            )
        if snapshot.status is not WriteStatus.UNKNOWN:
            return DeliveryResult(snapshot.id, snapshot.status)
        try:
            if snapshot.proposal.kind is WriteKind.TASK:
                found = await self._gateway.find_task(snapshot.idempotency_key)
            else:
                found = await self._gateway.find_event(snapshot.idempotency_key)
            if found is None:
                return await asyncio.to_thread(
                    self._record_confirmed_absence,
                    snapshot,
                    claim.owner,
                    claim.actor_id,
                )
            validate_write_receipt(snapshot, found)
        except (
            PermanentDeliveryError,
            RetryableDeliveryError,
            UnknownDeliveryOutcomeError,
        ) as error:
            return await asyncio.to_thread(
                self._refresh_unknown,
                snapshot,
                claim.owner,
                error.code,
                _safe_request_id(error.provider_request_id),
                claim.actor_id,
            )
        except (DomainInvariantError, IdempotencyConflictError):
            return await asyncio.to_thread(
                self._refresh_unknown,
                snapshot,
                claim.owner,
                FailureCode.IDEMPOTENCY_CONFLICT,
                actor_id=claim.actor_id,
            )
        except Exception:
            return await asyncio.to_thread(
                self._refresh_unknown,
                snapshot,
                claim.owner,
                actor_id=claim.actor_id,
            )
        return await asyncio.to_thread(
            self._record_success,
            snapshot,
            found,
            reconciliation_owner=claim.owner,
            actor_id=claim.actor_id,
        )

    def _recover_expired(self, limit: int) -> tuple[UUID, ...]:
        now = self._now()
        failure = _failure(
            FailureCode.UNKNOWN_REMOTE_OUTCOME,
            FailureDisposition.UNKNOWN_OUTCOME,
            _UNKNOWN_MESSAGE,
            now,
        )
        with self._unit_of_work() as uow:
            recovered = tuple(uow.write_intents.recover_expired_ids(now, failure, limit))
            for intent_id in recovered:
                current = _required_intent(uow.write_intents.get(intent_id))
                append_delivery_transition(
                    uow.workflow_events,
                    WriteStatus.IN_FLIGHT,
                    current,
                    now,
                )
            uow.commit()
        return recovered

    def _claim_due_unknown(self) -> _ReconciliationClaim | None:
        now = self._now()
        owner = f"reconcile:{uuid4().hex}"
        with self._unit_of_work() as uow:
            claimed = tuple(
                uow.write_intents.claim_due_unknown_ids(
                    owner,
                    now,
                    now + self._reconciliation_lease_duration,
                    1,
                )
            )
            uow.commit()
        if not claimed:
            return None
        return _ReconciliationClaim(claimed[0], owner)

    def _claim_unknown(
        self,
        intent_id: UUID,
        force: bool,
        actor_id: str | None = None,
    ) -> _ReconciliationClaim | None:
        now = self._now()
        owner = f"reconcile:{uuid4().hex}"
        with self._unit_of_work() as uow:
            claimed = uow.write_intents.claim_unknown(
                intent_id,
                owner,
                now,
                now + self._reconciliation_lease_duration,
                force=force,
            )
            uow.commit()
        return _ReconciliationClaim(intent_id, owner, actor_id) if claimed is not None else None

    def _claim_due(self, limit: int) -> tuple[UUID, ...]:
        now = self._now()
        with self._unit_of_work() as uow:
            claimed = tuple(
                uow.write_intents.claim_due_with_previous_statuses(
                    self._worker_id,
                    now,
                    now + self._lease_duration,
                    limit,
                )
            )
            for intent_id, previous_status in claimed:
                current = _required_intent(uow.write_intents.get(intent_id))
                append_delivery_transition(
                    uow.workflow_events,
                    previous_status,
                    current,
                    now,
                )
            uow.commit()
        return tuple(intent_id for intent_id, _ in claimed)

    def _load_execution_snapshot(
        self,
        intent_id: UUID,
    ) -> tuple[WriteIntent, WriteReceipt | None, bool]:
        now = self._now()
        with self._unit_of_work() as uow:
            intent = _required_intent(uow.write_intents.get(intent_id))
            receipt = uow.write_receipts.for_intent(intent.id)
            if receipt is not None:
                validate_write_receipt(intent, receipt)
            executable = receipt is None and _is_executable(uow, intent, now, self._worker_id)
            return intent, receipt, executable

    def _load_reconciliation_snapshot(
        self,
        intent_id: UUID,
        owner: str,
    ) -> tuple[WriteIntent, WriteReceipt | None]:
        now = self._now()
        with self._unit_of_work() as uow:
            intent = _required_intent(uow.write_intents.get(intent_id))
            if not _holds_reconciliation_lease(intent, owner, now):
                raise OperationConflictError("The write reconciliation lease is no longer current")
            receipt = uow.write_receipts.for_intent(intent.id)
            if receipt is None and intent.status is WriteStatus.UNKNOWN:
                _require_approved(uow, intent)
            if receipt is not None:
                validate_write_receipt(intent, receipt)
            return intent, receipt

    def _record_gateway_failure(
        self,
        snapshot: WriteIntent,
        error: DeliveryGatewayError,
    ) -> DeliveryResult:
        message = {
            FailureDisposition.RETRYABLE: _RETRYABLE_MESSAGE,
            FailureDisposition.PERMANENT: _PERMANENT_MESSAGE,
            FailureDisposition.UNKNOWN_OUTCOME: _UNKNOWN_MESSAGE,
        }[error.disposition]
        return self._record_failure(
            snapshot,
            error.code,
            error.disposition,
            message,
            provider_request_id=_safe_request_id(error.provider_request_id),
        )

    def _record_failure(
        self,
        snapshot: WriteIntent,
        code: FailureCode,
        disposition: FailureDisposition,
        message: str,
        *,
        provider_request_id: str | None = None,
    ) -> DeliveryResult:
        now = self._now()
        with self._unit_of_work() as uow:
            current = _required_intent(uow.write_intents.get(snapshot.id))
            existing = uow.write_receipts.for_intent(current.id)
            if existing is not None:
                validate_write_receipt(current, existing)
                return self._commit_success(uow, current, replayed=True)
            if current != snapshot:
                return DeliveryResult(current.id, current.status)
            effective_disposition = disposition
            effective_message = message
            if (
                disposition is FailureDisposition.RETRYABLE
                and current.attempt_count >= self._max_attempts
            ):
                effective_disposition = FailureDisposition.PERMANENT
                effective_message = _EXHAUSTED_MESSAGE
            failure = _failure(
                code,
                effective_disposition,
                effective_message,
                now,
                provider_request_id,
            )
            if effective_disposition is FailureDisposition.RETRYABLE:
                next_attempt = self._retry_scheduler.next_attempt_at(now, current.attempt_count)
                updated = transition_write_intent(
                    current,
                    WriteStatus.RETRY_WAIT,
                    now,
                    failure=failure,
                    next_attempt_at=next_attempt,
                )
            elif effective_disposition is FailureDisposition.PERMANENT:
                updated = transition_write_intent(
                    current,
                    WriteStatus.PERMANENT_FAILED,
                    now,
                    failure=failure,
                )
            else:
                updated = transition_write_intent(
                    current,
                    WriteStatus.UNKNOWN,
                    now,
                    failure=failure,
                )
            uow.write_intents.save(updated, current.version)
            append_delivery_transition(uow.workflow_events, current, updated, now)
            self._reduce_meeting(uow, updated.approval_id, now)
            uow.commit()
        return DeliveryResult(updated.id, updated.status)

    def _record_confirmed_absence(
        self,
        snapshot: WriteIntent,
        owner: str,
        actor_id: str | None = None,
    ) -> DeliveryResult:
        now = self._now()
        with self._unit_of_work() as uow:
            current = _required_intent(uow.write_intents.get(snapshot.id))
            if current != snapshot or not _holds_reconciliation_lease(current, owner, now):
                return DeliveryResult(current.id, current.status)
            if current.attempt_count >= self._max_attempts:
                failure = _failure(
                    FailureCode.PROVIDER_UNAVAILABLE,
                    FailureDisposition.PERMANENT,
                    _EXHAUSTED_MESSAGE,
                    now,
                )
                updated = transition_write_intent(
                    current,
                    WriteStatus.PERMANENT_FAILED,
                    now,
                    failure=failure,
                )
            else:
                failure = _failure(
                    FailureCode.UNKNOWN_REMOTE_OUTCOME,
                    FailureDisposition.RETRYABLE,
                    _ABSENT_MESSAGE,
                    now,
                )
                updated = transition_write_intent(
                    current,
                    WriteStatus.RETRY_WAIT,
                    now,
                    failure=failure,
                    next_attempt_at=self._retry_scheduler.next_attempt_at(
                        now,
                        current.attempt_count,
                    ),
                )
            uow.write_intents.save(updated, current.version)
            append_delivery_transition(
                uow.workflow_events,
                current,
                updated,
                now,
                actor_id=actor_id,
            )
            self._reduce_meeting(uow, updated.approval_id, now, actor_id=actor_id)
            uow.commit()
        return DeliveryResult(updated.id, updated.status)

    def _refresh_unknown(
        self,
        snapshot: WriteIntent,
        owner: str,
        code: FailureCode = FailureCode.UNKNOWN_REMOTE_OUTCOME,
        provider_request_id: str | None = None,
        actor_id: str | None = None,
    ) -> DeliveryResult:
        now = self._now()
        failure = _failure(
            code,
            FailureDisposition.UNKNOWN_OUTCOME,
            _UNKNOWN_MESSAGE,
            now,
            provider_request_id,
        )
        with self._unit_of_work() as uow:
            current = _required_intent(uow.write_intents.get(snapshot.id))
            if current != snapshot or not _holds_reconciliation_lease(current, owner, now):
                return DeliveryResult(current.id, current.status)
            reconcile_attempt_count = current.reconcile_attempt_count + 1
            updated = WriteIntent.model_validate(
                current.model_dump(mode="python")
                | {
                    "last_failure": failure,
                    "next_reconcile_at": self._retry_scheduler.next_attempt_at(
                        now,
                        reconcile_attempt_count,
                    ),
                    "reconcile_attempt_count": reconcile_attempt_count,
                    "reconcile_lease_owner": None,
                    "reconcile_lease_expires_at": None,
                    "updated_at": now,
                    "version": current.version + 1,
                }
            )
            uow.write_intents.save(updated, current.version)
            append_delivery_transition(
                uow.workflow_events,
                current,
                updated,
                now,
                change_kind=DeliveryChangeKind.RECONCILIATION_REFRESH,
                actor_id=actor_id,
            )
            self._reduce_meeting(uow, updated.approval_id, now, actor_id=actor_id)
            uow.commit()
        return DeliveryResult(updated.id, updated.status)

    def _record_success(
        self,
        snapshot: WriteIntent,
        receipt: WriteReceipt,
        *,
        replayed: bool = False,
        reconciliation_owner: str | None = None,
        actor_id: str | None = None,
    ) -> DeliveryResult:
        validate_write_receipt(snapshot, receipt)
        now = self._now()
        with self._unit_of_work() as uow:
            current = _required_intent(uow.write_intents.get(snapshot.id))
            existing = uow.write_receipts.for_intent(current.id)
            if reconciliation_owner is not None and (
                current != snapshot
                or not _holds_reconciliation_lease(
                    current,
                    reconciliation_owner,
                    now,
                )
            ):
                return DeliveryResult(current.id, current.status)
            if existing is not None:
                validate_write_receipt(current, existing)
                return self._commit_success(
                    uow,
                    current,
                    replayed=True,
                    actor_id=actor_id,
                )
            if current != snapshot and not _same_binding(current, snapshot):
                return DeliveryResult(current.id, current.status)
            validate_write_receipt(current, receipt)
            if current.status not in {WriteStatus.IN_FLIGHT, WriteStatus.UNKNOWN}:
                return DeliveryResult(current.id, current.status)
            uow.write_receipts.add(receipt)
            return self._commit_success(
                uow,
                current,
                now=now,
                replayed=replayed,
                actor_id=actor_id,
            )

    def _load_intent(self, intent_id: UUID) -> WriteIntent:
        with self._unit_of_work() as uow:
            return _required_intent(uow.write_intents.get(intent_id))

    def _commit_success(
        self,
        uow: DeliveryUnitOfWork,
        current: WriteIntent,
        *,
        now: datetime | None = None,
        replayed: bool,
        actor_id: str | None = None,
    ) -> DeliveryResult:
        if current.status is WriteStatus.SUCCEEDED:
            return DeliveryResult(current.id, current.status, replayed=True)
        recorded_at = now or self._now()
        updated = transition_write_intent(current, WriteStatus.SUCCEEDED, recorded_at)
        uow.write_intents.save(updated, current.version)
        append_delivery_transition(
            uow.workflow_events,
            current,
            updated,
            recorded_at,
            actor_id=actor_id,
        )
        self._reduce_meeting(
            uow,
            updated.approval_id,
            recorded_at,
            actor_id=actor_id,
        )
        uow.commit()
        return DeliveryResult(updated.id, updated.status, replayed=replayed)

    def _reduce_meeting(
        self,
        uow: DeliveryUnitOfWork,
        approval_id: UUID,
        now: datetime,
        *,
        actor_id: str | None = None,
    ) -> None:
        approval = _required_approval(uow.approvals.get(approval_id))
        meeting = _required_meeting(uow.meetings.get(approval.meeting_id))
        intents = tuple(uow.write_intents.list_for_approval(approval.id))
        recap_ready = uow.recaps.for_approval(approval.id) is not None
        target = derive_filing_status(intents, recap_ready=recap_ready)
        if target is meeting.status:
            return
        failure = (
            _filing_failure(intents)
            if target
            in {
                MeetingStatus.PARTIALLY_FILED,
                MeetingStatus.FILING_FAILED,
            }
            else None
        )
        updated = transition_meeting(meeting, target, now, failure=failure)
        uow.meetings.save(updated, meeting.version)
        append_meeting_transition(
            uow.workflow_events,
            meeting,
            updated,
            now,
            actor_id=actor_id,
        )

    def _now(self) -> datetime:
        now = self._clock.now()
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("clock must return an aware datetime")
        return now


def _is_executable(
    uow: DeliveryUnitOfWork,
    intent: WriteIntent,
    now: datetime,
    worker_id: str | None = None,
) -> bool:
    if intent.status is not WriteStatus.IN_FLIGHT:
        return False
    if worker_id is not None and intent.lease_owner != worker_id:
        return False
    if intent.lease_expires_at is None or intent.lease_expires_at <= now:
        return False
    try:
        _require_approved(uow, intent)
    except ValueError:
        return False
    return True


def _holds_reconciliation_lease(
    intent: WriteIntent,
    owner: str,
    now: datetime,
) -> bool:
    return (
        intent.status is WriteStatus.UNKNOWN
        and intent.reconcile_lease_owner == owner
        and intent.reconcile_lease_expires_at is not None
        and intent.reconcile_lease_expires_at > now
    )


def _require_approved(uow: DeliveryUnitOfWork, intent: WriteIntent) -> None:
    approval = _required_approval(uow.approvals.get(intent.approval_id))
    meeting = _required_meeting(uow.meetings.get(intent.meeting_id))
    if (
        approval.meeting_id != intent.meeting_id
        or meeting.approved_review_id != approval.review_revision_id
        or meeting.status is not MeetingStatus.FILING
    ):
        raise ValueError(_INVALID_INTENT_MESSAGE)


def _failure(
    code: FailureCode,
    disposition: FailureDisposition,
    message: str,
    occurred_at: datetime,
    provider_request_id: str | None = None,
) -> WorkflowFailure:
    return WorkflowFailure(
        code=code,
        disposition=disposition,
        safe_message=message,
        provider_request_id=provider_request_id,
        occurred_at=occurred_at,
    )


def _filing_failure(intents: tuple[WriteIntent, ...]) -> WorkflowFailure:
    failures = sorted(
        (
            intent
            for intent in intents
            if intent.status is WriteStatus.PERMANENT_FAILED and intent.last_failure is not None
        ),
        key=lambda item: str(item.id),
    )
    if not failures:
        raise ValueError("A failed filing state requires a permanent intent failure")
    failure = failures[0].last_failure
    if failure is None:
        raise ValueError("A failed filing state requires a permanent intent failure")
    return failure


def _safe_request_id(value: str | None) -> str | None:
    if value is None or not value or len(value) > 200:
        return None
    if value != value.strip() or any(
        ord(character) < 32 or ord(character) == 127 for character in value
    ):
        return None
    return value


def _same_binding(left: WriteIntent, right: WriteIntent) -> bool:
    return (
        left.id == right.id
        and left.meeting_id == right.meeting_id
        and left.approval_id == right.approval_id
        and left.idempotency_key == right.idempotency_key
        and left.payload_digest == right.payload_digest
    )


def _required_intent(intent: WriteIntent | None) -> WriteIntent:
    if intent is None:
        raise ValueError("Write intent was not found")
    return intent


def _required_approval(approval: Approval | None) -> Approval:
    if approval is None:
        raise ValueError("Approval was not found")
    return approval


def _required_meeting(meeting: Meeting | None) -> Meeting:
    if meeting is None:
        raise ValueError("Meeting was not found")
    return meeting
