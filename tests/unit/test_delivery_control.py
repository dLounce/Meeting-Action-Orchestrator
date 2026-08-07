from __future__ import annotations

import asyncio
from collections.abc import Sequence
from datetime import date, datetime, timedelta, timezone
from threading import get_ident
from types import TracebackType
from uuid import UUID

import pytest

from meeting_action_orchestrator.application.delivery_control import DeliveryControlService
from meeting_action_orchestrator.application.errors import (
    OperationConflictError,
    ResourceNotFoundError,
)
from meeting_action_orchestrator.application.state_machine import transition_write_intent
from meeting_action_orchestrator.domain.enums import (
    DeadlineResolution,
    DeliveryOperationKind,
    DeliveryOperationStatus,
    FailureCode,
    FailureDisposition,
    MeetingStatus,
    WriteStatus,
)
from meeting_action_orchestrator.domain.errors import IdempotencyConflictError
from meeting_action_orchestrator.domain.models import (
    Approval,
    ConnectorTarget,
    DateDeadline,
    DeliveryOperationBinding,
    Meeting,
    TaskProposal,
    WorkflowFailure,
    WriteIntent,
    WriteReceipt,
)
from meeting_action_orchestrator.domain.workflow_events import (
    DeliveryTransitionMetadata,
    MeetingTransitionMetadata,
    WorkflowEventDraft,
)

NOW = datetime(2026, 8, 7, 14, 0, tzinfo=timezone.utc)
MEETING_ID = UUID(int=1)
APPROVAL_ID = UUID(int=2)
REVIEW_ID = UUID(int=3)


def uid(value: int) -> UUID:
    return UUID(int=value)


def make_failure(
    disposition: FailureDisposition,
    code: FailureCode = FailureCode.CONNECTOR_REJECTED,
) -> WorkflowFailure:
    return WorkflowFailure(
        code=code,
        disposition=disposition,
        safe_message="The connector rejected the approved action",
        occurred_at=NOW - timedelta(minutes=1),
    )


def make_meeting(status: MeetingStatus = MeetingStatus.FILING) -> Meeting:
    failed = status in {MeetingStatus.PARTIALLY_FILED, MeetingStatus.FILING_FAILED}
    return Meeting(
        id=MEETING_ID,
        ingest_key="upload-one",
        title="Release planning",
        audio_asset_id=uid(4),
        occurred_at=NOW - timedelta(hours=1),
        timezone="UTC",
        status=status,
        current_transcript_id=uid(5),
        current_review_id=REVIEW_ID,
        approved_review_id=REVIEW_ID,
        failure=make_failure(FailureDisposition.PERMANENT) if failed else None,
        version=4,
        created_at=NOW - timedelta(hours=2),
        updated_at=NOW - timedelta(minutes=2),
    )


def make_approval() -> Approval:
    return Approval(
        id=APPROVAL_ID,
        meeting_id=MEETING_ID,
        review_revision_id=REVIEW_ID,
        review_digest="a" * 64,
        request_key="approval-one",
        actor_id="owner",
        approved_at=NOW - timedelta(minutes=2),
    )


def make_intent(
    value: int,
    status: WriteStatus,
    *,
    attempt_count: int = 1,
    meeting_id: UUID = MEETING_ID,
    approval_id: UUID = APPROVAL_ID,
) -> WriteIntent:
    proposal = TaskProposal(
        source_action_id=uid(100 + value),
        target=ConnectorTarget(connector_id="tasks", resource_id="inbox"),
        title=f"Publish brief {value}",
        deadline=DateDeadline(
            value=date(2026, 8, 14),
            timezone="UTC",
            source_text="by August 14",
            resolution=DeadlineResolution.EXPLICIT,
        ),
    )
    last_failure = None
    if status is WriteStatus.UNKNOWN:
        last_failure = WorkflowFailure(
            code=FailureCode.UNKNOWN_REMOTE_OUTCOME,
            disposition=FailureDisposition.UNKNOWN_OUTCOME,
            safe_message="The connector outcome is unknown and requires reconciliation",
            occurred_at=NOW - timedelta(minutes=1),
        )
    elif status is WriteStatus.PERMANENT_FAILED:
        last_failure = make_failure(FailureDisposition.PERMANENT)
    return WriteIntent(
        id=uid(value),
        meeting_id=meeting_id,
        approval_id=approval_id,
        idempotency_key=f"mao_v1_{value:064x}",
        proposal=proposal,
        status=status,
        attempt_count=attempt_count,
        next_reconcile_at=NOW - timedelta(minutes=1) if status is WriteStatus.UNKNOWN else None,
        last_failure=last_failure,
        version=attempt_count,
        created_at=NOW - timedelta(minutes=3),
        updated_at=NOW - timedelta(minutes=1),
    )


def make_receipt(intent: WriteIntent) -> WriteReceipt:
    return WriteReceipt(
        id=uid(1000 + intent.id.int),
        intent_id=intent.id,
        idempotency_key=intent.idempotency_key,
        payload_digest=intent.payload_digest,
        provider="trusted-mcp",
        external_id=f"remote-{intent.id}",
        recorded_at=NOW,
    )


class Clock:
    def now(self) -> datetime:
        return NOW


class MutableClock:
    def __init__(self, current: datetime = NOW) -> None:
        self.current = current

    def now(self) -> datetime:
        return self.current


class Scheduler:
    def __init__(self) -> None:
        self.attempts: list[int] = []

    def next_attempt_at(self, now: datetime, attempt_count: int) -> datetime:
        self.attempts.append(attempt_count)
        return now + timedelta(seconds=30)


class Database:
    def __init__(
        self,
        *intents: WriteIntent,
        meeting: Meeting | None = None,
    ) -> None:
        self.meetings = {MEETING_ID: meeting or make_meeting()}
        self.approvals = {MEETING_ID: make_approval()}
        self.intents = {intent.id: intent for intent in intents}
        self.receipts: dict[UUID, WriteReceipt] = {}
        self.bindings: dict[str, DeliveryOperationBinding] = {}
        self.depth = 0
        self.commits = 0
        self.persistence_threads: set[int] = set()
        self.events: list[WorkflowEventDraft] = []

    def unit_of_work(self) -> UnitOfWork:
        return UnitOfWork(self)


def delivery_events(database: Database) -> tuple[DeliveryTransitionMetadata, ...]:
    return tuple(
        event.safe_metadata
        for event in database.events
        if isinstance(event.safe_metadata, DeliveryTransitionMetadata)
    )


def meeting_events(database: Database) -> tuple[MeetingTransitionMetadata, ...]:
    return tuple(
        event.safe_metadata
        for event in database.events
        if isinstance(event.safe_metadata, MeetingTransitionMetadata)
    )


class MeetingRepository:
    def __init__(self, database: Database) -> None:
        self.database = database

    def get(self, meeting_id: UUID) -> Meeting | None:
        return self.database.meetings.get(meeting_id)

    def save(self, meeting: Meeting, expected_version: int) -> None:
        assert self.database.meetings[meeting.id].version == expected_version
        self.database.meetings[meeting.id] = meeting


class ApprovalRepository:
    def __init__(self, database: Database) -> None:
        self.database = database

    def for_meeting(self, meeting_id: UUID) -> Approval | None:
        return self.database.approvals.get(meeting_id)


class IntentRepository:
    def __init__(self, database: Database) -> None:
        self.database = database

    def get(self, intent_id: UUID) -> WriteIntent | None:
        return self.database.intents.get(intent_id)

    def list_for_approval(self, approval_id: UUID) -> Sequence[WriteIntent]:
        return tuple(
            sorted(
                (
                    intent
                    for intent in self.database.intents.values()
                    if intent.approval_id == approval_id
                ),
                key=lambda item: str(item.id),
            )
        )

    def save(self, intent: WriteIntent, expected_version: int) -> None:
        assert self.database.intents[intent.id].version == expected_version
        self.database.intents[intent.id] = intent


class ReceiptRepository:
    def __init__(self, database: Database) -> None:
        self.database = database

    def for_intent(self, intent_id: UUID) -> WriteReceipt | None:
        return self.database.receipts.get(intent_id)


class EventRepository:
    def __init__(self, database: Database) -> None:
        self.database = database

    def append(self, draft: WorkflowEventDraft) -> object:
        self.database.events.append(draft)
        return draft


class DeliveryOperationRepository:
    def __init__(self, database: Database) -> None:
        self.database = database

    def add(self, binding: DeliveryOperationBinding) -> None:
        assert binding.request_key not in self.database.bindings
        self.database.bindings[binding.request_key] = binding

    def get(self, request_key: str) -> DeliveryOperationBinding | None:
        return self.database.bindings.get(request_key)

    def claim(
        self,
        request_key: str,
        owner: str,
        now: datetime,
        lease_until: datetime,
    ) -> DeliveryOperationBinding | None:
        binding = self.database.bindings[request_key]
        available = binding.status is DeliveryOperationStatus.PENDING or (
            binding.status is DeliveryOperationStatus.RUNNING
            and binding.lease_expires_at is not None
            and binding.lease_expires_at <= now
        )
        if not available:
            return None
        claimed = DeliveryOperationBinding.model_validate(
            binding.model_dump(mode="python")
            | {
                "status": DeliveryOperationStatus.RUNNING,
                "lease_owner": owner,
                "lease_expires_at": lease_until,
                "completed_at": None,
                "version": binding.version + 1,
                "updated_at": now,
            }
        )
        self.database.bindings[request_key] = claimed
        return claimed

    def release(
        self,
        request_key: str,
        owner: str,
        expected_version: int,
        now: datetime,
    ) -> bool:
        binding = self.database.bindings[request_key]
        if (
            binding.status is not DeliveryOperationStatus.RUNNING
            or binding.lease_owner != owner
            or binding.version != expected_version
        ):
            return False
        released = DeliveryOperationBinding.model_validate(
            binding.model_dump(mode="python")
            | {
                "status": DeliveryOperationStatus.PENDING,
                "lease_owner": None,
                "lease_expires_at": None,
                "version": binding.version + 1,
                "updated_at": now,
            }
        )
        self.database.bindings[request_key] = released
        return True

    def renew(
        self,
        request_key: str,
        owner: str,
        expected_version: int,
        now: datetime,
        lease_until: datetime,
    ) -> DeliveryOperationBinding | None:
        binding = self.database.bindings[request_key]
        if (
            binding.status is not DeliveryOperationStatus.RUNNING
            or binding.lease_owner != owner
            or binding.version != expected_version
            or binding.lease_expires_at is None
            or binding.lease_expires_at <= now
        ):
            return None
        renewed = DeliveryOperationBinding.model_validate(
            binding.model_dump(mode="python")
            | {
                "lease_expires_at": lease_until,
                "version": binding.version + 1,
                "updated_at": now,
            }
        )
        self.database.bindings[request_key] = renewed
        return renewed

    def complete(
        self,
        request_key: str,
        owner: str,
        expected_version: int,
        now: datetime,
    ) -> bool:
        binding = self.database.bindings[request_key]
        if (
            binding.status is not DeliveryOperationStatus.RUNNING
            or binding.lease_owner != owner
            or binding.version != expected_version
            or binding.lease_expires_at is None
            or binding.lease_expires_at <= now
        ):
            return False
        completed = DeliveryOperationBinding.model_validate(
            binding.model_dump(mode="python")
            | {
                "status": DeliveryOperationStatus.COMPLETED,
                "lease_owner": None,
                "lease_expires_at": None,
                "completed_at": now,
                "version": binding.version + 1,
                "updated_at": now,
            }
        )
        self.database.bindings[request_key] = completed
        return True


class UnitOfWork:
    def __init__(self, database: Database) -> None:
        self.database = database
        self.meetings = MeetingRepository(database)
        self.approvals = ApprovalRepository(database)
        self.write_intents = IntentRepository(database)
        self.write_receipts = ReceiptRepository(database)
        self.delivery_operations = DeliveryOperationRepository(database)
        self.workflow_events = EventRepository(database)

    def __enter__(self) -> UnitOfWork:
        self.database.persistence_threads.add(get_ident())
        self.database.depth += 1
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        self.database.depth -= 1
        return False

    def commit(self) -> None:
        self.database.commits += 1


class Reconciler:
    def __init__(self, database: Database, *, resolve: bool = True) -> None:
        self.database = database
        self.resolve = resolve
        self.calls: list[UUID] = []
        self.seen_statuses: list[WriteStatus] = []
        self.actor_ids: list[str | None] = []

    async def reconcile_intent(
        self,
        intent_id: UUID,
        actor_id: str | None = None,
    ) -> object:
        assert self.database.depth == 0
        self.calls.append(intent_id)
        self.actor_ids.append(actor_id)
        intent = self.database.intents[intent_id]
        self.seen_statuses.append(intent.status)
        if self.resolve:
            updated = transition_write_intent(intent, WriteStatus.SUCCEEDED, NOW)
            self.database.intents[intent_id] = updated
            self.database.receipts[intent_id] = make_receipt(intent)
        return object()


class CrashingReconciler(Reconciler):
    async def reconcile_intent(
        self,
        intent_id: UUID,
        actor_id: str | None = None,
    ) -> object:
        assert self.database.depth == 0
        self.calls.append(intent_id)
        self.actor_ids.append(actor_id)
        raise RuntimeError("connector stopped")


class BlockingReconciler(Reconciler):
    def __init__(self, database: Database) -> None:
        super().__init__(database, resolve=False)
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    async def reconcile_intent(
        self,
        intent_id: UUID,
        actor_id: str | None = None,
    ) -> object:
        assert self.database.depth == 0
        self.calls.append(intent_id)
        self.actor_ids.append(actor_id)
        self.entered.set()
        await self.release.wait()
        return object()


class AdvancingReconciler(Reconciler):
    def __init__(self, database: Database, clock: MutableClock) -> None:
        super().__init__(database, resolve=False)
        self.clock = clock

    async def reconcile_intent(
        self,
        intent_id: UUID,
        actor_id: str | None = None,
    ) -> object:
        result = await super().reconcile_intent(intent_id, actor_id)
        self.clock.current += timedelta(seconds=90)
        return result


def service(
    database: Database,
    reconciler: Reconciler | None = None,
    scheduler: Scheduler | None = None,
    clock: Clock | MutableClock | None = None,
    operation_lease_duration: timedelta = timedelta(minutes=2),
) -> tuple[DeliveryControlService, Reconciler, Scheduler]:
    connector = reconciler or Reconciler(database)
    retry_scheduler = scheduler or Scheduler()
    return (
        DeliveryControlService(
            unit_of_work=database.unit_of_work,
            reconciler=connector,
            clock=clock or Clock(),
            retry_scheduler=retry_scheduler,
            operation_lease_duration=operation_lease_duration,
        ),
        connector,
        retry_scheduler,
    )


@pytest.mark.asyncio
async def test_retry_queues_a_selected_permanent_failure_and_is_state_idempotent() -> None:
    failed = make_intent(10, WriteStatus.PERMANENT_FAILED, attempt_count=2)
    untouched = make_intent(11, WriteStatus.PENDING, attempt_count=0)
    database = Database(failed, untouched, meeting=make_meeting(MeetingStatus.FILING_FAILED))
    control, reconciler, scheduler = service(database)

    first = await control.retry(
        MEETING_ID,
        intent_ids=(failed.id,),
        request_key="retry-one",
        actor_id="owner",
    )
    version = database.intents[failed.id].version
    second = await control.retry(
        MEETING_ID,
        intent_ids=(failed.id,),
        request_key="retry-one",
        actor_id="owner",
    )

    updated = database.intents[failed.id]
    assert updated.status is WriteStatus.RETRY_WAIT
    assert updated.next_attempt_at == NOW + timedelta(seconds=30)
    assert updated.attempt_count == 2
    assert updated.version == version
    assert database.intents[untouched.id] == untouched
    assert database.meetings[MEETING_ID].status is MeetingStatus.FILING
    assert first.replayed is False
    assert second.replayed is True
    assert scheduler.attempts == [2]
    assert not reconciler.calls
    assert database.persistence_threads
    assert get_ident() not in database.persistence_threads
    assert [
        (event.previous_status, event.current_status) for event in delivery_events(database)
    ] == [(WriteStatus.PERMANENT_FAILED, WriteStatus.RETRY_WAIT)]
    assert [
        (event.previous_status, event.current_status) for event in meeting_events(database)
    ] == [(MeetingStatus.FILING_FAILED, MeetingStatus.FILING)]
    assert all(event.actor_id == "owner" for event in database.events)


@pytest.mark.asyncio
async def test_retry_reconciles_unknown_before_it_can_return_to_the_create_queue() -> None:
    unknown = make_intent(10, WriteStatus.UNKNOWN)
    database = Database(unknown)
    control, reconciler, _ = service(database)

    result = await control.retry(
        MEETING_ID,
        intent_ids=(unknown.id,),
        request_key="retry-one",
        actor_id="owner",
    )

    assert reconciler.calls == [unknown.id]
    assert reconciler.actor_ids == ["owner"]
    assert database.intents[unknown.id].status is WriteStatus.SUCCEEDED
    assert result.receipts == (database.receipts[unknown.id],)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "code",
    [
        FailureCode.IDEMPOTENCY_CONFLICT,
        FailureCode.INTERNAL,
        FailureCode.UNKNOWN_REMOTE_OUTCOME,
    ],
)
async def test_retry_routes_ambiguous_permanent_failures_through_reconciliation(
    code: FailureCode,
) -> None:
    failed = make_intent(10, WriteStatus.PERMANENT_FAILED, attempt_count=2).model_copy(
        update={"last_failure": make_failure(FailureDisposition.PERMANENT, code)}
    )
    database = Database(failed, meeting=make_meeting(MeetingStatus.FILING_FAILED))
    reconciler = Reconciler(database, resolve=False)
    control, _, scheduler = service(database, reconciler)

    result = await control.retry(
        MEETING_ID,
        intent_ids=(failed.id,),
        request_key=f"retry-{code.value}",
        actor_id="owner",
    )

    updated = database.intents[failed.id]
    assert reconciler.calls == [failed.id]
    assert reconciler.actor_ids == ["owner"]
    assert reconciler.seen_statuses == [WriteStatus.UNKNOWN]
    assert updated.status is WriteStatus.UNKNOWN
    assert updated.next_reconcile_at == NOW
    assert updated.attempt_count == failed.attempt_count
    assert result.intents[0].status is WriteStatus.UNKNOWN
    assert not scheduler.attempts
    assert [
        (event.previous_status, event.current_status) for event in delivery_events(database)
    ] == [(WriteStatus.PERMANENT_FAILED, WriteStatus.UNKNOWN)]
    assert [
        (event.previous_status, event.current_status) for event in meeting_events(database)
    ] == [(MeetingStatus.FILING_FAILED, MeetingStatus.FILING)]
    assert all(event.actor_id == "owner" for event in database.events)


@pytest.mark.asyncio
async def test_reconcile_calls_only_selected_unknown_intents() -> None:
    unknown = make_intent(10, WriteStatus.UNKNOWN)
    pending = make_intent(11, WriteStatus.PENDING, attempt_count=0)
    failed = make_intent(12, WriteStatus.PERMANENT_FAILED)
    database = Database(unknown, pending, failed)
    control, reconciler, _ = service(database)

    result = await control.reconcile(
        MEETING_ID,
        intent_ids=(pending.id, unknown.id),
        request_key="reconcile-one",
        actor_id="owner",
    )

    assert reconciler.calls == [unknown.id]
    assert reconciler.actor_ids == ["owner"]
    assert tuple(intent.id for intent in result.intents) == (unknown.id, pending.id, failed.id)
    assert database.intents[pending.id].status is WriteStatus.PENDING
    assert database.intents[failed.id].status is WriteStatus.PERMANENT_FAILED


@pytest.mark.asyncio
async def test_empty_selection_applies_to_every_unknown_intent() -> None:
    first = make_intent(10, WriteStatus.UNKNOWN)
    second = make_intent(11, WriteStatus.UNKNOWN)
    database = Database(first, second)
    control, reconciler, _ = service(database)

    await control.reconcile(
        MEETING_ID,
        intent_ids=(),
        request_key="reconcile-all",
        actor_id="owner",
    )

    assert reconciler.calls == [first.id, second.id]
    assert reconciler.actor_ids == ["owner", "owner"]


@pytest.mark.asyncio
async def test_complete_selection_is_validated_before_any_retry_is_applied() -> None:
    failed = make_intent(10, WriteStatus.PERMANENT_FAILED, attempt_count=2)
    foreign_id = uid(999)
    database = Database(failed, meeting=make_meeting(MeetingStatus.FILING_FAILED))
    control, reconciler, scheduler = service(database)

    with pytest.raises(ResourceNotFoundError):
        await control.retry(
            MEETING_ID,
            intent_ids=(failed.id, foreign_id),
            request_key="retry-one",
            actor_id="owner",
        )

    assert database.intents[failed.id] == failed
    assert not database.bindings
    assert not reconciler.calls
    assert not scheduler.attempts


@pytest.mark.asyncio
async def test_exhausted_failure_is_an_idempotent_noop() -> None:
    exhausted = make_intent(10, WriteStatus.PERMANENT_FAILED, attempt_count=5)
    database = Database(exhausted, meeting=make_meeting(MeetingStatus.FILING_FAILED))
    control, _, scheduler = service(database)

    result = await control.retry(
        MEETING_ID,
        intent_ids=(exhausted.id,),
        request_key="retry-one",
        actor_id="owner",
    )

    assert database.intents[exhausted.id] == exhausted
    assert result.replayed is False
    assert not scheduler.attempts
    assert not database.events


@pytest.mark.asyncio
async def test_duplicate_selection_is_rejected_without_reconciliation() -> None:
    unknown = make_intent(10, WriteStatus.UNKNOWN)
    database = Database(unknown)
    control, reconciler, _ = service(database)

    with pytest.raises(OperationConflictError):
        await control.reconcile(
            MEETING_ID,
            intent_ids=(unknown.id, unknown.id),
            request_key="reconcile-one",
            actor_id="owner",
        )

    assert not reconciler.calls


@pytest.mark.asyncio
async def test_corrupt_approval_binding_is_rejected() -> None:
    foreign = make_intent(
        10,
        WriteStatus.UNKNOWN,
        meeting_id=uid(999),
    )
    database = Database(foreign)
    control, reconciler, _ = service(database)

    with pytest.raises(OperationConflictError):
        await control.reconcile(
            MEETING_ID,
            intent_ids=(),
            request_key="reconcile-one",
            actor_id="owner",
        )

    assert not reconciler.calls


@pytest.mark.asyncio
async def test_request_binding_resumes_after_connector_crash() -> None:
    unknown = make_intent(10, WriteStatus.UNKNOWN)
    database = Database(unknown)
    crashing = CrashingReconciler(database)
    control, _, _ = service(database, crashing)

    with pytest.raises(RuntimeError, match="connector stopped"):
        await control.reconcile(
            MEETING_ID,
            intent_ids=(unknown.id,),
            request_key="reconcile-crash",
            actor_id="owner",
        )

    binding = database.bindings["reconcile-crash"]
    assert binding.operation is DeliveryOperationKind.RECONCILE
    assert binding.meeting_id == MEETING_ID
    assert binding.status is DeliveryOperationStatus.PENDING
    assert binding.lease_owner is None
    resumed, reconciler, _ = service(database)

    result = await resumed.reconcile(
        MEETING_ID,
        intent_ids=(unknown.id,),
        request_key="reconcile-crash",
        actor_id="owner",
    )

    assert reconciler.calls == [unknown.id]
    assert result.intents[0].status is WriteStatus.SUCCEEDED
    assert database.bindings["reconcile-crash"].status is DeliveryOperationStatus.COMPLETED


@pytest.mark.asyncio
async def test_completed_request_replays_the_current_snapshot_without_reconciliation() -> None:
    unknown = make_intent(10, WriteStatus.UNKNOWN)
    database = Database(unknown)
    reconciler = Reconciler(database, resolve=False)
    control, _, _ = service(database, reconciler)

    first = await control.reconcile(
        MEETING_ID,
        intent_ids=(unknown.id,),
        request_key="reconcile-completed",
        actor_id="owner",
    )
    scheduled = WriteIntent.model_validate(
        database.intents[unknown.id].model_dump(mode="python")
        | {
            "next_reconcile_at": NOW + timedelta(minutes=1),
            "reconcile_attempt_count": 1,
            "version": unknown.version + 1,
            "updated_at": NOW,
        }
    )
    database.intents[unknown.id] = scheduled

    replay = await control.reconcile(
        MEETING_ID,
        intent_ids=(unknown.id,),
        request_key="reconcile-completed",
        actor_id="owner",
    )

    assert first.replayed is False
    assert replay.replayed is True
    assert replay.intents == (scheduled,)
    assert reconciler.calls == [unknown.id]


@pytest.mark.asyncio
async def test_live_duplicate_request_cannot_share_operation_execution() -> None:
    unknown = make_intent(10, WriteStatus.UNKNOWN)
    database = Database(unknown)
    reconciler = BlockingReconciler(database)
    control, _, _ = service(database, reconciler)
    first = asyncio.create_task(
        control.reconcile(
            MEETING_ID,
            intent_ids=(unknown.id,),
            request_key="reconcile-concurrent",
            actor_id="owner",
        )
    )
    await reconciler.entered.wait()

    with pytest.raises(OperationConflictError, match="already in progress"):
        await control.reconcile(
            MEETING_ID,
            intent_ids=(unknown.id,),
            request_key="reconcile-concurrent",
            actor_id="owner",
        )

    reconciler.release.set()
    result = await first
    assert result.replayed is False
    assert reconciler.calls == [unknown.id]
    assert database.bindings["reconcile-concurrent"].status is DeliveryOperationStatus.COMPLETED


@pytest.mark.asyncio
async def test_operation_lease_is_renewed_across_a_long_multi_intent_command() -> None:
    intents = tuple(make_intent(value, WriteStatus.UNKNOWN) for value in range(10, 13))
    database = Database(*intents)
    clock = MutableClock()
    reconciler = AdvancingReconciler(database, clock)
    control, _, _ = service(
        database,
        reconciler,
        clock=clock,
        operation_lease_duration=timedelta(minutes=2),
    )

    result = await control.reconcile(
        MEETING_ID,
        intent_ids=tuple(intent.id for intent in intents),
        request_key="reconcile-long-running",
        actor_id="owner",
    )

    binding = database.bindings["reconcile-long-running"]
    assert result.replayed is False
    assert reconciler.calls == [intent.id for intent in intents]
    assert clock.current == NOW + timedelta(seconds=270)
    assert binding.status is DeliveryOperationStatus.COMPLETED
    assert binding.version == 5


@pytest.mark.asyncio
async def test_request_key_conflict_does_not_mutate_another_selection() -> None:
    first = make_intent(10, WriteStatus.UNKNOWN)
    second = make_intent(11, WriteStatus.UNKNOWN)
    database = Database(first, second)
    reconciler = Reconciler(database, resolve=False)
    control, _, _ = service(database, reconciler)
    await control.reconcile(
        MEETING_ID,
        intent_ids=(first.id,),
        request_key="shared-key",
        actor_id="owner",
    )

    with pytest.raises(IdempotencyConflictError):
        await control.reconcile(
            MEETING_ID,
            intent_ids=(second.id,),
            request_key="shared-key",
            actor_id="owner",
        )

    assert reconciler.calls == [first.id]
    assert database.intents[first.id] == first
    assert database.intents[second.id] == second


@pytest.mark.asyncio
async def test_request_key_binds_operation_and_actor() -> None:
    unknown = make_intent(10, WriteStatus.UNKNOWN)
    database = Database(unknown)
    reconciler = Reconciler(database, resolve=False)
    control, _, _ = service(database, reconciler)
    await control.retry(
        MEETING_ID,
        intent_ids=(unknown.id,),
        request_key="bound-key",
        actor_id="owner",
    )

    with pytest.raises(IdempotencyConflictError):
        await control.reconcile(
            MEETING_ID,
            intent_ids=(unknown.id,),
            request_key="bound-key",
            actor_id="owner",
        )
    with pytest.raises(IdempotencyConflictError):
        await control.retry(
            MEETING_ID,
            intent_ids=(unknown.id,),
            request_key="bound-key",
            actor_id="another-owner",
        )

    assert reconciler.calls == [unknown.id]


@pytest.mark.asyncio
async def test_selection_fingerprint_is_order_independent() -> None:
    first = make_intent(10, WriteStatus.PENDING, attempt_count=0)
    second = make_intent(11, WriteStatus.PENDING, attempt_count=0)
    database = Database(first, second)
    control, reconciler, _ = service(database)

    await control.reconcile(
        MEETING_ID,
        intent_ids=(first.id, second.id),
        request_key="canonical-selection",
        actor_id="owner",
    )
    replay = await control.reconcile(
        MEETING_ID,
        intent_ids=(second.id, first.id),
        request_key="canonical-selection",
        actor_id="owner",
    )

    assert replay.replayed is True
    assert not reconciler.calls
    assert len(database.bindings) == 1


@pytest.mark.asyncio
async def test_request_key_is_global_across_meetings() -> None:
    first = make_intent(10, WriteStatus.UNKNOWN)
    database = Database(first)
    other_meeting_id = uid(20)
    other_approval_id = uid(21)
    other_meeting = Meeting.model_validate(
        make_meeting().model_dump(mode="python")
        | {"id": other_meeting_id, "ingest_key": "upload-two"}
    )
    other_approval = Approval.model_validate(
        make_approval().model_dump(mode="python")
        | {
            "id": other_approval_id,
            "meeting_id": other_meeting_id,
            "request_key": "approval-two",
        }
    )
    second = make_intent(
        11,
        WriteStatus.UNKNOWN,
        meeting_id=other_meeting_id,
        approval_id=other_approval_id,
    )
    database.meetings[other_meeting_id] = other_meeting
    database.approvals[other_meeting_id] = other_approval
    database.intents[second.id] = second
    reconciler = Reconciler(database, resolve=False)
    control, _, _ = service(database, reconciler)
    await control.reconcile(
        MEETING_ID,
        intent_ids=(first.id,),
        request_key="global-key",
        actor_id="owner",
    )

    with pytest.raises(IdempotencyConflictError):
        await control.reconcile(
            other_meeting_id,
            intent_ids=(second.id,),
            request_key="global-key",
            actor_id="owner",
        )

    assert reconciler.calls == [first.id]
    assert database.intents[second.id] == second
