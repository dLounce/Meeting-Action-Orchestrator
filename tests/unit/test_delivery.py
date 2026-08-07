from __future__ import annotations

from collections.abc import Sequence
from datetime import date, datetime, timedelta, timezone
from threading import get_ident
from types import TracebackType
from uuid import UUID

import pytest

from meeting_action_orchestrator.application.delivery import (
    ApprovedOutboxExecutor,
    FullJitterRetryScheduler,
    PersistedApprovalAuthorizer,
)
from meeting_action_orchestrator.application.state_machine import transition_write_intent
from meeting_action_orchestrator.domain.enums import (
    DeadlineResolution,
    FailureCode,
    FailureDisposition,
    MeetingStatus,
    WriteStatus,
)
from meeting_action_orchestrator.domain.models import (
    Approval,
    ConnectorTarget,
    DateDeadline,
    Meeting,
    RecapArtifact,
    TaskProposal,
    WorkflowFailure,
    WriteIntent,
    WriteReceipt,
)
from meeting_action_orchestrator.infrastructure.mcp_gateway import (
    PermanentMcpError,
    RetryableMcpError,
    UnknownMcpOutcomeError,
)

NOW = datetime(2026, 8, 7, 12, 0, tzinfo=timezone.utc)
MEETING_ID = UUID(int=1)
APPROVAL_ID = UUID(int=2)
REVIEW_ID = UUID(int=3)


def uid(value: int) -> UUID:
    return UUID(int=value)


def failure(
    disposition: FailureDisposition,
    *,
    code: FailureCode = FailureCode.PROVIDER_UNAVAILABLE,
    message: str = "A safe failure",
) -> WorkflowFailure:
    return WorkflowFailure(
        code=code,
        disposition=disposition,
        safe_message=message,
        occurred_at=NOW - timedelta(minutes=1),
    )


def make_meeting(*, approved_review_id: UUID = REVIEW_ID) -> Meeting:
    return Meeting(
        id=MEETING_ID,
        ingest_key="upload-one",
        title="Release planning",
        audio_asset_id=uid(4),
        occurred_at=NOW - timedelta(hours=2),
        timezone="UTC",
        status=MeetingStatus.FILING,
        current_transcript_id=uid(5),
        current_review_id=REVIEW_ID,
        approved_review_id=approved_review_id,
        version=4,
        created_at=NOW - timedelta(hours=3),
        updated_at=NOW - timedelta(minutes=10),
    )


def make_approval() -> Approval:
    return Approval(
        id=APPROVAL_ID,
        meeting_id=MEETING_ID,
        review_revision_id=REVIEW_ID,
        review_digest="a" * 64,
        request_key="approval-one",
        actor_id="owner",
        approved_at=NOW - timedelta(minutes=10),
    )


def make_recap() -> RecapArtifact:
    return RecapArtifact(
        id=uid(6),
        meeting_id=MEETING_ID,
        approval_id=APPROVAL_ID,
        content="# Release planning",
        created_at=NOW - timedelta(minutes=10),
    )


def make_intent(
    value: int = 10,
    *,
    status: WriteStatus = WriteStatus.PENDING,
    attempt_count: int = 0,
    expired: bool = False,
) -> WriteIntent:
    proposal = TaskProposal(
        source_action_id=uid(100 + value),
        target=ConnectorTarget(connector_id="tasks", resource_id="inbox"),
        title=f"Publish release brief {value}",
        deadline=DateDeadline(
            value=date(2026, 8, 14),
            timezone="UTC",
            source_text="by August 14",
            resolution=DeadlineResolution.EXPLICIT,
        ),
    )
    updated_at = NOW - timedelta(minutes=5)
    lease_owner = None
    lease_expires_at = None
    next_attempt_at = None
    last_failure = None
    if status is WriteStatus.IN_FLIGHT:
        lease_owner = "worker-one"
        lease_expires_at = NOW - timedelta(seconds=1) if expired else NOW + timedelta(minutes=2)
    elif status is WriteStatus.RETRY_WAIT:
        next_attempt_at = NOW - timedelta(seconds=1)
        last_failure = failure(FailureDisposition.RETRYABLE)
    elif status is WriteStatus.UNKNOWN:
        last_failure = failure(
            FailureDisposition.UNKNOWN_OUTCOME,
            code=FailureCode.UNKNOWN_REMOTE_OUTCOME,
        )
    elif status is WriteStatus.PERMANENT_FAILED:
        last_failure = failure(FailureDisposition.PERMANENT)
    return WriteIntent(
        id=uid(value),
        meeting_id=MEETING_ID,
        approval_id=APPROVAL_ID,
        idempotency_key=f"mao_v1_{value:064x}",
        proposal=proposal,
        status=status,
        attempt_count=attempt_count,
        next_attempt_at=next_attempt_at,
        lease_owner=lease_owner,
        lease_expires_at=lease_expires_at,
        last_failure=last_failure,
        version=attempt_count,
        created_at=NOW - timedelta(minutes=10),
        updated_at=updated_at,
    )


def make_receipt(
    intent: WriteIntent,
    *,
    payload_digest: str | None = None,
    reconciled: bool = False,
) -> WriteReceipt:
    return WriteReceipt(
        id=uid(1000 + intent.id.int),
        intent_id=intent.id,
        idempotency_key=intent.idempotency_key,
        payload_digest=payload_digest or intent.payload_digest,
        provider="trusted-mcp",
        external_id=f"remote-{intent.id}",
        external_url="https://tasks.example.com/item",
        reconciled=reconciled,
        recorded_at=NOW,
    )


class MutableClock:
    def __init__(self, current: datetime = NOW) -> None:
        self.current = current

    def now(self) -> datetime:
        return self.current


class FixedScheduler:
    def __init__(self, delay: timedelta = timedelta(seconds=37)) -> None:
        self.delay = delay
        self.attempts: list[int] = []

    def next_attempt_at(self, now: datetime, attempt_count: int) -> datetime:
        self.attempts.append(attempt_count)
        return now + self.delay


class FakeDatabase:
    def __init__(self, *intents: WriteIntent) -> None:
        self.meetings = {MEETING_ID: make_meeting()}
        self.approvals = {APPROVAL_ID: make_approval()}
        self.recaps = {APPROVAL_ID: make_recap()}
        self.intents = {intent.id: intent for intent in intents}
        self.receipts: dict[UUID, WriteReceipt] = {}
        self.transaction_depth = 0
        self.commits = 0
        self.persistence_threads: set[int] = set()

    def unit_of_work(self) -> FakeUnitOfWork:
        return FakeUnitOfWork(self)


class FakeMeetingRepository:
    def __init__(self, database: FakeDatabase) -> None:
        self.database = database

    def get(self, meeting_id: UUID) -> Meeting | None:
        return self.database.meetings.get(meeting_id)

    def save(self, meeting: Meeting, expected_version: int) -> None:
        current = self.database.meetings[meeting.id]
        assert current.version == expected_version
        self.database.meetings[meeting.id] = meeting


class FakeApprovalRepository:
    def __init__(self, database: FakeDatabase) -> None:
        self.database = database

    def get(self, approval_id: UUID) -> Approval | None:
        return self.database.approvals.get(approval_id)


class FakeRecapRepository:
    def __init__(self, database: FakeDatabase) -> None:
        self.database = database

    def for_approval(self, approval_id: UUID) -> RecapArtifact | None:
        return self.database.recaps.get(approval_id)


class FakeIntentRepository:
    def __init__(self, database: FakeDatabase) -> None:
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

    def claim_due_ids(
        self,
        worker_id: str,
        now: datetime,
        lease_until: datetime,
        limit: int,
    ) -> Sequence[UUID]:
        candidates = sorted(self.database.intents.values(), key=lambda item: str(item.id))
        claimed: list[UUID] = []
        for intent in candidates:
            due = intent.status is WriteStatus.PENDING or (
                intent.status is WriteStatus.RETRY_WAIT
                and intent.next_attempt_at is not None
                and intent.next_attempt_at <= now
            )
            if not due or len(claimed) >= limit:
                continue
            updated = transition_write_intent(
                intent,
                WriteStatus.IN_FLIGHT,
                now,
                lease_owner=worker_id,
                lease_expires_at=lease_until,
            )
            self.database.intents[intent.id] = updated
            claimed.append(intent.id)
        return tuple(claimed)

    def recover_expired_ids(
        self,
        now: datetime,
        recovered_failure: WorkflowFailure,
        limit: int,
    ) -> Sequence[UUID]:
        candidates = sorted(self.database.intents.values(), key=lambda item: str(item.id))
        recovered: list[UUID] = []
        for intent in candidates:
            expired = (
                intent.status is WriteStatus.IN_FLIGHT
                and intent.lease_expires_at is not None
                and intent.lease_expires_at <= now
            )
            if not expired or len(recovered) >= limit:
                continue
            updated = transition_write_intent(
                intent,
                WriteStatus.UNKNOWN,
                now,
                failure=recovered_failure,
            )
            self.database.intents[intent.id] = updated
            recovered.append(intent.id)
        return tuple(recovered)

    def list_unknown_ids(self, limit: int) -> Sequence[UUID]:
        return tuple(
            intent.id
            for intent in sorted(self.database.intents.values(), key=lambda item: str(item.id))
            if intent.status is WriteStatus.UNKNOWN
        )[:limit]

    def save(self, intent: WriteIntent, expected_version: int) -> None:
        current = self.database.intents[intent.id]
        assert current.version == expected_version
        self.database.intents[intent.id] = intent


class FakeReceiptRepository:
    def __init__(self, database: FakeDatabase) -> None:
        self.database = database

    def add(self, receipt: WriteReceipt) -> None:
        assert receipt.intent_id not in self.database.receipts
        self.database.receipts[receipt.intent_id] = receipt

    def for_intent(self, intent_id: UUID) -> WriteReceipt | None:
        return self.database.receipts.get(intent_id)


class FakeUnitOfWork:
    def __init__(self, database: FakeDatabase) -> None:
        self.database = database
        self.meetings = FakeMeetingRepository(database)
        self.approvals = FakeApprovalRepository(database)
        self.recaps = FakeRecapRepository(database)
        self.write_intents = FakeIntentRepository(database)
        self.write_receipts = FakeReceiptRepository(database)

    def __enter__(self) -> FakeUnitOfWork:
        self.database.persistence_threads.add(get_ident())
        self.database.transaction_depth += 1
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        self.database.transaction_depth -= 1
        return False

    def commit(self) -> None:
        self.database.commits += 1


class FakeGateway:
    def __init__(
        self,
        database: FakeDatabase,
        *,
        writes: Sequence[WriteReceipt | Exception] = (),
        lookups: Sequence[WriteReceipt | None | Exception] = (),
    ) -> None:
        self.database = database
        self.writes = list(writes)
        self.lookups = list(lookups)
        self.calls: list[str] = []
        self.seen_intents: list[WriteIntent] = []

    async def ensure_task(self, intent: WriteIntent) -> WriteReceipt:
        return self._write("ensure_task", intent)

    async def ensure_event(self, intent: WriteIntent) -> WriteReceipt:
        return self._write("ensure_event", intent)

    async def find_task(self, idempotency_key: str) -> WriteReceipt | None:
        return self._lookup("find_task", idempotency_key)

    async def find_event(self, idempotency_key: str) -> WriteReceipt | None:
        return self._lookup("find_event", idempotency_key)

    def _write(self, operation: str, intent: WriteIntent) -> WriteReceipt:
        assert self.database.transaction_depth == 0
        self.calls.append(operation)
        self.seen_intents.append(intent)
        result = self.writes.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    def _lookup(self, operation: str, _idempotency_key: str) -> WriteReceipt | None:
        assert self.database.transaction_depth == 0
        self.calls.append(operation)
        result = self.lookups.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


class AdvancingGateway(FakeGateway):
    def __init__(self, database: FakeDatabase, clock: MutableClock) -> None:
        super().__init__(database)
        self.clock = clock
        self.provider_threads: list[int] = []
        self.lease_expirations: list[datetime] = []

    async def ensure_task(self, intent: WriteIntent) -> WriteReceipt:
        assert intent.lease_expires_at is not None
        self.calls.append("ensure_task")
        self.seen_intents.append(intent)
        self.provider_threads.append(get_ident())
        self.lease_expirations.append(intent.lease_expires_at)
        self.clock.current += timedelta(seconds=25)
        return make_receipt(intent)


def executor(
    database: FakeDatabase,
    gateway: FakeGateway,
    *,
    clock: MutableClock | None = None,
    scheduler: FixedScheduler | None = None,
) -> ApprovedOutboxExecutor:
    return ApprovedOutboxExecutor(
        unit_of_work=database.unit_of_work,
        gateway=gateway,
        clock=clock or MutableClock(),
        retry_scheduler=scheduler or FixedScheduler(),
        worker_id="worker-one",
    )


@pytest.mark.asyncio
async def test_claim_reloads_the_persisted_intent_and_writes_outside_the_transaction() -> None:
    pending = make_intent()
    database = FakeDatabase(pending)
    gateway = FakeGateway(database, writes=(make_receipt(pending),))

    batch = await executor(database, gateway).run_once()

    delivered = database.intents[pending.id]
    assert gateway.seen_intents[0].status is WriteStatus.IN_FLIGHT
    assert gateway.seen_intents[0].attempt_count == 1
    assert gateway.seen_intents[0].version == 1
    assert delivered.status is WriteStatus.SUCCEEDED
    assert database.meetings[MEETING_ID].status is MeetingStatus.COMPLETED
    assert batch.results[0].status is WriteStatus.SUCCEEDED


@pytest.mark.asyncio
async def test_delivery_claims_each_intent_with_a_fresh_lease() -> None:
    pending = tuple(make_intent(value) for value in range(10, 16))
    database = FakeDatabase(*pending)
    clock = MutableClock()
    gateway = AdvancingGateway(database, clock)
    event_loop_thread = get_ident()

    batch = await executor(database, gateway, clock=clock).run_once(limit=len(pending))

    assert [result.status for result in batch.results] == [WriteStatus.SUCCEEDED] * len(pending)
    assert gateway.lease_expirations == [
        NOW + timedelta(seconds=120 + 25 * index) for index in range(len(pending))
    ]
    assert gateway.provider_threads == [event_loop_thread] * len(pending)
    assert database.persistence_threads
    assert event_loop_thread not in database.persistence_threads


@pytest.mark.asyncio
async def test_existing_receipt_is_replayed_without_an_external_call() -> None:
    claimed = make_intent(status=WriteStatus.IN_FLIGHT, attempt_count=1)
    database = FakeDatabase(claimed)
    database.receipts[claimed.id] = make_receipt(claimed)
    gateway = FakeGateway(database)

    result = await executor(database, gateway).deliver_intent(claimed.id)

    assert result.replayed is True
    assert database.intents[claimed.id].status is WriteStatus.SUCCEEDED
    assert not gateway.calls


@pytest.mark.asyncio
async def test_retryable_failure_uses_full_jitter_schedule_and_sanitized_metadata() -> None:
    pending = make_intent()
    database = FakeDatabase(pending)
    error = RetryableMcpError(
        FailureCode.RATE_LIMITED,
        FailureDisposition.RETRYABLE,
        "Bearer private-token",
        "unsafe\nrequest-id",
    )
    gateway = FakeGateway(database, writes=(error,))
    scheduler = FixedScheduler()

    await executor(database, gateway, scheduler=scheduler).run_once()

    updated = database.intents[pending.id]
    assert updated.status is WriteStatus.RETRY_WAIT
    assert updated.next_attempt_at == NOW + timedelta(seconds=37)
    assert updated.last_failure is not None
    assert updated.last_failure.safe_message == "The connector is temporarily unavailable"
    assert updated.last_failure.provider_request_id is None
    assert "private-token" not in updated.model_dump_json()
    assert scheduler.attempts == [1]


@pytest.mark.asyncio
async def test_fifth_retryable_failure_becomes_permanent() -> None:
    retrying = make_intent(
        status=WriteStatus.RETRY_WAIT,
        attempt_count=4,
    )
    database = FakeDatabase(retrying)
    error = RetryableMcpError(
        FailureCode.PROVIDER_UNAVAILABLE,
        FailureDisposition.RETRYABLE,
        "temporary",
    )
    gateway = FakeGateway(database, writes=(error,))

    await executor(database, gateway).run_once()

    updated = database.intents[retrying.id]
    assert updated.attempt_count == 5
    assert updated.status is WriteStatus.PERMANENT_FAILED
    assert updated.last_failure is not None
    assert updated.last_failure.disposition is FailureDisposition.PERMANENT
    assert database.meetings[MEETING_ID].status is MeetingStatus.FILING_FAILED


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (
            PermanentMcpError(
                FailureCode.CONNECTOR_REJECTED,
                FailureDisposition.PERMANENT,
                "rejected",
            ),
            WriteStatus.PERMANENT_FAILED,
        ),
        (
            UnknownMcpOutcomeError(
                FailureCode.UNKNOWN_REMOTE_OUTCOME,
                FailureDisposition.UNKNOWN_OUTCOME,
                "possibly created",
            ),
            WriteStatus.UNKNOWN,
        ),
    ],
)
async def test_gateway_failures_enter_the_matching_delivery_state(
    error: Exception,
    expected: WriteStatus,
) -> None:
    pending = make_intent()
    database = FakeDatabase(pending)
    gateway = FakeGateway(database, writes=(error,))

    await executor(database, gateway).run_once()

    assert database.intents[pending.id].status is expected


@pytest.mark.asyncio
async def test_unknown_outcome_is_reconciled_before_another_create() -> None:
    unknown = make_intent(status=WriteStatus.UNKNOWN, attempt_count=1)
    database = FakeDatabase(unknown)
    gateway = FakeGateway(
        database,
        writes=(make_receipt(unknown),),
        lookups=(None,),
    )
    clock = MutableClock()
    scheduler = FixedScheduler()
    service = executor(database, gateway, clock=clock, scheduler=scheduler)

    first = await service.run_once()

    assert gateway.calls == ["find_task"]
    assert first.results[0].status is WriteStatus.RETRY_WAIT
    clock.current = NOW + timedelta(seconds=38)

    await service.run_once()

    assert gateway.calls == ["find_task", "ensure_task"]
    assert database.intents[unknown.id].status is WriteStatus.SUCCEEDED


@pytest.mark.asyncio
async def test_lookup_failure_keeps_an_unknown_intent_out_of_create_path() -> None:
    unknown = make_intent(status=WriteStatus.UNKNOWN, attempt_count=1)
    database = FakeDatabase(unknown)
    lookup_error = RetryableMcpError(
        FailureCode.PROVIDER_UNAVAILABLE,
        FailureDisposition.RETRYABLE,
        "secret provider failure",
    )
    gateway = FakeGateway(database, lookups=(lookup_error,))

    await executor(database, gateway).run_once()

    updated = database.intents[unknown.id]
    assert gateway.calls == ["find_task"]
    assert updated.status is WriteStatus.UNKNOWN
    assert updated.last_failure is not None
    assert updated.last_failure.safe_message == (
        "The connector outcome is unknown and requires reconciliation"
    )


@pytest.mark.asyncio
async def test_expired_in_flight_write_is_recovered_through_lookup() -> None:
    expired = make_intent(
        status=WriteStatus.IN_FLIGHT,
        attempt_count=1,
        expired=True,
    )
    database = FakeDatabase(expired)
    gateway = FakeGateway(database, lookups=(make_receipt(expired, reconciled=True),))

    batch = await executor(database, gateway).run_once()

    assert batch.recovered == (expired.id,)
    assert gateway.calls == ["find_task"]
    assert database.intents[expired.id].status is WriteStatus.SUCCEEDED
    assert database.receipts[expired.id].reconciled is True


@pytest.mark.asyncio
async def test_invalid_receipt_binding_is_a_permanent_failure() -> None:
    pending = make_intent()
    database = FakeDatabase(pending)
    gateway = FakeGateway(database, writes=(make_receipt(pending, payload_digest="f" * 64),))

    await executor(database, gateway).run_once()

    updated = database.intents[pending.id]
    assert updated.status is WriteStatus.PERMANENT_FAILED
    assert updated.last_failure is not None
    assert updated.last_failure.code is FailureCode.IDEMPOTENCY_CONFLICT


@pytest.mark.asyncio
async def test_filing_status_reduces_to_partial_when_only_some_writes_succeed() -> None:
    claimed = make_intent(status=WriteStatus.IN_FLIGHT, attempt_count=1)
    succeeded = make_intent(11, status=WriteStatus.SUCCEEDED, attempt_count=1)
    database = FakeDatabase(claimed, succeeded)
    gateway = FakeGateway(
        database,
        writes=(
            PermanentMcpError(
                FailureCode.CONNECTOR_REJECTED,
                FailureDisposition.PERMANENT,
                "rejected",
            ),
        ),
    )

    await executor(database, gateway).deliver_intent(claimed.id)

    meeting = database.meetings[MEETING_ID]
    assert meeting.status is MeetingStatus.PARTIALLY_FILED
    assert meeting.failure is not None
    assert meeting.failure.code is FailureCode.CONNECTOR_REJECTED


@pytest.mark.asyncio
async def test_persisted_authorizer_requires_the_exact_active_approved_snapshot() -> None:
    claimed = make_intent(status=WriteStatus.IN_FLIGHT, attempt_count=1)
    database = FakeDatabase(claimed)
    authorizer = PersistedApprovalAuthorizer(database.unit_of_work, MutableClock())

    assert await authorizer.permits(claimed) is True
    assert await authorizer.permits(claimed.model_copy(update={"version": 99})) is False
    database.meetings[MEETING_ID] = make_meeting(approved_review_id=uid(999))
    assert await authorizer.permits(claimed) is False
    assert database.transaction_depth == 0
    assert get_ident() not in database.persistence_threads


@pytest.mark.asyncio
async def test_persisted_authorizer_propagates_indeterminate_storage_failure() -> None:
    claimed = make_intent(status=WriteStatus.IN_FLIGHT, attempt_count=1)

    def unavailable_unit_of_work() -> FakeUnitOfWork:
        raise RuntimeError("database unavailable")

    authorizer = PersistedApprovalAuthorizer(unavailable_unit_of_work, MutableClock())

    with pytest.raises(RuntimeError, match="database unavailable"):
        await authorizer.permits(claimed)


def test_full_jitter_scheduler_uses_exponential_ceiling_and_positive_delay() -> None:
    scheduler = FullJitterRetryScheduler(
        base_delay=timedelta(seconds=2),
        maximum_delay=timedelta(seconds=10),
        random_value=lambda: 0.5,
    )
    zero_scheduler = FullJitterRetryScheduler(random_value=lambda: 0.0)

    assert scheduler.next_attempt_at(NOW, 4) == NOW + timedelta(seconds=5)
    assert zero_scheduler.next_attempt_at(NOW, 1) > NOW
