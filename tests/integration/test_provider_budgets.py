from __future__ import annotations

import asyncio
import sqlite3
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from meeting_action_orchestrator.application.errors import (
    PersistenceIntegrityError,
    ProviderBudgetExhaustedError,
    ProviderBudgetIntegrityError,
    ProviderBudgetLeaseLostError,
)
from meeting_action_orchestrator.application.processing import ProcessingScheduler
from meeting_action_orchestrator.application.provider_budget import ProviderBudgetService
from meeting_action_orchestrator.domain.enums import (
    AudioMediaType,
    FailureCode,
    FailureDisposition,
    ProcessingJobStatus,
    ProcessingStage,
    ProviderCallRole,
    ProviderOperation,
    ProviderSettlementOutcome,
    ProviderUsageKind,
)
from meeting_action_orchestrator.domain.models import (
    AudioAsset,
    Meeting,
    ProcessingJob,
    WorkflowFailure,
)
from meeting_action_orchestrator.domain.provider_budget import (
    DEFAULT_PROVIDER_BUDGET_LIMITS,
    ProviderBudgetAccount,
    ProviderBudgetLimits,
    ProviderBudgetReservation,
    ProviderBudgetReservationRequest,
    ProviderDispatchContext,
    ProviderUsage,
    provider_dispatch_digest,
    provider_reservation_fingerprint,
)
from meeting_action_orchestrator.infrastructure.database import Database
from meeting_action_orchestrator.infrastructure.repositories import (
    SqliteProviderBudgetAccountRepository,
    SqliteUnitOfWork,
)

NOW = datetime(2026, 8, 7, 9, 0, tzinfo=timezone.utc)
AUDIO_ID = UUID("10000000-0000-4000-8000-000000000001")
MEETING_ID = UUID("20000000-0000-4000-8000-000000000001")
JOB_ID = UUID("30000000-0000-4000-8000-000000000001")


@dataclass
class MutableClock:
    current: datetime = NOW

    def now(self) -> datetime:
        return self.current


class AdvancingUnitOfWork(SqliteUnitOfWork):
    def __init__(self, database: Database, clock: MutableClock) -> None:
        super().__init__(database)
        self._clock = clock

    def __enter__(self) -> SqliteUnitOfWork:
        entered = super().__enter__()
        self._clock.current += timedelta(minutes=5)
        return entered


async def _inline_to_thread(
    call: Callable[..., object], /, *args: object, **kwargs: object
) -> object:
    return call(*args, **kwargs)


@pytest.fixture
def inline_budget_threads(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(asyncio, "to_thread", _inline_to_thread)


def create_database(path: Path) -> Database:
    database = Database(path)
    assert database.migrate() == 11
    with SqliteUnitOfWork(database) as uow:
        uow.audio_assets.add(
            AudioAsset(
                id=AUDIO_ID,
                storage_key="recording.wav",
                original_name="recording.wav",
                detected_media_type=AudioMediaType.WAV,
                size_bytes=1_024,
                duration_ms=60_000,
                sha256="a" * 64,
                created_at=NOW,
            )
        )
        uow.meetings.add(
            Meeting(
                id=MEETING_ID,
                ingest_key="provider-budget-test",
                title="Provider budget test",
                audio_asset_id=AUDIO_ID,
                occurred_at=NOW,
                timezone="UTC",
                created_at=NOW,
                updated_at=NOW,
            )
        )
        uow.commit()
    return database


def budget_limits(
    stage: ProcessingStage,
    limits: ProviderBudgetLimits,
) -> dict[ProcessingStage, ProviderBudgetLimits]:
    configured = dict(DEFAULT_PROVIDER_BUDGET_LIMITS)
    configured[stage] = limits
    return configured


def schedule_and_claim(
    database: Database,
    clock: MutableClock,
    stage: ProcessingStage = ProcessingStage.EXTRACTION,
    *,
    limits: dict[ProcessingStage, ProviderBudgetLimits] | None = None,
) -> ProcessingJob:
    scheduler = ProcessingScheduler(
        unit_of_work=lambda: SqliteUnitOfWork(database),
        clock=clock,
        id_factory=lambda: JOB_ID,
        budget_limits=limits,
        budget_policy_version=7,
    )
    scheduler.enqueue(MEETING_ID, stage)
    with SqliteUnitOfWork(database) as uow:
        claimed = uow.processing_jobs.claim_due(
            stage,
            "budget-worker",
            clock.now(),
            clock.now() + timedelta(minutes=5),
            1,
        )
        uow.commit()
    return claimed[0]


def dispatch(job: ProcessingJob) -> ProviderDispatchContext:
    assert job.lease_owner is not None
    assert job.claim_token is not None
    return ProviderDispatchContext(
        processing_job_id=job.id,
        attempt_number=job.attempt_count,
        lease_owner=job.lease_owner,
        claim_token=job.claim_token,
    )


def response_request(
    key: str,
    *,
    input_tokens: int = 100,
    output_tokens: int = 50,
    digest: str = "b" * 64,
) -> ProviderBudgetReservationRequest:
    return ProviderBudgetReservationRequest(
        dispatch_key=key,
        operation_digest=digest,
        operation=ProviderOperation.RESPONSES_CREATE,
        role=ProviderCallRole.EXTRACT,
        model="gpt-test",
        reserved_input_tokens=input_tokens,
        reserved_output_tokens=output_tokens,
    )


def service(database: Database, clock: MutableClock) -> ProviderBudgetService:
    return ProviderBudgetService(
        unit_of_work=lambda: SqliteUnitOfWork(database),
        clock=clock,
    )


def test_scheduler_snapshots_account_atomically_and_replays(tmp_path: Path) -> None:
    database = create_database(tmp_path / "application.sqlite3")
    clock = MutableClock()
    scheduler = ProcessingScheduler(
        unit_of_work=lambda: SqliteUnitOfWork(database),
        clock=clock,
        id_factory=lambda: JOB_ID,
        budget_policy_version=7,
    )

    first = scheduler.enqueue(MEETING_ID, ProcessingStage.EXTRACTION)
    replay = scheduler.enqueue(MEETING_ID, ProcessingStage.EXTRACTION)

    with SqliteUnitOfWork(database) as uow:
        account = uow.provider_budget_accounts.get(first.id)
    assert replay == first
    assert account is not None
    assert account.policy_version == 7
    assert account.limits == DEFAULT_PROVIDER_BUDGET_LIMITS[ProcessingStage.EXTRACTION]
    with pytest.raises(ValueError, match="cover every processing stage"):
        ProcessingScheduler(
            unit_of_work=lambda: SqliteUnitOfWork(database),
            clock=clock,
            budget_limits={},
        )


class RejectingAccountRepository(SqliteProviderBudgetAccountRepository):
    def add(self, _account: ProviderBudgetAccount) -> None:
        raise RuntimeError("account insert failed")


class RejectingAccountUnitOfWork(SqliteUnitOfWork):
    def __enter__(self) -> RejectingAccountUnitOfWork:
        super().__enter__()
        if self._connection is None:
            raise RuntimeError("unit of work is not active")
        self.provider_budget_accounts = RejectingAccountRepository(self._connection)
        return self


def test_account_failure_rolls_back_processing_job(tmp_path: Path) -> None:
    database = create_database(tmp_path / "application.sqlite3")
    scheduler = ProcessingScheduler(
        unit_of_work=lambda: RejectingAccountUnitOfWork(database),
        clock=MutableClock(),
        id_factory=lambda: JOB_ID,
    )

    with pytest.raises(RuntimeError, match="account insert failed"):
        scheduler.enqueue(MEETING_ID, ProcessingStage.TRANSCRIPTION)

    with SqliteUnitOfWork(database) as uow:
        assert uow.processing_jobs.get(JOB_ID) is None
        assert uow.provider_budget_accounts.get(JOB_ID) is None


def test_existing_job_without_account_fails_closed(tmp_path: Path) -> None:
    database = create_database(tmp_path / "application.sqlite3")
    clock = MutableClock()
    with SqliteUnitOfWork(database) as uow:
        uow.processing_jobs.add(
            ProcessingJob(
                id=JOB_ID,
                meeting_id=MEETING_ID,
                stage=ProcessingStage.EXTRACTION,
                max_attempts=2,
                created_at=NOW,
                updated_at=NOW,
            )
        )
        uow.commit()
    scheduler = ProcessingScheduler(
        unit_of_work=lambda: SqliteUnitOfWork(database),
        clock=clock,
    )

    with pytest.raises(ProviderBudgetIntegrityError):
        scheduler.enqueue(MEETING_ID, ProcessingStage.EXTRACTION)


async def test_reserve_settle_and_exact_replay(
    tmp_path: Path,
    inline_budget_threads: None,
) -> None:
    database = create_database(tmp_path / "application.sqlite3")
    clock = MutableClock()
    job = schedule_and_claim(database, clock)
    assert job.claim_token is not None
    controller = service(database, clock)
    request = response_request("dispatch-one")

    reservation = await controller.reserve(dispatch(job), request)
    replay = await controller.reserve(dispatch(job), request)
    settlement = await controller.settle(
        reservation.id,
        outcome=ProviderSettlementOutcome.SUCCEEDED,
        usage=ProviderUsage(
            kind=ProviderUsageKind.TOKENS,
            input_tokens=70,
            output_tokens=20,
        ),
    )
    settlement_replay = await controller.settle(
        reservation.id,
        outcome=ProviderSettlementOutcome.SUCCEEDED,
        usage=settlement.usage,
    )

    with SqliteUnitOfWork(database) as uow:
        usage = uow.provider_budget_reservations.usage_for_job(job.id)
    assert replay == reservation
    assert settlement_replay == settlement
    assert usage.provider_requests == 1
    assert usage.input_tokens == 70
    assert usage.output_tokens == 20
    with pytest.raises(ProviderBudgetIntegrityError):
        await controller.reserve(
            dispatch(job),
            response_request("dispatch-one", digest="c" * 64),
        )


class CommitThenRaiseUnitOfWork(SqliteUnitOfWork):
    def commit(self) -> None:
        super().commit()
        raise RuntimeError("commit outcome was ambiguous")


class AmbiguousCommitFactory:
    def __init__(self, database: Database) -> None:
        self._database = database
        self.calls = 0

    def __call__(self) -> SqliteUnitOfWork:
        self.calls += 1
        if self.calls == 1:
            return CommitThenRaiseUnitOfWork(self._database)
        return SqliteUnitOfWork(self._database)


async def test_ambiguous_reservation_and_settlement_commits_reconcile(
    tmp_path: Path,
    inline_budget_threads: None,
) -> None:
    database = create_database(tmp_path / "application.sqlite3")
    clock = MutableClock()
    job = schedule_and_claim(database, clock)
    reserve_factory = AmbiguousCommitFactory(database)
    reserve_service = ProviderBudgetService(
        unit_of_work=reserve_factory,
        clock=clock,
        id_factory=lambda: UUID("40000000-0000-4000-8000-000000000001"),
    )

    reservation = await reserve_service.reserve(
        dispatch(job), response_request("ambiguous-reserve")
    )
    settle_factory = AmbiguousCommitFactory(database)
    settle_service = ProviderBudgetService(
        unit_of_work=settle_factory,
        clock=clock,
    )
    settlement = await settle_service.settle(
        reservation.id,
        outcome=ProviderSettlementOutcome.FAILED,
        usage=ProviderUsage(kind=ProviderUsageKind.NONE),
    )

    assert reserve_factory.calls == 2
    assert settle_factory.calls == 2
    assert settlement.reservation_id == reservation.id
    with SqliteUnitOfWork(database) as uow:
        assert len(uow.provider_budget_reservations.list_for_job(job.id)) == 1
        assert uow.provider_budget_settlements.get(reservation.id) == settlement


async def test_corrupt_reservation_is_detached_as_budget_integrity_failure(
    tmp_path: Path,
    inline_budget_threads: None,
) -> None:
    database = create_database(tmp_path / "application.sqlite3")
    clock = MutableClock()
    job = schedule_and_claim(database, clock)
    corrupt = reservation_for(job, "corrupt", sequence=1, output_tokens=1)
    values = list(reservation_values(corrupt))
    values[7] = "0" * 64
    with database.transaction(immediate=True) as connection:
        connection.execute(reservation_insert_sql(), values)

    with (
        SqliteUnitOfWork(database) as uow,
        pytest.raises(PersistenceIntegrityError) as persisted,
    ):
        uow.provider_budget_reservations.get(corrupt.id)
    assert persisted.value.__cause__ is None
    assert persisted.value.__context__ is None
    assert "0" * 64 not in str(persisted.value)

    controller = service(database, clock)
    with pytest.raises(ProviderBudgetIntegrityError) as reserve_error:
        await controller.reserve(dispatch(job), response_request("corrupt", output_tokens=1))
    with pytest.raises(ProviderBudgetIntegrityError) as settle_error:
        await controller.settle(
            corrupt.id,
            outcome=ProviderSettlementOutcome.FAILED,
            usage=ProviderUsage(kind=ProviderUsageKind.NONE),
        )
    for error in (reserve_error.value, settle_error.value):
        assert str(error) == "Provider budget accounting failed an integrity check"
        assert error.__cause__ is None
        assert error.__context__ is None
        assert "0" * 64 not in str(error)


def test_concurrent_exact_replay_is_single_reservation(tmp_path: Path) -> None:
    database = create_database(tmp_path / "application.sqlite3")
    clock = MutableClock()
    job = schedule_and_claim(database, clock)
    controller = service(database, clock)
    context = dispatch(job)
    request = response_request("concurrent-dispatch")

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = tuple(executor.map(lambda _: controller._reserve(context, request), range(2)))

    assert results[0] == results[1]
    with SqliteUnitOfWork(database) as uow:
        assert len(uow.provider_budget_reservations.list_for_job(job.id)) == 1


def test_concurrent_distinct_dispatches_serialize_against_lifetime_cap(
    tmp_path: Path,
) -> None:
    database = create_database(tmp_path / "application.sqlite3")
    clock = MutableClock()
    limits = budget_limits(
        ProcessingStage.EXTRACTION,
        ProviderBudgetLimits(
            preflight_request_limit=1,
            provider_request_limit=1,
            input_token_limit=1_000,
            output_token_limit=1_000,
        ),
    )
    job = schedule_and_claim(database, clock, limits=limits)
    controller = service(database, clock)

    def reserve(key: str) -> object:
        try:
            return controller._reserve(dispatch(job), response_request(key))
        except ProviderBudgetExhaustedError as error:
            return error

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = tuple(executor.map(reserve, ("first", "second")))

    assert sum(isinstance(result, ProviderBudgetExhaustedError) for result in results) == 1
    with SqliteUnitOfWork(database) as uow:
        assert len(uow.provider_budget_reservations.list_for_job(job.id)) == 1


def test_unsettled_and_failed_calls_remain_fully_charged(tmp_path: Path) -> None:
    database = create_database(tmp_path / "application.sqlite3")
    clock = MutableClock()
    limits = budget_limits(
        ProcessingStage.EXTRACTION,
        ProviderBudgetLimits(
            preflight_request_limit=2,
            provider_request_limit=2,
            input_token_limit=150,
            output_token_limit=100,
        ),
    )
    job = schedule_and_claim(database, clock, limits=limits)
    controller = service(database, clock)
    first = controller._reserve(dispatch(job), response_request("unresolved", output_tokens=50))

    with pytest.raises(ProviderBudgetExhaustedError):
        controller._reserve(
            dispatch(job),
            response_request("blocked", input_tokens=51, output_tokens=50),
        )
    controller._settle(
        first.id,
        outcome=ProviderSettlementOutcome.FAILED,
        usage=ProviderUsage(kind=ProviderUsageKind.NONE),
    )
    with SqliteUnitOfWork(database) as uow:
        usage = uow.provider_budget_reservations.usage_for_job(job.id)
    assert usage.input_tokens == 100
    assert usage.output_tokens == 50


def test_reservation_requires_the_current_unexpired_lease(tmp_path: Path) -> None:
    database = create_database(tmp_path / "application.sqlite3")
    clock = MutableClock()
    job = schedule_and_claim(database, clock)
    controller = service(database, clock)

    with pytest.raises(ProviderBudgetLeaseLostError):
        controller._reserve(
            ProviderDispatchContext(
                processing_job_id=job.id,
                attempt_number=job.attempt_count,
                lease_owner="another-worker",
                claim_token=job.claim_token,
            ),
            response_request("wrong-owner"),
        )
    clock.current += timedelta(minutes=5)
    with pytest.raises(ProviderBudgetLeaseLostError):
        controller._reserve(dispatch(job), response_request("expired-lease"))
    with SqliteUnitOfWork(database) as uow:
        assert uow.provider_budget_reservations.list_for_job(job.id) == ()


def test_reservation_rechecks_time_after_transaction_acquisition(tmp_path: Path) -> None:
    database = create_database(tmp_path / "application.sqlite3")
    clock = MutableClock()
    job = schedule_and_claim(database, clock)
    controller = ProviderBudgetService(
        unit_of_work=lambda: AdvancingUnitOfWork(database, clock),
        clock=clock,
    )

    with pytest.raises(ProviderBudgetLeaseLostError):
        controller._reserve(dispatch(job), response_request("expired-during-lock-wait"))

    with SqliteUnitOfWork(database) as uow:
        assert uow.provider_budget_reservations.list_for_job(job.id) == ()


def test_manual_retry_same_attempt_and_owner_rejects_stale_claim_token(tmp_path: Path) -> None:
    database = create_database(tmp_path / "application.sqlite3")
    clock = MutableClock()
    first = schedule_and_claim(database, clock)
    stale_context = dispatch(first)
    failure = WorkflowFailure(
        code=FailureCode.INTERNAL,
        disposition=FailureDisposition.PERMANENT,
        safe_message="The processing attempt failed",
        occurred_at=clock.now(),
    )
    failed = ProcessingJob.model_validate(
        first.model_dump(mode="python")
        | {
            "status": ProcessingJobStatus.FAILED,
            "lease_owner": None,
            "lease_expires_at": None,
            "claim_token": None,
            "last_failure": failure,
        }
    )
    queued = ProcessingJob.model_validate(
        failed.model_dump(mode="python")
        | {
            "status": ProcessingJobStatus.READY,
            "attempt_count": 0,
            "last_failure": None,
        }
    )
    with SqliteUnitOfWork(database) as uow:
        uow.processing_jobs.save(
            failed,
            first.status,
            first.lease_owner,
            first.lease_expires_at,
            first.claim_token,
        )
        uow.processing_jobs.save(
            queued,
            failed.status,
            failed.lease_owner,
            failed.lease_expires_at,
            failed.claim_token,
        )
        second = uow.processing_jobs.claim_due(
            ProcessingStage.EXTRACTION,
            "budget-worker",
            clock.now(),
            clock.now() + timedelta(minutes=5),
            1,
        )[0]
        uow.commit()

    assert second.attempt_count == first.attempt_count == 1
    assert second.lease_owner == first.lease_owner
    assert second.claim_token != first.claim_token
    controller = service(database, clock)
    with pytest.raises(ProviderBudgetLeaseLostError):
        controller._reserve(stale_context, response_request("stale-claim"))
    reservation = controller._reserve(dispatch(second), response_request("current-claim"))
    assert reservation.claim_token == second.claim_token


def test_settlement_is_independent_of_processing_lease(tmp_path: Path) -> None:
    database = create_database(tmp_path / "application.sqlite3")
    clock = MutableClock()
    job = schedule_and_claim(database, clock)
    controller = service(database, clock)
    reservation = controller._reserve(dispatch(job), response_request("late-settlement"))
    clock.current += timedelta(minutes=5)

    settlement = controller._settle(
        reservation.id,
        outcome=ProviderSettlementOutcome.SUCCEEDED,
        usage=ProviderUsage(
            kind=ProviderUsageKind.TOKENS,
            input_tokens=80,
            output_tokens=40,
        ),
    )

    assert settlement.reservation_id == reservation.id
    with SqliteUnitOfWork(database) as uow:
        assert uow.provider_budget_settlements.get(reservation.id) == settlement


def test_settlement_overrun_is_recorded_and_blocks_later_reservation(
    tmp_path: Path,
) -> None:
    database = create_database(tmp_path / "application.sqlite3")
    clock = MutableClock()
    limits = budget_limits(
        ProcessingStage.EXTRACTION,
        ProviderBudgetLimits(
            preflight_request_limit=2,
            provider_request_limit=2,
            input_token_limit=200,
            output_token_limit=50,
        ),
    )
    job = schedule_and_claim(database, clock, limits=limits)
    controller = service(database, clock)
    first = controller._reserve(dispatch(job), response_request("overrun", output_tokens=50))

    controller._settle(
        first.id,
        outcome=ProviderSettlementOutcome.SUCCEEDED,
        usage=ProviderUsage(
            kind=ProviderUsageKind.TOKENS,
            input_tokens=100,
            output_tokens=51,
        ),
    )

    with SqliteUnitOfWork(database) as uow:
        usage = uow.provider_budget_reservations.usage_for_job(job.id)
        with pytest.raises(sqlite3.IntegrityError, match="output token budget exhausted"):
            uow.provider_budget_reservations.add(
                reservation_for(job, "after-overrun", sequence=2, output_tokens=1)
            )
    assert usage.output_tokens == 51


def reservation_for(
    job: ProcessingJob,
    key: str,
    *,
    sequence: int,
    output_tokens: int,
) -> ProviderBudgetReservation:
    assert job.claim_token is not None
    dispatch_digest = provider_dispatch_digest(key)
    operation_digest = "d" * 64
    fingerprint = provider_reservation_fingerprint(
        job.id,
        job.attempt_count,
        job.claim_token,
        dispatch_digest,
        operation_digest,
        ProviderOperation.RESPONSES_CREATE,
        ProviderCallRole.EXTRACT,
        "gpt-test",
        1,
        output_tokens,
        0,
    )
    return ProviderBudgetReservation(
        id=uuid4(),
        processing_job_id=job.id,
        sequence=sequence,
        attempt_number=job.attempt_count,
        claim_token=job.claim_token,
        dispatch_digest=dispatch_digest,
        operation_digest=operation_digest,
        request_fingerprint=fingerprint,
        operation=ProviderOperation.RESPONSES_CREATE,
        role=ProviderCallRole.EXTRACT,
        model="gpt-test",
        reserved_input_tokens=1,
        reserved_output_tokens=output_tokens,
        created_at=NOW,
    )


def test_raw_stage_attempt_shape_and_malformed_values_are_rejected(
    tmp_path: Path,
) -> None:
    database = create_database(tmp_path / "application.sqlite3")
    clock = MutableClock()
    job = schedule_and_claim(database, clock)

    with database.transaction(immediate=True) as connection:
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                """
                INSERT INTO provider_budget_reservations (
                    id, processing_job_id, sequence, attempt_number, claim_token,
                    dispatch_digest, operation_digest, request_fingerprint,
                    operation, role, model,
                    reserved_input_tokens, reserved_output_tokens,
                    reserved_audio_duration_ms, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "not-a-uuid",
                    str(job.id),
                    1,
                    job.attempt_count,
                    str(job.claim_token),
                    "1" * 64,
                    "2" * 64,
                    "3" * 64,
                    "responses_create",
                    "extract",
                    "gpt-test",
                    1,
                    1,
                    0,
                    "not-a-timestamp",
                ),
            )
        wrong_attempt = reservation_for(job, "wrong-attempt", sequence=1, output_tokens=1)
        values = list(reservation_values(wrong_attempt))
        values[3] = job.attempt_count + 1
        with pytest.raises(sqlite3.IntegrityError, match="active processing attempt"):
            connection.execute(reservation_insert_sql(), values)
        values = list(reservation_values(wrong_attempt))
        values[4] = str(uuid4())
        with pytest.raises(sqlite3.IntegrityError, match="active processing attempt"):
            connection.execute(reservation_insert_sql(), values)
        with pytest.raises(sqlite3.IntegrityError, match="does not match job stage"):
            connection.execute(
                reservation_insert_sql(),
                (
                    str(uuid4()),
                    str(job.id),
                    1,
                    job.attempt_count,
                    str(job.claim_token),
                    "4" * 64,
                    "5" * 64,
                    "6" * 64,
                    "transcription_create",
                    "transcription",
                    "gpt-test",
                    0,
                    0,
                    1,
                    "2026-08-07T09:00:00.000000+00:00",
                ),
            )
        whitespace_model = list(
            reservation_values(
                reservation_for(job, "whitespace-model", sequence=1, output_tokens=1)
            )
        )
        whitespace_model[10] = " gpt-test"
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(reservation_insert_sql(), whitespace_model)


def test_raw_account_stage_must_match_processing_job(tmp_path: Path) -> None:
    database = create_database(tmp_path / "application.sqlite3")
    second_job_id = UUID("30000000-0000-4000-8000-000000000002")

    with database.transaction(immediate=True) as connection:
        connection.execute(
            """
            INSERT INTO processing_jobs (
                id, meeting_id, stage, status, attempt_count, max_attempts,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(second_job_id),
                str(MEETING_ID),
                "transcription",
                "ready",
                0,
                3,
                str(NOW),
                str(NOW),
            ),
        )
        with pytest.raises(sqlite3.IntegrityError, match="FOREIGN KEY"):
            connection.execute(
                """
                INSERT INTO provider_budget_accounts (
                    processing_job_id, stage, policy_version, legacy_locked,
                    preflight_request_limit, provider_request_limit,
                    input_token_limit, output_token_limit,
                    audio_duration_ms_limit, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(second_job_id),
                    "extraction",
                    1,
                    0,
                    1,
                    1,
                    1,
                    1,
                    None,
                    "2026-08-07T09:00:00.000000+00:00",
                ),
            )


def reservation_insert_sql() -> str:
    return """
        INSERT INTO provider_budget_reservations (
            id, processing_job_id, sequence, attempt_number, claim_token,
            dispatch_digest, operation_digest, request_fingerprint, operation, role, model,
            reserved_input_tokens, reserved_output_tokens,
            reserved_audio_duration_ms, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """


def reservation_values(reservation: ProviderBudgetReservation) -> tuple[object, ...]:
    return (
        str(reservation.id),
        str(reservation.processing_job_id),
        reservation.sequence,
        reservation.attempt_number,
        str(reservation.claim_token),
        reservation.dispatch_digest,
        reservation.operation_digest,
        reservation.request_fingerprint,
        reservation.operation.value,
        reservation.role.value,
        reservation.model,
        reservation.reserved_input_tokens,
        reservation.reserved_output_tokens,
        reservation.reserved_audio_duration_ms,
        "2026-08-07T09:00:00.000000+00:00",
    )


def test_append_only_guards_reject_direct_deletes_but_meeting_cascades(
    tmp_path: Path,
) -> None:
    database = create_database(tmp_path / "application.sqlite3")
    clock = MutableClock()
    job = schedule_and_claim(database, clock)
    controller = service(database, clock)
    reservation = controller._reserve(dispatch(job), response_request("cascade"))
    controller._settle(
        reservation.id,
        outcome=ProviderSettlementOutcome.FAILED,
        usage=ProviderUsage(kind=ProviderUsageKind.NONE),
    )

    with database.transaction(immediate=True) as connection:
        updates = (
            "UPDATE provider_budget_accounts SET policy_version = policy_version + 1",
            "UPDATE provider_budget_reservations SET model = 'changed'",
            "UPDATE provider_budget_settlements SET outcome = 'abandoned'",
        )
        for statement in updates:
            with pytest.raises(sqlite3.IntegrityError):
                connection.execute(statement)
        statements = (
            ("DELETE FROM provider_budget_settlements WHERE reservation_id = ?", reservation.id),
            ("DELETE FROM provider_budget_reservations WHERE id = ?", reservation.id),
            ("DELETE FROM provider_budget_accounts WHERE processing_job_id = ?", job.id),
            ("DELETE FROM processing_jobs WHERE id = ?", job.id),
        )
        for statement, identity in statements:
            with pytest.raises(sqlite3.IntegrityError):
                connection.execute(statement, (str(identity),))
        connection.execute("DELETE FROM meetings WHERE id = ?", (str(MEETING_ID),))

    with database.connect() as connection:
        counts = (
            connection.execute("SELECT COUNT(*) FROM processing_jobs").fetchone()[0],
            connection.execute("SELECT COUNT(*) FROM provider_budget_accounts").fetchone()[0],
            connection.execute("SELECT COUNT(*) FROM provider_budget_reservations").fetchone()[0],
            connection.execute("SELECT COUNT(*) FROM provider_budget_settlements").fetchone()[0],
        )
    assert counts == (0, 0, 0, 0)


def test_non_utc_clock_is_normalized_and_naive_clock_is_rejected(tmp_path: Path) -> None:
    database = create_database(tmp_path / "application.sqlite3")
    offset_clock = MutableClock(
        datetime(
            2026,
            8,
            7,
            14,
            30,
            tzinfo=timezone(timedelta(hours=5, minutes=30)),
        )
    )
    job = schedule_and_claim(database, offset_clock)
    reservation = service(database, offset_clock)._reserve(
        dispatch(job), response_request("offset")
    )

    with database.connect() as connection:
        persisted = connection.execute(
            "SELECT created_at FROM provider_budget_reservations WHERE id = ?",
            (str(reservation.id),),
        ).fetchone()[0]
    assert persisted == "2026-08-07T09:00:00.000000+00:00"
    naive_clock = MutableClock(NOW.replace(tzinfo=None))
    with pytest.raises(ValueError, match="timestamps must be timezone-aware"):
        service(database, naive_clock)._reserve(dispatch(job), response_request("naive-clock"))
