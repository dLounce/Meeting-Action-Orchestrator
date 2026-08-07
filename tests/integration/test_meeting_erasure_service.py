from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Barrier
from uuid import UUID

import pytest

from meeting_action_orchestrator.application.errors import (
    MeetingErasureBlockedError,
    MeetingErasureIntegrityError,
    MeetingErasureRequestConflictError,
    StaleWorkflowVersionError,
)
from meeting_action_orchestrator.application.meeting_erasure import (
    ErasureKeyRegistry,
    MeetingErasureResult,
    MeetingErasureService,
)
from meeting_action_orchestrator.application.processing import ProcessingScheduler
from meeting_action_orchestrator.application.provider_budget import ProviderBudgetService
from meeting_action_orchestrator.domain.enums import (
    AudioMediaType,
    DeliveryOperationKind,
    DeliveryOperationStatus,
    MeetingErasureOperation,
    MeetingErasureReason,
    MeetingErasureRecordingState,
    ProcessingJobStatus,
    ProcessingStage,
    ProviderCallRole,
    ProviderOperation,
    ProviderSettlementOutcome,
    ProviderUsageKind,
    RecordingCleanupReason,
    RecordingCleanupStatus,
)
from meeting_action_orchestrator.domain.models import (
    AudioAsset,
    DeliveryOperationBinding,
    IngestRequestBinding,
    Meeting,
    MeetingErasureJob,
    MeetingErasureOperationBinding,
    MeetingErasureTombstone,
    ProcessingJob,
    RecordingCleanupJob,
)
from meeting_action_orchestrator.domain.provider_budget import (
    ProviderBudgetReservationRequest,
    ProviderDispatchContext,
    ProviderUsage,
)
from meeting_action_orchestrator.infrastructure.database import Database
from meeting_action_orchestrator.infrastructure.erasure_tokens import (
    ErasureKeyVerificationError,
    ErasureTokenKeyring,
)
from meeting_action_orchestrator.infrastructure.repositories import SqliteUnitOfWork

NOW = datetime(2026, 8, 7, 9, 0, tzinfo=timezone.utc)
SECRET = b"e" * 32


class FrozenClock:
    def now(self) -> datetime:
        return NOW


class CommitThenRaiseUnitOfWork(SqliteUnitOfWork):
    def commit(self) -> None:
        super().commit()
        raise RuntimeError("commit outcome unavailable")


class CommitForbiddenUnitOfWork(SqliteUnitOfWork):
    def commit(self) -> None:
        raise AssertionError("readiness validation must not commit")


def migrated_database(tmp_path: Path) -> Database:
    tmp_path.mkdir(parents=True, exist_ok=True)
    database = Database(tmp_path / "application.sqlite3")
    database.migrate()
    return database


def keyring() -> ErasureTokenKeyring:
    return ErasureTokenKeyring("current", {"current": SECRET})


def registry(database: Database, tokens: ErasureTokenKeyring) -> ErasureKeyRegistry:
    return ErasureKeyRegistry(
        unit_of_work=lambda: SqliteUnitOfWork(database),
        validation_unit_of_work=lambda: SqliteUnitOfWork(database, immediate=False),
        tokens=tokens,
        clock=FrozenClock(),
    )


def service(
    database: Database,
    tokens: ErasureTokenKeyring,
    *,
    unit_of_work: type[SqliteUnitOfWork] = SqliteUnitOfWork,
) -> MeetingErasureService:
    return MeetingErasureService(
        unit_of_work=lambda: unit_of_work(database),
        tokens=tokens,
        key_registry=registry(database, tokens),
        clock=FrozenClock(),
    )


def audio_asset(number: int = 1) -> AudioAsset:
    return AudioAsset(
        id=UUID(int=number),
        storage_key=f"{number:032x}.wav",
        original_name="recording.wav",
        detected_media_type=AudioMediaType.WAV,
        size_bytes=1_024,
        duration_ms=4_000,
        sha256=f"{number % 16:x}" * 64,
        created_at=NOW,
    )


def meeting(number: int, audio_id: UUID, *, version: int = 0) -> Meeting:
    return Meeting(
        id=UUID(int=10_000 + number),
        ingest_key=f"ingest-{number}",
        title=f"Meeting {number}",
        audio_asset_id=audio_id,
        timezone="UTC",
        version=version,
        created_at=NOW,
        updated_at=NOW,
    )


def seed_meeting(
    database: Database,
    value: Meeting,
    audio: AudioAsset | None = None,
) -> None:
    with SqliteUnitOfWork(database) as uow:
        if audio is not None:
            uow.audio_assets.add(audio)
        uow.meetings.add(value)
        uow.ingest_requests.add(
            IngestRequestBinding(
                ingest_key=value.ingest_key,
                fingerprint_version=1,
                request_fingerprint=f"{value.id.int % 16:x}" * 64,
                created_at=NOW,
            )
        )
        uow.commit()


def add_full_graph(database: Database, value: Meeting) -> None:
    transcript = UUID(int=20_001)
    review = UUID(int=20_002)
    approval = UUID(int=20_003)
    intent = UUID(int=20_004)
    with database.transaction(immediate=True) as connection:
        connection.execute(
            """
            INSERT INTO transcripts (
                id, meeting_id, audio_asset_id, provider, model, language,
                segments_json, text, sha256, usage_json, created_at
            ) VALUES (?, ?, ?, 'provider', 'model', 'en', '[]', 'text', ?, '{}', ?)
            """,
            (str(transcript), str(value.id), str(value.audio_asset_id), "a" * 64, str(NOW)),
        )
        connection.execute(
            """
            INSERT INTO review_revisions (
                id, meeting_id, transcript_id, revision_number, origin,
                payload_json, content_digest, created_at
            ) VALUES (?, ?, ?, 1, 'human', '{}', ?, ?)
            """,
            (str(review), str(value.id), str(transcript), "b" * 64, str(NOW)),
        )
        connection.execute(
            """
            INSERT INTO approvals (
                id, meeting_id, review_revision_id, review_digest,
                request_key, actor_id, approved_at
            ) VALUES (?, ?, ?, ?, 'approval-key', 'actor', ?)
            """,
            (str(approval), str(value.id), str(review), "b" * 64, str(NOW)),
        )
        connection.execute(
            """
            INSERT INTO recap_artifacts (
                id, meeting_id, approval_id, format, content, sha256, created_at
            ) VALUES (?, ?, ?, 'markdown', 'recap', ?, ?)
            """,
            (str(UUID(int=20_005)), str(value.id), str(approval), "c" * 64, str(NOW)),
        )
        connection.execute(
            """
            INSERT INTO write_intents (
                id, meeting_id, approval_id, source_action_id, kind, connector_id,
                resource_id, idempotency_key, payload_json, payload_sha256, status,
                created_at, updated_at
            ) VALUES (?, ?, ?, 'action', 'task', 'connector', 'resource', ?, '{}', ?,
                'succeeded', ?, ?)
            """,
            (
                str(intent),
                str(value.id),
                str(approval),
                f"mao_v1_{'d' * 64}",
                "e" * 64,
                str(NOW),
                str(NOW),
            ),
        )
        connection.execute(
            """
            INSERT INTO write_attempts (
                id, intent_id, attempt_number, started_at, ended_at, outcome
            ) VALUES (?, ?, 1, ?, ?, 'succeeded')
            """,
            (str(UUID(int=20_006)), str(intent), str(NOW), str(NOW)),
        )
        connection.execute(
            """
            INSERT INTO write_receipts (
                id, intent_id, idempotency_key, payload_digest, provider,
                external_id, reconciled, recorded_at
            ) VALUES (?, ?, ?, ?, 'provider', 'external', 0, ?)
            """,
            (
                str(UUID(int=20_007)),
                str(intent),
                f"mao_v1_{'d' * 64}",
                "e" * 64,
                str(NOW),
            ),
        )
        connection.execute(
            """
            INSERT INTO workflow_events (
                id, meeting_id, sequence, type, safe_metadata_json, occurred_at
            ) VALUES (?, ?, 1, 'approved', '{}', ?)
            """,
            (str(UUID(int=20_008)), str(value.id), str(NOW)),
        )
        connection.execute(
            """
            INSERT INTO delivery_operation_bindings (
                request_key, meeting_id, operation, actor_id, selection_fingerprint,
                status, completed_at, version, created_at, updated_at
            ) VALUES ('delivery-key', ?, 'retry', 'actor', ?, 'completed', ?, 0, ?, ?)
            """,
            (str(value.id), "f" * 64, str(NOW), str(NOW), str(NOW)),
        )


def add_provider_budget_graph(database: Database, value: Meeting) -> None:
    job_id = UUID(int=20_009)
    scheduler = ProcessingScheduler(
        unit_of_work=lambda: SqliteUnitOfWork(database),
        clock=FrozenClock(),
        id_factory=lambda: job_id,
    )
    scheduler.enqueue(value.id, ProcessingStage.TRANSCRIPTION)
    with SqliteUnitOfWork(database) as uow:
        claimed = uow.processing_jobs.claim_due(
            ProcessingStage.TRANSCRIPTION,
            "provider-budget-worker",
            NOW,
            NOW + timedelta(minutes=5),
            1,
        )[0]
        uow.commit()
    assert claimed.claim_token is not None
    controller = ProviderBudgetService(
        unit_of_work=lambda: SqliteUnitOfWork(database),
        clock=FrozenClock(),
        id_factory=lambda: UUID(int=20_010),
    )
    reservation = controller._reserve(
        ProviderDispatchContext(
            processing_job_id=claimed.id,
            attempt_number=claimed.attempt_count,
            lease_owner="provider-budget-worker",
            claim_token=claimed.claim_token,
        ),
        ProviderBudgetReservationRequest(
            dispatch_key="erasure-provider-call",
            operation_digest="1" * 64,
            operation=ProviderOperation.TRANSCRIPTION_CREATE,
            role=ProviderCallRole.TRANSCRIPTION,
            model="transcribe-test",
            reserved_audio_duration_ms=4_000,
        ),
    )
    controller._settle(
        reservation.id,
        outcome=ProviderSettlementOutcome.SUCCEEDED,
        usage=ProviderUsage(
            kind=ProviderUsageKind.DURATION,
            audio_duration_ms=4_000,
        ),
    )
    succeeded = ProcessingJob.model_validate(
        claimed.model_dump(mode="python")
        | {
            "status": ProcessingJobStatus.SUCCEEDED,
            "lease_owner": None,
            "lease_expires_at": None,
            "claim_token": None,
            "updated_at": NOW,
        }
    )
    with SqliteUnitOfWork(database) as uow:
        uow.processing_jobs.save(
            succeeded,
            claimed.status,
            claimed.lease_owner,
            claimed.lease_expires_at,
            claimed.claim_token,
        )
        uow.commit()


def cleanup_job(
    number: int,
    digest: str,
    status: RecordingCleanupStatus,
) -> RecordingCleanupJob:
    terminal = status is RecordingCleanupStatus.SUCCEEDED
    return RecordingCleanupJob(
        id=UUID(int=30_000 + number),
        storage_key=f"{30_000 + number:032x}.wav",
        expected_sha256=digest,
        expected_size_bytes=1_024,
        reason=RecordingCleanupReason.ORPHAN_RECONCILIATION,
        status=status,
        attempt_count=int(terminal),
        max_attempts=5,
        created_at=NOW,
        updated_at=NOW,
        completed_at=NOW if terminal else None,
    )


def test_request_purges_full_graph_and_creates_token_only_state(tmp_path: Path) -> None:
    database = migrated_database(tmp_path)
    tokens = keyring()
    audio = audio_asset()
    value = meeting(1, audio.id)
    seed_meeting(database, value, audio)
    add_full_graph(database, value)
    add_provider_budget_graph(database, value)

    result = service(database, tokens)._request(value.id, 0, "erase-request", "actor")

    with database.connect() as connection:
        assert connection.execute("SELECT COUNT(*) FROM meetings").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM audio_assets").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM transcripts").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM review_revisions").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM approvals").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM recap_artifacts").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM write_intents").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM write_attempts").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM write_receipts").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM workflow_events").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM processing_jobs").fetchone()[0] == 0
        assert (
            connection.execute("SELECT COUNT(*) FROM provider_budget_accounts").fetchone()[0] == 0
        )
        assert (
            connection.execute("SELECT COUNT(*) FROM provider_budget_reservations").fetchone()[0]
            == 0
        )
        assert (
            connection.execute("SELECT COUNT(*) FROM provider_budget_settlements").fetchone()[0]
            == 0
        )
        assert (
            connection.execute("SELECT COUNT(*) FROM delivery_operation_bindings").fetchone()[0]
            == 0
        )
        durable = " ".join(
            str(tuple(row))
            for rows in (
                connection.execute("SELECT * FROM meeting_erasure_jobs").fetchall(),
                connection.execute("SELECT * FROM meeting_erasure_tombstones").fetchall(),
                connection.execute("SELECT * FROM meeting_erasure_operation_bindings").fetchall(),
            )
            for row in rows
        )
    assert value.ingest_key not in durable
    assert str(value.id) not in durable
    assert result.job.recording_state is MeetingErasureRecordingState.CLEANUP_PENDING


def test_key_registry_registers_once_and_validates_read_only(tmp_path: Path) -> None:
    database = migrated_database(tmp_path)
    tokens = keyring()
    coordinator = ErasureKeyRegistry(
        unit_of_work=lambda: SqliteUnitOfWork(database),
        validation_unit_of_work=lambda: CommitForbiddenUnitOfWork(
            database,
            immediate=False,
        ),
        tokens=tokens,
        clock=FrozenClock(),
    )

    assert coordinator.ensure_registered_sync() == ("current",)
    assert coordinator.ensure_registered_sync() == ("current",)
    assert coordinator.validate_registered_sync() == ("current",)
    with database.connect() as connection:
        assert connection.execute("SELECT COUNT(*) FROM erasure_key_verifiers").fetchone()[0] == 1


def test_key_registry_wrong_secret_fails_without_overwriting_verifier(tmp_path: Path) -> None:
    database = migrated_database(tmp_path)
    original = keyring()
    registry(database, original).ensure_registered_sync()
    wrong = ErasureTokenKeyring("current", {"current": b"w" * 32})

    with pytest.raises(ErasureKeyVerificationError):
        registry(database, wrong).ensure_registered_sync()

    with SqliteUnitOfWork(database, immediate=False) as uow:
        persisted = tuple(uow.erasure_key_verifiers.list_all())
    original.validate_verifiers(persisted)


def test_commit_ambiguity_replays_the_authoritative_job(tmp_path: Path) -> None:
    database = migrated_database(tmp_path)
    tokens = keyring()
    audio = audio_asset()
    value = meeting(1, audio.id)
    seed_meeting(database, value, audio)
    ambiguous = service(database, tokens, unit_of_work=CommitThenRaiseUnitOfWork)

    with pytest.raises(RuntimeError, match="commit outcome unavailable"):
        ambiguous._request(value.id, 0, "erase-request", "actor")
    replay = service(database, tokens)._request(value.id, 0, "erase-request", "actor")

    assert replay.replayed
    with database.connect() as connection:
        assert connection.execute("SELECT COUNT(*) FROM meeting_erasure_jobs").fetchone()[0] == 1
        assert (
            connection.execute("SELECT COUNT(*) FROM meeting_erasure_tombstones").fetchone()[0] == 1
        )


@pytest.mark.parametrize(
    ("meeting_number", "expected_version", "actor_id"),
    [(2, 0, "actor"), (1, 1, "actor"), (1, 0, "other-actor")],
)
def test_reused_request_key_conflicts_without_retargeting(
    tmp_path: Path,
    meeting_number: int,
    expected_version: int,
    actor_id: str,
) -> None:
    database = migrated_database(tmp_path)
    tokens = keyring()
    first_audio = audio_asset(1)
    second_audio = audio_asset(2)
    first = meeting(1, first_audio.id)
    second = meeting(2, second_audio.id)
    seed_meeting(database, first, first_audio)
    seed_meeting(database, second, second_audio)
    erasures = service(database, tokens)
    erasures._request(first.id, 0, "same-request", "actor")
    target = first if meeting_number == 1 else second

    with pytest.raises(MeetingErasureRequestConflictError):
        erasures._request(target.id, expected_version, "same-request", actor_id)

    with SqliteUnitOfWork(database, immediate=False) as uow:
        assert uow.meetings.get(second.id) == second


def test_new_request_key_converges_on_tombstone_and_checks_version(tmp_path: Path) -> None:
    database = migrated_database(tmp_path)
    tokens = keyring()
    audio = audio_asset()
    value = meeting(1, audio.id, version=3)
    seed_meeting(database, value, audio)
    erasures = service(database, tokens)
    created = erasures._request(value.id, 3, "first-request", "actor")

    replay = erasures._request(value.id, 3, "second-request", "actor")
    with pytest.raises(StaleWorkflowVersionError):
        erasures._request(value.id, 2, "third-request", "actor")

    assert replay.replayed
    assert replay.job.id == created.job.id


def test_live_delivery_lease_blocks_but_stale_lease_is_purgeable(tmp_path: Path) -> None:
    database = migrated_database(tmp_path)
    tokens = keyring()
    audio = audio_asset()
    value = meeting(1, audio.id)
    seed_meeting(database, value, audio)
    live = DeliveryOperationBinding(
        request_key="delivery",
        meeting_id=value.id,
        operation=DeliveryOperationKind.RETRY,
        actor_id="actor",
        selection_fingerprint="a" * 64,
        status=DeliveryOperationStatus.RUNNING,
        lease_owner="worker",
        lease_expires_at=NOW + timedelta(minutes=1),
        created_at=NOW - timedelta(minutes=1),
        updated_at=NOW,
    )
    with SqliteUnitOfWork(database) as uow:
        uow.delivery_operations.add(live)
        uow.commit()

    with pytest.raises(MeetingErasureBlockedError):
        service(database, tokens)._request(value.id, 0, "live-request", "actor")
    with database.transaction(immediate=True) as connection:
        connection.execute(
            "UPDATE delivery_operation_bindings SET lease_expires_at = ?",
            (str(NOW - timedelta(seconds=1)),),
        )

    result = service(database, tokens)._request(value.id, 0, "stale-request", "actor")
    assert result.job.recording_state is MeetingErasureRecordingState.CLEANUP_PENDING


@pytest.mark.parametrize("lease_expires_at", [None, "not-a-timestamp"])
def test_running_delivery_with_unusable_lease_fails_closed(
    tmp_path: Path,
    lease_expires_at: str | None,
) -> None:
    database = migrated_database(tmp_path)
    tokens = keyring()
    audio = audio_asset()
    value = meeting(1, audio.id)
    seed_meeting(database, value, audio)
    live = DeliveryOperationBinding(
        request_key="delivery",
        meeting_id=value.id,
        operation=DeliveryOperationKind.RETRY,
        actor_id="actor",
        selection_fingerprint="a" * 64,
        status=DeliveryOperationStatus.RUNNING,
        lease_owner="worker",
        lease_expires_at=NOW + timedelta(minutes=1),
        created_at=NOW - timedelta(minutes=1),
        updated_at=NOW,
    )
    with SqliteUnitOfWork(database) as uow:
        uow.delivery_operations.add(live)
        uow.commit()
    with database.transaction(immediate=True) as connection:
        connection.execute(
            "UPDATE delivery_operation_bindings SET lease_expires_at = ?",
            (lease_expires_at,),
        )

    with pytest.raises(MeetingErasureBlockedError):
        service(database, tokens)._request(value.id, 0, "erase-request", "actor")

    with SqliteUnitOfWork(database, immediate=False) as uow:
        assert uow.meetings.get(value.id) == value


@pytest.mark.parametrize("blocker", ["processing", "in_flight", "unknown"])
def test_active_and_uncertain_work_blocks_erasure(tmp_path: Path, blocker: str) -> None:
    database = migrated_database(tmp_path)
    tokens = keyring()
    audio = audio_asset()
    value = meeting(1, audio.id)
    seed_meeting(database, value, audio)
    if blocker == "processing":
        with SqliteUnitOfWork(database) as uow:
            uow.processing_jobs.add(processing_job(value.id))
            uow.commit()
    else:
        add_full_graph(database, value)
        with database.transaction(immediate=True) as connection:
            connection.execute(
                "UPDATE write_intents SET status = ?",
                (blocker,),
            )

    with pytest.raises(MeetingErasureBlockedError):
        service(database, tokens)._request(value.id, 0, "erase-request", "actor")


@pytest.mark.parametrize("table", ["processing_jobs", "write_intents"])
def test_unknown_legacy_work_status_fails_closed(tmp_path: Path, table: str) -> None:
    database = migrated_database(tmp_path)
    tokens = keyring()
    audio = audio_asset()
    value = meeting(1, audio.id)
    seed_meeting(database, value, audio)
    if table == "processing_jobs":
        with SqliteUnitOfWork(database) as uow:
            uow.processing_jobs.add(processing_job(value.id))
            uow.commit()
    else:
        add_full_graph(database, value)
    statements = {
        "processing_jobs": "UPDATE processing_jobs SET status = 'unrecognized'",
        "write_intents": "UPDATE write_intents SET status = 'unrecognized'",
    }
    with database.transaction(immediate=True) as connection:
        connection.execute(statements[table])

    with pytest.raises(MeetingErasureBlockedError):
        service(database, tokens)._request(value.id, 0, "erase-request", "actor")

    with SqliteUnitOfWork(database, immediate=False) as uow:
        assert uow.meetings.get(value.id) == value


def processing_job(meeting_id: UUID) -> ProcessingJob:
    return ProcessingJob(
        id=UUID(int=40_001),
        meeting_id=meeting_id,
        stage=ProcessingStage.TRANSCRIPTION,
        status=ProcessingJobStatus.RUNNING,
        attempt_count=1,
        max_attempts=3,
        lease_owner="worker",
        lease_expires_at=NOW + timedelta(minutes=1),
        claim_token=UUID(int=40_002),
        created_at=NOW,
        updated_at=NOW,
    )


def test_shared_audio_waiters_promote_when_the_last_owner_is_erased(tmp_path: Path) -> None:
    database = migrated_database(tmp_path)
    tokens = keyring()
    audio = audio_asset()
    first = meeting(1, audio.id)
    second = meeting(2, audio.id)
    seed_meeting(database, first, audio)
    seed_meeting(database, second)
    erasures = service(database, tokens)

    waiting = erasures._request(first.id, 0, "erase-first", "actor").job
    final = erasures._request(second.id, 0, "erase-second", "actor").job

    with SqliteUnitOfWork(database, immediate=False) as uow:
        promoted = uow.meeting_erasures.get(waiting.id)
        cleanup = uow.recording_cleanups.get(final.cleanup_job_id)
        assert uow.audio_assets.get(audio.id) is None
    assert waiting.recording_state is MeetingErasureRecordingState.WAITING_SHARED
    assert promoted is not None
    assert promoted.recording_state is MeetingErasureRecordingState.CLEANUP_PENDING
    assert promoted.cleanup_job_id == final.cleanup_job_id
    assert promoted.database_checkpointed_at is None
    assert cleanup is not None


def test_same_sha_preflight_removes_success_history_and_blocks_unresolved(
    tmp_path: Path,
) -> None:
    database = migrated_database(tmp_path)
    tokens = keyring()
    audio = audio_asset()
    value = meeting(1, audio.id)
    seed_meeting(database, value, audio)
    succeeded = cleanup_job(1, audio.sha256, RecordingCleanupStatus.SUCCEEDED)
    with SqliteUnitOfWork(database) as uow:
        uow.recording_cleanups.add(succeeded)
        uow.commit()

    created = service(database, tokens)._request(value.id, 0, "erase-request", "actor")

    with SqliteUnitOfWork(database, immediate=False) as uow:
        history = tuple(uow.recording_cleanups.list_by_expected_sha256(audio.sha256))
    assert len(history) == 1
    assert history[0].id == created.job.cleanup_job_id

    database = migrated_database(tmp_path / "blocked")
    audio = audio_asset()
    value = meeting(1, audio.id)
    seed_meeting(database, value, audio)
    unresolved = cleanup_job(2, audio.sha256, RecordingCleanupStatus.READY)
    with SqliteUnitOfWork(database) as uow:
        uow.recording_cleanups.add(unresolved)
        uow.commit()
    with pytest.raises(MeetingErasureBlockedError):
        service(database, tokens)._request(value.id, 0, "blocked-request", "actor")


def test_orphan_job_under_old_rotation_key_fails_closed(tmp_path: Path) -> None:
    database = migrated_database(tmp_path)
    old = ErasureTokenKeyring("old", {"old": b"o" * 32})
    registry(database, old).ensure_registered_sync()
    audio = audio_asset()
    value = meeting(1, audio.id)
    seed_meeting(database, value, audio)
    token = old.meeting_token(value.id)
    orphan = MeetingErasureJob(
        id=UUID(int=50_001),
        token_version=token.token_version,
        token_key_id=token.key_id,
        meeting_token=token.digest,
        reason=MeetingErasureReason.USER_REQUEST,
        erased_meeting_version=0,
        recording_state=MeetingErasureRecordingState.WAITING_SHARED,
        pending_audio_asset_id=audio.id,
        created_at=NOW,
        updated_at=NOW,
    )
    with SqliteUnitOfWork(database) as uow:
        uow.meeting_erasures.add(orphan)
        uow.commit()
    rotated = ErasureTokenKeyring("new", {"new": b"n" * 32, "old": b"o" * 32})

    with pytest.raises(MeetingErasureIntegrityError):
        service(database, rotated)._request(value.id, 0, "erase-request", "actor")


def test_forged_cross_resource_binding_fails_closed(tmp_path: Path) -> None:
    database = migrated_database(tmp_path)
    tokens = keyring()
    registry(database, tokens).ensure_registered_sync()
    target_id = UUID(int=60_001)
    other_id = UUID(int=60_002)
    other_token = tokens.meeting_token(other_id)
    job = MeetingErasureJob(
        id=UUID(int=60_003),
        token_version=other_token.token_version,
        token_key_id=other_token.key_id,
        meeting_token=other_token.digest,
        reason=MeetingErasureReason.USER_REQUEST,
        erased_meeting_version=0,
        recording_state=MeetingErasureRecordingState.WAITING_SHARED,
        pending_audio_asset_id=UUID(int=60_004),
        created_at=NOW,
        updated_at=NOW,
    )
    binding = MeetingErasureOperationBinding.create(
        tokens.request_key_token("forged-request"),
        tokens.actor_token("actor"),
        tokens.meeting_token(target_id),
        job.id,
        MeetingErasureOperation.REQUEST,
        0,
        NOW,
    )
    with SqliteUnitOfWork(database) as uow:
        uow.meeting_erasures.add(job)
        uow.meeting_erasure_operations.add(binding)
        uow.commit()

    with pytest.raises(MeetingErasureIntegrityError):
        service(database, tokens)._request(target_id, 0, "forged-request", "actor")


def test_post_commit_orphan_history_does_not_rewind_erasure_state(tmp_path: Path) -> None:
    database = migrated_database(tmp_path)
    tokens = keyring()
    audio = audio_asset()
    value = meeting(1, audio.id)
    seed_meeting(database, value, audio)
    created = service(database, tokens)._request(value.id, 0, "erase-request", "actor")
    orphan = cleanup_job(9, audio.sha256, RecordingCleanupStatus.READY)
    with SqliteUnitOfWork(database) as uow:
        uow.recording_cleanups.add(orphan)
        uow.commit()
    with SqliteUnitOfWork(database, immediate=False) as uow:
        persisted = uow.meeting_erasures.get(created.job.id)
        history = uow.recording_cleanups.list_by_expected_sha256(audio.sha256)

    assert persisted == created.job
    assert {item.id for item in history} == {created.job.cleanup_job_id, orphan.id}


@pytest.mark.parametrize(
    "pointer",
    ["current_transcript_id", "current_review_id", "approved_review_id"],
)
def test_reverse_current_pointer_corruption_rolls_back_without_collateral_delete(
    tmp_path: Path,
    pointer: str,
) -> None:
    database = migrated_database(tmp_path)
    tokens = keyring()
    target_audio = audio_asset(1)
    other_audio = audio_asset(2)
    target = meeting(1, target_audio.id)
    other = meeting(2, other_audio.id)
    seed_meeting(database, target, target_audio)
    seed_meeting(database, other, other_audio)
    add_full_graph(database, target)
    artifact_id = UUID(int=20_001 if pointer == "current_transcript_id" else 20_002)
    history = cleanup_job(1, target_audio.sha256, RecordingCleanupStatus.SUCCEEDED)
    with SqliteUnitOfWork(database) as uow:
        uow.recording_cleanups.add(history)
        uow.commit()
    statements = {
        "current_transcript_id": "UPDATE meetings SET current_transcript_id = ? WHERE id = ?",
        "current_review_id": "UPDATE meetings SET current_review_id = ? WHERE id = ?",
        "approved_review_id": "UPDATE meetings SET approved_review_id = ? WHERE id = ?",
    }
    with database.transaction(immediate=True) as connection:
        connection.execute(statements[pointer], (str(artifact_id), str(other.id)))

    with pytest.raises(MeetingErasureIntegrityError):
        service(database, tokens)._request(target.id, 0, "erase-request", "actor")

    with SqliteUnitOfWork(database, immediate=False) as uow:
        assert uow.meetings.get(target.id) is not None
        assert uow.meetings.get(other.id) is not None
        assert uow.recording_cleanups.get(history.id) == history


def test_cross_owner_approval_edge_cannot_collateral_delete_another_meeting(
    tmp_path: Path,
) -> None:
    database = migrated_database(tmp_path)
    tokens = keyring()
    target_audio = audio_asset(1)
    other_audio = audio_asset(2)
    target = meeting(1, target_audio.id)
    other = meeting(2, other_audio.id)
    seed_meeting(database, target, target_audio)
    seed_meeting(database, other, other_audio)
    add_full_graph(database, target)
    with database.transaction(immediate=True) as connection:
        connection.execute(
            "UPDATE write_intents SET meeting_id = ?",
            (str(other.id),),
        )

    with pytest.raises(MeetingErasureIntegrityError):
        service(database, tokens)._request(target.id, 0, "erase-request", "actor")

    with database.connect() as connection:
        assert connection.execute("SELECT COUNT(*) FROM meetings").fetchone()[0] == 2
        assert connection.execute("SELECT COUNT(*) FROM write_intents").fetchone()[0] == 1


def test_old_key_ingest_tombstone_collision_fails_closed(tmp_path: Path) -> None:
    database = migrated_database(tmp_path)
    old = ErasureTokenKeyring("old", {"old": b"o" * 32})
    register_old = registry(database, old)
    register_old.ensure_registered_sync()
    audio = audio_asset()
    value = meeting(1, audio.id)
    seed_meeting(database, value, audio)
    erased_id = UUID(int=70_001)
    erased_token = old.meeting_token(erased_id)
    job = MeetingErasureJob(
        id=UUID(int=70_002),
        token_version=erased_token.token_version,
        token_key_id=erased_token.key_id,
        meeting_token=erased_token.digest,
        reason=MeetingErasureReason.USER_REQUEST,
        erased_meeting_version=0,
        recording_state=MeetingErasureRecordingState.WAITING_SHARED,
        pending_audio_asset_id=UUID(int=70_003),
        created_at=NOW,
        updated_at=NOW,
    )
    tombstone = MeetingErasureTombstone.create(
        job.id,
        erased_token,
        old.ingest_key_token(value.ingest_key),
        NOW,
    )
    with SqliteUnitOfWork(database) as uow:
        uow.meeting_erasures.add(job)
        uow.meeting_erasure_tombstones.add(tombstone)
        uow.commit()
    rotated = ErasureTokenKeyring("new", {"new": b"n" * 32, "old": b"o" * 32})

    with pytest.raises(MeetingErasureIntegrityError):
        service(database, rotated)._request(value.id, 0, "erase-request", "actor")

    with SqliteUnitOfWork(database, immediate=False) as uow:
        assert uow.meetings.get(value.id) == value
        assert uow.audio_assets.get(audio.id) == audio


def test_linked_succeeded_same_sha_history_blocks_preflight(tmp_path: Path) -> None:
    database = migrated_database(tmp_path)
    tokens = keyring()
    registry(database, tokens).ensure_registered_sync()
    audio = audio_asset()
    value = meeting(1, audio.id)
    seed_meeting(database, value, audio)
    cleanup = RecordingCleanupJob.model_validate(
        cleanup_job(1, audio.sha256, RecordingCleanupStatus.SUCCEEDED).model_dump(mode="python")
        | {"reason": RecordingCleanupReason.MEETING_ERASURE}
    )
    token = tokens.meeting_token(UUID(int=80_001))
    linked = MeetingErasureJob(
        id=UUID(int=80_002),
        token_version=token.token_version,
        token_key_id=token.key_id,
        meeting_token=token.digest,
        reason=MeetingErasureReason.USER_REQUEST,
        erased_meeting_version=0,
        recording_state=MeetingErasureRecordingState.CLEANUP_PENDING,
        cleanup_job_id=cleanup.id,
        database_checkpointed_at=NOW,
        created_at=NOW,
        updated_at=NOW,
    )
    with SqliteUnitOfWork(database) as uow:
        uow.recording_cleanups.add(cleanup)
        uow.meeting_erasures.add(linked)
        uow.commit()

    with pytest.raises(MeetingErasureBlockedError):
        service(database, tokens)._request(value.id, 0, "erase-request", "actor")

    with SqliteUnitOfWork(database, immediate=False) as uow:
        assert uow.meetings.get(value.id) == value
        assert uow.recording_cleanups.get(cleanup.id) == cleanup


def test_concurrent_request_keys_converge_on_one_erasure_job(tmp_path: Path) -> None:
    database = migrated_database(tmp_path)
    tokens = keyring()
    audio = audio_asset()
    value = meeting(1, audio.id)
    seed_meeting(database, value, audio)
    erasures = service(database, tokens)
    barrier = Barrier(2)

    def erase(request_key: str) -> MeetingErasureResult:
        barrier.wait(timeout=2)
        return erasures._request(value.id, 0, request_key, "actor")

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = tuple(executor.map(erase, ("first-request", "second-request")))

    assert len({result.job.id for result in results}) == 1
    assert {result.replayed for result in results} == {False, True}
    with database.connect() as connection:
        assert connection.execute("SELECT COUNT(*) FROM meeting_erasure_jobs").fetchone()[0] == 1
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM meeting_erasure_operation_bindings"
            ).fetchone()[0]
            == 2
        )


def test_concurrent_global_request_key_cannot_erase_two_meetings(tmp_path: Path) -> None:
    database = migrated_database(tmp_path)
    tokens = keyring()
    first_audio = audio_asset(1)
    second_audio = audio_asset(2)
    first = meeting(1, first_audio.id)
    second = meeting(2, second_audio.id)
    seed_meeting(database, first, first_audio)
    seed_meeting(database, second, second_audio)
    erasures = service(database, tokens)
    barrier = Barrier(2)

    def erase(
        value: Meeting,
    ) -> MeetingErasureResult | MeetingErasureRequestConflictError:
        barrier.wait(timeout=2)
        try:
            return erasures._request(value.id, 0, "global-request", "actor")
        except MeetingErasureRequestConflictError as error:
            return error

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = tuple(executor.map(erase, (first, second)))

    assert sum(not isinstance(item, Exception) for item in outcomes) == 1
    assert sum(isinstance(item, MeetingErasureRequestConflictError) for item in outcomes) == 1
    with database.connect() as connection:
        assert connection.execute("SELECT COUNT(*) FROM meetings").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM meeting_erasure_jobs").fetchone()[0] == 1
