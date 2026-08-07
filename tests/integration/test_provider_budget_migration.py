from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID

import pytest

from meeting_action_orchestrator.application.errors import ProviderBudgetExhaustedError
from meeting_action_orchestrator.application.provider_budget import ProviderBudgetService
from meeting_action_orchestrator.domain.enums import (
    ProcessingStage,
    ProviderCallRole,
    ProviderOperation,
)
from meeting_action_orchestrator.domain.provider_budget import (
    ProviderBudgetReservationRequest,
    ProviderDispatchContext,
)
from meeting_action_orchestrator.infrastructure import database as database_module
from meeting_action_orchestrator.infrastructure.database import Database
from meeting_action_orchestrator.infrastructure.repositories import SqliteUnitOfWork

NOW = datetime(2026, 8, 7, 9, 0, tzinfo=timezone.utc)
JOB_ID = UUID("30000000-0000-4000-8000-000000000001")


class Clock:
    def now(self) -> datetime:
        return NOW


def create_v10_database(path: Path, monkeypatch: pytest.MonkeyPatch) -> Database:
    database = Database(path)
    migrations = database_module.MIGRATIONS
    monkeypatch.setattr(database_module, "MIGRATIONS", migrations[:10])
    assert database.migrate() == 10
    with database.transaction() as connection:
        connection.execute(
            """
            INSERT INTO audio_assets (
                id, storage_key, original_name, media_type, size_bytes,
                duration_ms, sha256, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "10000000-0000-4000-8000-000000000001",
                "recording.wav",
                "recording.wav",
                "audio/wav",
                1_024,
                60_000,
                "a" * 64,
                str(NOW),
            ),
        )
        connection.execute(
            """
            INSERT INTO meetings (
                id, ingest_key, title, audio_asset_id, occurred_at, timezone,
                participants_json, status, version, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "20000000-0000-4000-8000-000000000001",
                "legacy-provider-budget",
                "Legacy provider budget",
                "10000000-0000-4000-8000-000000000001",
                str(NOW),
                "UTC",
                "[]",
                "ingested",
                0,
                str(NOW),
                str(NOW),
            ),
        )
        connection.execute(
            """
            INSERT INTO processing_jobs (
                id, meeting_id, stage, status, attempt_count, max_attempts,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(JOB_ID),
                "20000000-0000-4000-8000-000000000001",
                "transcription",
                "ready",
                0,
                3,
                str(NOW),
                str(NOW),
            ),
        )
    monkeypatch.setattr(database_module, "MIGRATIONS", migrations)
    return database


def test_v11_backfills_locked_zero_limit_accounts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = create_v10_database(tmp_path / "application.sqlite3", monkeypatch)

    assert database.migrate() == 11
    assert database.migrate() == 11
    with SqliteUnitOfWork(database) as uow:
        account = uow.provider_budget_accounts.get(JOB_ID)
        claimed = uow.processing_jobs.claim_due(
            ProcessingStage.TRANSCRIPTION,
            "legacy-worker",
            NOW,
            NOW + timedelta(minutes=5),
            1,
        )
        uow.commit()
    assert account is not None
    assert account.legacy_locked
    assert account.policy_version == 1
    assert set(account.limits.model_dump().values()) == {0}
    assert account.created_at == NOW
    assert claimed[0].attempt_count == 1
    assert claimed[0].claim_token is not None

    controller = ProviderBudgetService(
        unit_of_work=lambda: SqliteUnitOfWork(database),
        clock=Clock(),
    )
    with pytest.raises(ProviderBudgetExhaustedError):
        controller._reserve(
            ProviderDispatchContext(
                processing_job_id=JOB_ID,
                attempt_number=1,
                lease_owner="legacy-worker",
                claim_token=claimed[0].claim_token,
            ),
            ProviderBudgetReservationRequest(
                dispatch_key="legacy-call",
                operation_digest="b" * 64,
                operation=ProviderOperation.TRANSCRIPTION_CREATE,
                role=ProviderCallRole.TRANSCRIPTION,
                model="transcribe-test",
                reserved_audio_duration_ms=60_000,
            ),
        )

    with database.connect() as connection:
        assert connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []


def test_v11_rebuild_preserves_running_job_with_deterministic_claim_token(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = create_v10_database(tmp_path / "application.sqlite3", monkeypatch)
    with database.transaction(immediate=True) as connection:
        connection.execute(
            """
            UPDATE processing_jobs
            SET status = 'running', attempt_count = 1, lease_owner = 'legacy-worker',
                lease_expires_at = '2026-08-07T14:35:00+05:30',
                updated_at = '2026-08-07T14:30:00+05:30'
            WHERE id = ?
            """,
            (str(JOB_ID),),
        )

    assert database.migrate() == 11
    with SqliteUnitOfWork(database, immediate=False) as uow:
        job = uow.processing_jobs.get(JOB_ID)
    assert job is not None
    assert job.status.value == "running"
    assert job.claim_token == JOB_ID
    assert job.updated_at == NOW
    assert job.lease_expires_at == NOW + timedelta(minutes=5)
    with database.connect() as connection:
        row = connection.execute(
            "SELECT claim_token, updated_at, lease_expires_at FROM processing_jobs WHERE id = ?",
            (str(JOB_ID),),
        ).fetchone()
        assert row is not None
        assert tuple(row) == (
            str(JOB_ID),
            "2026-08-07T09:00:00.000000+00:00",
            "2026-08-07T09:05:00.000000+00:00",
        )


def test_v11_processing_job_rebuild_rolls_back_atomically(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = create_v10_database(tmp_path / "application.sqlite3", monkeypatch)
    with database.transaction(immediate=True) as connection:
        connection.execute(
            """
            UPDATE processing_jobs
            SET status = 'running', attempt_count = 1, lease_owner = 'legacy-worker',
                lease_expires_at = 'invalid-timestamp'
            WHERE id = ?
            """,
            (str(JOB_ID),),
        )

    with pytest.raises(sqlite3.IntegrityError):
        database.migrate()

    with database.connect() as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 10
        columns = {row["name"] for row in connection.execute("PRAGMA table_info(processing_jobs)")}
        persisted = connection.execute(
            "SELECT status, lease_expires_at FROM processing_jobs WHERE id = ?",
            (str(JOB_ID),),
        ).fetchone()
        provider_table = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            ("provider_budget_accounts",),
        ).fetchone()
    assert persisted is not None
    assert "claim_token" not in columns
    assert tuple(persisted) == ("running", "invalid-timestamp")
    assert provider_table is None


def test_v11_rebuild_preserves_submillisecond_running_lease_order(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = create_v10_database(tmp_path / "application.sqlite3", monkeypatch)
    with database.transaction(immediate=True) as connection:
        connection.execute(
            """
            UPDATE processing_jobs
            SET status = 'running', attempt_count = 1, lease_owner = 'legacy-worker',
                updated_at = '2026-08-07T09:00:00.000100+00:00',
                lease_expires_at = '2026-08-07T09:00:00.000200+00:00'
            WHERE id = ?
            """,
            (str(JOB_ID),),
        )

    assert database.migrate() == 11
    with SqliteUnitOfWork(database, immediate=False) as uow:
        job = uow.processing_jobs.get(JOB_ID)
    assert job is not None
    assert job.updated_at.microsecond == 100
    assert job.lease_expires_at is not None
    assert job.lease_expires_at.microsecond == 200
    assert job.lease_expires_at > job.updated_at
    with database.connect() as connection:
        row = connection.execute(
            "SELECT updated_at, lease_expires_at FROM processing_jobs WHERE id = ?",
            (str(JOB_ID),),
        ).fetchone()
    assert row is not None
    assert tuple(row) == (
        "2026-08-07T09:00:00.000100+00:00",
        "2026-08-07T09:00:00.000200+00:00",
    )


def test_v11_rejects_invalid_nullable_timestamp_without_losing_retry_shape(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = create_v10_database(tmp_path / "application.sqlite3", monkeypatch)
    failure = (
        '{"code":"internal","disposition":"retryable",'
        '"safe_message":"Retry later","provider_request_id":null,'
        '"occurred_at":"2026-08-07T09:00:00+00:00"}'
    )
    with database.transaction(immediate=True) as connection:
        connection.execute(
            """
            UPDATE processing_jobs
            SET status = 'retry_wait', attempt_count = 1,
                next_attempt_at = 'invalid-timestamp', last_failure_json = ?
            WHERE id = ?
            """,
            (failure, str(JOB_ID)),
        )

    with pytest.raises(sqlite3.IntegrityError):
        database.migrate()

    with database.connect() as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 10
        columns = {row["name"] for row in connection.execute("PRAGMA table_info(processing_jobs)")}
        persisted = connection.execute(
            "SELECT status, next_attempt_at, last_failure_json FROM processing_jobs WHERE id = ?",
            (str(JOB_ID),),
        ).fetchone()
        provider_table = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            ("provider_budget_accounts",),
        ).fetchone()
    assert persisted is not None
    assert tuple(persisted) == ("retry_wait", "invalid-timestamp", failure)
    assert "claim_token" not in columns
    assert provider_table is None
