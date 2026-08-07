from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from meeting_action_orchestrator.infrastructure import database as database_module
from meeting_action_orchestrator.infrastructure.database import Database

NOW = "2026-08-07T09:00:00+00:00"
LATER = "2026-08-07T09:05:00+00:00"
UTC_NOW = "2026-08-07T09:00:00.000000+00:00"
UTC_LATER = "2026-08-07T09:05:00.000000+00:00"


def insert_raw_erasure_job(
    connection: sqlite3.Connection,
    **updates: object,
) -> None:
    values: dict[str, object] = {
        "id": "00000000-0000-0000-0000-000000000101",
        "token_version": 1,
        "token_key_id": "current",
        "meeting_token": "a" * 64,
        "reason": "user_request",
        "erased_meeting_version": 1,
        "status": "active",
        "recording_state": "waiting_shared",
        "pending_audio_asset_id": "00000000-0000-0000-0000-000000000201",
        "cleanup_job_id": None,
        "database_checkpointed_at": None,
        "retry_count": 0,
        "next_attempt_at": None,
        "lease_owner": None,
        "lease_expires_at": None,
        "last_failure_code": None,
        "last_failure_disposition": None,
        "last_failure_occurred_at": None,
        "remediation_count": 0,
        "max_remediations": 3,
        "version": 0,
        "created_at": UTC_NOW,
        "updated_at": UTC_NOW,
        "completed_at": None,
    }
    connection.execute(
        """
        INSERT INTO meeting_erasure_jobs (
            id, token_version, token_key_id, meeting_token, reason,
            erased_meeting_version, status, recording_state,
            pending_audio_asset_id, cleanup_job_id, database_checkpointed_at,
            retry_count, next_attempt_at, lease_owner, lease_expires_at,
            last_failure_code, last_failure_disposition, last_failure_occurred_at,
            remediation_count, max_remediations, version, created_at,
            updated_at, completed_at
        ) VALUES (
            :id, :token_version, :token_key_id, :meeting_token, :reason,
            :erased_meeting_version, :status, :recording_state,
            :pending_audio_asset_id, :cleanup_job_id, :database_checkpointed_at,
            :retry_count, :next_attempt_at, :lease_owner, :lease_expires_at,
            :last_failure_code, :last_failure_disposition, :last_failure_occurred_at,
            :remediation_count, :max_remediations, :version, :created_at,
            :updated_at, :completed_at
        )
        """,
        values | updates,
    )


def insert_raw_cleanup(
    connection: sqlite3.Connection,
    job_id: str,
    storage_key: str,
    status: str = "ready",
    reason: str = "meeting_erasure",
) -> None:
    terminal = status in {"succeeded", "failed"}
    failure = None
    if status == "failed":
        failure = (
            '{"code":"internal","disposition":"permanent",'
            '"safe_message":"Cleanup failed","provider_request_id":null,'
            f'"occurred_at":"{NOW}"}}'
        )
    connection.execute(
        """
        INSERT INTO recording_cleanup_jobs (
            id, storage_key, expected_sha256, expected_size_bytes, reason,
            status, attempt_count, max_attempts, next_attempt_at, lease_owner,
            lease_expires_at, last_failure_json, created_at, updated_at, completed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            job_id,
            storage_key,
            "a" * 64,
            16,
            reason,
            status,
            5 if status == "failed" else int(terminal),
            5,
            None,
            None,
            None,
            failure,
            NOW,
            NOW,
            LATER if terminal else None,
        ),
    )


def test_v9_rebuild_preserves_cleanup_rows_and_ownership_triggers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = Database(tmp_path / "application.sqlite3")
    with monkeypatch.context() as migration_patch:
        migration_patch.setattr(database_module, "MIGRATIONS", database_module.MIGRATIONS[:8])
        assert database.migrate() == 8
    rows = (
        ("1", "1" * 32 + ".wav", "ready", 0, None, None, None, None),
        ("2", "2" * 32 + ".wav", "running", 1, None, "worker", LATER, None),
        ("3", "3" * 32 + ".wav", "retry_wait", 2, LATER, None, None, None),
        ("4", "4" * 32 + ".wav", "succeeded", 1, None, None, None, LATER),
        ("5", "5" * 32 + ".wav", "failed", 5, None, None, None, LATER),
    )
    failure_json = (
        '{"code":"internal","disposition":"retryable",'
        '"safe_message":"Cleanup failed","provider_request_id":null,'
        f'"occurred_at":"{NOW}"}}'
    )
    with database.transaction(immediate=True) as connection:
        connection.executemany(
            """
            INSERT INTO recording_cleanup_jobs (
                id, storage_key, expected_sha256, expected_size_bytes, reason,
                status, attempt_count, max_attempts, next_attempt_at, lease_owner,
                lease_expires_at, last_failure_json, created_at, updated_at, completed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    row[0],
                    row[1],
                    "a" * 64,
                    16,
                    "abandoned_ingest",
                    row[2],
                    row[3],
                    5,
                    row[4],
                    row[5],
                    row[6],
                    failure_json if row[2] in {"retry_wait", "failed"} else None,
                    NOW,
                    NOW,
                    row[7],
                )
                for row in rows
            ],
        )
        before = tuple(
            dict(row)
            for row in connection.execute(
                "SELECT * FROM recording_cleanup_jobs ORDER BY id"
            ).fetchall()
        )

    assert database.migrate() == 10
    with database.connect() as connection:
        after = tuple(
            dict(row)
            for row in connection.execute(
                "SELECT * FROM recording_cleanup_jobs ORDER BY id"
            ).fetchall()
        )
        triggers = {
            row["name"]
            for row in connection.execute(
                """
                SELECT name FROM sqlite_master
                WHERE type = 'trigger' AND name LIKE '%cleanup%'
                """
            ).fetchall()
        }
        foreign_key_violations = connection.execute("PRAGMA foreign_key_check").fetchall()

    assert after == before
    assert {
        "recording_cleanup_jobs_reject_owned_insert",
        "recording_cleanup_jobs_reject_owned_update",
        "audio_assets_reject_cleanup_insert",
        "audio_assets_reject_cleanup_update",
        "recording_cleanup_jobs_reject_identity_update",
    } <= triggers
    assert foreign_key_violations == []


def test_v9_erasure_tables_are_token_only_and_have_expected_foreign_keys(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "application.sqlite3")
    database.migrate()

    with database.connect() as connection:
        columns = {
            table: {
                row["name"] for row in connection.execute(f"PRAGMA table_info({table})").fetchall()
            }
            for table in (
                "meeting_erasure_jobs",
                "meeting_erasure_tombstones",
                "meeting_erasure_operation_bindings",
            )
        }
        job_targets = {
            row["table"]
            for row in connection.execute(
                "PRAGMA foreign_key_list(meeting_erasure_jobs)"
            ).fetchall()
        }

    assert "meeting_id" not in columns["meeting_erasure_jobs"]
    assert "meeting_id" not in columns["meeting_erasure_tombstones"]
    assert "ingest_key" not in columns["meeting_erasure_tombstones"]
    assert "actor_id" not in columns["meeting_erasure_operation_bindings"]
    assert "actor_token" in columns["meeting_erasure_operation_bindings"]
    assert job_targets == {"erasure_key_verifiers", "recording_cleanup_jobs"}


def test_v9_adds_all_purge_and_key_reference_indexes(tmp_path: Path) -> None:
    database = Database(tmp_path / "application.sqlite3")
    database.migrate()

    expected = {
        "idx_recording_cleanup_jobs_sha_status",
        "idx_write_intents_meeting_status",
        "idx_meetings_audio_asset_id",
        "idx_transcripts_audio_asset_id",
        "idx_review_revisions_transcript_id",
        "idx_approvals_review_revision_id",
        "idx_recap_artifacts_meeting_id",
        "idx_meeting_erasure_jobs_token_key",
        "idx_meeting_erasure_tombstones_token_key",
        "idx_meeting_erasure_operations_token_key",
    }
    with database.connect() as connection:
        indexes = {
            row["name"]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'index'"
            ).fetchall()
        }
        query_plan = " ".join(
            row["detail"]
            for row in connection.execute(
                "EXPLAIN QUERY PLAN SELECT id FROM review_revisions WHERE transcript_id = ?",
                ("transcript",),
            ).fetchall()
        )
        cleanup_plan = " ".join(
            row["detail"]
            for row in connection.execute(
                """
                EXPLAIN QUERY PLAN
                SELECT id FROM recording_cleanup_jobs
                WHERE expected_sha256 = ? AND status = ?
                """,
                ("a" * 64, "succeeded"),
            ).fetchall()
        )

    assert expected <= indexes
    assert "idx_review_revisions_transcript_id" in query_plan
    assert "idx_recording_cleanup_jobs_sha_status" in cleanup_plan


def test_v9_failure_restores_exact_v8_schema_and_data_then_retries(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = Database(tmp_path / "application.sqlite3")
    original_migrations = database_module.MIGRATIONS
    with monkeypatch.context() as migration_patch:
        migration_patch.setattr(database_module, "MIGRATIONS", original_migrations[:8])
        assert database.migrate() == 8
    with database.transaction(immediate=True) as connection:
        insert_raw_cleanup(
            connection,
            "legacy",
            "a" * 32 + ".wav",
            reason="abandoned_ingest",
        )
        before_schema = connection.execute(
            """
            SELECT sql FROM sqlite_master
            WHERE type = 'table' AND name = 'recording_cleanup_jobs'
            """
        ).fetchone()[0]
        before_row = dict(connection.execute("SELECT * FROM recording_cleanup_jobs").fetchone())

    failing_v9 = database_module.SCHEMA_V9 + "\nSELECT * FROM missing_v9_table;"
    with monkeypatch.context() as migration_patch:
        migration_patch.setattr(
            database_module,
            "MIGRATIONS",
            (*original_migrations[:8], (9, failing_v9)),
        )
        with pytest.raises(sqlite3.OperationalError):
            database.migrate()

    with database.connect() as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 8
        assert (
            connection.execute(
                """
                SELECT sql FROM sqlite_master
                WHERE type = 'table' AND name = 'recording_cleanup_jobs'
                """
            ).fetchone()[0]
            == before_schema
        )
        assert dict(connection.execute("SELECT * FROM recording_cleanup_jobs").fetchone()) == (
            before_row
        )
        assert (
            connection.execute(
                """
                SELECT 1 FROM sqlite_master
                WHERE type = 'table' AND name = 'meeting_erasure_jobs'
                """
            ).fetchone()
            is None
        )

    assert database.migrate() == 10
    with database.connect() as connection:
        assert dict(connection.execute("SELECT * FROM recording_cleanup_jobs").fetchone()) == (
            before_row
        )
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []


def test_v9_schema_supports_stock_sqlite_integrity_dump_and_restore(tmp_path: Path) -> None:
    path = tmp_path / "application.sqlite3"
    Database(path).migrate()

    with sqlite3.connect(path) as connection:
        assert connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        dump = "\n".join(connection.iterdump())
    with sqlite3.connect(":memory:") as restored:
        restored.executescript(dump)
        assert restored.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert (
            restored.execute(
                "SELECT 1 FROM sqlite_master WHERE name = 'meeting_erasure_jobs'"
            ).fetchone()[0]
            == 1
        )


@pytest.mark.parametrize(
    ("key_id", "version", "digest", "created_at"),
    [
        (sqlite3.Binary(b"current"), 1, "a" * 64, UTC_NOW),
        ("current", 1, sqlite3.Binary(b"a" * 64), UTC_NOW),
        ("current", 1.5, "a" * 64, UTC_NOW),
        ("current", 1, "a" * 64, "2026-02-30T09:00:00.000000+00:00"),
        ("current", 1, "a" * 64, "2026-08-07T24:00:00.000000+00:00"),
        ("current", 1, "a" * 64, "2026-08-07T09:00:00.000000"),
    ],
)
def test_v9_rejects_nonloadable_verifier_values(
    tmp_path: Path,
    key_id: object,
    version: object,
    digest: object,
    created_at: object,
) -> None:
    database = Database(tmp_path / "application.sqlite3")
    database.migrate()

    with pytest.raises(sqlite3.IntegrityError), database.transaction(immediate=True) as connection:
        connection.execute(
            """
            INSERT INTO erasure_key_verifiers (
                key_id, verifier_version, verifier_digest, created_at
            ) VALUES (?, ?, ?, ?)
            """,
            (key_id, version, digest, created_at),
        )


@pytest.mark.parametrize(
    "updates",
    [
        {"id": "00000000-0000-0000-0000-00000000000-"},
        {"token_version": 1.5},
        {"meeting_token": sqlite3.Binary(b"a" * 64)},
        {"retry_count": 0.5},
        {"lease_owner": " worker ", "lease_expires_at": UTC_LATER},
        {"created_at": "2023-02-29T09:00:00.000000+00:00"},
    ],
)
def test_v9_rejects_nonloadable_erasure_job_values(
    tmp_path: Path,
    updates: dict[str, object],
) -> None:
    database = Database(tmp_path / "application.sqlite3")
    database.migrate()
    with database.transaction(immediate=True) as connection:
        connection.execute(
            """
            INSERT INTO erasure_key_verifiers (
                key_id, verifier_version, verifier_digest, created_at
            ) VALUES ('current', 1, ?, ?)
            """,
            ("a" * 64, UTC_NOW),
        )

    with pytest.raises(sqlite3.IntegrityError), database.transaction(immediate=True) as connection:
        insert_raw_erasure_job(connection, **updates)


def test_v9_accepts_valid_leap_day_and_rebuilt_ownership_triggers(tmp_path: Path) -> None:
    database = Database(tmp_path / "application.sqlite3")
    database.migrate()
    with database.transaction(immediate=True) as connection:
        connection.execute(
            """
            INSERT INTO erasure_key_verifiers (
                key_id, verifier_version, verifier_digest, created_at
            ) VALUES ('current', 1, ?, '2024-02-29T23:59:59.999999+00:00')
            """,
            ("a" * 64,),
        )
        connection.execute(
            """
            INSERT INTO audio_assets (
                id, storage_key, original_name, media_type, size_bytes,
                duration_ms, sha256, created_at
            ) VALUES ('audio-1', ?, 'recording.wav', 'audio/wav', 16, 1, ?, ?)
            """,
            ("b" * 32 + ".wav", "b" * 64, NOW),
        )
        with pytest.raises(sqlite3.IntegrityError):
            insert_raw_cleanup(connection, "cleanup-1", "b" * 32 + ".wav")
        insert_raw_cleanup(connection, "cleanup-2", "c" * 32 + ".wav")
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                """
                INSERT INTO audio_assets (
                    id, storage_key, original_name, media_type, size_bytes,
                    duration_ms, sha256, created_at
                ) VALUES ('audio-2', ?, 'recording.wav', 'audio/wav', 16, 1, ?, ?)
                """,
                ("c" * 32 + ".wav", "c" * 64, NOW),
            )


def test_v9_raw_transition_guards_require_cleanup_evidence_and_fresh_checkpoint(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "application.sqlite3")
    database.migrate()
    cleanup_id = "00000000-0000-0000-0000-000000000301"
    with database.transaction(immediate=True) as connection:
        connection.execute(
            """
            INSERT INTO erasure_key_verifiers (
                key_id, verifier_version, verifier_digest, created_at
            ) VALUES ('current', 1, ?, ?)
            """,
            ("a" * 64, UTC_NOW),
        )
        insert_raw_cleanup(connection, cleanup_id, "d" * 32 + ".wav")
        with pytest.raises(sqlite3.IntegrityError):
            insert_raw_erasure_job(
                connection,
                recording_state="failed",
                pending_audio_asset_id=None,
                cleanup_job_id=cleanup_id,
                last_failure_code="recording_cleanup_rejected",
                last_failure_disposition="permanent",
                last_failure_occurred_at=UTC_NOW,
            )
        insert_raw_erasure_job(connection, database_checkpointed_at=UTC_NOW)
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                """
                UPDATE meeting_erasure_jobs
                SET recording_state = 'removed', pending_audio_asset_id = NULL,
                    version = 1
                WHERE id = '00000000-0000-0000-0000-000000000101'
                """
            )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                """
                UPDATE meeting_erasure_jobs
                SET recording_state = 'cleanup_pending', pending_audio_asset_id = NULL,
                    cleanup_job_id = ?, version = 1
                WHERE id = '00000000-0000-0000-0000-000000000101'
                """,
                (cleanup_id,),
            )
        connection.execute(
            """
            UPDATE meeting_erasure_jobs
            SET recording_state = 'cleanup_pending', pending_audio_asset_id = NULL,
                cleanup_job_id = ?, database_checkpointed_at = NULL, version = 1
            WHERE id = '00000000-0000-0000-0000-000000000101'
            """,
            (cleanup_id,),
        )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                """
                UPDATE meeting_erasure_jobs
                SET recording_state = 'removed', cleanup_job_id = NULL, version = 2
                WHERE id = '00000000-0000-0000-0000-000000000101'
                """
            )
        connection.execute(
            """
            UPDATE recording_cleanup_jobs
            SET status = 'succeeded', attempt_count = 1, updated_at = ?, completed_at = ?
            WHERE id = ?
            """,
            (NOW, LATER, cleanup_id),
        )
        connection.execute(
            """
            UPDATE meeting_erasure_jobs
            SET recording_state = 'removed', cleanup_job_id = NULL,
                database_checkpointed_at = NULL, version = 2
            WHERE id = '00000000-0000-0000-0000-000000000101'
            """
        )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                """
                UPDATE meeting_erasure_jobs
                SET status = 'completed', completed_at = ?, version = 3
                WHERE id = '00000000-0000-0000-0000-000000000101'
                """,
                (UTC_NOW,),
            )
        connection.execute(
            """
            UPDATE meeting_erasure_jobs
            SET status = 'completed', database_checkpointed_at = ?,
                completed_at = ?, version = 3
            WHERE id = '00000000-0000-0000-0000-000000000101'
            """,
            (UTC_NOW, UTC_NOW),
        )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                """
                UPDATE meeting_erasure_jobs SET version = 4
                WHERE id = '00000000-0000-0000-0000-000000000101'
                """
            )


@pytest.mark.parametrize("target_status", ["ready", "running", "succeeded"])
def test_v9_terminal_cleanup_cannot_reset_outside_full_erasure_group(
    tmp_path: Path,
    target_status: str,
) -> None:
    database = Database(tmp_path / "application.sqlite3")
    database.migrate()
    cleanup_id = "00000000-0000-0000-0000-000000000302"
    with database.transaction(immediate=True) as connection:
        connection.execute(
            """
            INSERT INTO erasure_key_verifiers (
                key_id, verifier_version, verifier_digest, created_at
            ) VALUES ('current', 1, ?, ?)
            """,
            ("a" * 64, UTC_NOW),
        )
        insert_raw_cleanup(
            connection,
            cleanup_id,
            "e" * 32 + ".wav",
            status="failed",
        )
        insert_raw_erasure_job(
            connection,
            status="failed",
            recording_state="failed",
            pending_audio_asset_id=None,
            cleanup_job_id=cleanup_id,
            database_checkpointed_at=UTC_NOW,
            last_failure_code="recording_cleanup_rejected",
            last_failure_disposition="permanent",
            last_failure_occurred_at=UTC_NOW,
            completed_at=UTC_NOW,
        )
        fields = {
            "ready": (None, None, None),
            "running": ("worker", UTC_LATER, None),
            "succeeded": (None, None, LATER),
        }[target_status]
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                """
                UPDATE recording_cleanup_jobs
                SET status = ?, attempt_count = 0, lease_owner = ?,
                    lease_expires_at = ?, last_failure_json = NULL,
                    completed_at = ?
                WHERE id = ?
                """,
                (target_status, *fields, cleanup_id),
            )


def test_every_database_connection_enforces_erasure_security_pragmas(tmp_path: Path) -> None:
    database = Database(tmp_path / "application.sqlite3")
    database.migrate()

    for _ in range(3):
        with database.connect() as connection:
            assert connection.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
            assert connection.execute("PRAGMA foreign_keys").fetchone()[0] == 1
            assert connection.execute("PRAGMA secure_delete").fetchone()[0] == 1
            assert connection.execute("PRAGMA synchronous").fetchone()[0] == 2
    assert database.healthcheck()


def test_database_connect_closes_connection_when_pragma_verification_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connections: list[TrackingConnection] = []
    original_connect = sqlite3.connect

    def connect(*args: object, **kwargs: object) -> TrackingConnection:
        kwargs["factory"] = TrackingConnection
        connection = original_connect(*args, **kwargs)
        connections.append(connection)
        return connection

    def reject(_connection: sqlite3.Connection) -> None:
        raise RuntimeError("pragma mismatch")

    monkeypatch.setattr(database_module.sqlite3, "connect", connect)
    monkeypatch.setattr(database_module, "_verify_connection_pragmas", reject)
    database = Database(tmp_path / "application.sqlite3")

    with pytest.raises(RuntimeError, match="pragma mismatch"):
        database.connect()

    assert len(connections) == 1
    assert connections[0].closed


class TrackingConnection(sqlite3.Connection):
    closed = False

    def close(self) -> None:
        self.closed = True
        super().close()
