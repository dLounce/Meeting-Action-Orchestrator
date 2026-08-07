from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from meeting_action_orchestrator.infrastructure import database as database_module
from meeting_action_orchestrator.infrastructure.database import SCHEMA_V1, Database


def test_migrate_creates_expected_schema(tmp_path: Path) -> None:
    database = Database(tmp_path / "application.sqlite3")

    version = database.migrate()

    with database.connect() as connection:
        rows = connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
        ).fetchall()
    names = {row["name"] for row in rows}
    assert version == 8
    assert {
        "approvals",
        "audio_assets",
        "delivery_operation_bindings",
        "ingest_request_bindings",
        "meeting_operation_bindings",
        "meetings",
        "processing_jobs",
        "recording_cleanup_jobs",
        "recap_artifacts",
        "review_revisions",
        "transcripts",
        "workflow_events",
        "write_attempts",
        "write_intents",
        "write_receipts",
    } <= names


def test_migrate_is_idempotent(tmp_path: Path) -> None:
    database = Database(tmp_path / "application.sqlite3")

    assert database.migrate() == 8
    assert database.migrate() == 8
    assert database.healthcheck()


def test_migrate_executes_trigger_bodies_as_single_statements(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    migration = """
    CREATE TABLE source_records (
        id INTEGER PRIMARY KEY,
        value TEXT NOT NULL
    );
    CREATE TABLE source_record_audit (
        source_id INTEGER NOT NULL,
        value TEXT NOT NULL
    );
    CREATE TRIGGER audit_source_record_update
    AFTER UPDATE ON source_records
    BEGIN
        INSERT INTO source_record_audit (source_id, value)
        VALUES (NEW.id, OLD.value);
        INSERT INTO source_record_audit (source_id, value)
        VALUES (NEW.id, NEW.value);
    END;
    INSERT INTO source_records (id, value) VALUES (2, 'seed;value');
    """
    monkeypatch.setattr(database_module, "MIGRATIONS", ((1, migration),))
    database = Database(tmp_path / "application.sqlite3")

    assert database.migrate() == 1
    with database.transaction() as connection:
        connection.execute("INSERT INTO source_records (id, value) VALUES (?, ?)", (1, "before"))
        connection.execute("UPDATE source_records SET value = ? WHERE id = ?", ("after", 1))
    with database.connect() as connection:
        values = connection.execute(
            "SELECT value FROM source_record_audit ORDER BY rowid"
        ).fetchall()
        seed = connection.execute("SELECT value FROM source_records WHERE id = ?", (2,)).fetchone()

    assert [row["value"] for row in values] == ["before", "after"]
    assert seed["value"] == "seed;value"


def test_migrate_does_not_repeat_trigger_migration(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    migration = """
    CREATE TABLE source_records (id INTEGER PRIMARY KEY);
    CREATE TABLE source_record_audit (source_id INTEGER NOT NULL);
    CREATE TRIGGER audit_source_record_insert
    AFTER INSERT ON source_records
    BEGIN
        INSERT INTO source_record_audit (source_id) VALUES (NEW.id);
    END;
    """
    monkeypatch.setattr(database_module, "MIGRATIONS", ((1, migration),))
    database = Database(tmp_path / "application.sqlite3")

    assert database.migrate() == 1
    assert database.migrate() == 1
    with database.transaction() as connection:
        connection.execute("INSERT INTO source_records (id) VALUES (?)", (1,))
    with database.connect() as connection:
        trigger_count = connection.execute(
            """
            SELECT COUNT(*) FROM sqlite_master
            WHERE type = 'trigger' AND name = 'audit_source_record_insert'
            """
        ).fetchone()[0]
        audit_count = connection.execute("SELECT COUNT(*) FROM source_record_audit").fetchone()[0]

    assert trigger_count == 1
    assert audit_count == 1


def test_migrate_rolls_back_trigger_migration_on_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    migration = """
    CREATE TABLE source_records (id INTEGER PRIMARY KEY);
    CREATE TABLE source_record_audit (source_id INTEGER NOT NULL);
    CREATE TRIGGER audit_source_record_insert
    AFTER INSERT ON source_records
    BEGIN
        INSERT INTO source_record_audit (source_id) VALUES (NEW.id);
    END;
    INSERT INTO missing_table (id) VALUES (1);
    """
    monkeypatch.setattr(database_module, "MIGRATIONS", ((1, migration),))
    database = Database(tmp_path / "application.sqlite3")

    with pytest.raises(sqlite3.OperationalError, match="missing_table"):
        database.migrate()

    with database.connect() as connection:
        version = connection.execute("PRAGMA user_version").fetchone()[0]
        objects = connection.execute(
            """
            SELECT name FROM sqlite_master
            WHERE name IN (
                'source_records',
                'source_record_audit',
                'audit_source_record_insert'
            )
            """
        ).fetchall()
    assert version == 0
    assert objects == []


def test_migrate_rolls_back_incomplete_trailing_sql(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    migration = """
    CREATE TABLE completed_records (id INTEGER PRIMARY KEY);
    CREATE TABLE incomplete_records (
    """
    monkeypatch.setattr(database_module, "MIGRATIONS", ((1, migration),))
    database = Database(tmp_path / "application.sqlite3")

    with pytest.raises(sqlite3.OperationalError, match="incomplete input"):
        database.migrate()

    with database.connect() as connection:
        version = connection.execute("PRAGMA user_version").fetchone()[0]
        completed_table = connection.execute(
            """
            SELECT name FROM sqlite_master
            WHERE type = 'table' AND name = 'completed_records'
            """
        ).fetchone()
    assert version == 0
    assert completed_table is None


@pytest.mark.parametrize(
    "trailing",
    ["-- trailing ; ' note", "/* trailing ; ' note */"],
)
def test_migrate_accepts_comment_only_tail(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    trailing: str,
) -> None:
    migration = f"""
    CREATE TABLE completed_records (id INTEGER PRIMARY KEY);
    {trailing}
    """
    monkeypatch.setattr(database_module, "MIGRATIONS", ((1, migration),))
    database = Database(tmp_path / "application.sqlite3")

    assert database.migrate() == 1
    with database.connect() as connection:
        completed_table = connection.execute(
            """
            SELECT name FROM sqlite_master
            WHERE type = 'table' AND name = 'completed_records'
            """
        ).fetchone()
    assert completed_table is not None


def test_migrate_rejects_lexically_incomplete_tail(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    migration = """
    CREATE TABLE completed_records (id INTEGER PRIMARY KEY);
    INSERT INTO completed_records (id) VALUES ('unterminated
    """
    monkeypatch.setattr(database_module, "MIGRATIONS", ((1, migration),))
    database = Database(tmp_path / "application.sqlite3")

    with pytest.raises(sqlite3.OperationalError, match="incomplete trailing SQL"):
        database.migrate()

    with database.connect() as connection:
        version = connection.execute("PRAGMA user_version").fetchone()[0]
        completed_table = connection.execute(
            """
            SELECT name FROM sqlite_master
            WHERE type = 'table' AND name = 'completed_records'
            """
        ).fetchone()
    assert version == 0
    assert completed_table is None


def test_migrate_upgrades_existing_version_one_database(tmp_path: Path) -> None:
    path = tmp_path / "application.sqlite3"
    with sqlite3.connect(path) as connection:
        connection.executescript(SCHEMA_V1)
        connection.execute(
            """
            INSERT INTO audio_assets (
                id, storage_key, original_name, media_type, size_bytes,
                duration_ms, sha256, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "20000000-0000-4000-8000-000000000001",
                "legacy.wav",
                "customer-private-name.wav",
                "audio/wav",
                128,
                1000,
                "a" * 64,
                "2026-08-07 09:00:00+00:00",
            ),
        )
        connection.execute("PRAGMA user_version = 1")
    database = Database(path)

    assert database.migrate() == 8
    with database.connect() as connection:
        tables = connection.execute(
            """
            SELECT name FROM sqlite_master
            WHERE type = 'table' AND name IN (
                'delivery_operation_bindings', 'meeting_operation_bindings',
                'recording_cleanup_jobs', 'ingest_request_bindings'
            )
            """
        ).fetchall()
        original_name = connection.execute(
            "SELECT original_name FROM audio_assets WHERE id = ?",
            ("20000000-0000-4000-8000-000000000001",),
        ).fetchone()["original_name"]
    assert {row["name"] for row in tables} == {
        "delivery_operation_bindings",
        "ingest_request_bindings",
        "meeting_operation_bindings",
        "recording_cleanup_jobs",
    }
    assert original_name == "recording.wav"


def test_v8_migration_leaves_legacy_meetings_unbound(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = Database(tmp_path / "application.sqlite3")
    with monkeypatch.context() as migration_patch:
        migration_patch.setattr(database_module, "MIGRATIONS", database_module.MIGRATIONS[:7])
        assert database.migrate() == 7
    with database.transaction(immediate=True) as connection:
        connection.execute(
            """
            INSERT INTO audio_assets (
                id, storage_key, original_name, media_type, size_bytes,
                duration_ms, sha256, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "20000000-0000-4000-8000-000000000001",
                "00000000000000000000000000000001.wav",
                "recording.wav",
                "audio/wav",
                128,
                1_000,
                "a" * 64,
                "2026-08-07 08:00:00+00:00",
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
                "10000000-0000-4000-8000-000000000001",
                "legacy-upload",
                "Legacy planning",
                "20000000-0000-4000-8000-000000000001",
                "2026-08-07 08:00:00+00:00",
                "UTC",
                "[]",
                "ingested",
                0,
                "2026-08-07 08:00:00+00:00",
                "2026-08-07 08:00:00+00:00",
            ),
        )

    assert database.migrate() == 8
    with database.connect() as connection:
        binding_count = connection.execute(
            "SELECT COUNT(*) FROM ingest_request_bindings"
        ).fetchone()[0]

    assert binding_count == 0


def test_migrate_adds_meeting_keyset_indexes(tmp_path: Path) -> None:
    database = Database(tmp_path / "application.sqlite3")
    database.migrate()

    with database.connect() as connection:
        rows = connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'meetings'"
        ).fetchall()

    names = {row["name"] for row in rows}
    assert "idx_meetings_created_id" in names
    assert "idx_meetings_status_created_id" in names


def test_transaction_rolls_back_on_failure(tmp_path: Path) -> None:
    database = Database(tmp_path / "application.sqlite3")
    database.migrate()

    with pytest.raises(RuntimeError, match="stop"), database.transaction() as connection:
        connection.execute(
            "INSERT INTO audio_assets VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            ("asset", "key", "meeting.wav", "audio/wav", 1, None, "digest", "now"),
        )
        raise RuntimeError("stop")

    with database.connect() as connection:
        count = connection.execute("SELECT COUNT(*) FROM audio_assets").fetchone()[0]
    assert count == 0


def test_foreign_keys_are_enforced(tmp_path: Path) -> None:
    database = Database(tmp_path / "application.sqlite3")
    database.migrate()

    with pytest.raises(sqlite3.IntegrityError), database.transaction() as connection:
        connection.execute(
            """
                INSERT INTO meetings (
                    id, ingest_key, title, audio_asset_id, occurred_at, timezone,
                    participants_json, status, version, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
            (
                "meeting",
                "ingest",
                "Planning",
                "missing",
                "2026-08-07T08:00:00+00:00",
                "UTC",
                "[]",
                "ingested",
                1,
                "2026-08-07T08:00:00+00:00",
                "2026-08-07T08:00:00+00:00",
            ),
        )
