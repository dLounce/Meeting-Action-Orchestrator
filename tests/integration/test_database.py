from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from meeting_action_orchestrator.infrastructure.database import SCHEMA_V1, Database


def test_migrate_creates_expected_schema(tmp_path: Path) -> None:
    database = Database(tmp_path / "application.sqlite3")

    version = database.migrate()

    with database.connect() as connection:
        rows = connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
        ).fetchall()
    names = {row["name"] for row in rows}
    assert version == 3
    assert {
        "approvals",
        "audio_assets",
        "delivery_operation_bindings",
        "meetings",
        "processing_jobs",
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

    assert database.migrate() == 3
    assert database.migrate() == 3
    assert database.healthcheck()


def test_migrate_upgrades_existing_version_one_database(tmp_path: Path) -> None:
    path = tmp_path / "application.sqlite3"
    with sqlite3.connect(path) as connection:
        connection.executescript(SCHEMA_V1)
        connection.execute("PRAGMA user_version = 1")
    database = Database(path)

    assert database.migrate() == 3
    with database.connect() as connection:
        table = connection.execute(
            """
            SELECT name FROM sqlite_master
            WHERE type = 'table' AND name = 'delivery_operation_bindings'
            """
        ).fetchone()
    assert table is not None


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
