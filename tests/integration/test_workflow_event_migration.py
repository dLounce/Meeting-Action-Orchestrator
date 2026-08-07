from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from meeting_action_orchestrator.domain.enums import AudioMediaType
from meeting_action_orchestrator.domain.hashing import canonical_json
from meeting_action_orchestrator.domain.workflow_events import MeetingIngestedMetadata
from meeting_action_orchestrator.infrastructure import database as database_module
from meeting_action_orchestrator.infrastructure.database import Database

NOW = "2026-08-07T12:00:00.000000+00:00"
ASSET_ID = "74640d17-2866-45a6-adb9-7b1cac58842b"
MEETING_ID = "7d9f7fa6-b1e7-4258-a257-02836c687f38"
OTHER_MEETING_ID = "496a2f9d-40c0-4fb1-8210-b59f7399292a"
EVENT_ID = "6e18ce3d-51df-42f3-87e8-35c71dfbc7c4"
SECOND_EVENT_ID = "96ba0f5a-32f2-4c0c-ad6c-7aebf79458cf"
RECORDING_DIGEST = "a" * 64
SAFE_METADATA = canonical_json(
    MeetingIngestedMetadata(
        recording_digest=RECORDING_DIGEST,
        media_type=AudioMediaType.WAV,
        size_bytes=1_024,
        duration_ms=60_000,
    )
)
TRIGGERS = {
    "workflow_events_require_contiguous_insert",
    "workflow_events_reject_duplicate_id_insert",
    "workflow_events_reject_update",
    "workflow_events_reject_direct_delete",
}


def migrate_to_v9(database: Database, monkeypatch: pytest.MonkeyPatch) -> None:
    with monkeypatch.context() as migration_patch:
        migration_patch.setattr(database_module, "MIGRATIONS", database_module.MIGRATIONS[:9])
        assert database.migrate() == 9


def seed_meeting(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        INSERT INTO audio_assets (
            id, storage_key, original_name, media_type, size_bytes,
            duration_ms, sha256, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            ASSET_ID,
            "recording.wav",
            "private-original.wav",
            AudioMediaType.WAV.value,
            1_024,
            60_000,
            RECORDING_DIGEST,
            NOW,
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
            MEETING_ID,
            "workflow-event-migration",
            "Planning",
            ASSET_ID,
            NOW,
            "UTC",
            "[]",
            "ingested",
            0,
            NOW,
            NOW,
        ),
    )


def seed_other_meeting(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        INSERT INTO meetings (
            id, ingest_key, title, audio_asset_id, occurred_at, timezone,
            participants_json, status, version, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            OTHER_MEETING_ID,
            "workflow-event-migration-other",
            "Other planning",
            ASSET_ID,
            NOW,
            "UTC",
            "[]",
            "ingested",
            0,
            NOW,
            NOW,
        ),
    )


def insert_event(
    connection: sqlite3.Connection,
    event_id: str,
    sequence: object,
) -> None:
    connection.execute(
        """
        INSERT INTO workflow_events (
            id, meeting_id, sequence, type, actor_id, safe_metadata_json, occurred_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            event_id,
            MEETING_ID,
            sequence,
            "meeting_ingested",
            "portfolio-owner",
            SAFE_METADATA,
            NOW,
        ),
    )


def installed_triggers(connection: sqlite3.Connection) -> set[str]:
    return {
        row["name"]
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'trigger'"
        ).fetchall()
        if row["name"] in TRIGGERS
    }


def test_v10_fresh_install_is_idempotent_and_integral(tmp_path: Path) -> None:
    database = Database(tmp_path / "application.sqlite3")

    assert database.migrate() == 11
    assert database.migrate() == 11
    with database.connect() as connection:
        assert installed_triggers(connection) == TRIGGERS
        assert connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []


def test_v10_upgrade_preserves_contiguous_v9_events(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = Database(tmp_path / "application.sqlite3")
    migrate_to_v9(database, monkeypatch)
    with database.transaction(immediate=True) as connection:
        seed_meeting(connection)
        insert_event(connection, EVENT_ID, 1)
        insert_event(connection, SECOND_EVENT_ID, 2)
        before = tuple(
            dict(row)
            for row in connection.execute(
                "SELECT * FROM workflow_events ORDER BY sequence"
            ).fetchall()
        )

    assert database.migrate() == 11
    assert database.migrate() == 11
    with database.connect() as connection:
        after = tuple(
            dict(row)
            for row in connection.execute(
                "SELECT * FROM workflow_events ORDER BY sequence"
            ).fetchall()
        )
        assert installed_triggers(connection) == TRIGGERS
        assert connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"

    assert after == before


def test_v10_rejects_direct_mutation_and_gaps_but_allows_parent_cascade(
    tmp_path: Path,
) -> None:
    database = Database(tmp_path / "application.sqlite3")
    database.migrate()
    with database.transaction(immediate=True) as connection:
        seed_meeting(connection)
        insert_event(connection, EVENT_ID, 1)

    with (
        pytest.raises(sqlite3.IntegrityError, match="append-only"),
        database.transaction(immediate=True) as connection,
    ):
        connection.execute(
            "UPDATE workflow_events SET actor_id = actor_id WHERE id = ?",
            (EVENT_ID,),
        )
    with (
        pytest.raises(sqlite3.IntegrityError, match="append-only"),
        database.transaction(immediate=True) as connection,
    ):
        connection.execute("DELETE FROM workflow_events WHERE id = ?", (EVENT_ID,))
    with (
        pytest.raises(sqlite3.IntegrityError, match="not contiguous"),
        database.transaction(immediate=True) as connection,
    ):
        insert_event(connection, SECOND_EVENT_ID, 3)

    with database.transaction(immediate=True) as connection:
        assert connection.execute("SELECT COUNT(*) FROM workflow_events").fetchone()[0] == 1
        connection.execute("DELETE FROM meetings WHERE id = ?", (MEETING_ID,))
    with database.connect() as connection:
        assert connection.execute("SELECT COUNT(*) FROM workflow_events").fetchone()[0] == 0
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
        assert connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"


@pytest.mark.parametrize(
    ("replacement_meeting_id", "replacement_sequence"),
    [(MEETING_ID, 2), (OTHER_MEETING_ID, 1)],
)
def test_v10_rejects_replace_conflicts_without_mutating_the_original_event(
    tmp_path: Path,
    replacement_meeting_id: str,
    replacement_sequence: int,
) -> None:
    database = Database(tmp_path / "application.sqlite3")
    database.migrate()
    with database.transaction(immediate=True) as connection:
        seed_meeting(connection)
        seed_other_meeting(connection)
        insert_event(connection, EVENT_ID, 1)
    with database.connect() as connection:
        original = dict(
            connection.execute(
                "SELECT * FROM workflow_events WHERE id = ?",
                (EVENT_ID,),
            ).fetchone()
        )

    with (
        pytest.raises(sqlite3.IntegrityError, match="identity already exists"),
        database.transaction(immediate=True) as connection,
    ):
        connection.execute(
            """
            INSERT OR REPLACE INTO workflow_events (
                id, meeting_id, sequence, type, actor_id,
                safe_metadata_json, occurred_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                EVENT_ID,
                replacement_meeting_id,
                replacement_sequence,
                "tampered",
                "private-tampered-actor",
                "{}",
                NOW,
            ),
        )

    with database.connect() as connection:
        restored = dict(
            connection.execute(
                "SELECT * FROM workflow_events WHERE id = ?",
                (EVENT_ID,),
            ).fetchone()
        )
        other_count = connection.execute(
            "SELECT COUNT(*) FROM workflow_events WHERE meeting_id = ?",
            (OTHER_MEETING_ID,),
        ).fetchone()[0]
        assert connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"

    assert restored == original
    assert other_count == 0


@pytest.mark.parametrize("sequence", ["one", 1.5])
def test_v10_insert_trigger_rejects_non_integer_sequences(
    tmp_path: Path,
    sequence: object,
) -> None:
    database = Database(tmp_path / "application.sqlite3")
    database.migrate()
    with database.transaction(immediate=True) as connection:
        seed_meeting(connection)

    with (
        pytest.raises(sqlite3.IntegrityError, match="not contiguous"),
        database.transaction(immediate=True) as connection,
    ):
        insert_event(connection, EVENT_ID, sequence)

    with database.connect() as connection:
        assert connection.execute("SELECT COUNT(*) FROM workflow_events").fetchone()[0] == 0
        assert connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"


@pytest.mark.parametrize("sequence", ["one", 1.5])
def test_v10_upgrade_rejects_non_integer_legacy_sequences(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    sequence: object,
) -> None:
    database = Database(tmp_path / "application.sqlite3")
    migrate_to_v9(database, monkeypatch)
    with database.transaction(immediate=True) as connection:
        seed_meeting(connection)
        insert_event(connection, EVENT_ID, sequence)

    with pytest.raises(sqlite3.IntegrityError):
        database.migrate()

    with database.connect() as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 9
        assert installed_triggers(connection) == set()
        row = connection.execute(
            "SELECT sequence, typeof(sequence) AS sequence_type FROM workflow_events"
        ).fetchone()
        assert row["sequence"] == sequence
        assert row["sequence_type"] in {"text", "real"}
        assert connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"


def test_v10_rejects_legacy_gaps_without_partial_migration(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = Database(tmp_path / "application.sqlite3")
    migrate_to_v9(database, monkeypatch)
    with database.transaction(immediate=True) as connection:
        seed_meeting(connection)
        insert_event(connection, EVENT_ID, 1)
        insert_event(connection, SECOND_EVENT_ID, 3)

    with pytest.raises(sqlite3.IntegrityError):
        database.migrate()
    with database.connect() as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 9
        assert installed_triggers(connection) == set()
        assert connection.execute("SELECT COUNT(*) FROM workflow_events").fetchone()[0] == 2
        assert connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    with database.transaction(immediate=True) as connection:
        connection.execute(
            "UPDATE workflow_events SET sequence = 2 WHERE id = ?",
            (SECOND_EVENT_ID,),
        )

    assert database.migrate() == 11


def test_v10_failure_rolls_back_installed_triggers_and_retries(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = Database(tmp_path / "application.sqlite3")
    migrate_to_v9(database, monkeypatch)
    with database.transaction(immediate=True) as connection:
        seed_meeting(connection)
        insert_event(connection, EVENT_ID, 1)
    original_migrations = database_module.MIGRATIONS
    failing_v10 = database_module.SCHEMA_V10 + "\nSELECT * FROM missing_v10_table;"

    with monkeypatch.context() as migration_patch:
        migration_patch.setattr(
            database_module,
            "MIGRATIONS",
            (*original_migrations[:9], (10, failing_v10)),
        )
        with pytest.raises(sqlite3.OperationalError):
            database.migrate()

    with database.connect() as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 9
        assert installed_triggers(connection) == set()
        assert connection.execute("SELECT COUNT(*) FROM workflow_events").fetchone()[0] == 1
        assert connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"

    assert database.migrate() == 11
