from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID

import pytest

from meeting_action_orchestrator.domain.enums import AudioMediaType
from meeting_action_orchestrator.domain.models import (
    AudioAsset,
    IngestAudioIdentity,
    IngestRequestBinding,
    IngestRequestIdentity,
    Meeting,
)
from meeting_action_orchestrator.infrastructure.database import Database
from meeting_action_orchestrator.infrastructure.repositories import SqliteUnitOfWork

NOW = datetime(2026, 8, 7, 8, 0, tzinfo=timezone.utc)
ASSET_ID = UUID("20000000-0000-4000-8000-000000000001")
MEETING_ID = UUID("10000000-0000-4000-8000-000000000001")


def asset() -> AudioAsset:
    return AudioAsset(
        id=ASSET_ID,
        storage_key="00000000000000000000000000000001.wav",
        original_name="recording.wav",
        detected_media_type=AudioMediaType.WAV,
        size_bytes=128,
        duration_ms=1_000,
        sha256="a" * 64,
        created_at=NOW,
    )


def meeting(ingest_key: str = "upload-one") -> Meeting:
    return Meeting(
        id=MEETING_ID,
        ingest_key=ingest_key,
        title="Planning",
        audio_asset_id=ASSET_ID,
        occurred_at=NOW,
        timezone="UTC",
        created_at=NOW,
        updated_at=NOW,
    )


def binding(
    ingest_key: str = "upload-one",
    fingerprint_version: int = 1,
) -> IngestRequestBinding:
    if fingerprint_version == 1:
        request = IngestRequestIdentity(
            ingest_key=ingest_key,
            title="Planning",
            occurred_at=NOW,
            timezone="UTC",
        )
        return IngestRequestBinding.create(
            request,
            IngestAudioIdentity(sha256="a" * 64, size_bytes=128),
            NOW,
        )
    return IngestRequestBinding(
        ingest_key=ingest_key,
        fingerprint_version=fingerprint_version,
        request_fingerprint="b" * 64,
        created_at=NOW,
    )


def database(tmp_path: Path) -> Database:
    result = Database(tmp_path / "application.sqlite3")
    result.migrate()
    return result


def test_ingest_request_binding_round_trips_with_its_meeting(tmp_path: Path) -> None:
    store = database(tmp_path)
    expected = binding()

    with SqliteUnitOfWork(store) as uow:
        uow.audio_assets.add(asset())
        uow.meetings.add(meeting())
        uow.ingest_requests.add(expected)
        uow.commit()

    with SqliteUnitOfWork(store, immediate=False) as uow:
        loaded = uow.ingest_requests.get(expected.ingest_key)

    assert loaded == expected


def test_ingest_request_binding_supports_future_positive_versions(tmp_path: Path) -> None:
    store = database(tmp_path)
    expected = binding(fingerprint_version=2)

    with SqliteUnitOfWork(store) as uow:
        uow.audio_assets.add(asset())
        uow.meetings.add(meeting())
        uow.ingest_requests.add(expected)
        uow.commit()

    with SqliteUnitOfWork(store, immediate=False) as uow:
        assert uow.ingest_requests.get(expected.ingest_key) == expected


def test_ingest_request_binding_rolls_back_with_its_meeting(tmp_path: Path) -> None:
    store = database(tmp_path)

    with SqliteUnitOfWork(store) as uow:
        uow.audio_assets.add(asset())
        uow.meetings.add(meeting())
        uow.ingest_requests.add(binding())

    with SqliteUnitOfWork(store, immediate=False) as uow:
        assert uow.meetings.get(MEETING_ID) is None
        assert uow.ingest_requests.get("upload-one") is None


def test_ingest_request_binding_requires_an_existing_meeting_key(tmp_path: Path) -> None:
    store = database(tmp_path)

    with pytest.raises(sqlite3.IntegrityError, match="FOREIGN KEY"), SqliteUnitOfWork(store) as uow:
        uow.ingest_requests.add(binding())
        uow.commit()


def test_ingest_request_binding_cannot_be_updated(tmp_path: Path) -> None:
    store = database(tmp_path)
    with SqliteUnitOfWork(store) as uow:
        uow.audio_assets.add(asset())
        uow.meetings.add(meeting())
        uow.ingest_requests.add(binding())
        uow.commit()

    with (
        pytest.raises(sqlite3.IntegrityError, match="immutable"),
        store.transaction(immediate=True) as connection,
    ):
        connection.execute(
            """
            UPDATE ingest_request_bindings
            SET request_fingerprint = ? WHERE ingest_key = ?
            """,
            ("b" * 64, "upload-one"),
        )


def test_ingest_request_binding_is_deleted_with_its_meeting(tmp_path: Path) -> None:
    store = database(tmp_path)
    with SqliteUnitOfWork(store) as uow:
        uow.audio_assets.add(asset())
        uow.meetings.add(meeting())
        uow.ingest_requests.add(binding())
        uow.commit()

    with store.transaction(immediate=True) as connection:
        connection.execute("DELETE FROM meetings WHERE id = ?", (str(MEETING_ID),))

    with SqliteUnitOfWork(store, immediate=False) as uow:
        assert uow.ingest_requests.get("upload-one") is None


@pytest.mark.parametrize(
    ("fingerprint_version", "request_fingerprint"),
    [
        (0, "a" * 64),
        (-1, "a" * 64),
        (1, "A" * 64),
        (1, "a" * 63),
    ],
)
def test_ingest_request_binding_constraints_reject_invalid_identity(
    tmp_path: Path,
    fingerprint_version: int,
    request_fingerprint: str,
) -> None:
    store = database(tmp_path)
    with SqliteUnitOfWork(store) as uow:
        uow.audio_assets.add(asset())
        uow.meetings.add(meeting())
        uow.commit()

    with pytest.raises(sqlite3.IntegrityError), store.transaction(immediate=True) as connection:
        connection.execute(
            """
            INSERT INTO ingest_request_bindings (
                ingest_key, fingerprint_version, request_fingerprint, created_at
            ) VALUES (?, ?, ?, ?)
            """,
            (
                "upload-one",
                fingerprint_version,
                request_fingerprint,
                str(NOW),
            ),
        )
