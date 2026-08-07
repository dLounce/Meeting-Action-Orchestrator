from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

from meeting_action_orchestrator.domain.enums import MeetingOperationKind, ProcessingStage
from meeting_action_orchestrator.domain.hashing import canonical_sha256
from meeting_action_orchestrator.domain.models import MeetingOperationBinding
from meeting_action_orchestrator.infrastructure.repositories import SqliteUnitOfWork
from tests.integration.test_processing_jobs import MEETING_ID, create_database

NOW = datetime(2026, 8, 7, 15, 0, tzinfo=timezone.utc)


def binding(*, request_key: str = "meeting-operation-one") -> MeetingOperationBinding:
    identity = {
        "actor_id": "owner",
        "expected_version": 0,
        "meeting_id": MEETING_ID,
        "operation": MeetingOperationKind.PROCESSING_RETRY,
        "request_key": request_key,
        "stage": ProcessingStage.TRANSCRIPTION,
    }
    return MeetingOperationBinding(
        request_key=request_key,
        meeting_id=MEETING_ID,
        operation=MeetingOperationKind.PROCESSING_RETRY,
        actor_id="owner",
        stage=ProcessingStage.TRANSCRIPTION,
        expected_version=0,
        request_fingerprint=canonical_sha256(identity),
        created_at=NOW,
    )


def test_meeting_operation_binding_round_trips_and_rolls_back(tmp_path: Path) -> None:
    database = create_database(tmp_path / "application.sqlite3")
    original = binding()

    with SqliteUnitOfWork(database) as uow:
        uow.meeting_operations.add(original)
        uow.commit()
    with SqliteUnitOfWork(database) as uow:
        restored = uow.meeting_operations.get(original.request_key)
    with pytest.raises(RuntimeError, match="stop"), SqliteUnitOfWork(database) as uow:
        uow.meeting_operations.add(binding(request_key="rolled-back"))
        raise RuntimeError("stop")
    with SqliteUnitOfWork(database) as uow:
        rolled_back = uow.meeting_operations.get("rolled-back")

    assert restored == original
    assert rolled_back is None


def test_meeting_operation_key_is_unique_and_cascades_with_meeting(tmp_path: Path) -> None:
    database = create_database(tmp_path / "application.sqlite3")
    original = binding()

    with SqliteUnitOfWork(database) as uow:
        uow.meeting_operations.add(original)
        uow.commit()
    with pytest.raises(sqlite3.IntegrityError), SqliteUnitOfWork(database) as uow:
        uow.meeting_operations.add(original)
    with database.transaction(immediate=True) as connection:
        connection.execute("DELETE FROM meetings WHERE id = ?", (str(MEETING_ID),))
    with SqliteUnitOfWork(database) as uow:
        restored = uow.meeting_operations.get(original.request_key)

    assert restored is None


def test_meeting_operation_schema_rejects_invalid_stage_pair(tmp_path: Path) -> None:
    database = create_database(tmp_path / "application.sqlite3")

    with pytest.raises(sqlite3.IntegrityError), database.transaction(immediate=True) as connection:
        connection.execute(
            """
            INSERT INTO meeting_operation_bindings (
                request_key, meeting_id, operation, actor_id, stage,
                expected_version, request_fingerprint, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "invalid-cancellation",
                str(MEETING_ID),
                "cancellation",
                "owner",
                "transcription",
                0,
                "a" * 64,
                str(NOW),
            ),
        )
