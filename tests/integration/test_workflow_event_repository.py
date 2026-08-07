from __future__ import annotations

import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Barrier
from uuid import UUID

import pytest

from meeting_action_orchestrator.application.ports import WorkflowEventCursor
from meeting_action_orchestrator.domain.enums import (
    AudioMediaType,
    MeetingStatus,
    ProcessingStage,
)
from meeting_action_orchestrator.domain.hashing import canonical_json
from meeting_action_orchestrator.domain.models import AudioAsset, Meeting
from meeting_action_orchestrator.domain.workflow_events import (
    MeetingIngestedMetadata,
    MeetingTransitionMetadata,
    ProcessingAttemptMetadata,
    ProcessingAuditOutcome,
    ProcessingRetryRequestedMetadata,
    WorkflowEvent,
    WorkflowEventDraft,
    WorkflowEventType,
)
from meeting_action_orchestrator.infrastructure.database import Database
from meeting_action_orchestrator.infrastructure.repositories import SqliteUnitOfWork
from meeting_action_orchestrator.infrastructure.workflow_events import (
    WORKFLOW_EVENT_PAGE_LIMIT,
    WorkflowEventIntegrityError,
    WorkflowEventWriteModeError,
)

NOW = datetime(2026, 8, 7, 12, 0, tzinfo=timezone.utc)
UTC_NOW = NOW.isoformat(timespec="microseconds")
ASSET_ID = UUID("74640d17-2866-45a6-adb9-7b1cac58842b")
MEETING_ID = UUID("7d9f7fa6-b1e7-4258-a257-02836c687f38")
SECOND_MEETING_ID = UUID("496a2f9d-40c0-4fb1-8210-b59f7399292a")
RAW_EVENT_ID = "6e18ce3d-51df-42f3-87e8-35c71dfbc7c4"
RECORDING_DIGEST = "a" * 64
CANONICAL_INGESTED_METADATA = canonical_json(
    MeetingIngestedMetadata(
        recording_digest=RECORDING_DIGEST,
        media_type=AudioMediaType.WAV,
        size_bytes=1_024,
        duration_ms=60_000,
    )
)


def create_database(path: Path) -> Database:
    database = Database(path)
    database.migrate()
    with SqliteUnitOfWork(database) as uow:
        uow.audio_assets.add(
            AudioAsset(
                id=ASSET_ID,
                storage_key="recording.wav",
                original_name="private-original.wav",
                detected_media_type=AudioMediaType.WAV,
                size_bytes=1_024,
                duration_ms=60_000,
                sha256=RECORDING_DIGEST,
                created_at=NOW,
            )
        )
        for meeting_id, ingest_key in (
            (MEETING_ID, "workflow-event-one"),
            (SECOND_MEETING_ID, "workflow-event-two"),
        ):
            uow.meetings.add(
                Meeting(
                    id=meeting_id,
                    ingest_key=ingest_key,
                    title="Planning",
                    audio_asset_id=ASSET_ID,
                    occurred_at=NOW,
                    timezone="UTC",
                    created_at=NOW,
                    updated_at=NOW,
                )
            )
        uow.commit()
    return database


def draft(
    ordinal: int = 0,
    *,
    meeting_id: UUID = MEETING_ID,
    actor_id: str | None = "portfolio-owner",
) -> WorkflowEventDraft:
    if ordinal == 0:
        return WorkflowEventDraft(
            meeting_id=meeting_id,
            type=WorkflowEventType.MEETING_INGESTED,
            actor_id=actor_id,
            safe_metadata=MeetingIngestedMetadata(
                recording_digest=RECORDING_DIGEST,
                media_type=AudioMediaType.WAV,
                size_bytes=1_024,
                duration_ms=60_000,
            ),
            occurred_at=NOW,
        )
    return WorkflowEventDraft(
        meeting_id=meeting_id,
        type=WorkflowEventType.MEETING_TRANSITIONED,
        actor_id=actor_id,
        safe_metadata=MeetingTransitionMetadata(
            previous_status=MeetingStatus.AWAITING_APPROVAL,
            current_status=MeetingStatus.AWAITING_APPROVAL,
            meeting_version=ordinal,
        ),
        occurred_at=NOW + timedelta(microseconds=ordinal),
    )


def append(database: Database, value: WorkflowEventDraft) -> WorkflowEvent:
    with SqliteUnitOfWork(database) as uow:
        event = uow.workflow_events.append(value)
        uow.commit()
    return event


def test_event_round_trips_with_canonical_safe_metadata(tmp_path: Path) -> None:
    database = create_database(tmp_path / "application.sqlite3")
    original = append(database, draft())

    with SqliteUnitOfWork(database, immediate=False) as uow:
        restored = tuple(uow.workflow_events.list_page(MEETING_ID, cursor=None, limit=10))
    with database.connect() as connection:
        row = connection.execute(
            "SELECT actor_id, safe_metadata_json, occurred_at FROM workflow_events"
        ).fetchone()

    assert restored == (original,)
    assert row is not None
    assert row["actor_id"] == "portfolio-owner"
    assert json.loads(row["safe_metadata_json"]) == {
        "duration_ms": 60_000,
        "kind": "meeting-ingested/v1",
        "media_type": "audio/wav",
        "recording_digest": RECORDING_DIGEST,
        "size_bytes": 1_024,
    }
    assert "private-original.wav" not in row["safe_metadata_json"]
    assert "recording.wav" not in row["safe_metadata_json"]
    assert row["occurred_at"] == UTC_NOW


def test_latest_processing_event_is_stage_scoped_and_returns_retry_epoch(
    tmp_path: Path,
) -> None:
    database = create_database(tmp_path / "application.sqlite3")
    append(
        database,
        WorkflowEventDraft(
            meeting_id=MEETING_ID,
            type=WorkflowEventType.PROCESSING_ATTEMPTED,
            safe_metadata=ProcessingAttemptMetadata(
                stage=ProcessingStage.TRANSCRIPTION,
                attempt_number=1,
                outcome=ProcessingAuditOutcome.STARTED,
                input_digest="b" * 64,
            ),
            occurred_at=NOW,
        ),
    )
    extraction = append(
        database,
        WorkflowEventDraft(
            meeting_id=MEETING_ID,
            type=WorkflowEventType.PROCESSING_ATTEMPTED,
            safe_metadata=ProcessingAttemptMetadata(
                stage=ProcessingStage.EXTRACTION,
                attempt_number=1,
                outcome=ProcessingAuditOutcome.STARTED,
                input_digest="c" * 64,
            ),
            occurred_at=NOW,
        ),
    )
    retry = append(
        database,
        WorkflowEventDraft(
            meeting_id=MEETING_ID,
            type=WorkflowEventType.PROCESSING_RETRY_REQUESTED,
            actor_id="portfolio-owner",
            safe_metadata=ProcessingRetryRequestedMetadata(
                stage=ProcessingStage.TRANSCRIPTION,
                previous_attempt_count=1,
                meeting_version=3,
            ),
            occurred_at=NOW,
        ),
    )

    with SqliteUnitOfWork(database, immediate=False) as uow:
        latest_transcription = uow.workflow_events.latest_processing_event(
            MEETING_ID,
            ProcessingStage.TRANSCRIPTION,
        )
        latest_extraction = uow.workflow_events.latest_processing_event(
            MEETING_ID,
            ProcessingStage.EXTRACTION,
        )
        missing = uow.workflow_events.latest_processing_event(
            SECOND_MEETING_ID,
            ProcessingStage.TRANSCRIPTION,
        )

    assert latest_transcription == retry
    assert latest_extraction == extraction
    assert missing is None


def test_latest_processing_event_rejects_corrupted_selected_row(tmp_path: Path) -> None:
    database = create_database(tmp_path / "application.sqlite3")
    event = append(
        database,
        WorkflowEventDraft(
            meeting_id=MEETING_ID,
            type=WorkflowEventType.PROCESSING_ATTEMPTED,
            safe_metadata=ProcessingAttemptMetadata(
                stage=ProcessingStage.TRANSCRIPTION,
                attempt_number=1,
                outcome=ProcessingAuditOutcome.STARTED,
                input_digest="b" * 64,
            ),
            occurred_at=NOW,
        ),
    )
    with database.transaction(immediate=True) as connection:
        connection.execute("DROP TRIGGER workflow_events_reject_update")
        connection.execute(
            """
            UPDATE workflow_events
            SET safe_metadata_json = ?
            WHERE id = ?
            """,
            (
                json.dumps(event.safe_metadata.model_dump(mode="json"), indent=2),
                str(event.id),
            ),
        )

    with (
        SqliteUnitOfWork(database, immediate=False) as uow,
        pytest.raises(WorkflowEventIntegrityError),
    ):
        uow.workflow_events.latest_processing_event(
            MEETING_ID,
            ProcessingStage.TRANSCRIPTION,
        )


def test_event_append_normalizes_offset_timestamp_before_return(tmp_path: Path) -> None:
    database = create_database(tmp_path / "application.sqlite3")
    offset = timezone(timedelta(hours=5, minutes=30))
    value = draft().model_copy(update={"occurred_at": datetime(2026, 8, 7, 17, 30, tzinfo=offset)})
    original = append(database, value)

    with SqliteUnitOfWork(database, immediate=False) as uow:
        restored = tuple(uow.workflow_events.list_page(MEETING_ID, cursor=None, limit=10))

    assert restored == (original,)
    assert original.occurred_at.isoformat(timespec="microseconds") == (
        "2026-08-07T12:00:00.000000+00:00"
    )


def test_event_append_rolls_back_without_consuming_sequence(tmp_path: Path) -> None:
    database = create_database(tmp_path / "application.sqlite3")

    with pytest.raises(RuntimeError, match="stop"), SqliteUnitOfWork(database) as uow:
        uow.workflow_events.append(draft())
        raise RuntimeError("stop")

    event = append(database, draft())

    assert event.sequence == 1


def test_event_pages_are_stable_and_meeting_scoped(tmp_path: Path) -> None:
    database = create_database(tmp_path / "application.sqlite3")
    events = tuple(append(database, draft(ordinal)) for ordinal in range(6))
    append(database, draft(meeting_id=SECOND_MEETING_ID))

    with SqliteUnitOfWork(database, immediate=False) as uow:
        first = tuple(uow.workflow_events.list_page(MEETING_ID, cursor=None, limit=2))
        cursor = WorkflowEventCursor(
            meeting_id=MEETING_ID,
            sequence=first[-1].sequence,
        )
        second = tuple(uow.workflow_events.list_page(MEETING_ID, cursor=cursor, limit=2))
        remaining = tuple(
            uow.workflow_events.list_page(
                MEETING_ID,
                cursor=WorkflowEventCursor(
                    meeting_id=MEETING_ID,
                    sequence=second[-1].sequence,
                ),
                limit=10,
            )
        )

    assert first + second + remaining == events
    assert [event.sequence for event in first + second + remaining] == list(range(1, 7))


def test_event_page_rejects_cross_meeting_cursor_and_unbounded_limit(
    tmp_path: Path,
) -> None:
    database = create_database(tmp_path / "application.sqlite3")

    with SqliteUnitOfWork(database, immediate=False) as uow:
        with pytest.raises(ValueError, match="another meeting"):
            uow.workflow_events.list_page(
                MEETING_ID,
                cursor=WorkflowEventCursor(meeting_id=SECOND_MEETING_ID, sequence=1),
                limit=10,
            )
        for limit in (True, 0, WORKFLOW_EVENT_PAGE_LIMIT + 1):
            with pytest.raises(ValueError, match="page limit"):
                uow.workflow_events.list_page(
                    MEETING_ID,
                    cursor=None,
                    limit=limit,
                )


def test_event_append_requires_immediate_unit_of_work(tmp_path: Path) -> None:
    database = create_database(tmp_path / "application.sqlite3")

    with (
        pytest.raises(WorkflowEventWriteModeError),
        SqliteUnitOfWork(
            database,
            immediate=False,
        ) as uow,
    ):
        uow.workflow_events.append(draft())


@pytest.mark.parametrize("operation", ["commit", "rollback"])
def test_event_append_rejects_a_finished_unit_of_work(
    tmp_path: Path,
    operation: str,
) -> None:
    database = create_database(tmp_path / "application.sqlite3")

    with SqliteUnitOfWork(database) as uow:
        getattr(uow, operation)()
        with pytest.raises(WorkflowEventWriteModeError, match="immediate unit of work"):
            uow.workflow_events.append(draft())

    with database.connect() as connection:
        assert connection.execute("SELECT COUNT(*) FROM workflow_events").fetchone()[0] == 0


def test_concurrent_event_appends_allocate_contiguous_sequences(tmp_path: Path) -> None:
    database = create_database(tmp_path / "application.sqlite3")
    worker_count = 8
    barrier = Barrier(worker_count)

    def write(ordinal: int) -> WorkflowEvent:
        barrier.wait()
        return append(database, draft(ordinal + 1, actor_id=None))

    with ThreadPoolExecutor(max_workers=worker_count) as pool:
        events = tuple(pool.map(write, range(worker_count)))

    with SqliteUnitOfWork(database, immediate=False) as uow:
        stored = tuple(
            uow.workflow_events.list_page(
                MEETING_ID,
                cursor=None,
                limit=worker_count,
            )
        )

    assert sorted(event.sequence for event in events) == list(range(1, worker_count + 1))
    assert [event.sequence for event in stored] == list(range(1, worker_count + 1))
    assert {event.id for event in stored} == {event.id for event in events}


def test_events_cascade_with_their_meeting(tmp_path: Path) -> None:
    database = create_database(tmp_path / "application.sqlite3")
    append(database, draft())

    with database.transaction(immediate=True) as connection:
        connection.execute("DELETE FROM meetings WHERE id = ?", (str(MEETING_ID),))
    with database.connect() as connection:
        count = connection.execute(
            "SELECT COUNT(*) FROM workflow_events WHERE meeting_id = ?",
            (str(MEETING_ID),),
        ).fetchone()[0]

    assert count == 0


@pytest.mark.parametrize(
    ("column", "value"),
    [
        ("id", None),
        ("id", 123),
        ("id", RAW_EVENT_ID.upper()),
        ("id", f"{{{RAW_EVENT_ID}}}"),
        ("sequence", "one"),
        ("type", 123),
        ("occurred_at", 123),
        ("occurred_at", NOW.isoformat()),
        ("occurred_at", "2026-08-07T17:30:00.000000+05:30"),
        ("actor_id", b"private-transcript-marker"),
        ("actor_id", " private-transcript-marker "),
        ("safe_metadata_json", CANONICAL_INGESTED_METADATA.encode("ascii")),
        (
            "safe_metadata_json",
            '{"duration_ms":60000,"kind":"meeting-ingested/v1",'
            '"media_type":"private-transcript-marker","media_type":"audio/wav",'
            f'"recording_digest":"{RECORDING_DIGEST}",'
            '"size_bytes":1024}',
        ),
        (
            "safe_metadata_json",
            json.dumps(json.loads(CANONICAL_INGESTED_METADATA), sort_keys=True),
        ),
        (
            "safe_metadata_json",
            '{"kind":"meeting-ingested/v1","media_type":"audio/wav",'
            f'"recording_digest":"{RECORDING_DIGEST}",'
            '"size_bytes":1024,"duration_ms":60000,'
            '"transcript":"private-transcript-marker"}',
        ),
    ],
)
def test_malformed_stored_rows_fail_closed(
    tmp_path: Path,
    column: str,
    value: object,
) -> None:
    database = create_database(tmp_path / "application.sqlite3")
    marker = "private-transcript-marker"
    control = append(database, draft(actor_id=None))
    with SqliteUnitOfWork(database, immediate=False) as uow:
        assert tuple(uow.workflow_events.list_page(MEETING_ID, cursor=None, limit=10)) == (control,)
    values: dict[str, object] = {
        "id": RAW_EVENT_ID,
        "meeting_id": str(MEETING_ID),
        "sequence": 2,
        "type": WorkflowEventType.MEETING_INGESTED.value,
        "actor_id": marker,
        "safe_metadata_json": CANONICAL_INGESTED_METADATA,
        "occurred_at": UTC_NOW,
    }
    values[column] = value
    with database.transaction(immediate=True) as connection:
        connection.execute("DROP TRIGGER workflow_events_require_contiguous_insert")
        connection.execute(
            """
            INSERT INTO workflow_events (
                id, meeting_id, sequence, type, actor_id, safe_metadata_json, occurred_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                values["id"],
                values["meeting_id"],
                values["sequence"],
                values["type"],
                values["actor_id"],
                values["safe_metadata_json"],
                values["occurred_at"],
            ),
        )

    with (
        SqliteUnitOfWork(database, immediate=False) as uow,
        pytest.raises(WorkflowEventIntegrityError) as failure,
    ):
        uow.workflow_events.list_page(MEETING_ID, cursor=None, limit=10)

    assert marker not in str(failure.value)
    assert failure.value.__cause__ is None
    assert failure.value.__context__ is None


def test_reader_rejects_a_corrupted_legacy_sequence_gap(tmp_path: Path) -> None:
    database = create_database(tmp_path / "application.sqlite3")
    append(database, draft(actor_id=None))
    marker = "private-gap-marker"
    with database.transaction(immediate=True) as connection:
        connection.execute("DROP TRIGGER workflow_events_require_contiguous_insert")
        connection.execute(
            """
            INSERT INTO workflow_events (
                id, meeting_id, sequence, type, actor_id, safe_metadata_json, occurred_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                RAW_EVENT_ID,
                str(MEETING_ID),
                3,
                WorkflowEventType.MEETING_INGESTED.value,
                marker,
                CANONICAL_INGESTED_METADATA,
                UTC_NOW,
            ),
        )

    with (
        SqliteUnitOfWork(database, immediate=False) as uow,
        pytest.raises(WorkflowEventIntegrityError) as failure,
    ):
        uow.workflow_events.list_page(MEETING_ID, cursor=None, limit=10)

    assert "not contiguous" in str(failure.value)
    assert marker not in str(failure.value)
    assert failure.value.__cause__ is None
    assert failure.value.__context__ is None


def test_schema_rejects_duplicate_event_sequence(tmp_path: Path) -> None:
    database = create_database(tmp_path / "application.sqlite3")
    original = append(database, draft())

    with pytest.raises(sqlite3.IntegrityError), database.transaction(immediate=True) as connection:
        connection.execute(
            """
            INSERT INTO workflow_events (
                id, meeting_id, sequence, type, actor_id, safe_metadata_json, occurred_at
            ) SELECT ?, meeting_id, sequence, type, actor_id, safe_metadata_json, occurred_at
            FROM workflow_events WHERE id = ?
            """,
            ("1e3a308f-7b72-4812-becd-16a60a55cc5e", str(original.id)),
        )
