from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID

import pytest

from meeting_action_orchestrator.application.ports import MeetingListCursor
from meeting_action_orchestrator.application.state_machine import transition_meeting
from meeting_action_orchestrator.domain.enums import (
    AudioMediaType,
    MeetingStatus,
    ProviderUsageKind,
    ReviewOrigin,
)
from meeting_action_orchestrator.domain.models import (
    AudioAsset,
    Decision,
    EvidenceRef,
    Meeting,
    ReviewRevision,
    Transcript,
    TranscriptSegment,
)
from meeting_action_orchestrator.domain.provider_budget import ProviderUsage
from meeting_action_orchestrator.infrastructure.database import Database
from meeting_action_orchestrator.infrastructure.repositories import (
    PersistenceConflictError,
    SqliteUnitOfWork,
)

NOW = datetime(2026, 8, 7, 8, 0, tzinfo=timezone.utc)
ASSET_ID = UUID("83bde818-5f19-48fb-967f-42cf40671cf7")
MEETING_ID = UUID("2be66883-5a9c-4e07-b807-bf864d2c69c4")
TRANSCRIPT_ID = UUID("0f49c55c-e7c2-481a-80a8-6a0a198db6c2")
SEGMENT_ID = UUID("23558eb3-37bd-48b6-9094-09507ef72735")
REVIEW_ID = UUID("bc960e9e-17af-4464-aa30-fd6a5671fedc")
DECISION_ID = UUID("d9ef94d0-57d6-4f9a-87bc-812cb0a6dc20")


def asset() -> AudioAsset:
    return AudioAsset(
        id=ASSET_ID,
        storage_key="recording.wav",
        original_name="recording.wav",
        detected_media_type=AudioMediaType.WAV,
        size_bytes=1024,
        duration_ms=60_000,
        sha256="a" * 64,
        created_at=NOW,
    )


def meeting() -> Meeting:
    return Meeting(
        id=MEETING_ID,
        ingest_key="upload-1",
        title="Planning",
        audio_asset_id=ASSET_ID,
        occurred_at=NOW,
        timezone="UTC",
        created_at=NOW,
        updated_at=NOW,
    )


def transcript() -> Transcript:
    text = "The team approved the revised launch plan."
    return Transcript(
        id=TRANSCRIPT_ID,
        meeting_id=MEETING_ID,
        audio_asset_id=ASSET_ID,
        provider="openai",
        model="transcribe-test",
        language="en",
        text=text,
        segments=(
            TranscriptSegment(
                id=SEGMENT_ID,
                ordinal=0,
                start_ms=0,
                end_ms=2500,
                speaker="Speaker 1",
                text=text,
            ),
        ),
        created_at=NOW,
    )


def review() -> ReviewRevision:
    return ReviewRevision(
        id=REVIEW_ID,
        meeting_id=MEETING_ID,
        transcript_id=TRANSCRIPT_ID,
        revision_number=1,
        origin=ReviewOrigin.MODEL,
        recap_markdown="# Planning\n\nThe revised launch plan was approved.\n",
        decisions=(
            Decision(
                id=DECISION_ID,
                summary="Approve the revised launch plan",
                evidence=(
                    EvidenceRef(
                        segment_ids=(SEGMENT_ID,),
                        quote="approved the revised launch plan",
                    ),
                ),
                confidence=0.95,
            ),
        ),
        created_at=NOW,
    )


def test_round_trips_meeting_transcript_and_review(tmp_path: Path) -> None:
    database = Database(tmp_path / "application.sqlite3")
    database.migrate()
    original_meeting = meeting()
    original_transcript = transcript()
    original_review = review()

    with SqliteUnitOfWork(database) as uow:
        uow.audio_assets.add(asset())
        uow.meetings.add(original_meeting)
        uow.transcripts.add(original_transcript)
        uow.reviews.add(original_review)
        uow.commit()

    with SqliteUnitOfWork(database) as uow:
        assert uow.audio_assets.get(ASSET_ID) == asset()
        assert uow.meetings.get(MEETING_ID) == original_meeting
        assert uow.transcripts.get(TRANSCRIPT_ID) == original_transcript
        assert uow.reviews.get(REVIEW_ID) == original_review


@pytest.mark.parametrize(
    "usage",
    [
        ProviderUsage(
            kind=ProviderUsageKind.TOKENS,
            input_tokens=120,
            output_tokens=30,
        ),
        ProviderUsage(
            kind=ProviderUsageKind.DURATION,
            audio_duration_ms=60_001,
        ),
    ],
)
def test_round_trips_strict_transcription_usage(
    tmp_path: Path,
    usage: ProviderUsage,
) -> None:
    database = Database(tmp_path / "application.sqlite3")
    database.migrate()
    original = transcript().model_copy(update={"usage": usage})

    with SqliteUnitOfWork(database) as uow:
        uow.audio_assets.add(asset())
        uow.meetings.add(meeting())
        uow.transcripts.add(original)
        uow.commit()

    with SqliteUnitOfWork(database) as uow:
        assert uow.transcripts.get(TRANSCRIPT_ID) == original


def test_loads_legacy_empty_transcription_usage_as_absent(tmp_path: Path) -> None:
    database = Database(tmp_path / "application.sqlite3")
    database.migrate()

    with SqliteUnitOfWork(database) as uow:
        uow.audio_assets.add(asset())
        uow.meetings.add(meeting())
        uow.transcripts.add(transcript())
        uow.commit()
    with database.transaction() as connection:
        connection.execute(
            "UPDATE transcripts SET usage_json = '{}' WHERE id = ?",
            (str(TRANSCRIPT_ID),),
        )

    with SqliteUnitOfWork(database) as uow:
        persisted = uow.transcripts.get(TRANSCRIPT_ID)
    assert persisted is not None
    assert persisted.usage is None


def test_rejects_stale_meeting_update(tmp_path: Path) -> None:
    database = Database(tmp_path / "application.sqlite3")
    database.migrate()
    original = meeting()

    with SqliteUnitOfWork(database) as uow:
        uow.audio_assets.add(asset())
        uow.meetings.add(original)
        uow.commit()

    cancelled = transition_meeting(original, MeetingStatus.CANCELLED, NOW)
    with SqliteUnitOfWork(database) as uow:
        uow.meetings.save(cancelled, expected_version=0)
        uow.commit()

    competing = transition_meeting(original, MeetingStatus.TRANSCRIBING, NOW)
    with pytest.raises(PersistenceConflictError), SqliteUnitOfWork(database) as uow:
        uow.meetings.save(competing, expected_version=0)


def test_read_only_unit_of_work_does_not_reserve_the_writer(tmp_path: Path) -> None:
    database = Database(tmp_path / "application.sqlite3")
    database.migrate()

    with SqliteUnitOfWork(database, immediate=False) as reader:
        assert reader.meetings.get(MEETING_ID) is None
        with SqliteUnitOfWork(database) as writer:
            writer.commit()


def test_meeting_keyset_pages_are_stable_with_tied_timestamps(tmp_path: Path) -> None:
    database = Database(tmp_path / "application.sqlite3")
    database.migrate()
    records = tuple(
        meeting().model_copy(
            update={
                "id": UUID(f"10000000-0000-4000-8000-{ordinal:012d}"),
                "ingest_key": f"upload-{ordinal}",
                "status": MeetingStatus.CANCELLED if ordinal == 4 else MeetingStatus.INGESTED,
                "created_at": NOW
                + timedelta(minutes=2 if ordinal == 5 else 1 if ordinal > 1 else 0),
                "updated_at": NOW
                + timedelta(minutes=2 if ordinal == 5 else 1 if ordinal > 1 else 0),
            }
        )
        for ordinal in range(1, 6)
    )

    with SqliteUnitOfWork(database) as uow:
        uow.audio_assets.add(asset())
        for record in records:
            uow.meetings.add(record)
        uow.commit()

    with SqliteUnitOfWork(database, immediate=False) as uow:
        first = tuple(uow.meetings.list_page(status=None, cursor=None, limit=2))
        second = tuple(
            uow.meetings.list_page(
                status=None,
                cursor=MeetingListCursor(created_at=first[-1].created_at, id=first[-1].id),
                limit=2,
            )
        )
        filtered = tuple(
            uow.meetings.list_page(
                status=MeetingStatus.INGESTED,
                cursor=None,
                limit=10,
            )
        )

    assert [item.ingest_key for item in first] == ["upload-5", "upload-4"]
    assert [item.ingest_key for item in second] == ["upload-3", "upload-2"]
    assert {item.id for item in first}.isdisjoint(item.id for item in second)
    assert [item.ingest_key for item in filtered] == [
        "upload-5",
        "upload-3",
        "upload-2",
        "upload-1",
    ]


def test_meeting_created_at_is_an_immutable_persistence_key(tmp_path: Path) -> None:
    database = Database(tmp_path / "application.sqlite3")
    database.migrate()
    original = meeting()

    with SqliteUnitOfWork(database) as uow:
        uow.audio_assets.add(asset())
        uow.meetings.add(original)
        uow.commit()

    moved = original.model_copy(update={"created_at": NOW + timedelta(minutes=1)})
    with pytest.raises(PersistenceConflictError), SqliteUnitOfWork(database) as uow:
        uow.meetings.save(moved, expected_version=0)


def test_meeting_keyset_queries_seek_from_the_cursor(tmp_path: Path) -> None:
    database = Database(tmp_path / "application.sqlite3")
    database.migrate()
    cursor_time = str(NOW)
    cursor_id = str(MEETING_ID)

    with database.connect() as connection:
        unfiltered = connection.execute(
            """
            EXPLAIN QUERY PLAN SELECT * FROM meetings
            WHERE (created_at, id) < (?, ?)
            ORDER BY created_at DESC, id DESC LIMIT ?
            """,
            (cursor_time, cursor_id, 20),
        ).fetchall()
        filtered = connection.execute(
            """
            EXPLAIN QUERY PLAN SELECT * FROM meetings
            WHERE status = ? AND (created_at, id) < (?, ?)
            ORDER BY created_at DESC, id DESC LIMIT ?
            """,
            (MeetingStatus.INGESTED.value, cursor_time, cursor_id, 20),
        ).fetchall()

    assert any(
        "SEARCH meetings USING INDEX idx_meetings_created_id" in row["detail"] for row in unfiltered
    )
    assert any(
        "SEARCH meetings USING INDEX idx_meetings_status_created_id" in row["detail"]
        for row in filtered
    )
