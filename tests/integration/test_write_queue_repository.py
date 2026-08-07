from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID

import pytest

from meeting_action_orchestrator.domain.enums import (
    AudioMediaType,
    FailureCode,
    FailureDisposition,
    WriteStatus,
)
from meeting_action_orchestrator.domain.models import (
    AudioAsset,
    ConnectorTarget,
    Meeting,
    TaskProposal,
    WorkflowFailure,
    WriteIntent,
)
from meeting_action_orchestrator.infrastructure.database import Database
from meeting_action_orchestrator.infrastructure.repositories import SqliteUnitOfWork

NOW = datetime(2026, 8, 7, 10, 0, tzinfo=timezone.utc)
ASSET_ID = UUID("5e816b68-8eaa-419a-a388-d3d09ad24649")
MEETING_ID = UUID("3de368a2-1c22-41eb-8194-08d6baf3bcb9")
TRANSCRIPT_ID = UUID("86c89a5c-78af-4826-bb2a-f1811858831b")
REVIEW_ID = UUID("170181dc-e426-4f56-9516-485512a2dcbd")
APPROVAL_ID = UUID("fb2ca4a1-d442-49a6-af40-f69796c8c509")
ACTION_ID = UUID("7be09297-a683-4c1a-8892-29c2ec34d680")
INTENT_ID = UUID("d98d7410-88bd-44ce-be9e-02a9ab9d753a")


def create_database(path: Path) -> Database:
    database = Database(path)
    database.migrate()
    with SqliteUnitOfWork(database) as uow:
        uow.audio_assets.add(
            AudioAsset(
                id=ASSET_ID,
                storage_key="write-recording.wav",
                original_name="write-recording.wav",
                detected_media_type=AudioMediaType.WAV,
                size_bytes=1_024,
                duration_ms=60_000,
                sha256="e" * 64,
                created_at=NOW,
            )
        )
        uow.meetings.add(
            Meeting(
                id=MEETING_ID,
                ingest_key="write-queue-test",
                title="Write queue test",
                audio_asset_id=ASSET_ID,
                timezone="UTC",
                created_at=NOW,
                updated_at=NOW,
            )
        )
        uow.commit()
    with database.transaction(immediate=True) as connection:
        connection.execute(
            """
            INSERT INTO transcripts (
                id, meeting_id, audio_asset_id, provider, model, language,
                segments_json, text, sha256, provider_request_id, usage_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(TRANSCRIPT_ID),
                str(MEETING_ID),
                str(ASSET_ID),
                "openai",
                "test",
                "en",
                "[]",
                "Approved",
                "a" * 64,
                None,
                "{}",
                str(NOW),
            ),
        )
        connection.execute(
            """
            INSERT INTO review_revisions (
                id, meeting_id, transcript_id, revision_number, origin,
                payload_json, content_digest, actor_id, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(REVIEW_ID),
                str(MEETING_ID),
                str(TRANSCRIPT_ID),
                1,
                "human",
                "{}",
                "b" * 64,
                "reviewer",
                str(NOW),
            ),
        )
        connection.execute(
            """
            INSERT INTO approvals (
                id, meeting_id, review_revision_id, review_digest,
                request_key, actor_id, approved_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(APPROVAL_ID),
                str(MEETING_ID),
                str(REVIEW_ID),
                "b" * 64,
                "write-approval",
                "reviewer",
                str(NOW),
            ),
        )
    intent = WriteIntent(
        id=INTENT_ID,
        meeting_id=MEETING_ID,
        approval_id=APPROVAL_ID,
        idempotency_key=f"mao_v1_{'c' * 64}",
        proposal=TaskProposal(
            source_action_id=ACTION_ID,
            target=ConnectorTarget(connector_id="tasks", resource_id="inbox"),
            title="Send the approved brief",
        ),
        created_at=NOW,
        updated_at=NOW,
    )
    with SqliteUnitOfWork(database) as uow:
        uow.write_intents.add_many((intent,))
        uow.commit()
    return database


def unknown_failure(at: datetime) -> WorkflowFailure:
    return WorkflowFailure(
        code=FailureCode.UNKNOWN_REMOTE_OUTCOME,
        disposition=FailureDisposition.UNKNOWN_OUTCOME,
        safe_message="The remote write outcome is unknown",
        occurred_at=at,
    )


def test_claim_ids_are_exclusive_and_expired_writes_become_unknown(tmp_path: Path) -> None:
    database = create_database(tmp_path / "application.sqlite3")
    lease_until = NOW + timedelta(seconds=30)

    with SqliteUnitOfWork(database) as uow:
        first = uow.write_intents.claim_due_ids("worker-a", NOW, lease_until, 1)
        uow.commit()
    with SqliteUnitOfWork(database) as uow:
        competing = uow.write_intents.claim_due_ids("worker-b", NOW, lease_until, 1)
        early = uow.write_intents.recover_expired_ids(
            NOW,
            unknown_failure(NOW),
            1,
        )
        uow.commit()
    with SqliteUnitOfWork(database) as uow:
        recovered = uow.write_intents.recover_expired_ids(
            lease_until,
            unknown_failure(lease_until),
            1,
        )
        unknown = uow.write_intents.list_unknown_ids(1)
        intent = uow.write_intents.get(INTENT_ID)
        uow.commit()

    assert first == (INTENT_ID,)
    assert competing == ()
    assert early == ()
    assert recovered == (INTENT_ID,)
    assert unknown == (INTENT_ID,)
    assert intent is not None
    assert intent.status is WriteStatus.UNKNOWN
    assert intent.attempt_count == 1
    assert intent.version == 2


def test_expired_write_recovery_requires_unknown_disposition(tmp_path: Path) -> None:
    database = create_database(tmp_path / "application.sqlite3")
    failure = WorkflowFailure(
        code=FailureCode.PROVIDER_UNAVAILABLE,
        disposition=FailureDisposition.RETRYABLE,
        safe_message="The provider is unavailable",
        occurred_at=NOW,
    )

    with pytest.raises(ValueError, match="unknown-outcome"), SqliteUnitOfWork(database) as uow:
        uow.write_intents.recover_expired_ids(NOW, failure, 1)
