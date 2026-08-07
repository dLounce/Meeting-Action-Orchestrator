from __future__ import annotations

import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import cast
from uuid import UUID

import httpx

from meeting_action_orchestrator.api import ApiDependencies, create_app
from meeting_action_orchestrator.api.adapters import UnitOfWorkQueryFacade
from meeting_action_orchestrator.api.contracts import (
    Authenticator,
    DeliveryService,
    MeetingErasureApiService,
    MeetingWorkflowService,
    Principal,
    ProcessingController,
    ReadinessCheck,
    ReadinessResult,
    ReviewEditor,
)
from meeting_action_orchestrator.domain.enums import AudioMediaType
from meeting_action_orchestrator.domain.models import AudioAsset, Meeting
from meeting_action_orchestrator.domain.workflow_events import (
    MeetingIngestedMetadata,
    WorkflowEventDraft,
    WorkflowEventType,
)
from meeting_action_orchestrator.infrastructure.database import Database
from meeting_action_orchestrator.infrastructure.repositories import SqliteUnitOfWork

NOW = datetime(2026, 8, 7, 12, 0, tzinfo=timezone.utc)
MEETING_ID = UUID("10000000-0000-4000-8000-000000000001")
ASSET_ID = UUID("30000000-0000-4000-8000-000000000001")
TOKEN = "a" * 32


class TokenAuthenticator:
    async def authenticate(self, token: str) -> Principal | None:
        return Principal(subject="portfolio-owner") if token == TOKEN else None


class Ready:
    async def check(self) -> ReadinessResult:
        return ReadinessResult((ReadinessCheck("database", True),))


def create_database(path: Path) -> Database:
    database = Database(path)
    database.migrate()
    with SqliteUnitOfWork(database) as unit_of_work:
        unit_of_work.audio_assets.add(
            AudioAsset(
                id=ASSET_ID,
                storage_key="recording.wav",
                original_name="private-original.wav",
                detected_media_type=AudioMediaType.WAV,
                size_bytes=1_024,
                duration_ms=60_000,
                sha256="a" * 64,
                created_at=NOW,
            )
        )
        unit_of_work.meetings.add(
            Meeting(
                id=MEETING_ID,
                ingest_key="private-upload-key",
                title="Planning",
                audio_asset_id=ASSET_ID,
                occurred_at=NOW,
                timezone="UTC",
                created_at=NOW,
                updated_at=NOW,
            )
        )
        for _ in range(101):
            unit_of_work.workflow_events.append(
                WorkflowEventDraft(
                    meeting_id=MEETING_ID,
                    type=WorkflowEventType.MEETING_INGESTED,
                    actor_id="portfolio-owner",
                    safe_metadata=MeetingIngestedMetadata(
                        recording_digest="a" * 64,
                        media_type=AudioMediaType.WAV,
                        size_bytes=1_024,
                        duration_ms=60_000,
                    ),
                    occurred_at=NOW,
                )
            )
        unit_of_work.commit()
    return database


def dependencies(database: Database, factory_threads: list[int]) -> ApiDependencies:
    def read_unit_of_work() -> SqliteUnitOfWork:
        factory_threads.append(threading.get_ident())
        return SqliteUnitOfWork(database, immediate=False)

    return ApiDependencies(
        workflow=cast(MeetingWorkflowService, object()),
        queries=UnitOfWorkQueryFacade(read_unit_of_work),
        processing_controls=cast(ProcessingController, object()),
        reviews=cast(ReviewEditor, object()),
        deliveries=cast(DeliveryService, object()),
        erasures=cast(MeetingErasureApiService, object()),
        authenticator=cast(Authenticator, TokenAuthenticator()),
        readiness=Ready(),
        max_upload_bytes=1_024,
    )


async def get(
    app_dependencies: ApiDependencies,
    path: str,
) -> httpx.Response:
    transport = httpx.ASGITransport(
        app=create_app(app_dependencies),
        raise_app_exceptions=False,
    )
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        return await client.get(
            path,
            headers={"Authorization": f"Bearer {TOKEN}"},
        )


async def test_sqlite_event_api_pages_at_the_limit_and_hides_erased_meetings(
    tmp_path: Path,
) -> None:
    database = create_database(tmp_path / "application.sqlite3")
    main_thread = threading.get_ident()
    factory_threads: list[int] = []
    app_dependencies = dependencies(database, factory_threads)

    first = await get(app_dependencies, f"/v1/meetings/{MEETING_ID}/events?limit=100")
    first_payload = first.json()
    cursor = first_payload["next_cursor"]
    second = await get(
        app_dependencies,
        f"/v1/meetings/{MEETING_ID}/events?limit=100&cursor={cursor}",
    )
    second_payload = second.json()

    assert first.status_code == 200
    assert [item["sequence"] for item in first_payload["items"]] == list(range(1, 101))
    assert isinstance(cursor, str)
    assert second.status_code == 200
    assert [item["sequence"] for item in second_payload["items"]] == [101]
    assert second_payload["next_cursor"] is None
    assert factory_threads
    assert all(thread_id != main_thread for thread_id in factory_threads)

    with database.transaction(immediate=True) as connection:
        connection.execute("DELETE FROM meetings WHERE id = ?", (str(MEETING_ID),))
    erased = await get(app_dependencies, f"/v1/meetings/{MEETING_ID}/events")
    with database.connect() as connection:
        event_count = connection.execute(
            "SELECT COUNT(*) FROM workflow_events WHERE meeting_id = ?",
            (str(MEETING_ID),),
        ).fetchone()[0]

    assert erased.status_code == 404
    assert erased.json()["detail"] == "The requested resource was not found."
    assert erased.json()["instance"] == "/v1/meetings/{meeting_id}/events"
    assert event_count == 0
    assert "private-upload-key" not in erased.text
