from __future__ import annotations

from datetime import datetime, timezone
from typing import cast
from uuid import UUID

import httpx

from meeting_action_orchestrator.api import ApiDependencies, create_app
from meeting_action_orchestrator.api.contracts import (
    Authenticator,
    DeliveryService,
    MeetingErasureApiService,
    MeetingQueryService,
    MeetingWorkflowService,
    Principal,
    ProcessingController,
    ReadinessCheck,
    ReadinessResult,
    ReviewEditor,
    WorkflowEventPageResult,
)
from meeting_action_orchestrator.api.workflow_event_cursors import format_workflow_event_cursor
from meeting_action_orchestrator.application.errors import ResourceNotFoundError
from meeting_action_orchestrator.application.ports import WorkflowEventCursor
from meeting_action_orchestrator.domain.enums import AudioMediaType
from meeting_action_orchestrator.domain.workflow_events import (
    MeetingIngestedMetadata,
    WorkflowEvent,
    WorkflowEventType,
)

NOW = datetime(2026, 8, 7, 12, 0, tzinfo=timezone.utc)
MEETING_ID = UUID("10000000-0000-4000-8000-000000000001")
OTHER_MEETING_ID = UUID("10000000-0000-4000-8000-000000000002")
TOKEN = "a" * 32
ACTOR_ID = "portfolio-owner"
PRIVATE_REQUEST_ID = "req_private_raw_marker"


def event(sequence: int) -> WorkflowEvent:
    return WorkflowEvent(
        id=UUID(f"20000000-0000-4000-8000-{sequence:012d}"),
        meeting_id=MEETING_ID,
        sequence=sequence,
        type=WorkflowEventType.MEETING_INGESTED,
        actor_id=ACTOR_ID,
        safe_metadata=MeetingIngestedMetadata(
            recording_digest="a" * 64,
            media_type=AudioMediaType.WAV,
            size_bytes=1_024,
            duration_ms=60_000,
        ),
        occurred_at=NOW,
    )


class EventQueries:
    def __init__(
        self,
        result: WorkflowEventPageResult | None = None,
        error: Exception | None = None,
    ) -> None:
        self.result = result or WorkflowEventPageResult(items=(), next_cursor=None)
        self.error = error
        self.calls: list[tuple[UUID, WorkflowEventCursor | None, int]] = []

    async def list_workflow_events(
        self,
        meeting_id: UUID,
        *,
        cursor: WorkflowEventCursor | None,
        limit: int,
    ) -> WorkflowEventPageResult:
        self.calls.append((meeting_id, cursor, limit))
        if self.error is not None:
            raise self.error
        return self.result


class TokenAuthenticator:
    async def authenticate(self, token: str) -> Principal | None:
        return Principal(subject=ACTOR_ID) if token == TOKEN else None


class Ready:
    async def check(self) -> ReadinessResult:
        return ReadinessResult((ReadinessCheck("test", True),))


def dependencies(queries: EventQueries | None = None) -> ApiDependencies:
    return ApiDependencies(
        workflow=cast(MeetingWorkflowService, object()),
        queries=cast(MeetingQueryService, queries or EventQueries()),
        processing_controls=cast(ProcessingController, object()),
        reviews=cast(ReviewEditor, object()),
        deliveries=cast(DeliveryService, object()),
        erasures=cast(MeetingErasureApiService, object()),
        authenticator=cast(Authenticator, TokenAuthenticator()),
        readiness=Ready(),
        max_upload_bytes=1_024,
    )


async def request(
    path: str,
    *,
    services: ApiDependencies | None = None,
    headers: dict[str, str] | list[tuple[str, str]] | None = None,
) -> httpx.Response:
    transport = httpx.ASGITransport(
        app=create_app(services or dependencies()),
        raise_app_exceptions=False,
    )
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        return await client.get(path, headers=headers)


def authorization() -> dict[str, str]:
    return {"Authorization": f"Bearer {TOKEN}"}


async def test_workflow_event_route_requires_authentication() -> None:
    queries = EventQueries()

    missing = await request(
        f"/v1/meetings/{MEETING_ID}/events",
        services=dependencies(queries),
    )
    invalid = await request(
        f"/v1/meetings/{MEETING_ID}/events",
        services=dependencies(queries),
        headers={"Authorization": "Bearer invalid"},
    )

    assert missing.status_code == 401
    assert missing.headers["content-type"] == "application/problem+json"
    assert invalid.status_code == 401
    assert queries.calls == []


async def test_workflow_event_route_returns_an_ordered_privacy_safe_page() -> None:
    anchor = WorkflowEventCursor(meeting_id=MEETING_ID, sequence=2)
    queries = EventQueries(
        WorkflowEventPageResult(
            items=(event(1), event(2)),
            next_cursor=anchor,
        )
    )

    response = await request(
        f"/v1/meetings/{MEETING_ID}/events?limit=2",
        services=dependencies(queries),
        headers=authorization(),
    )

    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store"
    payload = response.json()
    assert [item["sequence"] for item in payload["items"]] == [1, 2]
    assert all(item["actor_id"] == ACTOR_ID for item in payload["items"])
    assert payload["next_cursor"] == format_workflow_event_cursor(anchor)
    assert queries.calls == [(MEETING_ID, None, 2)]
    for hidden_name in (
        "provider_request_id",
        "client_request_id",
        "idempotency_key",
        "ingest_key",
        PRIVATE_REQUEST_ID,
    ):
        assert hidden_name not in response.text


async def test_workflow_event_route_decodes_its_meeting_bound_cursor() -> None:
    cursor = WorkflowEventCursor(meeting_id=MEETING_ID, sequence=2)
    encoded = format_workflow_event_cursor(cursor)
    assert encoded is not None
    queries = EventQueries()

    response = await request(
        f"/v1/meetings/{MEETING_ID}/events?cursor={encoded}&limit=1",
        services=dependencies(queries),
        headers=authorization(),
    )

    assert response.status_code == 200
    assert queries.calls == [(MEETING_ID, cursor, 1)]


async def test_workflow_event_route_rejects_ambiguous_and_cross_meeting_cursors() -> None:
    cursor = format_workflow_event_cursor(
        WorkflowEventCursor(meeting_id=OTHER_MEETING_ID, sequence=1)
    )
    assert cursor is not None
    queries = EventQueries()

    cross_meeting = await request(
        f"/v1/meetings/{MEETING_ID}/events?cursor={cursor}",
        services=dependencies(queries),
        headers=authorization(),
    )
    duplicate = await request(
        f"/v1/meetings/{MEETING_ID}/events?cursor={cursor}&cursor={cursor}",
        services=dependencies(queries),
        headers=authorization(),
    )

    for response in (cross_meeting, duplicate):
        assert response.status_code == 400
        assert response.headers["content-type"] == "application/problem+json"
        assert response.json()["type"].endswith("invalid-page-cursor")
        assert response.json()["instance"] == "/v1/meetings/{meeting_id}/events"
        assert cursor not in response.text
        assert str(MEETING_ID) not in response.json()["instance"]
    assert queries.calls == []


async def test_workflow_event_route_validates_limits_with_rfc_problems() -> None:
    for limit in ("0", "101", "true", "1.5"):
        response = await request(
            f"/v1/meetings/{MEETING_ID}/events?limit={limit}",
            headers=authorization(),
        )

        assert response.status_code == 422
        assert response.headers["content-type"] == "application/problem+json"
        assert response.json()["instance"] == "/v1/meetings/{meeting_id}/events"


async def test_workflow_event_route_uses_the_generic_meeting_not_found_problem() -> None:
    response = await request(
        f"/v1/meetings/{MEETING_ID}/events",
        services=dependencies(EventQueries(error=ResourceNotFoundError("private-meeting"))),
        headers=authorization(),
    )

    assert response.status_code == 404
    assert response.headers["content-type"] == "application/problem+json"
    assert response.json()["type"].endswith("not-found")
    assert response.json()["detail"] == "The requested resource was not found."
    assert response.json()["instance"] == "/v1/meetings/{meeting_id}/events"
    assert "private-meeting" not in response.text


def test_workflow_event_openapi_is_authenticated_typed_and_paginated() -> None:
    schema = create_app(dependencies()).openapi()
    operation = schema["paths"]["/v1/meetings/{meeting_id}/events"]["get"]

    assert operation["security"] == [{"HTTPBearer": []}]
    parameters = {parameter["name"]: parameter for parameter in operation["parameters"]}
    assert parameters["limit"]["schema"]["minimum"] == 1
    assert parameters["limit"]["schema"]["maximum"] == 100
    assert "meeting-bound" in parameters["cursor"]["description"]
    for status in ("400", "401", "404", "422", "500", "503"):
        assert set(operation["responses"][status]["content"]) == {"application/problem+json"}
    event_schema = schema["components"]["schemas"]["WorkflowEventResponse"]
    assert set(event_schema["properties"]) == {
        "id",
        "meeting_id",
        "sequence",
        "type",
        "actor_id",
        "safe_metadata",
        "occurred_at",
    }
    metadata_schema = event_schema["properties"]["safe_metadata"]
    assert metadata_schema["discriminator"]["propertyName"] == "kind"
    assert len(metadata_schema["oneOf"]) == len(WorkflowEventType)
