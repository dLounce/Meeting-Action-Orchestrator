from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import datetime, timezone
from typing import BinaryIO
from uuid import UUID

import httpx

from meeting_action_orchestrator.api import ApiDependencies, create_app
from meeting_action_orchestrator.api.contracts import (
    DeliveryResult,
    Principal,
    ReadinessCheck,
    ReadinessResult,
)
from meeting_action_orchestrator.application.reviewing import ActionEdit, IssueResolutionEdit
from meeting_action_orchestrator.application.workflow import (
    ApprovalResult,
    IngestMeeting,
)
from meeting_action_orchestrator.domain.enums import MeetingStatus, ReviewOrigin, WriteKind
from meeting_action_orchestrator.domain.models import (
    Approval,
    Meeting,
    RecapArtifact,
    ReviewRevision,
    Transcript,
    TranscriptSegment,
)

NOW = datetime(2026, 8, 7, 9, 0, tzinfo=timezone.utc)
MEETING_ID = UUID("10000000-0000-4000-8000-000000000001")
AUDIO_ID = UUID("20000000-0000-4000-8000-000000000001")
TRANSCRIPT_ID = UUID("30000000-0000-4000-8000-000000000001")
SEGMENT_ID = UUID("40000000-0000-4000-8000-000000000001")
REVIEW_ID = UUID("50000000-0000-4000-8000-000000000001")
ACTION_ID = UUID("60000000-0000-4000-8000-000000000001")
ISSUE_ID = UUID("60000000-0000-4000-8000-000000000002")
APPROVAL_ID = UUID("70000000-0000-4000-8000-000000000001")
RECAP_ID = UUID("80000000-0000-4000-8000-000000000001")
VALID_TOKEN = MEETING_ID.hex


def meeting() -> Meeting:
    return Meeting(
        id=MEETING_ID,
        ingest_key="upload-one",
        title="Release planning",
        audio_asset_id=AUDIO_ID,
        occurred_at=NOW,
        timezone="Asia/Calcutta",
        status=MeetingStatus.INGESTED,
        created_at=NOW,
        updated_at=NOW,
    )


def transcript() -> Transcript:
    text = "The release plan was approved."
    return Transcript(
        id=TRANSCRIPT_ID,
        meeting_id=MEETING_ID,
        audio_asset_id=AUDIO_ID,
        provider="test",
        model="test",
        language="en",
        text=text,
        segments=(
            TranscriptSegment(
                id=SEGMENT_ID,
                ordinal=0,
                start_ms=0,
                end_ms=1000,
                speaker="Mira",
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
        purpose="Confirm the release plan",
        recap_markdown="# Release planning",
        created_at=NOW,
    )


class FakeAuthenticator:
    async def authenticate(self, token: str) -> Principal | None:
        return Principal("portfolio-owner") if token == VALID_TOKEN else None


class FakeReadiness:
    def __init__(self, ready: bool = True) -> None:
        self.ready = ready

    async def check(self) -> ReadinessResult:
        return ReadinessResult((ReadinessCheck("database", self.ready),))


class FakeWorkflow:
    def __init__(self) -> None:
        self.created: IngestMeeting | None = None
        self.content = b""

    async def ingest(self, command: IngestMeeting, stream: BinaryIO) -> Meeting:
        self.created = command
        self.content = stream.read()
        return meeting()

    async def process(self, meeting_id: UUID) -> Meeting:
        assert meeting_id == MEETING_ID
        return meeting()

    async def get_meeting(self, meeting_id: UUID) -> Meeting:
        assert meeting_id == MEETING_ID
        return meeting()

    async def approve(
        self,
        meeting_id: UUID,
        *,
        expected_digest: str,
        request_key: str,
        actor_id: str,
    ) -> ApprovalResult:
        current = review()
        assert meeting_id == MEETING_ID
        assert expected_digest == current.content_digest
        assert request_key == "approval-one"
        assert actor_id == "portfolio-owner"
        approval = Approval(
            id=APPROVAL_ID,
            meeting_id=MEETING_ID,
            review_revision_id=REVIEW_ID,
            review_digest=current.content_digest,
            request_key=request_key,
            actor_id=actor_id,
            approved_at=NOW,
        )
        recap = RecapArtifact(
            id=RECAP_ID,
            meeting_id=MEETING_ID,
            approval_id=APPROVAL_ID,
            content=current.recap_markdown,
            created_at=NOW,
        )
        return ApprovalResult(approval, recap, ())


class FakeQueries:
    async def get_transcript(self, meeting_id: UUID) -> Transcript:
        assert meeting_id == MEETING_ID
        return transcript()

    async def get_review(self, meeting_id: UUID) -> ReviewRevision:
        assert meeting_id == MEETING_ID
        return review()

    async def get_delivery(self, meeting_id: UUID) -> DeliveryResult:
        assert meeting_id == MEETING_ID
        return DeliveryResult(meeting(), ())


class FakeReviews:
    def __init__(self) -> None:
        self.action_edit: ActionEdit | None = None
        self.delivery_kind: WriteKind | None = None
        self.issue_edit: IssueResolutionEdit | None = None

    async def revise_action(
        self,
        meeting_id: UUID,
        *,
        expected_digest: str,
        edit: ActionEdit,
        actor_id: str,
    ) -> ReviewRevision:
        assert meeting_id == MEETING_ID
        assert expected_digest == review().content_digest
        assert actor_id == "portfolio-owner"
        self.action_edit = edit
        return review()

    async def revise_delivery(
        self,
        meeting_id: UUID,
        *,
        expected_digest: str,
        action_id: UUID,
        kind: WriteKind,
        enabled: bool,
        actor_id: str,
    ) -> ReviewRevision:
        assert meeting_id == MEETING_ID
        assert expected_digest == review().content_digest
        assert action_id == ACTION_ID
        assert enabled
        assert actor_id == "portfolio-owner"
        self.delivery_kind = kind
        return review()

    async def revise_issue(
        self,
        meeting_id: UUID,
        *,
        expected_digest: str,
        edit: IssueResolutionEdit,
        actor_id: str,
    ) -> ReviewRevision:
        assert meeting_id == MEETING_ID
        assert expected_digest == review().content_digest
        assert actor_id == "portfolio-owner"
        self.issue_edit = edit
        return review()


class FakeDeliveries:
    async def retry(
        self,
        meeting_id: UUID,
        *,
        intent_ids: tuple[UUID, ...],
        request_key: str,
        actor_id: str,
    ) -> DeliveryResult:
        assert meeting_id == MEETING_ID
        assert intent_ids == ()
        assert request_key == "delivery-one"
        assert actor_id == "portfolio-owner"
        return DeliveryResult(meeting(), ())

    async def reconcile(
        self,
        meeting_id: UUID,
        *,
        intent_ids: tuple[UUID, ...],
        request_key: str,
        actor_id: str,
    ) -> DeliveryResult:
        assert meeting_id == MEETING_ID
        assert intent_ids == ()
        assert request_key == "delivery-one"
        assert actor_id == "portfolio-owner"
        return DeliveryResult(meeting(), ())


class ChunkedBody(httpx.AsyncByteStream):
    async def __aiter__(self) -> AsyncIterator[bytes]:
        yield b"x" * (512 * 1024)
        yield b"y" * (512 * 1024)
        yield b"zz"


def dependencies(*, ready: bool = True, max_upload_bytes: int = 1024) -> ApiDependencies:
    return ApiDependencies(
        workflow=FakeWorkflow(),
        queries=FakeQueries(),
        reviews=FakeReviews(),
        deliveries=FakeDeliveries(),
        authenticator=FakeAuthenticator(),
        readiness=FakeReadiness(ready),
        max_upload_bytes=max_upload_bytes,
    )


async def request(
    path: str,
    *,
    method: str = "GET",
    services: ApiDependencies | None = None,
    headers: dict[str, str] | None = None,
    json: object | None = None,
    files: dict[str, tuple[str | None, bytes | str, str | None]] | None = None,
) -> httpx.Response:
    app = create_app(services or dependencies())
    transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        return await client.request(
            method,
            path,
            headers=headers,
            json=json,
            files=files,
        )


def authorization(**headers: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {VALID_TOKEN}", **headers}


async def test_health_and_readiness_are_public() -> None:
    live = await request("/health/live")
    ready = await request("/health/ready", services=dependencies(ready=False))

    assert live.status_code == 200
    assert live.json()["service"] == "meeting-action-orchestrator"
    assert ready.status_code == 503
    assert ready.json() == {
        "status": "not_ready",
        "checks": [{"name": "database", "status": "not_ready"}],
    }


async def test_protected_routes_require_a_valid_bearer_token() -> None:
    missing = await request(f"/v1/meetings/{MEETING_ID}")
    invalid = await request(
        f"/v1/meetings/{MEETING_ID}",
        headers={"Authorization": "Bearer invalid"},
    )

    assert missing.status_code == 401
    assert missing.headers["content-type"] == "application/problem+json"
    assert missing.headers["www-authenticate"] == "Bearer"
    assert invalid.status_code == 401


async def test_create_meeting_accepts_bounded_multipart_audio() -> None:
    services = dependencies()
    response = await request(
        "/v1/meetings",
        method="POST",
        services=services,
        headers=authorization(**{"Idempotency-Key": "upload-one"}),
        files={
            "metadata": (
                None,
                '{"title":"Release planning","occurred_at":"2026-08-07T09:00:00Z",'
                '"timezone":"Asia/Calcutta","participants":[]}',
                None,
            ),
            "recording": ("meeting.wav", b"RIFF0000WAVEdata", "audio/wav"),
        },
    )

    workflow = services.workflow
    assert isinstance(workflow, FakeWorkflow)
    assert response.status_code == 201
    assert response.headers["location"] == f"/v1/meetings/{MEETING_ID}"
    assert response.headers["etag"] == '"meeting-0"'
    assert workflow.created is not None
    assert workflow.created.ingest_key == "upload-one"
    assert workflow.content == b"RIFF0000WAVEdata"


async def test_create_meeting_rejects_oversized_recording() -> None:
    response = await request(
        "/v1/meetings",
        method="POST",
        services=dependencies(max_upload_bytes=4),
        headers=authorization(**{"Idempotency-Key": "upload-one"}),
        files={
            "metadata": (
                None,
                '{"title":"Release planning","occurred_at":"2026-08-07T09:00:00Z",'
                '"timezone":"UTC"}',
                None,
            ),
            "recording": ("meeting.wav", b"too-large", "audio/wav"),
        },
    )

    assert response.status_code == 413
    assert response.headers["content-type"] == "application/problem+json"


async def test_create_meeting_rejects_invalid_timezone_before_ingest() -> None:
    services = dependencies()
    response = await request(
        "/v1/meetings",
        method="POST",
        services=services,
        headers=authorization(**{"Idempotency-Key": "upload-one"}),
        files={
            "metadata": (
                None,
                '{"title":"Release planning","occurred_at":"2026-08-07T09:00:00Z",'
                '"timezone":"Mars/Olympus"}',
                None,
            ),
            "recording": ("meeting.wav", b"RIFF0000WAVEdata", "audio/wav"),
        },
    )

    workflow = services.workflow
    assert isinstance(workflow, FakeWorkflow)
    assert response.status_code == 422
    assert workflow.created is None


async def test_read_endpoints_return_strong_etags() -> None:
    transcript_response = await request(
        f"/v1/meetings/{MEETING_ID}/transcript",
        headers=authorization(),
    )
    review_response = await request(
        f"/v1/meetings/{MEETING_ID}/review",
        headers=authorization(),
    )

    assert transcript_response.status_code == 200
    assert transcript_response.headers["etag"] == f'"{transcript().sha256}"'
    assert review_response.status_code == 200
    assert review_response.headers["etag"] == f'"{review().content_digest}"'
    assert review_response.json()["revision_number"] == 1


async def test_review_revision_requires_and_forwards_if_match() -> None:
    services = dependencies()
    missing = await request(
        f"/v1/meetings/{MEETING_ID}/review/actions/{ACTION_ID}",
        method="PATCH",
        services=services,
        headers=authorization(),
        json={
            "title": "Publish the brief",
            "timezone": "UTC",
            "recap_markdown": "# Updated recap",
        },
    )
    updated = await request(
        f"/v1/meetings/{MEETING_ID}/review/actions/{ACTION_ID}",
        method="PATCH",
        services=services,
        headers=authorization(**{"If-Match": f'"{review().content_digest}"'}),
        json={
            "title": "Publish the brief",
            "timezone": "UTC",
            "recap_markdown": "# Updated recap",
        },
    )

    editor = services.reviews
    assert isinstance(editor, FakeReviews)
    assert missing.status_code == 428
    assert updated.status_code == 200
    assert editor.action_edit is not None
    assert editor.action_edit.title == "Publish the brief"
    assert editor.action_edit.recap_markdown == "# Updated recap"


async def test_delivery_selection_forwards_the_review_precondition() -> None:
    services = dependencies()
    response = await request(
        f"/v1/meetings/{MEETING_ID}/review/actions/{ACTION_ID}/deliveries/task",
        method="PUT",
        services=services,
        headers=authorization(**{"If-Match": f'"{review().content_digest}"'}),
        json={"enabled": True},
    )

    editor = services.reviews
    assert isinstance(editor, FakeReviews)
    assert response.status_code == 200
    assert editor.delivery_kind is WriteKind.TASK


async def test_issue_resolution_is_an_explicit_authenticated_revision() -> None:
    services = dependencies()
    response = await request(
        f"/v1/meetings/{MEETING_ID}/review/issues/{ISSUE_ID}",
        method="PATCH",
        services=services,
        headers=authorization(**{"If-Match": f'"{review().content_digest}"'}),
        json={"status": "accepted_risk", "resolution_note": "Accepted for this release."},
    )

    editor = services.reviews
    assert isinstance(editor, FakeReviews)
    assert response.status_code == 200
    assert editor.issue_edit is not None
    assert editor.issue_edit.status.value == "accepted_risk"
    assert editor.issue_edit.resolution_note == "Accepted for this release."


async def test_approval_requires_precondition_and_idempotency_headers() -> None:
    response = await request(
        f"/v1/meetings/{MEETING_ID}/approval",
        method="POST",
        headers=authorization(
            **{
                "If-Match": f'"{review().content_digest}"',
                "Idempotency-Key": "approval-one",
            }
        ),
    )

    assert response.status_code == 201
    assert response.headers["etag"] == f'"{review().content_digest}"'
    assert response.json()["approval_id"] == str(APPROVAL_ID)
    assert response.json()["replayed"] is False


async def test_retry_and_reconcile_require_idempotency_keys() -> None:
    for operation in ("retry", "reconcile"):
        response = await request(
            f"/v1/meetings/{MEETING_ID}/delivery/{operation}",
            method="POST",
            headers=authorization(**{"Idempotency-Key": "delivery-one"}),
            json={"intent_ids": []},
        )

        assert response.status_code == 200
        assert response.json()["meeting"]["id"] == str(MEETING_ID)


async def test_api_responses_apply_security_and_request_id_headers() -> None:
    response = await request("/health/live")

    assert response.headers["content-security-policy"] == (
        "default-src 'none'; base-uri 'none'; frame-ancestors 'none'"
    )
    assert response.headers["x-content-type-options"] == "nosniff"
    assert len(response.headers["x-request-id"]) == 32
    assert response.headers["cache-control"] == "no-store"


async def test_request_body_limit_rejects_declared_oversized_payloads_early() -> None:
    response = await request(
        "/v1/meetings",
        method="POST",
        services=dependencies(max_upload_bytes=4),
        headers={"Content-Length": str(1024 * 1024 + 5)},
    )

    assert response.status_code == 413
    assert response.headers["content-type"] == "application/problem+json"
    assert response.json()["type"].endswith("request-body-size")


async def test_request_body_limit_counts_chunked_payloads() -> None:
    app = create_app(dependencies(max_upload_bytes=1))
    transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        response = await client.post("/v1/meetings", content=ChunkedBody())

    assert response.status_code == 413
    assert response.headers["content-type"] == "application/problem+json"


def test_openapi_secures_all_data_routes_and_describes_problem_media() -> None:
    schema = create_app(dependencies()).openapi()

    for path, item in schema["paths"].items():
        for operation in item.values():
            if path.startswith("/v1/"):
                assert operation["security"] == [{"HTTPBearer": []}]
                problem = operation["responses"]["400"]
                assert set(problem["content"]) == {"application/problem+json"}
    approval = schema["paths"]["/v1/meetings/{meeting_id}/approval"]["post"]
    assert "200" in approval["responses"]
    assert "201" in approval["responses"]
