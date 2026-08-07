from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import datetime, timedelta, timezone
from typing import BinaryIO
from uuid import UUID

import httpx

from meeting_action_orchestrator.api import ApiDependencies, create_app
from meeting_action_orchestrator.api.contracts import (
    DeliveryResult,
    MeetingPageResult,
    Principal,
    ProcessingResult,
    ReadinessCheck,
    ReadinessResult,
)
from meeting_action_orchestrator.application.errors import (
    MeetingErasureBlockedError,
    OperationConflictError,
    ResourceNotFoundError,
    StaleWorkflowVersionError,
)
from meeting_action_orchestrator.application.meeting_erasure import MeetingErasureResult
from meeting_action_orchestrator.application.ports import MeetingListCursor
from meeting_action_orchestrator.application.processing_control import ProcessingControlResult
from meeting_action_orchestrator.application.reviewing import ActionEdit, IssueResolutionEdit
from meeting_action_orchestrator.application.workflow import (
    ApprovalResult,
    IngestMeeting,
)
from meeting_action_orchestrator.domain.enums import (
    FailureCode,
    FailureDisposition,
    MeetingErasureReason,
    MeetingErasureRecordingState,
    MeetingErasureStatus,
    MeetingStatus,
    ProcessingJobStatus,
    ProcessingStage,
    ReviewOrigin,
    WriteKind,
    WriteStatus,
)
from meeting_action_orchestrator.domain.models import (
    Approval,
    ConnectorTarget,
    Meeting,
    MeetingErasureJob,
    ProcessingJob,
    RecapArtifact,
    ReviewRevision,
    TaskProposal,
    Transcript,
    TranscriptSegment,
    WorkflowFailure,
    WriteIntent,
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
JOB_ID = UUID("90000000-0000-4000-8000-000000000001")
EXTRACTION_JOB_ID = UUID("90000000-0000-4000-8000-000000000002")
ERASURE_JOB_ID = UUID("e0000000-0000-4000-8000-000000000001")
PRIVATE_ERASURE_KEY_ID = "private-key-id"
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


def recap() -> RecapArtifact:
    current = review()
    return RecapArtifact(
        id=RECAP_ID,
        meeting_id=MEETING_ID,
        approval_id=APPROVAL_ID,
        content=current.recap_markdown,
        created_at=NOW,
    )


def processing_job() -> ProcessingJob:
    return ProcessingJob(
        id=JOB_ID,
        meeting_id=MEETING_ID,
        stage=ProcessingStage.TRANSCRIPTION,
        status=ProcessingJobStatus.RUNNING,
        attempt_count=1,
        max_attempts=3,
        lease_owner="private-worker",
        lease_expires_at=NOW + timedelta(minutes=5),
        claim_token=UUID(int=7001),
        created_at=NOW,
        updated_at=NOW,
    )


def erasure_job() -> MeetingErasureJob:
    return MeetingErasureJob(
        id=ERASURE_JOB_ID,
        token_version=1,
        token_key_id=PRIVATE_ERASURE_KEY_ID,
        meeting_token="f" * 64,
        reason=MeetingErasureReason.USER_REQUEST,
        erased_meeting_version=0,
        status=MeetingErasureStatus.ACTIVE,
        recording_state=MeetingErasureRecordingState.WAITING_SHARED,
        pending_audio_asset_id=AUDIO_ID,
        retry_count=2,
        remediation_count=1,
        max_remediations=3,
        version=3,
        created_at=NOW,
        updated_at=NOW,
    )


def retrying_processing_job() -> ProcessingJob:
    return ProcessingJob(
        id=EXTRACTION_JOB_ID,
        meeting_id=MEETING_ID,
        stage=ProcessingStage.EXTRACTION,
        status=ProcessingJobStatus.RETRY_WAIT,
        attempt_count=1,
        max_attempts=2,
        next_attempt_at=NOW + timedelta(minutes=5),
        last_failure=WorkflowFailure(
            code=FailureCode.PROVIDER_UNAVAILABLE,
            disposition=FailureDisposition.RETRYABLE,
            safe_message="The provider is temporarily unavailable",
            provider_request_id="private-provider-request",
            occurred_at=NOW,
        ),
        created_at=NOW,
        updated_at=NOW,
    )


def unknown_write_intent() -> WriteIntent:
    return WriteIntent(
        id=UUID("a0000000-0000-4000-8000-000000000001"),
        meeting_id=MEETING_ID,
        approval_id=APPROVAL_ID,
        idempotency_key=f"mao_v1_{'a' * 64}",
        proposal=TaskProposal(
            source_action_id=ACTION_ID,
            target=ConnectorTarget(connector_id="tasks", resource_id="inbox"),
            title="Publish the approved brief",
        ),
        status=WriteStatus.UNKNOWN,
        next_reconcile_at=NOW,
        last_failure=WorkflowFailure(
            code=FailureCode.UNKNOWN_REMOTE_OUTCOME,
            disposition=FailureDisposition.UNKNOWN_OUTCOME,
            safe_message="The connector outcome is unknown",
            occurred_at=NOW,
        ),
        created_at=NOW,
        updated_at=NOW,
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
    def __init__(self, ingest_error: Exception | None = None) -> None:
        self.created: IngestMeeting | None = None
        self.content = b""
        self.ingest_error = ingest_error

    async def ingest(self, command: IngestMeeting, stream: BinaryIO) -> Meeting:
        self.created = command
        self.content = stream.read()
        if self.ingest_error is not None:
            raise self.ingest_error
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
        return ApprovalResult(approval, recap(), ())


class FakeQueries:
    def __init__(self) -> None:
        self.delivery_result = DeliveryResult(meeting(), ())

    async def list_meetings(
        self,
        *,
        status: MeetingStatus | None,
        cursor: MeetingListCursor | None,
        limit: int,
    ) -> MeetingPageResult:
        assert limit >= 1
        if status is not None:
            assert status is MeetingStatus.INGESTED
        return MeetingPageResult(
            items=(meeting(),),
            next_cursor=(
                MeetingListCursor(created_at=NOW, id=MEETING_ID) if cursor is None else None
            ),
        )

    async def get_processing(self, meeting_id: UUID) -> ProcessingResult:
        assert meeting_id == MEETING_ID
        return ProcessingResult(
            meeting_id=meeting_id,
            jobs=(processing_job(), retrying_processing_job()),
        )

    async def get_transcript(self, meeting_id: UUID) -> Transcript:
        assert meeting_id == MEETING_ID
        return transcript()

    async def get_review(self, meeting_id: UUID) -> ReviewRevision:
        assert meeting_id == MEETING_ID
        return review()

    async def get_recap(self, meeting_id: UUID) -> RecapArtifact:
        assert meeting_id == MEETING_ID
        return recap()

    async def get_delivery(self, meeting_id: UUID) -> DeliveryResult:
        assert meeting_id == MEETING_ID
        return self.delivery_result


class MissingRecapQueries(FakeQueries):
    async def get_recap(self, meeting_id: UUID) -> RecapArtifact:
        assert meeting_id == MEETING_ID
        raise ResourceNotFoundError("Recap")


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


class FakeProcessingControls:
    def __init__(self, *, replayed: bool = False) -> None:
        self.replayed = replayed
        self.retry_request: tuple[UUID, int, str, str] | None = None
        self.cancel_request: tuple[UUID, int, str, str] | None = None

    async def retry(
        self,
        meeting_id: UUID,
        *,
        expected_version: int,
        request_key: str,
        actor_id: str,
    ) -> ProcessingControlResult:
        self.retry_request = (meeting_id, expected_version, request_key, actor_id)
        updated = meeting().model_copy(update={"version": 1})
        queued = processing_job().model_copy(
            update={
                "status": ProcessingJobStatus.READY,
                "attempt_count": 0,
                "lease_owner": None,
                "lease_expires_at": None,
            }
        )
        return ProcessingControlResult(updated, (queued,), self.replayed)

    async def cancel(
        self,
        meeting_id: UUID,
        *,
        expected_version: int,
        request_key: str,
        actor_id: str,
    ) -> ProcessingControlResult:
        self.cancel_request = (meeting_id, expected_version, request_key, actor_id)
        cancelled = meeting().model_copy(update={"status": MeetingStatus.CANCELLED, "version": 1})
        return ProcessingControlResult(cancelled, (), self.replayed)


class FakeErasures:
    def __init__(
        self,
        *,
        replayed: bool = False,
        request_error: Exception | None = None,
        get_error: Exception | None = None,
        retry_error: Exception | None = None,
    ) -> None:
        self.replayed = replayed
        self.request_error = request_error
        self.get_error = get_error
        self.retry_error = retry_error
        self.request_call: tuple[UUID, int, str, str] | None = None
        self.retry_call: tuple[UUID, int, str, str] | None = None

    async def request(
        self,
        meeting_id: UUID,
        *,
        expected_version: int,
        request_key: str,
        actor_id: str,
    ) -> MeetingErasureResult:
        self.request_call = (meeting_id, expected_version, request_key, actor_id)
        if self.request_error is not None:
            raise self.request_error
        return MeetingErasureResult(erasure_job(), replayed=self.replayed)

    async def get(self, erasure_job_id: UUID) -> MeetingErasureJob:
        assert erasure_job_id == ERASURE_JOB_ID
        if self.get_error is not None:
            raise self.get_error
        return erasure_job()

    async def retry(
        self,
        erasure_job_id: UUID,
        *,
        expected_version: int,
        request_key: str,
        actor_id: str,
    ) -> MeetingErasureResult:
        self.retry_call = (erasure_job_id, expected_version, request_key, actor_id)
        if self.retry_error is not None:
            raise self.retry_error
        return MeetingErasureResult(erasure_job(), replayed=self.replayed)


class ChunkedBody(httpx.AsyncByteStream):
    async def __aiter__(self) -> AsyncIterator[bytes]:
        yield b"x" * (512 * 1024)
        yield b"y" * (512 * 1024)
        yield b"zz"


def dependencies(
    *,
    ready: bool = True,
    max_upload_bytes: int = 1024,
    queries: FakeQueries | None = None,
    processing_controls: FakeProcessingControls | None = None,
    workflow: FakeWorkflow | None = None,
    erasures: FakeErasures | None = None,
) -> ApiDependencies:
    return ApiDependencies(
        workflow=workflow or FakeWorkflow(),
        queries=queries or FakeQueries(),
        processing_controls=processing_controls or FakeProcessingControls(),
        reviews=FakeReviews(),
        deliveries=FakeDeliveries(),
        erasures=erasures or FakeErasures(),
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
    assert workflow.created.actor_id == "portfolio-owner"
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


async def test_create_meeting_rejects_request_supplied_actor_identity() -> None:
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
                '"timezone":"UTC","actor_id":"request-supplied-actor"}',
                None,
            ),
            "recording": ("meeting.wav", b"RIFF0000WAVEdata", "audio/wav"),
        },
    )

    workflow = services.workflow
    assert isinstance(workflow, FakeWorkflow)
    assert response.status_code == 422
    assert response.headers["content-type"] == "application/problem+json"
    assert workflow.created is None
    assert "request-supplied-actor" not in response.text


async def test_ingest_conflict_does_not_disclose_tombstone_state_or_request_key() -> None:
    marker = "retired-erased-private-ingest"
    workflow = FakeWorkflow(
        OperationConflictError("The ingest request conflicts with the current workflow state")
    )
    response = await request(
        "/v1/meetings",
        method="POST",
        services=dependencies(workflow=workflow),
        headers=authorization(**{"Idempotency-Key": marker}),
        files={
            "metadata": (
                None,
                '{"title":"Release planning","occurred_at":"2026-08-07T09:00:00Z",'
                '"timezone":"UTC"}',
                None,
            ),
            "recording": ("meeting.wav", b"RIFF0000WAVEdata", "audio/wav"),
        },
    )

    assert response.status_code == 409
    assert response.json()["detail"] == "The operation conflicts with the current workflow state."
    assert marker not in response.text
    assert "erased" not in response.text.casefold()
    assert "retired" not in response.text.casefold()


async def test_meeting_collection_uses_filter_bound_opaque_cursors() -> None:
    first = await request(
        "/v1/meetings?status=ingested&limit=1",
        headers=authorization(),
    )

    assert first.status_code == 200
    payload = first.json()
    assert [item["id"] for item in payload["items"]] == [str(MEETING_ID)]
    cursor = payload["next_cursor"]
    assert isinstance(cursor, str)
    assert "=" not in cursor

    second = await request(
        f"/v1/meetings?status=ingested&limit=1&cursor={cursor}",
        headers=authorization(),
    )
    mismatched = await request(
        f"/v1/meetings?status=completed&limit=1&cursor={cursor}",
        headers=authorization(),
    )

    assert second.status_code == 200
    assert second.json()["next_cursor"] is None
    assert mismatched.status_code == 400
    assert mismatched.json()["type"].endswith("invalid-page-cursor")


async def test_meeting_collection_rejects_invalid_limits_and_cursors() -> None:
    for query in ("limit=0", "limit=101"):
        response = await request(f"/v1/meetings?{query}", headers=authorization())
        assert response.status_code == 422

    malformed = await request(
        "/v1/meetings?cursor=not-a-valid-cursor",
        headers=authorization(),
    )
    assert malformed.status_code == 400


async def test_processing_read_excludes_worker_and_lease_state() -> None:
    response = await request(
        f"/v1/meetings/{MEETING_ID}/processing",
        headers=authorization(),
    )
    removed_mutation = await request(
        f"/v1/meetings/{MEETING_ID}/processing",
        method="POST",
        headers=authorization(),
    )

    assert response.status_code == 200
    job = response.json()["jobs"][0]
    assert job["id"] == str(JOB_ID)
    assert job["status"] == "running"
    assert "lease_owner" not in job
    assert "lease_expires_at" not in job
    assert "claim_token" not in job
    retrying = response.json()["jobs"][1]
    assert retrying["failure"] == {
        "code": "provider_unavailable",
        "disposition": "retryable",
        "message": "The provider is temporarily unavailable",
        "occurred_at": "2026-08-07T09:00:00Z",
    }
    assert removed_mutation.status_code == 405


async def test_processing_retry_requires_concurrency_and_idempotency_headers() -> None:
    controls = FakeProcessingControls()
    services = dependencies(processing_controls=controls)
    response = await request(
        f"/v1/meetings/{MEETING_ID}/processing/retry",
        method="POST",
        services=services,
        headers=authorization(
            **{
                "If-Match": '"meeting-0"',
                "Idempotency-Key": "processing-retry-one",
            }
        ),
    )

    assert response.status_code == 202
    assert response.headers["location"] == f"/v1/meetings/{MEETING_ID}/processing"
    assert len(response.headers["etag"].strip('"')) == 64
    assert response.json()["replayed"] is False
    assert response.json()["jobs"][0]["status"] == "ready"
    assert controls.retry_request == (
        MEETING_ID,
        0,
        "processing-retry-one",
        "portfolio-owner",
    )

    missing = await request(
        f"/v1/meetings/{MEETING_ID}/processing/retry",
        method="POST",
        headers=authorization(**{"Idempotency-Key": "processing-retry-one"}),
    )
    assert missing.status_code == 428

    missing_key = await request(
        f"/v1/meetings/{MEETING_ID}/processing/retry",
        method="POST",
        headers=authorization(**{"If-Match": '"meeting-0"'}),
    )
    assert missing_key.status_code == 400


async def test_processing_retry_replay_returns_ok() -> None:
    response = await request(
        f"/v1/meetings/{MEETING_ID}/processing/retry",
        method="POST",
        services=dependencies(processing_controls=FakeProcessingControls(replayed=True)),
        headers=authorization(
            **{
                "If-Match": '"meeting-0"',
                "Idempotency-Key": "processing-retry-one",
            }
        ),
    )

    assert response.status_code == 200
    assert response.json()["replayed"] is True


async def test_cancellation_returns_the_new_meeting_version() -> None:
    controls = FakeProcessingControls()
    response = await request(
        f"/v1/meetings/{MEETING_ID}/cancellation",
        method="PUT",
        services=dependencies(processing_controls=controls),
        headers=authorization(
            **{
                "If-Match": '"meeting-0"',
                "Idempotency-Key": "cancellation-one",
            }
        ),
    )

    assert response.status_code == 200
    assert response.headers["etag"] == '"meeting-1"'
    assert response.json()["status"] == "cancelled"
    assert controls.cancel_request == (
        MEETING_ID,
        0,
        "cancellation-one",
        "portfolio-owner",
    )


async def test_recap_read_returns_the_canonical_artifact_etag() -> None:
    response = await request(
        f"/v1/meetings/{MEETING_ID}/recap",
        headers=authorization(),
    )

    assert response.status_code == 200
    assert response.headers["etag"] == f'"{recap().sha256}"'
    assert response.json() == {
        "id": str(RECAP_ID),
        "meeting_id": str(MEETING_ID),
        "approval_id": str(APPROVAL_ID),
        "format": "markdown",
        "content": review().recap_markdown,
        "sha256": recap().sha256,
        "created_at": "2026-08-07T09:00:00Z",
    }

    missing = await request(
        f"/v1/meetings/{MEETING_ID}/recap",
        services=dependencies(queries=MissingRecapQueries()),
        headers=authorization(),
    )
    assert missing.status_code == 404


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


async def test_delivery_etag_changes_when_only_reconciliation_schedule_changes() -> None:
    queries = FakeQueries()
    first_intent = unknown_write_intent()
    queries.delivery_result = DeliveryResult(meeting(), (first_intent,))
    services = dependencies(queries=queries)

    first = await request(
        f"/v1/meetings/{MEETING_ID}/delivery",
        services=services,
        headers=authorization(),
    )
    scheduled = WriteIntent.model_validate(
        first_intent.model_dump(mode="python")
        | {
            "next_reconcile_at": NOW + timedelta(minutes=1),
            "reconcile_attempt_count": 1,
            "version": 1,
        }
    )
    queries.delivery_result = DeliveryResult(meeting(), (scheduled,))
    second = await request(
        f"/v1/meetings/{MEETING_ID}/delivery",
        services=services,
        headers=authorization(),
    )

    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json()["meeting"]["version"] == second.json()["meeting"]["version"]
    assert first.headers["etag"] != second.headers["etag"]


async def test_retry_and_reconcile_require_and_return_control_headers() -> None:
    for operation in ("retry", "reconcile"):
        missing = await request(
            f"/v1/meetings/{MEETING_ID}/delivery/{operation}",
            method="POST",
            headers=authorization(),
            json={"intent_ids": []},
        )
        response = await request(
            f"/v1/meetings/{MEETING_ID}/delivery/{operation}",
            method="POST",
            headers=authorization(**{"Idempotency-Key": "delivery-one"}),
            json={"intent_ids": []},
        )

        assert missing.status_code == 400
        assert response.status_code == 200
        assert response.json()["meeting"]["id"] == str(MEETING_ID)
        assert response.headers["etag"].startswith('"')


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
    assert response.json()["instance"] == "/unmatched"
    assert MEETING_ID.hex not in response.text


async def test_request_body_limit_counts_chunked_payloads() -> None:
    app = create_app(dependencies(max_upload_bytes=1))
    transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        response = await client.post("/v1/meetings", content=ChunkedBody())

    assert response.status_code == 413
    assert response.headers["content-type"] == "application/problem+json"


async def test_meeting_erasure_request_returns_a_privacy_safe_job_resource() -> None:
    erasures = FakeErasures()
    response = await request(
        f"/v1/meetings/{MEETING_ID}",
        method="DELETE",
        services=dependencies(erasures=erasures),
        headers=authorization(
            **{
                "If-Match": '"meeting-0"',
                "Idempotency-Key": "erase-meeting-one",
            }
        ),
    )

    assert response.status_code == 202
    assert response.headers["location"] == f"/v1/meeting-erasures/{ERASURE_JOB_ID}"
    assert response.headers["etag"] == '"erasure-3"'
    assert response.headers["idempotency-replayed"] == "false"
    assert erasures.request_call == (
        MEETING_ID,
        0,
        "erase-meeting-one",
        "portfolio-owner",
    )
    payload = response.json()
    assert set(payload) == {
        "id",
        "status",
        "recording_state",
        "reason",
        "retry_count",
        "remediation_count",
        "max_remediations",
        "version",
        "failure",
        "next_attempt_at",
        "created_at",
        "updated_at",
        "completed_at",
    }
    assert payload["id"] == str(ERASURE_JOB_ID)
    assert payload["status"] == "active"
    assert payload["recording_state"] == "waiting_shared"
    for private_value in (
        PRIVATE_ERASURE_KEY_ID,
        "f" * 64,
        str(MEETING_ID),
        str(AUDIO_ID),
        "erase-meeting-one",
        "portfolio-owner",
    ):
        assert private_value not in response.text


async def test_erasure_resource_etag_has_one_canonical_body_across_replay_and_get() -> None:
    created = await request(
        f"/v1/meetings/{MEETING_ID}",
        method="DELETE",
        services=dependencies(erasures=FakeErasures()),
        headers=authorization(
            **{
                "If-Match": '"meeting-0"',
                "Idempotency-Key": "erase-meeting-one",
            }
        ),
    )
    replayed = await request(
        f"/v1/meetings/{MEETING_ID}",
        method="DELETE",
        services=dependencies(erasures=FakeErasures(replayed=True)),
        headers=authorization(
            **{
                "If-Match": '"meeting-0"',
                "Idempotency-Key": "erase-meeting-one",
            }
        ),
    )
    fetched = await request(
        f"/v1/meeting-erasures/{ERASURE_JOB_ID}",
        services=dependencies(),
        headers=authorization(),
    )

    assert created.status_code == 202
    assert replayed.status_code == 200
    assert fetched.status_code == 200
    assert created.headers["idempotency-replayed"] == "false"
    assert replayed.headers["idempotency-replayed"] == "true"
    assert created.headers["etag"] == replayed.headers["etag"] == fetched.headers["etag"]
    assert created.content == replayed.content == fetched.content


async def test_erasure_retry_uses_erasure_concurrency_and_idempotency_headers() -> None:
    erasures = FakeErasures()
    response = await request(
        f"/v1/meeting-erasures/{ERASURE_JOB_ID}/retry",
        method="POST",
        services=dependencies(erasures=erasures),
        headers=authorization(
            **{
                "If-Match": '"erasure-3"',
                "Idempotency-Key": "retry-erasure-one",
            }
        ),
    )
    missing_precondition = await request(
        f"/v1/meeting-erasures/{ERASURE_JOB_ID}/retry",
        method="POST",
        headers=authorization(**{"Idempotency-Key": "retry-erasure-one"}),
    )
    missing_key = await request(
        f"/v1/meeting-erasures/{ERASURE_JOB_ID}/retry",
        method="POST",
        headers=authorization(**{"If-Match": '"erasure-3"'}),
    )

    assert response.status_code == 202
    assert response.headers["location"] == f"/v1/meeting-erasures/{ERASURE_JOB_ID}"
    assert response.headers["etag"] == '"erasure-3"'
    assert erasures.retry_call == (
        ERASURE_JOB_ID,
        3,
        "retry-erasure-one",
        "portfolio-owner",
    )
    assert missing_precondition.status_code == 428
    assert "erasure job version" in missing_precondition.json()["detail"]
    assert missing_key.status_code == 400


async def test_erasure_retry_replay_returns_the_canonical_resource_with_ok() -> None:
    response = await request(
        f"/v1/meeting-erasures/{ERASURE_JOB_ID}/retry",
        method="POST",
        services=dependencies(erasures=FakeErasures(replayed=True)),
        headers=authorization(
            **{
                "If-Match": '"erasure-3"',
                "Idempotency-Key": "retry-erasure-one",
            }
        ),
    )

    assert response.status_code == 200
    assert response.headers["idempotency-replayed"] == "true"
    assert response.headers["etag"] == '"erasure-3"'
    assert response.json()["id"] == str(ERASURE_JOB_ID)


async def test_erasure_mutations_reject_duplicate_control_headers_before_dispatch() -> None:
    cases = (
        (
            f"/v1/meetings/{MEETING_ID}",
            "DELETE",
            "If-Match",
            '"meeting-0"',
        ),
        (
            f"/v1/meetings/{MEETING_ID}",
            "DELETE",
            "Idempotency-Key",
            "erase-meeting-one",
        ),
        (
            f"/v1/meeting-erasures/{ERASURE_JOB_ID}/retry",
            "POST",
            "If-Match",
            '"erasure-3"',
        ),
        (
            f"/v1/meeting-erasures/{ERASURE_JOB_ID}/retry",
            "POST",
            "Idempotency-Key",
            "retry-erasure-one",
        ),
    )
    for path, method, duplicate_name, duplicate_value in cases:
        erasures = FakeErasures()
        app = create_app(dependencies(erasures=erasures))
        transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
        headers = [
            ("Authorization", f"Bearer {VALID_TOKEN}"),
            (
                "If-Match",
                '"meeting-0"' if method == "DELETE" else '"erasure-3"',
            ),
            (
                "Idempotency-Key",
                "erase-meeting-one" if method == "DELETE" else "retry-erasure-one",
            ),
            (duplicate_name, duplicate_value),
        ]
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://testserver",
        ) as client:
            response = await client.request(method, path, headers=headers)

        assert response.status_code == 400
        assert response.json()["type"].endswith("ambiguous-control-header")
        assert erasures.request_call is None
        assert erasures.retry_call is None


async def test_erasure_errors_use_safe_resource_specific_problem_types() -> None:
    stale_delete = await request(
        f"/v1/meetings/{MEETING_ID}",
        method="DELETE",
        services=dependencies(erasures=FakeErasures(request_error=StaleWorkflowVersionError())),
        headers=authorization(
            **{
                "If-Match": '"meeting-0"',
                "Idempotency-Key": "erase-meeting-one",
            }
        ),
    )
    stale_retry = await request(
        f"/v1/meeting-erasures/{ERASURE_JOB_ID}/retry",
        method="POST",
        services=dependencies(erasures=FakeErasures(retry_error=StaleWorkflowVersionError())),
        headers=authorization(
            **{
                "If-Match": '"erasure-3"',
                "Idempotency-Key": "retry-erasure-one",
            }
        ),
    )
    blocked = await request(
        f"/v1/meetings/{MEETING_ID}",
        method="DELETE",
        services=dependencies(erasures=FakeErasures(request_error=MeetingErasureBlockedError())),
        headers=authorization(
            **{
                "If-Match": '"meeting-0"',
                "Idempotency-Key": "erase-meeting-one",
            }
        ),
    )
    missing = await request(
        f"/v1/meeting-erasures/{ERASURE_JOB_ID}",
        services=dependencies(
            erasures=FakeErasures(get_error=ResourceNotFoundError("private-erasure-resource"))
        ),
        headers=authorization(),
    )
    invalid_uuid = await request(
        "/v1/meeting-erasures/not-a-uuid",
        headers=authorization(),
    )

    assert stale_delete.status_code == 412
    assert stale_delete.json()["type"].endswith("stale-meeting")
    assert stale_delete.json()["instance"] == "/v1/meetings/{meeting_id}"
    assert str(MEETING_ID) not in stale_delete.json()["instance"]
    assert stale_retry.status_code == 412
    assert stale_retry.json()["type"].endswith("stale-erasure")
    assert stale_retry.json()["instance"] == ("/v1/meeting-erasures/{erasure_job_id}/retry")
    assert str(ERASURE_JOB_ID) not in stale_retry.json()["instance"]
    assert blocked.status_code == 409
    assert blocked.json()["detail"] == "The operation conflicts with the current workflow state."
    assert missing.status_code == 404
    assert "private-erasure-resource" not in missing.text
    assert missing.json()["instance"] == "/v1/meeting-erasures/{erasure_job_id}"
    assert str(ERASURE_JOB_ID) not in missing.json()["instance"]
    assert invalid_uuid.status_code == 422
    assert invalid_uuid.headers["content-type"] == "application/problem+json"
    assert invalid_uuid.json()["instance"] == "/v1/meeting-erasures/{erasure_job_id}"
    assert "not-a-uuid" not in invalid_uuid.json()["instance"]


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
    processing_retry = schema["paths"]["/v1/meetings/{meeting_id}/processing/retry"]["post"]
    assert "200" in processing_retry["responses"]
    assert "202" in processing_retry["responses"]
    assert {
        parameter["name"]: parameter["required"]
        for parameter in processing_retry["parameters"]
        if parameter["in"] == "header"
    } == {"If-Match": True, "Idempotency-Key": True}
    assert set(processing_retry["responses"]["202"]["headers"]) == {"ETag", "Location"}
    cancellation = schema["paths"]["/v1/meetings/{meeting_id}/cancellation"]["put"]
    assert {
        parameter["name"]: parameter["required"]
        for parameter in cancellation["parameters"]
        if parameter["in"] == "header"
    } == {"If-Match": True, "Idempotency-Key": True}
    assert "ETag" in cancellation["responses"]["200"]["headers"]
    delivery_read = schema["paths"]["/v1/meetings/{meeting_id}/delivery"]["get"]
    assert "ETag" in delivery_read["responses"]["200"]["headers"]
    for operation in ("retry", "reconcile"):
        control = schema["paths"][f"/v1/meetings/{{meeting_id}}/delivery/{operation}"]["post"]
        assert {
            parameter["name"]: parameter["required"]
            for parameter in control["parameters"]
            if parameter["in"] == "header"
        } == {"Idempotency-Key": True}
        assert "ETag" in control["responses"]["200"]["headers"]
    erasure_operations = (
        schema["paths"]["/v1/meetings/{meeting_id}"]["delete"],
        schema["paths"]["/v1/meeting-erasures/{erasure_job_id}"]["get"],
        schema["paths"]["/v1/meeting-erasures/{erasure_job_id}/retry"]["post"],
    )
    for operation in erasure_operations:
        for status in (400, 401, 404, 409, 412, 413, 422, 428, 500, 503):
            problem = operation["responses"][str(status)]
            assert set(problem["content"]) == {"application/problem+json"}
            problem_schema = problem["content"]["application/problem+json"]["schema"]
            assert problem_schema["title"] == "ProblemDetail"
            assert "$defs" not in str(problem_schema)
    erasure_delete, erasure_read, erasure_retry = erasure_operations
    for operation in (erasure_delete, erasure_retry):
        assert {
            parameter["name"]: parameter["required"]
            for parameter in operation["parameters"]
            if parameter["in"] == "header"
        } == {"If-Match": True, "Idempotency-Key": True}
        assert set(operation["responses"]["202"]["headers"]) == {
            "ETag",
            "Idempotency-Replayed",
            "Location",
        }
        assert set(operation["responses"]["200"]["headers"]) == {
            "ETag",
            "Idempotency-Replayed",
            "Location",
        }
    assert set(erasure_read["responses"]["200"]["headers"]) == {"ETag"}
    erasure_schema = schema["components"]["schemas"]["MeetingErasureResponse"]
    assert set(erasure_schema["properties"]) == {
        "id",
        "status",
        "recording_state",
        "reason",
        "retry_count",
        "remediation_count",
        "max_remediations",
        "version",
        "failure",
        "next_attempt_at",
        "created_at",
        "updated_at",
        "completed_at",
    }
