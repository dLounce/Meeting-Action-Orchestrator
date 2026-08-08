from __future__ import annotations

from http import HTTPStatus
from pathlib import Path
from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, File, Form, Header, Query, Response, UploadFile
from pydantic import ValidationError

from meeting_action_orchestrator.api.dependencies import (
    ApiDependenciesValue,
    PrincipalValue,
    format_etag,
    format_meeting_cursor,
    parse_idempotency_key,
    parse_meeting_cursor,
    parse_meeting_precondition,
    parse_review_precondition,
)
from meeting_action_orchestrator.api.openapi import problem_responses
from meeting_action_orchestrator.api.problems import (
    VALIDATION_PROBLEM_TYPE,
    FieldViolation,
    ProblemError,
    create_problem,
)
from meeting_action_orchestrator.api.schemas import (
    ActionRevisionRequest,
    ApprovalResponse,
    CreateMeetingRequest,
    DeliveryOperationRequest,
    DeliveryResponse,
    DeliverySelectionRequest,
    HealthResponse,
    IssueResolutionRequest,
    MeetingListResponse,
    MeetingResponse,
    ProcessingControlResponse,
    ProcessingResponse,
    ReadinessResponse,
    RecapResponse,
    ReviewResponse,
    TranscriptResponse,
)
from meeting_action_orchestrator.application.reviewing import ActionEdit, IssueResolutionEdit
from meeting_action_orchestrator.application.workflow import IngestMeeting
from meeting_action_orchestrator.domain.enums import IssueStatus, MeetingStatus, WriteKind
from meeting_action_orchestrator.domain.errors import DomainError
from meeting_action_orchestrator.domain.hashing import canonical_sha256

PROBLEM_RESPONSES = problem_responses((400, 401, 404, 409, 412, 413, 422, 428, 500, 502, 503))
ETAG_HEADER = {
    "description": "Strong validator for the returned representation.",
    "schema": {"type": "string"},
}
LOCATION_HEADER = {
    "description": "Canonical resource for the processing state.",
    "schema": {"type": "string"},
}
PROCESSING_CONTROL_HEADERS = {
    "ETag": ETAG_HEADER,
    "Location": LOCATION_HEADER,
}
SUPPORTED_DECLARED_MEDIA_TYPES = frozenset(
    {
        "application/octet-stream",
        "audio/mp4",
        "audio/mpeg",
        "audio/wav",
        "audio/x-m4a",
        "audio/x-wav",
        "video/mp4",
    }
)

health_router = APIRouter(tags=["health"])
meeting_router = APIRouter(
    prefix="/v1/meetings",
    tags=["meetings"],
    responses=PROBLEM_RESPONSES,
)


@health_router.get(
    "/health/live",
    response_model=HealthResponse,
    operation_id="getLiveness",
)
async def liveness(dependencies: ApiDependenciesValue) -> HealthResponse:
    return HealthResponse(version=dependencies.service_version)


@health_router.get(
    "/health/ready",
    response_model=ReadinessResponse,
    responses={503: {"model": ReadinessResponse}},
    operation_id="getReadiness",
)
async def readiness(
    response: Response,
    dependencies: ApiDependenciesValue,
) -> ReadinessResponse:
    result = await dependencies.readiness.check()
    if not result.ready:
        response.status_code = HTTPStatus.SERVICE_UNAVAILABLE
    return ReadinessResponse.from_result(result)


@meeting_router.post(
    "",
    status_code=HTTPStatus.CREATED,
    response_model=MeetingResponse,
    operation_id="createMeeting",
)
async def create_meeting(
    *,
    response: Response,
    metadata: Annotated[str, Form(min_length=2, max_length=32_000)],
    recording: Annotated[UploadFile, File()],
    dependencies: ApiDependenciesValue,
    principal: PrincipalValue,
    idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
) -> MeetingResponse:
    request_key = parse_idempotency_key(idempotency_key)
    request = _parse_create_request(metadata)
    try:
        original_name = await _validate_upload(recording, dependencies.max_upload_bytes)
        command = IngestMeeting(
            title=request.title,
            occurred_at=request.occurred_at,
            timezone=request.timezone,
            original_name=original_name,
            ingest_key=request_key,
            actor_id=principal.subject,
            participants=tuple(item.to_domain() for item in request.participants),
        )
        try:
            meeting = await dependencies.workflow.ingest(command, recording.file)
        except DomainError:
            raise
        except ValueError as exc:
            raise _invalid_input("The recording or meeting metadata is invalid.") from exc
    finally:
        await recording.close()
    response.headers["Location"] = f"/v1/meetings/{meeting.id}"
    response.headers["ETag"] = format_etag(f"meeting-{meeting.version}")
    return MeetingResponse.from_domain(meeting)


@meeting_router.get(
    "",
    response_model=MeetingListResponse,
    operation_id="listMeetings",
)
async def list_meetings(
    dependencies: ApiDependenciesValue,
    _principal: PrincipalValue,
    status: MeetingStatus | None = None,
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
    cursor: Annotated[str | None, Query(min_length=1, max_length=128)] = None,
) -> MeetingListResponse:
    result = await dependencies.queries.list_meetings(
        status=status,
        cursor=parse_meeting_cursor(cursor, status),
        limit=limit,
    )
    return MeetingListResponse.from_result(
        result,
        format_meeting_cursor(result.next_cursor, status),
    )


@meeting_router.get(
    "/{meeting_id}",
    response_model=MeetingResponse,
    operation_id="getMeeting",
)
async def get_meeting(
    meeting_id: UUID,
    response: Response,
    dependencies: ApiDependenciesValue,
    _principal: PrincipalValue,
) -> MeetingResponse:
    meeting = await dependencies.workflow.get_meeting(meeting_id)
    response.headers["ETag"] = format_etag(f"meeting-{meeting.version}")
    return MeetingResponse.from_domain(meeting)


@meeting_router.get(
    "/{meeting_id}/processing",
    response_model=ProcessingResponse,
    operation_id="getMeetingProcessing",
)
async def get_processing(
    meeting_id: UUID,
    dependencies: ApiDependenciesValue,
    _principal: PrincipalValue,
) -> ProcessingResponse:
    result = await dependencies.queries.get_processing(meeting_id)
    return ProcessingResponse.from_result(result)


@meeting_router.post(
    "/{meeting_id}/processing/retry",
    status_code=HTTPStatus.ACCEPTED,
    response_model=ProcessingControlResponse,
    responses={
        HTTPStatus.OK: {
            "model": ProcessingControlResponse,
            "headers": PROCESSING_CONTROL_HEADERS,
        },
        HTTPStatus.ACCEPTED: {"headers": PROCESSING_CONTROL_HEADERS},
    },
    operation_id="retryMeetingProcessing",
)
async def retry_processing(
    meeting_id: UUID,
    *,
    response: Response,
    dependencies: ApiDependenciesValue,
    principal: PrincipalValue,
    if_match: Annotated[str, Header(alias="If-Match")],
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
) -> ProcessingControlResponse:
    result = await dependencies.processing_controls.retry(
        meeting_id,
        expected_version=parse_meeting_precondition(if_match),
        request_key=parse_idempotency_key(idempotency_key),
        actor_id=principal.subject,
    )
    payload = ProcessingControlResponse.from_result(result)
    response.status_code = HTTPStatus.OK if result.replayed else HTTPStatus.ACCEPTED
    response.headers["Location"] = f"/v1/meetings/{meeting_id}/processing"
    response.headers["ETag"] = format_etag(canonical_sha256(payload))
    return payload


@meeting_router.put(
    "/{meeting_id}/cancellation",
    response_model=MeetingResponse,
    responses={HTTPStatus.OK: {"headers": {"ETag": ETAG_HEADER}}},
    operation_id="cancelMeeting",
)
async def cancel_meeting(
    meeting_id: UUID,
    *,
    response: Response,
    dependencies: ApiDependenciesValue,
    principal: PrincipalValue,
    if_match: Annotated[str, Header(alias="If-Match")],
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
) -> MeetingResponse:
    result = await dependencies.processing_controls.cancel(
        meeting_id,
        expected_version=parse_meeting_precondition(if_match),
        request_key=parse_idempotency_key(idempotency_key),
        actor_id=principal.subject,
    )
    response.headers["ETag"] = format_etag(f"meeting-{result.meeting.version}")
    return MeetingResponse.from_domain(result.meeting)


@meeting_router.get(
    "/{meeting_id}/transcript",
    response_model=TranscriptResponse,
    operation_id="getMeetingTranscript",
)
async def get_transcript(
    meeting_id: UUID,
    response: Response,
    dependencies: ApiDependenciesValue,
    _principal: PrincipalValue,
) -> TranscriptResponse:
    transcript = await dependencies.queries.get_transcript(meeting_id)
    response.headers["ETag"] = format_etag(transcript.sha256)
    return TranscriptResponse.from_domain(transcript)


@meeting_router.get(
    "/{meeting_id}/review",
    response_model=ReviewResponse,
    operation_id="getMeetingReview",
)
async def get_review(
    meeting_id: UUID,
    response: Response,
    dependencies: ApiDependenciesValue,
    _principal: PrincipalValue,
) -> ReviewResponse:
    review = await dependencies.queries.get_review(meeting_id)
    response.headers["ETag"] = format_etag(review.content_digest)
    return ReviewResponse.from_domain(review)


@meeting_router.get(
    "/{meeting_id}/recap",
    response_model=RecapResponse,
    operation_id="getMeetingRecap",
)
async def get_recap(
    meeting_id: UUID,
    response: Response,
    dependencies: ApiDependenciesValue,
    _principal: PrincipalValue,
) -> RecapResponse:
    recap = await dependencies.queries.get_recap(meeting_id)
    response.headers["ETag"] = format_etag(recap.sha256)
    return RecapResponse.from_domain(recap)


@meeting_router.patch(
    "/{meeting_id}/review/actions/{action_id}",
    response_model=ReviewResponse,
    operation_id="reviseMeetingAction",
)
async def revise_action(
    meeting_id: UUID,
    action_id: UUID,
    *,
    request: ActionRevisionRequest,
    response: Response,
    dependencies: ApiDependenciesValue,
    principal: PrincipalValue,
    if_match: Annotated[str | None, Header(alias="If-Match")] = None,
) -> ReviewResponse:
    expected_digest = parse_review_precondition(if_match)
    edit = ActionEdit(
        action_id=action_id,
        title=request.title,
        owner=request.owner,
        due_date=request.due_date,
        due_time=request.due_time,
        timezone=request.timezone,
        notes=request.notes,
        recap_markdown=request.recap_markdown,
    )
    try:
        review = await dependencies.reviews.revise_action(
            meeting_id,
            expected_digest=expected_digest,
            edit=edit,
            actor_id=principal.subject,
        )
    except DomainError:
        raise
    except ValueError as exc:
        raise _invalid_input("The action revision is invalid.") from exc
    response.headers["ETag"] = format_etag(review.content_digest)
    return ReviewResponse.from_domain(review)


@meeting_router.patch(
    "/{meeting_id}/review/issues/{issue_id}",
    response_model=ReviewResponse,
    operation_id="resolveReviewIssue",
)
async def resolve_issue(
    meeting_id: UUID,
    issue_id: UUID,
    *,
    request: IssueResolutionRequest,
    response: Response,
    dependencies: ApiDependenciesValue,
    principal: PrincipalValue,
    if_match: Annotated[str | None, Header(alias="If-Match")] = None,
) -> ReviewResponse:
    expected_digest = parse_review_precondition(if_match)
    edit = IssueResolutionEdit(
        issue_id=issue_id,
        status=IssueStatus(request.status),
        resolution_note=request.resolution_note,
    )
    try:
        review = await dependencies.reviews.revise_issue(
            meeting_id,
            expected_digest=expected_digest,
            edit=edit,
            actor_id=principal.subject,
        )
    except DomainError:
        raise
    except ValueError as exc:
        raise _invalid_input("The issue resolution is invalid.") from exc
    response.headers["ETag"] = format_etag(review.content_digest)
    return ReviewResponse.from_domain(review)


@meeting_router.put(
    "/{meeting_id}/review/actions/{action_id}/deliveries/{kind}",
    response_model=ReviewResponse,
    operation_id="selectActionDelivery",
)
async def select_delivery(
    meeting_id: UUID,
    action_id: UUID,
    kind: WriteKind,
    *,
    request: DeliverySelectionRequest,
    response: Response,
    dependencies: ApiDependenciesValue,
    principal: PrincipalValue,
    if_match: Annotated[str | None, Header(alias="If-Match")] = None,
) -> ReviewResponse:
    expected_digest = parse_review_precondition(if_match)
    try:
        review = await dependencies.reviews.revise_delivery(
            meeting_id,
            expected_digest=expected_digest,
            action_id=action_id,
            kind=kind,
            enabled=request.enabled,
            actor_id=principal.subject,
        )
    except DomainError:
        raise
    except ValueError as exc:
        raise _invalid_input("The delivery selection is invalid.") from exc
    response.headers["ETag"] = format_etag(review.content_digest)
    return ReviewResponse.from_domain(review)


@meeting_router.post(
    "/{meeting_id}/approval",
    status_code=HTTPStatus.CREATED,
    response_model=ApprovalResponse,
    responses={HTTPStatus.OK: {"model": ApprovalResponse}},
    operation_id="approveMeetingReview",
)
async def approve_review(
    meeting_id: UUID,
    *,
    response: Response,
    dependencies: ApiDependenciesValue,
    principal: PrincipalValue,
    if_match: Annotated[str | None, Header(alias="If-Match")] = None,
    idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
) -> ApprovalResponse:
    expected_digest = parse_review_precondition(if_match)
    request_key = parse_idempotency_key(idempotency_key)
    result = await dependencies.workflow.approve(
        meeting_id,
        expected_digest=expected_digest,
        request_key=request_key,
        actor_id=principal.subject,
    )
    response.status_code = HTTPStatus.OK if result.replayed else HTTPStatus.CREATED
    response.headers["Location"] = f"/v1/meetings/{meeting_id}/delivery"
    response.headers["ETag"] = format_etag(result.approval.review_digest)
    return ApprovalResponse.from_result(result)


@meeting_router.get(
    "/{meeting_id}/delivery",
    response_model=DeliveryResponse,
    operation_id="getMeetingDelivery",
    responses={HTTPStatus.OK: {"headers": {"ETag": ETAG_HEADER}}},
)
async def get_delivery(
    meeting_id: UUID,
    response: Response,
    dependencies: ApiDependenciesValue,
    _principal: PrincipalValue,
) -> DeliveryResponse:
    result = await dependencies.queries.get_delivery(meeting_id)
    payload = DeliveryResponse.from_result(result)
    response.headers["ETag"] = format_etag(canonical_sha256(payload))
    return payload


@meeting_router.post(
    "/{meeting_id}/delivery/retry",
    response_model=DeliveryResponse,
    operation_id="retryMeetingDelivery",
    responses={HTTPStatus.OK: {"headers": {"ETag": ETAG_HEADER}}},
)
async def retry_delivery(
    meeting_id: UUID,
    *,
    request: DeliveryOperationRequest,
    response: Response,
    dependencies: ApiDependenciesValue,
    principal: PrincipalValue,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
) -> DeliveryResponse:
    result = await dependencies.deliveries.retry(
        meeting_id,
        intent_ids=request.intent_ids,
        request_key=parse_idempotency_key(idempotency_key),
        actor_id=principal.subject,
    )
    payload = DeliveryResponse.from_result(result)
    response.headers["ETag"] = format_etag(canonical_sha256(payload))
    return payload


@meeting_router.post(
    "/{meeting_id}/delivery/reconcile",
    response_model=DeliveryResponse,
    operation_id="reconcileMeetingDelivery",
    responses={HTTPStatus.OK: {"headers": {"ETag": ETAG_HEADER}}},
)
async def reconcile_delivery(
    meeting_id: UUID,
    *,
    request: DeliveryOperationRequest,
    response: Response,
    dependencies: ApiDependenciesValue,
    principal: PrincipalValue,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
) -> DeliveryResponse:
    result = await dependencies.deliveries.reconcile(
        meeting_id,
        intent_ids=request.intent_ids,
        request_key=parse_idempotency_key(idempotency_key),
        actor_id=principal.subject,
    )
    payload = DeliveryResponse.from_result(result)
    response.headers["ETag"] = format_etag(canonical_sha256(payload))
    return payload


def _parse_create_request(metadata: str) -> CreateMeetingRequest:
    try:
        return CreateMeetingRequest.model_validate_json(metadata)
    except ValidationError as exc:
        errors = tuple(
            FieldViolation(
                location=_metadata_error_location(error.get("loc", ())),
                message=str(error.get("msg", "Invalid value")),
                code=str(error.get("type", "validation_error")),
            )
            for error in exc.errors(include_url=False, include_context=False, include_input=False)
        )
        raise ProblemError(
            create_problem(
                422,
                title="Request validation failed",
                detail="The meeting metadata is invalid.",
                type_uri=VALIDATION_PROBLEM_TYPE,
                errors=errors,
            )
        ) from exc


async def _validate_upload(recording: UploadFile, max_upload_bytes: int) -> str:
    original_name = recording.filename or ""
    normalized = original_name.replace("\\", "/")
    if (
        not original_name
        or len(original_name) > 200
        or Path(normalized).name != original_name
        or original_name in {".", ".."}
        or "\x00" in original_name
    ):
        raise _invalid_input("The recording filename is invalid.")
    content_type = (recording.content_type or "application/octet-stream").lower()
    if content_type not in SUPPORTED_DECLARED_MEDIA_TYPES:
        raise _invalid_input("The recording must be declared as MP3, M4A, MP4, or WAV audio.")
    size = recording.size
    if size is None:
        size = 0
        while chunk := await recording.read(1024 * 1024):
            size += len(chunk)
            if size > max_upload_bytes:
                break
        await recording.seek(0)
    if size == 0:
        raise _invalid_input("The recording is empty.")
    if size > max_upload_bytes:
        raise ProblemError(
            create_problem(
                413,
                detail="The recording exceeds the configured upload limit.",
                type_uri="urn:meeting-action-orchestrator:problem:recording-too-large",
            )
        )
    return original_name


def _invalid_input(detail: str) -> ProblemError:
    return ProblemError(
        create_problem(
            422,
            title="Request validation failed",
            detail=detail,
            type_uri=VALIDATION_PROBLEM_TYPE,
        )
    )


def _metadata_error_location(location: Any) -> str:
    parts = ["body", "metadata"]
    if isinstance(location, tuple):
        parts.extend(str(item) for item in location)
    return ".".join(parts)
