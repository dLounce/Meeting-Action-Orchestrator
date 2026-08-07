from __future__ import annotations

from http import HTTPStatus
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Header, Request, Response

from meeting_action_orchestrator.api.dependencies import (
    ApiDependenciesValue,
    PrincipalValue,
    format_etag,
    parse_erasure_precondition,
    parse_idempotency_key,
    parse_meeting_precondition,
)
from meeting_action_orchestrator.api.erasure_schemas import MeetingErasureResponse
from meeting_action_orchestrator.api.openapi import problem_responses
from meeting_action_orchestrator.api.problems import ProblemError, create_problem
from meeting_action_orchestrator.application.errors import StaleWorkflowVersionError

PROBLEM_RESPONSES = problem_responses((400, 401, 404, 409, 412, 413, 422, 428, 500, 503))
ETAG_HEADER = {
    "description": "Strong validator for the returned erasure job representation.",
    "schema": {"type": "string"},
}
LOCATION_HEADER = {
    "description": "Canonical meeting erasure job resource.",
    "schema": {"type": "string"},
}
REPLAY_HEADER = {
    "description": "Whether the idempotency key replayed an existing request binding.",
    "schema": {"type": "boolean"},
}
MUTATION_HEADERS = {
    "ETag": ETAG_HEADER,
    "Idempotency-Replayed": REPLAY_HEADER,
    "Location": LOCATION_HEADER,
}

erasure_router = APIRouter(
    tags=["meeting erasures"],
    responses=PROBLEM_RESPONSES,
)


def _require_single_control_headers(request: Request) -> None:
    for header_name in ("if-match", "idempotency-key"):
        if len(request.headers.getlist(header_name)) != 1:
            raise ProblemError(
                create_problem(
                    400,
                    detail="The request contains ambiguous control headers.",
                    type_uri="urn:meeting-action-orchestrator:problem:ambiguous-control-header",
                )
            )


@erasure_router.delete(
    "/v1/meetings/{meeting_id}",
    status_code=HTTPStatus.ACCEPTED,
    response_model=MeetingErasureResponse,
    responses={
        HTTPStatus.OK: {
            "model": MeetingErasureResponse,
            "headers": MUTATION_HEADERS,
        },
        HTTPStatus.ACCEPTED: {"headers": MUTATION_HEADERS},
    },
    operation_id="eraseMeeting",
)
async def erase_meeting(
    meeting_id: UUID,
    request: Request,
    response: Response,
    dependencies: ApiDependenciesValue,
    principal: PrincipalValue,
    if_match: Annotated[
        str,
        Header(
            alias="If-Match",
            description="Strong ETag of the meeting version to erase.",
        ),
    ],
    idempotency_key: Annotated[
        str,
        Header(
            alias="Idempotency-Key",
            description="Stable key for this erasure request.",
        ),
    ],
) -> MeetingErasureResponse:
    _require_single_control_headers(request)
    result = await dependencies.erasures.request(
        meeting_id,
        expected_version=parse_meeting_precondition(if_match),
        request_key=parse_idempotency_key(idempotency_key),
        actor_id=principal.subject,
    )
    payload = MeetingErasureResponse.from_domain(result.job)
    response.status_code = HTTPStatus.OK if result.replayed else HTTPStatus.ACCEPTED
    response.headers["Location"] = f"/v1/meeting-erasures/{result.job.id}"
    response.headers["ETag"] = format_etag(f"erasure-{result.job.version}")
    response.headers["Idempotency-Replayed"] = str(result.replayed).lower()
    return payload


@erasure_router.get(
    "/v1/meeting-erasures/{erasure_job_id}",
    response_model=MeetingErasureResponse,
    responses={HTTPStatus.OK: {"headers": {"ETag": ETAG_HEADER}}},
    operation_id="getMeetingErasure",
)
async def get_meeting_erasure(
    erasure_job_id: UUID,
    response: Response,
    dependencies: ApiDependenciesValue,
    _principal: PrincipalValue,
) -> MeetingErasureResponse:
    job = await dependencies.erasures.get(erasure_job_id)
    response.headers["ETag"] = format_etag(f"erasure-{job.version}")
    return MeetingErasureResponse.from_domain(job)


@erasure_router.post(
    "/v1/meeting-erasures/{erasure_job_id}/retry",
    status_code=HTTPStatus.ACCEPTED,
    response_model=MeetingErasureResponse,
    responses={
        HTTPStatus.OK: {
            "model": MeetingErasureResponse,
            "headers": MUTATION_HEADERS,
        },
        HTTPStatus.ACCEPTED: {"headers": MUTATION_HEADERS},
    },
    operation_id="retryMeetingErasure",
)
async def retry_meeting_erasure(
    erasure_job_id: UUID,
    request: Request,
    response: Response,
    dependencies: ApiDependenciesValue,
    principal: PrincipalValue,
    if_match: Annotated[
        str,
        Header(
            alias="If-Match",
            description="Strong ETag of the erasure job version to retry.",
        ),
    ],
    idempotency_key: Annotated[
        str,
        Header(
            alias="Idempotency-Key",
            description="Stable key for this remediation request.",
        ),
    ],
) -> MeetingErasureResponse:
    _require_single_control_headers(request)
    try:
        result = await dependencies.erasures.retry(
            erasure_job_id,
            expected_version=parse_erasure_precondition(if_match),
            request_key=parse_idempotency_key(idempotency_key),
            actor_id=principal.subject,
        )
    except StaleWorkflowVersionError as error:
        raise ProblemError(
            create_problem(
                412,
                detail="The erasure job changed before the operation completed.",
                type_uri="urn:meeting-action-orchestrator:problem:stale-erasure",
            )
        ) from error
    payload = MeetingErasureResponse.from_domain(result.job)
    response.status_code = HTTPStatus.OK if result.replayed else HTTPStatus.ACCEPTED
    response.headers["Location"] = f"/v1/meeting-erasures/{result.job.id}"
    response.headers["ETag"] = format_etag(f"erasure-{result.job.version}")
    response.headers["Idempotency-Replayed"] = str(result.replayed).lower()
    return payload
