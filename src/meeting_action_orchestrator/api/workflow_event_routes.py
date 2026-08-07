from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Query, Request

from meeting_action_orchestrator.api.dependencies import ApiDependenciesValue, PrincipalValue
from meeting_action_orchestrator.api.openapi import problem_responses
from meeting_action_orchestrator.api.workflow_event_cursors import (
    format_workflow_event_cursor,
    parse_workflow_event_cursor_values,
)
from meeting_action_orchestrator.api.workflow_event_schemas import WorkflowEventPageResponse

workflow_event_router = APIRouter(
    prefix="/v1/meetings",
    tags=["meeting events"],
    responses=problem_responses((400, 401, 404, 422, 500, 503)),
)


@workflow_event_router.get(
    "/{meeting_id}/events",
    response_model=WorkflowEventPageResponse,
    operation_id="listMeetingWorkflowEvents",
)
async def list_workflow_events(
    request: Request,
    meeting_id: UUID,
    dependencies: ApiDependenciesValue,
    _principal: PrincipalValue,
    limit: Annotated[
        int,
        Query(ge=1, le=100, description="Maximum events returned in ascending sequence order."),
    ] = 20,
    _cursor: Annotated[
        str | None,
        Query(alias="cursor", description="Opaque meeting-bound workflow event page cursor."),
    ] = None,
) -> WorkflowEventPageResponse:
    result = await dependencies.queries.list_workflow_events(
        meeting_id,
        cursor=parse_workflow_event_cursor_values(
            request.query_params.getlist("cursor"), meeting_id
        ),
        limit=limit,
    )
    return WorkflowEventPageResponse.from_result(
        result,
        format_workflow_event_cursor(result.next_cursor),
    )
