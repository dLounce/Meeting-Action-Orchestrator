from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict

from meeting_action_orchestrator.domain.enums import (
    FailureDisposition,
    MeetingErasureFailureCode,
    MeetingErasureReason,
    MeetingErasureRecordingState,
    MeetingErasureStatus,
)
from meeting_action_orchestrator.domain.models import MeetingErasureJob


class ErasureApiModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class MeetingErasureFailureResponse(ErasureApiModel):
    code: MeetingErasureFailureCode
    disposition: FailureDisposition
    occurred_at: datetime


class MeetingErasureResponse(ErasureApiModel):
    id: UUID
    status: MeetingErasureStatus
    recording_state: MeetingErasureRecordingState
    reason: MeetingErasureReason
    retry_count: int
    remediation_count: int
    max_remediations: int
    version: int
    failure: MeetingErasureFailureResponse | None
    next_attempt_at: datetime | None
    created_at: datetime
    updated_at: datetime
    completed_at: datetime | None

    @classmethod
    def from_domain(
        cls,
        job: MeetingErasureJob,
    ) -> MeetingErasureResponse:
        failure = job.last_failure
        return cls(
            id=job.id,
            status=job.status,
            recording_state=job.recording_state,
            reason=job.reason,
            retry_count=job.retry_count,
            remediation_count=job.remediation_count,
            max_remediations=job.max_remediations,
            version=job.version,
            failure=(
                MeetingErasureFailureResponse(
                    code=failure.code,
                    disposition=failure.disposition,
                    occurred_at=failure.occurred_at,
                )
                if failure is not None
                else None
            ),
            next_attempt_at=job.next_attempt_at,
            created_at=job.created_at,
            updated_at=job.updated_at,
            completed_at=job.completed_at,
        )
