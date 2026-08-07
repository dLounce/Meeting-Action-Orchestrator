from __future__ import annotations

import asyncio
from collections.abc import Callable
from uuid import UUID

from meeting_action_orchestrator.application.errors import ResourceNotFoundError
from meeting_action_orchestrator.application.meeting_erasure import (
    MeetingErasureRemediationService,
    MeetingErasureResult,
    MeetingErasureService,
)
from meeting_action_orchestrator.application.ports import UnitOfWork
from meeting_action_orchestrator.domain.models import MeetingErasureJob

UnitOfWorkFactory = Callable[[], UnitOfWork]


class AsyncErasureFacade:
    def __init__(
        self,
        requests: MeetingErasureService,
        remediations: MeetingErasureRemediationService,
        queries: UnitOfWorkFactory,
    ) -> None:
        self._requests = requests
        self._remediations = remediations
        self._queries = queries

    async def request(
        self,
        meeting_id: UUID,
        *,
        expected_version: int,
        request_key: str,
        actor_id: str,
    ) -> MeetingErasureResult:
        return await self._requests.request(
            meeting_id,
            expected_version=expected_version,
            request_key=request_key,
            actor_id=actor_id,
        )

    async def get(self, erasure_job_id: UUID) -> MeetingErasureJob:
        return await asyncio.to_thread(self._get, erasure_job_id)

    async def retry(
        self,
        erasure_job_id: UUID,
        *,
        expected_version: int,
        request_key: str,
        actor_id: str,
    ) -> MeetingErasureResult:
        return await self._remediations.retry(
            erasure_job_id,
            expected_version=expected_version,
            request_key=request_key,
            actor_id=actor_id,
        )

    def _get(self, erasure_job_id: UUID) -> MeetingErasureJob:
        with self._queries() as uow:
            job = uow.meeting_erasures.get(erasure_job_id)
        if job is None:
            raise ResourceNotFoundError("Meeting erasure job")
        return job
