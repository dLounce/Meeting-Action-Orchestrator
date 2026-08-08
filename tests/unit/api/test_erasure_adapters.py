from __future__ import annotations

import threading
from typing import Literal, cast
from uuid import UUID

import pytest

from meeting_action_orchestrator.api.erasure_adapters import (
    AsyncErasureFacade,
    UnitOfWorkFactory,
)
from meeting_action_orchestrator.application.errors import ResourceNotFoundError
from meeting_action_orchestrator.application.meeting_erasure import (
    MeetingErasureRemediationService,
    MeetingErasureResult,
    MeetingErasureService,
)
from meeting_action_orchestrator.domain.models import MeetingErasureJob
from tests.unit.api.test_app import ERASURE_JOB_ID, MEETING_ID, erasure_job


class RequestService:
    def __init__(self) -> None:
        self.calls: list[tuple[UUID, int, str, str]] = []

    async def request(
        self,
        meeting_id: UUID,
        *,
        expected_version: int,
        request_key: str,
        actor_id: str,
    ) -> MeetingErasureResult:
        self.calls.append((meeting_id, expected_version, request_key, actor_id))
        return MeetingErasureResult(erasure_job(), replayed=True)


class RemediationService:
    def __init__(self) -> None:
        self.calls: list[tuple[UUID, int, str, str]] = []

    async def retry(
        self,
        erasure_job_id: UUID,
        *,
        expected_version: int,
        request_key: str,
        actor_id: str,
    ) -> MeetingErasureResult:
        self.calls.append((erasure_job_id, expected_version, request_key, actor_id))
        return MeetingErasureResult(erasure_job())


class ErasureRepository:
    def __init__(self, value: MeetingErasureJob | None) -> None:
        self.value = value
        self.thread_id: int | None = None

    def get(self, erasure_job_id: UUID) -> MeetingErasureJob | None:
        assert erasure_job_id == ERASURE_JOB_ID
        self.thread_id = threading.get_ident()
        return self.value


class QueryUnitOfWork:
    def __init__(self, value: MeetingErasureJob | None) -> None:
        self.meeting_erasures = ErasureRepository(value)

    def __enter__(self) -> QueryUnitOfWork:
        return self

    def __exit__(self, *_args: object) -> Literal[False]:
        return False


def facade(
    value: MeetingErasureJob | None,
) -> tuple[AsyncErasureFacade, RequestService, RemediationService, QueryUnitOfWork]:
    requests = RequestService()
    remediations = RemediationService()
    queries = QueryUnitOfWork(value)
    return (
        AsyncErasureFacade(
            cast(MeetingErasureService, requests),
            cast(MeetingErasureRemediationService, remediations),
            cast(UnitOfWorkFactory, lambda: queries),
        ),
        requests,
        remediations,
        queries,
    )


async def test_erasure_facade_forwards_commands_and_offloads_queries() -> None:
    adapter, requests, remediations, queries = facade(erasure_job())
    main_thread = threading.get_ident()

    requested = await adapter.request(
        MEETING_ID,
        expected_version=7,
        request_key="erase-one",
        actor_id="portfolio-owner",
    )
    retried = await adapter.retry(
        ERASURE_JOB_ID,
        expected_version=3,
        request_key="retry-one",
        actor_id="portfolio-owner",
    )
    loaded = await adapter.get(ERASURE_JOB_ID)

    assert requested.replayed
    assert not retried.replayed
    assert loaded == erasure_job()
    assert requests.calls == [(MEETING_ID, 7, "erase-one", "portfolio-owner")]
    assert remediations.calls == [(ERASURE_JOB_ID, 3, "retry-one", "portfolio-owner")]
    assert queries.meeting_erasures.thread_id != main_thread


async def test_erasure_facade_hides_missing_jobs_behind_resource_error() -> None:
    adapter, _, _, _ = facade(None)

    with pytest.raises(ResourceNotFoundError, match="Meeting erasure job"):
        await adapter.get(ERASURE_JOB_ID)
