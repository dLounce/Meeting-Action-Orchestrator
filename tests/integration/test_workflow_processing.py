from __future__ import annotations

import io
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from threading import get_ident
from uuid import UUID

import pytest

from meeting_action_orchestrator.agents.contracts import (
    AgentResult,
    AgentRunContext,
    ExtractionRequest,
    MeetingExtraction,
)
from meeting_action_orchestrator.application.errors import (
    ProviderOutputError,
    ProviderTransientError,
)
from meeting_action_orchestrator.application.mapping import DeliveryTargets
from meeting_action_orchestrator.application.ports import (
    SpecialistProvider,
    TranscriptionProvider,
)
from meeting_action_orchestrator.application.processing import (
    ProcessingOutcome,
    ProcessingWorker,
)
from meeting_action_orchestrator.application.state_machine import transition_meeting
from meeting_action_orchestrator.application.workflow import (
    IngestMeeting,
    MeetingWorkflow,
)
from meeting_action_orchestrator.domain.enums import (
    FailureCode,
    FailureDisposition,
    MeetingStatus,
    ProcessingJobStatus,
    ProcessingStage,
)
from meeting_action_orchestrator.domain.models import (
    ConnectorTarget,
    ProcessingJob,
    WorkflowFailure,
)
from meeting_action_orchestrator.infrastructure.database import Database
from meeting_action_orchestrator.infrastructure.openai_transcription import TranscriptionOutput
from meeting_action_orchestrator.infrastructure.repositories import SqliteUnitOfWork
from tests.integration.test_workflow import (
    NOW,
    FakeRecordingStore,
    FakeSpecialists,
    FakeTranscriber,
)


@dataclass
class MutableClock:
    current: datetime = NOW

    def now(self) -> datetime:
        return self.current


class FixedRetryScheduler:
    def schedule(self, now: datetime, attempt_count: int) -> datetime:
        del attempt_count
        return now + timedelta(seconds=30)


class TransientTranscriber:
    async def transcribe(
        self,
        audio_path: Path,
        language: str | None = None,
    ) -> TranscriptionOutput:
        del audio_path, language
        raise ProviderTransientError


class ExpiringOnceTranscriber:
    def __init__(self, clock: MutableClock) -> None:
        self._clock = clock
        self._delegate = FakeTranscriber()
        self._calls = 0

    async def transcribe(
        self,
        audio_path: Path,
        language: str | None = None,
    ) -> TranscriptionOutput:
        self._calls += 1
        if self._calls == 1:
            self._clock.current += timedelta(seconds=11)
        return await self._delegate.transcribe(audio_path, language)


class FailingSpecialists(FakeSpecialists):
    def __init__(self, error: Exception) -> None:
        self._error = error

    async def extract(
        self,
        request: ExtractionRequest,
        context: AgentRunContext,
    ) -> AgentResult[MeetingExtraction]:
        del request, context
        raise self._error


def create_workflow(
    tmp_path: Path,
    clock: MutableClock,
    transcriber: TranscriptionProvider | None = None,
    specialists: SpecialistProvider | None = None,
    persistence_threads: set[int] | None = None,
) -> tuple[MeetingWorkflow, Database]:
    database = Database(tmp_path / "workflow.sqlite3")
    database.migrate()

    def unit_of_work() -> SqliteUnitOfWork:
        if persistence_threads is not None:
            persistence_threads.add(get_ident())
        return SqliteUnitOfWork(database)

    service = MeetingWorkflow(
        unit_of_work=unit_of_work,
        recording_store=FakeRecordingStore(tmp_path / "audio"),
        transcriber=transcriber or FakeTranscriber(),
        specialists=specialists or FakeSpecialists(),
        clock=clock,
        delivery_targets=DeliveryTargets(
            task=ConnectorTarget(connector_id="tasks", resource_id="inbox"),
            calendar=ConnectorTarget(connector_id="calendar", resource_id="primary"),
        ),
        max_agent_requests=5,
        max_agent_output_tokens=12_000,
    )
    return service, database


def ingest(service: MeetingWorkflow, ingest_key: str = "durable-upload") -> UUID:
    meeting = service.ingest(
        IngestMeeting(
            title="Raw meeting",
            occurred_at=NOW,
            timezone="Asia/Calcutta",
            original_name="meeting.wav",
            ingest_key=ingest_key,
        ),
        io.BytesIO(b"RIFF\x00\x00\x00\x00WAVEdata"),
    )
    return meeting.id


def create_worker(
    service: MeetingWorkflow,
    database: Database,
    clock: MutableClock,
    worker_id: str,
) -> ProcessingWorker:
    return ProcessingWorker(
        unit_of_work=lambda: SqliteUnitOfWork(database),
        handlers=service.processing_handlers(),
        clock=clock,
        retry_scheduler=FixedRetryScheduler(),
        worker_id=worker_id,
        lease_duration=timedelta(seconds=10),
    )


def lease_failure(at: datetime) -> WorkflowFailure:
    return WorkflowFailure(
        code=FailureCode.PROVIDER_TIMEOUT,
        disposition=FailureDisposition.RETRYABLE,
        safe_message="The processing lease expired",
        occurred_at=at,
    )


def claim_and_start(
    database: Database,
    meeting_id: UUID,
    stage: ProcessingStage,
    target: MeetingStatus,
    clock: MutableClock,
) -> ProcessingJob:
    with SqliteUnitOfWork(database) as uow:
        queued = uow.processing_jobs.find_for_stage(meeting_id, stage)
        assert queued is not None
        claimed = uow.processing_jobs.claim_due(
            stage,
            "crashed-worker",
            clock.now(),
            clock.now() + timedelta(seconds=10),
            1,
        )
        assert len(claimed) == 1
        meeting = uow.meetings.get(meeting_id)
        assert meeting is not None
        started = transition_meeting(meeting, target, clock.now())
        uow.meetings.save(started, meeting.version)
        uow.commit()
    return claimed[0]


def test_ingest_persists_transcription_job_with_meeting(tmp_path: Path) -> None:
    clock = MutableClock()
    service, database = create_workflow(tmp_path, clock)

    meeting_id = ingest(service)

    with SqliteUnitOfWork(database) as uow:
        job = uow.processing_jobs.find_for_stage(
            meeting_id,
            ProcessingStage.TRANSCRIPTION,
        )
        binding = uow.ingest_requests.get("durable-upload")
    assert job is not None
    assert binding is not None
    assert job.status is ProcessingJobStatus.READY
    assert job.attempt_count == 0


async def test_workers_run_transcription_then_extraction(tmp_path: Path) -> None:
    clock = MutableClock()
    service, database = create_workflow(tmp_path, clock)
    meeting_id = ingest(service)
    worker = create_worker(service, database, clock, "worker-a")

    transcription = await worker.run_once(ProcessingStage.TRANSCRIPTION)

    assert transcription[0].outcome is ProcessingOutcome.SUCCEEDED
    with SqliteUnitOfWork(database) as uow:
        meeting = uow.meetings.get(meeting_id)
        extraction_job = uow.processing_jobs.find_for_stage(
            meeting_id,
            ProcessingStage.EXTRACTION,
        )
    assert meeting is not None
    assert meeting.status is MeetingStatus.TRANSCRIBED
    assert extraction_job is not None
    assert extraction_job.status is ProcessingJobStatus.READY

    extraction = await worker.run_once(ProcessingStage.EXTRACTION)

    assert extraction[0].outcome is ProcessingOutcome.SUCCEEDED
    assert service.get_meeting(meeting_id).status is MeetingStatus.AWAITING_APPROVAL


async def test_workflow_transactions_run_outside_the_event_loop(tmp_path: Path) -> None:
    clock = MutableClock()
    persistence_threads: set[int] = set()
    service, database = create_workflow(
        tmp_path,
        clock,
        persistence_threads=persistence_threads,
    )
    meeting_id = ingest(service)
    worker = create_worker(service, database, clock, "worker-a")
    event_loop_thread = get_ident()
    persistence_threads.clear()

    await worker.run_once(ProcessingStage.TRANSCRIPTION)

    transcription_threads = set(persistence_threads)
    persistence_threads.clear()
    await worker.run_once(ProcessingStage.EXTRACTION)

    assert transcription_threads
    assert event_loop_thread not in transcription_threads
    assert persistence_threads
    assert event_loop_thread not in persistence_threads
    assert service.get_meeting(meeting_id).status is MeetingStatus.AWAITING_APPROVAL


async def test_reclaimed_transcription_resumes_processing_state(tmp_path: Path) -> None:
    clock = MutableClock()
    service, database = create_workflow(tmp_path, clock)
    meeting_id = ingest(service)
    first_attempt = claim_and_start(
        database,
        meeting_id,
        ProcessingStage.TRANSCRIPTION,
        MeetingStatus.TRANSCRIBING,
        clock,
    )
    clock.current += timedelta(seconds=10)
    worker = create_worker(service, database, clock, "worker-b")

    result = await worker.run_once(ProcessingStage.TRANSCRIPTION)

    assert first_attempt.attempt_count == 1
    assert result[0].outcome is ProcessingOutcome.SUCCEEDED
    assert result[0].job is not None
    assert result[0].job.attempt_count == 2
    assert service.get_meeting(meeting_id).status is MeetingStatus.TRANSCRIBED


async def test_reclaimed_extraction_resumes_processing_state(tmp_path: Path) -> None:
    clock = MutableClock()
    service, database = create_workflow(tmp_path, clock)
    meeting_id = ingest(service)
    initial_worker = create_worker(service, database, clock, "worker-a")
    await initial_worker.run_once(ProcessingStage.TRANSCRIPTION)
    first_attempt = claim_and_start(
        database,
        meeting_id,
        ProcessingStage.EXTRACTION,
        MeetingStatus.EXTRACTING,
        clock,
    )
    clock.current += timedelta(seconds=10)
    recovered_worker = create_worker(service, database, clock, "worker-b")

    result = await recovered_worker.run_once(ProcessingStage.EXTRACTION)

    assert first_attempt.attempt_count == 1
    assert result[0].outcome is ProcessingOutcome.SUCCEEDED
    assert result[0].job is not None
    assert result[0].job.attempt_count == 2
    assert service.get_meeting(meeting_id).status is MeetingStatus.AWAITING_APPROVAL


async def test_stage_handler_returns_classified_failure(tmp_path: Path) -> None:
    clock = MutableClock()
    service, database = create_workflow(tmp_path, clock, TransientTranscriber())
    meeting_id = ingest(service)
    worker = create_worker(service, database, clock, "worker-a")

    result = await worker.run_once(ProcessingStage.TRANSCRIPTION)

    assert result[0].outcome is ProcessingOutcome.RETRY_SCHEDULED
    assert result[0].job is not None
    assert result[0].job.last_failure is not None
    assert result[0].job.last_failure.disposition is FailureDisposition.RETRYABLE
    meeting = service.get_meeting(meeting_id)
    assert meeting.status is MeetingStatus.TRANSCRIPTION_FAILED
    assert meeting.failure is not None
    assert meeting.failure.disposition is FailureDisposition.RETRYABLE


@pytest.mark.parametrize(
    ("error", "expected_code"),
    [
        (ProviderTransientError(), FailureCode.PROVIDER_UNAVAILABLE),
        (ProviderOutputError(), FailureCode.INVALID_MODEL_OUTPUT),
    ],
)
async def test_extraction_failures_preserve_provider_category(
    tmp_path: Path,
    error: Exception,
    expected_code: FailureCode,
) -> None:
    clock = MutableClock()
    service, database = create_workflow(
        tmp_path,
        clock,
        specialists=FailingSpecialists(error),
    )
    meeting_id = ingest(service)
    worker = create_worker(service, database, clock, "worker-a")
    await worker.run_once(ProcessingStage.TRANSCRIPTION)

    result = await worker.run_once(ProcessingStage.EXTRACTION)

    assert result[0].outcome is ProcessingOutcome.RETRY_SCHEDULED
    assert result[0].job is not None
    assert result[0].job.last_failure is not None
    assert result[0].job.last_failure.code is expected_code
    meeting = service.get_meeting(meeting_id)
    assert meeting.failure is not None
    assert meeting.failure.code is expected_code


async def test_transcription_completion_fault_is_scheduled_for_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = MutableClock()
    service, database = create_workflow(tmp_path, clock)
    meeting_id = ingest(service)
    worker = create_worker(service, database, clock, "worker-a")

    def fail_completion(*_arguments: object) -> None:
        raise sqlite3.OperationalError("database is busy")

    monkeypatch.setattr(service, "_complete_transcription", fail_completion)

    result = await worker.run_once(ProcessingStage.TRANSCRIPTION)

    assert result[0].outcome is ProcessingOutcome.RETRY_SCHEDULED
    assert result[0].job is not None
    assert result[0].job.last_failure is not None
    assert result[0].job.last_failure.code is FailureCode.INTERNAL
    assert result[0].job.last_failure.disposition is FailureDisposition.RETRYABLE
    meeting = service.get_meeting(meeting_id)
    assert meeting.status is MeetingStatus.TRANSCRIPTION_FAILED
    assert meeting.failure == result[0].job.last_failure


async def test_extraction_completion_fault_is_scheduled_for_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = MutableClock()
    service, database = create_workflow(tmp_path, clock)
    meeting_id = ingest(service)
    worker = create_worker(service, database, clock, "worker-a")
    await worker.run_once(ProcessingStage.TRANSCRIPTION)

    def fail_completion(*_arguments: object) -> None:
        raise sqlite3.OperationalError("database is busy")

    monkeypatch.setattr(service, "_complete_extraction", fail_completion)

    result = await worker.run_once(ProcessingStage.EXTRACTION)

    assert result[0].outcome is ProcessingOutcome.RETRY_SCHEDULED
    assert result[0].job is not None
    assert result[0].job.last_failure is not None
    assert result[0].job.last_failure.code is FailureCode.INTERNAL
    assert result[0].job.last_failure.disposition is FailureDisposition.RETRYABLE
    meeting = service.get_meeting(meeting_id)
    assert meeting.status is MeetingStatus.EXTRACTION_FAILED
    assert meeting.failure == result[0].job.last_failure


async def test_expired_handler_cannot_commit_stage_output(tmp_path: Path) -> None:
    clock = MutableClock()
    transcriber = ExpiringOnceTranscriber(clock)
    service, database = create_workflow(tmp_path, clock, transcriber)
    meeting_id = ingest(service)
    expired_worker = create_worker(service, database, clock, "worker-a")

    expired = await expired_worker.run_once(ProcessingStage.TRANSCRIPTION)

    assert expired[0].outcome is ProcessingOutcome.LEASE_LOST
    with SqliteUnitOfWork(database) as uow:
        meeting = uow.meetings.get(meeting_id)
        transcript = uow.transcripts.latest_for_meeting(meeting_id)
        extraction = uow.processing_jobs.find_for_stage(
            meeting_id,
            ProcessingStage.EXTRACTION,
        )
    assert meeting is not None
    assert meeting.status is MeetingStatus.TRANSCRIBING
    assert transcript is None
    assert extraction is None

    recovered_worker = create_worker(service, database, clock, "worker-b")
    recovered = await recovered_worker.run_once(ProcessingStage.TRANSCRIPTION)

    assert recovered[0].outcome is ProcessingOutcome.SUCCEEDED
    assert service.get_meeting(meeting_id).status is MeetingStatus.TRANSCRIBED
