from __future__ import annotations

import io
import sqlite3
from dataclasses import dataclass, replace
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
    RecapDraft,
    RecapRequest,
    VerificationReport,
    VerificationRequest,
)
from meeting_action_orchestrator.application.errors import (
    AudioAssetIdentityMismatchError,
    ProviderBudgetExhaustedError,
    ProviderBudgetIntegrityError,
    ProviderError,
    ProviderInputError,
    ProviderOutputError,
    ProviderPermanentError,
    ProviderPermanentOutputError,
    ProviderRateLimitError,
    ProviderTimeoutError,
    ProviderTransientError,
    WorkflowBusyError,
)
from meeting_action_orchestrator.application.mapping import DeliveryTargets
from meeting_action_orchestrator.application.ports import (
    SpecialistProvider,
    TranscriptionProvider,
    TranscriptionRunContext,
)
from meeting_action_orchestrator.application.processing import (
    ProcessingOutcome,
    ProcessingWorker,
)
from meeting_action_orchestrator.application.processing_control import ProcessingControlService
from meeting_action_orchestrator.application.provider_policy import ProviderErrorMetadata
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
    ProviderBudgetDimension,
)
from meeting_action_orchestrator.domain.models import (
    ConnectorTarget,
    Meeting,
    ProcessingJob,
    Transcript,
    WorkflowFailure,
)
from meeting_action_orchestrator.domain.workflow_events import (
    ProcessingAttemptMetadata,
    ProcessingAuditOutcome,
    ProcessingRetryRequestedMetadata,
    SpecialistHandoffMetadata,
    SpecialistRole,
    WorkflowEvent,
    WorkflowEventType,
)
from meeting_action_orchestrator.infrastructure.database import Database
from meeting_action_orchestrator.infrastructure.openai_transcription import TranscriptionOutput
from meeting_action_orchestrator.infrastructure.repositories import SqliteUnitOfWork
from tests.integration.test_workflow import (
    ERASURE_TOKENS,
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


class AdvancingUnitOfWork(SqliteUnitOfWork):
    def __init__(self, database: Database, clock: MutableClock, advance: timedelta) -> None:
        super().__init__(database)
        self._clock = clock
        self._advance = advance

    def __enter__(self) -> SqliteUnitOfWork:
        entered = super().__enter__()
        self._clock.current += self._advance
        return entered


class FixedRetryScheduler:
    def schedule(self, now: datetime, attempt_count: int) -> datetime:
        del attempt_count
        return now + timedelta(seconds=30)


class TransientTranscriber:
    async def transcribe(
        self,
        audio_path: Path,
        language: str | None = None,
        *,
        context: TranscriptionRunContext,
    ) -> TranscriptionOutput:
        del audio_path, language, context
        raise ProviderTransientError


class PermanentTranscriber:
    async def transcribe(
        self,
        audio_path: Path,
        language: str | None = None,
        *,
        context: TranscriptionRunContext,
    ) -> TranscriptionOutput:
        del audio_path, language, context
        raise ProviderInputError


class IdentityMismatchTranscriber:
    async def transcribe(
        self,
        audio_path: Path,
        language: str | None = None,
        *,
        context: TranscriptionRunContext,
    ) -> TranscriptionOutput:
        del audio_path, language, context
        raise AudioAssetIdentityMismatchError


class ExpiringOnceTranscriber:
    def __init__(self, clock: MutableClock) -> None:
        self._clock = clock
        self._delegate = FakeTranscriber()
        self._calls = 0

    async def transcribe(
        self,
        audio_path: Path,
        language: str | None = None,
        *,
        context: TranscriptionRunContext,
    ) -> TranscriptionOutput:
        self._calls += 1
        if self._calls == 1:
            self._clock.current += timedelta(seconds=11)
        return await self._delegate.transcribe(
            audio_path,
            language,
            context=context,
        )


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


class RecapFailingSpecialists(FakeSpecialists):
    async def write_recap(
        self,
        request: RecapRequest,
        context: AgentRunContext,
    ) -> AgentResult[RecapDraft]:
        del request, context
        raise ProviderTransientError


class TimedSpecialists(FakeSpecialists):
    def __init__(self, clock: MutableClock, *, expire_on_verify: bool = False) -> None:
        self._clock = clock
        self._expire_on_verify = expire_on_verify

    async def extract(
        self,
        request: ExtractionRequest,
        context: AgentRunContext,
    ) -> AgentResult[MeetingExtraction]:
        result = replace(
            await super().extract(request, context),
            workflow_request_ids=("req_private_raw_marker",),
        )
        self._clock.current += timedelta(seconds=1)
        return result

    async def write_recap(
        self,
        request: RecapRequest,
        context: AgentRunContext,
    ) -> AgentResult[RecapDraft]:
        result = replace(
            await super().write_recap(request, context),
            workflow_request_ids=("req_private_raw_marker",),
        )
        self._clock.current += timedelta(seconds=1)
        return result

    async def verify(
        self,
        request: VerificationRequest,
        context: AgentRunContext,
    ) -> AgentResult[VerificationReport]:
        result = replace(
            await super().verify(request, context),
            workflow_request_ids=("req_private_raw_marker",),
        )
        self._clock.current += timedelta(seconds=9 if self._expire_on_verify else 1)
        return result


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
        erasure_tokens=ERASURE_TOKENS,
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
            actor_id="test-actor",
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


def workflow_events(database: Database, meeting_id: UUID) -> tuple[WorkflowEvent, ...]:
    with SqliteUnitOfWork(database, immediate=False) as uow:
        return tuple(uow.workflow_events.list_page(meeting_id, cursor=None, limit=100))


def processing_metadata(
    database: Database,
    meeting_id: UUID,
    stage: ProcessingStage,
) -> tuple[ProcessingAttemptMetadata, ...]:
    return tuple(
        event.safe_metadata
        for event in workflow_events(database, meeting_id)
        if event.type is WorkflowEventType.PROCESSING_ATTEMPTED
        and isinstance(event.safe_metadata, ProcessingAttemptMetadata)
        and event.safe_metadata.stage is stage
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
        budget = uow.provider_budget_accounts.get(job.id) if job is not None else None
        binding = uow.ingest_requests.get("durable-upload")
    assert job is not None
    assert binding is not None
    assert job.status is ProcessingJobStatus.READY
    assert job.attempt_count == 0
    assert budget is not None
    assert budget.stage is ProcessingStage.TRANSCRIPTION
    assert budget.limits.provider_request_limit == 3
    assert budget.limits.audio_duration_ms_limit == 21_600_000


def test_stage_mutation_rechecks_lease_after_transaction_acquisition(tmp_path: Path) -> None:
    clock = MutableClock()
    service, database = create_workflow(tmp_path, clock)
    meeting_id = ingest(service)
    with SqliteUnitOfWork(database) as uow:
        claimed = uow.processing_jobs.claim_due(
            ProcessingStage.TRANSCRIPTION,
            "worker-a",
            clock.now(),
            clock.now() + timedelta(seconds=10),
            1,
        )[0]
        meeting = uow.meetings.get(meeting_id)
        uow.commit()
    assert meeting is not None
    service._unit_of_work = lambda: AdvancingUnitOfWork(
        database,
        clock,
        timedelta(seconds=10),
    )

    with pytest.raises(WorkflowBusyError):
        service._start_stage(meeting, MeetingStatus.TRANSCRIBING, job=claimed)

    with SqliteUnitOfWork(database, immediate=False) as uow:
        persisted = uow.meetings.get(meeting_id)
    assert persisted is not None
    assert persisted.status is MeetingStatus.INGESTED


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


async def test_stored_audio_identity_mismatch_is_terminal(tmp_path: Path) -> None:
    clock = MutableClock()
    service, database = create_workflow(tmp_path, clock, IdentityMismatchTranscriber())
    meeting_id = ingest(service)
    worker = create_worker(service, database, clock, "worker-a")

    result = await worker.run_once(ProcessingStage.TRANSCRIPTION)

    assert result[0].outcome is ProcessingOutcome.FAILED
    assert result[0].job is not None
    assert result[0].job.last_failure is not None
    assert result[0].job.last_failure.code is FailureCode.INTERNAL
    assert result[0].job.last_failure.disposition is FailureDisposition.PERMANENT
    assert result[0].job.last_failure.safe_message == (
        "The stored recording identity could not be verified"
    )
    meeting = service.get_meeting(meeting_id)
    assert meeting.failure == result[0].job.last_failure


@pytest.mark.parametrize(
    ("error", "expected_code", "expected_disposition", "expected_outcome"),
    [
        (
            ProviderTransientError(),
            FailureCode.PROVIDER_UNAVAILABLE,
            FailureDisposition.RETRYABLE,
            ProcessingOutcome.RETRY_SCHEDULED,
        ),
        (
            ProviderOutputError(),
            FailureCode.INVALID_MODEL_OUTPUT,
            FailureDisposition.RETRYABLE,
            ProcessingOutcome.RETRY_SCHEDULED,
        ),
        (
            ProviderInputError(),
            FailureCode.INVALID_INPUT,
            FailureDisposition.PERMANENT,
            ProcessingOutcome.FAILED,
        ),
        (
            ProviderPermanentError(),
            FailureCode.INTERNAL,
            FailureDisposition.PERMANENT,
            ProcessingOutcome.FAILED,
        ),
        (
            ProviderPermanentOutputError(),
            FailureCode.INVALID_MODEL_OUTPUT,
            FailureDisposition.PERMANENT,
            ProcessingOutcome.FAILED,
        ),
        (
            ProviderTimeoutError(),
            FailureCode.PROVIDER_TIMEOUT,
            FailureDisposition.RETRYABLE,
            ProcessingOutcome.RETRY_SCHEDULED,
        ),
        (
            ProviderRateLimitError(),
            FailureCode.RATE_LIMITED,
            FailureDisposition.RETRYABLE,
            ProcessingOutcome.RETRY_SCHEDULED,
        ),
        (
            ProviderError(),
            FailureCode.INTERNAL,
            FailureDisposition.PERMANENT,
            ProcessingOutcome.FAILED,
        ),
        (
            ProviderBudgetExhaustedError(ProviderBudgetDimension.PROVIDER_REQUESTS),
            FailureCode.PROVIDER_BUDGET_EXHAUSTED,
            FailureDisposition.PERMANENT,
            ProcessingOutcome.FAILED,
        ),
        (
            ProviderBudgetIntegrityError(),
            FailureCode.INTERNAL,
            FailureDisposition.PERMANENT,
            ProcessingOutcome.FAILED,
        ),
    ],
)
async def test_extraction_failures_preserve_provider_category(
    tmp_path: Path,
    error: Exception,
    expected_code: FailureCode,
    expected_disposition: FailureDisposition,
    expected_outcome: ProcessingOutcome,
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

    assert result[0].outcome is expected_outcome
    assert result[0].job is not None
    assert result[0].job.last_failure is not None
    assert result[0].job.last_failure.code is expected_code
    assert result[0].job.last_failure.disposition is expected_disposition
    meeting = service.get_meeting(meeting_id)
    assert meeting.failure is not None
    assert meeting.failure.code is expected_code
    assert meeting.failure.disposition is expected_disposition


async def test_provider_retry_hint_is_persisted_and_delays_retry(tmp_path: Path) -> None:
    clock = MutableClock()
    provider_failure = ProviderRateLimitError(
        metadata=ProviderErrorMetadata(
            http_status=429,
            request_id="req_rate_limited",
            retry_after_seconds=45,
        )
    )
    service, database = create_workflow(
        tmp_path,
        clock,
        specialists=FailingSpecialists(provider_failure),
    )
    meeting_id = ingest(service)
    worker = create_worker(service, database, clock, "worker-a")
    await worker.run_once(ProcessingStage.TRANSCRIPTION)

    result = await worker.run_once(ProcessingStage.EXTRACTION)

    assert result[0].outcome is ProcessingOutcome.RETRY_SCHEDULED
    assert result[0].job is not None
    assert result[0].job.next_attempt_at == clock.now() + timedelta(seconds=75)
    assert result[0].job.next_attempt_at > clock.now() + timedelta(seconds=45)
    assert result[0].job.last_failure is not None
    assert result[0].job.last_failure.retry_after_seconds == 45
    with SqliteUnitOfWork(database) as restarted:
        persisted_job = restarted.processing_jobs.get(result[0].job.id)
        persisted_meeting = restarted.meetings.get(meeting_id)
    assert persisted_job is not None
    assert persisted_job.last_failure == result[0].job.last_failure
    assert persisted_meeting is not None
    assert persisted_meeting.failure == result[0].job.last_failure


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

    audit = processing_metadata(database, meeting_id, ProcessingStage.TRANSCRIPTION)
    assert [(item.attempt_number, item.outcome) for item in audit] == [
        (1, ProcessingAuditOutcome.STARTED),
        (1, ProcessingAuditOutcome.RETRY_SCHEDULED),
        (2, ProcessingAuditOutcome.STARTED),
        (2, ProcessingAuditOutcome.SUCCEEDED),
    ]
    assert audit[1].failure_code is FailureCode.PROVIDER_TIMEOUT
    assert audit[1].retry_delay_ms == 0


async def test_committed_artifact_repairs_expired_attempt_without_phantom_reclaim(
    tmp_path: Path,
) -> None:
    clock = MutableClock()
    service, database = create_workflow(tmp_path, clock)
    meeting_id = ingest(service)
    worker = create_worker(service, database, clock, "worker-a")
    claimed = worker._claim(ProcessingStage.TRANSCRIPTION, 1)

    assert await service.execute_transcription_job(claimed[0]) is None
    clock.current += timedelta(seconds=10)

    recovered = await create_worker(
        service,
        database,
        clock,
        "worker-b",
    ).run_once(ProcessingStage.TRANSCRIPTION)

    with SqliteUnitOfWork(database, immediate=False) as uow:
        job = uow.processing_jobs.find_for_stage(
            meeting_id,
            ProcessingStage.TRANSCRIPTION,
        )
    assert recovered == ()
    assert job is not None
    assert job.status is ProcessingJobStatus.SUCCEEDED
    assert job.attempt_count == 1
    assert [
        item.outcome
        for item in processing_metadata(
            database,
            meeting_id,
            ProcessingStage.TRANSCRIPTION,
        )
    ] == [ProcessingAuditOutcome.STARTED, ProcessingAuditOutcome.SUCCEEDED]


async def test_persisted_permanent_failure_repairs_expired_job_without_retry(
    tmp_path: Path,
) -> None:
    clock = MutableClock()
    service, database = create_workflow(tmp_path, clock, PermanentTranscriber())
    meeting_id = ingest(service)
    worker = create_worker(service, database, clock, "worker-a")
    claimed = worker._claim(ProcessingStage.TRANSCRIPTION, 1)

    failure = await service.execute_transcription_job(claimed[0])
    assert failure is not None
    assert failure.code is FailureCode.INVALID_INPUT
    clock.current += timedelta(seconds=10)

    recovered = await create_worker(
        service,
        database,
        clock,
        "worker-b",
    ).run_once(ProcessingStage.TRANSCRIPTION)

    with SqliteUnitOfWork(database, immediate=False) as uow:
        job = uow.processing_jobs.find_for_stage(
            meeting_id,
            ProcessingStage.TRANSCRIPTION,
        )
    audit = processing_metadata(database, meeting_id, ProcessingStage.TRANSCRIPTION)
    assert recovered == ()
    assert job is not None
    assert job.status is ProcessingJobStatus.FAILED
    assert job.attempt_count == 1
    assert job.last_failure is not None
    assert job.last_failure.code is FailureCode.INVALID_INPUT
    assert [(item.outcome, item.failure_code) for item in audit] == [
        (ProcessingAuditOutcome.STARTED, None),
        (ProcessingAuditOutcome.FAILED, FailureCode.INVALID_INPUT),
    ]


async def test_manual_retry_epoch_closes_only_the_new_expired_attempt(
    tmp_path: Path,
) -> None:
    clock = MutableClock()
    service, database = create_workflow(tmp_path, clock, PermanentTranscriber())
    meeting_id = ingest(service)
    initial = create_worker(service, database, clock, "worker-a")
    result = await initial.run_once(ProcessingStage.TRANSCRIPTION)
    assert result[0].outcome is ProcessingOutcome.FAILED
    failed_meeting = service.get_meeting(meeting_id)
    control = ProcessingControlService(
        unit_of_work=lambda: SqliteUnitOfWork(database),
        clock=clock,
    )

    retried = await control.retry(
        meeting_id,
        expected_version=failed_meeting.version,
        request_key="manual-retry-one",
        actor_id="portfolio-owner",
    )
    current_worker = create_worker(service, database, clock, "worker-b")
    claimed = current_worker._claim(ProcessingStage.TRANSCRIPTION, 1)
    service._start_stage(
        retried.meeting,
        MeetingStatus.TRANSCRIBING,
        job=claimed[0],
    )
    clock.current += timedelta(seconds=10)

    await create_worker(
        service,
        database,
        clock,
        "worker-c",
    ).run_once(ProcessingStage.TRANSCRIPTION)

    events = workflow_events(database, meeting_id)
    retry_index = next(
        index
        for index, event in enumerate(events)
        if event.type is WorkflowEventType.PROCESSING_RETRY_REQUESTED
    )
    retry_event = events[retry_index]
    assert isinstance(retry_event.safe_metadata, ProcessingRetryRequestedMetadata)
    assert retry_event.actor_id == "portfolio-owner"
    audit = tuple(
        event.safe_metadata
        for event in events[retry_index + 1 :]
        if event.type is WorkflowEventType.PROCESSING_ATTEMPTED
        and isinstance(event.safe_metadata, ProcessingAttemptMetadata)
    )
    assert [(item.attempt_number, item.outcome) for item in audit[:3]] == [
        (1, ProcessingAuditOutcome.STARTED),
        (1, ProcessingAuditOutcome.RETRY_SCHEDULED),
        (2, ProcessingAuditOutcome.STARTED),
    ]
    assert audit[1].retry_delay_ms == 0


async def test_ambiguous_artifact_commit_finishes_as_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = MutableClock()
    service, database = create_workflow(tmp_path, clock)
    meeting_id = ingest(service)
    worker = create_worker(service, database, clock, "worker-a")
    complete = service._complete_transcription

    def commit_then_raise(
        target_meeting_id: UUID,
        transcript: Transcript,
        job: ProcessingJob,
    ) -> Meeting:
        complete(target_meeting_id, transcript, job)
        raise sqlite3.OperationalError("commit outcome unavailable")

    monkeypatch.setattr(service, "_complete_transcription", commit_then_raise)

    result = await worker.run_once(ProcessingStage.TRANSCRIPTION)

    assert result[0].outcome is ProcessingOutcome.SUCCEEDED
    assert service.get_meeting(meeting_id).status is MeetingStatus.TRANSCRIBED
    assert [
        item.outcome
        for item in processing_metadata(
            database,
            meeting_id,
            ProcessingStage.TRANSCRIPTION,
        )
    ] == [ProcessingAuditOutcome.STARTED, ProcessingAuditOutcome.SUCCEEDED]


async def test_persisted_failure_wins_when_failure_commit_reports_unknown(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = MutableClock()
    service, database = create_workflow(tmp_path, clock, PermanentTranscriber())
    meeting_id = ingest(service)
    worker = create_worker(service, database, clock, "worker-a")
    fail_stage = service._fail_stage

    def commit_then_raise(
        target_meeting_id: UUID,
        target: MeetingStatus,
        failure: WorkflowFailure,
        *,
        job: ProcessingJob,
    ) -> None:
        fail_stage(target_meeting_id, target, failure, job=job)
        raise sqlite3.OperationalError("commit outcome unavailable")

    monkeypatch.setattr(service, "_fail_stage", commit_then_raise)

    result = await worker.run_once(ProcessingStage.TRANSCRIPTION)

    assert result[0].outcome is ProcessingOutcome.FAILED
    assert result[0].job is not None
    assert result[0].job.last_failure is not None
    assert result[0].job.last_failure.code is FailureCode.INVALID_INPUT
    audit = processing_metadata(database, meeting_id, ProcessingStage.TRANSCRIPTION)
    assert audit[-1].outcome is ProcessingAuditOutcome.FAILED
    assert audit[-1].failure_code is FailureCode.INVALID_INPUT


async def test_partial_specialist_chain_emits_no_committed_handoff(
    tmp_path: Path,
) -> None:
    clock = MutableClock()
    service, database = create_workflow(
        tmp_path,
        clock,
        specialists=RecapFailingSpecialists(),
    )
    meeting_id = ingest(service)
    worker = create_worker(service, database, clock, "worker-a")
    await worker.run_once(ProcessingStage.TRANSCRIPTION)

    result = await worker.run_once(ProcessingStage.EXTRACTION)

    events = workflow_events(database, meeting_id)
    assert result[0].outcome is ProcessingOutcome.RETRY_SCHEDULED
    assert not any(event.type is WorkflowEventType.SPECIALIST_HANDOFF_COMPLETED for event in events)
    assert not any(event.type is WorkflowEventType.REVIEW_REVISED for event in events)


async def test_extraction_lease_loss_commits_no_handoff_or_review(
    tmp_path: Path,
) -> None:
    clock = MutableClock()
    specialists = TimedSpecialists(clock, expire_on_verify=True)
    service, database = create_workflow(
        tmp_path,
        clock,
        specialists=specialists,
    )
    meeting_id = ingest(service)
    worker = create_worker(service, database, clock, "worker-a")
    await worker.run_once(ProcessingStage.TRANSCRIPTION)

    result = await worker.run_once(ProcessingStage.EXTRACTION)

    events = workflow_events(database, meeting_id)
    assert result[0].outcome is ProcessingOutcome.LEASE_LOST
    assert not any(event.type is WorkflowEventType.SPECIALIST_HANDOFF_COMPLETED for event in events)
    assert not any(event.type is WorkflowEventType.REVIEW_REVISED for event in events)
    assert service.get_meeting(meeting_id).status is MeetingStatus.EXTRACTING


async def test_handoff_events_preserve_completion_order_and_identifier_privacy(
    tmp_path: Path,
) -> None:
    clock = MutableClock()
    specialists = TimedSpecialists(clock)
    service, database = create_workflow(
        tmp_path,
        clock,
        specialists=specialists,
    )
    meeting_id = ingest(service)
    worker = create_worker(service, database, clock, "worker-a")
    await worker.run_once(ProcessingStage.TRANSCRIPTION)

    result = await worker.run_once(ProcessingStage.EXTRACTION)

    events = tuple(
        event
        for event in workflow_events(database, meeting_id)
        if event.type is WorkflowEventType.SPECIALIST_HANDOFF_COMPLETED
    )
    metadata = tuple(event.safe_metadata for event in events)
    assert result[0].outcome is ProcessingOutcome.SUCCEEDED
    assert all(isinstance(item, SpecialistHandoffMetadata) for item in metadata)
    assert [
        item.specialist for item in metadata if isinstance(item, SpecialistHandoffMetadata)
    ] == [
        SpecialistRole.EXTRACT,
        SpecialistRole.RECAP,
        SpecialistRole.VERIFY,
    ]
    assert [event.occurred_at for event in events] == sorted(event.occurred_at for event in events)
    serialized = "\n".join(event.safe_metadata.model_dump_json() for event in events)
    assert "req_private_raw_marker" not in serialized
    assert "request_ids_digest" in serialized
    meeting = service.get_meeting(meeting_id)
    with SqliteUnitOfWork(database, immediate=False) as uow:
        asset = uow.audio_assets.get(meeting.audio_asset_id)
        transcript = uow.transcripts.latest_for_meeting(meeting_id)
        review = (
            uow.reviews.get(meeting.current_review_id)
            if meeting.current_review_id is not None
            else None
        )
    assert asset is not None
    assert transcript is not None
    assert review is not None
    transcription_success = processing_metadata(
        database,
        meeting_id,
        ProcessingStage.TRANSCRIPTION,
    )[-1]
    extraction_success = processing_metadata(
        database,
        meeting_id,
        ProcessingStage.EXTRACTION,
    )[-1]
    assert transcription_success.outcome is ProcessingAuditOutcome.SUCCEEDED
    assert transcription_success.input_digest == asset.sha256
    assert transcription_success.output_digest == transcript.sha256
    assert extraction_success.outcome is ProcessingAuditOutcome.SUCCEEDED
    assert extraction_success.input_digest == transcript.sha256
    assert extraction_success.output_digest == review.content_digest
