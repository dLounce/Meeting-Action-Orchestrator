from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import BinaryIO, Protocol, TypeVar
from uuid import UUID, uuid4

from meeting_action_orchestrator.agents.contracts import (
    AgentBudget,
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
    ResourceNotFoundError,
    ReviewDigestMismatchError,
    StaleWorkflowVersionError,
    WorkflowBusyError,
)
from meeting_action_orchestrator.application.mapping import (
    DeliveryTargets,
    TranscriptionOutputLike,
    build_canonical_record,
    build_extraction_request,
    map_review_package,
    map_transcription,
    render_recap,
    transcript_input,
)
from meeting_action_orchestrator.application.ports import UnitOfWork
from meeting_action_orchestrator.application.reviewing import (
    ActionEdit,
    IssueResolutionEdit,
)
from meeting_action_orchestrator.application.reviewing import (
    revise_action as apply_action_edit,
)
from meeting_action_orchestrator.application.reviewing import (
    revise_delivery as apply_delivery_edit,
)
from meeting_action_orchestrator.application.reviewing import (
    revise_issue as apply_issue_resolution,
)
from meeting_action_orchestrator.application.state_machine import transition_meeting
from meeting_action_orchestrator.domain.enums import (
    AudioMediaType,
    FailureCode,
    FailureDisposition,
    MeetingStatus,
    WriteKind,
)
from meeting_action_orchestrator.domain.errors import IdempotencyConflictError
from meeting_action_orchestrator.domain.models import (
    Approval,
    AudioAsset,
    Meeting,
    PersonRef,
    RecapArtifact,
    ReviewRevision,
    WorkflowFailure,
    WriteIntent,
)
from meeting_action_orchestrator.domain.services import (
    approve_review,
    create_recap_artifact,
    project_write_intents,
    validate_review_evidence,
)
from meeting_action_orchestrator.infrastructure.audio import StoredAudio
from meeting_action_orchestrator.infrastructure.openai_agents import (
    OpenAIAgentConfigurationError,
    OpenAIAgentOutputError,
    OpenAIAgentTransientError,
)
from meeting_action_orchestrator.infrastructure.openai_transcription import (
    OpenAITranscriptionConfigurationError,
    OpenAITranscriptionInputError,
    OpenAITranscriptionTransientError,
)


class Clock(Protocol):
    def now(self) -> datetime: ...


class RecordingStore(Protocol):
    def put(self, stream: BinaryIO, original_name: str) -> StoredAudio: ...

    def path(self, storage_key: str) -> Path: ...


class TranscriptionProvider(Protocol):
    async def transcribe(
        self,
        audio_path: Path,
        language: str | None = None,
    ) -> TranscriptionOutputLike: ...


class SpecialistProvider(Protocol):
    async def extract(
        self,
        request: ExtractionRequest,
        context: AgentRunContext,
    ) -> AgentResult[MeetingExtraction]: ...

    async def write_recap(
        self,
        request: RecapRequest,
        context: AgentRunContext,
    ) -> AgentResult[RecapDraft]: ...

    async def verify(
        self,
        request: VerificationRequest,
        context: AgentRunContext,
    ) -> AgentResult[VerificationReport]: ...


@dataclass(frozen=True, slots=True)
class IngestMeeting:
    title: str
    occurred_at: datetime
    timezone: str
    original_name: str
    ingest_key: str
    participants: tuple[PersonRef, ...] = ()


@dataclass(frozen=True, slots=True)
class ApprovalResult:
    approval: Approval
    recap: RecapArtifact
    intents: tuple[WriteIntent, ...]
    replayed: bool = False


@dataclass(frozen=True, slots=True)
class ReviewUpdateResult:
    meeting: Meeting
    review: ReviewRevision


class SystemClock:
    def now(self) -> datetime:
        return datetime.now(timezone.utc)


class MeetingWorkflow:
    def __init__(
        self,
        *,
        unit_of_work: Callable[[], UnitOfWork],
        recording_store: RecordingStore,
        transcriber: TranscriptionProvider,
        specialists: SpecialistProvider,
        clock: Clock,
        delivery_targets: DeliveryTargets,
        max_agent_requests: int,
        max_agent_output_tokens: int,
    ) -> None:
        self._unit_of_work = unit_of_work
        self._recording_store = recording_store
        self._transcriber = transcriber
        self._specialists = specialists
        self._clock = clock
        self._delivery_targets = delivery_targets
        self._max_agent_requests = max_agent_requests
        self._max_agent_output_tokens = max_agent_output_tokens

    def ingest(self, command: IngestMeeting, stream: BinaryIO) -> Meeting:
        if command.occurred_at.tzinfo is None or command.occurred_at.utcoffset() is None:
            raise ValueError("Meeting time must include a UTC offset")
        stored = self._recording_store.put(stream, command.original_name)
        now = self._clock.now()
        with self._unit_of_work() as uow:
            existing = uow.meetings.find_by_ingest_key(command.ingest_key)
            if existing is not None:
                asset = _required(uow.audio_assets.get(existing.audio_asset_id), "Audio asset")
                if asset.sha256 != stored.sha256:
                    raise IdempotencyConflictError(command.ingest_key)
                return existing
            asset = uow.audio_assets.find_by_sha256(stored.sha256)
            if asset is None:
                asset = AudioAsset(
                    id=uuid4(),
                    storage_key=stored.storage_key,
                    original_name=stored.original_name,
                    detected_media_type=AudioMediaType(stored.metadata.media_type),
                    size_bytes=stored.size_bytes,
                    duration_ms=stored.metadata.duration_ms,
                    sha256=stored.sha256,
                    created_at=now,
                )
                uow.audio_assets.add(asset)
            meeting = Meeting(
                id=uuid4(),
                ingest_key=command.ingest_key,
                title=command.title,
                audio_asset_id=asset.id,
                occurred_at=command.occurred_at,
                timezone=command.timezone,
                participants=command.participants,
                created_at=now,
                updated_at=now,
            )
            uow.meetings.add(meeting)
            uow.commit()
        return meeting

    async def process(self, meeting_id: UUID) -> Meeting:
        meeting = self.get_meeting(meeting_id)
        if meeting.status in {MeetingStatus.INGESTED, MeetingStatus.TRANSCRIPTION_FAILED}:
            meeting = await self._transcribe(meeting)
        if meeting.status in {MeetingStatus.TRANSCRIBED, MeetingStatus.EXTRACTION_FAILED}:
            meeting = await self._extract(meeting)
        if meeting.status in {MeetingStatus.TRANSCRIBING, MeetingStatus.EXTRACTING}:
            raise WorkflowBusyError
        return meeting

    def get_meeting(self, meeting_id: UUID) -> Meeting:
        with self._unit_of_work() as uow:
            meeting = uow.meetings.get(meeting_id)
        return _required(meeting, "Meeting")

    def approve(
        self,
        meeting_id: UUID,
        *,
        expected_digest: str,
        expected_version: int,
        request_key: str,
        actor_id: str,
    ) -> ApprovalResult:
        now = self._clock.now()
        with self._unit_of_work() as uow:
            replay = uow.approvals.find_by_request_key(request_key)
            if replay is not None:
                if replay.meeting_id != meeting_id or replay.review_digest != expected_digest:
                    raise IdempotencyConflictError(request_key)
                recap = _required(uow.recaps.for_approval(replay.id), "Recap")
                intents = tuple(uow.write_intents.list_for_approval(replay.id))
                return ApprovalResult(replay, recap, intents, replayed=True)
            meeting = _required(uow.meetings.get(meeting_id), "Meeting")
            if meeting.version != expected_version:
                raise StaleWorkflowVersionError
            review = _required(uow.reviews.latest_for_meeting(meeting_id), "Review")
            transcript = _required(uow.transcripts.get(review.transcript_id), "Transcript")
            if review.content_digest != expected_digest:
                raise ReviewDigestMismatchError
            approval = approve_review(
                approval_id=uuid4(),
                meeting=meeting,
                review=review,
                transcript=transcript,
                request_key=request_key,
                actor_id=actor_id,
                approved_at=now,
            )
            recap = create_recap_artifact(
                artifact_id=uuid4(),
                meeting=meeting,
                review=review,
                approval=approval,
                created_at=now,
            )
            intents = project_write_intents(
                meeting=meeting,
                review=review,
                approval=approval,
                created_at=now,
            )
            approved = transition_meeting(
                meeting,
                MeetingStatus.APPROVED,
                now,
                approved_review_id=review.id,
            )
            if intents:
                approved = transition_meeting(approved, MeetingStatus.FILING, now)
            else:
                approved = transition_meeting(approved, MeetingStatus.COMPLETED, now)
            uow.approvals.add(approval)
            uow.recaps.add(recap)
            uow.write_intents.add_many(intents)
            uow.meetings.save(approved, meeting.version)
            uow.commit()
        return ApprovalResult(approval, recap, intents)

    def revise_action(
        self,
        meeting_id: UUID,
        *,
        edit: ActionEdit,
        expected_digest: str,
        expected_version: int,
        actor_id: str,
    ) -> ReviewUpdateResult:
        now = self._clock.now()
        with self._unit_of_work() as uow:
            meeting = _required(uow.meetings.get(meeting_id), "Meeting")
            review = self._current_review(uow, meeting, expected_digest, expected_version)
            try:
                revised = apply_action_edit(
                    review=review,
                    edit=edit,
                    revision_id=uuid4(),
                    actor_id=actor_id,
                    created_at=now,
                )
            except KeyError as error:
                raise ResourceNotFoundError("Action item") from error
            validate_review_evidence(
                revised, _required(uow.transcripts.get(review.transcript_id), "Transcript")
            )
            updated = transition_meeting(
                meeting,
                MeetingStatus.AWAITING_APPROVAL,
                now,
                review_id=revised.id,
            )
            uow.reviews.add(revised)
            uow.meetings.save(updated, meeting.version)
            uow.commit()
        return ReviewUpdateResult(updated, revised)

    def revise_issue(
        self,
        meeting_id: UUID,
        *,
        edit: IssueResolutionEdit,
        expected_digest: str,
        expected_version: int,
        actor_id: str,
    ) -> ReviewUpdateResult:
        now = self._clock.now()
        with self._unit_of_work() as uow:
            meeting = _required(uow.meetings.get(meeting_id), "Meeting")
            review = self._current_review(uow, meeting, expected_digest, expected_version)
            try:
                revised = apply_issue_resolution(
                    review=review,
                    edit=edit,
                    revision_id=uuid4(),
                    actor_id=actor_id,
                    created_at=now,
                )
            except KeyError as error:
                raise ResourceNotFoundError("Review issue") from error
            updated = transition_meeting(
                meeting,
                MeetingStatus.AWAITING_APPROVAL,
                now,
                review_id=revised.id,
            )
            uow.reviews.add(revised)
            uow.meetings.save(updated, meeting.version)
            uow.commit()
        return ReviewUpdateResult(updated, revised)

    def revise_delivery(
        self,
        meeting_id: UUID,
        *,
        action_id: UUID,
        kind: WriteKind,
        enabled: bool,
        expected_digest: str,
        expected_version: int,
        actor_id: str,
    ) -> ReviewUpdateResult:
        now = self._clock.now()
        with self._unit_of_work() as uow:
            meeting = _required(uow.meetings.get(meeting_id), "Meeting")
            review = self._current_review(uow, meeting, expected_digest, expected_version)
            try:
                revised = apply_delivery_edit(
                    review=review,
                    action_id=action_id,
                    kind=kind,
                    enabled=enabled,
                    targets=self._delivery_targets,
                    revision_id=uuid4(),
                    actor_id=actor_id,
                    created_at=now,
                )
            except KeyError as error:
                raise ResourceNotFoundError("Action item") from error
            updated = transition_meeting(
                meeting,
                MeetingStatus.AWAITING_APPROVAL,
                now,
                review_id=revised.id,
            )
            uow.reviews.add(revised)
            uow.meetings.save(updated, meeting.version)
            uow.commit()
        return ReviewUpdateResult(updated, revised)

    async def _transcribe(self, meeting: Meeting) -> Meeting:
        meeting = self._start_stage(meeting, MeetingStatus.TRANSCRIBING)
        with self._unit_of_work() as uow:
            asset = _required(uow.audio_assets.get(meeting.audio_asset_id), "Audio asset")
        try:
            output = await self._transcriber.transcribe(
                self._recording_store.path(asset.storage_key)
            )
            transcript = map_transcription(meeting, output, self._clock.now())
        except Exception as error:
            self._fail_stage(
                meeting.id,
                MeetingStatus.TRANSCRIPTION_FAILED,
                _transcription_failure(error, self._clock.now()),
            )
            raise
        now = self._clock.now()
        with self._unit_of_work() as uow:
            current = _required(uow.meetings.get(meeting.id), "Meeting")
            completed = transition_meeting(
                current,
                MeetingStatus.TRANSCRIBED,
                now,
                transcript_id=transcript.id,
            )
            uow.transcripts.add(transcript)
            uow.meetings.save(completed, current.version)
            uow.commit()
        return completed

    async def _extract(self, meeting: Meeting) -> Meeting:
        meeting = self._start_stage(meeting, MeetingStatus.EXTRACTING)
        with self._unit_of_work() as uow:
            transcript = _required(uow.transcripts.latest_for_meeting(meeting.id), "Transcript")
            latest_review = uow.reviews.latest_for_meeting(meeting.id)
        revision_number = latest_review.revision_number + 1 if latest_review else 1
        budget = AgentBudget(self._max_agent_requests, self._max_agent_output_tokens)
        try:
            extraction = await self._specialists.extract(
                build_extraction_request(meeting, transcript),
                AgentRunContext(str(meeting.id), "extract", budget),
            )
            record = build_canonical_record(meeting, extraction.output)
            recap = await self._specialists.write_recap(
                RecapRequest(meeting_id=str(meeting.id), record=record),
                AgentRunContext(str(meeting.id), "recap", budget),
            )
            verification = await self._specialists.verify(
                VerificationRequest(
                    meeting_id=str(meeting.id),
                    transcript=transcript_input(transcript),
                    record=record,
                    recap=recap.output,
                ),
                AgentRunContext(str(meeting.id), "verify", budget),
            )
            package = map_review_package(
                meeting=meeting,
                transcript=transcript,
                extraction=extraction.output,
                recap_markdown=render_recap(meeting, extraction.output, record, recap.output),
                verification=verification.output,
                targets=self._delivery_targets,
                created_at=self._clock.now(),
                revision_number=revision_number,
            )
            validate_review_evidence(package.review, transcript)
        except Exception as error:
            self._fail_stage(
                meeting.id,
                MeetingStatus.EXTRACTION_FAILED,
                _extraction_failure(error, self._clock.now()),
            )
            raise
        now = self._clock.now()
        with self._unit_of_work() as uow:
            current = _required(uow.meetings.get(meeting.id), "Meeting")
            updated_payload = current.model_dump(mode="python") | {
                "participants": package.participants or current.participants,
                "title": extraction.output.suggested_title[:200],
            }
            updated = Meeting.model_validate(updated_payload)
            completed = transition_meeting(
                updated,
                MeetingStatus.AWAITING_APPROVAL,
                now,
                review_id=package.review.id,
            )
            uow.reviews.add(package.review)
            uow.meetings.save(completed, current.version)
            uow.commit()
        return completed

    def _start_stage(self, meeting: Meeting, target: MeetingStatus) -> Meeting:
        now = self._clock.now()
        with self._unit_of_work() as uow:
            current = _required(uow.meetings.get(meeting.id), "Meeting")
            started = transition_meeting(current, target, now)
            uow.meetings.save(started, current.version)
            uow.commit()
        return started

    def _current_review(
        self,
        uow: UnitOfWork,
        meeting: Meeting,
        expected_digest: str,
        expected_version: int,
    ) -> ReviewRevision:
        if meeting.status is not MeetingStatus.AWAITING_APPROVAL:
            raise WorkflowBusyError
        if meeting.version != expected_version:
            raise StaleWorkflowVersionError
        review = _required(uow.reviews.latest_for_meeting(meeting.id), "Review")
        if meeting.current_review_id != review.id or review.content_digest != expected_digest:
            raise ReviewDigestMismatchError
        return review

    def _fail_stage(
        self,
        meeting_id: UUID,
        target: MeetingStatus,
        failure: WorkflowFailure,
    ) -> None:
        now = self._clock.now()
        with self._unit_of_work() as uow:
            current = _required(uow.meetings.get(meeting_id), "Meeting")
            failed = transition_meeting(current, target, now, failure=failure)
            uow.meetings.save(failed, current.version)
            uow.commit()


ModelT = TypeVar("ModelT")


def _required(value: ModelT | None, resource: str) -> ModelT:
    if value is None:
        raise ResourceNotFoundError(resource)
    return value


def _request_id(error: Exception) -> str | None:
    value = getattr(error, "request_id", None)
    return value if isinstance(value, str) and value else None


def _transcription_failure(error: Exception, occurred_at: datetime) -> WorkflowFailure:
    if isinstance(error, OpenAITranscriptionConfigurationError):
        return _failure(
            error,
            FailureCode.PROVIDER_AUTH,
            FailureDisposition.PERMANENT,
            "The transcription provider is not configured",
            occurred_at,
        )
    if isinstance(error, OpenAITranscriptionInputError):
        return _failure(
            error,
            FailureCode.INVALID_INPUT,
            FailureDisposition.PERMANENT,
            "The recording was rejected by the transcription provider",
            occurred_at,
        )
    if isinstance(error, OpenAITranscriptionTransientError):
        return _failure(
            error,
            FailureCode.PROVIDER_UNAVAILABLE,
            FailureDisposition.RETRYABLE,
            "The transcription provider is temporarily unavailable",
            occurred_at,
        )
    return _failure(
        error,
        FailureCode.INTERNAL,
        FailureDisposition.PERMANENT,
        "Transcription could not be completed",
        occurred_at,
    )


def _extraction_failure(error: Exception, occurred_at: datetime) -> WorkflowFailure:
    if isinstance(error, OpenAIAgentConfigurationError):
        return _failure(
            error,
            FailureCode.PROVIDER_AUTH,
            FailureDisposition.PERMANENT,
            "The analysis provider is not configured",
            occurred_at,
        )
    if isinstance(error, (OpenAIAgentTransientError, OpenAIAgentOutputError)):
        return _failure(
            error,
            FailureCode.INVALID_MODEL_OUTPUT,
            FailureDisposition.RETRYABLE,
            "Meeting analysis could not be completed",
            occurred_at,
        )
    return _failure(
        error,
        FailureCode.INTERNAL,
        FailureDisposition.PERMANENT,
        "Meeting analysis could not be completed",
        occurred_at,
    )


def _failure(
    error: Exception,
    code: FailureCode,
    disposition: FailureDisposition,
    message: str,
    occurred_at: datetime,
) -> WorkflowFailure:
    return WorkflowFailure(
        code=code,
        disposition=disposition,
        safe_message=message,
        provider_request_id=_request_id(error),
        occurred_at=occurred_at,
    )
