from __future__ import annotations

import asyncio
import hmac
import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from math import isfinite
from typing import BinaryIO, Protocol, TypeVar
from uuid import UUID, uuid4

from meeting_action_orchestrator.agents.contracts import (
    AgentBudget,
    AgentRunContext,
    RecapRequest,
    VerificationRequest,
)
from meeting_action_orchestrator.application.auditing import (
    append_delivery_transition,
    append_meeting_transition,
    append_review_revision,
    specialist_handoff_draft,
)
from meeting_action_orchestrator.application.errors import (
    AudioAssetIdentityMismatchError,
    OperationConflictError,
    ProviderBudgetExhaustedError,
    ProviderBudgetIntegrityError,
    ProviderConfigurationError,
    ProviderError,
    ProviderInputError,
    ProviderOutputError,
    ProviderPermanentError,
    ProviderPermanentOutputError,
    ProviderRateLimitError,
    ProviderTimeoutError,
    ProviderTransientError,
    ResourceNotFoundError,
    ReviewDigestMismatchError,
    StaleWorkflowVersionError,
    WorkflowBusyError,
)
from meeting_action_orchestrator.application.mapping import (
    DeliveryTargets,
    build_canonical_record,
    build_extraction_request,
    map_review_package,
    map_transcription,
    render_recap,
    transcript_input,
)
from meeting_action_orchestrator.application.ports import (
    ErasureTokenCodec,
    RecordingStore,
    SpecialistProvider,
    StoredAudio,
    TranscriptionProvider,
    TranscriptionRunContext,
    UnitOfWork,
)
from meeting_action_orchestrator.application.processing import (
    ProcessingHandler,
    ProcessingScheduler,
)
from meeting_action_orchestrator.application.provider_policy import sanitize_provider_identifier
from meeting_action_orchestrator.application.recording_cleanup import RecordingCleanupScheduler
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
    ProcessingJobStatus,
    ProcessingStage,
    RecordingCleanupReason,
    WriteKind,
)
from meeting_action_orchestrator.domain.errors import (
    IdempotencyConflictError,
    InvalidDomainValueError,
)
from meeting_action_orchestrator.domain.models import (
    Approval,
    AudioAsset,
    IngestAudioIdentity,
    IngestRequestBinding,
    IngestRequestIdentity,
    Meeting,
    PersonRef,
    ProcessingJob,
    RecapArtifact,
    ReviewRevision,
    Transcript,
    WorkflowFailure,
    WriteIntent,
)
from meeting_action_orchestrator.domain.provider_budget import ProviderDispatchContext
from meeting_action_orchestrator.domain.services import (
    approve_review,
    create_recap_artifact,
    project_write_intents,
    validate_review_evidence,
)
from meeting_action_orchestrator.domain.workflow_events import (
    DeliveryChangeKind,
    MeetingIngestedMetadata,
    ReviewApprovedMetadata,
    ReviewChangeKind,
    SpecialistRole,
    WorkflowEventDraft,
    WorkflowEventType,
)

_NEUTRAL_AUDIO_NAMES = {
    AudioMediaType.MP3: "recording.mp3",
    AudioMediaType.MP4: "recording.m4a",
    AudioMediaType.M4A: "recording.m4a",
    AudioMediaType.WAV: "recording.wav",
    AudioMediaType.X_WAV: "recording.wav",
}
logger = logging.getLogger(__name__)


class Clock(Protocol):
    def now(self) -> datetime: ...


@dataclass(frozen=True, slots=True)
class IngestMeeting:
    title: str
    occurred_at: datetime
    timezone: str
    original_name: str
    ingest_key: str
    actor_id: str
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
        erasure_tokens: ErasureTokenCodec,
        transcriber: TranscriptionProvider,
        specialists: SpecialistProvider,
        clock: Clock,
        delivery_targets: DeliveryTargets,
        max_agent_requests: int,
        max_agent_output_tokens: int,
        processing_scheduler: ProcessingScheduler | None = None,
        recording_cleanup_scheduler: RecordingCleanupScheduler | None = None,
    ) -> None:
        self._unit_of_work = unit_of_work
        self._recording_store = recording_store
        self._erasure_tokens = erasure_tokens
        self._transcriber = transcriber
        self._specialists = specialists
        self._clock = clock
        self._delivery_targets = delivery_targets
        self._max_agent_requests = max_agent_requests
        self._max_agent_output_tokens = max_agent_output_tokens
        self._processing_scheduler = processing_scheduler or ProcessingScheduler(
            unit_of_work=unit_of_work,
            clock=clock,
        )
        self._recording_cleanup_scheduler = (
            recording_cleanup_scheduler
            or RecordingCleanupScheduler(
                unit_of_work=unit_of_work,
                clock=clock,
            )
        )

    def ingest(self, command: IngestMeeting, stream: BinaryIO) -> Meeting:
        request = IngestRequestIdentity(
            ingest_key=command.ingest_key,
            title=command.title,
            occurred_at=command.occurred_at,
            timezone=command.timezone,
            participants=command.participants,
        )
        stored = self._recording_store.put(stream, command.original_name)
        try:
            audio = IngestAudioIdentity(
                sha256=stored.sha256,
                size_bytes=stored.size_bytes,
            )
            now = self._clock.now()
            with self._unit_of_work() as uow:
                tombstone = uow.meeting_erasure_tombstones.find_by_ingest_key_tokens(
                    self._erasure_tokens.ingest_key_tokens(request.ingest_key)
                )
                if tombstone is not None:
                    raise OperationConflictError(
                        "The ingest request conflicts with the current workflow state"
                    )
                existing = uow.meetings.find_by_ingest_key(request.ingest_key)
                if existing is not None:
                    binding = uow.ingest_requests.get(request.ingest_key)
                    if binding is None or not self._matches_ingest_request(
                        binding,
                        request,
                        audio,
                    ):
                        raise IdempotencyConflictError(request.ingest_key)
                    persisted_asset = _required(
                        uow.audio_assets.get(existing.audio_asset_id),
                        "Audio asset",
                    )
                    if (
                        persisted_asset.sha256 != audio.sha256
                        or persisted_asset.size_bytes != audio.size_bytes
                    ):
                        raise AudioAssetIdentityMismatchError
                    meeting = existing
                else:
                    asset = uow.audio_assets.find_by_sha256(stored.sha256)
                    if asset is not None and asset.size_bytes != stored.size_bytes:
                        raise AudioAssetIdentityMismatchError
                    if asset is None:
                        media_type = AudioMediaType(stored.metadata.media_type)
                        asset = AudioAsset(
                            id=uuid4(),
                            storage_key=stored.storage_key,
                            original_name=_NEUTRAL_AUDIO_NAMES[media_type],
                            detected_media_type=media_type,
                            size_bytes=stored.size_bytes,
                            duration_ms=stored.metadata.duration_ms,
                            sha256=stored.sha256,
                            created_at=now,
                        )
                        uow.audio_assets.add(asset)
                    meeting = Meeting(
                        id=uuid4(),
                        ingest_key=request.ingest_key,
                        title=request.title,
                        audio_asset_id=asset.id,
                        occurred_at=request.occurred_at,
                        timezone=request.timezone,
                        participants=request.participants,
                        created_at=now,
                        updated_at=now,
                    )
                    uow.meetings.add(meeting)
                    uow.ingest_requests.add(
                        IngestRequestBinding.create(
                            request=request,
                            audio=audio,
                            created_at=now,
                        )
                    )
                    self._processing_scheduler.enqueue_in(
                        uow,
                        meeting.id,
                        ProcessingStage.TRANSCRIPTION,
                        scheduled_at=now,
                    )
                    uow.workflow_events.append(
                        WorkflowEventDraft(
                            meeting_id=meeting.id,
                            type=WorkflowEventType.MEETING_INGESTED,
                            actor_id=command.actor_id,
                            safe_metadata=MeetingIngestedMetadata(
                                recording_digest=asset.sha256,
                                media_type=asset.detected_media_type,
                                size_bytes=asset.size_bytes,
                                duration_ms=asset.duration_ms,
                            ),
                            occurred_at=now,
                        )
                    )
                    uow.commit()
        except BaseException:
            self._schedule_abandoned_recording(stored)
            raise
        self._schedule_abandoned_recording(stored)
        return meeting

    @staticmethod
    def _matches_ingest_request(
        binding: IngestRequestBinding,
        request: IngestRequestIdentity,
        audio: IngestAudioIdentity,
    ) -> bool:
        try:
            expected = request.fingerprint(audio, binding.fingerprint_version)
        except InvalidDomainValueError:
            return False
        return hmac.compare_digest(binding.request_fingerprint, expected)

    def _schedule_abandoned_recording(self, stored: StoredAudio) -> None:
        try:
            self._recording_cleanup_scheduler.schedule_if_unreferenced(
                storage_key=stored.storage_key,
                expected_sha256=stored.sha256,
                expected_size_bytes=stored.size_bytes,
                reason=RecordingCleanupReason.ABANDONED_INGEST,
            )
        except Exception as error:
            logger.warning(
                "recording cleanup scheduling failed",
                extra={
                    "fields": {
                        "event": "recording_cleanup_schedule_failed",
                        "reason": RecordingCleanupReason.ABANDONED_INGEST.value,
                        "exception_type": type(error).__name__,
                    }
                },
            )

    def get_meeting(self, meeting_id: UUID) -> Meeting:
        with self._unit_of_work() as uow:
            meeting = uow.meetings.get(meeting_id)
        return _required(meeting, "Meeting")

    def processing_handlers(self) -> dict[ProcessingStage, ProcessingHandler]:
        return {
            ProcessingStage.TRANSCRIPTION: self.execute_transcription_job,
            ProcessingStage.EXTRACTION: self.execute_extraction_job,
        }

    async def execute_transcription_job(
        self,
        job: ProcessingJob,
    ) -> WorkflowFailure | None:
        invalid = await asyncio.to_thread(
            self._validate_job,
            job,
            ProcessingStage.TRANSCRIPTION,
        )
        if invalid is not None:
            return invalid
        meeting = await asyncio.to_thread(self.get_meeting, job.meeting_id)
        if meeting.current_transcript_id is not None:
            return None
        allowed = {
            MeetingStatus.INGESTED,
            MeetingStatus.TRANSCRIBING,
            MeetingStatus.TRANSCRIPTION_FAILED,
        }
        if meeting.status not in allowed:
            return _invalid_job_failure(self._clock.now())
        try:
            await self._transcribe(meeting, job)
        except Exception as error:
            failure = _transcription_failure(error, self._clock.now())
            await asyncio.to_thread(
                self._fail_stage,
                meeting.id,
                MeetingStatus.TRANSCRIPTION_FAILED,
                failure,
                job=job,
            )
            return failure
        return None

    async def execute_extraction_job(
        self,
        job: ProcessingJob,
    ) -> WorkflowFailure | None:
        invalid = await asyncio.to_thread(
            self._validate_job,
            job,
            ProcessingStage.EXTRACTION,
        )
        if invalid is not None:
            return invalid
        meeting = await asyncio.to_thread(self.get_meeting, job.meeting_id)
        if meeting.current_review_id is not None:
            return None
        allowed = {
            MeetingStatus.TRANSCRIBED,
            MeetingStatus.EXTRACTING,
            MeetingStatus.EXTRACTION_FAILED,
        }
        if meeting.status not in allowed:
            return _invalid_job_failure(self._clock.now())
        try:
            await self._extract(meeting, job)
        except Exception as error:
            failure = _extraction_failure(error, self._clock.now())
            await asyncio.to_thread(
                self._fail_stage,
                meeting.id,
                MeetingStatus.EXTRACTION_FAILED,
                failure,
                job=job,
            )
            return failure
        return None

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
                completed = transition_meeting(approved, MeetingStatus.FILING, now)
            else:
                completed = transition_meeting(approved, MeetingStatus.COMPLETED, now)
            uow.approvals.add(approval)
            uow.recaps.add(recap)
            uow.write_intents.add_many(intents)
            uow.meetings.save(completed, meeting.version)
            append_meeting_transition(
                uow.workflow_events,
                meeting,
                approved,
                now,
                actor_id=actor_id,
            )
            uow.workflow_events.append(
                WorkflowEventDraft(
                    meeting_id=meeting.id,
                    type=WorkflowEventType.REVIEW_APPROVED,
                    actor_id=actor_id,
                    safe_metadata=ReviewApprovedMetadata(
                        revision_number=review.revision_number,
                        review_digest=review.content_digest,
                        write_intent_count=len(intents),
                    ),
                    occurred_at=now,
                )
            )
            for intent in intents:
                append_delivery_transition(
                    uow.workflow_events,
                    None,
                    intent,
                    now,
                    change_kind=DeliveryChangeKind.CREATED,
                    actor_id=actor_id,
                )
            append_meeting_transition(
                uow.workflow_events,
                approved,
                completed,
                now,
                actor_id=actor_id,
            )
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
            append_review_revision(
                uow.workflow_events,
                revised,
                ReviewChangeKind.ACTION_EDITED,
                now,
                actor_id=actor_id,
            )
            append_meeting_transition(
                uow.workflow_events,
                meeting,
                updated,
                now,
                actor_id=actor_id,
            )
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
            append_review_revision(
                uow.workflow_events,
                revised,
                ReviewChangeKind.ISSUE_UPDATED,
                now,
                actor_id=actor_id,
            )
            append_meeting_transition(
                uow.workflow_events,
                meeting,
                updated,
                now,
                actor_id=actor_id,
            )
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
            append_review_revision(
                uow.workflow_events,
                revised,
                ReviewChangeKind.DELIVERY_UPDATED,
                now,
                actor_id=actor_id,
            )
            append_meeting_transition(
                uow.workflow_events,
                meeting,
                updated,
                now,
                actor_id=actor_id,
            )
            uow.commit()
        return ReviewUpdateResult(updated, revised)

    async def _transcribe(
        self,
        meeting: Meeting,
        job: ProcessingJob,
    ) -> Meeting:
        meeting = await asyncio.to_thread(
            self._start_stage,
            meeting,
            MeetingStatus.TRANSCRIBING,
            job=job,
        )
        asset = await asyncio.to_thread(self._load_audio_asset, meeting.audio_asset_id)
        dispatch = _provider_dispatch_context(job)
        try:
            output = await self._transcriber.transcribe(
                self._recording_store.path(asset.storage_key),
                context=TranscriptionRunContext(
                    dispatch=dispatch,
                    audio_duration_ms=asset.duration_ms,
                    audio_size_bytes=asset.size_bytes,
                    audio_sha256=asset.sha256,
                ),
            )
            transcript = map_transcription(meeting, output, self._clock.now())
        except Exception as error:
            await asyncio.to_thread(
                self._fail_stage,
                meeting.id,
                MeetingStatus.TRANSCRIPTION_FAILED,
                _transcription_failure(error, self._clock.now()),
                job=job,
            )
            raise
        return await asyncio.to_thread(
            self._complete_transcription,
            meeting.id,
            transcript,
            job,
        )

    async def _extract(
        self,
        meeting: Meeting,
        job: ProcessingJob,
    ) -> Meeting:
        meeting = await asyncio.to_thread(
            self._start_stage,
            meeting,
            MeetingStatus.EXTRACTING,
            job=job,
        )
        transcript, revision_number = await asyncio.to_thread(
            self._load_extraction_inputs,
            meeting.id,
        )
        budget = AgentBudget(self._max_agent_requests, self._max_agent_output_tokens)
        dispatch = _provider_dispatch_context(job)
        try:
            extraction_request = build_extraction_request(meeting, transcript)
            extraction = await self._specialists.extract(
                extraction_request,
                AgentRunContext(str(job.id), "extract", budget, dispatch),
            )
            extraction_handoff = await asyncio.to_thread(
                specialist_handoff_draft,
                meeting_id=meeting.id,
                specialist=SpecialistRole.EXTRACT,
                processing_attempt_number=job.attempt_count,
                request=extraction_request,
                result=extraction,
                occurred_at=self._clock.now(),
            )
            record = build_canonical_record(meeting, extraction.output)
            recap_request = RecapRequest(meeting_id=str(meeting.id), record=record)
            recap = await self._specialists.write_recap(
                recap_request,
                AgentRunContext(str(job.id), "recap", budget, dispatch),
            )
            recap_handoff = await asyncio.to_thread(
                specialist_handoff_draft,
                meeting_id=meeting.id,
                specialist=SpecialistRole.RECAP,
                processing_attempt_number=job.attempt_count,
                request=recap_request,
                result=recap,
                occurred_at=self._clock.now(),
            )
            verification_request = VerificationRequest(
                meeting_id=str(meeting.id),
                transcript=transcript_input(transcript),
                record=record,
                recap=recap.output,
            )
            verification = await self._specialists.verify(
                verification_request,
                AgentRunContext(str(job.id), "verify", budget, dispatch),
            )
            verification_handoff = await asyncio.to_thread(
                specialist_handoff_draft,
                meeting_id=meeting.id,
                specialist=SpecialistRole.VERIFY,
                processing_attempt_number=job.attempt_count,
                request=verification_request,
                result=verification,
                occurred_at=self._clock.now(),
            )
            handoffs = (
                extraction_handoff,
                recap_handoff,
                verification_handoff,
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
            await asyncio.to_thread(
                self._fail_stage,
                meeting.id,
                MeetingStatus.EXTRACTION_FAILED,
                _extraction_failure(error, self._clock.now()),
                job=job,
            )
            raise
        return await asyncio.to_thread(
            self._complete_extraction,
            meeting.id,
            package.review,
            job,
            handoffs,
        )

    def _load_audio_asset(self, audio_asset_id: UUID) -> AudioAsset:
        with self._unit_of_work() as uow:
            return _required(uow.audio_assets.get(audio_asset_id), "Audio asset")

    def _complete_transcription(
        self,
        meeting_id: UUID,
        transcript: Transcript,
        job: ProcessingJob,
    ) -> Meeting:
        with self._unit_of_work() as uow:
            now = self._clock.now()
            self._require_active_job(uow, job, ProcessingStage.TRANSCRIPTION, now)
            current = _required(uow.meetings.get(meeting_id), "Meeting")
            completed = transition_meeting(
                current,
                MeetingStatus.TRANSCRIBED,
                now,
                transcript_id=transcript.id,
            )
            uow.transcripts.add(transcript)
            uow.meetings.save(completed, current.version)
            self._processing_scheduler.enqueue_in(
                uow,
                meeting_id,
                ProcessingStage.EXTRACTION,
                scheduled_at=now,
            )
            append_meeting_transition(uow.workflow_events, current, completed, now)
            uow.commit()
        return completed

    def _load_extraction_inputs(self, meeting_id: UUID) -> tuple[Transcript, int]:
        with self._unit_of_work() as uow:
            transcript = _required(uow.transcripts.latest_for_meeting(meeting_id), "Transcript")
            latest_review = uow.reviews.latest_for_meeting(meeting_id)
        revision_number = latest_review.revision_number + 1 if latest_review else 1
        return transcript, revision_number

    def _complete_extraction(
        self,
        meeting_id: UUID,
        review: ReviewRevision,
        job: ProcessingJob,
        handoffs: tuple[WorkflowEventDraft, WorkflowEventDraft, WorkflowEventDraft],
    ) -> Meeting:
        with self._unit_of_work() as uow:
            now = self._clock.now()
            self._require_active_job(uow, job, ProcessingStage.EXTRACTION, now)
            current = _required(uow.meetings.get(meeting_id), "Meeting")
            completed = transition_meeting(
                current,
                MeetingStatus.AWAITING_APPROVAL,
                now,
                review_id=review.id,
            )
            uow.reviews.add(review)
            uow.meetings.save(completed, current.version)
            for handoff in handoffs:
                uow.workflow_events.append(handoff)
            append_review_revision(
                uow.workflow_events,
                review,
                ReviewChangeKind.MODEL_CREATED,
                now,
            )
            append_meeting_transition(uow.workflow_events, current, completed, now)
            uow.commit()
        return completed

    def _start_stage(
        self,
        meeting: Meeting,
        target: MeetingStatus,
        *,
        job: ProcessingJob,
    ) -> Meeting:
        with self._unit_of_work() as uow:
            now = self._clock.now()
            stage = _processing_stage_for(target)
            self._require_active_job(uow, job, stage, now)
            current = _required(uow.meetings.get(meeting.id), "Meeting")
            if current.status is target:
                return current
            started = transition_meeting(current, target, now)
            uow.meetings.save(started, current.version)
            append_meeting_transition(uow.workflow_events, current, started, now)
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
        *,
        job: ProcessingJob,
    ) -> None:
        with self._unit_of_work() as uow:
            now = self._clock.now()
            self._require_active_job(
                uow,
                job,
                _processing_stage_for(target),
                now,
            )
            current = _required(uow.meetings.get(meeting_id), "Meeting")
            if current.status is target:
                return
            not_started = {
                MeetingStatus.TRANSCRIPTION_FAILED: MeetingStatus.INGESTED,
                MeetingStatus.EXTRACTION_FAILED: MeetingStatus.TRANSCRIBED,
            }
            if current.status is not_started[target]:
                return
            failed = transition_meeting(current, target, now, failure=failure)
            uow.meetings.save(failed, current.version)
            append_meeting_transition(uow.workflow_events, current, failed, now)
            uow.commit()

    def _validate_job(
        self,
        job: ProcessingJob,
        stage: ProcessingStage,
    ) -> WorkflowFailure | None:
        try:
            with self._unit_of_work() as uow:
                now = self._clock.now()
                self._require_active_job(uow, job, stage, now)
        except WorkflowBusyError:
            return _invalid_job_failure(now)
        return None

    @staticmethod
    def _require_active_job(
        uow: UnitOfWork,
        job: ProcessingJob,
        stage: ProcessingStage,
        now: datetime,
    ) -> None:
        persisted = uow.processing_jobs.get(job.id)
        active = (
            job.stage is stage
            and job.status is ProcessingJobStatus.RUNNING
            and persisted is not None
            and persisted.meeting_id == job.meeting_id
            and persisted.stage is stage
            and persisted.status is ProcessingJobStatus.RUNNING
            and persisted.attempt_count == job.attempt_count
            and persisted.lease_owner == job.lease_owner
            and persisted.claim_token == job.claim_token
            and persisted.lease_expires_at is not None
            and persisted.lease_expires_at > now
        )
        if not active:
            raise WorkflowBusyError


ModelT = TypeVar("ModelT")


def _required(value: ModelT | None, resource: str) -> ModelT:
    if value is None:
        raise ResourceNotFoundError(resource)
    return value


def _provider_dispatch_context(job: ProcessingJob) -> ProviderDispatchContext:
    if job.lease_owner is None or job.claim_token is None:
        raise WorkflowBusyError
    return ProviderDispatchContext(
        processing_job_id=job.id,
        attempt_number=job.attempt_count,
        lease_owner=job.lease_owner,
        claim_token=job.claim_token,
    )


def _processing_stage_for(status: MeetingStatus) -> ProcessingStage:
    transcription_statuses = {
        MeetingStatus.TRANSCRIBING,
        MeetingStatus.TRANSCRIPTION_FAILED,
    }
    if status in transcription_statuses:
        return ProcessingStage.TRANSCRIPTION
    extraction_statuses = {
        MeetingStatus.EXTRACTING,
        MeetingStatus.EXTRACTION_FAILED,
    }
    if status in extraction_statuses:
        return ProcessingStage.EXTRACTION
    raise ValueError(f"{status.value} is not a processing stage status")


def _invalid_job_failure(occurred_at: datetime) -> WorkflowFailure:
    return WorkflowFailure(
        code=FailureCode.INVALID_INPUT,
        disposition=FailureDisposition.PERMANENT,
        safe_message="The processing job is not active for this stage",
        occurred_at=occurred_at,
    )


def _request_id(error: Exception) -> str | None:
    return sanitize_provider_identifier(getattr(error, "request_id", None))


def _transcription_failure(error: Exception, occurred_at: datetime) -> WorkflowFailure:
    if isinstance(error, AudioAssetIdentityMismatchError):
        return _failure(
            error,
            FailureCode.INTERNAL,
            FailureDisposition.PERMANENT,
            "The stored recording identity could not be verified",
            occurred_at,
        )
    if isinstance(error, ProviderBudgetExhaustedError):
        return _failure(
            error,
            FailureCode.PROVIDER_BUDGET_EXHAUSTED,
            FailureDisposition.PERMANENT,
            "The processing job provider budget is exhausted",
            occurred_at,
        )
    if isinstance(error, ProviderBudgetIntegrityError):
        return _failure(
            error,
            FailureCode.INTERNAL,
            FailureDisposition.PERMANENT,
            "Provider budget accounting could not be verified",
            occurred_at,
        )
    if isinstance(error, ProviderConfigurationError):
        return _failure(
            error,
            FailureCode.PROVIDER_AUTH,
            FailureDisposition.PERMANENT,
            "The transcription provider is not configured",
            occurred_at,
        )
    if isinstance(error, ProviderInputError):
        return _failure(
            error,
            FailureCode.INVALID_INPUT,
            FailureDisposition.PERMANENT,
            "The recording was rejected by the transcription provider",
            occurred_at,
        )
    if isinstance(error, ProviderTransientError):
        return _failure(
            error,
            _transient_provider_failure_code(error),
            FailureDisposition.RETRYABLE,
            "The transcription provider is temporarily unavailable",
            occurred_at,
        )
    if isinstance(error, ProviderOutputError):
        return _failure(
            error,
            FailureCode.INVALID_MODEL_OUTPUT,
            FailureDisposition.RETRYABLE,
            "The transcription provider returned invalid output",
            occurred_at,
        )
    if isinstance(error, ProviderPermanentOutputError):
        return _failure(
            error,
            FailureCode.INVALID_MODEL_OUTPUT,
            FailureDisposition.PERMANENT,
            "The transcription provider cannot complete this request",
            occurred_at,
        )
    if isinstance(error, ProviderPermanentError):
        return _failure(
            error,
            _permanent_provider_failure_code(error),
            FailureDisposition.PERMANENT,
            "The transcription provider cannot complete this request",
            occurred_at,
        )
    if isinstance(error, ProviderError):
        return _failure(
            error,
            FailureCode.INTERNAL,
            FailureDisposition.PERMANENT,
            "The transcription provider request failed",
            occurred_at,
        )
    return _failure(
        error,
        FailureCode.INTERNAL,
        FailureDisposition.RETRYABLE,
        "Transcription could not be completed",
        occurred_at,
    )


def _extraction_failure(error: Exception, occurred_at: datetime) -> WorkflowFailure:
    if isinstance(error, ProviderBudgetExhaustedError):
        return _failure(
            error,
            FailureCode.PROVIDER_BUDGET_EXHAUSTED,
            FailureDisposition.PERMANENT,
            "The processing job provider budget is exhausted",
            occurred_at,
        )
    if isinstance(error, ProviderBudgetIntegrityError):
        return _failure(
            error,
            FailureCode.INTERNAL,
            FailureDisposition.PERMANENT,
            "Provider budget accounting could not be verified",
            occurred_at,
        )
    if isinstance(error, ProviderConfigurationError):
        return _failure(
            error,
            FailureCode.PROVIDER_AUTH,
            FailureDisposition.PERMANENT,
            "The analysis provider is not configured",
            occurred_at,
        )
    if isinstance(error, ProviderInputError):
        return _failure(
            error,
            FailureCode.INVALID_INPUT,
            FailureDisposition.PERMANENT,
            "The analysis provider rejected the request",
            occurred_at,
        )
    if isinstance(error, ProviderTransientError):
        return _failure(
            error,
            _transient_provider_failure_code(error),
            FailureDisposition.RETRYABLE,
            "The analysis provider is temporarily unavailable",
            occurred_at,
        )
    if isinstance(error, ProviderOutputError):
        return _failure(
            error,
            FailureCode.INVALID_MODEL_OUTPUT,
            FailureDisposition.RETRYABLE,
            "Meeting analysis could not be completed",
            occurred_at,
        )
    if isinstance(error, ProviderPermanentOutputError):
        return _failure(
            error,
            FailureCode.INVALID_MODEL_OUTPUT,
            FailureDisposition.PERMANENT,
            "The analysis provider cannot complete this request",
            occurred_at,
        )
    if isinstance(error, ProviderPermanentError):
        return _failure(
            error,
            _permanent_provider_failure_code(error),
            FailureDisposition.PERMANENT,
            "The analysis provider cannot complete this request",
            occurred_at,
        )
    if isinstance(error, ProviderError):
        return _failure(
            error,
            FailureCode.INTERNAL,
            FailureDisposition.PERMANENT,
            "The analysis provider request failed",
            occurred_at,
        )
    return _failure(
        error,
        FailureCode.INTERNAL,
        FailureDisposition.RETRYABLE,
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
    retry_after_seconds = None
    value = getattr(error, "retry_after_seconds", None)
    if (
        disposition is FailureDisposition.RETRYABLE
        and not isinstance(value, bool)
        and isinstance(value, (int, float))
        and isfinite(value)
        and 0 <= value <= 600
    ):
        retry_after_seconds = float(value)
    return WorkflowFailure(
        code=code,
        disposition=disposition,
        safe_message=message,
        provider_request_id=_request_id(error),
        retry_after_seconds=retry_after_seconds,
        occurred_at=occurred_at,
    )


def _transient_provider_failure_code(error: Exception) -> FailureCode:
    status = getattr(error, "http_status", None)
    if isinstance(error, ProviderTimeoutError) or status == 408:
        return FailureCode.PROVIDER_TIMEOUT
    if isinstance(error, ProviderRateLimitError) or status == 429:
        return FailureCode.RATE_LIMITED
    return FailureCode.PROVIDER_UNAVAILABLE


def _permanent_provider_failure_code(error: Exception) -> FailureCode:
    status = getattr(error, "http_status", None)
    if status == 408:
        return FailureCode.PROVIDER_TIMEOUT
    if status == 429:
        return FailureCode.RATE_LIMITED
    if isinstance(status, int) and status >= 500:
        return FailureCode.PROVIDER_UNAVAILABLE
    return FailureCode.INTERNAL
