from __future__ import annotations

import hashlib
import io
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timezone
from pathlib import Path
from threading import Barrier, Lock
from typing import BinaryIO, TypeVar
from uuid import UUID

import pytest

from meeting_action_orchestrator.agents.contracts import (
    ActionItemCandidate,
    AgentResult,
    AgentRunContext,
    AgentUsage,
    DecisionCandidate,
    EvidenceRef,
    ExtractionRequest,
    MeetingExtraction,
    ParticipantCandidate,
    RecapDraft,
    RecapRequest,
    StrictModel,
    VerificationReport,
    VerificationRequest,
)
from meeting_action_orchestrator.application.errors import StaleWorkflowVersionError
from meeting_action_orchestrator.application.mapping import DeliveryTargets
from meeting_action_orchestrator.application.reviewing import ActionEdit
from meeting_action_orchestrator.application.workflow import IngestMeeting, MeetingWorkflow
from meeting_action_orchestrator.domain.enums import MeetingStatus, ReviewOrigin, WriteKind
from meeting_action_orchestrator.domain.errors import IdempotencyConflictError
from meeting_action_orchestrator.domain.models import ConnectorTarget, PersonRef
from meeting_action_orchestrator.infrastructure.audio import AudioMetadata, StoredAudio
from meeting_action_orchestrator.infrastructure.database import Database
from meeting_action_orchestrator.infrastructure.openai_transcription import (
    TranscriptionOutput,
    TranscriptionSegment,
    TranscriptionUsage,
)
from meeting_action_orchestrator.infrastructure.repositories import SqliteUnitOfWork

NOW = datetime(2026, 8, 7, 8, 0, tzinfo=timezone.utc)
OutputT = TypeVar("OutputT", bound=StrictModel)


class FrozenClock:
    def now(self) -> datetime:
        return NOW


class FakeRecordingStore:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.published_keys: list[str] = []
        self._lock = Lock()

    def put(self, stream: BinaryIO, original_name: str) -> StoredAudio:
        content = stream.read()
        digest = hashlib.sha256(content).hexdigest()
        with self._lock:
            storage_key = f"recording-{len(self.published_keys) + 1}.wav"
            self.published_keys.append(storage_key)
        path = self.root / storage_key
        self.root.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
        return StoredAudio(
            storage_key=path.name,
            original_name=original_name,
            path=path,
            size_bytes=len(content),
            sha256=digest,
            metadata=AudioMetadata("audio/wav", 4000, "pcm_s16le", 16000, 1),
        )

    def path(self, storage_key: str) -> Path:
        return self.root / storage_key

    def delete(self, storage_key: str) -> None:
        self.path(storage_key).unlink(missing_ok=True)


class BarrierRecordingStore(FakeRecordingStore):
    def __init__(self, root: Path, barrier: Barrier) -> None:
        super().__init__(root)
        self._barrier = barrier

    def put(self, stream: BinaryIO, original_name: str) -> StoredAudio:
        stored = super().put(stream, original_name)
        self._barrier.wait(timeout=5)
        return stored


class CommitThenRaiseUnitOfWork(SqliteUnitOfWork):
    def commit(self) -> None:
        super().commit()
        raise RuntimeError("commit outcome unavailable")


class FailBeforeCommitUnitOfWork(SqliteUnitOfWork):
    def commit(self) -> None:
        raise RuntimeError("commit did not persist")


class FakeTranscriber:
    async def transcribe(
        self,
        audio_path: Path,
        language: str | None = None,
    ) -> TranscriptionOutput:
        assert audio_path.suffix == ".wav"
        assert language is None
        text = "Mira approved the release plan. Dev will publish the brief by 2026-08-14."
        return TranscriptionOutput(
            model="transcribe-test",
            provider_request_id="transcription-request",
            language="en",
            text=text,
            duration_seconds=4.0,
            segments=(
                TranscriptionSegment(
                    id="segment-1",
                    start_ms=0,
                    end_ms=4000,
                    speaker="Mira",
                    text=text,
                ),
            ),
            usage=TranscriptionUsage(seconds=4.0),
        )


class FakeSpecialists:
    async def extract(
        self,
        request: ExtractionRequest,
        context: AgentRunContext,
    ) -> AgentResult[MeetingExtraction]:
        assert context.stage == "extract"
        segment_id = request.transcript.segments[0].id
        output = MeetingExtraction(
            suggested_title="Release planning",
            purpose="Confirm the release plan",
            participants=[],
            decisions=[
                DecisionCandidate(
                    statement="Approve the release plan",
                    owner="Mira",
                    rationale=None,
                    confidence="high",
                    evidence=[
                        EvidenceRef(
                            segment_id=segment_id,
                            quote="approved the release plan",
                        )
                    ],
                )
            ],
            action_items=[
                ActionItemCandidate(
                    description="Publish the brief",
                    owner="Dev",
                    due_expression="2026-08-14",
                    dependency=None,
                    confidence="high",
                    requires_clarification=False,
                    evidence=[
                        EvidenceRef(
                            segment_id=segment_id,
                            quote="publish the brief by 2026-08-14",
                        )
                    ],
                )
            ],
            open_questions=[],
            risks=[],
            warnings=[],
        )
        return result(output)

    async def write_recap(
        self,
        request: RecapRequest,
        context: AgentRunContext,
    ) -> AgentResult[RecapDraft]:
        assert context.stage == "recap"
        assert request.record.title == "Release planning"
        return result(
            RecapDraft(
                title="Release planning",
                overview="The release plan was approved.",
                highlights=[],
            )
        )

    async def verify(
        self,
        request: VerificationRequest,
        context: AgentRunContext,
    ) -> AgentResult[VerificationReport]:
        assert context.stage == "verify"
        assert request.record.items
        return result(VerificationReport(verdict="pass", findings=[]))


def result(output: OutputT) -> AgentResult[OutputT]:
    return AgentResult(
        output=output,
        usage=AgentUsage(requests=1, input_tokens=10, output_tokens=10, total_tokens=20),
        model="test",
        provider_request_ids=("request",),
    )


def workflow(
    tmp_path: Path,
    *,
    recording_store: FakeRecordingStore | None = None,
    specialists: FakeSpecialists | None = None,
    unit_of_work_type: type[SqliteUnitOfWork] = SqliteUnitOfWork,
) -> tuple[MeetingWorkflow, Database]:
    database = Database(tmp_path / "workflow.sqlite3")
    database.migrate()
    service = MeetingWorkflow(
        unit_of_work=lambda: unit_of_work_type(database),
        recording_store=recording_store or FakeRecordingStore(tmp_path / "audio"),
        transcriber=FakeTranscriber(),
        specialists=specialists or FakeSpecialists(),
        clock=FrozenClock(),
        delivery_targets=DeliveryTargets(
            task=ConnectorTarget(connector_id="tasks", resource_id="inbox"),
            calendar=ConnectorTarget(connector_id="calendar", resource_id="primary"),
        ),
        max_agent_requests=5,
        max_agent_output_tokens=12_000,
    )
    return service, database


def test_conflicting_ingest_removes_the_unreferenced_recording(tmp_path: Path) -> None:
    service, _ = workflow(tmp_path)
    command = IngestMeeting(
        title="Raw meeting",
        occurred_at=NOW,
        timezone="Asia/Calcutta",
        original_name="meeting.wav",
        ingest_key="upload-one",
    )
    service.ingest(command, io.BytesIO(b"RIFF\x00\x00\x00\x00WAVEfirst"))

    with pytest.raises(IdempotencyConflictError):
        service.ingest(command, io.BytesIO(b"RIFF\x00\x00\x00\x00WAVEsecond"))

    recordings = tuple((tmp_path / "audio").glob("*.wav"))
    assert len(recordings) == 1


def test_duplicate_content_keeps_the_shared_recording(tmp_path: Path) -> None:
    service, _ = workflow(tmp_path)
    content = b"RIFF\x00\x00\x00\x00WAVEshared"
    first = IngestMeeting(
        title="First meeting",
        occurred_at=NOW,
        timezone="UTC",
        original_name="first.wav",
        ingest_key="upload-one",
    )
    second = IngestMeeting(
        title="Second meeting",
        occurred_at=NOW,
        timezone="UTC",
        original_name="second.wav",
        ingest_key="upload-two",
    )

    first_meeting = service.ingest(first, io.BytesIO(content))
    second_meeting = service.ingest(second, io.BytesIO(content))

    recordings = tuple((tmp_path / "audio").glob("*.wav"))
    assert first_meeting.audio_asset_id == second_meeting.audio_asset_id
    assert len(recordings) == 1


def test_failed_duplicate_ingest_only_removes_its_staged_recording(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recording_store = FakeRecordingStore(tmp_path / "audio")
    service, _ = workflow(tmp_path, recording_store=recording_store)
    content = b"RIFF\x00\x00\x00\x00WAVEshared"
    first = IngestMeeting(
        title="First meeting",
        occurred_at=NOW,
        timezone="UTC",
        original_name="private-first-name.wav",
        ingest_key="upload-one",
    )
    second = IngestMeeting(
        title="Second meeting",
        occurred_at=NOW,
        timezone="UTC",
        original_name="private-second-name.wav",
        ingest_key="upload-two",
    )
    retained = service.ingest(first, io.BytesIO(content))

    def fail_enqueue(*args: object, **kwargs: object) -> None:
        del args, kwargs
        raise RuntimeError("queue unavailable")

    monkeypatch.setattr(service._processing_scheduler, "enqueue_in", fail_enqueue)

    with pytest.raises(RuntimeError, match="queue unavailable"):
        service.ingest(second, io.BytesIO(content))

    assert recording_store.published_keys == ["recording-1.wav", "recording-2.wav"]
    assert recording_store.path("recording-1.wav").exists()
    assert not recording_store.path("recording-2.wav").exists()
    assert service.get_meeting(retained.id) == retained


def test_ambiguous_committed_ingest_retains_its_referenced_recording(tmp_path: Path) -> None:
    recording_store = FakeRecordingStore(tmp_path / "audio")
    service, database = workflow(
        tmp_path,
        recording_store=recording_store,
        unit_of_work_type=CommitThenRaiseUnitOfWork,
    )
    command = IngestMeeting(
        title="Persisted meeting",
        occurred_at=NOW,
        timezone="UTC",
        original_name="meeting.wav",
        ingest_key="upload-one",
    )

    with pytest.raises(RuntimeError, match="commit outcome unavailable"):
        service.ingest(command, io.BytesIO(b"RIFF\x00\x00\x00\x00WAVEpersisted"))

    with SqliteUnitOfWork(database, immediate=False) as uow:
        meeting = uow.meetings.find_by_ingest_key(command.ingest_key)
        assert meeting is not None
        asset = uow.audio_assets.get(meeting.audio_asset_id)
    assert asset is not None
    assert recording_store.path(asset.storage_key).exists()
    assert tuple(recording_store.root.glob("*.wav")) == (recording_store.path(asset.storage_key),)


def test_failed_precommit_ingest_removes_its_unique_recording(tmp_path: Path) -> None:
    recording_store = FakeRecordingStore(tmp_path / "audio")
    service, database = workflow(
        tmp_path,
        recording_store=recording_store,
        unit_of_work_type=FailBeforeCommitUnitOfWork,
    )
    command = IngestMeeting(
        title="Rolled back meeting",
        occurred_at=NOW,
        timezone="UTC",
        original_name="meeting.wav",
        ingest_key="upload-one",
    )

    with pytest.raises(RuntimeError, match="commit did not persist"):
        service.ingest(command, io.BytesIO(b"RIFF\x00\x00\x00\x00WAVErolled-back"))

    with SqliteUnitOfWork(database, immediate=False) as uow:
        assert uow.meetings.find_by_ingest_key(command.ingest_key) is None
        assert (
            uow.audio_assets.find_by_sha256(
                hashlib.sha256(b"RIFF\x00\x00\x00\x00WAVErolled-back").hexdigest()
            )
            is None
        )
    assert tuple(recording_store.root.glob("*.wav")) == ()


def test_concurrent_same_content_ingests_keep_only_the_winning_storage_key(
    tmp_path: Path,
) -> None:
    recording_store = BarrierRecordingStore(tmp_path / "audio", Barrier(2))
    service, database = workflow(tmp_path, recording_store=recording_store)
    content = b"RIFF\x00\x00\x00\x00WAVEconcurrent"
    commands = (
        IngestMeeting(
            title="First concurrent meeting",
            occurred_at=NOW,
            timezone="UTC",
            original_name="first.wav",
            ingest_key="concurrent-one",
        ),
        IngestMeeting(
            title="Second concurrent meeting",
            occurred_at=NOW,
            timezone="UTC",
            original_name="second.wav",
            ingest_key="concurrent-two",
        ),
    )

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(service.ingest, command, io.BytesIO(content)) for command in commands
        ]
        meetings = tuple(future.result(timeout=10) for future in futures)

    assert meetings[0].audio_asset_id == meetings[1].audio_asset_id
    with SqliteUnitOfWork(database, immediate=False) as uow:
        asset = uow.audio_assets.get(meetings[0].audio_asset_id)
    assert asset is not None
    assert len(recording_store.published_keys) == 2
    assert tuple(recording_store.root.glob("*.wav")) == (recording_store.path(asset.storage_key),)


def test_ingest_persists_a_neutral_audio_name(tmp_path: Path) -> None:
    service, database = workflow(tmp_path)
    meeting = service.ingest(
        IngestMeeting(
            title="Private planning",
            occurred_at=NOW,
            timezone="UTC",
            original_name="customer-acquisition-confidential.wav",
            ingest_key="upload-one",
        ),
        io.BytesIO(b"RIFF\x00\x00\x00\x00WAVEprivate"),
    )

    with SqliteUnitOfWork(database) as uow:
        asset = uow.audio_assets.get(meeting.audio_asset_id)

    assert asset is not None
    assert asset.original_name == "recording.wav"


class ConflictingMetadataSpecialists(FakeSpecialists):
    async def extract(
        self,
        request: ExtractionRequest,
        context: AgentRunContext,
    ) -> AgentResult[MeetingExtraction]:
        extracted = await super().extract(request, context)
        segment_id = request.transcript.segments[0].id
        participant = ParticipantCandidate(
            display_name="Model Supplied Person",
            speaker_labels=["Mira"],
            evidence=[EvidenceRef(segment_id=segment_id, quote="Mira")],
        )
        return result(
            extracted.output.model_copy(
                update={
                    "suggested_title": "Release planning",
                    "participants": [participant],
                }
            )
        )


@pytest.mark.asyncio
async def test_extraction_preserves_submitted_meeting_metadata(tmp_path: Path) -> None:
    submitted_participants = (
        PersonRef(display_name="Mira Patel", email="mira@example.com"),
        PersonRef(display_name="Dev Rao", email="dev@example.com"),
    )
    service, database = workflow(
        tmp_path,
        specialists=ConflictingMetadataSpecialists(),
    )
    ingested = service.ingest(
        IngestMeeting(
            title="Submitted customer title",
            occurred_at=NOW,
            timezone="Asia/Calcutta",
            original_name="meeting.wav",
            ingest_key="upload-1",
            participants=submitted_participants,
        ),
        io.BytesIO(b"RIFF\x00\x00\x00\x00WAVEdata"),
    )

    reviewed = await service.process(ingested.id)

    assert reviewed.title == "Submitted customer title"
    assert reviewed.participants == submitted_participants
    with SqliteUnitOfWork(database) as uow:
        review = uow.reviews.latest_for_meeting(reviewed.id)
    assert review is not None
    assert review.recap_markdown.startswith("# Release planning")


@pytest.mark.asyncio
async def test_workflow_creates_no_external_intents_before_approval(tmp_path: Path) -> None:
    service, database = workflow(tmp_path)
    ingested = service.ingest(
        IngestMeeting(
            title="Raw meeting",
            occurred_at=NOW,
            timezone="Asia/Calcutta",
            original_name="meeting.wav",
            ingest_key="upload-1",
        ),
        io.BytesIO(b"RIFF\x00\x00\x00\x00WAVEdata"),
    )

    reviewed = await service.process(ingested.id)

    assert reviewed.status is MeetingStatus.AWAITING_APPROVAL
    with SqliteUnitOfWork(database) as uow:
        assert uow.approvals.for_meeting(reviewed.id) is None
        review = uow.reviews.latest_for_meeting(reviewed.id)
        assert review is not None

    approved = service.approve(
        reviewed.id,
        expected_digest=review.content_digest,
        expected_version=reviewed.version,
        request_key="approval-1",
        actor_id="reviewer",
    )

    assert approved.replayed is False
    assert len(approved.intents) == 2
    assert service.get_meeting(reviewed.id).status is MeetingStatus.FILING


@pytest.mark.asyncio
async def test_approval_request_replay_returns_existing_projection(tmp_path: Path) -> None:
    service, database = workflow(tmp_path)
    ingested = service.ingest(
        IngestMeeting(
            title="Raw meeting",
            occurred_at=NOW,
            timezone="Asia/Calcutta",
            original_name="meeting.wav",
            ingest_key="upload-1",
        ),
        io.BytesIO(b"RIFF\x00\x00\x00\x00WAVEdata"),
    )
    reviewed = await service.process(ingested.id)
    digest = review_digest(database, reviewed.id)

    first = service.approve(
        reviewed.id,
        expected_digest=digest,
        expected_version=reviewed.version,
        request_key="approval-1",
        actor_id="reviewer",
    )
    replay = service.approve(
        reviewed.id,
        expected_digest=digest,
        expected_version=reviewed.version,
        request_key="approval-1",
        actor_id="reviewer",
    )

    assert replay.replayed is True
    assert replay.approval == first.approval
    assert replay.intents == first.intents


@pytest.mark.asyncio
async def test_human_edit_creates_new_revision_before_approval(tmp_path: Path) -> None:
    service, database = workflow(tmp_path)
    ingested = service.ingest(
        IngestMeeting(
            title="Raw meeting",
            occurred_at=NOW,
            timezone="Asia/Calcutta",
            original_name="meeting.wav",
            ingest_key="upload-1",
        ),
        io.BytesIO(b"RIFF\x00\x00\x00\x00WAVEdata"),
    )
    reviewed = await service.process(ingested.id)
    with SqliteUnitOfWork(database) as uow:
        original = uow.reviews.latest_for_meeting(reviewed.id)
    assert original is not None

    updated = service.revise_action(
        reviewed.id,
        edit=ActionEdit(
            action_id=original.action_items[0].id,
            title="Publish the final brief",
            owner="Dev",
            due_date=date(2026, 8, 15),
            due_time=None,
            timezone="Asia/Calcutta",
            notes="Publish after legal review.",
        ),
        expected_digest=original.content_digest,
        expected_version=reviewed.version,
        actor_id="reviewer",
    )

    assert updated.review.revision_number == 2
    assert updated.review.origin is ReviewOrigin.HUMAN
    assert updated.review.content_digest != original.content_digest
    assert updated.review.action_items[0].title == "Publish the final brief"
    assert updated.review.recap_markdown == original.recap_markdown
    assert updated.meeting.current_review_id == updated.review.id
    with pytest.raises(StaleWorkflowVersionError):
        service.approve(
            reviewed.id,
            expected_digest=updated.review.content_digest,
            expected_version=reviewed.version,
            request_key="approval-stale",
            actor_id="reviewer",
        )


@pytest.mark.asyncio
async def test_delivery_revision_changes_approved_intent_projection(tmp_path: Path) -> None:
    service, database = workflow(tmp_path)
    ingested = service.ingest(
        IngestMeeting(
            title="Raw meeting",
            occurred_at=NOW,
            timezone="Asia/Calcutta",
            original_name="meeting.wav",
            ingest_key="upload-1",
        ),
        io.BytesIO(b"RIFF\x00\x00\x00\x00WAVEdata"),
    )
    reviewed = await service.process(ingested.id)
    with SqliteUnitOfWork(database) as uow:
        original = uow.reviews.latest_for_meeting(reviewed.id)
    assert original is not None

    updated = service.revise_delivery(
        reviewed.id,
        action_id=original.action_items[0].id,
        kind=WriteKind.CALENDAR_EVENT,
        enabled=False,
        expected_digest=original.content_digest,
        expected_version=reviewed.version,
        actor_id="reviewer",
    )
    approved = service.approve(
        reviewed.id,
        expected_digest=updated.review.content_digest,
        expected_version=updated.meeting.version,
        request_key="approval-1",
        actor_id="reviewer",
    )

    assert len(approved.intents) == 1
    assert approved.intents[0].proposal.kind is WriteKind.TASK


def review_digest(database: Database, meeting_id: UUID) -> str:
    with SqliteUnitOfWork(database) as uow:
        review = uow.reviews.latest_for_meeting(meeting_id)
    assert review is not None
    return review.content_digest
