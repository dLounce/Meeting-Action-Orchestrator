from __future__ import annotations

import hashlib
import io
from datetime import date, datetime, timezone
from pathlib import Path
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
from meeting_action_orchestrator.domain.models import ConnectorTarget
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

    def put(self, stream: BinaryIO, original_name: str) -> StoredAudio:
        content = stream.read()
        digest = hashlib.sha256(content).hexdigest()
        path = self.root / f"{digest}.wav"
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


def workflow(tmp_path: Path) -> tuple[MeetingWorkflow, Database]:
    database = Database(tmp_path / "workflow.sqlite3")
    database.migrate()
    service = MeetingWorkflow(
        unit_of_work=lambda: SqliteUnitOfWork(database),
        recording_store=FakeRecordingStore(tmp_path / "audio"),
        transcriber=FakeTranscriber(),
        specialists=FakeSpecialists(),
        clock=FrozenClock(),
        delivery_targets=DeliveryTargets(
            task=ConnectorTarget(connector_id="tasks", resource_id="inbox"),
            calendar=ConnectorTarget(connector_id="calendar", resource_id="primary"),
        ),
        max_agent_requests=5,
        max_agent_output_tokens=12_000,
    )
    return service, database


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
