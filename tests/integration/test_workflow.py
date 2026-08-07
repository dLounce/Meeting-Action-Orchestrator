from __future__ import annotations

import hashlib
import io
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from threading import Barrier, Lock
from typing import BinaryIO, ClassVar, TypeVar
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
from meeting_action_orchestrator.application.errors import (
    AudioAssetIdentityMismatchError,
    StaleWorkflowVersionError,
)
from meeting_action_orchestrator.application.mapping import DeliveryTargets
from meeting_action_orchestrator.application.ports import TranscriptionRunContext
from meeting_action_orchestrator.application.processing import (
    FullJitterRetryScheduler,
    ProcessingWorker,
)
from meeting_action_orchestrator.application.recording_cleanup import RecordingCleanupScheduler
from meeting_action_orchestrator.application.reviewing import ActionEdit
from meeting_action_orchestrator.application.workflow import IngestMeeting, MeetingWorkflow
from meeting_action_orchestrator.domain.enums import (
    MeetingStatus,
    ProcessingStage,
    ProviderUsageKind,
    RecordingCleanupReason,
    ReviewOrigin,
    WriteKind,
)
from meeting_action_orchestrator.domain.errors import IdempotencyConflictError
from meeting_action_orchestrator.domain.models import (
    ConnectorTarget,
    IngestAudioIdentity,
    IngestRequestIdentity,
    Meeting,
    PersonRef,
    RecordingCleanupJob,
)
from meeting_action_orchestrator.domain.provider_budget import ProviderUsage
from meeting_action_orchestrator.infrastructure.audio import AudioMetadata, StoredAudio
from meeting_action_orchestrator.infrastructure.database import Database
from meeting_action_orchestrator.infrastructure.erasure_tokens import ErasureTokenKeyring
from meeting_action_orchestrator.infrastructure.openai_transcription import (
    TranscriptionOutput,
    TranscriptionSegment,
)
from meeting_action_orchestrator.infrastructure.repositories import SqliteUnitOfWork

NOW = datetime(2026, 8, 7, 8, 0, tzinfo=timezone.utc)
OutputT = TypeVar("OutputT", bound=StrictModel)
PARTICIPANTS = (
    PersonRef(display_name="Mira", email="Mira@example.com"),
    PersonRef(display_name="Dev", email=None),
)
ERASURE_TOKENS = ErasureTokenKeyring("current", {"current": b"e" * 32})


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
            storage_key = f"{len(self.published_keys) + 1:032x}.wav"
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
    failed_paths: ClassVar[set[Path]] = set()

    def commit(self) -> None:
        if self._database.path not in self.failed_paths:
            self.failed_paths.add(self._database.path)
            raise RuntimeError("commit did not persist")
        super().commit()


class FailingCleanupScheduler(RecordingCleanupScheduler):
    def schedule_if_unreferenced(
        self,
        *,
        storage_key: str,
        expected_sha256: str,
        expected_size_bytes: int,
        reason: RecordingCleanupReason,
    ) -> RecordingCleanupJob | None:
        del storage_key, expected_sha256, expected_size_bytes, reason
        raise RuntimeError("cleanup scheduling unavailable")


class FakeTranscriber:
    async def transcribe(
        self,
        audio_path: Path,
        language: str | None = None,
        *,
        context: TranscriptionRunContext,
    ) -> TranscriptionOutput:
        assert audio_path.suffix == ".wav"
        assert language is None
        assert context.audio_size_bytes > 0
        assert context.audio_duration_ms > 0
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
            usage=ProviderUsage(kind=ProviderUsageKind.DURATION, audio_duration_ms=4000),
        )


class FakeSpecialists:
    async def extract(
        self,
        request: ExtractionRequest,
        context: AgentRunContext,
    ) -> AgentResult[MeetingExtraction]:
        assert context.stage == "extract"
        assert context.dispatch is not None
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
        assert context.dispatch is not None
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
        assert context.dispatch is not None
        assert request.record.items
        return result(VerificationReport(verdict="pass", findings=[]))


def result(output: OutputT) -> AgentResult[OutputT]:
    return AgentResult(
        output=output,
        usage=AgentUsage(requests=1, input_tokens=10, output_tokens=10, total_tokens=20),
        model="test",
        workflow_request_ids=("request",),
    )


def workflow(
    tmp_path: Path,
    *,
    recording_store: FakeRecordingStore | None = None,
    erasure_tokens: ErasureTokenKeyring = ERASURE_TOKENS,
    specialists: FakeSpecialists | None = None,
    unit_of_work_type: type[SqliteUnitOfWork] = SqliteUnitOfWork,
    cleanup_scheduler_type: type[RecordingCleanupScheduler] = RecordingCleanupScheduler,
) -> tuple[MeetingWorkflow, Database]:
    database = Database(tmp_path / "workflow.sqlite3")
    database.migrate()
    service = MeetingWorkflow(
        unit_of_work=lambda: unit_of_work_type(database),
        recording_store=recording_store or FakeRecordingStore(tmp_path / "audio"),
        erasure_tokens=erasure_tokens,
        transcriber=FakeTranscriber(),
        specialists=specialists or FakeSpecialists(),
        clock=FrozenClock(),
        delivery_targets=DeliveryTargets(
            task=ConnectorTarget(connector_id="tasks", resource_id="inbox"),
            calendar=ConnectorTarget(connector_id="calendar", resource_id="primary"),
        ),
        max_agent_requests=5,
        max_agent_output_tokens=12_000,
        recording_cleanup_scheduler=cleanup_scheduler_type(
            unit_of_work=lambda: unit_of_work_type(database),
            clock=FrozenClock(),
        ),
    )
    return service, database


async def process_meeting(
    service: MeetingWorkflow,
    database: Database,
    meeting_id: UUID,
) -> Meeting:
    worker = ProcessingWorker(
        unit_of_work=lambda: SqliteUnitOfWork(database),
        handlers=service.processing_handlers(),
        clock=FrozenClock(),
        retry_scheduler=FullJitterRetryScheduler(random_value=lambda: 0.0),
        worker_id="test-worker",
    )
    await worker.run_once(ProcessingStage.TRANSCRIPTION)
    await worker.run_once(ProcessingStage.EXTRACTION)
    return service.get_meeting(meeting_id)


def test_exact_ingest_replay_uses_normalized_metadata_and_ignores_filename(
    tmp_path: Path,
) -> None:
    recording_store = FakeRecordingStore(tmp_path / "audio")
    service, database = workflow(tmp_path, recording_store=recording_store)
    content = b"RIFF\x00\x00\x00\x00WAVEexact"
    original = IngestMeeting(
        title=" Planning ",
        occurred_at=NOW,
        timezone="UTC",
        original_name="private-first-name.wav",
        ingest_key=" upload-one ",
        actor_id="test-actor",
        participants=(
            PersonRef(display_name=" Mira ", email=" Mira@example.com "),
            PersonRef(display_name=" Dev ", email=None),
        ),
    )
    replayed = IngestMeeting(
        title="Planning",
        occurred_at=NOW.astimezone(timezone(timedelta(hours=5, minutes=30))),
        timezone="UTC",
        original_name="unrelated-second-name.wav",
        ingest_key="upload-one",
        actor_id="test-actor",
        participants=PARTICIPANTS,
    )

    first = service.ingest(original, io.BytesIO(content))
    second = service.ingest(replayed, io.BytesIO(content))

    assert second == first
    assert first.ingest_key == "upload-one"
    assert first.title == "Planning"
    with SqliteUnitOfWork(database, immediate=False) as uow:
        binding = uow.ingest_requests.get("upload-one")
        cleanup = uow.recording_cleanups.find_by_storage_key("00000000000000000000000000000002.wav")
    assert binding is not None
    assert binding.fingerprint_version == 1
    assert cleanup is not None
    assert cleanup.expected_sha256 == hashlib.sha256(content).hexdigest()


@pytest.mark.parametrize(
    ("title", "occurred_at", "timezone_name", "participants"),
    [
        ("Retrospective", NOW, "UTC", PARTICIPANTS),
        ("Planning", NOW + timedelta(minutes=1), "UTC", PARTICIPANTS),
        ("Planning", NOW, "Asia/Calcutta", PARTICIPANTS),
        (
            "Planning",
            NOW,
            "UTC",
            (
                PersonRef(display_name="Mira Patel", email="Mira@example.com"),
                PARTICIPANTS[1],
            ),
        ),
        (
            "Planning",
            NOW,
            "UTC",
            (
                PersonRef(display_name="Mira", email="other@example.com"),
                PARTICIPANTS[1],
            ),
        ),
        ("Planning", NOW, "UTC", tuple(reversed(PARTICIPANTS))),
        (
            "Planning",
            NOW,
            "UTC",
            (
                PersonRef(display_name="Mira", email="mira@example.com"),
                PARTICIPANTS[1],
            ),
        ),
    ],
)
def test_changed_ingest_metadata_conflicts_and_schedules_the_staged_recording(
    tmp_path: Path,
    title: str,
    occurred_at: datetime,
    timezone_name: str,
    participants: tuple[PersonRef, ...],
) -> None:
    service, database = workflow(tmp_path)
    content = b"RIFF\x00\x00\x00\x00WAVEmetadata"
    original = IngestMeeting(
        title="Planning",
        occurred_at=NOW,
        timezone="UTC",
        original_name="meeting.wav",
        ingest_key="upload-one",
        actor_id="test-actor",
        participants=PARTICIPANTS,
    )
    changed = IngestMeeting(
        title=title,
        occurred_at=occurred_at,
        timezone=timezone_name,
        original_name="meeting.wav",
        ingest_key="upload-one",
        actor_id="test-actor",
        participants=participants,
    )
    service.ingest(original, io.BytesIO(content))

    with pytest.raises(IdempotencyConflictError):
        service.ingest(changed, io.BytesIO(content))

    with SqliteUnitOfWork(database, immediate=False) as uow:
        cleanup = uow.recording_cleanups.find_by_storage_key("00000000000000000000000000000002.wav")
    assert cleanup is not None
    assert cleanup.expected_sha256 == hashlib.sha256(content).hexdigest()


def test_invalid_direct_ingest_metadata_is_rejected_before_recording_storage(
    tmp_path: Path,
) -> None:
    recording_store = FakeRecordingStore(tmp_path / "audio")
    service, _ = workflow(tmp_path, recording_store=recording_store)
    command = IngestMeeting(
        title="Planning",
        occurred_at=NOW,
        timezone="Mars/Olympus",
        original_name="meeting.wav",
        ingest_key="upload-one",
        actor_id="test-actor",
    )

    with pytest.raises(ValueError, match="IANA timezone"):
        service.ingest(command, io.BytesIO(b"RIFF\x00\x00\x00\x00WAVEinvalid"))

    assert recording_store.published_keys == []


def test_conflicting_ingest_schedules_the_unreferenced_recording(tmp_path: Path) -> None:
    service, database = workflow(tmp_path)
    command = IngestMeeting(
        title="Raw meeting",
        occurred_at=NOW,
        timezone="Asia/Calcutta",
        original_name="meeting.wav",
        ingest_key="upload-one",
        actor_id="test-actor",
    )
    service.ingest(command, io.BytesIO(b"RIFF\x00\x00\x00\x00WAVEfirst"))

    with pytest.raises(IdempotencyConflictError):
        service.ingest(command, io.BytesIO(b"RIFF\x00\x00\x00\x00WAVEsecond"))

    recordings = tuple((tmp_path / "audio").glob("*.wav"))
    assert len(recordings) == 2
    with SqliteUnitOfWork(database, immediate=False) as uow:
        cleanup = uow.recording_cleanups.find_by_storage_key("00000000000000000000000000000002.wav")
    assert cleanup is not None
    assert cleanup.expected_sha256 == hashlib.sha256(b"RIFF\x00\x00\x00\x00WAVEsecond").hexdigest()


def test_duplicate_content_schedules_only_the_unowned_recording(tmp_path: Path) -> None:
    service, database = workflow(tmp_path)
    content = b"RIFF\x00\x00\x00\x00WAVEshared"
    first = IngestMeeting(
        title="First meeting",
        occurred_at=NOW,
        timezone="UTC",
        original_name="first.wav",
        ingest_key="upload-one",
        actor_id="test-actor",
    )
    second = IngestMeeting(
        title="Second meeting",
        occurred_at=NOW,
        timezone="UTC",
        original_name="second.wav",
        ingest_key="upload-two",
        actor_id="test-actor",
    )

    first_meeting = service.ingest(first, io.BytesIO(content))
    second_meeting = service.ingest(second, io.BytesIO(content))

    recordings = tuple((tmp_path / "audio").glob("*.wav"))
    assert first_meeting.audio_asset_id == second_meeting.audio_asset_id
    assert len(recordings) == 2
    with SqliteUnitOfWork(database, immediate=False) as uow:
        cleanup = uow.recording_cleanups.find_by_storage_key("00000000000000000000000000000002.wav")
    assert cleanup is not None
    assert cleanup.expected_sha256 == hashlib.sha256(content).hexdigest()


def test_failed_duplicate_ingest_schedules_its_staged_recording(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    recording_store = FakeRecordingStore(tmp_path / "audio")
    service, database = workflow(tmp_path, recording_store=recording_store)
    content = b"RIFF\x00\x00\x00\x00WAVEshared"
    first = IngestMeeting(
        title="First meeting",
        occurred_at=NOW,
        timezone="UTC",
        original_name="private-first-name.wav",
        ingest_key="upload-one",
        actor_id="test-actor",
    )
    second = IngestMeeting(
        title="Second meeting",
        occurred_at=NOW,
        timezone="UTC",
        original_name="private-second-name.wav",
        ingest_key="upload-two",
        actor_id="test-actor",
    )
    retained = service.ingest(first, io.BytesIO(content))

    def fail_enqueue(*args: object, **kwargs: object) -> None:
        del args, kwargs
        raise RuntimeError("queue unavailable")

    monkeypatch.setattr(service._processing_scheduler, "enqueue_in", fail_enqueue)

    with pytest.raises(RuntimeError, match="queue unavailable"):
        service.ingest(second, io.BytesIO(content))

    assert recording_store.published_keys == [
        "00000000000000000000000000000001.wav",
        "00000000000000000000000000000002.wav",
    ]
    assert recording_store.path("00000000000000000000000000000001.wav").exists()
    assert recording_store.path("00000000000000000000000000000002.wav").exists()
    with SqliteUnitOfWork(database, immediate=False) as uow:
        cleanup = uow.recording_cleanups.find_by_storage_key("00000000000000000000000000000002.wav")
    assert cleanup is not None
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
        actor_id="test-actor",
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
    with SqliteUnitOfWork(database, immediate=False) as uow:
        binding = uow.ingest_requests.get(command.ingest_key)
        assert uow.recording_cleanups.find_by_storage_key(asset.storage_key) is None
    assert binding is not None

    recovered, _ = workflow(tmp_path, recording_store=recording_store)
    replay = recovered.ingest(
        command,
        io.BytesIO(b"RIFF\x00\x00\x00\x00WAVEpersisted"),
    )

    assert replay == meeting
    with SqliteUnitOfWork(database, immediate=False) as uow:
        cleanup = uow.recording_cleanups.find_by_storage_key("00000000000000000000000000000002.wav")
    assert cleanup is not None


def test_ambiguous_cleanup_commit_replays_the_ingest_and_keeps_the_job(
    tmp_path: Path,
) -> None:
    recording_store = FakeRecordingStore(tmp_path / "audio")
    service, database = workflow(tmp_path, recording_store=recording_store)
    command = IngestMeeting(
        title="Persisted meeting",
        occurred_at=NOW,
        timezone="UTC",
        original_name="meeting.wav",
        ingest_key="upload-one",
        actor_id="test-actor",
    )
    content = b"RIFF\x00\x00\x00\x00WAVEpersisted"
    original = service.ingest(command, io.BytesIO(content))
    ambiguous = MeetingWorkflow(
        unit_of_work=lambda: CommitThenRaiseUnitOfWork(database),
        recording_store=recording_store,
        erasure_tokens=ERASURE_TOKENS,
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

    replay = ambiguous.ingest(command, io.BytesIO(content))

    assert replay == original
    abandoned_key = "00000000000000000000000000000002.wav"
    with SqliteUnitOfWork(database, immediate=False) as uow:
        cleanup = uow.recording_cleanups.find_by_storage_key(abandoned_key)
    assert cleanup is not None
    assert cleanup.expected_sha256 == hashlib.sha256(content).hexdigest()


def test_failed_precommit_ingest_schedules_its_unique_recording(tmp_path: Path) -> None:
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
        actor_id="test-actor",
    )

    with pytest.raises(RuntimeError, match="commit did not persist"):
        service.ingest(command, io.BytesIO(b"RIFF\x00\x00\x00\x00WAVErolled-back"))

    with SqliteUnitOfWork(database, immediate=False) as uow:
        assert uow.meetings.find_by_ingest_key(command.ingest_key) is None
        assert uow.ingest_requests.get(command.ingest_key) is None
        assert (
            uow.audio_assets.find_by_sha256(
                hashlib.sha256(b"RIFF\x00\x00\x00\x00WAVErolled-back").hexdigest()
            )
            is None
        )
    assert tuple(recording_store.root.glob("*.wav")) == (
        recording_store.path("00000000000000000000000000000001.wav"),
    )
    with SqliteUnitOfWork(database, immediate=False) as uow:
        cleanup = uow.recording_cleanups.find_by_storage_key("00000000000000000000000000000001.wav")
    assert cleanup is not None


def test_legacy_meeting_without_binding_fails_closed_on_replay(tmp_path: Path) -> None:
    recording_store = FakeRecordingStore(tmp_path / "audio")
    service, database = workflow(tmp_path, recording_store=recording_store)
    command = IngestMeeting(
        title="Planning",
        occurred_at=NOW,
        timezone="UTC",
        original_name="meeting.wav",
        ingest_key="upload-one",
        actor_id="test-actor",
        participants=PARTICIPANTS,
    )
    content = b"RIFF\x00\x00\x00\x00WAVElegacy"
    meeting = service.ingest(command, io.BytesIO(content))
    with database.transaction(immediate=True) as connection:
        connection.execute(
            "DELETE FROM ingest_request_bindings WHERE ingest_key = ?",
            (command.ingest_key,),
        )

    with pytest.raises(IdempotencyConflictError):
        service.ingest(command, io.BytesIO(content))

    assert service.get_meeting(meeting.id) == meeting
    with SqliteUnitOfWork(database, immediate=False) as uow:
        assert uow.ingest_requests.get(command.ingest_key) is None
        cleanup = uow.recording_cleanups.find_by_storage_key("00000000000000000000000000000002.wav")
    assert cleanup is not None


def test_unsupported_persisted_fingerprint_version_fails_closed(tmp_path: Path) -> None:
    service, database = workflow(tmp_path)
    command = IngestMeeting(
        title="Planning",
        occurred_at=NOW,
        timezone="UTC",
        original_name="meeting.wav",
        ingest_key="upload-one",
        actor_id="test-actor",
    )
    content = b"RIFF\x00\x00\x00\x00WAVEfuture"
    service.ingest(command, io.BytesIO(content))
    with database.transaction(immediate=True) as connection:
        connection.execute(
            "DELETE FROM ingest_request_bindings WHERE ingest_key = ?",
            (command.ingest_key,),
        )
        connection.execute(
            """
            INSERT INTO ingest_request_bindings (
                ingest_key, fingerprint_version, request_fingerprint, created_at
            ) VALUES (?, ?, ?, ?)
            """,
            (command.ingest_key, 2, "a" * 64, str(NOW)),
        )

    with pytest.raises(IdempotencyConflictError):
        service.ingest(command, io.BytesIO(content))

    with SqliteUnitOfWork(database, immediate=False) as uow:
        cleanup = uow.recording_cleanups.find_by_storage_key("00000000000000000000000000000002.wav")
    assert cleanup is not None


def test_deduplicated_audio_size_mismatch_fails_without_binding_new_key(
    tmp_path: Path,
) -> None:
    service, database = workflow(tmp_path)
    content = b"RIFF\x00\x00\x00\x00WAVEidentity"
    first = IngestMeeting(
        title="First",
        occurred_at=NOW,
        timezone="UTC",
        original_name="first.wav",
        ingest_key="upload-one",
        actor_id="test-actor",
    )
    second = IngestMeeting(
        title="Second",
        occurred_at=NOW,
        timezone="UTC",
        original_name="second.wav",
        ingest_key="upload-two",
        actor_id="test-actor",
    )
    retained = service.ingest(first, io.BytesIO(content))
    with database.transaction(immediate=True) as connection:
        connection.execute(
            "UPDATE audio_assets SET size_bytes = size_bytes + 1 WHERE id = ?",
            (str(retained.audio_asset_id),),
        )

    with pytest.raises(AudioAssetIdentityMismatchError):
        service.ingest(second, io.BytesIO(content))

    with SqliteUnitOfWork(database, immediate=False) as uow:
        assert uow.meetings.find_by_ingest_key(second.ingest_key) is None
        assert uow.ingest_requests.get(second.ingest_key) is None
        cleanup = uow.recording_cleanups.find_by_storage_key("00000000000000000000000000000002.wav")
    assert cleanup is not None
    assert cleanup.expected_size_bytes == len(content)


def test_exact_replay_rejects_tampered_persisted_audio_identity(tmp_path: Path) -> None:
    service, database = workflow(tmp_path)
    content = b"RIFF\x00\x00\x00\x00WAVEbound-identity"
    command = IngestMeeting(
        title="Planning",
        occurred_at=NOW,
        timezone="UTC",
        original_name="meeting.wav",
        ingest_key="upload-one",
        actor_id="test-actor",
    )
    retained = service.ingest(command, io.BytesIO(content))
    with database.transaction(immediate=True) as connection:
        connection.execute(
            "UPDATE audio_assets SET size_bytes = size_bytes + 1 WHERE id = ?",
            (str(retained.audio_asset_id),),
        )

    with pytest.raises(AudioAssetIdentityMismatchError):
        service.ingest(command, io.BytesIO(content))

    with SqliteUnitOfWork(database, immediate=False) as uow:
        assert uow.meetings.find_by_ingest_key(command.ingest_key) is not None
        assert uow.ingest_requests.get(command.ingest_key) is not None
        cleanup = uow.recording_cleanups.find_by_storage_key("00000000000000000000000000000002.wav")
    assert cleanup is not None
    assert cleanup.expected_size_bytes == len(content)


def test_concurrent_same_content_ingests_assign_exact_storage_key_ownership(
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
            actor_id="test-actor",
        ),
        IngestMeeting(
            title="Second concurrent meeting",
            occurred_at=NOW,
            timezone="UTC",
            original_name="second.wav",
            ingest_key="concurrent-two",
            actor_id="test-actor",
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
        cleanup_jobs = tuple(
            uow.recording_cleanups.find_by_storage_key(key)
            for key in recording_store.published_keys
        )
    assert asset is not None
    assert len(recording_store.published_keys) == 2
    assert len(tuple(recording_store.root.glob("*.wav"))) == 2
    assert cleanup_jobs[recording_store.published_keys.index(asset.storage_key)] is None
    abandoned = tuple(job for job in cleanup_jobs if job is not None)
    assert len(abandoned) == 1
    assert abandoned[0].storage_key != asset.storage_key


def test_concurrent_exact_replays_create_one_meeting_and_one_binding(tmp_path: Path) -> None:
    recording_store = BarrierRecordingStore(tmp_path / "audio", Barrier(2))
    service, database = workflow(tmp_path, recording_store=recording_store)
    content = b"RIFF\x00\x00\x00\x00WAVEsame-key"
    commands = (
        IngestMeeting(
            title="Planning",
            occurred_at=NOW,
            timezone="UTC",
            original_name="first.wav",
            ingest_key="concurrent-key",
            actor_id="test-actor",
            participants=PARTICIPANTS,
        ),
        IngestMeeting(
            title="Planning",
            occurred_at=NOW,
            timezone="UTC",
            original_name="second.wav",
            ingest_key="concurrent-key",
            actor_id="test-actor",
            participants=PARTICIPANTS,
        ),
    )

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(service.ingest, command, io.BytesIO(content)) for command in commands
        ]
        meetings = tuple(future.result(timeout=10) for future in futures)

    assert meetings[0] == meetings[1]
    with SqliteUnitOfWork(database, immediate=False) as uow:
        binding = uow.ingest_requests.get("concurrent-key")
        asset = uow.audio_assets.get(meetings[0].audio_asset_id)
        cleanup_jobs = tuple(
            uow.recording_cleanups.find_by_storage_key(key)
            for key in recording_store.published_keys
        )
    assert binding is not None
    assert asset is not None
    assert len(tuple(job for job in cleanup_jobs if job is not None)) == 1
    assert cleanup_jobs[recording_store.published_keys.index(asset.storage_key)] is None


def test_concurrent_changed_request_binds_one_payload_and_rejects_the_other(
    tmp_path: Path,
) -> None:
    recording_store = BarrierRecordingStore(tmp_path / "audio", Barrier(2))
    service, database = workflow(tmp_path, recording_store=recording_store)
    content = b"RIFF\x00\x00\x00\x00WAVEconflicting-key"
    commands = (
        IngestMeeting(
            title="First request",
            occurred_at=NOW,
            timezone="UTC",
            original_name="first.wav",
            ingest_key="concurrent-key",
            actor_id="test-actor",
        ),
        IngestMeeting(
            title="Second request",
            occurred_at=NOW,
            timezone="UTC",
            original_name="second.wav",
            ingest_key="concurrent-key",
            actor_id="test-actor",
        ),
    )

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(service.ingest, command, io.BytesIO(content)) for command in commands
        ]
        errors = tuple(future.exception(timeout=10) for future in futures)
        successes: list[Meeting] = [
            futures[index].result(timeout=10) for index, error in enumerate(errors) if error is None
        ]
        conflicts = tuple(error for error in errors if isinstance(error, IdempotencyConflictError))

    assert len(successes) == 1
    assert len(conflicts) == 1
    assert all(error is None or isinstance(error, IdempotencyConflictError) for error in errors)
    with SqliteUnitOfWork(database, immediate=False) as uow:
        persisted = uow.meetings.find_by_ingest_key("concurrent-key")
        binding = uow.ingest_requests.get("concurrent-key")
        asset = uow.audio_assets.get(successes[0].audio_asset_id)
        cleanup_jobs = tuple(
            uow.recording_cleanups.find_by_storage_key(key)
            for key in recording_store.published_keys
        )
    assert persisted is not None
    assert persisted == successes[0]
    assert persisted.title in {"First request", "Second request"}
    assert binding is not None
    assert asset is not None
    assert persisted.occurred_at is not None
    expected_request = IngestRequestIdentity(
        ingest_key=persisted.ingest_key,
        title=persisted.title,
        occurred_at=persisted.occurred_at,
        timezone=persisted.timezone,
        participants=persisted.participants,
    )
    expected_audio = IngestAudioIdentity(
        sha256=asset.sha256,
        size_bytes=asset.size_bytes,
    )
    assert binding.request_fingerprint == expected_request.fingerprint(
        expected_audio,
        binding.fingerprint_version,
    )
    assert len(tuple(job for job in cleanup_jobs if job is not None)) == 1
    assert cleanup_jobs[recording_store.published_keys.index(asset.storage_key)] is None


def test_cleanup_scheduling_failure_does_not_mask_ingest_outcomes(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    service, _ = workflow(
        tmp_path,
        cleanup_scheduler_type=FailingCleanupScheduler,
    )
    command = IngestMeeting(
        title="Planning",
        occurred_at=NOW,
        timezone="UTC",
        original_name="meeting.wav",
        ingest_key="upload-one",
        actor_id="test-actor",
    )

    meeting = service.ingest(command, io.BytesIO(b"RIFF\x00\x00\x00\x00WAVEfirst"))
    with pytest.raises(IdempotencyConflictError):
        service.ingest(command, io.BytesIO(b"RIFF\x00\x00\x00\x00WAVEsecond"))

    assert service.get_meeting(meeting.id) == meeting
    records = [
        record
        for record in caplog.records
        if record.getMessage() == "recording cleanup scheduling failed"
    ]
    assert len(records) == 2
    assert all(
        getattr(record, "fields", None)
        == {
            "event": "recording_cleanup_schedule_failed",
            "reason": "abandoned_ingest",
            "exception_type": "RuntimeError",
        }
        for record in records
    )


def test_ingest_persists_a_neutral_audio_name(tmp_path: Path) -> None:
    service, database = workflow(tmp_path)
    meeting = service.ingest(
        IngestMeeting(
            title="Private planning",
            occurred_at=NOW,
            timezone="UTC",
            original_name="customer-acquisition-confidential.wav",
            ingest_key="upload-one",
            actor_id="test-actor",
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
            actor_id="test-actor",
            participants=submitted_participants,
        ),
        io.BytesIO(b"RIFF\x00\x00\x00\x00WAVEdata"),
    )

    reviewed = await process_meeting(service, database, ingested.id)

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
            actor_id="test-actor",
        ),
        io.BytesIO(b"RIFF\x00\x00\x00\x00WAVEdata"),
    )

    reviewed = await process_meeting(service, database, ingested.id)

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
            actor_id="test-actor",
        ),
        io.BytesIO(b"RIFF\x00\x00\x00\x00WAVEdata"),
    )
    reviewed = await process_meeting(service, database, ingested.id)
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
            actor_id="test-actor",
        ),
        io.BytesIO(b"RIFF\x00\x00\x00\x00WAVEdata"),
    )
    reviewed = await process_meeting(service, database, ingested.id)
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
            actor_id="test-actor",
        ),
        io.BytesIO(b"RIFF\x00\x00\x00\x00WAVEdata"),
    )
    reviewed = await process_meeting(service, database, ingested.id)
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
