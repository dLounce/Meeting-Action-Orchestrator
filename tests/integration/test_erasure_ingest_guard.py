from __future__ import annotations

import io
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Event, Lock
from typing import Any, ClassVar
from uuid import UUID

import pytest

import meeting_action_orchestrator.application.workflow as workflow_module
from meeting_action_orchestrator.application.errors import (
    OperationConflictError,
    PersistenceIntegrityError,
)
from meeting_action_orchestrator.application.meeting_erasure import (
    ErasureKeyRegistry,
    MeetingErasureService,
)
from meeting_action_orchestrator.application.workflow import IngestMeeting
from meeting_action_orchestrator.domain.enums import (
    MeetingErasureReason,
    MeetingErasureRecordingState,
)
from meeting_action_orchestrator.domain.models import (
    MeetingErasureJob,
    MeetingErasureTombstone,
)
from meeting_action_orchestrator.infrastructure.database import Database
from meeting_action_orchestrator.infrastructure.erasure_tokens import ErasureTokenKeyring
from meeting_action_orchestrator.infrastructure.repositories import SqliteUnitOfWork
from tests.integration.test_workflow import (
    NOW,
    FakeRecordingStore,
    FrozenClock,
    workflow,
)

INGEST_KEY = "private-erasure-ingest-key"
CONTENT = b"RIFF\x00\x00\x00\x00WAVEerasure-guard"


class SignalingRecordingStore(FakeRecordingStore):
    def __init__(self, root: Path) -> None:
        super().__init__(root)
        self.published = Event()

    def put(self, stream: Any, original_name: str) -> Any:
        stored = super().put(stream, original_name)
        self.published.set()
        return stored


class PauseFirstUnitOfWork(SqliteUnitOfWork):
    entered: ClassVar[Event] = Event()
    release: ClassVar[Event] = Event()
    state_lock: ClassVar[Lock] = Lock()
    pause_next: ClassVar[bool] = True

    @classmethod
    def reset(cls) -> None:
        cls.entered = Event()
        cls.release = Event()
        cls.pause_next = True

    def __enter__(self) -> PauseFirstUnitOfWork:
        super().__enter__()
        unit_of_work_type = type(self)
        with unit_of_work_type.state_lock:
            pause = unit_of_work_type.pause_next
            unit_of_work_type.pause_next = False
        if pause:
            unit_of_work_type.entered.set()
            if not unit_of_work_type.release.wait(timeout=5):
                raise TimeoutError("ingest transaction release timed out")
        return self


class AttemptSignalingUnitOfWork(SqliteUnitOfWork):
    def __init__(self, database: Database, attempted: Event) -> None:
        super().__init__(database)
        self._attempted = attempted

    def __enter__(self) -> AttemptSignalingUnitOfWork:
        self._attempted.set()
        super().__enter__()
        return self


def command() -> IngestMeeting:
    return IngestMeeting(
        title="Erasure guard",
        occurred_at=NOW,
        timezone="UTC",
        original_name="private-recording.wav",
        ingest_key=INGEST_KEY,
        actor_id="test-actor",
    )


def add_tombstone(
    uow: SqliteUnitOfWork,
    tokens: ErasureTokenKeyring,
    *,
    number: int,
) -> MeetingErasureJob:
    key_id = tokens.active_key_id
    if uow.erasure_key_verifiers.get(key_id) is None:
        uow.erasure_key_verifiers.add(tokens.verifier(key_id, NOW))
    meeting_id = UUID(int=80_000 + number)
    job = MeetingErasureJob(
        id=UUID(int=90_000 + number),
        token_version=1,
        token_key_id=key_id,
        meeting_token=tokens.meeting_token(meeting_id).digest,
        reason=MeetingErasureReason.USER_REQUEST,
        erased_meeting_version=0,
        recording_state=MeetingErasureRecordingState.WAITING_SHARED,
        pending_audio_asset_id=UUID(int=70_000 + number),
        created_at=NOW,
        updated_at=NOW,
    )
    uow.meeting_erasures.add(job)
    uow.meeting_erasure_tombstones.add(
        MeetingErasureTombstone.create(
            job.id,
            tokens.meeting_token(meeting_id),
            tokens.ingest_key_token(INGEST_KEY),
            NOW,
        )
    )
    return job


def test_tombstone_conflict_rolls_back_and_schedules_staged_recording_cleanup(
    tmp_path: Path,
) -> None:
    tokens = ErasureTokenKeyring("current", {"current": b"c" * 32})
    recording_store = FakeRecordingStore(tmp_path / "audio")
    service, database = workflow(
        tmp_path,
        recording_store=recording_store,
        erasure_tokens=tokens,
    )
    with SqliteUnitOfWork(database) as uow:
        add_tombstone(uow, tokens, number=1)
        uow.commit()

    with pytest.raises(OperationConflictError) as raised:
        service.ingest(command(), io.BytesIO(CONTENT))

    assert str(raised.value) == "The ingest request conflicts with the current workflow state"
    assert INGEST_KEY not in str(raised.value)
    assert "erased" not in str(raised.value).casefold()
    with SqliteUnitOfWork(database, immediate=False) as uow:
        assert uow.meetings.find_by_ingest_key(INGEST_KEY) is None
        assert uow.ingest_requests.get(INGEST_KEY) is None
        cleanup = uow.recording_cleanups.find_by_storage_key("00000000000000000000000000000001.wav")
    assert cleanup is not None


def test_rotated_keyring_matches_an_old_ingest_tombstone(tmp_path: Path) -> None:
    old = ErasureTokenKeyring("old", {"old": b"o" * 32})
    rotating = ErasureTokenKeyring("new", {"new": b"n" * 32, "old": b"o" * 32})
    service, database = workflow(tmp_path, erasure_tokens=rotating)
    with SqliteUnitOfWork(database) as uow:
        add_tombstone(uow, old, number=1)
        uow.commit()

    with pytest.raises(OperationConflictError):
        service.ingest(command(), io.BytesIO(CONTENT))

    with SqliteUnitOfWork(database, immediate=False) as uow:
        assert uow.meetings.find_by_ingest_key(INGEST_KEY) is None


def test_multiple_rotation_candidates_fail_closed_and_cleanup_the_staged_file(
    tmp_path: Path,
) -> None:
    old = ErasureTokenKeyring("old", {"old": b"o" * 32})
    new = ErasureTokenKeyring("new", {"new": b"n" * 32})
    rotating = ErasureTokenKeyring("new", {"new": b"n" * 32, "old": b"o" * 32})
    service, database = workflow(tmp_path, erasure_tokens=rotating)
    with SqliteUnitOfWork(database) as uow:
        add_tombstone(uow, old, number=1)
        add_tombstone(uow, new, number=2)
        uow.commit()

    with pytest.raises(PersistenceIntegrityError):
        service.ingest(command(), io.BytesIO(CONTENT))

    with SqliteUnitOfWork(database, immediate=False) as uow:
        assert uow.meetings.find_by_ingest_key(INGEST_KEY) is None
        cleanup = uow.recording_cleanups.find_by_storage_key("00000000000000000000000000000001.wav")
    assert cleanup is not None


def test_committed_tombstone_wins_against_a_concurrent_ingest(tmp_path: Path) -> None:
    tokens = ErasureTokenKeyring("current", {"current": b"c" * 32})
    recording_store = SignalingRecordingStore(tmp_path / "audio")
    service, database = workflow(
        tmp_path,
        recording_store=recording_store,
        erasure_tokens=tokens,
    )
    with ThreadPoolExecutor(max_workers=1) as executor:
        with SqliteUnitOfWork(database) as uow:
            add_tombstone(uow, tokens, number=1)
            future = executor.submit(service.ingest, command(), io.BytesIO(CONTENT))
            assert recording_store.published.wait(timeout=5)
            uow.commit()
        with pytest.raises(OperationConflictError):
            future.result(timeout=5)

    with SqliteUnitOfWork(database, immediate=False) as uow:
        assert uow.meetings.find_by_ingest_key(INGEST_KEY) is None
        assert (
            uow.meeting_erasure_tombstones.find_by_ingest_key_tokens(
                tokens.ingest_key_tokens(INGEST_KEY)
            )
            is not None
        )


def test_serialized_ingest_is_erased_before_its_identity_can_be_reused(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    tokens = ErasureTokenKeyring("current", {"current": b"c" * 32})
    audio_id = UUID("10000000-0000-4000-8000-000000000010")
    meeting_id = UUID("10000000-0000-4000-8000-000000000011")
    generated_ids = iter((audio_id, meeting_id))
    monkeypatch.setattr(workflow_module, "uuid4", lambda: next(generated_ids))
    PauseFirstUnitOfWork.reset()
    service, database = workflow(
        tmp_path,
        erasure_tokens=tokens,
        unit_of_work_type=PauseFirstUnitOfWork,
    )
    attempted = Event()
    registry = ErasureKeyRegistry(
        unit_of_work=lambda: AttemptSignalingUnitOfWork(database, attempted),
        validation_unit_of_work=lambda: SqliteUnitOfWork(database, immediate=False),
        tokens=tokens,
        clock=FrozenClock(),
    )
    erasures = MeetingErasureService(
        unit_of_work=lambda: SqliteUnitOfWork(database),
        tokens=tokens,
        key_registry=registry,
        clock=FrozenClock(),
    )

    def request_erasure() -> Any:
        return erasures._request(
            meeting_id,
            0,
            "erase-concurrent-ingest",
            "portfolio-owner",
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        ingest_future = executor.submit(service.ingest, command(), io.BytesIO(CONTENT))
        assert PauseFirstUnitOfWork.entered.wait(timeout=5)
        erasure_future = executor.submit(request_erasure)
        assert attempted.wait(timeout=5)
        PauseFirstUnitOfWork.release.set()
        ingested = ingest_future.result(timeout=5)
        erased = erasure_future.result(timeout=5)

    assert ingested.id == meeting_id
    assert erased.job.id
    with SqliteUnitOfWork(database, immediate=False) as uow:
        assert uow.meetings.find_by_ingest_key(INGEST_KEY) is None
        assert (
            uow.meeting_erasure_tombstones.find_by_ingest_key_tokens(
                tokens.ingest_key_tokens(INGEST_KEY)
            )
            is not None
        )
    with pytest.raises(OperationConflictError):
        service.ingest(command(), io.BytesIO(CONTENT))
