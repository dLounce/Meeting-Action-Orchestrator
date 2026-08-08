from datetime import datetime, timezone
from uuid import UUID

import pytest
from pydantic import ValidationError

from meeting_action_orchestrator.domain.enums import MeetingOperationKind, ProcessingStage
from meeting_action_orchestrator.domain.hashing import canonical_sha256
from meeting_action_orchestrator.domain.models import MeetingOperationBinding

NOW = datetime(2026, 8, 7, 9, 0, tzinfo=timezone.utc)
MEETING_ID = UUID("10000000-0000-4000-8000-000000000001")


def binding(
    *,
    operation: MeetingOperationKind = MeetingOperationKind.PROCESSING_RETRY,
    stage: ProcessingStage | None = ProcessingStage.TRANSCRIPTION,
    fingerprint: str | None = None,
) -> MeetingOperationBinding:
    identity = {
        "actor_id": "portfolio-owner",
        "expected_version": 4,
        "meeting_id": MEETING_ID,
        "operation": operation,
        "request_key": "meeting-operation-one",
        "stage": stage,
    }
    return MeetingOperationBinding(
        request_key="meeting-operation-one",
        meeting_id=MEETING_ID,
        operation=operation,
        actor_id="portfolio-owner",
        stage=stage,
        expected_version=4,
        request_fingerprint=fingerprint or canonical_sha256(identity),
        created_at=NOW,
    )


def test_meeting_operation_binding_is_immutable_and_fingerprint_bound() -> None:
    operation = binding()
    actor_field = "actor_id"

    with pytest.raises(ValidationError):
        setattr(operation, actor_field, "another-owner")
    with pytest.raises(ValidationError, match="fingerprint does not match"):
        binding(fingerprint="f" * 64)


@pytest.mark.parametrize(
    ("operation", "stage"),
    [
        (MeetingOperationKind.PROCESSING_RETRY, None),
        (MeetingOperationKind.CANCELLATION, ProcessingStage.EXTRACTION),
    ],
)
def test_meeting_operation_binding_enforces_operation_stage(
    operation: MeetingOperationKind,
    stage: ProcessingStage | None,
) -> None:
    with pytest.raises(ValidationError, match="stage does not match"):
        binding(operation=operation, stage=stage)


def test_cancellation_binding_requires_no_processing_stage() -> None:
    operation = binding(operation=MeetingOperationKind.CANCELLATION, stage=None)

    assert operation.stage is None
    assert operation.operation is MeetingOperationKind.CANCELLATION
