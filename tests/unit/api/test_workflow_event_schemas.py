from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID

from meeting_action_orchestrator.api.workflow_event_schemas import WorkflowEventResponse
from meeting_action_orchestrator.domain.enums import (
    AudioMediaType,
    MeetingStatus,
    ProcessingStage,
    ReviewOrigin,
    WriteKind,
    WriteStatus,
)
from meeting_action_orchestrator.domain.workflow_events import (
    DeliveryChangeKind,
    DeliveryTransitionMetadata,
    MeetingIngestedMetadata,
    MeetingTransitionMetadata,
    ProcessingAttemptMetadata,
    ProcessingAuditOutcome,
    ProcessingRetryRequestedMetadata,
    ReviewApprovedMetadata,
    ReviewChangeKind,
    ReviewRevisionMetadata,
    SpecialistHandoffMetadata,
    SpecialistRole,
    WorkflowEvent,
    WorkflowEventMetadata,
    WorkflowEventType,
)

NOW = datetime(2026, 8, 7, 12, 0, tzinfo=timezone.utc)
MEETING_ID = UUID("10000000-0000-4000-8000-000000000001")
DIGEST_A = "a" * 64
DIGEST_B = "b" * 64
DIGEST_C = "c" * 64
ACTOR_ID = "portfolio-owner"
PRIVATE_REQUEST_ID = "req_private_raw_marker"


def metadata_cases() -> tuple[tuple[WorkflowEventType, WorkflowEventMetadata], ...]:
    return (
        (
            WorkflowEventType.MEETING_INGESTED,
            MeetingIngestedMetadata(
                recording_digest=DIGEST_A,
                media_type=AudioMediaType.WAV,
                size_bytes=1_024,
                duration_ms=60_000,
            ),
        ),
        (
            WorkflowEventType.MEETING_TRANSITIONED,
            MeetingTransitionMetadata(
                previous_status=MeetingStatus.INGESTED,
                current_status=MeetingStatus.TRANSCRIBING,
                meeting_version=1,
            ),
        ),
        (
            WorkflowEventType.PROCESSING_ATTEMPTED,
            ProcessingAttemptMetadata(
                stage=ProcessingStage.TRANSCRIPTION,
                attempt_number=1,
                outcome=ProcessingAuditOutcome.STARTED,
                input_digest=DIGEST_A,
            ),
        ),
        (
            WorkflowEventType.PROCESSING_RETRY_REQUESTED,
            ProcessingRetryRequestedMetadata(
                stage=ProcessingStage.TRANSCRIPTION,
                previous_attempt_count=1,
                meeting_version=2,
            ),
        ),
        (
            WorkflowEventType.SPECIALIST_HANDOFF_COMPLETED,
            SpecialistHandoffMetadata(
                specialist=SpecialistRole.EXTRACT,
                processing_attempt_number=1,
                model_identifier="gpt-5-mini",
                input_digest=DIGEST_A,
                output_digest=DIGEST_B,
                request_ids_digest=DIGEST_C,
                request_count=1,
                input_tokens=100,
                output_tokens=20,
            ),
        ),
        (
            WorkflowEventType.REVIEW_REVISED,
            ReviewRevisionMetadata(
                revision_number=1,
                review_digest=DIGEST_A,
                origin=ReviewOrigin.MODEL,
                change_kind=ReviewChangeKind.MODEL_CREATED,
                decision_count=1,
                action_count=2,
                question_count=3,
                risk_count=4,
                issue_count=5,
                blocking_issue_count=1,
            ),
        ),
        (
            WorkflowEventType.REVIEW_APPROVED,
            ReviewApprovedMetadata(
                revision_number=1,
                review_digest=DIGEST_A,
                write_intent_count=2,
            ),
        ),
        (
            WorkflowEventType.DELIVERY_TRANSITIONED,
            DeliveryTransitionMetadata(
                change_kind=DeliveryChangeKind.CREATED,
                write_kind=WriteKind.TASK,
                write_intent_digest=DIGEST_A,
                current_status=WriteStatus.PENDING,
                attempt_count=0,
                reconciliation_count=0,
            ),
        ),
    )


def test_public_projection_handles_every_workflow_event_type_explicitly() -> None:
    cases = metadata_cases()

    responses = tuple(
        WorkflowEventResponse.from_domain(
            WorkflowEvent(
                id=UUID(f"20000000-0000-4000-8000-{sequence:012d}"),
                meeting_id=MEETING_ID,
                sequence=sequence,
                type=event_type,
                actor_id=ACTOR_ID,
                safe_metadata=metadata,
                occurred_at=NOW,
            )
        )
        for sequence, (event_type, metadata) in enumerate(cases, start=1)
    )

    assert {response.type for response in responses} == set(WorkflowEventType)
    assert {response.safe_metadata.kind for response in responses} == {
        metadata.kind for _, metadata in cases
    }
    assert all(response.actor_id == ACTOR_ID for response in responses)
    assert all(response.meeting_id == MEETING_ID for response in responses)


def test_processing_projection_preserves_an_unavailable_input_digest() -> None:
    response = WorkflowEventResponse.from_domain(
        WorkflowEvent(
            id=UUID("20000000-0000-4000-8000-000000000001"),
            meeting_id=MEETING_ID,
            sequence=1,
            type=WorkflowEventType.PROCESSING_ATTEMPTED,
            actor_id=None,
            safe_metadata=ProcessingAttemptMetadata(
                stage=ProcessingStage.TRANSCRIPTION,
                attempt_number=1,
                outcome=ProcessingAuditOutcome.STARTED,
            ),
            occurred_at=NOW,
        )
    )

    assert response.safe_metadata.input_digest is None
    assert '"input_digest":null' in response.model_dump_json()


def test_public_projection_has_a_fixed_privacy_safe_surface() -> None:
    event_type, metadata = metadata_cases()[4]
    response = WorkflowEventResponse.from_domain(
        WorkflowEvent(
            id=UUID("20000000-0000-4000-8000-000000000001"),
            meeting_id=MEETING_ID,
            sequence=1,
            type=event_type,
            actor_id=ACTOR_ID,
            safe_metadata=metadata,
            occurred_at=NOW,
        )
    )
    payload = response.model_dump(mode="json")
    rendered = response.model_dump_json()

    assert set(payload) == {
        "id",
        "meeting_id",
        "sequence",
        "type",
        "actor_id",
        "safe_metadata",
        "occurred_at",
    }
    assert set(payload["safe_metadata"]) == {
        "kind",
        "specialist",
        "processing_attempt_number",
        "model_identifier",
        "input_digest",
        "output_digest",
        "request_ids_digest",
        "request_count",
        "input_tokens",
        "output_tokens",
        "cached_input_tokens",
        "reasoning_tokens",
    }
    for hidden_name in (
        "provider_request_id",
        "client_request_id",
        "idempotency_key",
        "ingest_key",
        PRIVATE_REQUEST_ID,
    ):
        assert hidden_name not in rendered
