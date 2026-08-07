from datetime import datetime, timedelta, timezone
from typing import cast
from uuid import UUID

import pytest
from pydantic import ValidationError

from meeting_action_orchestrator.application.ports import WorkflowEventCursor
from meeting_action_orchestrator.application.state_machine import (
    MEETING_TRANSITIONS as APPLICATION_MEETING_TRANSITIONS,
)
from meeting_action_orchestrator.application.state_machine import (
    WRITE_TRANSITIONS as APPLICATION_WRITE_TRANSITIONS,
)
from meeting_action_orchestrator.domain.enums import (
    AudioMediaType,
    FailureCode,
    FailureDisposition,
    MeetingStatus,
    ProcessingStage,
    ReviewOrigin,
    WriteKind,
    WriteStatus,
)
from meeting_action_orchestrator.domain.hashing import canonical_json
from meeting_action_orchestrator.domain.transition_rules import (
    MEETING_TRANSITIONS,
    WRITE_TRANSITIONS,
)
from meeting_action_orchestrator.domain.workflow_events import (
    WORKFLOW_RETRY_DELAY_MAX_MS,
    WORKFLOW_SEQUENCE_MAX,
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
    WorkflowEventDraft,
    WorkflowEventType,
    workflow_request_ids_digest,
    workflow_retry_delay_ms,
    workflow_write_intent_digest,
)

NOW = datetime(2026, 8, 7, 10, 0, tzinfo=timezone.utc)
MEETING_ID = UUID("e5818d7f-d0ca-47db-92c6-f61d3a815c38")
WRITE_INTENT_ID = UUID("ffb1b01a-2ace-4df5-8393-18e434b5329d")


def ingested_metadata() -> MeetingIngestedMetadata:
    return MeetingIngestedMetadata(
        recording_digest="a" * 64,
        media_type=AudioMediaType.WAV,
        size_bytes=1_024,
        duration_ms=60_000,
    )


def specialist_metadata() -> SpecialistHandoffMetadata:
    return SpecialistHandoffMetadata(
        specialist=SpecialistRole.EXTRACT,
        processing_attempt_number=1,
        model_identifier="gpt-5.4-mini",
        input_digest="a" * 64,
        output_digest="b" * 64,
        request_ids_digest=workflow_request_ids_digest(("req-server-one", "client-one")),
        request_count=1,
        input_tokens=800,
        output_tokens=200,
        cached_input_tokens=100,
        reasoning_tokens=50,
    )


def test_event_redacts_actor_from_repr_and_serialization() -> None:
    actor = "private-actor@example.com"
    draft = WorkflowEventDraft(
        meeting_id=MEETING_ID,
        type=WorkflowEventType.MEETING_INGESTED,
        actor_id=actor,
        safe_metadata=ingested_metadata(),
        occurred_at=NOW,
    )

    assert draft.actor_id == actor
    assert actor not in repr(draft)
    assert actor not in str(draft)
    assert actor not in draft.model_dump_json()
    assert actor not in canonical_json(draft)
    assert "actor_id" not in draft.model_dump()


def test_event_rejects_metadata_for_another_event_type() -> None:
    with pytest.raises(ValidationError, match="does not match"):
        WorkflowEventDraft(
            meeting_id=MEETING_ID,
            type=WorkflowEventType.REVIEW_APPROVED,
            safe_metadata=ingested_metadata(),
            occurred_at=NOW,
        )


def test_cancellation_is_a_meeting_transition_without_a_processing_attempt() -> None:
    metadata = MeetingTransitionMetadata(
        previous_status=MeetingStatus.INGESTED,
        current_status=MeetingStatus.CANCELLED,
        meeting_version=1,
    )

    assert metadata.current_status is MeetingStatus.CANCELLED
    assert "cancelled" not in {outcome.value for outcome in ProcessingAuditOutcome}


@pytest.mark.parametrize(
    "status",
    [MeetingStatus.AWAITING_APPROVAL, MeetingStatus.FILING],
)
def test_meeting_audit_allows_intentional_shared_graph_self_transitions(
    status: MeetingStatus,
) -> None:
    metadata = MeetingTransitionMetadata(
        previous_status=status,
        current_status=status,
        meeting_version=2,
    )

    assert metadata.previous_status is metadata.current_status


@pytest.mark.parametrize(
    ("previous", "current"),
    [
        (MeetingStatus.INGESTED, MeetingStatus.COMPLETED),
        (MeetingStatus.COMPLETED, MeetingStatus.FILING),
        (MeetingStatus.TRANSCRIBING, MeetingStatus.CANCELLED),
    ],
)
def test_meeting_audit_rejects_transitions_outside_the_shared_graph(
    previous: MeetingStatus,
    current: MeetingStatus,
) -> None:
    with pytest.raises(ValidationError, match="meeting transition"):
        MeetingTransitionMetadata(
            previous_status=previous,
            current_status=current,
            meeting_version=2,
        )


def test_shared_transition_graphs_are_immutable_and_reexported() -> None:
    assert APPLICATION_MEETING_TRANSITIONS is MEETING_TRANSITIONS
    assert APPLICATION_WRITE_TRANSITIONS is WRITE_TRANSITIONS
    meeting_graph = cast(dict[MeetingStatus, frozenset[MeetingStatus]], MEETING_TRANSITIONS)
    write_graph = cast(dict[WriteStatus, frozenset[WriteStatus]], WRITE_TRANSITIONS)

    with pytest.raises(TypeError):
        meeting_graph[MeetingStatus.INGESTED] = frozenset()
    with pytest.raises(TypeError):
        write_graph[WriteStatus.PENDING] = frozenset()


@pytest.mark.parametrize("sequence", [False, 0, WORKFLOW_SEQUENCE_MAX + 1])
def test_event_cursor_stays_within_workflow_sequence_bounds(sequence: int) -> None:
    with pytest.raises(ValueError, match="workflow event sequence"):
        WorkflowEventCursor(meeting_id=MEETING_ID, sequence=sequence)


@pytest.mark.parametrize(
    "field",
    [
        "transcript",
        "email",
        "content",
        "prompt",
        "provider_response",
        "provider_request_id",
        "request_body",
    ],
)
def test_metadata_rejects_sensitive_or_unlisted_fields_without_echoing_values(
    field: str,
) -> None:
    marker = "private-value-that-must-not-escape"
    payload: dict[str, object] = {
        "recording_digest": "a" * 64,
        "media_type": AudioMediaType.WAV,
        "size_bytes": 1_024,
        "duration_ms": 60_000,
        field: marker,
    }

    with pytest.raises(ValidationError) as failure:
        MeetingIngestedMetadata.model_validate(payload)

    assert marker not in str(failure.value)


@pytest.mark.parametrize(
    "identifier",
    [
        "model with spaces",
        "owner@example.com",
        "model?api_key=secret",
        "x" * 129,
    ],
)
def test_specialist_model_identifier_is_bounded_and_url_safe(identifier: str) -> None:
    payload = specialist_metadata().model_dump(mode="python") | {"model_identifier": identifier}

    with pytest.raises(ValidationError):
        SpecialistHandoffMetadata.model_validate(payload)


@pytest.mark.parametrize("attempt_number", [False, 0, 1_000_000_001])
def test_specialist_handoff_requires_a_bounded_processing_attempt(
    attempt_number: int,
) -> None:
    payload = specialist_metadata().model_dump(mode="python") | {
        "processing_attempt_number": attempt_number
    }

    with pytest.raises(ValidationError, match="processing_attempt_number"):
        SpecialistHandoffMetadata.model_validate(payload)


def test_request_identifier_digest_is_deterministic_and_keeps_raw_ids_out_of_dumps() -> None:
    request_ids = ("req-private-server", "client-private-transport")
    digest = workflow_request_ids_digest(request_ids)
    metadata = specialist_metadata().model_copy(update={"request_ids_digest": digest})
    dumped = metadata.model_dump_json()

    assert digest == workflow_request_ids_digest(request_ids)
    assert digest != workflow_request_ids_digest(tuple(reversed(request_ids)))
    assert digest in dumped
    assert all(request_id not in dumped for request_id in request_ids)


@pytest.mark.parametrize(
    "request_ids",
    [(), "req-one", ("",), ("with space",), ("x" * 201,)],
)
def test_request_identifier_digest_rejects_unbounded_input(
    request_ids: tuple[str, ...] | str,
) -> None:
    with pytest.raises(ValueError, match="Workflow request") as failure:
        workflow_request_ids_digest(request_ids)
    raw_values = (request_ids,) if isinstance(request_ids, str) else request_ids
    assert all(not value or value not in str(failure.value) for value in raw_values)


def test_safe_metadata_models_have_only_bounded_scalar_fields() -> None:
    values = (
        ingested_metadata(),
        MeetingTransitionMetadata(
            previous_status=MeetingStatus.INGESTED,
            current_status=MeetingStatus.TRANSCRIBING,
            meeting_version=1,
        ),
        ProcessingAttemptMetadata(
            stage=ProcessingStage.TRANSCRIPTION,
            attempt_number=1,
            outcome=ProcessingAuditOutcome.SUCCEEDED,
            input_digest="a" * 64,
            output_digest="b" * 64,
        ),
        ProcessingRetryRequestedMetadata(
            stage=ProcessingStage.TRANSCRIPTION,
            previous_attempt_count=1,
            meeting_version=3,
        ),
        specialist_metadata(),
        ReviewRevisionMetadata(
            revision_number=1,
            review_digest="c" * 64,
            origin=ReviewOrigin.MODEL,
            change_kind=ReviewChangeKind.MODEL_CREATED,
            decision_count=1,
            action_count=2,
            question_count=0,
            risk_count=1,
            issue_count=1,
            blocking_issue_count=0,
        ),
        ReviewApprovedMetadata(
            revision_number=1,
            review_digest="c" * 64,
            write_intent_count=2,
        ),
        DeliveryTransitionMetadata(
            change_kind=DeliveryChangeKind.STATUS_TRANSITION,
            write_kind=WriteKind.TASK,
            write_intent_digest=workflow_write_intent_digest(WRITE_INTENT_ID),
            previous_status=WriteStatus.PENDING,
            current_status=WriteStatus.IN_FLIGHT,
            attempt_count=1,
            reconciliation_count=0,
        ),
    )

    for metadata in values:
        assert all(
            value is None or isinstance(value, (str, int, bool))
            for value in metadata.model_dump(mode="json").values()
        )


@pytest.mark.parametrize(
    "updates",
    [
        {
            "outcome": ProcessingAuditOutcome.STARTED,
            "output_digest": "b" * 64,
        },
        {
            "outcome": ProcessingAuditOutcome.RETRY_SCHEDULED,
            "failure_code": FailureCode.RATE_LIMITED,
            "failure_disposition": FailureDisposition.PERMANENT,
            "retry_delay_ms": 30_000,
        },
        {
            "outcome": ProcessingAuditOutcome.FAILED,
            "failure_code": FailureCode.INTERNAL,
            "failure_disposition": FailureDisposition.RETRYABLE,
        },
    ],
)
def test_processing_metadata_rejects_inconsistent_outcomes(
    updates: dict[str, object],
) -> None:
    payload: dict[str, object] = {
        "stage": ProcessingStage.TRANSCRIPTION,
        "attempt_number": 1,
        "outcome": ProcessingAuditOutcome.STARTED,
        "input_digest": "a" * 64,
    }
    payload.update(updates)

    with pytest.raises(ValidationError):
        ProcessingAttemptMetadata.model_validate(payload)


def test_processing_success_can_omit_an_unavailable_output_digest() -> None:
    metadata = ProcessingAttemptMetadata(
        stage=ProcessingStage.TRANSCRIPTION,
        attempt_number=1,
        outcome=ProcessingAuditOutcome.SUCCEEDED,
        input_digest="a" * 64,
    )

    assert metadata.output_digest is None


def test_processing_attempt_can_omit_an_unavailable_input_digest() -> None:
    metadata = ProcessingAttemptMetadata(
        stage=ProcessingStage.TRANSCRIPTION,
        attempt_number=1,
        outcome=ProcessingAuditOutcome.STARTED,
    )

    assert metadata.input_digest is None


def test_review_events_bind_the_exact_immutable_review_digest() -> None:
    revision = ReviewRevisionMetadata(
        revision_number=1,
        review_digest="c" * 64,
        origin=ReviewOrigin.MODEL,
        change_kind=ReviewChangeKind.MODEL_CREATED,
        decision_count=1,
        action_count=1,
        question_count=0,
        risk_count=0,
        issue_count=0,
        blocking_issue_count=0,
    )
    approval = ReviewApprovedMetadata(
        revision_number=1,
        review_digest=revision.review_digest,
        write_intent_count=1,
    )

    assert revision.review_digest == approval.review_digest
    assert revision.review_digest in revision.model_dump_json()
    assert approval.review_digest in approval.model_dump_json()


def test_review_event_rejects_non_digest_content_without_echoing_it() -> None:
    marker = "private-review-content"

    with pytest.raises(ValidationError) as failure:
        ReviewApprovedMetadata(
            revision_number=1,
            review_digest=marker,
            write_intent_count=1,
        )

    assert marker not in str(failure.value)


@pytest.mark.parametrize(
    ("disposition", "retry_exhausted"),
    [
        (FailureDisposition.PERMANENT, False),
        (FailureDisposition.UNKNOWN_OUTCOME, False),
        (FailureDisposition.RETRYABLE, True),
    ],
)
def test_processing_terminal_failure_records_retry_exhaustion_explicitly(
    disposition: FailureDisposition,
    retry_exhausted: bool,
) -> None:
    metadata = ProcessingAttemptMetadata(
        stage=ProcessingStage.EXTRACTION,
        attempt_number=2,
        outcome=ProcessingAuditOutcome.FAILED,
        input_digest="a" * 64,
        failure_code=FailureCode.INTERNAL,
        failure_disposition=disposition,
        retry_exhausted=retry_exhausted,
    )

    assert metadata.failure_disposition is disposition
    assert metadata.retry_exhausted is retry_exhausted


@pytest.mark.parametrize(
    "updates",
    [
        {
            "outcome": ProcessingAuditOutcome.FAILED,
            "failure_disposition": FailureDisposition.RETRYABLE,
        },
        {
            "outcome": ProcessingAuditOutcome.FAILED,
            "failure_disposition": FailureDisposition.PERMANENT,
            "retry_exhausted": True,
        },
        {
            "outcome": ProcessingAuditOutcome.RETRY_SCHEDULED,
            "failure_disposition": FailureDisposition.RETRYABLE,
            "retry_delay_ms": 30_000,
            "retry_exhausted": True,
        },
    ],
)
def test_processing_retry_exhaustion_rejects_inconsistent_terminality(
    updates: dict[str, object],
) -> None:
    payload: dict[str, object] = {
        "stage": ProcessingStage.EXTRACTION,
        "attempt_number": 2,
        "outcome": ProcessingAuditOutcome.FAILED,
        "input_digest": "a" * 64,
        "failure_code": FailureCode.INTERNAL,
        "failure_disposition": FailureDisposition.PERMANENT,
    }
    payload.update(updates)

    with pytest.raises(ValidationError, match="exhaustion"):
        ProcessingAttemptMetadata.model_validate(payload)


def test_retry_delay_rounds_fractional_schedule_up_to_milliseconds() -> None:
    delay = timedelta(seconds=1, microseconds=1)
    milliseconds = workflow_retry_delay_ms(delay)

    assert milliseconds == 1_001
    assert milliseconds / 1_000 >= delay.total_seconds()
    assert workflow_retry_delay_ms(timedelta(0)) == 0


class NonFiniteTimedelta(timedelta):
    def total_seconds(self) -> float:
        return float("inf")


@pytest.mark.parametrize(
    "delay",
    [
        timedelta(microseconds=-1),
        timedelta(milliseconds=WORKFLOW_RETRY_DELAY_MAX_MS + 1),
        NonFiniteTimedelta(),
        True,
    ],
)
def test_retry_delay_rejects_invalid_values_without_echoing_them(
    delay: timedelta | bool,
) -> None:
    with pytest.raises(ValueError, match="finite bounded"):
        workflow_retry_delay_ms(cast(timedelta, delay))


@pytest.mark.parametrize(
    ("status", "disposition"),
    [
        (WriteStatus.RETRY_WAIT, FailureDisposition.PERMANENT),
        (WriteStatus.UNKNOWN, FailureDisposition.RETRYABLE),
        (WriteStatus.PERMANENT_FAILED, FailureDisposition.UNKNOWN_OUTCOME),
    ],
)
def test_delivery_metadata_binds_failure_disposition_to_status(
    status: WriteStatus,
    disposition: FailureDisposition,
) -> None:
    with pytest.raises(ValidationError):
        DeliveryTransitionMetadata(
            change_kind=DeliveryChangeKind.STATUS_TRANSITION,
            write_kind=WriteKind.TASK,
            write_intent_digest=workflow_write_intent_digest(WRITE_INTENT_ID),
            previous_status=WriteStatus.IN_FLIGHT,
            current_status=status,
            attempt_count=1,
            reconciliation_count=0,
            failure_code=FailureCode.INTERNAL,
            failure_disposition=disposition,
        )


def test_delivery_metadata_distinguishes_same_kind_intents_without_raw_ids() -> None:
    second_id = UUID("4864b05a-e479-47cc-a968-083a750b63d2")
    first = DeliveryTransitionMetadata(
        change_kind=DeliveryChangeKind.STATUS_TRANSITION,
        write_kind=WriteKind.TASK,
        write_intent_digest=workflow_write_intent_digest(WRITE_INTENT_ID),
        previous_status=WriteStatus.PENDING,
        current_status=WriteStatus.IN_FLIGHT,
        attempt_count=1,
        reconciliation_count=0,
    )
    second = DeliveryTransitionMetadata(
        change_kind=DeliveryChangeKind.STATUS_TRANSITION,
        write_kind=WriteKind.TASK,
        write_intent_digest=workflow_write_intent_digest(second_id),
        previous_status=WriteStatus.PENDING,
        current_status=WriteStatus.IN_FLIGHT,
        attempt_count=1,
        reconciliation_count=0,
    )
    dumped = first.model_dump_json() + second.model_dump_json()

    assert first.write_intent_digest != second.write_intent_digest
    assert first != second
    assert str(WRITE_INTENT_ID) not in dumped
    assert str(second_id) not in dumped


def test_delivery_audit_allows_only_creation_or_shared_graph_transitions() -> None:
    created = DeliveryTransitionMetadata(
        change_kind=DeliveryChangeKind.CREATED,
        write_kind=WriteKind.TASK,
        write_intent_digest=workflow_write_intent_digest(WRITE_INTENT_ID),
        current_status=WriteStatus.PENDING,
        attempt_count=0,
        reconciliation_count=0,
    )

    assert created.previous_status is None
    with pytest.raises(ValidationError, match="delivery transition"):
        DeliveryTransitionMetadata(
            change_kind=DeliveryChangeKind.STATUS_TRANSITION,
            write_kind=WriteKind.TASK,
            write_intent_digest=workflow_write_intent_digest(WRITE_INTENT_ID),
            previous_status=WriteStatus.PENDING,
            current_status=WriteStatus.SUCCEEDED,
            attempt_count=1,
            reconciliation_count=0,
        )
    with pytest.raises(ValidationError, match="delivery transition"):
        DeliveryTransitionMetadata(
            change_kind=DeliveryChangeKind.CREATED,
            write_kind=WriteKind.TASK,
            write_intent_digest=workflow_write_intent_digest(WRITE_INTENT_ID),
            current_status=WriteStatus.IN_FLIGHT,
            attempt_count=1,
            reconciliation_count=0,
        )


def test_delivery_audit_allows_unknown_reconciliation_refresh() -> None:
    refreshed = DeliveryTransitionMetadata(
        change_kind=DeliveryChangeKind.RECONCILIATION_REFRESH,
        write_kind=WriteKind.CALENDAR_EVENT,
        write_intent_digest=workflow_write_intent_digest(WRITE_INTENT_ID),
        previous_status=WriteStatus.UNKNOWN,
        current_status=WriteStatus.UNKNOWN,
        attempt_count=1,
        reconciliation_count=2,
        failure_code=FailureCode.UNKNOWN_REMOTE_OUTCOME,
        failure_disposition=FailureDisposition.UNKNOWN_OUTCOME,
    )

    assert refreshed.reconciliation_count == 2
    with pytest.raises(ValidationError, match="delivery transition"):
        DeliveryTransitionMetadata(
            change_kind=DeliveryChangeKind.RECONCILIATION_REFRESH,
            write_kind=WriteKind.CALENDAR_EVENT,
            write_intent_digest=workflow_write_intent_digest(WRITE_INTENT_ID),
            previous_status=WriteStatus.UNKNOWN,
            current_status=WriteStatus.UNKNOWN,
            attempt_count=1,
            reconciliation_count=0,
            failure_code=FailureCode.UNKNOWN_REMOTE_OUTCOME,
            failure_disposition=FailureDisposition.UNKNOWN_OUTCOME,
        )


@pytest.mark.parametrize(
    "updates",
    [
        {"attempt_count": 0},
        {"reconciliation_count": 1},
        {
            "previous_status": WriteStatus.IN_FLIGHT,
            "current_status": WriteStatus.UNKNOWN,
            "reconciliation_count": 1,
            "failure_code": FailureCode.UNKNOWN_REMOTE_OUTCOME,
            "failure_disposition": FailureDisposition.UNKNOWN_OUTCOME,
        },
    ],
)
def test_delivery_audit_rejects_impossible_counters(updates: dict[str, object]) -> None:
    payload: dict[str, object] = {
        "change_kind": DeliveryChangeKind.STATUS_TRANSITION,
        "write_kind": WriteKind.TASK,
        "write_intent_digest": workflow_write_intent_digest(WRITE_INTENT_ID),
        "previous_status": WriteStatus.PENDING,
        "current_status": WriteStatus.IN_FLIGHT,
        "attempt_count": 1,
        "reconciliation_count": 0,
    }
    payload.update(updates)

    with pytest.raises(ValidationError, match="count"):
        DeliveryTransitionMetadata.model_validate(payload)


def test_write_intent_digest_rejects_raw_identifiers_without_echoing_them() -> None:
    raw_id = str(WRITE_INTENT_ID)

    with pytest.raises(ValueError, match="Workflow write intent") as failure:
        workflow_write_intent_digest(cast(UUID, raw_id))

    assert raw_id not in str(failure.value)
    assert failure.value.__cause__ is None
    assert failure.value.__context__ is None
