from datetime import date, datetime, timezone
from uuid import UUID

import pytest

from meeting_action_orchestrator.domain import (
    ActionItem,
    CalendarEventProposal,
    ConnectorTarget,
    DateDeadline,
    DeadlineResolution,
    DeliveryDirective,
    DomainInvariantError,
    EvidenceRef,
    IdempotencyConflictError,
    IssueSeverity,
    Meeting,
    MeetingStatus,
    OpenQuestion,
    ReviewIssue,
    ReviewOrigin,
    ReviewRevision,
    Risk,
    TaskProposal,
    Transcript,
    TranscriptSegment,
    WriteKind,
    WriteReceipt,
    approve_review,
    create_recap_artifact,
    project_write_intents,
    validate_review_evidence,
    validate_write_receipt,
)

NOW = datetime(2026, 6, 7, 9, 0, tzinfo=timezone.utc)
TRANSCRIPT_ID = UUID(int=10)
REVIEW_ID = UUID(int=30)
EXPECTED_INTENT_COUNT = 2


def uid(value: int) -> UUID:
    return UUID(int=value)


def make_meeting() -> Meeting:
    return Meeting(
        id=uid(1),
        ingest_key="upload-1",
        title="Launch planning",
        audio_asset_id=uid(2),
        occurred_at=NOW,
        timezone="Asia/Calcutta",
        status=MeetingStatus.AWAITING_APPROVAL,
        current_transcript_id=TRANSCRIPT_ID,
        current_review_id=REVIEW_ID,
        created_at=NOW,
        updated_at=NOW,
    )


def make_review(*, issues: tuple[ReviewIssue, ...] = ()) -> ReviewRevision:
    action = ActionItem(
        id=uid(20),
        title="Send launch brief",
        description="Send the approved brief to the launch team.",
        deadline=DateDeadline(
            value=date(2026, 6, 12),
            timezone="Asia/Calcutta",
            source_text="by Friday",
            resolution=DeadlineResolution.RELATIVE_TO_MEETING,
        ),
        confidence=0.94,
        evidence=(EvidenceRef(segment_ids=(uid(11),), quote="send the launch brief"),),
    )
    directive = DeliveryDirective(
        action_item_id=action.id,
        task_target=ConnectorTarget(connector_id="tasks", resource_id="inbox"),
        create_calendar_event=True,
        calendar_target=ConnectorTarget(connector_id="calendar", resource_id="primary"),
    )
    return ReviewRevision(
        id=REVIEW_ID,
        meeting_id=uid(1),
        transcript_id=TRANSCRIPT_ID,
        revision_number=1,
        origin=ReviewOrigin.MODEL,
        purpose="Prepare the launch team",
        recap_markdown="# Launch planning\n\nThe launch brief is due Friday.",
        action_items=(action,),
        open_questions=(
            OpenQuestion(
                id=uid(22),
                question="Who approves the final brief?",
                evidence=(EvidenceRef(segment_ids=(uid(11),), quote="launch brief"),),
            ),
        ),
        risks=(
            Risk(
                id=uid(23),
                description="Approval could miss the launch window.",
                evidence=(EvidenceRef(segment_ids=(uid(11),), quote="by Friday"),),
            ),
        ),
        issues=issues,
        directives=(directive,),
        created_at=NOW,
    )


def make_transcript() -> Transcript:
    text = "Mira will send the launch brief by Friday."
    return Transcript(
        id=TRANSCRIPT_ID,
        meeting_id=uid(1),
        audio_asset_id=uid(2),
        provider="openai",
        model="gpt-4o-mini-transcribe",
        language="en",
        text=text,
        segments=(
            TranscriptSegment(
                id=uid(11),
                ordinal=0,
                start_ms=0,
                end_ms=2_000,
                speaker="Mira",
                text=text,
            ),
        ),
        created_at=NOW,
    )


def test_approval_binds_current_review_digest() -> None:
    meeting = make_meeting()
    review = make_review()

    approval = approve_review(
        approval_id=uid(40),
        meeting=meeting,
        review=review,
        transcript=make_transcript(),
        request_key="approval-request-1",
        actor_id="shubham",
        approved_at=NOW,
    )

    assert approval.review_revision_id == review.id
    assert approval.review_digest == review.content_digest


def test_approval_rejects_stale_revision() -> None:
    meeting = make_meeting()
    review = make_review().model_copy(update={"id": uid(31)})

    with pytest.raises(DomainInvariantError, match="not the current meeting revision"):
        approve_review(
            approval_id=uid(40),
            meeting=meeting,
            review=review,
            transcript=make_transcript(),
            request_key="approval-request-1",
            actor_id="shubham",
            approved_at=NOW,
        )


def test_approval_rejects_open_blocking_issue() -> None:
    blocker = ReviewIssue(
        id=uid(50),
        item_id=uid(20),
        field="deadline",
        severity=IssueSeverity.BLOCKING,
        message="Deadline is ambiguous",
    )

    with pytest.raises(DomainInvariantError, match="unresolved blocking issues"):
        approve_review(
            approval_id=uid(40),
            meeting=make_meeting(),
            review=make_review(issues=(blocker,)),
            transcript=make_transcript(),
            request_key="approval-request-1",
            actor_id="shubham",
            approved_at=NOW,
        )


def test_projection_creates_deterministic_task_and_calendar_intents() -> None:
    meeting = make_meeting()
    review = make_review()
    approval = approve_review(
        approval_id=uid(40),
        meeting=meeting,
        review=review,
        transcript=make_transcript(),
        request_key="approval-request-1",
        actor_id="shubham",
        approved_at=NOW,
    )

    first = project_write_intents(
        meeting=meeting,
        review=review,
        approval=approval,
        created_at=NOW,
    )
    second = project_write_intents(
        meeting=meeting,
        review=review,
        approval=approval,
        created_at=NOW,
    )

    assert first == second
    assert len(first) == EXPECTED_INTENT_COUNT
    assert len({intent.idempotency_key for intent in first}) == EXPECTED_INTENT_COUNT
    assert isinstance(first[0].proposal, TaskProposal)
    assert isinstance(first[1].proposal, CalendarEventProposal)
    assert first[0].proposal.kind is WriteKind.TASK
    assert first[1].proposal.kind is WriteKind.CALENDAR_EVENT


def test_recap_artifact_copies_the_approved_markdown() -> None:
    meeting = make_meeting()
    review = make_review()
    approval = approve_review(
        approval_id=uid(40),
        meeting=meeting,
        review=review,
        transcript=make_transcript(),
        request_key="approval-request-1",
        actor_id="shubham",
        approved_at=NOW,
    )

    recap = create_recap_artifact(
        artifact_id=uid(41),
        meeting=meeting,
        review=review,
        approval=approval,
        created_at=NOW,
    )

    assert recap.content == review.recap_markdown


def test_projection_rejects_approval_for_another_digest() -> None:
    meeting = make_meeting()
    review = make_review()
    approval = approve_review(
        approval_id=uid(40),
        meeting=meeting,
        review=review,
        transcript=make_transcript(),
        request_key="approval-request-1",
        actor_id="shubham",
        approved_at=NOW,
    ).model_copy(update={"review_digest": "f" * 64})

    with pytest.raises(DomainInvariantError, match="digest does not match"):
        project_write_intents(
            meeting=meeting,
            review=review,
            approval=approval,
            created_at=NOW,
        )


def test_receipt_must_match_intent_idempotency_binding() -> None:
    meeting = make_meeting()
    review = make_review()
    approval = approve_review(
        approval_id=uid(40),
        meeting=meeting,
        review=review,
        transcript=make_transcript(),
        request_key="approval-request-1",
        actor_id="shubham",
        approved_at=NOW,
    )
    intent = project_write_intents(
        meeting=meeting,
        review=review,
        approval=approval,
        created_at=NOW,
    )[0]
    receipt = WriteReceipt(
        id=uid(70),
        intent_id=intent.id,
        idempotency_key=intent.idempotency_key,
        payload_digest=intent.payload_digest,
        provider="mcp",
        external_id="task-1",
        recorded_at=NOW,
    )

    validate_write_receipt(intent, receipt)
    conflicting = receipt.model_copy(update={"payload_digest": "f" * 64})
    with pytest.raises(IdempotencyConflictError):
        validate_write_receipt(intent, conflicting)


def test_review_evidence_rejects_cross_record_and_unknown_segment_references() -> None:
    review = make_review()
    transcript = make_transcript()

    with pytest.raises(DomainInvariantError, match="review and transcript do not match"):
        validate_review_evidence(
            review.model_copy(update={"meeting_id": uid(999)}),
            transcript,
        )

    action = review.action_items[0].model_copy(
        update={"evidence": (EvidenceRef(segment_ids=(uid(999),), quote="launch brief"),)}
    )
    with pytest.raises(DomainInvariantError, match="unknown segment"):
        validate_review_evidence(
            review.model_copy(update={"action_items": (action,)}),
            transcript,
        )


def test_approval_requires_an_approvable_meeting_and_current_transcript() -> None:
    review = make_review()
    transcript = make_transcript()

    with pytest.raises(DomainInvariantError, match="not awaiting approval"):
        approve_review(
            approval_id=uid(40),
            meeting=make_meeting().model_copy(update={"status": MeetingStatus.TRANSCRIBED}),
            review=review,
            transcript=transcript,
            request_key="approval-request-1",
            actor_id="shubham",
            approved_at=NOW,
        )

    with pytest.raises(DomainInvariantError, match="current transcript"):
        approve_review(
            approval_id=uid(40),
            meeting=make_meeting().model_copy(update={"current_transcript_id": uid(999)}),
            review=review,
            transcript=transcript,
            request_key="approval-request-1",
            actor_id="shubham",
            approved_at=NOW,
        )


def test_recap_rejects_approval_identity_and_digest_mismatches() -> None:
    meeting = make_meeting()
    review = make_review()
    approval = approve_review(
        approval_id=uid(40),
        meeting=meeting,
        review=review,
        transcript=make_transcript(),
        request_key="approval-request-1",
        actor_id="shubham",
        approved_at=NOW,
    )

    with pytest.raises(DomainInvariantError, match="approval does not match"):
        create_recap_artifact(
            artifact_id=uid(41),
            meeting=meeting,
            review=review,
            approval=approval.model_copy(update={"meeting_id": uid(999)}),
            created_at=NOW,
        )
    with pytest.raises(DomainInvariantError, match="approval does not match"):
        create_recap_artifact(
            artifact_id=uid(41),
            meeting=meeting,
            review=review,
            approval=approval.model_copy(update={"review_digest": "f" * 64}),
            created_at=NOW,
        )


def test_projection_fails_closed_on_incomplete_persisted_delivery_bindings() -> None:
    meeting = make_meeting()
    review = make_review()
    approval = approve_review(
        approval_id=uid(40),
        meeting=meeting,
        review=review,
        transcript=make_transcript(),
        request_key="approval-request-1",
        actor_id="shubham",
        approved_at=NOW,
    )
    directive = review.directives[0]

    with pytest.raises(DomainInvariantError, match="approval does not match"):
        project_write_intents(
            meeting=meeting,
            review=review,
            approval=approval.model_copy(update={"review_revision_id": uid(999)}),
            created_at=NOW,
        )

    missing_task_target = review.model_copy(
        update={"directives": (directive.model_copy(update={"task_target": None}),)}
    )
    with pytest.raises(DomainInvariantError, match="task target is missing"):
        project_write_intents(
            meeting=meeting,
            review=missing_task_target,
            approval=approval,
            created_at=NOW,
        )

    missing_calendar_target = review.model_copy(
        update={
            "directives": (
                directive.model_copy(
                    update={
                        "create_task": False,
                        "task_target": None,
                        "calendar_target": None,
                    }
                ),
            )
        }
    )
    with pytest.raises(DomainInvariantError, match="calendar details are incomplete"):
        project_write_intents(
            meeting=meeting,
            review=missing_calendar_target,
            approval=approval,
            created_at=NOW,
        )

    action_without_description = review.action_items[0].model_copy(update={"description": None})
    fallback_review = review.model_copy(update={"action_items": (action_without_description,)})
    fallback = project_write_intents(
        meeting=meeting,
        review=fallback_review,
        approval=approval,
        created_at=NOW,
    )
    assert all(
        intent.proposal.description == f"Follow-up from {meeting.title}" for intent in fallback
    )


def test_receipt_rejects_another_intent_identity_before_payload_validation() -> None:
    meeting = make_meeting()
    review = make_review()
    approval = approve_review(
        approval_id=uid(40),
        meeting=meeting,
        review=review,
        transcript=make_transcript(),
        request_key="approval-request-1",
        actor_id="shubham",
        approved_at=NOW,
    )
    intent = project_write_intents(
        meeting=meeting,
        review=review,
        approval=approval,
        created_at=NOW,
    )[0]
    receipt = WriteReceipt(
        id=uid(70),
        intent_id=uid(999),
        idempotency_key=intent.idempotency_key,
        payload_digest=intent.payload_digest,
        provider="mcp",
        external_id="task-1",
        recorded_at=NOW,
    )

    with pytest.raises(DomainInvariantError, match="does not reference its intent"):
        validate_write_receipt(intent, receipt)
