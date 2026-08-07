from __future__ import annotations

from datetime import date, datetime, timezone
from uuid import UUID

from meeting_action_orchestrator.agents.contracts import (
    ActionItemCandidate,
    DecisionCandidate,
    EvidenceRef,
    MeetingExtraction,
    ParticipantCandidate,
    RecapDraft,
    VerificationFinding,
    VerificationReport,
)
from meeting_action_orchestrator.application.mapping import (
    DeliveryTargets,
    build_canonical_record,
    map_review_package,
    render_recap,
)
from meeting_action_orchestrator.application.reviewing import (
    ActionEdit,
    IssueResolutionEdit,
    revise_action,
    revise_issue,
)
from meeting_action_orchestrator.domain.enums import (
    IssueSeverity,
    IssueStatus,
    MeetingStatus,
    ReviewOrigin,
)
from meeting_action_orchestrator.domain.models import (
    ConnectorTarget,
    Meeting,
    Transcript,
    TranscriptSegment,
)

NOW = datetime(2026, 8, 7, 8, 0, tzinfo=timezone.utc)
MEETING_ID = UUID("d4a91665-e12c-46da-a548-ed261aec64aa")
ASSET_ID = UUID("ad7fe858-792a-4840-8407-edb595f30ccd")
TRANSCRIPT_ID = UUID("5f6d41da-6634-4092-aed2-bbf9c855f950")
SEGMENT_ID = UUID("20ab16a4-a206-4eb7-84d2-696423b8fbb6")


def meeting() -> Meeting:
    return Meeting(
        id=MEETING_ID,
        ingest_key="mapping-test",
        title="Release planning",
        audio_asset_id=ASSET_ID,
        occurred_at=NOW,
        timezone="Asia/Calcutta",
        status=MeetingStatus.TRANSCRIBED,
        current_transcript_id=TRANSCRIPT_ID,
        created_at=NOW,
        updated_at=NOW,
    )


def transcript() -> Transcript:
    text = "Mira approved the release plan. Dev will publish the brief by Friday."
    return Transcript(
        id=TRANSCRIPT_ID,
        meeting_id=MEETING_ID,
        audio_asset_id=ASSET_ID,
        provider="openai",
        model="transcribe-test",
        language="en",
        text=text,
        segments=(
            TranscriptSegment(
                id=SEGMENT_ID,
                ordinal=0,
                start_ms=0,
                end_ms=4000,
                speaker="Mira",
                text=text,
            ),
        ),
        created_at=NOW,
    )


def extraction(due_expression: str | None = "2026-08-14") -> MeetingExtraction:
    evidence = EvidenceRef(segment_id=str(SEGMENT_ID), quote="approved the release plan")
    action_evidence = EvidenceRef(segment_id=str(SEGMENT_ID), quote="publish the brief by Friday")
    return MeetingExtraction(
        suggested_title="Release planning",
        purpose="Confirm the release plan",
        participants=[
            ParticipantCandidate(
                display_name="Mira",
                speaker_labels=["Mira"],
                evidence=[evidence],
            )
        ],
        decisions=[
            DecisionCandidate(
                statement="Approve the release plan",
                owner="Mira",
                rationale=None,
                confidence="high",
                evidence=[evidence],
            )
        ],
        action_items=[
            ActionItemCandidate(
                description="Publish the brief",
                owner="Dev",
                due_expression=due_expression,
                dependency=None,
                confidence="high",
                requires_clarification=False,
                evidence=[action_evidence],
            )
        ],
        open_questions=[],
        risks=[],
        warnings=[],
    )


def test_mapping_creates_digest_bound_review_and_delivery_directives() -> None:
    source = extraction()
    record = build_canonical_record(meeting(), source)
    recap = RecapDraft(
        title="Release planning", overview="The release plan is ready.", highlights=[]
    )
    markdown = render_recap(meeting(), source, record, recap)

    package = map_review_package(
        meeting=meeting(),
        transcript=transcript(),
        extraction=source,
        recap_markdown=markdown,
        verification=VerificationReport(verdict="pass", findings=[]),
        targets=DeliveryTargets(
            task=ConnectorTarget(connector_id="tasks", resource_id="inbox"),
            calendar=ConnectorTarget(connector_id="calendar", resource_id="primary"),
        ),
        created_at=NOW,
    )

    assert package.review.content_digest
    assert package.review.recap_markdown == markdown
    assert package.participants[0].display_name == "Mira"
    assert package.review.directives[0].create_task is True
    assert package.review.directives[0].create_calendar_event is True
    assert package.review.issues == ()


def test_relative_deadline_requires_human_resolution() -> None:
    source = extraction("by Friday")
    record = build_canonical_record(meeting(), source)
    recap = RecapDraft(
        title="Release planning", overview="The release plan is ready.", highlights=[]
    )

    package = map_review_package(
        meeting=meeting(),
        transcript=transcript(),
        extraction=source,
        recap_markdown=render_recap(meeting(), source, record, recap),
        verification=VerificationReport(verdict="pass", findings=[]),
        targets=DeliveryTargets(
            task=ConnectorTarget(connector_id="tasks", resource_id="inbox"),
            calendar=ConnectorTarget(connector_id="calendar", resource_id="primary"),
        ),
        created_at=NOW,
    )

    assert package.review.action_items[0].deadline is None
    assert package.review.directives[0].create_calendar_event is False
    assert any(issue.severity is IssueSeverity.BLOCKING for issue in package.review.issues)


def test_verifier_reference_outside_review_does_not_break_mapping() -> None:
    source = extraction()
    record = build_canonical_record(meeting(), source)
    recap = RecapDraft(
        title="Release planning", overview="The release plan is ready.", highlights=[]
    )
    finding = VerificationFinding(
        severity="warning",
        code="invalid_reference",
        subject_id=str(UUID("49d8a2d9-cce3-4a17-8bc5-e620913b0c8f")),
        message="The referenced item is not present",
        evidence=[],
    )

    package = map_review_package(
        meeting=meeting(),
        transcript=transcript(),
        extraction=source,
        recap_markdown=render_recap(meeting(), source, record, recap),
        verification=VerificationReport(verdict="pass", findings=[finding]),
        targets=DeliveryTargets(),
        created_at=NOW,
    )

    assert package.review.issues[0].item_id is None


def test_action_edit_preserves_recap_and_does_not_close_blocker() -> None:
    source = extraction("by Friday")
    record = build_canonical_record(meeting(), source)
    recap = RecapDraft(
        title="Release planning", overview="The release plan is ready.", highlights=[]
    )
    review = map_review_package(
        meeting=meeting(),
        transcript=transcript(),
        extraction=source,
        recap_markdown=render_recap(meeting(), source, record, recap),
        verification=VerificationReport(verdict="pass", findings=[]),
        targets=DeliveryTargets(),
        created_at=NOW,
    ).review

    revised = revise_action(
        review=review,
        edit=ActionEdit(
            action_id=review.action_items[0].id,
            title="Publish the final brief",
            owner="Dev",
            due_date=date(2026, 8, 14),
            due_time=None,
            timezone="Asia/Calcutta",
            notes="Publish after legal review.",
        ),
        revision_id=UUID("fe7c7822-45b5-4c39-b2c9-7cfb245424f6"),
        actor_id="reviewer",
        created_at=NOW,
    )

    assert revised.recap_markdown == review.recap_markdown
    assert revised.issues[0].status is IssueStatus.OPEN
    assert revised.origin is ReviewOrigin.HUMAN


def test_issue_resolution_requires_an_explicit_revision() -> None:
    source = extraction("by Friday")
    record = build_canonical_record(meeting(), source)
    recap = RecapDraft(
        title="Release planning", overview="The release plan is ready.", highlights=[]
    )
    review = map_review_package(
        meeting=meeting(),
        transcript=transcript(),
        extraction=source,
        recap_markdown=render_recap(meeting(), source, record, recap),
        verification=VerificationReport(verdict="pass", findings=[]),
        targets=DeliveryTargets(),
        created_at=NOW,
    ).review

    revised = revise_issue(
        review=review,
        edit=IssueResolutionEdit(
            issue_id=review.issues[0].id,
            status=IssueStatus.RESOLVED,
            resolution_note="Confirmed as 14 August with the assignee.",
        ),
        revision_id=UUID("234e83f0-3f3d-436d-8844-ad9748629272"),
        actor_id="reviewer",
        created_at=NOW,
    )

    assert revised.issues[0].status is IssueStatus.RESOLVED
    assert revised.issues[0].resolution_note == "Confirmed as 14 August with the assignee."
    assert revised.content_digest != review.content_digest
