from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from uuid import UUID

import pytest

from meeting_action_orchestrator.agents.contracts import (
    ActionItemCandidate,
    DecisionCandidate,
    EvidenceRef,
    MeetingExtraction,
    OpenQuestionCandidate,
    ParticipantCandidate,
    RecapDraft,
    ReferencedText,
    RiskCandidate,
    VerificationFinding,
    VerificationReport,
)
from meeting_action_orchestrator.application.mapping import (
    DeliveryTargets,
    build_canonical_record,
    build_extraction_request,
    calendar_proposal,
    map_review_package,
    map_transcription,
    render_recap,
    task_proposal,
)
from meeting_action_orchestrator.application.ports import TranscriptionSegmentLike
from meeting_action_orchestrator.application.reviewing import (
    ActionEdit,
    IssueResolutionEdit,
    revise_action,
    revise_delivery,
    revise_issue,
)
from meeting_action_orchestrator.domain.enums import (
    IssueSeverity,
    IssueStatus,
    MeetingStatus,
    ReviewOrigin,
    WriteKind,
)
from meeting_action_orchestrator.domain.models import (
    CalendarEventProposal,
    ConnectorTarget,
    DateTimeDeadline,
    Meeting,
    TaskProposal,
    Transcript,
    TranscriptSegment,
)
from meeting_action_orchestrator.domain.provider_budget import ProviderUsage

NOW = datetime(2026, 8, 7, 8, 0, tzinfo=timezone.utc)
MEETING_ID = UUID("d4a91665-e12c-46da-a548-ed261aec64aa")
ASSET_ID = UUID("ad7fe858-792a-4840-8407-edb595f30ccd")
TRANSCRIPT_ID = UUID("5f6d41da-6634-4092-aed2-bbf9c855f950")
SEGMENT_ID = UUID("20ab16a4-a206-4eb7-84d2-696423b8fbb6")


@dataclass(frozen=True, slots=True)
class StubTranscriptionSegment:
    id: str
    start_ms: int
    end_ms: int | None
    speaker: str | None
    text: str


@dataclass(frozen=True, slots=True)
class StubTranscriptionOutput:
    model: str
    provider_request_id: str | None
    language: str | None
    text: str
    duration_seconds: float | None
    segments: tuple[TranscriptionSegmentLike, ...]
    usage: ProviderUsage | None = None


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


def test_transcription_mapping_normalizes_segment_bounds_and_builds_agent_input() -> None:
    output = StubTranscriptionOutput(
        model="transcribe-test",
        provider_request_id="provider-request",
        language=None,
        text="First statement. Second statement.",
        duration_seconds=2.0,
        segments=(
            StubTranscriptionSegment("first", 0, None, "Mira", "First statement."),
            StubTranscriptionSegment("second", 2_000, 2_000, None, "Second statement."),
        ),
    )

    mapped = map_transcription(meeting(), output, NOW)
    request = build_extraction_request(meeting(), mapped)

    assert mapped.language == "und"
    assert mapped.provider_request_id == "provider-request"
    assert [segment.end_ms for segment in mapped.segments] == [2_000, 2_001]
    assert request.meeting_started_at == NOW
    assert request.transcript.sha256 == mapped.sha256
    assert [segment.speaker for segment in request.transcript.segments] == ["Mira", None]

    missing_time = meeting().model_copy(update={"occurred_at": None})
    with pytest.raises(ValueError, match="Meeting time is required"):
        build_extraction_request(missing_time, mapped)


def test_rich_mapping_preserves_all_item_kinds_and_surfaces_review_work() -> None:
    evidence = EvidenceRef(segment_id=str(SEGMENT_ID), quote="approved the release plan")
    source = MeetingExtraction(
        suggested_title="Release planning",
        purpose="Confirm | " + "p" * 1_000,
        participants=[
            ParticipantCandidate(display_name="Mira", speaker_labels=["Mira"], evidence=[evidence]),
            ParticipantCandidate(display_name="Mira", speaker_labels=["M"], evidence=[evidence]),
            ParticipantCandidate(
                display_name=None, speaker_labels=["Unknown"], evidence=[evidence]
            ),
        ],
        decisions=[
            DecisionCandidate(
                statement="Approve the release | plan",
                owner="Mira",
                rationale="The launch window is fixed.",
                confidence="medium",
                evidence=[evidence],
            )
        ],
        action_items=[
            ActionItemCandidate(
                description="Publish the brief | externally",
                owner=None,
                due_expression="2026-08-14T09:30:00",
                dependency="Legal approval",
                confidence="low",
                requires_clarification=True,
                evidence=[evidence],
            )
        ],
        open_questions=[
            OpenQuestionCandidate(
                question="Who confirms distribution?",
                owner="Mira",
                evidence=[evidence],
            )
        ],
        risks=[
            RiskCandidate(
                description="Legal review may slip.",
                owner=None,
                evidence=[evidence],
            )
        ],
        warnings=["w" * 1_005],
    )
    record = build_canonical_record(meeting(), source)
    recap = RecapDraft(
        title="Release | planning",
        overview="The release | plan is ready.",
        highlights=[ReferencedText(text="Launch | approved", source_ids=[record.items[0].id])],
    )
    markdown = render_recap(meeting(), source, record, recap)
    findings = [
        VerificationFinding(
            severity="warning",
            code="wrong_owner",
            subject_id=record.items[0].id,
            message="Confirm the recorded owner",
            evidence=[evidence],
        ),
        VerificationFinding(
            severity="warning",
            code="invalid_reference",
            subject_id="not-a-uuid",
            message="Ignore an invalid item reference",
            evidence=[],
        ),
        VerificationFinding(
            severity="warning",
            code="unsupported_claim",
            subject_id=None,
            message="Confirm the recap wording",
            evidence=[],
        ),
    ]

    package = map_review_package(
        meeting=meeting(),
        transcript=transcript(),
        extraction=source,
        recap_markdown=markdown,
        verification=VerificationReport(verdict="review_required", findings=findings),
        targets=DeliveryTargets(
            task=ConnectorTarget(connector_id="tasks", resource_id="inbox"),
            calendar=ConnectorTarget(connector_id="calendar", resource_id="primary"),
        ),
        created_at=NOW,
    )

    assert [item.kind for item in record.items] == [
        "decision",
        "action_item",
        "open_question",
        "risk",
    ]
    assert "Release \\| planning" in markdown
    assert "## Highlights" in markdown
    assert "## Review notes" in markdown
    assert tuple(person.display_name for person in package.participants) == ("Mira",)
    assert package.review.decisions[0].detail == (
        "Approve the release | plan\n\nThe launch window is fixed."
    )
    action = package.review.action_items[0]
    assert isinstance(action.deadline, DateTimeDeadline)
    assert action.deadline.at.utcoffset() is not None
    assert action.description is not None
    assert "Dependency: Legal approval" in action.description
    assert package.review.directives[0].create_calendar_event
    assert package.review.purpose is not None
    assert package.review.purpose.endswith("...")
    assert any(issue.field == "verification" for issue in package.review.issues)
    assert any(issue.item_id == package.review.decisions[0].id for issue in package.review.issues)
    assert sum(issue.severity is IssueSeverity.BLOCKING for issue in package.review.issues) >= 2


def test_delivery_proposal_helpers_handle_enabled_disabled_and_incomplete_directives() -> None:
    source = extraction()
    review = map_review_package(
        meeting=meeting(),
        transcript=transcript(),
        extraction=source,
        recap_markdown="# Release planning",
        verification=VerificationReport(verdict="pass", findings=[]),
        targets=DeliveryTargets(
            task=ConnectorTarget(connector_id="tasks", resource_id="inbox"),
            calendar=ConnectorTarget(connector_id="calendar", resource_id="primary"),
        ),
        created_at=NOW,
    ).review
    action_id = review.action_items[0].id

    assert isinstance(task_proposal(review, action_id), TaskProposal)
    assert isinstance(calendar_proposal(review, action_id), CalendarEventProposal)
    assert task_proposal(review, UUID(int=999)) is None
    assert calendar_proposal(review, UUID(int=999)) is None

    disabled = map_review_package(
        meeting=meeting(),
        transcript=transcript(),
        extraction=source,
        recap_markdown="# Release planning",
        verification=VerificationReport(verdict="pass", findings=[]),
        targets=DeliveryTargets(),
        created_at=NOW,
    ).review
    assert task_proposal(disabled, action_id) is None
    assert calendar_proposal(disabled, action_id) is None

    directive = review.directives[0]
    missing_task_target = review.model_copy(
        update={"directives": (directive.model_copy(update={"task_target": None}),)}
    )
    assert task_proposal(missing_task_target, action_id) is None
    missing_calendar_target = review.model_copy(
        update={"directives": (directive.model_copy(update={"calendar_target": None}),)}
    )
    assert calendar_proposal(missing_calendar_target, action_id) is None


@pytest.mark.parametrize(
    ("due_expression", "has_deadline", "has_deadline_issue"),
    [
        (None, False, False),
        ("2026-02-30", False, True),
        ("2026-08-14T09:30:00+05:30", True, False),
    ],
)
def test_deadline_mapping_handles_absent_invalid_and_offset_aware_values(
    due_expression: str | None,
    has_deadline: bool,
    has_deadline_issue: bool,
) -> None:
    review = map_review_package(
        meeting=meeting(),
        transcript=transcript(),
        extraction=extraction(due_expression),
        recap_markdown="# Release planning",
        verification=VerificationReport(verdict="pass", findings=[]),
        targets=DeliveryTargets(),
        created_at=NOW,
    ).review

    assert (review.action_items[0].deadline is not None) is has_deadline
    assert any(issue.field == "deadline" for issue in review.issues) is has_deadline_issue


def test_recap_omits_sections_that_have_no_source_content() -> None:
    source = extraction().model_copy(update={"purpose": None, "action_items": []})
    record = build_canonical_record(meeting(), source)
    markdown = render_recap(
        meeting(),
        source,
        record,
        RecapDraft(title="Release planning", overview="Decision only", highlights=[]),
    )

    assert "## Purpose" not in markdown
    assert "## Action items" not in markdown


def test_review_edits_reconcile_deadlines_and_delivery_targets() -> None:
    source = extraction()
    targets = DeliveryTargets(
        task=ConnectorTarget(connector_id="tasks", resource_id="inbox"),
        calendar=ConnectorTarget(connector_id="calendar", resource_id="primary"),
    )
    review = map_review_package(
        meeting=meeting(),
        transcript=transcript(),
        extraction=source,
        recap_markdown="# Release planning",
        verification=VerificationReport(verdict="pass", findings=[]),
        targets=targets,
        created_at=NOW,
    ).review
    action_id = review.action_items[0].id

    without_deadline = revise_action(
        review=review,
        edit=ActionEdit(
            action_id=action_id,
            title="Publish the brief",
            owner=None,
            due_date=None,
            due_time=None,
            timezone="Asia/Calcutta",
            notes=None,
            recap_markdown="# Human recap",
        ),
        revision_id=UUID(int=101),
        actor_id="reviewer",
        created_at=NOW,
    )
    assert without_deadline.recap_markdown == "# Human recap"
    assert not without_deadline.directives[0].create_calendar_event

    with_time = revise_action(
        review=review,
        edit=ActionEdit(
            action_id=action_id,
            title="Publish the brief",
            owner="Dev",
            due_date=date(2026, 8, 14),
            due_time=time(9, 30),
            timezone="Asia/Calcutta",
            notes="After legal approval",
        ),
        revision_id=UUID(int=102),
        actor_id="reviewer",
        created_at=NOW,
    )
    assert isinstance(with_time.action_items[0].deadline, DateTimeDeadline)

    task_disabled = revise_delivery(
        review=review,
        action_id=action_id,
        kind=WriteKind.TASK,
        enabled=False,
        targets=targets,
        revision_id=UUID(int=103),
        actor_id="reviewer",
        created_at=NOW,
    )
    assert not task_disabled.directives[0].create_task
    calendar_disabled = revise_delivery(
        review=review,
        action_id=action_id,
        kind=WriteKind.CALENDAR_EVENT,
        enabled=False,
        targets=targets,
        revision_id=UUID(int=104),
        actor_id="reviewer",
        created_at=NOW,
    )
    assert not calendar_disabled.directives[0].create_calendar_event


@pytest.mark.parametrize(
    ("due_date", "due_time", "timezone_name", "message"),
    [
        (None, time(9), "UTC", "due time requires a due date"),
        (date(2026, 3, 8), time(2, 30), "America/New_York", "does not exist"),
        (date(2026, 11, 1), time(1, 30), "America/New_York", "ambiguous"),
    ],
)
def test_action_revision_rejects_invalid_local_deadlines(
    due_date: date | None,
    due_time: time,
    timezone_name: str,
    message: str,
) -> None:
    review = map_review_package(
        meeting=meeting(),
        transcript=transcript(),
        extraction=extraction(),
        recap_markdown="# Release planning",
        verification=VerificationReport(verdict="pass", findings=[]),
        targets=DeliveryTargets(),
        created_at=NOW,
    ).review

    with pytest.raises(ValueError, match=message):
        revise_action(
            review=review,
            edit=ActionEdit(
                action_id=review.action_items[0].id,
                title="Publish the brief",
                owner="Dev",
                due_date=due_date,
                due_time=due_time,
                timezone=timezone_name,
                notes=None,
            ),
            revision_id=UUID(int=105),
            actor_id="reviewer",
            created_at=NOW,
        )


def test_review_revisions_reject_missing_or_closed_targets() -> None:
    review = map_review_package(
        meeting=meeting(),
        transcript=transcript(),
        extraction=extraction("by Friday"),
        recap_markdown="# Release planning",
        verification=VerificationReport(verdict="pass", findings=[]),
        targets=DeliveryTargets(),
        created_at=NOW,
    ).review
    missing_id = UUID(int=999)

    with pytest.raises(KeyError):
        revise_action(
            review=review,
            edit=ActionEdit(
                action_id=missing_id,
                title="Missing",
                owner=None,
                due_date=None,
                due_time=None,
                timezone="UTC",
                notes=None,
            ),
            revision_id=UUID(int=106),
            actor_id="reviewer",
            created_at=NOW,
        )
    with pytest.raises(KeyError):
        revise_delivery(
            review=review,
            action_id=missing_id,
            kind=WriteKind.TASK,
            enabled=True,
            targets=DeliveryTargets(),
            revision_id=UUID(int=107),
            actor_id="reviewer",
            created_at=NOW,
        )
    with pytest.raises(KeyError):
        revise_delivery(
            review=review.model_copy(update={"directives": ()}),
            action_id=review.action_items[0].id,
            kind=WriteKind.TASK,
            enabled=True,
            targets=DeliveryTargets(),
            revision_id=UUID(int=107),
            actor_id="reviewer",
            created_at=NOW,
        )
    with pytest.raises(ValueError, match="resolved deadline"):
        revise_delivery(
            review=review,
            action_id=review.action_items[0].id,
            kind=WriteKind.CALENDAR_EVENT,
            enabled=True,
            targets=DeliveryTargets(
                calendar=ConnectorTarget(connector_id="calendar", resource_id="primary")
            ),
            revision_id=UUID(int=108),
            actor_id="reviewer",
            created_at=NOW,
        )

    issue = review.issues[0]
    closed = review.model_copy(
        update={
            "issues": (
                issue.model_copy(
                    update={
                        "status": IssueStatus.RESOLVED,
                        "resolution_note": "Resolved earlier",
                    }
                ),
            )
        }
    )
    with pytest.raises(ValueError, match="must resolve or accept"):
        revise_issue(
            review=review,
            edit=IssueResolutionEdit(issue.id, IssueStatus.OPEN, "Still open"),
            revision_id=UUID(int=109),
            actor_id="reviewer",
            created_at=NOW,
        )
    with pytest.raises(ValueError, match="already closed"):
        revise_issue(
            review=closed,
            edit=IssueResolutionEdit(issue.id, IssueStatus.ACCEPTED_RISK, "Accepted earlier"),
            revision_id=UUID(int=110),
            actor_id="reviewer",
            created_at=NOW,
        )
    with pytest.raises(KeyError):
        revise_issue(
            review=review,
            edit=IssueResolutionEdit(missing_id, IssueStatus.ACCEPTED_RISK, "Not present"),
            revision_id=UUID(int=111),
            actor_id="reviewer",
            created_at=NOW,
        )
