from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime
from uuid import UUID, uuid5
from zoneinfo import ZoneInfo

from meeting_action_orchestrator.agents.contracts import (
    ActionItemCandidate,
    CanonicalMeetingRecord,
    DecisionCandidate,
    ExtractionRequest,
    MeetingExtraction,
    OpenQuestionCandidate,
    RecapDraft,
    RecordItem,
    RiskCandidate,
    TranscriptInput,
    TranscriptSegmentInput,
    VerificationFinding,
    VerificationReport,
)
from meeting_action_orchestrator.agents.contracts import (
    EvidenceRef as AgentEvidenceRef,
)
from meeting_action_orchestrator.application.ports import TranscriptionOutputLike
from meeting_action_orchestrator.domain.enums import (
    DeadlineResolution,
    IssueSeverity,
    Priority,
    ReviewOrigin,
)
from meeting_action_orchestrator.domain.hashing import text_sha256
from meeting_action_orchestrator.domain.models import (
    ActionItem,
    CalendarEventProposal,
    ConnectorTarget,
    DateDeadline,
    DateTimeDeadline,
    Decision,
    DeliveryDirective,
    EvidenceRef,
    Meeting,
    OpenQuestion,
    PersonRef,
    ReviewIssue,
    ReviewRevision,
    Risk,
    TaskProposal,
    Transcript,
    TranscriptSegment,
)


@dataclass(frozen=True, slots=True)
class DeliveryTargets:
    task: ConnectorTarget | None = None
    calendar: ConnectorTarget | None = None


@dataclass(frozen=True, slots=True)
class ReviewPackage:
    review: ReviewRevision
    participants: tuple[PersonRef, ...]


_DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_SPACE_PATTERN = re.compile(r"\s+")


def map_transcription(
    meeting: Meeting,
    output: TranscriptionOutputLike,
    created_at: datetime,
) -> Transcript:
    transcript_id = uuid5(meeting.id, f"transcript:{text_sha256(output.text)}")
    duration_ms = round(output.duration_seconds * 1000) if output.duration_seconds else None
    segments: list[TranscriptSegment] = []
    for index, segment in enumerate(output.segments):
        end_ms = segment.end_ms or duration_ms or segment.start_ms + 1
        end_ms = max(end_ms, segment.start_ms + 1)
        segments.append(
            TranscriptSegment(
                id=uuid5(transcript_id, f"segment:{index}:{segment.id}"),
                ordinal=index,
                start_ms=segment.start_ms,
                end_ms=end_ms,
                speaker=segment.speaker,
                text=segment.text,
            )
        )
    return Transcript(
        id=transcript_id,
        meeting_id=meeting.id,
        audio_asset_id=meeting.audio_asset_id,
        provider="openai",
        model=output.model,
        language=output.language or "und",
        text=output.text,
        segments=tuple(segments),
        provider_request_id=output.provider_request_id,
        usage=output.usage,
        created_at=created_at,
    )


def build_extraction_request(meeting: Meeting, transcript: Transcript) -> ExtractionRequest:
    if meeting.occurred_at is None:
        raise ValueError("Meeting time is required before extraction")
    return ExtractionRequest(
        meeting_id=str(meeting.id),
        meeting_started_at=meeting.occurred_at,
        timezone=meeting.timezone,
        transcript=transcript_input(transcript),
    )


def transcript_input(transcript: Transcript) -> TranscriptInput:
    return TranscriptInput(
        language=transcript.language,
        text=transcript.text,
        sha256=transcript.sha256,
        segments=[
            TranscriptSegmentInput(
                id=str(segment.id),
                start_ms=segment.start_ms,
                end_ms=segment.end_ms,
                speaker=segment.speaker,
                text=segment.text,
            )
            for segment in transcript.segments
        ],
    )


def build_canonical_record(
    meeting: Meeting,
    extraction: MeetingExtraction,
) -> CanonicalMeetingRecord:
    items: list[RecordItem] = []
    for index, candidate in enumerate(extraction.decisions):
        items.append(
            RecordItem(
                id=str(_entity_id(meeting.id, "decision", index, candidate.statement)),
                kind="decision",
                text=candidate.statement,
                owner=candidate.owner,
                confidence=candidate.confidence,
                evidence=candidate.evidence,
            )
        )
    for index, candidate in enumerate(extraction.action_items):
        items.append(
            RecordItem(
                id=str(_entity_id(meeting.id, "action", index, candidate.description)),
                kind="action_item",
                text=candidate.description,
                owner=candidate.owner,
                due_expression=candidate.due_expression,
                confidence=candidate.confidence,
                evidence=candidate.evidence,
            )
        )
    for index, candidate in enumerate(extraction.open_questions):
        items.append(
            RecordItem(
                id=str(_entity_id(meeting.id, "question", index, candidate.question)),
                kind="open_question",
                text=candidate.question,
                owner=candidate.owner,
                confidence="medium",
                evidence=candidate.evidence,
            )
        )
    for index, candidate in enumerate(extraction.risks):
        items.append(
            RecordItem(
                id=str(_entity_id(meeting.id, "risk", index, candidate.description)),
                kind="risk",
                text=candidate.description,
                owner=candidate.owner,
                confidence="medium",
                evidence=candidate.evidence,
            )
        )
    participants = [
        candidate.display_name
        for candidate in extraction.participants
        if candidate.display_name is not None
    ]
    return CanonicalMeetingRecord(
        title=extraction.suggested_title,
        purpose=extraction.purpose,
        participants=participants,
        items=items,
        warnings=extraction.warnings,
    )


def render_recap(
    meeting: Meeting,
    extraction: MeetingExtraction,
    record: CanonicalMeetingRecord,
    recap: RecapDraft,
) -> str:
    decisions = [item for item in record.items if item.kind == "decision"]
    actions = [item for item in record.items if item.kind == "action_item"]
    questions = [item for item in record.items if item.kind == "open_question"]
    risks = [item for item in record.items if item.kind == "risk"]
    lines = [f"# {_markdown(recap.title)}", "", _markdown(recap.overview), ""]
    if extraction.purpose:
        lines.extend(["## Purpose", "", _markdown(extraction.purpose), ""])
    if recap.highlights:
        lines.extend(["## Highlights", ""])
        lines.extend(f"- {_markdown(item.text)}" for item in recap.highlights)
        lines.append("")
    lines.extend(_record_section("Decisions", decisions))
    lines.extend(_action_section(actions))
    lines.extend(_record_section("Open questions", questions))
    lines.extend(_record_section("Risks", risks))
    if extraction.warnings:
        lines.extend(["## Review notes", ""])
        lines.extend(f"- {_markdown(item)}" for item in extraction.warnings)
        lines.append("")
    lines.extend(["## Meeting", "", f"- Reference: `{meeting.id}`"])
    return "\n".join(lines).strip()


def map_review_package(
    *,
    meeting: Meeting,
    transcript: Transcript,
    extraction: MeetingExtraction,
    recap_markdown: str,
    verification: VerificationReport,
    targets: DeliveryTargets,
    created_at: datetime,
    revision_number: int = 1,
) -> ReviewPackage:
    decisions = tuple(
        _map_decision(meeting.id, index, candidate)
        for index, candidate in enumerate(extraction.decisions)
    )
    actions: list[ActionItem] = []
    issues: list[ReviewIssue] = []
    directives: list[DeliveryDirective] = []
    for index, candidate in enumerate(extraction.action_items):
        action, action_issues = _map_action(meeting, index, candidate)
        actions.append(action)
        issues.extend(action_issues)
        directives.append(
            DeliveryDirective(
                action_item_id=action.id,
                create_task=targets.task is not None,
                task_target=targets.task,
                create_calendar_event=targets.calendar is not None and action.deadline is not None,
                calendar_target=targets.calendar if action.deadline is not None else None,
            )
        )
    questions = tuple(
        _map_question(meeting.id, index, candidate)
        for index, candidate in enumerate(extraction.open_questions)
    )
    risks = tuple(
        _map_risk(meeting.id, index, candidate) for index, candidate in enumerate(extraction.risks)
    )
    valid_item_ids = {
        *(item.id for item in decisions),
        *(item.id for item in actions),
        *(item.id for item in questions),
        *(item.id for item in risks),
    }
    issues.extend(_verification_issues(meeting.id, verification, valid_item_ids))
    for index, warning in enumerate(extraction.warnings):
        issues.append(
            ReviewIssue(
                id=_entity_id(meeting.id, "warning", index, warning),
                field="extraction",
                severity=IssueSeverity.WARNING,
                message=_short_title(warning, 1000),
            )
        )
    review_id = uuid5(
        meeting.id,
        f"review:{transcript.sha256}:{revision_number}:{text_sha256(recap_markdown)}",
    )
    participants = tuple(
        PersonRef(display_name=name)
        for name in dict.fromkeys(
            candidate.display_name
            for candidate in extraction.participants
            if candidate.display_name is not None
        )
    )
    review = ReviewRevision(
        id=review_id,
        meeting_id=meeting.id,
        transcript_id=transcript.id,
        revision_number=revision_number,
        origin=ReviewOrigin.MODEL,
        purpose=_short_title(extraction.purpose, 1000) if extraction.purpose else None,
        recap_markdown=recap_markdown,
        decisions=decisions,
        action_items=tuple(actions),
        open_questions=questions,
        risks=risks,
        issues=tuple(issues),
        directives=tuple(directives),
        created_at=created_at,
    )
    return ReviewPackage(review=review, participants=participants)


def _map_decision(meeting_id: UUID, index: int, candidate: DecisionCandidate) -> Decision:
    detail_parts = [candidate.statement]
    if candidate.rationale:
        detail_parts.append(candidate.rationale)
    return Decision(
        id=_entity_id(meeting_id, "decision", index, candidate.statement),
        summary=_short_title(candidate.statement, 1000),
        detail="\n\n".join(detail_parts),
        evidence=_evidence(candidate.evidence),
        confidence=_confidence(candidate.confidence),
    )


def _map_action(
    meeting: Meeting,
    index: int,
    candidate: ActionItemCandidate,
) -> tuple[ActionItem, list[ReviewIssue]]:
    action_id = _entity_id(meeting.id, "action", index, candidate.description)
    deadline = _deadline(candidate.due_expression, meeting.timezone)
    description = candidate.description
    if candidate.dependency:
        description = f"{description}\n\nDependency: {candidate.dependency}"
    action = ActionItem(
        id=action_id,
        title=_short_title(candidate.description, 200),
        description=description,
        assignee=_person(candidate.owner),
        deadline=deadline,
        priority=Priority.NORMAL,
        evidence=_evidence(candidate.evidence),
        confidence=_confidence(candidate.confidence),
    )
    issues: list[ReviewIssue] = []
    if candidate.requires_clarification:
        issues.append(
            _issue(
                meeting.id,
                action_id,
                "clarification",
                IssueSeverity.BLOCKING,
                "The extracted action requires clarification before approval",
            )
        )
    if candidate.due_expression and deadline is None:
        issues.append(
            _issue(
                meeting.id,
                action_id,
                "deadline",
                IssueSeverity.BLOCKING,
                f"Resolve the deadline expression: {candidate.due_expression}",
            )
        )
    if candidate.owner is None:
        issues.append(
            _issue(
                meeting.id,
                action_id,
                "assignee",
                IssueSeverity.WARNING,
                "No assignee was stated for this action",
            )
        )
    if candidate.confidence == "low":
        issues.append(
            _issue(
                meeting.id,
                action_id,
                "confidence",
                IssueSeverity.WARNING,
                "The extraction confidence is low",
            )
        )
    return action, issues


def _map_question(
    meeting_id: UUID,
    index: int,
    candidate: OpenQuestionCandidate,
) -> OpenQuestion:
    return OpenQuestion(
        id=_entity_id(meeting_id, "question", index, candidate.question),
        question=candidate.question,
        owner=_person(candidate.owner),
        evidence=_evidence(candidate.evidence),
    )


def _map_risk(meeting_id: UUID, index: int, candidate: RiskCandidate) -> Risk:
    return Risk(
        id=_entity_id(meeting_id, "risk", index, candidate.description),
        description=candidate.description,
        owner=_person(candidate.owner),
        evidence=_evidence(candidate.evidence),
    )


def _verification_issues(
    meeting_id: UUID,
    report: VerificationReport,
    valid_item_ids: set[UUID],
) -> list[ReviewIssue]:
    issues = [
        ReviewIssue(
            id=_entity_id(meeting_id, f"verification:{finding.code}", index, finding.message),
            item_id=_optional_uuid(finding, valid_item_ids),
            field=finding.code,
            severity=(
                IssueSeverity.BLOCKING if finding.severity == "blocker" else IssueSeverity.WARNING
            ),
            message=_short_title(finding.message, 1000),
        )
        for index, finding in enumerate(report.findings)
    ]
    has_blocker = any(issue.severity is IssueSeverity.BLOCKING for issue in issues)
    if report.verdict == "review_required" and not has_blocker:
        issues.append(
            ReviewIssue(
                id=_entity_id(meeting_id, "verification", len(issues), report.verdict),
                field="verification",
                severity=IssueSeverity.BLOCKING,
                message="The verification stage requires human review",
            )
        )
    return issues


def _optional_uuid(
    finding: VerificationFinding,
    valid_item_ids: set[UUID],
) -> UUID | None:
    if finding.subject_id is None:
        return None
    try:
        parsed = UUID(finding.subject_id)
    except ValueError:
        return None
    return parsed if parsed in valid_item_ids else None


def _deadline(value: str | None, timezone_name: str) -> DateDeadline | DateTimeDeadline | None:
    if value is None:
        return None
    normalized = value.strip()
    if _DATE_PATTERN.fullmatch(normalized):
        try:
            parsed_date = date.fromisoformat(normalized)
        except ValueError:
            return None
        return DateDeadline(
            value=parsed_date,
            timezone=timezone_name,
            source_text=value,
            resolution=DeadlineResolution.EXPLICIT,
        )
    try:
        parsed_datetime = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed_datetime.tzinfo is None:
        parsed_datetime = parsed_datetime.replace(tzinfo=ZoneInfo(timezone_name))
    return DateTimeDeadline(
        at=parsed_datetime,
        timezone=timezone_name,
        source_text=value,
        resolution=DeadlineResolution.EXPLICIT,
    )


def _evidence(items: list[AgentEvidenceRef]) -> tuple[EvidenceRef, ...]:
    return tuple(
        EvidenceRef(segment_ids=(UUID(item.segment_id),), quote=item.quote) for item in items
    )


def _person(name: str | None) -> PersonRef | None:
    return PersonRef(display_name=name) if name else None


def _confidence(value: str) -> float:
    return {"high": 0.95, "medium": 0.7, "low": 0.45}[value]


def _entity_id(meeting_id: UUID, kind: str, index: int, text: str) -> UUID:
    normalized = _SPACE_PATTERN.sub(" ", text).strip().casefold()
    return uuid5(meeting_id, f"{kind}:{index}:{normalized}")


def _issue(
    meeting_id: UUID,
    item_id: UUID,
    field: str,
    severity: IssueSeverity,
    message: str,
) -> ReviewIssue:
    return ReviewIssue(
        id=_entity_id(meeting_id, f"issue:{field}", 0, f"{item_id}:{message}"),
        item_id=item_id,
        field=field,
        severity=severity,
        message=message,
    )


def _short_title(value: str, limit: int) -> str:
    compact = _SPACE_PATTERN.sub(" ", value).strip()
    return compact if len(compact) <= limit else f"{compact[: limit - 3].rstrip()}..."


def _markdown(value: str) -> str:
    return _SPACE_PATTERN.sub(" ", value).replace("|", "\\|").strip()


def _record_section(title: str, items: list[RecordItem]) -> list[str]:
    if not items:
        return []
    lines = [f"## {title}", ""]
    lines.extend(f"- {_markdown(item.text)}" for item in items)
    lines.append("")
    return lines


def _action_section(items: list[RecordItem]) -> list[str]:
    if not items:
        return []
    lines = ["## Action items", "", "| Action | Owner | Deadline |", "|---|---|---|"]
    lines.extend(
        (
            f"| {_markdown(item.text)} | {_markdown(item.owner or 'Unassigned')} | "
            f"{_markdown(item.due_expression or 'Not set')} |"
        )
        for item in items
    )
    lines.append("")
    return lines


def task_proposal(review: ReviewRevision, action_id: UUID) -> TaskProposal | None:
    for directive in review.directives:
        if directive.action_item_id != action_id or not directive.create_task:
            continue
        action = next(item for item in review.action_items if item.id == action_id)
        if directive.task_target is None:
            return None
        return TaskProposal(
            source_action_id=action.id,
            target=directive.task_target,
            title=action.title,
            description=action.description or "",
            assignee=action.assignee,
            deadline=action.deadline,
            priority=action.priority,
        )
    return None


def calendar_proposal(review: ReviewRevision, action_id: UUID) -> CalendarEventProposal | None:
    for directive in review.directives:
        if directive.action_item_id != action_id or not directive.create_calendar_event:
            continue
        action = next(item for item in review.action_items if item.id == action_id)
        if directive.calendar_target is None or action.deadline is None:
            return None
        return CalendarEventProposal(
            source_action_id=action.id,
            target=directive.calendar_target,
            title=action.title,
            description=action.description or "",
            deadline=action.deadline,
            duration_minutes=directive.calendar_event_duration_minutes,
        )
    return None
