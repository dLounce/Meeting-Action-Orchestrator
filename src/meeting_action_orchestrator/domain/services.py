from __future__ import annotations

from datetime import datetime
from uuid import UUID, uuid5

from meeting_action_orchestrator.domain.enums import IssueSeverity, IssueStatus, MeetingStatus
from meeting_action_orchestrator.domain.errors import (
    DomainInvariantError,
    IdempotencyConflictError,
    InvariantCode,
)
from meeting_action_orchestrator.domain.hashing import canonical_sha256
from meeting_action_orchestrator.domain.models import (
    Approval,
    CalendarEventProposal,
    Meeting,
    RecapArtifact,
    ReviewRevision,
    TaskProposal,
    Transcript,
    WriteIntent,
    WriteReceipt,
)

_INTENT_NAMESPACE = UUID("4d6b931f-1a1f-5f32-b451-d338a17e19f0")


def validate_review_evidence(review: ReviewRevision, transcript: Transcript) -> None:
    if review.meeting_id != transcript.meeting_id or review.transcript_id != transcript.id:
        raise DomainInvariantError(InvariantCode.REVIEW_TRANSCRIPT)
    segments = {segment.id: segment for segment in transcript.segments}
    review_items = (
        *review.decisions,
        *review.action_items,
        *review.open_questions,
        *review.risks,
    )
    evidence_groups = [item.evidence for item in review_items]
    for evidence_group in evidence_groups:
        for evidence in evidence_group:
            try:
                source = " ".join(segments[segment_id].text for segment_id in evidence.segment_ids)
            except KeyError as exc:
                raise DomainInvariantError(InvariantCode.EVIDENCE_SEGMENT) from exc
            normalized_source = " ".join(source.split()).casefold()
            normalized_quote = " ".join(evidence.quote.split()).casefold()
            if normalized_quote not in normalized_source:
                raise DomainInvariantError(InvariantCode.EVIDENCE_QUOTE)


def approve_review(
    *,
    approval_id: UUID,
    meeting: Meeting,
    review: ReviewRevision,
    transcript: Transcript,
    request_key: str,
    actor_id: str,
    approved_at: datetime,
) -> Approval:
    if meeting.status is not MeetingStatus.AWAITING_APPROVAL:
        raise DomainInvariantError(InvariantCode.APPROVAL_STATUS)
    if meeting.id != review.meeting_id or meeting.current_review_id != review.id:
        raise DomainInvariantError(InvariantCode.APPROVAL_REVIEW)
    if meeting.current_transcript_id != review.transcript_id:
        raise DomainInvariantError(InvariantCode.APPROVAL_TRANSCRIPT)
    has_blocker = any(
        issue.severity is IssueSeverity.BLOCKING and issue.status is IssueStatus.OPEN
        for issue in review.issues
    )
    if has_blocker:
        raise DomainInvariantError(InvariantCode.APPROVAL_BLOCKER)
    validate_review_evidence(review, transcript)
    return Approval(
        id=approval_id,
        meeting_id=meeting.id,
        review_revision_id=review.id,
        review_digest=review.content_digest,
        request_key=request_key,
        actor_id=actor_id,
        approved_at=approved_at,
    )


def create_recap_artifact(
    *,
    artifact_id: UUID,
    meeting: Meeting,
    review: ReviewRevision,
    approval: Approval,
    created_at: datetime,
) -> RecapArtifact:
    if approval.meeting_id != meeting.id or approval.review_revision_id != review.id:
        raise DomainInvariantError(InvariantCode.RECAP_APPROVAL)
    if approval.review_digest != review.content_digest:
        raise DomainInvariantError(InvariantCode.RECAP_APPROVAL)
    return RecapArtifact(
        id=artifact_id,
        meeting_id=meeting.id,
        approval_id=approval.id,
        content=review.recap_markdown,
        created_at=created_at,
    )


def _intent_key(
    meeting: Meeting,
    review: ReviewRevision,
    proposal: TaskProposal | CalendarEventProposal,
) -> str:
    material = {
        "schema_version": 1,
        "connector_id": proposal.target.connector_id,
        "resource_id": proposal.target.resource_id,
        "meeting_id": meeting.id,
        "review_digest": review.content_digest,
        "kind": proposal.kind,
        "source_action_id": proposal.source_action_id,
        "payload_digest": canonical_sha256(proposal),
    }
    return f"mao_v1_{canonical_sha256(material)}"


def _build_intent(
    meeting: Meeting,
    approval: Approval,
    review: ReviewRevision,
    proposal: TaskProposal | CalendarEventProposal,
    created_at: datetime,
) -> WriteIntent:
    key = _intent_key(meeting, review, proposal)
    return WriteIntent(
        id=uuid5(_INTENT_NAMESPACE, key),
        meeting_id=meeting.id,
        approval_id=approval.id,
        idempotency_key=key,
        proposal=proposal,
        created_at=created_at,
        updated_at=created_at,
    )


def project_write_intents(
    *,
    meeting: Meeting,
    review: ReviewRevision,
    approval: Approval,
    created_at: datetime,
) -> tuple[WriteIntent, ...]:
    if approval.meeting_id != meeting.id or approval.review_revision_id != review.id:
        raise DomainInvariantError(InvariantCode.PROJECTION_APPROVAL)
    if approval.review_digest != review.content_digest:
        raise DomainInvariantError(InvariantCode.PROJECTION_DIGEST)
    actions = {action.id: action for action in review.action_items}
    intents: list[WriteIntent] = []
    for directive in review.directives:
        action = actions[directive.action_item_id]
        description = action.description or f"Follow-up from {meeting.title}"
        if directive.create_task:
            if directive.task_target is None:
                raise DomainInvariantError(InvariantCode.PROJECTION_TASK)
            task = TaskProposal(
                source_action_id=action.id,
                target=directive.task_target,
                title=action.title,
                description=description,
                assignee=action.assignee,
                deadline=action.deadline,
                priority=action.priority,
            )
            intents.append(_build_intent(meeting, approval, review, task, created_at))
        if directive.create_calendar_event:
            if directive.calendar_target is None or action.deadline is None:
                raise DomainInvariantError(InvariantCode.PROJECTION_CALENDAR)
            event = CalendarEventProposal(
                source_action_id=action.id,
                target=directive.calendar_target,
                title=action.title,
                description=description,
                deadline=action.deadline,
                duration_minutes=directive.calendar_event_duration_minutes,
            )
            intents.append(_build_intent(meeting, approval, review, event, created_at))
    return tuple(intents)


def validate_write_receipt(intent: WriteIntent, receipt: WriteReceipt) -> None:
    if receipt.intent_id != intent.id:
        raise DomainInvariantError(InvariantCode.RECEIPT_INTENT)
    if (
        receipt.idempotency_key != intent.idempotency_key
        or receipt.payload_digest != intent.payload_digest
    ):
        raise IdempotencyConflictError(intent.idempotency_key)
