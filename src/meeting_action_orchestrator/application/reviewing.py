from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time
from uuid import UUID
from zoneinfo import ZoneInfo

from meeting_action_orchestrator.application.mapping import DeliveryTargets
from meeting_action_orchestrator.domain.enums import (
    DeadlineResolution,
    IssueStatus,
    ReviewOrigin,
    WriteKind,
)
from meeting_action_orchestrator.domain.models import (
    ActionItem,
    DateDeadline,
    DateTimeDeadline,
    DeliveryDirective,
    PersonRef,
    ReviewIssue,
    ReviewRevision,
)


@dataclass(frozen=True, slots=True)
class ActionEdit:
    action_id: UUID
    title: str
    owner: str | None
    due_date: date | None
    due_time: time | None
    timezone: str
    notes: str | None
    recap_markdown: str | None = None


@dataclass(frozen=True, slots=True)
class IssueResolutionEdit:
    issue_id: UUID
    status: IssueStatus
    resolution_note: str


def revise_action(
    *,
    review: ReviewRevision,
    edit: ActionEdit,
    revision_id: UUID,
    actor_id: str,
    created_at: datetime,
) -> ReviewRevision:
    existing = _action(review, edit.action_id)
    deadline = _human_deadline(edit)
    updated_action = ActionItem(
        id=existing.id,
        title=edit.title,
        description=edit.notes,
        assignee=PersonRef(display_name=edit.owner) if edit.owner else None,
        deadline=deadline,
        priority=existing.priority,
        evidence=existing.evidence,
        confidence=1.0,
        origin=ReviewOrigin.HUMAN,
    )
    actions = tuple(
        updated_action if item.id == existing.id else item for item in review.action_items
    )
    directives = tuple(
        _reconcile_deadline(directive, updated_action)
        if directive.action_item_id == existing.id
        else directive
        for directive in review.directives
    )
    payload = review.model_dump(mode="python") | {
        "id": revision_id,
        "revision_number": review.revision_number + 1,
        "origin": ReviewOrigin.HUMAN,
        "recap_markdown": edit.recap_markdown or review.recap_markdown,
        "action_items": actions,
        "directives": directives,
        "content_digest": "",
        "actor_id": actor_id,
        "created_at": created_at,
    }
    return ReviewRevision.model_validate(payload)


def revise_issue(
    *,
    review: ReviewRevision,
    edit: IssueResolutionEdit,
    revision_id: UUID,
    actor_id: str,
    created_at: datetime,
) -> ReviewRevision:
    if edit.status is IssueStatus.OPEN:
        raise ValueError("An issue revision must resolve or accept the issue")
    found = False
    issues: list[ReviewIssue] = []
    for issue in review.issues:
        if issue.id != edit.issue_id:
            issues.append(issue)
            continue
        if issue.status is not IssueStatus.OPEN:
            raise ValueError("The review issue is already closed")
        found = True
        issues.append(
            issue.model_copy(
                update={
                    "status": edit.status,
                    "resolution_note": edit.resolution_note,
                }
            )
        )
    if not found:
        raise KeyError(edit.issue_id)
    payload = review.model_dump(mode="python") | {
        "id": revision_id,
        "revision_number": review.revision_number + 1,
        "origin": ReviewOrigin.HUMAN,
        "issues": tuple(issues),
        "content_digest": "",
        "actor_id": actor_id,
        "created_at": created_at,
    }
    return ReviewRevision.model_validate(payload)


def revise_delivery(
    *,
    review: ReviewRevision,
    action_id: UUID,
    kind: WriteKind,
    enabled: bool,
    targets: DeliveryTargets,
    revision_id: UUID,
    actor_id: str,
    created_at: datetime,
) -> ReviewRevision:
    action = _action(review, action_id)
    current = _directive(review, action_id)
    if kind is WriteKind.TASK:
        target = targets.task if enabled else None
        updated = current.model_copy(update={"create_task": enabled, "task_target": target})
    else:
        if enabled and action.deadline is None:
            raise ValueError("A calendar event requires a resolved deadline")
        target = targets.calendar if enabled else None
        updated = current.model_copy(
            update={"create_calendar_event": enabled, "calendar_target": target}
        )
    payload = review.model_dump(mode="python") | {
        "id": revision_id,
        "revision_number": review.revision_number + 1,
        "origin": ReviewOrigin.HUMAN,
        "directives": tuple(
            updated if item.action_item_id == action_id else item for item in review.directives
        ),
        "content_digest": "",
        "actor_id": actor_id,
        "created_at": created_at,
    }
    return ReviewRevision.model_validate(payload)


def _human_deadline(edit: ActionEdit) -> DateDeadline | DateTimeDeadline | None:
    if edit.due_date is None:
        if edit.due_time is not None:
            raise ValueError("A due time requires a due date")
        return None
    if edit.due_time is None:
        return DateDeadline(
            value=edit.due_date,
            timezone=edit.timezone,
            source_text=edit.due_date.isoformat(),
            resolution=DeadlineResolution.HUMAN_SET,
        )
    local = datetime.combine(edit.due_date, edit.due_time, ZoneInfo(edit.timezone))
    _validate_local_time(local)
    return DateTimeDeadline(
        at=local,
        timezone=edit.timezone,
        source_text=local.isoformat(),
        resolution=DeadlineResolution.HUMAN_SET,
    )


def _validate_local_time(value: datetime) -> None:
    timezone = value.tzinfo
    if not isinstance(timezone, ZoneInfo):
        raise ValueError("A valid IANA timezone is required")
    utc = value.astimezone(ZoneInfo("UTC"))
    round_trip = utc.astimezone(timezone)
    if round_trip.replace(fold=value.fold) != value:
        raise ValueError("The selected local time does not exist in this timezone")
    alternate = value.replace(fold=1 - value.fold)
    if alternate.utcoffset() != value.utcoffset():
        raise ValueError("The selected local time is ambiguous in this timezone")


def _action(review: ReviewRevision, action_id: UUID) -> ActionItem:
    for action in review.action_items:
        if action.id == action_id:
            return action
    raise KeyError(action_id)


def _directive(review: ReviewRevision, action_id: UUID) -> DeliveryDirective:
    for directive in review.directives:
        if directive.action_item_id == action_id:
            return directive
    raise KeyError(action_id)


def _reconcile_deadline(
    directive: DeliveryDirective,
    action: ActionItem,
) -> DeliveryDirective:
    if action.deadline is not None or not directive.create_calendar_event:
        return directive
    return directive.model_copy(update={"create_calendar_event": False, "calendar_target": None})
