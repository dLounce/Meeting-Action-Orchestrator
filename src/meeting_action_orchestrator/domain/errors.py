from __future__ import annotations

from enum import Enum

from meeting_action_orchestrator.domain.enums import MeetingStatus, WriteStatus


class DomainError(ValueError):
    pass


class DomainValueCode(str, Enum):
    TIMEZONE = "timezone must be an IANA timezone"
    ORIGINAL_NAME = "original_name must be a base file name"
    EMAIL = "email must be a valid address"
    SEGMENT_IDS = "segment_ids must be unique"
    CANONICAL_DATETIME = "canonical datetime must include a UTC offset"
    CANONICAL_TYPE = "canonical JSON received an unsupported value type"


class InvariantCode(str, Enum):
    MEETING_TIMESTAMPS = "meeting updated_at precedes created_at"
    MEETING_TRANSCRIPT = "meeting status requires a transcript"
    MEETING_REVIEW = "meeting status requires a review"
    MEETING_APPROVAL = "meeting status requires an approved review"
    MEETING_FAILURE = "failed status requires a failure"
    SEGMENT_RANGE = "transcript segment end_ms precedes start_ms"
    SEGMENT_ORDINALS = "transcript segment ordinals must be contiguous"
    SEGMENT_ORDER = "transcript segment start times must be ordered"
    TRANSCRIPT_HASH = "transcript hash does not match transcript text"
    DECISION_EVIDENCE = "model output requires decision evidence"
    ACTION_EVIDENCE = "model output requires action item evidence"
    QUESTION_EVIDENCE = "model output requires open question evidence"
    RISK_EVIDENCE = "model output requires risk evidence"
    ISSUE_OPEN_RESOLUTION = "open issue cannot have a resolution"
    ISSUE_CLOSED_RESOLUTION = "closed issue requires a resolution"
    TASK_TARGET_REQUIRED = "task creation requires a target"
    TASK_TARGET_DISABLED = "disabled task cannot have a target"
    CALENDAR_TARGET_REQUIRED = "calendar creation requires a target"
    CALENDAR_TARGET_DISABLED = "disabled calendar cannot have a target"
    DECISION_IDS = "decision IDs must be unique"
    ACTION_IDS = "action item IDs must be unique"
    QUESTION_IDS = "open question IDs must be unique"
    RISK_IDS = "risk IDs must be unique"
    ITEM_IDS = "review item IDs must be unique across categories"
    ISSUE_IDS = "issue IDs must be unique"
    DIRECTIVE_IDS = "directive action IDs must be unique"
    DIRECTIVE_COVERAGE = "each action requires one directive"
    ISSUE_ITEM = "issue references an unknown item"
    CALENDAR_DEADLINE = "calendar creation requires a resolved deadline"
    REVIEW_DIGEST = "review digest does not match review content"
    RECAP_HASH = "recap hash does not match recap content"
    WRITE_TIMESTAMPS = "write updated_at precedes created_at"
    WRITE_PAYLOAD_HASH = "write payload hash does not match proposal"
    WRITE_LEASE_REQUIRED = "in-flight write requires a lease"
    WRITE_LEASE_EXPIRY = "write lease must expire after the last update"
    WRITE_LEASE_FORBIDDEN = "only an in-flight write can hold a lease"
    WRITE_RETRY_REQUIRED = "retrying write requires a schedule"
    WRITE_RETRY_EXPIRY = "write retry must be scheduled after the last update"
    WRITE_RETRY_FORBIDDEN = "only retrying write can have a schedule"
    WRITE_FAILURE = "failed write status requires a failure"
    REVIEW_TRANSCRIPT = "review and transcript do not match"
    EVIDENCE_SEGMENT = "evidence references an unknown segment"
    EVIDENCE_QUOTE = "evidence quote is absent from its segments"
    APPROVAL_STATUS = "meeting is not awaiting approval"
    APPROVAL_REVIEW = "review is not the current meeting revision"
    APPROVAL_TRANSCRIPT = "review does not use the current transcript"
    APPROVAL_BLOCKER = "review has unresolved blocking issues"
    RECAP_APPROVAL = "recap approval does not match review"
    PROJECTION_APPROVAL = "approval does not match review"
    PROJECTION_DIGEST = "approval digest does not match review"
    PROJECTION_TASK = "task target is missing"
    PROJECTION_CALENDAR = "calendar details are incomplete"
    RECEIPT_INTENT = "write receipt does not reference its intent"
    TRANSITION_TIMESTAMP = "transition timestamp precedes last update"
    RETRY_DISPOSITION = "retry requires a retryable failure"
    UNKNOWN_DISPOSITION = "unknown state requires unknown outcome"
    PERMANENT_DISPOSITION = "failure must be permanent"
    TRANSITION_LEASE = "in-flight transition requires a lease"
    RECAP_MISSING = "approved recap is missing"


class InvalidDomainValueError(DomainError):
    def __init__(self, code: DomainValueCode, detail: str | None = None) -> None:
        message = code.value if detail is None else f"{code.value}: {detail}"
        super().__init__(message)


class DomainInvariantError(DomainError):
    def __init__(self, code: InvariantCode) -> None:
        super().__init__(code.value)


class InvalidMeetingTransitionError(DomainError):
    def __init__(self, current: MeetingStatus, target: MeetingStatus) -> None:
        super().__init__(f"meeting cannot transition from {current.value} to {target.value}")


class InvalidWriteTransitionError(DomainError):
    def __init__(self, current: WriteStatus, target: WriteStatus) -> None:
        super().__init__(f"write cannot transition from {current.value} to {target.value}")


class IdempotencyConflictError(DomainError):
    def __init__(self, key: str) -> None:
        super().__init__(f"idempotency key {key} is already bound to another payload")
