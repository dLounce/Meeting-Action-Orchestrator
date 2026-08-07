from __future__ import annotations

from enum import Enum

from meeting_action_orchestrator.domain.enums import MeetingStatus, WriteStatus


class DomainError(ValueError):
    pass


class DomainValueCode(str, Enum):
    TIMEZONE = "timezone must be an IANA timezone"
    ORIGINAL_NAME = "original_name must be a base file name"
    RECORDING_STORAGE_KEY = "storage_key must be a generated recording key"
    EMAIL = "email must be a valid address"
    SEGMENT_IDS = "segment_ids must be unique"
    CANONICAL_DATETIME = "canonical datetime must include a UTC offset"
    CANONICAL_TYPE = "canonical JSON received an unsupported value type"
    INGEST_FINGERPRINT_VERSION = "ingest request fingerprint version is unsupported"


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
    TRANSCRIPT_SIZE = "transcript segments exceed the supported size"
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
    WRITE_RECONCILE_REQUIRED = "unknown write requires a reconciliation schedule"
    WRITE_RECONCILE_EXPIRY = "write reconciliation cannot precede the last update"
    WRITE_RECONCILE_FORBIDDEN = "only an unknown write can have reconciliation state"
    WRITE_RECONCILE_LEASE_REQUIRED = "write reconciliation lease fields must be paired"
    WRITE_RECONCILE_LEASE_EXPIRY = "write reconciliation lease must expire after its update"
    WRITE_RECONCILE_LEASE_FORBIDDEN = "only an unknown write can hold reconciliation lease"
    WRITE_FAILURE = "failed write status requires a failure"
    DELIVERY_OPERATION_TIMESTAMPS = "delivery operation update precedes its creation"
    DELIVERY_OPERATION_LEASE_REQUIRED = "running delivery operation requires a lease"
    DELIVERY_OPERATION_LEASE_EXPIRY = "delivery operation lease must expire after its update"
    DELIVERY_OPERATION_LEASE_FORBIDDEN = "non-running delivery operation cannot hold a lease"
    DELIVERY_OPERATION_COMPLETION_REQUIRED = "completed delivery operation requires completion time"
    DELIVERY_OPERATION_COMPLETION_FORBIDDEN = "unfinished delivery operation cannot be completed"
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
    JOB_TIMESTAMPS = "processing job updated_at precedes created_at"
    JOB_ATTEMPTS = "processing job attempt count exceeds its limit"
    JOB_MAX_ATTEMPTS = "processing job retry limit does not match its stage"
    JOB_LEASE_REQUIRED = "running processing job requires a lease"
    JOB_LEASE_EXPIRY = "processing job lease must expire after its last update"
    JOB_LEASE_FORBIDDEN = "only a running processing job can hold a lease"
    JOB_RETRY_REQUIRED = "retrying processing job requires a schedule"
    JOB_RETRY_EXPIRY = "processing job retry cannot precede its last update"
    JOB_RETRY_FORBIDDEN = "only a retrying processing job can have a schedule"
    JOB_FAILURE_REQUIRED = "failed processing job status requires a failure"
    JOB_FAILURE_FORBIDDEN = "active or successful processing job cannot retain a failure"
    JOB_RETRY_DISPOSITION = "processing job retry requires a retryable failure"
    CLEANUP_TIMESTAMPS = "recording cleanup timestamps are inconsistent"
    CLEANUP_ATTEMPTS = "recording cleanup attempt count exceeds its limit"
    CLEANUP_LEASE_REQUIRED = "running recording cleanup requires a lease"
    CLEANUP_LEASE_EXPIRY = "recording cleanup lease must expire after its last update"
    CLEANUP_LEASE_FORBIDDEN = "only a running recording cleanup can hold a lease"
    CLEANUP_RETRY_REQUIRED = "retrying recording cleanup requires a schedule"
    CLEANUP_RETRY_EXPIRY = "recording cleanup retry cannot precede its last update"
    CLEANUP_RETRY_FORBIDDEN = "only retrying recording cleanup can have a schedule"
    CLEANUP_FAILURE_REQUIRED = "failed recording cleanup status requires a failure"
    CLEANUP_FAILURE_FORBIDDEN = "active or successful recording cleanup cannot retain a failure"
    CLEANUP_RETRY_DISPOSITION = "recording cleanup retry requires a retryable failure"
    CLEANUP_COMPLETION_REQUIRED = "terminal recording cleanup requires a completion time"
    CLEANUP_COMPLETION_FORBIDDEN = "unfinished recording cleanup cannot be completed"
    MEETING_OPERATION_STAGE = "meeting operation stage does not match its operation"
    MEETING_OPERATION_FINGERPRINT = "meeting operation fingerprint does not match its request"


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


class RecordingCleanupConflictError(DomainError):
    def __init__(self, storage_key: str) -> None:
        super().__init__(f"storage key {storage_key} is bound to different recording metadata")
