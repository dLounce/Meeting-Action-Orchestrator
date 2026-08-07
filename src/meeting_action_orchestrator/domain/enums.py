from enum import Enum


class AudioMediaType(str, Enum):
    MP3 = "audio/mpeg"
    MP4 = "audio/mp4"
    M4A = "audio/x-m4a"
    WAV = "audio/wav"
    X_WAV = "audio/x-wav"


class MeetingStatus(str, Enum):
    INGESTED = "ingested"
    TRANSCRIBING = "transcribing"
    TRANSCRIPTION_FAILED = "transcription_failed"
    TRANSCRIBED = "transcribed"
    EXTRACTING = "extracting"
    EXTRACTION_FAILED = "extraction_failed"
    AWAITING_APPROVAL = "awaiting_approval"
    APPROVED = "approved"
    FILING = "filing"
    PARTIALLY_FILED = "partially_filed"
    FILING_FAILED = "filing_failed"
    COMPLETED = "completed"
    CANCELLED = "cancelled"


class ProcessingStage(str, Enum):
    TRANSCRIPTION = "transcription"
    EXTRACTION = "extraction"


class ProcessingJobStatus(str, Enum):
    READY = "ready"
    RUNNING = "running"
    RETRY_WAIT = "retry_wait"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


class DeliveryOperationKind(str, Enum):
    RETRY = "retry"
    RECONCILE = "reconcile"


class ReviewOrigin(str, Enum):
    MODEL = "model"
    HUMAN = "human"


class DeadlineKind(str, Enum):
    DATE = "date"
    DATETIME = "datetime"


class DeadlineResolution(str, Enum):
    EXPLICIT = "explicit"
    RELATIVE_TO_MEETING = "relative_to_meeting"
    HUMAN_SET = "human_set"


class Priority(str, Enum):
    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    URGENT = "urgent"


class IssueSeverity(str, Enum):
    WARNING = "warning"
    BLOCKING = "blocking"


class IssueStatus(str, Enum):
    OPEN = "open"
    RESOLVED = "resolved"
    ACCEPTED_RISK = "accepted_risk"


class WriteKind(str, Enum):
    TASK = "task"
    CALENDAR_EVENT = "calendar_event"


class WriteStatus(str, Enum):
    PENDING = "pending"
    IN_FLIGHT = "in_flight"
    RETRY_WAIT = "retry_wait"
    UNKNOWN = "unknown"
    SUCCEEDED = "succeeded"
    PERMANENT_FAILED = "permanent_failed"


class FailureDisposition(str, Enum):
    RETRYABLE = "retryable"
    PERMANENT = "permanent"
    UNKNOWN_OUTCOME = "unknown_outcome"


class FailureCode(str, Enum):
    INVALID_INPUT = "invalid_input"
    UNSUPPORTED_MEDIA = "unsupported_media"
    RATE_LIMITED = "rate_limited"
    PROVIDER_TIMEOUT = "provider_timeout"
    PROVIDER_UNAVAILABLE = "provider_unavailable"
    INVALID_MODEL_OUTPUT = "invalid_model_output"
    PROVIDER_AUTH = "provider_auth"
    CONNECTOR_AUTH = "connector_auth"
    CONNECTOR_TARGET_MISSING = "connector_target_missing"
    CONNECTOR_REJECTED = "connector_rejected"
    IDEMPOTENCY_CONFLICT = "idempotency_conflict"
    UNKNOWN_REMOTE_OUTCOME = "unknown_remote_outcome"
    INTERNAL = "internal"
