from __future__ import annotations

from meeting_action_orchestrator.application.provider_policy import ProviderErrorMetadata
from meeting_action_orchestrator.domain.enums import FailureCode, FailureDisposition


class ApplicationError(RuntimeError):
    pass


class ResourceNotFoundError(ApplicationError):
    def __init__(self, resource: str) -> None:
        super().__init__(f"{resource} was not found")


class OperationConflictError(ApplicationError):
    pass


class ReviewDigestMismatchError(OperationConflictError):
    def __init__(self) -> None:
        super().__init__("The review changed before approval")


class WorkflowBusyError(OperationConflictError):
    def __init__(self) -> None:
        super().__init__("The workflow is already processing")


class StaleWorkflowVersionError(OperationConflictError):
    def __init__(self) -> None:
        super().__init__("The meeting changed before this operation completed")


class UnknownWriteOutcomeError(ApplicationError):
    pass


class TransientWriteError(ApplicationError):
    pass


class PermanentWriteError(ApplicationError):
    pass


class ProviderError(RuntimeError):
    def __init__(
        self,
        message: str = "Provider request failed",
        *,
        metadata: ProviderErrorMetadata | None = None,
    ) -> None:
        details = metadata or ProviderErrorMetadata()
        self.http_status = details.http_status
        self.provider_code = details.provider_code
        self.request_id = details.request_id
        self.response_id = details.response_id
        self.retry_after_seconds = details.retry_after_seconds
        self.retry_after_exceeds_limit = details.retry_after_exceeds_limit
        self.provider_should_retry = details.provider_should_retry
        self.retry_control_rejected = details.retry_control_rejected
        super().__init__(message)


class ProviderConfigurationError(ProviderError):
    pass


class ProviderInputError(ProviderError):
    pass


class ProviderTransientError(ProviderError):
    pass


class ProviderTimeoutError(ProviderTransientError):
    pass


class ProviderRateLimitError(ProviderTransientError):
    pass


class ProviderOutputError(ProviderError):
    pass


class ProviderPermanentError(ProviderError):
    pass


class ProviderPermanentOutputError(ProviderPermanentError):
    pass


class RecordingCleanupError(ApplicationError):
    pass


class RetryableRecordingCleanupError(RecordingCleanupError):
    def __init__(self) -> None:
        super().__init__("Recording cleanup can be retried")


class PermanentRecordingCleanupError(RecordingCleanupError):
    def __init__(self) -> None:
        super().__init__("Recording cleanup was rejected for safety")


class AudioAssetIdentityMismatchError(ApplicationError):
    def __init__(self) -> None:
        super().__init__("Stored recording identity does not match persisted audio metadata")


class DeliveryGatewayError(RuntimeError):
    def __init__(
        self,
        code: FailureCode,
        disposition: FailureDisposition,
        safe_message: str,
        provider_request_id: str | None = None,
    ) -> None:
        self.code = code
        self.disposition = disposition
        self.safe_message = safe_message
        self.provider_request_id = provider_request_id
        self.request_id = provider_request_id
        super().__init__(safe_message)


class RetryableDeliveryError(DeliveryGatewayError):
    pass


class PermanentDeliveryError(DeliveryGatewayError):
    pass


class UnknownDeliveryOutcomeError(DeliveryGatewayError):
    pass
