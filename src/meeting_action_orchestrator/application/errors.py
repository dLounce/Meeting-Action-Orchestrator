from __future__ import annotations

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
    pass


class ProviderConfigurationError(ProviderError):
    pass


class ProviderInputError(ProviderError):
    pass


class ProviderTransientError(ProviderError):
    pass


class ProviderOutputError(ProviderError):
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
