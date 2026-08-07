from __future__ import annotations


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
