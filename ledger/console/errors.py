"""Errors confined to the non-authoritative console process."""

from __future__ import annotations

from enum import StrEnum


class ConsoleError(Exception):
    """Base class for expected console failures."""


class ConsoleConfigError(ConsoleError):
    """Raised when production console configuration is unsafe or incomplete."""


class ConsoleStateError(ConsoleError):
    """Raised when console state cannot be read or changed safely."""


class WorkflowNotFoundError(ConsoleStateError):
    """Raised without distinguishing missing workflows from other users' workflows."""


class WorkflowConflictError(ConsoleStateError):
    """Raised for stale versions, busy workflows, or illegal transitions."""


class BackendError(ConsoleError):
    """Base class for backend transport, protocol, and domain failures."""


class BackendTransportError(BackendError):
    """Raised when no validated backend response was received."""


class BackendProtocolError(BackendError):
    """Raised when the backend response cannot establish a safe outcome."""


class FailureDisposition(StrEnum):
    """Safe workflow outcome inferred from a validated backend failure."""

    TERMINAL = "terminal"
    RETRYABLE = "retryable"
    UNCERTAIN = "uncertain"


class BackendDomainError(BackendError):
    """A validated, structured error returned by the Pine backend."""

    def __init__(
        self,
        *,
        status_code: int,
        code: str,
        message: str,
        details: tuple[str, ...] = (),
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.message = message
        self.details = details

    @property
    def disposition(self) -> FailureDisposition:
        """Classify capture safety from the backend's stable error contract."""

        if self.code in {
            "invalid_request",
            "schema_not_found",
            "invalid_forecast",
            "fresh_window_conflict",
            "idempotency_conflict",
            "integrity_error",
        }:
            return FailureDisposition.TERMINAL
        if self.code in {"snapshot_unavailable", "unauthorized"}:
            return FailureDisposition.RETRYABLE
        return FailureDisposition.UNCERTAIN
