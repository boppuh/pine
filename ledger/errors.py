"""Domain exceptions for the ledger integrity substrate."""

from __future__ import annotations

from collections.abc import Sequence


class LedgerError(Exception):
    """Base class for expected ledger failures."""


class IntegrityError(LedgerError):
    """Raised when an operation would weaken a committed record's integrity."""


class SchemaNotFoundError(LedgerError):
    """Raised when a requested forecast schema is not registered."""


class ForecastValidationError(LedgerError):
    """Raised when a forecast cannot be validated without ambiguity."""

    def __init__(self, errors: Sequence[str]) -> None:
        self.errors = list(errors)
        detail = "; ".join(self.errors) if self.errors else "forecast is invalid"
        super().__init__(detail)


class FreshWindowError(IntegrityError):
    """Raised when a preregistered OOS window overlaps previously observed data."""


class IdempotencyConflictError(IntegrityError):
    """Raised when an idempotency key is reused for a different capture request."""


class SnapshotCaptureError(IntegrityError):
    """Raised when a snapshot provider fails or returns invalid frozen state."""


class RunStateError(IntegrityError):
    """Raised when a run cannot make the requested lifecycle transition."""


class PredictionNotFoundError(LedgerError):
    """Raised when a committed prediction is not available to the read API."""


class ReadCursorError(LedgerError):
    """Raised when a prediction-list cursor is malformed or used with new filters."""
