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
