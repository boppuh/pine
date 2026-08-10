"""Validated query and presentation helpers for read-only console inspection."""

from __future__ import annotations

import json
from collections.abc import Iterable
from datetime import datetime
from urllib.parse import urlencode

from pydantic import BaseModel, ConfigDict, Field, field_validator

from ledger.integrity import PredictionStatus, RegistrationStatus
from ledger.read_models import ResultState


class PredictionListQuery(BaseModel):
    """Closed browser query contract for the prediction list."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    cursor: str | None = Field(default=None, min_length=1, max_length=4096)
    registration_status: RegistrationStatus | None = None
    status: PredictionStatus | None = None
    strategy_id: str | None = Field(default=None, min_length=1, max_length=256)
    result_state: ResultState | None = None

    @field_validator(
        "cursor",
        "registration_status",
        "status",
        "strategy_id",
        "result_state",
        mode="before",
    )
    @classmethod
    def empty_values_are_unset(cls, value: object) -> object:
        return None if value == "" else value

    @field_validator("cursor", "strategy_id")
    @classmethod
    def strings_are_normalized(cls, value: str | None) -> str | None:
        if value is not None and (value != value.strip() or "\x00" in value):
            raise ValueError("query values must be normalized")
        return value

    def filter_values(self) -> dict[str, str]:
        """Return stable strings used to re-render the filter controls."""

        return {
            "registration_status": (
                "" if self.registration_status is None else self.registration_status.value
            ),
            "status": "" if self.status is None else self.status.value,
            "strategy_id": self.strategy_id or "",
            "result_state": "" if self.result_state is None else self.result_state.value,
        }

    def page_url(self, *, cursor: str | None = None) -> str:
        """Build a same-origin list URL without carrying an obsolete cursor."""

        parameters = [(name, value) for name, value in self.filter_values().items() if value]
        if cursor is not None:
            parameters.append(("cursor", cursor))
        query = urlencode(parameters)
        return "/predictions" if not query else f"/predictions?{query}"


def parse_prediction_query(pairs: Iterable[tuple[str, str]]) -> PredictionListQuery:
    """Reject duplicate and unknown browser query parameters before validation."""

    values: dict[str, str] = {}
    for name, value in pairs:
        if name in values:
            raise ValueError("prediction query parameters must be unique")
        values[name] = value
    return PredictionListQuery.model_validate(values)


def display_timestamp(value: datetime | None) -> str:
    """Format an authoritative timestamp without implying local timezone semantics."""

    if value is None:
        return "Not recorded"
    return value.isoformat(timespec="seconds")


def display_json(value: object | None) -> str:
    """Render bounded structured context as escaped, deterministic plain text."""

    if value is None:
        return "Not recorded"
    return json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True)
