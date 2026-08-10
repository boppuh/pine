"""Strict browser form models for the server-rendered capture flow."""

from __future__ import annotations

from datetime import date
from typing import Literal, Self
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    ValidationInfo,
    field_validator,
    model_validator,
)

from ledger.console.models import CaptureInput
from ledger.integrity import DateWindow, ExpectedMetrics, StrategyEdgeForecast

CAPTURE_SCHEMA_ID = "finance/strategy-edge:1"


class HypothesisForm(BaseModel):
    """Validated source text for a new or revised transient workflow."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    schema_id: Literal["finance/strategy-edge:1"] = CAPTURE_SCHEMA_ID
    source_text: str = Field(min_length=1, max_length=200_000)
    workflow_id: str | None = None
    version: int | None = Field(default=None, ge=0)

    @model_validator(mode="after")
    def revision_fields_are_complete(self) -> Self:
        if (self.workflow_id is None) != (self.version is None):
            raise ValueError("workflow_id and version must be supplied together")
        if self.workflow_id is not None:
            parsed = UUID(self.workflow_id)
            if parsed.version != 4 or str(parsed) != self.workflow_id:
                raise ValueError("workflow_id must be a canonical UUIDv4")
        return self


class ReviewForm(BaseModel):
    """Flat HTML fields reassembled into the shared capture model."""

    model_config = ConfigDict(
        allow_inf_nan=False,
        extra="forbid",
        str_strip_whitespace=True,
    )

    version: int = Field(ge=0)
    schema_id: Literal["finance/strategy-edge:1"] = CAPTURE_SCHEMA_ID
    strategy_id: str = Field(min_length=1)
    sharpe: float
    win_rate: float = Field(ge=0, le=1)
    max_drawdown: float
    expectancy: float
    in_sample_start: date
    in_sample_end: date
    out_of_sample_start: date
    out_of_sample_end: date
    invalidation: str = Field(min_length=1)
    edge_source: str = Field(min_length=1)
    decision: str = Field(min_length=1)
    family_id: str = Field(min_length=1)

    @field_validator("in_sample_end")
    @classmethod
    def in_sample_end_follows_start(cls, value: date, info: ValidationInfo) -> date:
        start = info.data.get("in_sample_start")
        if isinstance(start, date) and value < start:
            raise ValueError("In-sample end must be on or after its start")
        return value

    @field_validator("out_of_sample_end")
    @classmethod
    def out_of_sample_end_follows_start(cls, value: date, info: ValidationInfo) -> date:
        start = info.data.get("out_of_sample_start")
        if isinstance(start, date) and value < start:
            raise ValueError("Out-of-sample end must be on or after its start")
        return value

    def to_capture_input(
        self,
        *,
        body: str,
        schema_hash: str,
        lineage_context: dict[str, JsonValue] | None = None,
    ) -> CaptureInput:
        """Build a validated nested capture request while preserving lineage context."""

        lineage = dict(lineage_context or {})
        lineage["family_id"] = self.family_id
        return CaptureInput(
            schema_id=self.schema_id,
            schema_hash=schema_hash,
            forecast=StrategyEdgeForecast(
                strategy_id=self.strategy_id,
                expected_metrics=ExpectedMetrics(
                    sharpe=self.sharpe,
                    win_rate=self.win_rate,
                    max_drawdown=self.max_drawdown,
                    expectancy=self.expectancy,
                ),
                in_sample_window=DateWindow(
                    start=self.in_sample_start,
                    end=self.in_sample_end,
                ),
                out_of_sample_window=DateWindow(
                    start=self.out_of_sample_start,
                    end=self.out_of_sample_end,
                ),
                invalidation=self.invalidation,
                edge_source=self.edge_source,
            ),
            decision=self.decision,
            lineage=lineage,
            body=body,
        )


HYPOTHESIS_FIELDS = frozenset({"csrf_token", "schema_id", "source_text"})
HYPOTHESIS_REVISION_FIELDS = frozenset(
    {"csrf_token", "schema_id", "source_text", "workflow_id", "version"}
)
REVIEW_FIELDS = frozenset({"csrf_token", *ReviewForm.model_fields})
VERSION_FIELDS = frozenset({"csrf_token", "version"})
