"""Snapshot-provider boundary for pre-run strategy state."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from pydantic import JsonValue

from ledger.integrity import FrozenDict, RegistrationStatus, StrategyEdgeForecast


@dataclass(frozen=True, slots=True)
class PendingPrediction:
    """The immutable prediction context available before a snapshot is captured."""

    prediction_id: str
    run_id: str
    schema_id: str
    registration_status: RegistrationStatus
    forecast: StrategyEdgeForecast
    decision: str
    lineage: FrozenDict
    created_at: datetime


class SnapshotProvider(Protocol):
    """Capture frozen local state before a backtest executes."""

    def capture_snapshot(
        self,
        prediction: PendingPrediction,
        at: datetime,
    ) -> Mapping[str, JsonValue]:
        """Return JSON-compatible state available at ``at`` without running a test."""

        ...
