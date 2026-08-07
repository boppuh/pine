"""Strict, non-authoritative projections for verified ledger reads."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Literal

from pydantic import BaseModel, ConfigDict, JsonValue

from ledger.integrity import PredictionStatus, RegistrationStatus, StrategyEdgeForecast
from ledger.msm import GitCommit, Sha256, SnapshotDateWindow, Version
from ledger.results import (
    MetricUnits,
    MSMRegimeResult,
    MSMResultArtifactEvidence,
    MSMResultMetrics,
)
from ledger.run import RunState


class _ReadModel(BaseModel):
    """Frozen, closed base for data returned by the backend read API."""

    model_config = ConfigDict(allow_inf_nan=False, extra="forbid", frozen=True)


class IntegrityState(StrEnum):
    """Whether every evidence layer used by a projection was verified."""

    VERIFIED = "verified"
    FAILED = "failed"


class IntegrityReason(StrEnum):
    """Safe, non-sensitive reason for a failed summary projection."""

    REGISTRY_UNVERIFIED = "registry_unverified"
    RECORD_UNVERIFIED = "record_unverified"
    SCHEMA_UNVERIFIED = "schema_unverified"
    SNAPSHOT_UNVERIFIED = "snapshot_unverified"
    RUN_UNVERIFIED = "run_unverified"
    RESULT_UNVERIFIED = "result_unverified"
    QUARANTINED = "quarantined"


class ResultState(StrEnum):
    """Whether immutable result evidence exists for a prediction run."""

    PRESENT = "present"
    ABSENT = "absent"


class PredictionSummary(_ReadModel):
    """One safe prediction-list item."""

    prediction_id: str
    run_id: str
    registration_status: RegistrationStatus
    status: PredictionStatus
    transaction_state: Literal["committed"]
    strategy_id: str | None
    schema_id: str
    out_of_sample_window: SnapshotDateWindow | None
    created_at: datetime | None
    committed_at: datetime | None
    run_state: RunState | None
    result_state: ResultState
    integrity_state: IntegrityState
    integrity_reason: IntegrityReason | None = None


class PredictionPage(_ReadModel):
    """A stable keyset-paginated prediction page."""

    items: tuple[PredictionSummary, ...]
    next_cursor: str | None


class RunBindingProjection(_ReadModel):
    """Verified immutable binding attached when a run is first executed."""

    idempotency_key: str
    request_hash: str
    registration_status: RegistrationStatus
    strategy_id: str
    dataset_version: str
    envelope_hash: str
    bound_at: datetime


class RunProjection(_ReadModel):
    """Allocated run identity and lifecycle for a committed prediction."""

    run_id: str
    prediction_id: str
    started_at: datetime
    state: RunState
    execution_started_at: datetime | None
    completed_at: datetime | None
    exit_code: int | None
    failure_note: str | None
    binding: RunBindingProjection | None


class ResultEvidenceProjection(_ReadModel):
    """Verified stored MSM result evidence and its registry identity."""

    evidence_hash: str
    source_timestamp: datetime
    ingested_at: datetime
    metric_units: MetricUnits
    in_sample_metrics: MSMResultMetrics
    out_of_sample_metrics: MSMResultMetrics
    regime_breakdown: tuple[MSMRegimeResult, ...]
    artifacts: tuple[MSMResultArtifactEvidence, ...]


class IntegrityViolationProjection(_ReadModel):
    """Previously recorded immutable integrity warning."""

    field: str
    detected_at: datetime
    note: str


class SnapshotProvenance(_ReadModel):
    """Selected immutable snapshot identity without the full data-part manifest."""

    strategy_id: str
    strategy_spec_hash: Sha256
    git_commit: GitCommit
    parameter_count: int
    data_as_of_version: datetime
    dataset_version: Sha256
    in_sample_window: SnapshotDateWindow
    out_of_sample_window: SnapshotDateWindow
    cost_model_version: Version
    slippage_model_version: Version
    metric_definition_version: Version
    engine_version: Version
    random_seed: int
    captured_at: datetime


class PredictionDetail(_ReadModel):
    """Fully verified committed prediction projection."""

    prediction_id: str
    run_id: str
    schema_id: str
    schema_hash: str
    registration_status: RegistrationStatus
    forecast: StrategyEdgeForecast
    decision: str
    snapshot_ref: str
    snapshot: SnapshotProvenance
    lineage: dict[str, JsonValue]
    immutable_hash: str
    body: str
    status: PredictionStatus
    outcome: dict[str, JsonValue] | None
    grade: dict[str, JsonValue] | None
    resolution_metadata: dict[str, JsonValue] | None
    transaction_state: Literal["committed"]
    created_at: datetime
    committed_at: datetime
    run: RunProjection
    result: ResultEvidenceProjection | None
    integrity_violations: tuple[IntegrityViolationProjection, ...]
    integrity_state: Literal[IntegrityState.VERIFIED] = IntegrityState.VERIFIED


class LedgerStatus(_ReadModel):
    """Non-secret registry readiness metadata for the console."""

    status: Literal["ok"] = "ok"
    api_version: Literal["v1"] = "v1"
    registry_version: int
    committed_predictions: int
    quarantined_predictions: int
    integrity_violations: int
    run_results: int
