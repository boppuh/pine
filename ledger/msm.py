"""Strict adapter from MSM point-in-time snapshots to the ledger boundary."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, date, datetime
from typing import Annotated, Any, Literal, Protocol, Self

from pydantic import BaseModel, ConfigDict, Field, JsonValue, field_validator, model_validator

from ledger.json_utils import sha256_json
from ledger.snapshot import PendingPrediction

Sha256 = Annotated[str, Field(pattern=r"^sha256:[0-9a-f]{64}$")]
GitCommit = Annotated[str, Field(pattern=r"^[0-9a-f]{40,64}$")]
Version = Annotated[str, Field(min_length=1, max_length=256)]


class _SnapshotModel(BaseModel):
    """Frozen, closed Pydantic base for MSM's local connector contract."""

    model_config = ConfigDict(
        allow_inf_nan=False,
        extra="forbid",
        frozen=True,
    )


class SnapshotDateWindow(_SnapshotModel):
    """Inclusive date window embedded in a strategy snapshot."""

    start: date
    end: date

    @model_validator(mode="after")
    def end_must_not_precede_start(self) -> Self:
        if self.end < self.start:
            raise ValueError("end must be on or after start")
        return self


class UniverseDefinition(_SnapshotModel):
    """Exact active universe or an explicit all-minus-exclusions rule."""

    mode: Literal["explicit", "all_available_except"]
    symbols: list[Annotated[str, Field(min_length=1)]]
    excluded_symbols: list[Annotated[str, Field(min_length=1)]]

    @model_validator(mode="after")
    def symbols_are_unambiguous(self) -> Self:
        if len(self.symbols) != len(set(self.symbols)):
            raise ValueError("symbols must be unique")
        if len(self.excluded_symbols) != len(set(self.excluded_symbols)):
            raise ValueError("excluded_symbols must be unique")
        if self.mode == "explicit" and not self.symbols:
            raise ValueError("an explicit universe must contain symbols")
        if set(self.symbols) & set(self.excluded_symbols):
            raise ValueError("active and excluded symbols must be disjoint")
        return self


class DatasetPart(_SnapshotModel):
    """Identity of one active ClickHouse part at decision time."""

    name: Annotated[str, Field(min_length=1)]
    partition_id: Annotated[str, Field(min_length=1)]
    rows: int = Field(ge=0)
    bytes_on_disk: int = Field(ge=0)
    min_date: date
    max_date: date
    min_block_number: int = Field(ge=0)
    max_block_number: int = Field(ge=0)
    level: int = Field(ge=0)
    data_version: int = Field(ge=0)
    modification_epoch: int = Field(ge=0)

    @model_validator(mode="after")
    def ranges_are_ordered(self) -> Self:
        if self.max_date < self.min_date:
            raise ValueError("part max_date cannot precede min_date")
        if self.max_block_number < self.min_block_number:
            raise ValueError("part max_block_number cannot precede min_block_number")
        return self


class DatasetTable(_SnapshotModel):
    """Self-checking aggregate of active parts for one ClickHouse table."""

    table: Annotated[str, Field(pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")]
    part_count: int = Field(gt=0)
    row_count: int = Field(ge=0)
    min_date: date
    max_date: date
    parts: list[DatasetPart] = Field(min_length=1)

    @model_validator(mode="after")
    def summary_matches_parts(self) -> Self:
        if self.part_count != len(self.parts):
            raise ValueError("part_count does not match parts")
        if self.row_count != sum(part.rows for part in self.parts):
            raise ValueError("row_count does not match parts")
        if self.min_date != min(part.min_date for part in self.parts):
            raise ValueError("min_date does not match parts")
        if self.max_date != max(part.max_date for part in self.parts):
            raise ValueError("max_date does not match parts")
        names = [part.name for part in self.parts]
        if len(names) != len(set(names)):
            raise ValueError("part names must be unique within a table")
        expected_order = sorted(self.parts, key=lambda part: (part.partition_id, part.name))
        if self.parts != expected_order:
            raise ValueError("parts must be ordered by partition_id and name")
        return self


class DatasetManifest(_SnapshotModel):
    """Physical ClickHouse state covered by ``dataset_version``."""

    database: Annotated[str, Field(pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")]
    server_version: Version
    window: SnapshotDateWindow
    tables: list[DatasetTable] = Field(min_length=1)

    @model_validator(mode="after")
    def tables_are_unique_and_cover_window(self) -> Self:
        names = [table.table for table in self.tables]
        if len(names) != len(set(names)):
            raise ValueError("dataset manifest table names must be unique")
        for table in self.tables:
            if table.min_date > self.window.start or table.max_date < self.window.end:
                raise ValueError(f"{table.table} does not cover the dataset window")
        return self


class StrategySnapshot(_SnapshotModel):
    """Validated snapshot MSM must provide before a strategy run."""

    snapshot_format_version: Literal[1]
    strategy_id: Annotated[str, Field(min_length=1)]
    strategy_spec_hash: Sha256
    git_commit: GitCommit
    parameter_set: dict[str, JsonValue] = Field(min_length=1)
    parameter_count: int = Field(gt=0)
    data_as_of_version: datetime
    dataset_version: Sha256
    dataset_manifest: DatasetManifest
    universe_definition: UniverseDefinition
    in_sample_window: SnapshotDateWindow
    out_of_sample_window: SnapshotDateWindow
    cost_model_version: Version
    slippage_model_version: Version
    metric_definition_version: Version
    engine_version: Version
    random_seed: int
    captured_at: datetime

    @field_validator("data_as_of_version", "captured_at")
    @classmethod
    def timestamps_are_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("snapshot timestamps must be timezone-aware")
        return value

    @model_validator(mode="after")
    def cross_field_invariants_hold(self) -> Self:
        if self.parameter_count != len(self.parameter_set):
            raise ValueError("parameter_count does not match parameter_set")
        expected_dataset_window = SnapshotDateWindow(
            start=min(self.in_sample_window.start, self.out_of_sample_window.start),
            end=max(self.in_sample_window.end, self.out_of_sample_window.end),
        )
        if self.dataset_manifest.window != expected_dataset_window:
            raise ValueError("dataset manifest window does not match research windows")
        expected_dataset_version = sha256_json(self.dataset_manifest.model_dump(mode="json"))
        if self.dataset_version != expected_dataset_version:
            raise ValueError("dataset_version does not match dataset_manifest")
        if self.captured_at.astimezone(UTC) < self.data_as_of_version.astimezone(UTC):
            raise ValueError("captured_at cannot precede data_as_of_version")
        return self


class MSMSnapshotSource(Protocol):
    """Local MSM API consumed by ``MSMSnapshotProvider``."""

    def capture_snapshot(
        self,
        *,
        strategy_id: str,
        decision_at: datetime,
        in_sample_window: Mapping[str, date],
        out_of_sample_window: Mapping[str, date],
    ) -> Mapping[str, Any]:
        """Capture source state without invoking a backtest."""

        ...


class MSMSnapshotProvider:
    """Validate MSM output against the ledger's strict snapshot contract."""

    def __init__(self, source: MSMSnapshotSource) -> None:
        self.source = source

    def capture_snapshot(
        self,
        prediction: PendingPrediction,
        at: datetime,
    ) -> Mapping[str, JsonValue]:
        """Capture and bind MSM state to the pending forecast and decision time."""

        if prediction.schema_id != "finance/strategy-edge:1":
            raise ValueError(f"unsupported MSM forecast schema {prediction.schema_id!r}")
        forecast = prediction.forecast
        raw_snapshot = self.source.capture_snapshot(
            strategy_id=forecast.strategy_id,
            decision_at=at,
            in_sample_window={
                "start": forecast.in_sample_window.start,
                "end": forecast.in_sample_window.end,
            },
            out_of_sample_window={
                "start": forecast.out_of_sample_window.start,
                "end": forecast.out_of_sample_window.end,
            },
        )
        snapshot = StrategySnapshot.model_validate(raw_snapshot)
        if snapshot.strategy_id != forecast.strategy_id:
            raise ValueError("MSM snapshot strategy_id does not match the forecast")
        if snapshot.in_sample_window != SnapshotDateWindow(
            start=forecast.in_sample_window.start,
            end=forecast.in_sample_window.end,
        ):
            raise ValueError("MSM snapshot in-sample window does not match the forecast")
        if snapshot.out_of_sample_window != SnapshotDateWindow(
            start=forecast.out_of_sample_window.start,
            end=forecast.out_of_sample_window.end,
        ):
            raise ValueError("MSM snapshot out-of-sample window does not match the forecast")
        if snapshot.data_as_of_version.astimezone(UTC) != at.astimezone(UTC):
            raise ValueError("MSM snapshot data_as_of_version does not match decision_at")
        return snapshot.model_dump(mode="json")
