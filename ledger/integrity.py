"""Pydantic types that make committed ledger state write-once."""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from datetime import UTC, date, datetime
from enum import StrEnum
from typing import Any, ClassVar, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    PrivateAttr,
    field_serializer,
    field_validator,
    model_validator,
)

from ledger.errors import IntegrityError


class RegistrationStatus(StrEnum):
    """Permanent provenance assigned exactly once when a record is created."""

    PREREGISTERED = "preregistered"
    EXPLORATORY = "exploratory"
    IMPORTED = "imported"
    UNREGISTERED_EXTERNAL = "unregistered_external"


class PredictionStatus(StrEnum):
    """Lifecycle status; changes are persisted only through ``LedgerRegistry``."""

    OPEN = "open"
    RESOLVED = "resolved"
    INVALIDATED = "invalidated"
    QUARANTINED = "quarantined"


class _WriteOnceModel(BaseModel):
    """A Pydantic model sealed immediately after successful construction."""

    model_config = ConfigDict(
        allow_inf_nan=False,
        extra="forbid",
        validate_assignment=True,
    )

    _sealed: bool = PrivateAttr(default=False)
    _integrity_fields: ClassVar[frozenset[str]] = frozenset()

    def model_post_init(self, __context: Any) -> None:
        self._sealed = True

    def __setattr__(self, name: str, value: Any) -> None:
        if name in type(self).model_fields and getattr(self, "_sealed", False):
            classification = "immutable" if name in self._integrity_fields else "registry-owned"
            raise IntegrityError(
                f"{name!r} is {classification} after record creation; "
                "persist lifecycle changes through LedgerRegistry"
            )
        super().__setattr__(name, value)

    def model_copy(
        self,
        *,
        update: Mapping[str, Any] | None = None,
        deep: bool = False,
    ) -> Self:
        """Copy safely, refusing Pydantic's otherwise-unvalidated update escape hatch."""

        if update:
            raise IntegrityError("write-once models cannot be copied with field updates")
        return super().model_copy(deep=deep)

    def copy(
        self,
        *,
        include: Any = None,
        exclude: Any = None,
        update: Mapping[str, Any] | None = None,
        deep: bool = False,
    ) -> Self:
        """Close Pydantic's deprecated copy/update path around write-once fields."""

        if include is not None or exclude is not None or update:
            raise IntegrityError("write-once models cannot be copied with field changes")
        return self.model_copy(deep=deep)


class DateWindow(_WriteOnceModel):
    """An inclusive research data window."""

    start: date
    end: date

    @model_validator(mode="after")
    def end_must_not_precede_start(self) -> DateWindow:
        if self.end < self.start:
            raise ValueError("end must be on or after start")
        return self


class ExpectedMetrics(_WriteOnceModel):
    """Expected strategy metrics committed before a run."""

    sharpe: float
    win_rate: float = Field(ge=0, le=1)
    max_drawdown: float
    expectancy: float


class StrategyEdgeForecast(_WriteOnceModel):
    """Pydantic v2 runtime representation of ``finance/strategy-edge:1``."""

    strategy_id: str = Field(min_length=1)
    expected_metrics: ExpectedMetrics
    in_sample_window: DateWindow
    out_of_sample_window: DateWindow
    invalidation: str = Field(min_length=1)
    edge_source: str = Field(min_length=1)


def _freeze(value: JsonValue) -> JsonValue | FrozenDict | FrozenList:
    if isinstance(value, dict):
        return FrozenDict(value)
    if isinstance(value, list):
        return FrozenList(value)
    return value


def _thaw(value: Any) -> JsonValue:
    if isinstance(value, FrozenDict):
        return {key: _thaw(item) for key, item in value.items()}
    if isinstance(value, FrozenList):
        return [_thaw(item) for item in value]
    return value


class FrozenList(Sequence[Any]):
    """A recursively immutable JSON array with integrity-specific failures."""

    __slots__ = ("_data",)

    def __init__(self, value: Sequence[JsonValue]) -> None:
        self._data = tuple(_freeze(item) for item in value)

    def __getitem__(self, index: int | slice) -> Any:
        return self._data[index]

    def __len__(self) -> int:
        return len(self._data)

    def __iter__(self) -> Iterator[Any]:
        return iter(self._data)

    def __setitem__(self, index: int | slice, value: Any) -> None:
        raise IntegrityError("committed JSON arrays are immutable")

    def __delitem__(self, index: int | slice) -> None:
        raise IntegrityError("committed JSON arrays are immutable")

    def append(self, value: Any) -> None:
        raise IntegrityError("committed JSON arrays are immutable")

    def extend(self, values: Sequence[Any]) -> None:
        raise IntegrityError("committed JSON arrays are immutable")

    def insert(self, index: int, value: Any) -> None:
        raise IntegrityError("committed JSON arrays are immutable")

    def pop(self, index: int = -1) -> None:
        raise IntegrityError("committed JSON arrays are immutable")

    def remove(self, value: Any) -> None:
        raise IntegrityError("committed JSON arrays are immutable")

    def clear(self) -> None:
        raise IntegrityError("committed JSON arrays are immutable")


class FrozenDict(Mapping[str, Any]):
    """A recursively immutable JSON object used inside committed records."""

    __slots__ = ("_data",)

    def __init__(self, value: Mapping[str, JsonValue]) -> None:
        self._data = {str(key): _freeze(item) for key, item in value.items()}

    def __getitem__(self, key: str) -> Any:
        return self._data[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)

    def __repr__(self) -> str:
        return f"FrozenDict({self._data!r})"

    def __setitem__(self, key: str, value: Any) -> None:
        raise IntegrityError("committed JSON objects are immutable")

    def __delitem__(self, key: str) -> None:
        raise IntegrityError("committed JSON objects are immutable")

    def clear(self) -> None:
        raise IntegrityError("committed JSON objects are immutable")

    def pop(self, key: str, default: Any = None) -> None:
        raise IntegrityError("committed JSON objects are immutable")

    def popitem(self) -> None:
        raise IntegrityError("committed JSON objects are immutable")

    def setdefault(self, key: str, default: Any = None) -> None:
        raise IntegrityError("committed JSON objects are immutable")

    def update(self, *args: Any, **kwargs: Any) -> None:
        raise IntegrityError("committed JSON objects are immutable")

    def to_dict(self) -> dict[str, JsonValue]:
        """Return a detached, JSON-compatible copy."""

        thawed = _thaw(self)
        assert isinstance(thawed, dict)
        return thawed


def _validate_identifier(value: str) -> str:
    if not value or len(value) > 128:
        raise ValueError("identifier must contain 1..128 characters")
    if any(character in value for character in ("/", "\\", "\x00")):
        raise ValueError("identifier cannot contain path separators")
    return value


class PredictionDraft(_WriteOnceModel):
    """Validated, pre-write input. Registration status has no transition API."""

    _integrity_fields = frozenset(
        {"registration_status", "forecast", "decision", "snapshot", "lineage"}
    )

    prediction_id: str
    run_id: str
    schema_id: str = "finance/strategy-edge:1"
    registration_status: RegistrationStatus
    forecast: StrategyEdgeForecast
    decision: str = Field(min_length=1)
    snapshot: dict[str, JsonValue] = Field(min_length=1)
    lineage: dict[str, JsonValue] = Field(default_factory=dict)
    body: str = ""
    status: PredictionStatus = PredictionStatus.OPEN
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    _ids_are_safe = field_validator("prediction_id", "run_id")(_validate_identifier)

    @field_validator("created_at")
    @classmethod
    def created_at_must_be_timezone_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("created_at must be timezone-aware")
        return value


class PreregisteredCaptureRequest(_WriteOnceModel):
    """Structured, pre-snapshot input for the confirmatory capture path.

    Registration status is intentionally absent: this type can only create a new
    preregistered record and therefore cannot promote an exploratory record.
    """

    _integrity_fields = frozenset({"expected_schema_hash", "forecast", "decision", "lineage"})

    idempotency_key: str = Field(min_length=1, max_length=256)
    schema_id: str = "finance/strategy-edge:1"
    expected_schema_hash: str | None = Field(
        default=None,
        pattern=r"^sha256:[0-9a-f]{64}$",
    )
    forecast: StrategyEdgeForecast
    decision: str = Field(min_length=1)
    lineage: dict[str, JsonValue]
    body: str = ""

    @field_validator("lineage")
    @classmethod
    def lineage_requires_family_id(cls, value: dict[str, JsonValue]) -> dict[str, JsonValue]:
        family_id = value.get("family_id")
        if not isinstance(family_id, str) or not family_id.strip():
            raise ValueError("lineage.family_id must be a non-empty string")
        return value


class CommittedPrediction(_WriteOnceModel):
    """Sealed representation of the exact state committed by ``LedgerWriter``."""

    model_config = ConfigDict(
        allow_inf_nan=False,
        extra="forbid",
        validate_assignment=True,
        arbitrary_types_allowed=True,
    )

    _integrity_fields = frozenset(
        {
            "prediction_id",
            "run_id",
            "schema_id",
            "registration_status",
            "forecast",
            "decision",
            "snapshot",
            "snapshot_ref",
            "schema_hash",
            "lineage",
            "immutable_hash",
            "body",
            "created_at",
            "committed_at",
        }
    )

    prediction_id: str
    run_id: str
    schema_id: str
    schema_hash: str
    registration_status: RegistrationStatus
    forecast: StrategyEdgeForecast
    decision: str
    snapshot: FrozenDict
    snapshot_ref: str
    lineage: FrozenDict
    immutable_hash: str
    body: str
    status: PredictionStatus
    outcome: FrozenDict | None = None
    grade: FrozenDict | None = None
    resolution_metadata: FrozenDict | None = None
    created_at: datetime
    committed_at: datetime

    _ids_are_safe = field_validator("prediction_id", "run_id")(_validate_identifier)

    @field_validator(
        "snapshot", "lineage", "outcome", "grade", "resolution_metadata", mode="before"
    )
    @classmethod
    def json_objects_are_frozen(cls, value: Any) -> FrozenDict | None:
        if value is None or isinstance(value, FrozenDict):
            return value
        if not isinstance(value, Mapping):
            raise TypeError("committed JSON values must be objects")
        return FrozenDict(value)

    @field_serializer("snapshot", "lineage", "outcome", "grade", "resolution_metadata")
    def serialize_frozen_dict(self, value: FrozenDict | None) -> dict[str, JsonValue] | None:
        return None if value is None else value.to_dict()


IMMUTABLE_RECORD_FIELDS: frozenset[str] = frozenset(
    {
        "registration_status",
        "forecast",
        "decision",
        "snapshot",
        "schema_hash",
        "lineage",
    }
)


def immutable_payload(
    draft: PredictionDraft,
    *,
    schema_hash: str,
    snapshot_ref: str,
) -> dict[str, JsonValue]:
    """Build the canonical payload covered by the registry's immutable hash."""

    return {
        "registration_status": draft.registration_status.value,
        "forecast": draft.forecast.model_dump(mode="json"),
        "decision": draft.decision,
        "snapshot": draft.snapshot,
        "snapshot_ref": snapshot_ref,
        "schema_hash": schema_hash,
        "lineage": draft.lineage,
    }
