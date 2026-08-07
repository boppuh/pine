"""Strict console workflow types with no ledger-write capability."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from enum import StrEnum
from typing import Any, Self
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    field_serializer,
    field_validator,
    model_validator,
)

from ledger.api import CaptureResponse
from ledger.errors import IntegrityError
from ledger.extraction import DraftProposal
from ledger.integrity import FrozenDict, PreregisteredCaptureRequest, StrategyEdgeForecast


class WorkflowState(StrEnum):
    """Durable states for one console capture workflow."""

    EDITING = "editing"
    EXTRACTING = "extracting"
    REVIEWING = "reviewing"
    FROZEN = "frozen"
    SUBMITTING = "submitting"
    UNCERTAIN = "uncertain"
    RETRYABLE_FAILURE = "retryable_failure"
    TERMINAL_FAILURE = "terminal_failure"
    COMMITTED = "committed"
    CANCELLED = "cancelled"


RECONCILIATION_STATES = frozenset(
    {
        WorkflowState.FROZEN,
        WorkflowState.SUBMITTING,
        WorkflowState.UNCERTAIN,
        WorkflowState.RETRYABLE_FAILURE,
    }
)


class _ImmutableConsoleModel(BaseModel):
    """Close Pydantic copy/update escape hatches around frozen workflow evidence."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    def model_copy(
        self,
        *,
        update: Mapping[str, Any] | None = None,
        deep: bool = False,
    ) -> Self:
        if update:
            raise IntegrityError("frozen console models cannot be copied with field updates")
        return super().model_copy(deep=deep)

    def copy(
        self,
        *,
        include: Any = None,
        exclude: Any = None,
        update: Mapping[str, Any] | None = None,
        deep: bool = False,
    ) -> Self:
        if include is not None or exclude is not None or update:
            raise IntegrityError("frozen console models cannot be copied with field changes")
        return self.model_copy(deep=deep)


class CaptureInput(BaseModel):
    """Editable confirmation fields before the server freezes an exact request."""

    model_config = ConfigDict(allow_inf_nan=False, extra="forbid", frozen=True)

    schema_id: str = Field(default="finance/strategy-edge:1", min_length=1, max_length=256)
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

    def freeze(self, idempotency_key: str) -> FrozenCaptureRequest:
        """Build the only backend write request allowed by the console core."""

        request = PreregisteredCaptureRequest(
            idempotency_key=idempotency_key,
            schema_id=self.schema_id,
            forecast=self.forecast,
            decision=self.decision,
            lineage=self.lineage,
            body=self.body,
        )
        return FrozenCaptureRequest.model_validate(request.model_dump(mode="json"))


class FrozenCaptureRequest(_ImmutableConsoleModel):
    """Deeply immutable canonical request recovered for every capture attempt."""

    model_config = ConfigDict(
        allow_inf_nan=False,
        arbitrary_types_allowed=True,
        extra="forbid",
        frozen=True,
    )

    idempotency_key: str = Field(min_length=1, max_length=256)
    schema_id: str = Field(min_length=1, max_length=256)
    forecast: StrategyEdgeForecast
    decision: str = Field(min_length=1)
    lineage: FrozenDict
    body: str = ""

    @field_validator("lineage", mode="before")
    @classmethod
    def lineage_is_frozen(cls, value: object) -> FrozenDict:
        if isinstance(value, FrozenDict):
            return value
        if not isinstance(value, dict):
            raise ValueError("lineage must be a JSON object")
        frozen = FrozenDict(value)
        family_id = frozen.get("family_id")
        if not isinstance(family_id, str) or not family_id.strip():
            raise ValueError("lineage.family_id must be a non-empty string")
        return frozen

    @field_validator("idempotency_key")
    @classmethod
    def idempotency_key_is_console_owned(cls, value: str) -> str:
        return _validate_console_idempotency_key(value)

    @field_serializer("lineage")
    def serialize_lineage(self, value: FrozenDict) -> dict[str, JsonValue]:
        return value.to_dict()

    def to_backend_request(self) -> PreregisteredCaptureRequest:
        """Return a detached backend model from the canonical frozen values."""

        return PreregisteredCaptureRequest.model_validate(self.model_dump(mode="json"))


class FrozenCaptureResponse(_ImmutableConsoleModel):
    """Immutable receipt retained temporarily after authoritative capture."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    prediction_id: str
    run_id: str
    record_ref: str
    snapshot_ref: str
    schema_id: str
    schema_hash: str
    immutable_hash: str
    created: bool

    @classmethod
    def from_backend(cls, value: CaptureResponse) -> FrozenCaptureResponse:
        return cls.model_validate(value.model_dump(mode="json"))


class ConsoleWorkflow(_ImmutableConsoleModel):
    """Validated projection of one row in the separate console database."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    workflow_id: str
    user_id: str
    state: WorkflowState
    schema_id: str
    source_text: str | None
    proposal: DraftProposal | None = None
    idempotency_key: str
    frozen_request: FrozenCaptureRequest | None = None
    capture_response: FrozenCaptureResponse | None = None
    error_code: str | None = None
    error_details: dict[str, JsonValue] | None = None
    created_at: datetime
    updated_at: datetime
    expires_at: datetime | None
    version: int = Field(ge=0)

    @field_validator("workflow_id")
    @classmethod
    def workflow_id_is_uuid4(cls, value: str) -> str:
        parsed = UUID(value)
        if parsed.version != 4 or str(parsed) != value:
            raise ValueError("workflow_id must be a canonical UUIDv4")
        return value

    @field_validator("idempotency_key")
    @classmethod
    def idempotency_key_is_console_uuid(cls, value: str) -> str:
        return _validate_console_idempotency_key(value)

    @field_validator("created_at", "updated_at", "expires_at")
    @classmethod
    def timestamps_are_aware(cls, value: datetime | None) -> datetime | None:
        if value is not None and (value.tzinfo is None or value.utcoffset() is None):
            raise ValueError("console timestamps must be timezone-aware")
        return value

    @model_validator(mode="after")
    def state_matches_durable_payload(self) -> Self:
        frozen_states = {
            WorkflowState.FROZEN,
            WorkflowState.SUBMITTING,
            WorkflowState.UNCERTAIN,
            WorkflowState.RETRYABLE_FAILURE,
            WorkflowState.TERMINAL_FAILURE,
            WorkflowState.COMMITTED,
        }
        if self.state in frozen_states and self.frozen_request is None:
            raise ValueError("frozen workflow state requires an exact capture request")
        if self.state is WorkflowState.REVIEWING and self.proposal is None:
            raise ValueError("reviewing workflow requires a proposal")
        if self.state is WorkflowState.COMMITTED and self.capture_response is None:
            raise ValueError("committed workflow requires an immutable receipt")
        if self.state is not WorkflowState.COMMITTED and self.capture_response is not None:
            raise ValueError("only committed workflows may contain a receipt")
        if self.state in RECONCILIATION_STATES and self.expires_at is not None:
            raise ValueError("reconciliation workflow must not expire")
        if self.state is WorkflowState.TERMINAL_FAILURE and self.expires_at is None:
            raise ValueError("terminal failure workflow must expire")
        if self.state is WorkflowState.CANCELLED and (
            self.source_text is not None or self.proposal is not None
        ):
            raise ValueError("cancelled workflows must discard transient content")
        return self


def _validate_console_idempotency_key(value: str) -> str:
    if not value.startswith("console-"):
        raise ValueError("idempotency_key must be console-owned")
    try:
        parsed = UUID(value.removeprefix("console-"))
    except ValueError as exc:
        raise ValueError("idempotency_key must contain a canonical UUIDv4") from exc
    if parsed.version != 4 or f"console-{parsed}" != value:
        raise ValueError("idempotency_key must contain a canonical UUIDv4")
    return value
