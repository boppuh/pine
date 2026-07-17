"""Side-effect-free strategy-hypothesis extraction boundary."""

from __future__ import annotations

import copy
import logging
from collections.abc import Mapping
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal, Protocol, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    ValidationError,
    field_validator,
    model_validator,
)

from ledger.integrity import RegistrationStatus, StrategyEdgeForecast
from ledger.registry import LedgerRegistry
from ledger.schema_registry import SchemaRegistry

logger = logging.getLogger(__name__)


class ExtractionStatus(StrEnum):
    """Whether a free-text note produced a capture-ready structured proposal."""

    READY = "ready"
    UNABLE = "unable"


class HypothesisExtractionRequest(BaseModel):
    """Free text and schema selection supplied by a capture surface."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    text: str = Field(min_length=1, max_length=200_000)
    schema_id: str = Field(default="finance/strategy-edge:1", min_length=1, max_length=256)


class ExtractedHypothesis(BaseModel):
    """Provider output before authoritative schema and fresh-window checks."""

    model_config = ConfigDict(allow_inf_nan=False, extra="forbid")

    forecast: StrategyEdgeForecast
    decision: str = Field(min_length=1)
    lineage: dict[str, JsonValue]

    @field_validator("lineage")
    @classmethod
    def lineage_requires_family_id(cls, value: dict[str, JsonValue]) -> dict[str, JsonValue]:
        family_id = value.get("family_id")
        if not isinstance(family_id, str) or not family_id.strip():
            raise ValueError("lineage.family_id must be a non-empty string")
        return value


class DraftProposal(BaseModel):
    """Validated proposal shown to a user before confirmation has side effects."""

    model_config = ConfigDict(allow_inf_nan=False, extra="forbid")

    schema_id: str
    schema_hash: str
    registration_status: Literal[RegistrationStatus.PREREGISTERED] = (
        RegistrationStatus.PREREGISTERED
    )
    forecast: StrategyEdgeForecast
    decision: str
    lineage: dict[str, JsonValue]
    body: str
    fresh_window: bool


class ExtractionResult(BaseModel):
    """Explicit success or clean failure; invalid proposals are never returned ready."""

    model_config = ConfigDict(extra="forbid")

    status: ExtractionStatus
    proposal: DraftProposal | None = None
    errors: tuple[str, ...] = ()

    @model_validator(mode="after")
    def status_matches_payload(self) -> Self:
        if self.status is ExtractionStatus.READY:
            if self.proposal is None or self.errors:
                raise ValueError("ready extraction requires one proposal and no errors")
        elif self.proposal is not None or not self.errors:
            raise ValueError("unable extraction requires errors and no proposal")
        return self


class HypothesisExtractor(Protocol):
    """Future frontier-model adapter that returns structured data only."""

    async def extract(
        self,
        text: str,
        *,
        schema_id: str,
        schema: Mapping[str, Any],
    ) -> Mapping[str, Any] | None:
        """Return one candidate mapping, or ``None`` when extraction is incomplete."""

        ...


class ExtractionService:
    """Validate provider output and attach advisory fresh-window state."""

    def __init__(
        self,
        vault_root: str | Path,
        extractor: HypothesisExtractor,
        *,
        schema_registry: SchemaRegistry | None = None,
        registry: LedgerRegistry | None = None,
    ) -> None:
        self.vault_root = Path(vault_root).resolve()
        self.ledger_dir = self.vault_root / ".ledger"
        self.extractor = extractor
        self.schema_registry = schema_registry or SchemaRegistry(self.ledger_dir / "schemas")
        self.registry = registry or LedgerRegistry(self.ledger_dir / "registry.db")
        if self.schema_registry.schemas_dir.resolve() != (self.ledger_dir / "schemas").resolve():
            raise ValueError("schema registry and extraction service must use the same vault")
        if self.registry.db_path.resolve() != (self.ledger_dir / "registry.db").resolve():
            raise ValueError("registry and extraction service must use the same vault")

    async def propose(
        self,
        request: HypothesisExtractionRequest | Mapping[str, Any],
    ) -> ExtractionResult:
        """Produce a validated draft without writing ledger or snapshot state."""

        extraction_request = (
            request
            if isinstance(request, HypothesisExtractionRequest)
            else HypothesisExtractionRequest.model_validate(request)
        )
        schema = self.schema_registry.load(extraction_request.schema_id)
        try:
            candidate_data = await self.extractor.extract(
                extraction_request.text,
                schema_id=extraction_request.schema_id,
                schema=copy.deepcopy(schema),
            )
        except Exception:
            logger.exception(
                "hypothesis_extraction_provider_failed",
                extra={"schema_id": extraction_request.schema_id},
            )
            return ExtractionResult(
                status=ExtractionStatus.UNABLE,
                errors=("extraction provider failed",),
            )
        if candidate_data is None:
            return ExtractionResult(
                status=ExtractionStatus.UNABLE,
                errors=("unable to extract a complete strategy hypothesis",),
            )
        try:
            candidate = ExtractedHypothesis.model_validate(candidate_data)
        except ValidationError as exc:
            return ExtractionResult(
                status=ExtractionStatus.UNABLE,
                errors=tuple(_validation_errors(exc)),
            )

        forecast = candidate.forecast.model_dump(mode="json")
        valid, errors = self.schema_registry.validate_schema(forecast, schema)
        if not valid:
            return ExtractionResult(
                status=ExtractionStatus.UNABLE,
                errors=tuple(errors),
            )
        family_id = candidate.lineage["family_id"]
        assert isinstance(family_id, str)
        window = candidate.forecast.out_of_sample_window
        fresh_window = not self.registry.window_overlaps_touched(
            family_id,
            window.start,
            window.end,
        )
        proposal = DraftProposal(
            schema_id=extraction_request.schema_id,
            schema_hash=self.schema_registry.hash(schema),
            forecast=candidate.forecast,
            decision=candidate.decision,
            lineage=candidate.lineage,
            body=extraction_request.text,
            fresh_window=fresh_window,
        )
        logger.info(
            "hypothesis_extraction_ready",
            extra={
                "schema_id": proposal.schema_id,
                "strategy_id": proposal.forecast.strategy_id,
                "fresh_window": proposal.fresh_window,
            },
        )
        return ExtractionResult(status=ExtractionStatus.READY, proposal=proposal)


def _validation_errors(exc: ValidationError) -> list[str]:
    return [
        f"$.{'.'.join(str(item) for item in error['loc'])}: {error['msg']}"
        for error in exc.errors()
    ]
