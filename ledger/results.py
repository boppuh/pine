"""Immutable result evidence for successful ledger-bound MSM runs."""

from __future__ import annotations

import json
import logging
import sqlite3
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, JsonValue, field_validator, model_validator

from ledger.errors import IdempotencyConflictError, IntegrityError
from ledger.integrity import RegistrationStatus
from ledger.json_utils import canonical_json, sha256_json
from ledger.locking import ledger_lock
from ledger.msm import GitCommit, Sha256, SnapshotDateWindow, StrategySnapshot, Version
from ledger.registry import LedgerRegistry
from ledger.run import RunState

logger = logging.getLogger(__name__)

MetricUnits = Literal["finance/strategy-edge:decimal-v1"]
ResultSample = Literal["in_sample", "out_of_sample"]


class _ResultEvidenceModel(BaseModel):
    """Frozen, closed model for MSM result evidence."""

    model_config = ConfigDict(allow_inf_nan=False, extra="forbid", frozen=True)


class MSMResultMetrics(_ResultEvidenceModel):
    """Realized strategy metrics in canonical decimal units."""

    sharpe: float = Field(strict=True)
    win_rate: float = Field(ge=0, le=1, strict=True)
    max_drawdown: float = Field(ge=0, strict=True)
    expectancy: float = Field(strict=True)
    total_return: float = Field(strict=True)
    trade_count: int = Field(ge=0, strict=True)


class MSMRegimeResult(_ResultEvidenceModel):
    """Optional realized metrics for one explicitly named market regime."""

    sample: ResultSample
    regime_id: str = Field(min_length=1, max_length=256)
    metrics: MSMResultMetrics

    @field_validator("regime_id")
    @classmethod
    def regime_id_is_normalized(cls, value: str) -> str:
        if "\x00" in value or value != value.strip():
            raise ValueError("regime_id cannot contain NUL bytes or surrounding whitespace")
        return value


class MSMResultArtifactEvidence(_ResultEvidenceModel):
    """Content identity for one artifact supporting the result evidence."""

    relative_path: str = Field(min_length=1, max_length=1024)
    sha256: Sha256
    size_bytes: int = Field(ge=0, strict=True)

    @field_validator("relative_path")
    @classmethod
    def path_is_normalized_and_relative(cls, value: str) -> str:
        if "\\" in value or "\x00" in value:
            raise ValueError("artifact paths must use normalized POSIX separators")
        path = PurePosixPath(value)
        if (
            path.is_absolute()
            or str(path) != value
            or any(part in ("", ".", "..") for part in path.parts)
        ):
            raise ValueError("artifact paths must be normalized paths relative to the output root")
        return value


class MSMRunResultEvidence(_ResultEvidenceModel):
    """Versioned, self-contained result contract for one ledger-bound MSM run."""

    result_format_version: Literal[1]
    source_system: Literal["msm"] = "msm"
    metric_units: MetricUnits
    run_id: str = Field(min_length=1, max_length=256)
    prediction_id: str | None = Field(default=None, min_length=1, max_length=128)
    registration_status: RegistrationStatus
    strategy_id: str = Field(min_length=1, max_length=256)
    envelope_hash: Sha256
    dataset_version: Sha256
    git_commit: GitCommit
    metric_definition_version: Version
    source_timestamp: datetime
    in_sample_window: SnapshotDateWindow
    out_of_sample_window: SnapshotDateWindow
    in_sample_metrics: MSMResultMetrics
    out_of_sample_metrics: MSMResultMetrics
    regime_breakdown: tuple[MSMRegimeResult, ...] = ()
    artifacts: tuple[MSMResultArtifactEvidence, ...] = Field(min_length=1)
    metadata: dict[str, JsonValue] = Field(default_factory=dict)

    @field_validator("run_id", "prediction_id", "strategy_id")
    @classmethod
    def identifiers_are_normalized(cls, value: str | None) -> str | None:
        if value is not None and ("\x00" in value or value != value.strip()):
            raise ValueError("identifiers cannot contain NUL bytes or surrounding whitespace")
        return value

    @field_validator("source_timestamp")
    @classmethod
    def timestamp_is_timezone_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("source_timestamp must be timezone-aware")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def collections_are_unique_and_canonical(self) -> Self:
        artifact_paths = [artifact.relative_path for artifact in self.artifacts]
        if len(artifact_paths) != len(set(artifact_paths)):
            raise ValueError("artifact paths must be unique")
        if artifact_paths != sorted(artifact_paths):
            raise ValueError("artifacts must be sorted by relative_path")

        regime_keys = [(result.sample, result.regime_id) for result in self.regime_breakdown]
        if len(regime_keys) != len(set(regime_keys)):
            raise ValueError("regime results must be unique by sample and regime_id")
        if regime_keys != sorted(regime_keys):
            raise ValueError("regime results must be sorted by sample and regime_id")
        return self


class MSMResultIngestRequest(_ResultEvidenceModel):
    """Typed request to bind one result document to its completed run."""

    evidence: MSMRunResultEvidence


@dataclass(frozen=True, slots=True)
class MSMResultIngestResult:
    """Authoritative result of a new ingestion or exact idempotent retry."""

    run_id: str
    prediction_id: str | None
    registration_status: RegistrationStatus
    strategy_id: str
    dataset_version: str
    envelope_hash: str
    evidence_hash: str
    source_timestamp: datetime
    ingested_at: datetime
    created: bool


class MSMResultIngestor:
    """Validate and permanently attach canonical result evidence to a successful run."""

    def __init__(
        self,
        vault_root: str | Path,
        *,
        registry: LedgerRegistry | None = None,
        clock: Callable[[], datetime] | None = None,
        lock_timeout: float = 30.0,
    ) -> None:
        self.vault_root = Path(vault_root).resolve()
        self.ledger_dir = self.vault_root / ".ledger"
        self.ledger_dir.mkdir(parents=True, exist_ok=True)
        self.registry = registry or LedgerRegistry(self.ledger_dir / "registry.db")
        if self.registry.db_path.resolve() != (self.ledger_dir / "registry.db").resolve():
            raise ValueError("registry and result ingestor must use the same vault root")
        self.clock = clock or (lambda: datetime.now(UTC))
        self.lock_timeout = lock_timeout

    def ingest(
        self,
        request: MSMResultIngestRequest | Mapping[str, Any],
    ) -> MSMResultIngestResult:
        """Persist one canonical result document, or return its exact prior ingestion."""

        validated = (
            request
            if isinstance(request, MSMResultIngestRequest)
            else MSMResultIngestRequest.model_validate(request)
        )
        evidence = validated.evidence
        evidence_payload = evidence.model_dump(mode="json")
        evidence_hash = sha256_json(evidence_payload)

        with ledger_lock(self.ledger_dir, timeout=self.lock_timeout):
            connection = self.registry.connect()
            try:
                connection.execute("BEGIN IMMEDIATE")
                existing = self.registry.get_run_result(
                    evidence.run_id,
                    connection=connection,
                )
                run = self.registry.get_run(evidence.run_id, connection=connection)
                if existing is not None:
                    if existing["evidence_hash"] != evidence_hash:
                        raise IdempotencyConflictError(
                            "run already has different immutable result evidence"
                        )
                    result = self._result(existing, run, created=False)
                    connection.commit()
                    return result

                ingested_at = self._clock_time()
                snapshot = self._validate_run_binding(evidence, run)
                self._validate_times(evidence, run, ingested_at)
                self.registry.create_run_result(
                    run_id=evidence.run_id,
                    evidence_hash=evidence_hash,
                    evidence_json=canonical_json(evidence_payload),
                    source_timestamp=evidence.source_timestamp,
                    ingested_at=ingested_at,
                    connection=connection,
                )
                stored = self.registry.get_run_result(
                    evidence.run_id,
                    connection=connection,
                )
                if stored is None:  # pragma: no cover - same-transaction identity guarantee
                    raise IntegrityError("run result disappeared during ingestion")
                result = self._result(stored, run, created=True, snapshot=snapshot)
                connection.commit()
            except BaseException:
                if connection.in_transaction:
                    connection.rollback()
                logger.exception("msm_result_ingestion_failed", extra={"run_id": evidence.run_id})
                raise
            finally:
                connection.close()

        logger.info(
            "msm_result_ingested",
            extra={"run_id": result.run_id, "evidence_hash": result.evidence_hash},
        )
        return result

    def _clock_time(self) -> datetime:
        value = self.clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise IntegrityError("result ingestion clock must return a timezone-aware value")
        return value

    @staticmethod
    def _validate_run_binding(
        evidence: MSMRunResultEvidence,
        run: sqlite3.Row | None,
    ) -> StrategySnapshot:
        if run is None or run["envelope_json"] is None:
            raise IntegrityError(f"result evidence requires a bound run: {evidence.run_id}")
        if (
            run["state"] != RunState.COMPLETED.value
            or run["exit_code"] != 0
            or run["failure_note"] is not None
        ):
            raise IntegrityError("result evidence requires a successful terminal run")
        try:
            envelope = json.loads(run["envelope_json"])
        except (TypeError, json.JSONDecodeError) as exc:
            raise IntegrityError("run envelope is not valid JSON") from exc
        if not isinstance(envelope, dict) or sha256_json(envelope) != run["envelope_hash"]:
            raise IntegrityError("run envelope does not match its immutable hash")
        try:
            snapshot = StrategySnapshot.model_validate(envelope.get("snapshot"))
        except Exception as exc:
            raise IntegrityError("run envelope contains an invalid snapshot") from exc

        invariants = (
            evidence.run_id == run["run_id"] == envelope.get("run_id"),
            evidence.prediction_id == run["prediction_id"] == envelope.get("prediction_id"),
            evidence.registration_status.value
            == run["registration_status"]
            == envelope.get("registration_status"),
            evidence.strategy_id
            == run["strategy_id"]
            == envelope.get("strategy_id")
            == snapshot.strategy_id,
            evidence.envelope_hash == run["envelope_hash"],
            evidence.dataset_version == run["dataset_version"] == snapshot.dataset_version,
            evidence.git_commit == snapshot.git_commit,
            evidence.metric_definition_version == snapshot.metric_definition_version,
            evidence.in_sample_window == snapshot.in_sample_window,
            evidence.out_of_sample_window == snapshot.out_of_sample_window,
        )
        if not all(invariants):
            raise IntegrityError("result evidence provenance does not match its run envelope")
        return snapshot

    @staticmethod
    def _validate_times(
        evidence: MSMRunResultEvidence,
        run: sqlite3.Row | None,
        ingested_at: datetime,
    ) -> None:
        if run is None or run["execution_started_at"] is None or run["completed_at"] is None:
            raise IntegrityError("successful run is missing execution timestamps")
        try:
            execution_started_at = datetime.fromisoformat(run["execution_started_at"])
            completed_at = datetime.fromisoformat(run["completed_at"])
        except (TypeError, ValueError) as exc:
            raise IntegrityError("successful run has invalid execution timestamps") from exc
        if execution_started_at.tzinfo is None or completed_at.tzinfo is None:
            raise IntegrityError("successful run execution timestamps must be timezone-aware")
        source_timestamp = evidence.source_timestamp.astimezone(UTC)
        if source_timestamp < execution_started_at.astimezone(UTC):
            raise IntegrityError("result source_timestamp cannot precede run execution")
        if source_timestamp > ingested_at.astimezone(UTC):
            raise IntegrityError("result source_timestamp cannot follow ingestion time")
        if ingested_at.astimezone(UTC) < completed_at.astimezone(UTC):
            raise IntegrityError("result ingestion time cannot precede run completion")

    @classmethod
    def _result(
        cls,
        row: sqlite3.Row,
        run: sqlite3.Row | None,
        *,
        created: bool,
        snapshot: StrategySnapshot | None = None,
    ) -> MSMResultIngestResult:
        try:
            payload = json.loads(row["evidence_json"])
        except (TypeError, json.JSONDecodeError) as exc:
            raise IntegrityError("stored run result evidence is not valid JSON") from exc
        if not isinstance(payload, dict) or sha256_json(payload) != row["evidence_hash"]:
            raise IntegrityError("stored run result evidence does not match its immutable hash")
        try:
            evidence = MSMRunResultEvidence.model_validate(payload)
        except Exception as exc:
            raise IntegrityError("stored run result evidence is invalid") from exc
        if row["run_id"] != evidence.run_id:
            raise IntegrityError("stored run result identity does not match its evidence")
        verified_snapshot = snapshot or cls._validate_run_binding(evidence, run)
        if evidence.dataset_version != verified_snapshot.dataset_version:
            raise IntegrityError("stored result dataset version does not match its snapshot")
        try:
            source_timestamp = datetime.fromisoformat(row["source_timestamp"])
            ingested_at = datetime.fromisoformat(row["ingested_at"])
        except (TypeError, ValueError) as exc:
            raise IntegrityError("stored run result timestamps are invalid") from exc
        if source_timestamp.tzinfo is None or ingested_at.tzinfo is None:
            raise IntegrityError("stored run result timestamps must be timezone-aware")
        if source_timestamp.astimezone(UTC) != evidence.source_timestamp.astimezone(UTC):
            raise IntegrityError("stored result source timestamp does not match its evidence")
        cls._validate_times(evidence, run, ingested_at)
        return MSMResultIngestResult(
            run_id=evidence.run_id,
            prediction_id=evidence.prediction_id,
            registration_status=evidence.registration_status,
            strategy_id=evidence.strategy_id,
            dataset_version=evidence.dataset_version,
            envelope_hash=evidence.envelope_hash,
            evidence_hash=row["evidence_hash"],
            source_timestamp=source_timestamp,
            ingested_at=ingested_at,
            created=created,
        )
