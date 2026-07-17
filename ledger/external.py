"""Fail-closed audit recovery for MSM runs that bypassed the ledger wrapper."""

from __future__ import annotations

import json
import logging
import sqlite3
import uuid
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
from ledger.msm import Sha256, StrategySnapshot
from ledger.registry import LedgerRegistry
from ledger.run import RunState

logger = logging.getLogger(__name__)


class _ExternalEvidenceModel(BaseModel):
    """Frozen, closed model for caller-supplied retrospective evidence."""

    model_config = ConfigDict(allow_inf_nan=False, extra="forbid", frozen=True)


class ExternalArtifactEvidence(_ExternalEvidenceModel):
    """Content identity for one file produced by the bypassed MSM run."""

    relative_path: str = Field(min_length=1, max_length=1024)
    sha256: Sha256
    size_bytes: int = Field(ge=0)

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


class ExternalRunEvidence(_ExternalEvidenceModel):
    """Complete evidence document for one already-finished direct MSM run."""

    evidence_format_version: Literal[1]
    source_system: str = Field(
        default="msm",
        min_length=1,
        max_length=64,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9_.-]*$",
    )
    source_run_id: str = Field(min_length=1, max_length=256)
    strategy_id: str = Field(min_length=1, max_length=256)
    started_at: datetime
    completed_at: datetime
    exit_code: int = Field(strict=True)
    failure_note: str | None = Field(default=None, max_length=2048)
    command: tuple[str, ...] = Field(min_length=1)
    working_directory: str = Field(min_length=1, max_length=4096)
    snapshot: StrategySnapshot
    artifacts: tuple[ExternalArtifactEvidence, ...] = Field(min_length=1)
    metadata: dict[str, JsonValue] = Field(default_factory=dict)

    @field_validator("source_run_id", "strategy_id")
    @classmethod
    def identifiers_contain_no_nul(cls, value: str) -> str:
        if "\x00" in value or value != value.strip():
            raise ValueError("identifiers cannot contain NUL bytes or surrounding whitespace")
        return value

    @field_validator("command")
    @classmethod
    def command_is_safe_argv(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if any(not argument or "\x00" in argument for argument in value):
            raise ValueError("command arguments must be non-empty and contain no NUL bytes")
        return value

    @field_validator("working_directory")
    @classmethod
    def working_directory_is_absolute(cls, value: str) -> str:
        if "\x00" in value or not Path(value).is_absolute():
            raise ValueError("working_directory must be an absolute local path")
        return value

    @field_validator("started_at", "completed_at")
    @classmethod
    def timestamps_are_timezone_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("external run timestamps must be timezone-aware")
        return value

    @model_validator(mode="after")
    def evidence_is_internally_consistent(self) -> Self:
        if self.completed_at.astimezone(UTC) < self.started_at.astimezone(UTC):
            raise ValueError("completed_at cannot precede started_at")
        if self.snapshot.strategy_id != self.strategy_id:
            raise ValueError("snapshot strategy_id does not match external run strategy_id")
        paths = [artifact.relative_path for artifact in self.artifacts]
        if len(paths) != len(set(paths)):
            raise ValueError("artifact paths must be unique")
        if paths != sorted(paths):
            raise ValueError("artifacts must be sorted by relative_path")
        return self


class ExternalRunIngestRequest(_ExternalEvidenceModel):
    """Typed audit-recovery request with no caller-selectable registration status."""

    evidence: ExternalRunEvidence


@dataclass(frozen=True, slots=True)
class ExternalRunImportResult:
    """Authoritative result of a new import or an exact idempotent retry."""

    run_id: str
    source_system: str
    source_run_id: str
    registration_status: RegistrationStatus
    strategy_id: str
    dataset_version: str
    evidence_hash: str
    envelope_hash: str
    state: RunState
    exit_code: int
    failure_note: str | None
    created: bool


class ExternalRunIngestor:
    """Import explicit bypass-run evidence as permanent low-integrity state."""

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
            raise ValueError("registry and external ingestor must use the same vault root")
        self.clock = clock or (lambda: datetime.now(UTC))
        self.lock_timeout = lock_timeout

    def ingest(
        self,
        request: ExternalRunIngestRequest | Mapping[str, Any],
    ) -> ExternalRunImportResult:
        """Persist one evidence document, or return its exact prior import."""

        validated = (
            request
            if isinstance(request, ExternalRunIngestRequest)
            else ExternalRunIngestRequest.model_validate(request)
        )
        evidence = validated.evidence
        evidence_payload = evidence.model_dump(mode="json")
        evidence_hash = sha256_json(evidence_payload)

        with ledger_lock(self.ledger_dir, timeout=self.lock_timeout):
            connection = self.registry.connect()
            try:
                connection.execute("BEGIN IMMEDIATE")
                existing = self.registry.get_external_run_import(
                    evidence.source_system,
                    evidence.source_run_id,
                    connection=connection,
                )
                if existing is not None:
                    if existing["evidence_hash"] != evidence_hash:
                        raise IdempotencyConflictError(
                            "external source run identity is already bound to different evidence"
                        )
                    result = self._result(existing, created=False)
                    connection.commit()
                    return result

                ingested_at = self._clock_time()
                if ingested_at.astimezone(UTC) < evidence.completed_at.astimezone(UTC):
                    raise IntegrityError("external ingestion time cannot precede run completion")
                run_id = f"run_external_{uuid.uuid4().hex}"
                envelope = _external_envelope(
                    run_id=run_id,
                    evidence=evidence,
                    evidence_payload=evidence_payload,
                    evidence_hash=evidence_hash,
                    ingested_at=ingested_at,
                )
                envelope_hash = sha256_json(envelope)
                self.registry.create_external_run_import(
                    run_id=run_id,
                    source_system=evidence.source_system,
                    source_run_id=evidence.source_run_id,
                    evidence_hash=evidence_hash,
                    idempotency_key=_external_idempotency_key(evidence),
                    strategy_id=evidence.strategy_id,
                    dataset_version=evidence.snapshot.dataset_version,
                    envelope_json=canonical_json(envelope),
                    envelope_hash=envelope_hash,
                    started_at=evidence.started_at,
                    completed_at=evidence.completed_at,
                    ingested_at=ingested_at,
                    exit_code=evidence.exit_code,
                    failure_note=evidence.failure_note,
                    connection=connection,
                )
                stored = self.registry.get_external_run_import(
                    evidence.source_system,
                    evidence.source_run_id,
                    connection=connection,
                )
                if stored is None:  # pragma: no cover - same-transaction identity guarantee
                    raise IntegrityError("external run disappeared during its import")
                result = self._result(stored, created=True)
                connection.commit()
            except BaseException:
                if connection.in_transaction:
                    connection.rollback()
                logger.exception(
                    "external_run_import_failed",
                    extra={
                        "source_system": evidence.source_system,
                        "source_run_id": evidence.source_run_id,
                    },
                )
                raise
            finally:
                connection.close()

        logger.info(
            "external_run_imported",
            extra={
                "run_id": result.run_id,
                "source_system": result.source_system,
                "source_run_id": result.source_run_id,
            },
        )
        return result

    def _clock_time(self) -> datetime:
        value = self.clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise IntegrityError("external ingestion clock must return a timezone-aware value")
        return value

    @staticmethod
    def _result(row: sqlite3.Row, *, created: bool) -> ExternalRunImportResult:
        try:
            envelope = json.loads(row["envelope_json"])
        except (TypeError, json.JSONDecodeError) as exc:
            raise IntegrityError("external run envelope is not valid JSON") from exc
        if not isinstance(envelope, dict) or sha256_json(envelope) != row["envelope_hash"]:
            raise IntegrityError("external run envelope does not match its immutable hash")
        if envelope.get("registration_status") != RegistrationStatus.UNREGISTERED_EXTERNAL.value:
            raise IntegrityError("external run envelope is not permanently low-integrity")
        if row["registration_status"] != RegistrationStatus.UNREGISTERED_EXTERNAL.value:
            raise IntegrityError("external run binding is not permanently low-integrity")
        evidence_payload = envelope.get("external_evidence")
        if not isinstance(evidence_payload, dict):
            raise IntegrityError("external run envelope is missing its evidence")
        if sha256_json(evidence_payload) != row["evidence_hash"]:
            raise IntegrityError("external run evidence does not match its immutable hash")
        try:
            evidence = ExternalRunEvidence.model_validate(evidence_payload)
        except Exception as exc:
            raise IntegrityError("stored external run evidence is invalid") from exc
        expected_state = (
            RunState.COMPLETED
            if evidence.exit_code == 0 and evidence.failure_note is None
            else RunState.FAILED
        )
        expected_command = {
            "argv": list(evidence.command),
            "working_directory": evidence.working_directory,
        }
        invariants = (
            envelope.get("run_id") == row["run_id"],
            envelope.get("prediction_id") is None and row["prediction_id"] is None,
            envelope.get("strategy_id") == evidence.strategy_id == row["strategy_id"],
            envelope.get("command") == expected_command,
            envelope.get("snapshot") == evidence.snapshot.model_dump(mode="json"),
            envelope.get("external_evidence_hash") == row["evidence_hash"],
            envelope.get("low_integrity_reason") == "wrapper_bypass",
            row["source_system"] == evidence.source_system,
            row["source_run_id"] == evidence.source_run_id,
            row["dataset_version"] == evidence.snapshot.dataset_version,
            row["request_hash"] == row["evidence_hash"],
            row["idempotency_key"] == _external_idempotency_key(evidence),
            row["bound_at"] == row["ingested_at"],
            row["started_at"] == evidence.started_at.isoformat(),
            row["execution_started_at"] == evidence.started_at.isoformat(),
            row["completed_at"] == evidence.completed_at.isoformat(),
            row["exit_code"] == evidence.exit_code,
            row["failure_note"] == evidence.failure_note,
            row["state"] == expected_state.value,
        )
        if not all(invariants):
            raise IntegrityError("external run registry state does not match its evidence")
        return ExternalRunImportResult(
            run_id=row["run_id"],
            source_system=evidence.source_system,
            source_run_id=evidence.source_run_id,
            registration_status=RegistrationStatus.UNREGISTERED_EXTERNAL,
            strategy_id=evidence.strategy_id,
            dataset_version=evidence.snapshot.dataset_version,
            evidence_hash=row["evidence_hash"],
            envelope_hash=row["envelope_hash"],
            state=expected_state,
            exit_code=evidence.exit_code,
            failure_note=evidence.failure_note,
            created=created,
        )


def _external_idempotency_key(evidence: ExternalRunEvidence) -> str:
    identity_hash = sha256_json(
        {
            "source_system": evidence.source_system,
            "source_run_id": evidence.source_run_id,
        }
    )
    return f"external:{identity_hash}"


def _external_envelope(
    *,
    run_id: str,
    evidence: ExternalRunEvidence,
    evidence_payload: dict[str, Any],
    evidence_hash: str,
    ingested_at: datetime,
) -> dict[str, Any]:
    return {
        "envelope_format_version": 1,
        "run_id": run_id,
        "prediction_id": None,
        "registration_status": RegistrationStatus.UNREGISTERED_EXTERNAL.value,
        "strategy_id": evidence.strategy_id,
        "bound_at": ingested_at.isoformat(),
        "prediction_immutable_hash": None,
        "command": {
            "argv": list(evidence.command),
            "working_directory": evidence.working_directory,
        },
        "snapshot": evidence.snapshot.model_dump(mode="json"),
        "external_evidence": evidence_payload,
        "external_evidence_hash": evidence_hash,
        "low_integrity_reason": "wrapper_bypass",
    }
