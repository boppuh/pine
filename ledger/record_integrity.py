"""Registry-backed reconciliation for human-readable ledger artifacts."""

from __future__ import annotations

import json
import logging
import sqlite3
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any

import yaml
from pydantic import ValidationError

from ledger.integrity import PredictionStatus, StrategyEdgeForecast
from ledger.json_utils import canonical_json, sha256_json
from ledger.registry import LedgerRegistry
from ledger.watcher import (
    LedgerRecordEvent,
    ManagedPathViolation,
    ManagedViolationReason,
)

logger = logging.getLogger(__name__)

_MAX_RECORD_BYTES = 1024 * 1024
_SNAPSHOT_REASONS = frozenset(
    {
        ManagedViolationReason.SNAPSHOT_REWRITTEN,
        ManagedViolationReason.SNAPSHOT_DELETED,
    }
)


class IntegrityCheckState(StrEnum):
    """Outcome of reconciling one filesystem observation with registry truth."""

    CLEAN = "clean"
    QUARANTINED = "quarantined"
    UNREGISTERED = "unregistered"
    ERROR = "error"


@dataclass(frozen=True, slots=True)
class IntegrityCheckResult:
    """Structured result suitable for gating a later indexing callback."""

    prediction_id: str
    path: Path
    state: IntegrityCheckState
    violations: tuple[str, ...] = ()
    changed: bool = False

    @property
    def may_reindex(self) -> bool:
        """Return true only when the committed evidence passed every check."""

        return self.state is IntegrityCheckState.CLEAN


class RecordIntegrityChecker:
    """Compare ledger artifacts with the committed registry without rewriting them."""

    def __init__(
        self,
        vault_root: str | Path,
        *,
        registry: LedgerRegistry | None = None,
    ) -> None:
        self.vault_root = Path(vault_root).resolve()
        if not self.vault_root.is_dir():
            raise ValueError(f"vault root does not exist: {self.vault_root}")
        self.ledger_dir = self.vault_root / ".ledger"
        self.snapshots_dir = self.ledger_dir / "snapshots"
        self.registry = registry or LedgerRegistry(self.ledger_dir / "registry.db")

    def check_record(self, event: LedgerRecordEvent) -> IntegrityCheckResult:
        """Reconcile a Markdown event and quarantine semantic integrity changes."""

        path = event.path.resolve()
        if not path.is_relative_to(self.vault_root) or path.is_relative_to(self.ledger_dir):
            return self._error_result(event.prediction_id, path, "record path is outside the vault")
        try:
            row = self.registry.get_prediction(event.prediction_id)
            if row is None or row["transaction_state"] != "committed":
                return IntegrityCheckResult(
                    prediction_id=event.prediction_id,
                    path=path,
                    state=IntegrityCheckState.UNREGISTERED,
                )
            violations = self._inspect_record(path, row)
            if violations:
                changed = self.registry.quarantine_prediction(
                    event.prediction_id,
                    violations=violations,
                    detected_at=event.observed_at,
                )
                return self._quarantined_result(
                    event.prediction_id,
                    path,
                    violations,
                    changed=changed,
                )
            if row["status"] == PredictionStatus.QUARANTINED.value:
                return self._quarantined_result(event.prediction_id, path, {}, changed=False)
        except Exception as exc:
            logger.exception(
                "ledger_record_integrity_check_failed",
                extra={"prediction_id": event.prediction_id, "path": str(path)},
            )
            return self._error_result(event.prediction_id, path, str(exc))

        logger.info(
            "ledger_record_integrity_clean",
            extra={"prediction_id": event.prediction_id, "path": str(path)},
        )
        return IntegrityCheckResult(
            prediction_id=event.prediction_id,
            path=path,
            state=IntegrityCheckState.CLEAN,
        )

    def handle_managed_violation(
        self,
        violation: ManagedPathViolation,
    ) -> IntegrityCheckResult | None:
        """Quarantine a committed prediction for managed snapshot mutations."""

        if violation.reason not in _SNAPSHOT_REASONS:
            return None
        prediction_id = violation.path.stem
        try:
            row = self.registry.get_prediction(prediction_id)
            if row is None or row["transaction_state"] != "committed":
                return IntegrityCheckResult(
                    prediction_id=prediction_id,
                    path=violation.path,
                    state=IntegrityCheckState.UNREGISTERED,
                )
            note = f"watcher reported {violation.reason.value} with change {violation.change.value}"
            changed = self.registry.quarantine_prediction(
                prediction_id,
                violations={"snapshot": note},
                detected_at=violation.observed_at,
            )
            return self._quarantined_result(
                prediction_id,
                violation.path,
                {"snapshot": note},
                changed=changed,
            )
        except Exception as exc:
            logger.exception(
                "ledger_snapshot_integrity_check_failed",
                extra={"prediction_id": prediction_id, "path": str(violation.path)},
            )
            return self._error_result(prediction_id, violation.path, str(exc))

    def _inspect_record(
        self,
        path: Path,
        row: sqlite3.Row,
    ) -> dict[str, str]:
        try:
            frontmatter = _load_frontmatter(path)
        except FileNotFoundError:
            return {"record": "committed Markdown record is missing or malformed"}
        except (UnicodeDecodeError, ValueError, yaml.YAMLError):
            return {"record": "committed Markdown record is missing or malformed"}

        violations: dict[str, str] = {}
        expected_scalars = {
            "id": row["prediction_id"],
            "run_id": row["run_id"],
            "type": "prediction",
            "schema_id": row["schema_id"],
            "schema_hash": row["schema_hash"],
            "registration_status": row["registration_status"],
            "snapshot_ref": row["snapshot_ref"],
            "immutable_hash": row["immutable_hash"],
        }
        for field, expected in expected_scalars.items():
            if frontmatter.get(field) != expected:
                violations[field] = "frontmatter value differs from committed registry value"

        expected_lineage = json.loads(row["lineage_json"])
        observed_lineage = frontmatter.get("lineage")
        lineage: dict[str, Any] | None = None
        if isinstance(observed_lineage, Mapping):
            lineage = dict(observed_lineage)
            try:
                lineage_matches = canonical_json(lineage) == canonical_json(expected_lineage)
            except (TypeError, ValueError):
                lineage_matches = False
        else:
            lineage_matches = False
        if not lineage_matches:
            violations["lineage"] = "frontmatter value differs from committed registry value"

        forecast: dict[str, Any] | None = None
        try:
            forecast = StrategyEdgeForecast.model_validate(frontmatter.get("forecast")).model_dump(
                mode="json"
            )
        except ValidationError:
            violations["forecast"] = "frontmatter forecast is missing or malformed"

        decision = frontmatter.get("decision")
        if not isinstance(decision, str) or not decision:
            violations["decision"] = "frontmatter decision is missing or malformed"

        snapshot, snapshot_error = self._load_snapshot(row["snapshot_ref"])
        if snapshot_error is not None:
            violations["snapshot"] = snapshot_error

        registration_status = frontmatter.get("registration_status")
        snapshot_ref = frontmatter.get("snapshot_ref")
        schema_hash = frontmatter.get("schema_hash")
        if (
            forecast is not None
            and isinstance(decision, str)
            and decision
            and snapshot is not None
            and lineage is not None
            and isinstance(registration_status, str)
            and isinstance(snapshot_ref, str)
            and isinstance(schema_hash, str)
        ):
            payload = {
                "registration_status": registration_status,
                "forecast": forecast,
                "decision": decision,
                "snapshot": snapshot,
                "snapshot_ref": snapshot_ref,
                "schema_hash": schema_hash,
                "lineage": lineage,
            }
            try:
                observed_hash = sha256_json(payload)
            except (TypeError, ValueError):
                violations["immutable_payload"] = (
                    "record contains values that cannot be canonically hashed"
                )
            else:
                if observed_hash != row["immutable_hash"]:
                    violations["immutable_payload"] = (
                        "canonical immutable payload hash differs from committed registry hash; "
                        f"observed {observed_hash}"
                    )
        return violations

    def _load_snapshot(self, snapshot_ref: str) -> tuple[dict[str, Any] | None, str | None]:
        relative = Path(snapshot_ref)
        snapshot_path = (self.vault_root / relative).resolve()
        if relative.is_absolute() or not snapshot_path.is_relative_to(self.snapshots_dir.resolve()):
            raise ValueError("registry snapshot_ref escapes the managed snapshot directory")
        try:
            raw = snapshot_path.read_bytes()
        except FileNotFoundError:
            return None, "committed snapshot is missing"
        except OSError:
            raise
        try:
            snapshot = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError):
            return None, "committed snapshot is malformed"
        if not isinstance(snapshot, dict):
            return None, "committed snapshot must be a JSON object"
        try:
            canonical_json(snapshot)
        except (TypeError, ValueError):
            return None, "committed snapshot is not canonical JSON data"
        return snapshot, None

    @staticmethod
    def _quarantined_result(
        prediction_id: str,
        path: Path,
        violations: Mapping[str, str],
        *,
        changed: bool,
    ) -> IntegrityCheckResult:
        fields = tuple(sorted(violations))
        logger.warning(
            "ledger_record_integrity_quarantined",
            extra={
                "prediction_id": prediction_id,
                "path": str(path),
                "fields": fields,
                "changed": changed,
            },
        )
        return IntegrityCheckResult(
            prediction_id=prediction_id,
            path=path,
            state=IntegrityCheckState.QUARANTINED,
            violations=fields,
            changed=changed,
        )

    @staticmethod
    def _error_result(
        prediction_id: str,
        path: Path,
        note: str,
    ) -> IntegrityCheckResult:
        logger.error(
            "ledger_record_integrity_error",
            extra={"prediction_id": prediction_id, "path": str(path), "note": note},
        )
        return IntegrityCheckResult(
            prediction_id=prediction_id,
            path=path,
            state=IntegrityCheckState.ERROR,
            violations=("checker",),
        )


def _load_frontmatter(path: Path) -> dict[str, Any]:
    with path.open("rb") as handle:
        raw = handle.read(_MAX_RECORD_BYTES + 1)
    if len(raw) > _MAX_RECORD_BYTES:
        raise ValueError("record exceeds the frontmatter read limit")
    content = raw.decode("utf-8").replace("\r\n", "\n")
    if not content.startswith("---\n"):
        raise ValueError("record has no YAML frontmatter")
    parts = content.split("---\n", maxsplit=2)
    if len(parts) != 3:
        raise ValueError("record has unterminated YAML frontmatter")
    frontmatter = yaml.safe_load(parts[1])
    if not isinstance(frontmatter, dict):
        raise ValueError("record frontmatter must be an object")
    return frontmatter
