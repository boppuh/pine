"""Atomic, idempotent snapshot and Markdown ledger writer."""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml
from pydantic import ValidationError

from ledger.errors import ForecastValidationError, IntegrityError
from ledger.integrity import CommittedPrediction, PredictionDraft, immutable_payload
from ledger.json_utils import sha256_json
from ledger.locking import ledger_lock
from ledger.registry import LedgerRegistry
from ledger.schema_registry import SchemaRegistry

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class WriteResult:
    """Paths and hashes for an atomic write attempt."""

    prediction_id: str
    record_path: Path
    snapshot_path: Path
    schema_id: str
    schema_hash: str
    immutable_hash: str
    created: bool


class LedgerWriter:
    """Commit one snapshot/note pair under a registry transaction and file lock.

    The committed registry row is the visibility boundary. Files associated with an
    uncommitted row are transaction debris and must never be consumed as ledger state;
    manifests remove that debris after a process or machine restart.
    """

    def __init__(
        self,
        vault_root: str | Path,
        *,
        records_dir: str | Path | None = None,
        schema_registry: SchemaRegistry | None = None,
        registry: LedgerRegistry | None = None,
        lock_timeout: float = 30.0,
        failure_injector: Callable[[str], None] | None = None,
    ) -> None:
        self.vault_root = Path(vault_root).resolve()
        self.ledger_dir = self.vault_root / ".ledger"
        self.snapshots_dir = self.ledger_dir / "snapshots"
        self.records_dir = (
            Path(records_dir).resolve()
            if records_dir is not None
            else self.vault_root / "predictions"
        )
        if not self.records_dir.is_relative_to(self.vault_root):
            raise ValueError("records_dir must live inside vault_root")

        self.snapshots_dir.mkdir(parents=True, exist_ok=True)
        self.records_dir.mkdir(parents=True, exist_ok=True)
        self.schema_registry = schema_registry or SchemaRegistry(self.ledger_dir / "schemas")
        self.registry = registry or LedgerRegistry(self.ledger_dir / "registry.db")
        self.lock_timeout = lock_timeout
        self.failure_injector = failure_injector

        with ledger_lock(self.ledger_dir, timeout=self.lock_timeout):
            self._recover_unfinished_transactions()

    def write(self, prediction: PredictionDraft | Mapping[str, Any]) -> WriteResult:
        """Validate and atomically commit a record, or no-op on duplicate ID."""

        draft = self._coerce_draft(prediction)

        with ledger_lock(self.ledger_dir, timeout=self.lock_timeout):
            self._recover_unfinished_transactions()
            existing = self.registry.get_prediction(draft.prediction_id)
            if existing is not None:
                return self._idempotent_result(existing)

            forecast = draft.forecast.model_dump(mode="json")
            valid, errors = self.schema_registry.validate(forecast, draft.schema_id)
            if not valid:
                raise ForecastValidationError(errors)

            schema = self.schema_registry.load(draft.schema_id)
            schema_hash = self.schema_registry.hash(schema)
            snapshot_ref = f".ledger/snapshots/{draft.prediction_id}.json"
            immutable_hash = sha256_json(
                immutable_payload(
                    draft,
                    schema_hash=schema_hash,
                    snapshot_ref=snapshot_ref,
                )
            )
            committed_at = datetime.now(UTC)
            committed = CommittedPrediction(
                **draft.model_dump(),
                schema_hash=schema_hash,
                snapshot_ref=snapshot_ref,
                immutable_hash=immutable_hash,
                committed_at=committed_at,
            )
            return self._commit(committed)

    def _commit(self, prediction: CommittedPrediction) -> WriteResult:
        record_path = self.records_dir / f"{prediction.prediction_id}.md"
        snapshot_path = self.snapshots_dir / f"{prediction.prediction_id}.json"
        if record_path.exists() or snapshot_path.exists():
            raise IntegrityError(
                "unregistered artifact collision; refusing to overwrite possible audit evidence"
            )

        nonce = uuid.uuid4().hex
        record_temp = self.records_dir / f".{prediction.prediction_id}.{nonce}.tmp"
        snapshot_temp = self.snapshots_dir / f".{prediction.prediction_id}.{nonce}.tmp"
        manifest_path = self.snapshots_dir / f".txn-{prediction.prediction_id}-{nonce}.json"
        manifest = {
            "prediction_id": prediction.prediction_id,
            "record_path": str(record_path),
            "snapshot_path": str(snapshot_path),
            "record_temp": str(record_temp),
            "snapshot_temp": str(snapshot_temp),
        }

        database_committed = False
        snapshot_published = False
        record_published = False
        try:
            self._atomic_manifest_write(manifest_path, manifest)
            self._write_new_file(snapshot_temp, self._snapshot_bytes(prediction))
            self._write_new_file(record_temp, self._record_bytes(prediction))

            with self.registry.transaction() as connection:
                self.registry.begin_prediction(prediction, connection=connection)

                self._inject_failure("before_snapshot_publish")
                self._publish_no_replace(snapshot_temp, snapshot_path)
                snapshot_published = True
                snapshot_temp.unlink()
                self._fsync_directory(self.snapshots_dir)
                self._inject_failure("after_snapshot_publish")

                self._publish_no_replace(record_temp, record_path)
                record_published = True
                record_temp.unlink()
                self._fsync_directory(self.records_dir)
                self._inject_failure("after_record_publish")

                self.registry.commit_prediction(
                    prediction.prediction_id,
                    committed_at=prediction.committed_at,
                    connection=connection,
                )
            database_committed = True
        except BaseException:
            authoritatively_committed: bool | None = database_committed
            if not database_committed:
                try:
                    authoritatively_committed = self.registry.is_committed(prediction.prediction_id)
                except Exception:
                    authoritatively_committed = None
            if authoritatively_committed is True:
                self._remove_paths(record_temp, snapshot_temp, manifest_path)
            elif authoritatively_committed is False:
                cleanup_paths = [record_temp, snapshot_temp, manifest_path]
                if record_published or self._paths_share_inode(record_temp, record_path):
                    cleanup_paths.append(record_path)
                if snapshot_published or self._paths_share_inode(snapshot_temp, snapshot_path):
                    cleanup_paths.append(snapshot_path)
                self._remove_paths(*cleanup_paths)
            else:
                logger.error(
                    "ledger_prediction_commit_state_unknown",
                    extra={"prediction_id": prediction.prediction_id},
                )
            self._fsync_directory(self.records_dir)
            self._fsync_directory(self.snapshots_dir)
            logger.exception(
                "ledger_prediction_commit_failed",
                extra={"prediction_id": prediction.prediction_id},
            )
            raise
        else:
            self._remove_paths(manifest_path)
            self._fsync_directory(self.snapshots_dir)

        logger.info(
            "ledger_prediction_committed",
            extra={
                "prediction_id": prediction.prediction_id,
                "schema_id": prediction.schema_id,
                "registration_status": prediction.registration_status.value,
            },
        )
        return WriteResult(
            prediction_id=prediction.prediction_id,
            record_path=record_path,
            snapshot_path=snapshot_path,
            schema_id=prediction.schema_id,
            schema_hash=prediction.schema_hash,
            immutable_hash=prediction.immutable_hash,
            created=True,
        )

    def _idempotent_result(self, row: sqlite3.Row) -> WriteResult:
        if row["transaction_state"] != "committed":
            raise IntegrityError(
                f"prediction exists in non-committed state: {row['prediction_id']}"
            )
        record_path = self.records_dir / f"{row['prediction_id']}.md"
        snapshot_path = self.vault_root / row["snapshot_ref"]
        if not record_path.is_file() or not snapshot_path.is_file():
            raise IntegrityError(
                f"committed registry entry is missing an artifact: {row['prediction_id']}"
            )
        logger.info(
            "ledger_prediction_duplicate_noop",
            extra={"prediction_id": row["prediction_id"]},
        )
        return WriteResult(
            prediction_id=row["prediction_id"],
            record_path=record_path,
            snapshot_path=snapshot_path,
            schema_id=row["schema_id"],
            schema_hash=row["schema_hash"],
            immutable_hash=row["immutable_hash"],
            created=False,
        )

    def _recover_unfinished_transactions(self) -> None:
        # A crash while writing the manifest cannot have produced artifact temps yet,
        # because artifact staging begins only after the manifest rename returns.
        self._remove_paths(*self.snapshots_dir.glob(".txn-*.manifest-tmp"))

        for manifest_path in sorted(self.snapshots_dir.glob(".txn-*.json")):
            try:
                manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
                prediction_id = str(manifest["prediction_id"])
                paths = [
                    self._managed_path(manifest[key]) for key in ("record_temp", "snapshot_temp")
                ]
                if not self.registry.is_committed(prediction_id):
                    paths.extend(
                        self._managed_path(manifest[key])
                        for key in ("record_path", "snapshot_path")
                    )
                self._remove_paths(*paths, manifest_path)
            except (OSError, ValueError, KeyError, json.JSONDecodeError) as exc:
                raise IntegrityError(f"cannot safely recover transaction {manifest_path}") from exc

    def _managed_path(self, raw_path: str) -> Path:
        path = Path(raw_path).resolve()
        if not path.is_relative_to(self.vault_root):
            raise ValueError("transaction manifest references a path outside the vault")
        return path

    @staticmethod
    def _coerce_draft(prediction: PredictionDraft | Mapping[str, Any]) -> PredictionDraft:
        if isinstance(prediction, PredictionDraft):
            return prediction
        try:
            return PredictionDraft.model_validate(prediction)
        except ValidationError as exc:
            errors = [
                f"$.{'.'.join(str(item) for item in error['loc'])}: {error['msg']}"
                for error in exc.errors()
            ]
            raise ForecastValidationError(errors) from exc

    @staticmethod
    def _snapshot_bytes(prediction: CommittedPrediction) -> bytes:
        content = json.dumps(
            prediction.snapshot.to_dict(),
            allow_nan=False,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        return f"{content}\n".encode()

    @staticmethod
    def _record_bytes(prediction: CommittedPrediction) -> bytes:
        forecast = prediction.forecast.model_dump(mode="json")
        digest: dict[str, Any] = {
            "in_sample_window": forecast["in_sample_window"],
            "out_of_sample_window": forecast["out_of_sample_window"],
            "expected_metrics": forecast["expected_metrics"],
        }
        parameter_count = prediction.snapshot.to_dict().get("parameter_count")
        if parameter_count is not None:
            digest["parameter_count"] = parameter_count

        frontmatter = {
            "id": prediction.prediction_id,
            "run_id": prediction.run_id,
            "type": "prediction",
            "domain": prediction.schema_id.split("/", maxsplit=1)[0],
            "schema_id": prediction.schema_id,
            "schema_hash": prediction.schema_hash,
            "registration_status": prediction.registration_status.value,
            "created_at": prediction.created_at.isoformat(),
            "committed_at": prediction.committed_at.isoformat(),
            "forecast": forecast,
            "decision": prediction.decision,
            "snapshot_ref": prediction.snapshot_ref,
            "snapshot_digest": digest,
            "lineage": prediction.lineage.to_dict(),
            "immutable_hash": prediction.immutable_hash,
            "status": prediction.status.value,
            "outcome": None,
            "grade": None,
            "resolution_metadata": None,
        }
        yaml_text = yaml.safe_dump(
            frontmatter,
            allow_unicode=True,
            default_flow_style=False,
            sort_keys=False,
        )
        body = prediction.body.rstrip()
        return f"---\n{yaml_text}---\n\n{body}\n".encode()

    @staticmethod
    def _write_new_file(path: Path, content: bytes) -> None:
        descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())

    def _atomic_manifest_write(self, path: Path, manifest: Mapping[str, Any]) -> None:
        temporary = path.with_suffix(".manifest-tmp")
        content = (json.dumps(dict(manifest), ensure_ascii=False, sort_keys=True) + "\n").encode(
            "utf-8"
        )
        self._write_new_file(temporary, content)
        os.rename(temporary, path)
        self._fsync_directory(path.parent)

    @staticmethod
    def _publish_no_replace(source: Path, destination: Path) -> None:
        """Create a no-replace final link while retaining the staged ownership link."""

        try:
            os.link(source, destination, follow_symlinks=False)
        except FileExistsError as exc:
            raise IntegrityError(
                f"artifact appeared during commit; refusing to overwrite {destination}"
            ) from exc

    @staticmethod
    def _paths_share_inode(first: Path, second: Path) -> bool:
        """Return whether two paths are links to the same staged artifact."""

        try:
            return os.path.samestat(first.stat(), second.stat())
        except FileNotFoundError:
            return False

    def _inject_failure(self, phase: str) -> None:
        if self.failure_injector is not None:
            self.failure_injector(phase)

    @staticmethod
    def _remove_paths(*paths: Path) -> None:
        for path in paths:
            try:
                path.unlink()
            except FileNotFoundError:
                pass

    @staticmethod
    def _fsync_directory(path: Path) -> None:
        descriptor = os.open(path, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
