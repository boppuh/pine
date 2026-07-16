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
    run_id: str
    record_path: Path
    snapshot_path: Path
    schema_id: str
    schema_hash: str
    immutable_hash: str
    created: bool


@dataclass(slots=True)
class StagedWrite:
    """Filesystem publication awaiting its caller-owned SQLite commit."""

    result: WriteResult
    writer: LedgerWriter
    manifest_path: Path | None = None
    record_temp: Path | None = None
    snapshot_temp: Path | None = None
    record_published: bool = False
    snapshot_published: bool = False
    finished: bool = False

    def finalize(self) -> None:
        """Remove recovery metadata after the caller durably commits SQLite."""

        if self.finished:
            return
        self.writer._remove_paths(
            *(path for path in (self.record_temp, self.snapshot_temp, self.manifest_path) if path)
        )
        self.writer._fsync_directory(self.writer.records_dir)
        self.writer._fsync_directory(self.writer.snapshots_dir)
        self.finished = True

    def rollback(self) -> None:
        """Remove only artifacts proven to belong to this staged write."""

        if self.finished:
            return
        record_owned = self.record_published or self._temp_owns(
            self.record_temp, self.result.record_path
        )
        snapshot_owned = self.snapshot_published or self._temp_owns(
            self.snapshot_temp, self.result.snapshot_path
        )
        cleanup = [
            path
            for path in (self.record_temp, self.snapshot_temp, self.manifest_path)
            if path is not None
        ]
        if record_owned:
            cleanup.append(self.result.record_path)
        if snapshot_owned:
            cleanup.append(self.result.snapshot_path)
        self.writer._remove_paths(*cleanup)
        self.writer._fsync_directory(self.writer.records_dir)
        self.writer._fsync_directory(self.writer.snapshots_dir)
        self.finished = True

    def _temp_owns(self, temporary: Path | None, published: Path) -> bool:
        return temporary is not None and self.writer._paths_share_inode(temporary, published)


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
            self.recover_unfinished_transactions_locked()

    def write(self, prediction: PredictionDraft | Mapping[str, Any]) -> WriteResult:
        """Validate and atomically commit a record, or no-op on duplicate ID."""

        draft = self._coerce_draft(prediction)

        with ledger_lock(self.ledger_dir, timeout=self.lock_timeout):
            self.recover_unfinished_transactions_locked()
            connection = self.registry.connect()
            staged: StagedWrite | None = None
            try:
                connection.execute("BEGIN IMMEDIATE")
                staged = self.stage_in_transaction(draft, connection=connection)
                connection.commit()
            except BaseException:
                if connection.in_transaction:
                    connection.rollback()
                if staged is not None:
                    try:
                        committed = self.registry.is_committed(staged.result.prediction_id)
                    except Exception:
                        committed = None
                    if committed is True:
                        staged.finalize()
                    elif committed is False:
                        staged.rollback()
                logger.exception(
                    "ledger_prediction_commit_failed",
                    extra={"prediction_id": draft.prediction_id},
                )
                raise
            else:
                staged.finalize()
                logger.info(
                    "ledger_prediction_committed",
                    extra={
                        "prediction_id": staged.result.prediction_id,
                        "schema_id": staged.result.schema_id,
                    },
                )
                return staged.result
            finally:
                connection.close()

    def stage_in_transaction(
        self,
        prediction: PredictionDraft | Mapping[str, Any],
        *,
        connection: sqlite3.Connection,
        expected_schema_hash: str | None = None,
        committed_at: datetime | None = None,
    ) -> StagedWrite:
        """Stage artifacts and rows inside a caller-owned immediate transaction.

        The caller must commit SQLite and then call ``finalize()``, or roll back
        SQLite and call ``rollback()``. The durable manifest covers a process crash
        between those steps.
        """

        if not connection.in_transaction:
            raise IntegrityError("stage_in_transaction requires an active transaction")
        draft = self._coerce_draft(prediction)
        existing = self.registry.get_prediction(draft.prediction_id, connection=connection)
        if existing is not None:
            return StagedWrite(result=self.result_for_row(existing), writer=self, finished=True)

        committed = self._prepare_committed(
            draft,
            expected_schema_hash=expected_schema_hash,
            committed_at=committed_at,
        )
        return self._stage_committed(committed, connection=connection)

    def _prepare_committed(
        self,
        draft: PredictionDraft,
        *,
        expected_schema_hash: str | None = None,
        committed_at: datetime | None = None,
    ) -> CommittedPrediction:
        forecast = draft.forecast.model_dump(mode="json")
        schema = self.schema_registry.load(draft.schema_id)
        valid, errors = self.schema_registry.validate_schema(forecast, schema)
        if not valid:
            raise ForecastValidationError(errors)

        schema_hash = self.schema_registry.hash(schema)
        if expected_schema_hash is not None and schema_hash != expected_schema_hash:
            raise IntegrityError("forecast schema changed during the capture transaction")
        commit_time = committed_at or datetime.now(UTC)
        if commit_time.tzinfo is None or commit_time.utcoffset() is None:
            raise IntegrityError("committed_at must be timezone-aware")
        if commit_time < draft.created_at:
            raise IntegrityError("committed_at cannot precede created_at")
        snapshot_ref = f".ledger/snapshots/{draft.prediction_id}.json"
        immutable_hash = sha256_json(
            immutable_payload(
                draft,
                schema_hash=schema_hash,
                snapshot_ref=snapshot_ref,
            )
        )
        return CommittedPrediction(
            **draft.model_dump(),
            schema_hash=schema_hash,
            snapshot_ref=snapshot_ref,
            immutable_hash=immutable_hash,
            committed_at=commit_time,
        )

    def _stage_committed(
        self,
        prediction: CommittedPrediction,
        *,
        connection: sqlite3.Connection,
    ) -> StagedWrite:
        record_path = self.records_dir / f"{prediction.prediction_id}.md"
        snapshot_path = self.snapshots_dir / f"{prediction.prediction_id}.json"
        if os.path.lexists(record_path) or os.path.lexists(snapshot_path):
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
        staged = StagedWrite(
            result=WriteResult(
                prediction_id=prediction.prediction_id,
                run_id=prediction.run_id,
                record_path=record_path,
                snapshot_path=snapshot_path,
                schema_id=prediction.schema_id,
                schema_hash=prediction.schema_hash,
                immutable_hash=prediction.immutable_hash,
                created=True,
            ),
            writer=self,
            manifest_path=manifest_path,
            record_temp=record_temp,
            snapshot_temp=snapshot_temp,
        )
        try:
            self._atomic_manifest_write(manifest_path, manifest)
            self._write_new_file(snapshot_temp, self._snapshot_bytes(prediction))
            self._write_new_file(record_temp, self._record_bytes(prediction))

            self.registry.begin_prediction(prediction, connection=connection)

            self._inject_failure("before_snapshot_publish")
            self._publish_no_replace(snapshot_temp, snapshot_path)
            staged.snapshot_published = True
            snapshot_temp.unlink()
            self._fsync_directory(self.snapshots_dir)
            self._inject_failure("after_snapshot_publish")

            self._publish_no_replace(record_temp, record_path)
            staged.record_published = True
            record_temp.unlink()
            self._fsync_directory(self.records_dir)
            self._inject_failure("after_record_publish")

            self.registry.commit_prediction(
                prediction.prediction_id,
                committed_at=prediction.committed_at,
                connection=connection,
            )
        except BaseException:
            staged.rollback()
            raise

        return staged

    def result_for_row(self, row: sqlite3.Row) -> WriteResult:
        """Build an idempotent result from an authoritative committed row."""

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
            run_id=row["run_id"],
            record_path=record_path,
            snapshot_path=snapshot_path,
            schema_id=row["schema_id"],
            schema_hash=row["schema_hash"],
            immutable_hash=row["immutable_hash"],
            created=False,
        )

    def recover_unfinished_transactions_locked(self) -> None:
        """Recover manifests while the caller holds the process-level ledger lock."""

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
