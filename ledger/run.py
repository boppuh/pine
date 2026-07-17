"""Durable pre-run envelopes and the safe MSM process boundary."""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import subprocess
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any, Protocol

import yaml
from filelock import Timeout as FileLockTimeout
from pydantic import BaseModel, ConfigDict, Field, field_validator

from ledger.errors import (
    IdempotencyConflictError,
    IntegrityError,
    RunStateError,
    SnapshotCaptureError,
)
from ledger.integrity import RegistrationStatus, StrategyEdgeForecast
from ledger.json_utils import canonical_json, sha256_json
from ledger.locking import ledger_lock, run_execution_lock
from ledger.msm import MSMSnapshotSource, SnapshotDateWindow, StrategySnapshot
from ledger.writer import LedgerWriter

logger = logging.getLogger(__name__)


class RunState(StrEnum):
    """Execution lifecycle persisted by the registry."""

    REGISTERED = "registered"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class ProcessExecutor(Protocol):
    """Injectable, shell-free process execution boundary."""

    def __call__(
        self,
        command: Sequence[str],
        working_directory: Path,
        environment: Mapping[str, str],
    ) -> int:
        """Execute ``command`` and return its process exit code."""

        ...


class GitStateReader(Protocol):
    """Read the source identity of the checkout used as the command directory."""

    def __call__(self, working_directory: Path) -> tuple[str, bool]:
        """Return ``(HEAD commit, dirty)`` for the command checkout."""

        ...


class _RunRequest(BaseModel):
    """Shared immutable inputs for one idempotent wrapper invocation."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    idempotency_key: str = Field(min_length=1, max_length=256)
    command: tuple[str, ...] = Field(min_length=1)
    working_directory: Path | None = None

    @field_validator("command")
    @classmethod
    def command_is_safe_argv(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if any(not argument or "\x00" in argument for argument in value):
            raise ValueError("command arguments must be non-empty and contain no NUL bytes")
        return value


class PreregisteredRunRequest(_RunRequest):
    """Execute the run already allocated to a committed preregistered prediction."""

    prediction_id: str = Field(min_length=1, max_length=128)


class ExploratoryRunRequest(_RunRequest):
    """Create a permanent exploratory envelope, then execute it exactly once."""

    strategy_id: str = Field(min_length=1, max_length=256)
    in_sample_window: SnapshotDateWindow
    out_of_sample_window: SnapshotDateWindow


@dataclass(frozen=True, slots=True)
class RunResult:
    """Authoritative state returned after an execution or idempotent retry."""

    run_id: str
    prediction_id: str | None
    registration_status: RegistrationStatus
    strategy_id: str
    dataset_version: str
    envelope_hash: str
    state: RunState
    exit_code: int | None
    failure_note: str | None
    executed: bool


@dataclass(frozen=True, slots=True)
class _PreparedRun:
    run_id: str


class RunService:
    """Persist an auditable run envelope before invoking a local MSM command."""

    def __init__(
        self,
        vault_root: str | Path,
        snapshot_source: MSMSnapshotSource | None = None,
        *,
        writer: LedgerWriter | None = None,
        clock: Callable[[], datetime] | None = None,
        executor: ProcessExecutor | None = None,
        git_state_reader: GitStateReader | None = None,
    ) -> None:
        self.writer = writer or LedgerWriter(vault_root)
        resolved_root = Path(vault_root).resolve()
        if self.writer.vault_root != resolved_root:
            raise ValueError("writer and run service must use the same vault root")
        self.registry = self.writer.registry
        self.snapshot_source = snapshot_source
        self.clock = clock or (lambda: datetime.now(UTC))
        self.executor = executor or _subprocess_executor
        self.git_state_reader = git_state_reader or _read_git_state

    def run_preregistered(self, request: PreregisteredRunRequest) -> RunResult:
        """Bind and execute an existing committed preregistered prediction run."""

        prepared = self._prepare_preregistered(request)
        return self._execute(prepared.run_id)

    def run_exploratory(self, request: ExploratoryRunRequest) -> RunResult:
        """Capture MSM state, commit an exploratory envelope, then execute it."""

        prepared = self._prepare_exploratory(request)
        return self._execute(prepared.run_id)

    def _prepare_preregistered(self, request: PreregisteredRunRequest) -> _PreparedRun:
        working_directory = _working_directory(request.working_directory)
        request_hash = sha256_json(
            {
                "mode": RegistrationStatus.PREREGISTERED.value,
                "prediction_id": request.prediction_id,
                "command": list(request.command),
                "working_directory": str(working_directory),
            }
        )
        with ledger_lock(self.writer.ledger_dir, timeout=self.writer.lock_timeout):
            connection = self.registry.connect()
            try:
                connection.execute("BEGIN IMMEDIATE")
                existing = self._idempotent_run(
                    request.idempotency_key,
                    request_hash,
                    connection=connection,
                )
                if existing is not None:
                    connection.commit()
                    return _PreparedRun(existing["run_id"])

                prediction = self.registry.get_prediction(
                    request.prediction_id,
                    connection=connection,
                )
                if prediction is None or prediction["transaction_state"] != "committed":
                    raise IntegrityError("preregistered execution requires a committed prediction")
                if prediction["registration_status"] != RegistrationStatus.PREREGISTERED.value:
                    raise IntegrityError(
                        "only a preregistered prediction can use the preregistered run path"
                    )
                run = self.registry.get_run_by_prediction_id(
                    request.prediction_id,
                    connection=connection,
                )
                if run is None or run["run_id"] != prediction["run_id"]:
                    raise IntegrityError("committed prediction is missing its allocated run")
                bound = self.registry.get_run(run["run_id"], connection=connection)
                if bound is None or bound["state"] != RunState.REGISTERED.value:
                    raise RunStateError("prediction run has already started")
                if bound["envelope_json"] is not None:
                    raise RunStateError("prediction run is already bound to another request")

                forecast, snapshot = self._load_preregistered_snapshot(prediction)
                bound_at = self._clock_time()
                envelope = _envelope(
                    run_id=run["run_id"],
                    prediction_id=request.prediction_id,
                    registration_status=RegistrationStatus.PREREGISTERED,
                    strategy_id=forecast.strategy_id,
                    command=request.command,
                    working_directory=working_directory,
                    snapshot=snapshot,
                    bound_at=bound_at,
                    prediction_immutable_hash=prediction["immutable_hash"],
                )
                envelope_hash = sha256_json(envelope)
                self.registry.bind_preregistered_run(
                    run_id=run["run_id"],
                    idempotency_key=request.idempotency_key,
                    request_hash=request_hash,
                    strategy_id=forecast.strategy_id,
                    dataset_version=snapshot.dataset_version,
                    envelope_json=canonical_json(envelope),
                    envelope_hash=envelope_hash,
                    bound_at=bound_at,
                    connection=connection,
                )
                connection.commit()
            except BaseException:
                if connection.in_transaction:
                    connection.rollback()
                logger.exception(
                    "preregistered_run_envelope_failed",
                    extra={"prediction_id": request.prediction_id},
                )
                raise
            finally:
                connection.close()

        logger.info(
            "preregistered_run_envelope_committed",
            extra={"prediction_id": request.prediction_id, "run_id": run["run_id"]},
        )
        return _PreparedRun(run["run_id"])

    def _prepare_exploratory(self, request: ExploratoryRunRequest) -> _PreparedRun:
        working_directory = _working_directory(request.working_directory)
        request_hash = sha256_json(
            {
                "mode": RegistrationStatus.EXPLORATORY.value,
                "strategy_id": request.strategy_id,
                "in_sample_window": request.in_sample_window.model_dump(mode="json"),
                "out_of_sample_window": request.out_of_sample_window.model_dump(mode="json"),
                "command": list(request.command),
                "working_directory": str(working_directory),
            }
        )
        with ledger_lock(self.writer.ledger_dir, timeout=self.writer.lock_timeout):
            connection = self.registry.connect()
            try:
                connection.execute("BEGIN IMMEDIATE")
                existing = self._idempotent_run(
                    request.idempotency_key,
                    request_hash,
                    connection=connection,
                )
                if existing is not None:
                    connection.commit()
                    return _PreparedRun(existing["run_id"])

                bound_at = self._clock_time()
                snapshot = self._capture_exploratory_snapshot(request, bound_at)
                run_id = f"run_{uuid.uuid4().hex}"
                envelope = _envelope(
                    run_id=run_id,
                    prediction_id=None,
                    registration_status=RegistrationStatus.EXPLORATORY,
                    strategy_id=request.strategy_id,
                    command=request.command,
                    working_directory=working_directory,
                    snapshot=snapshot,
                    bound_at=bound_at,
                    prediction_immutable_hash=None,
                )
                envelope_hash = sha256_json(envelope)
                self.registry.create_exploratory_run(
                    run_id=run_id,
                    started_at=bound_at,
                    idempotency_key=request.idempotency_key,
                    request_hash=request_hash,
                    strategy_id=request.strategy_id,
                    dataset_version=snapshot.dataset_version,
                    envelope_json=canonical_json(envelope),
                    envelope_hash=envelope_hash,
                    connection=connection,
                )
                connection.commit()
            except BaseException:
                if connection.in_transaction:
                    connection.rollback()
                logger.exception(
                    "exploratory_run_envelope_failed",
                    extra={"idempotency_key": request.idempotency_key},
                )
                raise
            finally:
                connection.close()

        logger.info(
            "exploratory_run_envelope_committed",
            extra={"run_id": run_id, "strategy_id": request.strategy_id},
        )
        return _PreparedRun(run_id)

    def _execute(self, run_id: str) -> RunResult:
        row = self.registry.get_run(run_id)
        if row is None or row["envelope_json"] is None:
            raise IntegrityError(f"run has no durable execution envelope: {run_id}")
        if row["state"] in (RunState.COMPLETED.value, RunState.FAILED.value):
            return self._result(row, executed=False)

        execution_lock = run_execution_lock(self.writer.ledger_dir, run_id)
        try:
            execution_lock.acquire()
        except FileLockTimeout as exc:
            raise RunStateError(f"run is currently executing: {run_id}") from exc
        try:
            return self._execute_locked(run_id)
        finally:
            execution_lock.release()

    def _execute_locked(self, run_id: str) -> RunResult:
        """Execute or reconcile a run while holding its process-owned lease."""

        row = self.registry.get_run(run_id)
        if row is None or row["envelope_json"] is None:
            raise IntegrityError(f"run has no durable execution envelope: {run_id}")
        if row["state"] in (RunState.COMPLETED.value, RunState.FAILED.value):
            return self._result(row, executed=False)
        if row["state"] == RunState.RUNNING.value:
            self.registry.finish_run(
                run_id,
                completed_at=self._clock_time(),
                exit_code=1,
                failure_note="wrapper exited before recording process completion",
            )
            orphaned = self.registry.get_run(run_id)
            if orphaned is None:  # pragma: no cover - permanent registry identity
                raise IntegrityError(f"orphaned run disappeared: {run_id}")
            logger.warning("ledger_orphaned_run_failed", extra={"run_id": run_id})
            return self._result(orphaned, executed=False)
        if row["state"] != RunState.REGISTERED.value:
            raise RunStateError(f"run has unsupported state {row['state']}: {run_id}")

        envelope = self._verified_envelope(row)
        command = tuple(str(value) for value in envelope["command"]["argv"])
        working_directory = Path(str(envelope["command"]["working_directory"]))
        self._verify_command_checkout(envelope, working_directory)
        self.registry.start_run(run_id, started_at=self._clock_time())
        environment = self._execution_environment(envelope, row["envelope_hash"])
        logger.info(
            "ledger_run_started",
            extra={"run_id": run_id, "registration_status": envelope["registration_status"]},
        )
        try:
            exit_code = self.executor(command, working_directory, environment)
            if isinstance(exit_code, bool) or not isinstance(exit_code, int):
                raise TypeError("process executor must return an integer exit code")
        except KeyboardInterrupt:
            self.registry.finish_run(
                run_id,
                completed_at=self._clock_time(),
                exit_code=130,
                failure_note="wrapper interrupted",
            )
            raise
        except Exception as exc:
            failure_note = f"{type(exc).__name__}: {exc}"
            self.registry.finish_run(
                run_id,
                completed_at=self._clock_time(),
                exit_code=1,
                failure_note=failure_note,
            )
            failed = self.registry.get_run(run_id)
            if failed is None:  # pragma: no cover - permanent registry identity
                raise IntegrityError(f"failed run disappeared: {run_id}") from exc
            logger.exception("ledger_run_executor_failed", extra={"run_id": run_id})
            return self._result(failed, executed=False)

        failure_note = None if exit_code == 0 else f"process exited with code {exit_code}"
        self.registry.finish_run(
            run_id,
            completed_at=self._clock_time(),
            exit_code=exit_code,
            failure_note=failure_note,
        )
        finished = self.registry.get_run(run_id)
        if finished is None:  # pragma: no cover - protected by permanent registry identity
            raise IntegrityError(f"finished run disappeared: {run_id}")
        return self._result(finished, executed=True)

    def _verify_command_checkout(
        self,
        envelope: Mapping[str, Any],
        working_directory: Path,
    ) -> None:
        try:
            commit, dirty = self.git_state_reader(working_directory)
        except IntegrityError:
            raise
        except Exception as exc:
            raise IntegrityError("cannot inspect the MSM command checkout") from exc
        if dirty:
            raise IntegrityError("MSM command checkout is dirty")
        expected_commit = envelope.get("snapshot", {}).get("git_commit")
        if commit != expected_commit:
            raise IntegrityError("MSM command checkout does not match the snapshot git_commit")

    def _capture_exploratory_snapshot(
        self,
        request: ExploratoryRunRequest,
        decision_at: datetime,
    ) -> StrategySnapshot:
        if self.snapshot_source is None:
            raise SnapshotCaptureError("exploratory runs require a local MSM snapshot source")
        try:
            raw = self.snapshot_source.capture_snapshot(
                strategy_id=request.strategy_id,
                decision_at=decision_at,
                in_sample_window={
                    "start": request.in_sample_window.start,
                    "end": request.in_sample_window.end,
                },
                out_of_sample_window={
                    "start": request.out_of_sample_window.start,
                    "end": request.out_of_sample_window.end,
                },
            )
            snapshot = StrategySnapshot.model_validate(raw)
            _verify_snapshot_binding(
                snapshot,
                strategy_id=request.strategy_id,
                in_sample_window=request.in_sample_window,
                out_of_sample_window=request.out_of_sample_window,
                decision_at=decision_at,
            )
            return snapshot
        except SnapshotCaptureError:
            raise
        except Exception as exc:
            raise SnapshotCaptureError("exploratory MSM snapshot capture failed") from exc

    def _load_preregistered_snapshot(
        self,
        prediction: sqlite3.Row,
    ) -> tuple[StrategyEdgeForecast, StrategySnapshot]:
        result = self.writer.result_for_row(prediction)
        try:
            snapshot_value = json.loads(result.snapshot_path.read_text(encoding="utf-8"))
            snapshot = StrategySnapshot.model_validate(snapshot_value)
            frontmatter = _read_frontmatter(result.record_path)
            forecast = StrategyEdgeForecast.model_validate(frontmatter["forecast"])
            if frontmatter.get("id") != prediction["prediction_id"]:
                raise IntegrityError("prediction record id does not match the registry")
            if frontmatter.get("run_id") != prediction["run_id"]:
                raise IntegrityError("prediction record run_id does not match the registry")
            if frontmatter.get("schema_id") != prediction["schema_id"]:
                raise IntegrityError("prediction record schema_id does not match the registry")
            if frontmatter.get("schema_hash") != prediction["schema_hash"]:
                raise IntegrityError("prediction record schema_hash does not match the registry")
            if frontmatter.get("registration_status") != prediction["registration_status"]:
                raise IntegrityError(
                    "prediction record registration_status does not match the registry"
                )
            if frontmatter.get("snapshot_ref") != prediction["snapshot_ref"]:
                raise IntegrityError("prediction record snapshot_ref does not match the registry")
            if frontmatter.get("immutable_hash") != prediction["immutable_hash"]:
                raise IntegrityError("prediction record immutable_hash does not match the registry")
            if _isoformat(frontmatter.get("created_at")) != prediction["created_at"]:
                raise IntegrityError("prediction record created_at does not match the registry")
            if _isoformat(frontmatter.get("committed_at")) != prediction["committed_at"]:
                raise IntegrityError("prediction record committed_at does not match the registry")
            immutable_payload = {
                "registration_status": prediction["registration_status"],
                "forecast": forecast.model_dump(mode="json"),
                "decision": frontmatter["decision"],
                "snapshot": snapshot_value,
                "snapshot_ref": prediction["snapshot_ref"],
                "schema_hash": prediction["schema_hash"],
                "lineage": frontmatter["lineage"],
            }
            if sha256_json(immutable_payload) != prediction["immutable_hash"]:
                raise IntegrityError(
                    "prediction artifacts do not match the immutable registry hash"
                )
            _verify_snapshot_binding(
                snapshot,
                strategy_id=forecast.strategy_id,
                in_sample_window=SnapshotDateWindow(
                    start=forecast.in_sample_window.start,
                    end=forecast.in_sample_window.end,
                ),
                out_of_sample_window=SnapshotDateWindow(
                    start=forecast.out_of_sample_window.start,
                    end=forecast.out_of_sample_window.end,
                ),
                decision_at=datetime.fromisoformat(prediction["created_at"]),
            )
            return forecast, snapshot
        except IntegrityError:
            raise
        except Exception as exc:
            raise IntegrityError("committed prediction artifacts are not executable") from exc

    def _idempotent_run(
        self,
        idempotency_key: str,
        request_hash: str,
        *,
        connection: sqlite3.Connection,
    ) -> sqlite3.Row | None:
        existing = self.registry.get_run_by_idempotency_key(
            idempotency_key,
            connection=connection,
        )
        if existing is not None and existing["request_hash"] != request_hash:
            raise IdempotencyConflictError(
                "idempotency key is already bound to a different run request"
            )
        return existing

    def _clock_time(self) -> datetime:
        value = self.clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise IntegrityError("run clock must return a timezone-aware value")
        return value

    @staticmethod
    def _verified_envelope(row: sqlite3.Row) -> dict[str, Any]:
        try:
            envelope = json.loads(row["envelope_json"])
        except (TypeError, json.JSONDecodeError) as exc:
            raise IntegrityError("run envelope is not valid JSON") from exc
        if not isinstance(envelope, dict) or sha256_json(envelope) != row["envelope_hash"]:
            raise IntegrityError("run envelope does not match its immutable hash")
        snapshot = envelope.get("snapshot")
        if not isinstance(snapshot, dict):
            raise IntegrityError("run envelope is missing its snapshot")
        if envelope.get("registration_status") != row["registration_status"]:
            raise IntegrityError("run envelope registration_status does not match its binding")
        if envelope.get("strategy_id") != row["strategy_id"]:
            raise IntegrityError("run envelope strategy_id does not match its binding")
        if snapshot.get("dataset_version") != row["dataset_version"]:
            raise IntegrityError("run envelope dataset_version does not match its binding")
        return envelope

    def _result(self, row: sqlite3.Row, *, executed: bool) -> RunResult:
        envelope = self._verified_envelope(row)
        snapshot = envelope.get("snapshot")
        if not isinstance(snapshot, dict) or not isinstance(snapshot.get("dataset_version"), str):
            raise IntegrityError("run envelope is missing its dataset version")
        state = RunState(row["state"])
        if state in (RunState.COMPLETED, RunState.FAILED) and not isinstance(row["exit_code"], int):
            raise IntegrityError("terminal run is missing its exit code")
        return RunResult(
            run_id=row["run_id"],
            prediction_id=row["prediction_id"],
            registration_status=RegistrationStatus(envelope["registration_status"]),
            strategy_id=str(envelope["strategy_id"]),
            dataset_version=snapshot["dataset_version"],
            envelope_hash=row["envelope_hash"],
            state=state,
            exit_code=row["exit_code"],
            failure_note=row["failure_note"],
            executed=executed,
        )

    @staticmethod
    def _execution_environment(
        envelope: Mapping[str, Any],
        envelope_hash: str,
    ) -> dict[str, str]:
        environment = dict(os.environ)
        environment.update(
            {
                "LEDGER_RUN_ID": str(envelope["run_id"]),
                "LEDGER_REGISTRATION_STATUS": str(envelope["registration_status"]),
                "LEDGER_DATASET_VERSION": str(envelope["snapshot"]["dataset_version"]),
                "LEDGER_ENVELOPE_HASH": envelope_hash,
            }
        )
        prediction_id = envelope.get("prediction_id")
        if prediction_id is not None:
            environment["LEDGER_PREDICTION_ID"] = str(prediction_id)
        else:
            environment.pop("LEDGER_PREDICTION_ID", None)
        return environment


def _working_directory(value: Path | None) -> Path:
    path = (value or Path.cwd()).expanduser().resolve()
    if not path.is_dir():
        raise ValueError(f"working directory does not exist: {path}")
    return path


def _envelope(
    *,
    run_id: str,
    prediction_id: str | None,
    registration_status: RegistrationStatus,
    strategy_id: str,
    command: Sequence[str],
    working_directory: Path,
    snapshot: StrategySnapshot,
    bound_at: datetime,
    prediction_immutable_hash: str | None,
) -> dict[str, Any]:
    return {
        "envelope_format_version": 1,
        "run_id": run_id,
        "prediction_id": prediction_id,
        "registration_status": registration_status.value,
        "strategy_id": strategy_id,
        "bound_at": bound_at.isoformat(),
        "prediction_immutable_hash": prediction_immutable_hash,
        "command": {
            "argv": list(command),
            "working_directory": str(working_directory),
        },
        "snapshot": snapshot.model_dump(mode="json"),
    }


def _verify_snapshot_binding(
    snapshot: StrategySnapshot,
    *,
    strategy_id: str,
    in_sample_window: SnapshotDateWindow,
    out_of_sample_window: SnapshotDateWindow,
    decision_at: datetime,
) -> None:
    if snapshot.strategy_id != strategy_id:
        raise IntegrityError("MSM snapshot strategy_id does not match the run")
    if snapshot.in_sample_window != in_sample_window:
        raise IntegrityError("MSM snapshot in-sample window does not match the run")
    if snapshot.out_of_sample_window != out_of_sample_window:
        raise IntegrityError("MSM snapshot out-of-sample window does not match the run")
    if snapshot.data_as_of_version.astimezone(UTC) != decision_at.astimezone(UTC):
        raise IntegrityError("MSM snapshot data_as_of_version does not match the envelope time")


def _read_frontmatter(path: Path) -> dict[str, Any]:
    content = path.read_text(encoding="utf-8")
    if not content.startswith("---\n"):
        raise IntegrityError("prediction record has no YAML frontmatter")
    parts = content.split("---\n", maxsplit=2)
    if len(parts) != 3:
        raise IntegrityError("prediction record has malformed YAML frontmatter")
    value = yaml.safe_load(parts[1])
    if not isinstance(value, dict):
        raise IntegrityError("prediction record frontmatter is not an object")
    return value


def _isoformat(value: Any) -> str | None:
    if isinstance(value, datetime):
        return value.isoformat()
    return value if isinstance(value, str) else None


def _subprocess_executor(
    command: Sequence[str],
    working_directory: Path,
    environment: Mapping[str, str],
) -> int:
    completed = subprocess.run(
        list(command),
        cwd=working_directory,
        env=dict(environment),
        check=False,
        shell=False,
    )
    return completed.returncode


def _read_git_state(working_directory: Path) -> tuple[str, bool]:
    try:
        commit = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=working_directory,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        status = subprocess.run(
            ["git", "status", "--porcelain", "--untracked-files=normal"],
            cwd=working_directory,
            check=True,
            capture_output=True,
            text=True,
        ).stdout
    except (OSError, subprocess.CalledProcessError) as exc:
        raise IntegrityError("cannot inspect the MSM command checkout") from exc
    return commit, bool(status.strip())
