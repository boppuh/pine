from __future__ import annotations

import inspect
import json
import sqlite3
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from ledger.errors import (
    IdempotencyConflictError,
    IntegrityError,
    RunStateError,
    SnapshotCaptureError,
)
from ledger.integrity import PredictionDraft, RegistrationStatus
from ledger.json_utils import sha256_json
from ledger.locking import run_execution_lock
from ledger.msm import SnapshotDateWindow
from ledger.run import (
    ExploratoryRunRequest,
    PreregisteredRunRequest,
    RunService,
    RunState,
)
from ledger.writer import LedgerWriter

DECISION_AT = datetime(2026, 7, 16, 13, 30, tzinfo=UTC)
GIT_COMMIT = "b" * 40


def _clean_git(_working_directory: Path) -> tuple[str, bool]:
    return GIT_COMMIT, False


class FakeMSMSource:
    def __init__(
        self,
        snapshot: Mapping[str, Any],
        *,
        error: Exception | None = None,
    ) -> None:
        self.snapshot = snapshot
        self.error = error
        self.calls: list[dict[str, Any]] = []

    def capture_snapshot(self, **kwargs: Any) -> Mapping[str, Any]:
        self.calls.append(kwargs)
        if self.error is not None:
            raise self.error
        return self.snapshot


def _snapshot(
    *,
    strategy_id: str = "msm-strat-orb-001",
    in_start: str = "2020-01-01",
    in_end: str = "2023-12-31",
    out_start: str = "2024-01-01",
    out_end: str = "2025-12-31",
    data_as_of: datetime = DECISION_AT,
) -> dict[str, Any]:
    manifest = {
        "database": "default",
        "server_version": "26.1.3.52",
        "window": {"start": in_start, "end": out_end},
        "tables": [
            {
                "table": "ohlcv_1m",
                "part_count": 1,
                "row_count": 100,
                "min_date": in_start,
                "max_date": out_end,
                "parts": [
                    {
                        "name": "202001_1_1_0",
                        "partition_id": "202001",
                        "rows": 100,
                        "bytes_on_disk": 4096,
                        "min_date": in_start,
                        "max_date": out_end,
                        "min_block_number": 1,
                        "max_block_number": 1,
                        "level": 0,
                        "data_version": 1,
                        "modification_epoch": int((DECISION_AT - timedelta(days=1)).timestamp()),
                    }
                ],
            }
        ],
    }
    return {
        "snapshot_format_version": 1,
        "strategy_id": strategy_id,
        "strategy_spec_hash": f"sha256:{'a' * 64}",
        "git_commit": GIT_COMMIT,
        "parameter_set": {"lookback": 40, "threshold": 0.5},
        "parameter_count": 2,
        "data_as_of_version": data_as_of.isoformat(),
        "dataset_version": sha256_json(manifest),
        "dataset_manifest": manifest,
        "universe_definition": {
            "mode": "explicit",
            "symbols": ["AAPL", "MSFT"],
            "excluded_symbols": [],
        },
        "in_sample_window": {"start": in_start, "end": in_end},
        "out_of_sample_window": {"start": out_start, "end": out_end},
        "cost_model_version": "msm.equity-costs:v1",
        "slippage_model_version": "msm.next-open:v1",
        "metric_definition_version": "msm.strategy-edge-metrics:v1",
        "engine_version": "msm.backtest:v1",
        "random_seed": 42,
        "captured_at": DECISION_AT.isoformat(),
    }


def _exploratory_request(
    vault: Path,
    *,
    idempotency_key: str = "explore-01",
    command: tuple[str, ...] = ("uv", "run", "msm", "orb"),
) -> ExploratoryRunRequest:
    return ExploratoryRunRequest(
        idempotency_key=idempotency_key,
        strategy_id="msm-strat-orb-001",
        in_sample_window=SnapshotDateWindow(start="2020-01-01", end="2023-12-31"),
        out_of_sample_window=SnapshotDateWindow(start="2024-01-01", end="2025-12-31"),
        command=command,
        working_directory=vault,
    )


def test_exploratory_envelope_is_durable_before_executor_starts(vault: Path) -> None:
    source = FakeMSMSource(_snapshot())
    observed: dict[str, Any] = {}
    service: RunService

    def executor(
        command: Sequence[str],
        working_directory: Path,
        environment: Mapping[str, str],
    ) -> int:
        row = service.registry.get_run(environment["LEDGER_RUN_ID"])
        assert row is not None
        assert row["state"] == RunState.RUNNING.value
        envelope = json.loads(row["envelope_json"])
        assert envelope["registration_status"] == RegistrationStatus.EXPLORATORY.value
        assert envelope["prediction_id"] is None
        assert envelope["snapshot"]["dataset_version"] == environment["LEDGER_DATASET_VERSION"]
        assert environment["LEDGER_ENVELOPE_HASH"] == row["envelope_hash"]
        assert "LEDGER_PREDICTION_ID" not in environment
        assert service.registry.is_window_touched(
            "msm-strat-orb-001",
            "2024-01-01",
            "2025-12-31",
        )
        observed.update(
            command=tuple(command),
            working_directory=working_directory,
            envelope=envelope,
        )
        return 0

    service = RunService(
        vault,
        source,
        clock=lambda: DECISION_AT,
        executor=executor,
        git_state_reader=_clean_git,
    )
    result = service.run_exploratory(_exploratory_request(vault))

    assert result.state is RunState.COMPLETED
    assert result.registration_status is RegistrationStatus.EXPLORATORY
    assert result.executed is True
    assert observed["command"] == ("uv", "run", "msm", "orb")
    assert observed["working_directory"] == vault.resolve()
    assert len(source.calls) == 1
    assert service.registry.window_overlaps_touched(
        "msm-strat-orb-001",
        "2025-01-01",
        "2026-01-01",
    )


def test_exploratory_retry_does_not_recapture_or_reexecute(vault: Path) -> None:
    source = FakeMSMSource(_snapshot())
    calls = 0

    def executor(*_args: Any) -> int:
        nonlocal calls
        calls += 1
        return 0

    service = RunService(
        vault,
        source,
        clock=lambda: DECISION_AT,
        executor=executor,
        git_state_reader=_clean_git,
    )
    request = _exploratory_request(vault)

    first = service.run_exploratory(request)
    connection = service.registry.connect()
    try:
        touched_before = connection.execute("SELECT * FROM touched_windows").fetchall()
    finally:
        connection.close()
    second = service.run_exploratory(request)
    connection = service.registry.connect()
    try:
        touched_after = connection.execute("SELECT * FROM touched_windows").fetchall()
    finally:
        connection.close()

    assert second.run_id == first.run_id
    assert second.executed is False
    assert calls == 1
    assert len(source.calls) == 1
    assert [tuple(row) for row in touched_after] == [tuple(row) for row in touched_before]
    assert len(touched_after) == 1


def test_retry_reconciles_orphaned_running_run_without_reexecution(vault: Path) -> None:
    source = FakeMSMSource(_snapshot())
    executed = False

    def executor(*_args: Any) -> int:
        nonlocal executed
        executed = True
        return 0

    service = RunService(
        vault,
        source,
        clock=lambda: DECISION_AT,
        executor=executor,
        git_state_reader=_clean_git,
    )
    request = _exploratory_request(vault)
    prepared = service._prepare_exploratory(request)
    service.registry.start_run(
        prepared.run_id,
        started_at=DECISION_AT,
    )

    result = service.run_exploratory(request)

    assert result.run_id == prepared.run_id
    assert result.state is RunState.FAILED
    assert result.exit_code == 1
    assert result.executed is False
    assert executed is False
    assert len(source.calls) == 1
    row = service.registry.get_run(prepared.run_id)
    assert row is not None
    assert row["failure_note"] == "wrapper exited before recording process completion"


def test_retry_does_not_reconcile_running_run_while_owner_lock_is_live(vault: Path) -> None:
    service = RunService(
        vault,
        FakeMSMSource(_snapshot()),
        clock=lambda: DECISION_AT,
        executor=lambda *_args: 0,
        git_state_reader=_clean_git,
    )
    request = _exploratory_request(vault)
    prepared = service._prepare_exploratory(request)
    service.registry.start_run(
        prepared.run_id,
        started_at=DECISION_AT,
    )

    with run_execution_lock(service.writer.ledger_dir, prepared.run_id):
        with pytest.raises(RunStateError, match="currently executing"):
            service.run_exploratory(request)

    row = service.registry.get_run(prepared.run_id)
    assert row is not None
    assert row["state"] == RunState.RUNNING.value


def test_exploratory_idempotency_key_cannot_change_command(vault: Path) -> None:
    source = FakeMSMSource(_snapshot())
    service = RunService(
        vault,
        source,
        clock=lambda: DECISION_AT,
        executor=lambda *_: 0,
        git_state_reader=_clean_git,
    )
    service.run_exploratory(_exploratory_request(vault))

    with pytest.raises(IdempotencyConflictError, match="different run request"):
        service.run_exploratory(
            _exploratory_request(vault, command=("uv", "run", "msm", "different"))
        )


def test_snapshot_failure_creates_no_run_and_never_calls_executor(vault: Path) -> None:
    source = FakeMSMSource(_snapshot(), error=ConnectionError("ClickHouse offline"))
    executed = False

    def executor(*_args: Any) -> int:
        nonlocal executed
        executed = True
        return 0

    service = RunService(
        vault,
        source,
        clock=lambda: DECISION_AT,
        executor=executor,
        git_state_reader=_clean_git,
    )

    with pytest.raises(SnapshotCaptureError, match="snapshot capture failed"):
        service.run_exploratory(_exploratory_request(vault))

    connection = service.registry.connect()
    try:
        assert connection.execute("SELECT COUNT(*) FROM runs").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM run_bindings").fetchone()[0] == 0
    finally:
        connection.close()
    assert executed is False
    assert not service.registry.is_window_touched(
        "msm-strat-orb-001",
        "2024-01-01",
        "2025-12-31",
    )


def test_run_start_rolls_back_if_window_touch_cannot_commit(vault: Path) -> None:
    executed = False

    def executor(*_args: Any) -> int:
        nonlocal executed
        executed = True
        return 0

    service = RunService(
        vault,
        FakeMSMSource(_snapshot()),
        clock=lambda: DECISION_AT,
        executor=executor,
        git_state_reader=_clean_git,
    )
    prepared = service._prepare_exploratory(_exploratory_request(vault))
    connection = service.registry.connect()
    try:
        connection.execute(
            """
            CREATE TRIGGER reject_touched_window
            BEFORE INSERT ON touched_windows
            BEGIN
                SELECT RAISE(ABORT, 'simulated touch failure');
            END
            """
        )
        connection.commit()
    finally:
        connection.close()

    with pytest.raises(sqlite3.IntegrityError, match="simulated touch failure"):
        service._execute(prepared.run_id)

    row = service.registry.get_run(prepared.run_id)
    assert row is not None
    assert row["state"] == RunState.REGISTERED.value
    assert row["execution_started_at"] is None
    assert executed is False
    assert not service.registry.is_window_touched(
        "msm-strat-orb-001",
        "2024-01-01",
        "2025-12-31",
    )


@pytest.mark.parametrize(
    ("git_state", "message"),
    [
        (("c" * 40, False), "does not match"),
        ((GIT_COMMIT, True), "checkout is dirty"),
    ],
)
def test_command_checkout_must_match_snapshot_before_execution(
    vault: Path,
    git_state: tuple[str, bool],
    message: str,
) -> None:
    executed = False

    def executor(*_args: Any) -> int:
        nonlocal executed
        executed = True
        return 0

    service = RunService(
        vault,
        FakeMSMSource(_snapshot()),
        clock=lambda: DECISION_AT,
        executor=executor,
        git_state_reader=lambda _working_directory: git_state,
    )

    with pytest.raises(IntegrityError, match=message):
        service.run_exploratory(_exploratory_request(vault))

    connection = service.registry.connect()
    try:
        row = connection.execute(
            """
            SELECT runs.state, run_bindings.registration_status
            FROM runs JOIN run_bindings USING (run_id)
            """
        ).fetchone()
        assert row["state"] == RunState.REGISTERED.value
        assert row["registration_status"] == RegistrationStatus.EXPLORATORY.value
    finally:
        connection.close()
    assert executed is False
    assert not service.registry.is_window_touched(
        "msm-strat-orb-001",
        "2024-01-01",
        "2025-12-31",
    )


def test_nonzero_exit_is_permanent_failed_run(vault: Path) -> None:
    service = RunService(
        vault,
        FakeMSMSource(_snapshot()),
        clock=lambda: DECISION_AT,
        executor=lambda *_: 17,
        git_state_reader=_clean_git,
    )

    result = service.run_exploratory(_exploratory_request(vault))
    retry = service.run_exploratory(_exploratory_request(vault))

    assert result.state is RunState.FAILED
    assert result.exit_code == 17
    assert retry.executed is False
    assert service.registry.is_window_touched(
        "msm-strat-orb-001",
        "2024-01-01",
        "2025-12-31",
    )


def test_executor_exception_is_recorded_as_failed(vault: Path) -> None:
    calls = 0

    def executor(*_args: Any) -> int:
        nonlocal calls
        calls += 1
        raise OSError("runner missing")

    service = RunService(
        vault,
        FakeMSMSource(_snapshot()),
        clock=lambda: DECISION_AT,
        executor=executor,
        git_state_reader=_clean_git,
    )

    request = _exploratory_request(vault)
    first = service.run_exploratory(request)
    retry = service.run_exploratory(request)

    assert first.state is RunState.FAILED
    assert first.exit_code == 1
    assert first.failure_note == "OSError: runner missing"
    assert first.executed is False
    assert retry == first
    assert calls == 1

    connection = service.registry.connect()
    try:
        row = connection.execute("SELECT * FROM runs").fetchone()
        assert row["state"] == RunState.FAILED.value
        assert row["exit_code"] == 1
        assert row["failure_note"] == "OSError: runner missing"
    finally:
        connection.close()
    assert service.registry.is_window_touched(
        "msm-strat-orb-001",
        "2024-01-01",
        "2025-12-31",
    )


def test_preregistered_run_uses_existing_prediction_and_snapshot(
    vault: Path,
    valid_forecast: dict[str, object],
) -> None:
    writer = LedgerWriter(vault)
    prediction = PredictionDraft.model_validate(
        {
            "prediction_id": "pred_strict_01",
            "run_id": "run_strict_01",
            "registration_status": RegistrationStatus.PREREGISTERED,
            "forecast": valid_forecast,
            "decision": "Run the frozen specification.",
            "snapshot": _snapshot(),
            "lineage": {"family_id": "fam_strict_01"},
            "created_at": DECISION_AT,
        }
    )
    writer.write(prediction)
    observed_environment: dict[str, str] = {}

    def executor(
        _command: Sequence[str],
        _working_directory: Path,
        environment: Mapping[str, str],
    ) -> int:
        observed_environment.update(environment)
        assert service.registry.is_window_touched(
            "fam_strict_01",
            "2024-01-01",
            "2025-12-31",
        )
        return 0

    service = RunService(
        vault,
        writer=writer,
        clock=lambda: DECISION_AT,
        executor=executor,
        git_state_reader=_clean_git,
    )
    result = service.run_preregistered(
        PreregisteredRunRequest(
            idempotency_key="preregistered-run-01",
            prediction_id=prediction.prediction_id,
            command=("uv", "run", "msm", "orb"),
            working_directory=vault,
        )
    )

    assert result.run_id == prediction.run_id
    assert result.prediction_id == prediction.prediction_id
    assert result.registration_status is RegistrationStatus.PREREGISTERED
    assert observed_environment["LEDGER_PREDICTION_ID"] == prediction.prediction_id
    assert service.registry.window_overlaps_touched(
        "fam_strict_01",
        "2025-12-31",
        "2026-01-01",
    )


def test_tampered_preregistered_snapshot_prevents_execution(
    vault: Path,
    valid_forecast: dict[str, object],
) -> None:
    writer = LedgerWriter(vault)
    prediction = PredictionDraft.model_validate(
        {
            "prediction_id": "pred_tampered_01",
            "run_id": "run_tampered_01",
            "registration_status": RegistrationStatus.PREREGISTERED,
            "forecast": valid_forecast,
            "decision": "Run the frozen specification.",
            "snapshot": _snapshot(),
            "lineage": {"family_id": "fam_tampered_01"},
            "created_at": DECISION_AT,
        }
    )
    result = writer.write(prediction)
    snapshot = json.loads(result.snapshot_path.read_text(encoding="utf-8"))
    snapshot["random_seed"] = 99
    result.snapshot_path.write_text(json.dumps(snapshot), encoding="utf-8")
    executed = False

    def executor(*_args: Any) -> int:
        nonlocal executed
        executed = True
        return 0

    service = RunService(
        vault,
        writer=writer,
        clock=lambda: DECISION_AT,
        executor=executor,
        git_state_reader=_clean_git,
    )

    with pytest.raises(IntegrityError, match="immutable registry hash"):
        service.run_preregistered(
            PreregisteredRunRequest(
                idempotency_key="tampered-run-01",
                prediction_id=prediction.prediction_id,
                command=("uv", "run", "msm", "orb"),
                working_directory=vault,
            )
        )

    assert executed is False
    assert service.registry.get_run(prediction.run_id)["state"] == RunState.REGISTERED.value


def test_run_binding_is_write_once_and_exploratory_cannot_be_promoted(vault: Path) -> None:
    service = RunService(
        vault,
        FakeMSMSource(_snapshot()),
        clock=lambda: DECISION_AT,
        executor=lambda *_: 0,
        git_state_reader=_clean_git,
    )
    result = service.run_exploratory(_exploratory_request(vault))
    assert "registration_status" not in ExploratoryRunRequest.model_fields
    assert (
        "registration_status"
        not in inspect.signature(service.registry.create_exploratory_run).parameters
    )
    start_parameters = inspect.signature(service.registry.start_run).parameters
    assert "family_id" not in start_parameters
    assert "window_start" not in start_parameters
    assert "window_end" not in start_parameters
    assert not hasattr(service.registry, "bind_run")

    connection = service.registry.connect()
    try:
        with pytest.raises(sqlite3.IntegrityError, match="write-once"):
            connection.execute(
                "UPDATE run_bindings SET envelope_json = '{}' WHERE run_id = ?",
                (result.run_id,),
            )
        with pytest.raises(sqlite3.IntegrityError, match="permanent"):
            connection.execute("DELETE FROM run_bindings WHERE run_id = ?", (result.run_id,))
    finally:
        connection.close()
