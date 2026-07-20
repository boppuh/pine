from __future__ import annotations

import copy
import json
import sqlite3
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

import ledger.cli as cli
from ledger.errors import IdempotencyConflictError, IntegrityError
from ledger.integrity import PredictionDraft, PredictionStatus, RegistrationStatus
from ledger.json_utils import sha256_json
from ledger.msm import SnapshotDateWindow
from ledger.results import MSMResultIngestor, MSMRunResultEvidence
from ledger.run import (
    ExploratoryRunRequest,
    PreregisteredRunRequest,
    RunResult,
    RunService,
)
from ledger.writer import LedgerWriter

RUN_AT = datetime(2026, 7, 18, 13, 30, tzinfo=UTC)
INGESTED_AT = RUN_AT + timedelta(minutes=5)
GIT_COMMIT = "b" * 40


class FakeMSMSource:
    def __init__(self, snapshot: Mapping[str, Any]) -> None:
        self.snapshot = snapshot

    def capture_snapshot(self, **_kwargs: Any) -> Mapping[str, Any]:
        return self.snapshot


def _snapshot() -> dict[str, Any]:
    manifest = {
        "database": "msm",
        "server_version": "26.1.3.52",
        "window": {"start": "2020-01-01", "end": "2025-12-31"},
        "tables": [
            {
                "table": "ohlcv_5m",
                "part_count": 1,
                "row_count": 84361,
                "min_date": "2020-01-01",
                "max_date": "2025-12-31",
                "parts": [
                    {
                        "name": "all_1_1_0",
                        "partition_id": "all",
                        "rows": 84361,
                        "bytes_on_disk": 4096,
                        "min_date": "2020-01-01",
                        "max_date": "2025-12-31",
                        "min_block_number": 1,
                        "max_block_number": 1,
                        "level": 0,
                        "data_version": 1,
                        "modification_epoch": int((RUN_AT - timedelta(days=1)).timestamp()),
                    }
                ],
            }
        ],
    }
    return {
        "snapshot_format_version": 1,
        "strategy_id": "msm-strat-orb-001",
        "strategy_spec_hash": f"sha256:{'a' * 64}",
        "git_commit": GIT_COMMIT,
        "parameter_set": {"lookback": 40, "threshold": 0.5},
        "parameter_count": 2,
        "data_as_of_version": RUN_AT.isoformat(),
        "dataset_version": sha256_json(manifest),
        "dataset_manifest": manifest,
        "universe_definition": {
            "mode": "explicit",
            "symbols": ["AAPL", "MSFT"],
            "excluded_symbols": [],
        },
        "in_sample_window": {"start": "2020-01-01", "end": "2023-12-31"},
        "out_of_sample_window": {"start": "2024-01-01", "end": "2025-12-31"},
        "cost_model_version": "msm.equity-costs:v1",
        "slippage_model_version": "msm.next-open:v1",
        "metric_definition_version": "msm.strategy-edge-metrics:v1",
        "engine_version": "msm.backtest:v1",
        "random_seed": 42,
        "captured_at": RUN_AT.isoformat(),
    }


def _clean_git(working_directory: Path) -> tuple[str, bool]:
    del working_directory
    return GIT_COMMIT, False


def _executor(exit_code: int):
    def execute(
        command: Sequence[str],
        working_directory: Path,
        environment: Mapping[str, str],
    ) -> int:
        del command, working_directory, environment
        return exit_code

    return execute


def _run(
    vault: Path,
    *,
    exit_code: int = 0,
) -> tuple[RunResult, dict[str, Any]]:
    snapshot = _snapshot()
    service = RunService(
        vault,
        FakeMSMSource(snapshot),
        clock=lambda: RUN_AT,
        executor=_executor(exit_code),
        git_state_reader=_clean_git,
    )
    result = service.run_exploratory(
        ExploratoryRunRequest(
            idempotency_key=f"result-run-{exit_code}",
            strategy_id=snapshot["strategy_id"],
            in_sample_window=SnapshotDateWindow.model_validate(snapshot["in_sample_window"]),
            out_of_sample_window=SnapshotDateWindow.model_validate(
                snapshot["out_of_sample_window"]
            ),
            command=("uv", "run", "msm", "orb"),
            working_directory=vault,
        )
    )
    return result, snapshot


def _evidence(result: RunResult, snapshot: dict[str, Any]) -> dict[str, Any]:
    return {
        "result_format_version": 1,
        "source_system": "msm",
        "metric_units": "finance/strategy-edge:decimal-v1",
        "run_id": result.run_id,
        "prediction_id": result.prediction_id,
        "registration_status": result.registration_status.value,
        "strategy_id": result.strategy_id,
        "envelope_hash": result.envelope_hash,
        "dataset_version": result.dataset_version,
        "git_commit": snapshot["git_commit"],
        "metric_definition_version": snapshot["metric_definition_version"],
        "source_timestamp": (RUN_AT + timedelta(minutes=1)).isoformat(),
        "in_sample_window": snapshot["in_sample_window"],
        "out_of_sample_window": snapshot["out_of_sample_window"],
        "in_sample_metrics": {
            "sharpe": 1.44,
            "win_rate": 0.56,
            "max_drawdown": 0.12,
            "expectancy": 0.0018,
            "total_return": 0.31,
            "trade_count": 213,
        },
        "out_of_sample_metrics": {
            "sharpe": 0.91,
            "win_rate": 0.52,
            "max_drawdown": 0.18,
            "expectancy": 0.0009,
            "total_return": 0.14,
            "trade_count": 87,
        },
        "regime_breakdown": [
            {
                "sample": "in_sample",
                "regime_id": "high_volatility",
                "metrics": {
                    "sharpe": 1.2,
                    "win_rate": 0.54,
                    "max_drawdown": 0.1,
                    "expectancy": 0.0015,
                    "total_return": 0.2,
                    "trade_count": 90,
                },
            },
            {
                "sample": "out_of_sample",
                "regime_id": "high_volatility",
                "metrics": {
                    "sharpe": 0.7,
                    "win_rate": 0.51,
                    "max_drawdown": 0.16,
                    "expectancy": 0.0007,
                    "total_return": 0.08,
                    "trade_count": 34,
                },
            },
        ],
        "artifacts": [
            {
                "relative_path": "audit_results.json",
                "sha256": f"sha256:{'c' * 64}",
                "size_bytes": 128,
            },
            {
                "relative_path": "summary.csv",
                "sha256": f"sha256:{'d' * 64}",
                "size_bytes": 256,
            },
        ],
        "metadata": {"adapter": "msm-ledger-result:v1"},
    }


def test_successful_run_result_is_bound_immutably(vault: Path) -> None:
    run, snapshot = _run(vault)
    service = MSMResultIngestor(vault, clock=lambda: INGESTED_AT)

    result = service.ingest({"evidence": _evidence(run, snapshot)})

    assert result.created is True
    assert result.run_id == run.run_id
    assert result.registration_status is RegistrationStatus.EXPLORATORY
    assert result.envelope_hash == run.envelope_hash
    row = service.registry.get_run_result(run.run_id)
    assert row is not None
    assert row["evidence_hash"] == result.evidence_hash
    assert json.loads(row["evidence_json"])["out_of_sample_metrics"]["win_rate"] == 0.52
    connection = service.registry.connect()
    try:
        assert connection.execute("SELECT COUNT(*) FROM predictions").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM run_results").fetchone()[0] == 1
    finally:
        connection.close()


def test_exact_result_retry_is_a_noop(vault: Path) -> None:
    run, snapshot = _run(vault)
    request = {"evidence": _evidence(run, snapshot)}
    service = MSMResultIngestor(vault, clock=lambda: INGESTED_AT)

    first = service.ingest(request)
    second = service.ingest(request)

    assert first.created is True
    assert second.created is False
    assert second.evidence_hash == first.evidence_hash
    assert second.ingested_at == first.ingested_at
    connection = service.registry.connect()
    try:
        assert connection.execute("SELECT COUNT(*) FROM run_results").fetchone()[0] == 1
    finally:
        connection.close()


def test_changed_result_for_same_run_fails_closed(vault: Path) -> None:
    run, snapshot = _run(vault)
    original = _evidence(run, snapshot)
    service = MSMResultIngestor(vault, clock=lambda: INGESTED_AT)
    service.ingest({"evidence": original})
    changed = copy.deepcopy(original)
    changed["out_of_sample_metrics"]["sharpe"] = 9.9

    with pytest.raises(IdempotencyConflictError, match="different immutable"):
        service.ingest({"evidence": changed})


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("prediction_id", "pred_wrong"),
        ("registration_status", "preregistered"),
        ("strategy_id", "wrong-strategy"),
        ("envelope_hash", f"sha256:{'e' * 64}"),
        ("dataset_version", f"sha256:{'f' * 64}"),
        ("git_commit", "0" * 40),
        ("metric_definition_version", "wrong:v1"),
        ("in_sample_window", {"start": "2020-01-02", "end": "2023-12-31"}),
    ],
)
def test_result_provenance_must_match_run_envelope(
    vault: Path,
    field: str,
    value: object,
) -> None:
    run, snapshot = _run(vault)
    evidence = _evidence(run, snapshot)
    evidence[field] = value
    service = MSMResultIngestor(vault, clock=lambda: INGESTED_AT)

    with pytest.raises(IntegrityError, match="provenance"):
        service.ingest({"evidence": evidence})

    assert service.registry.get_run_result(run.run_id) is None


def test_failed_or_unknown_run_cannot_receive_results(vault: Path) -> None:
    failed, snapshot = _run(vault, exit_code=17)
    service = MSMResultIngestor(vault, clock=lambda: INGESTED_AT)

    with pytest.raises(IntegrityError, match="successful terminal"):
        service.ingest({"evidence": _evidence(failed, snapshot)})

    unknown = _evidence(failed, snapshot)
    unknown["run_id"] = "run_unknown"
    with pytest.raises(IntegrityError, match="bound run"):
        service.ingest({"evidence": unknown})

    connection = service.registry.connect()
    try:
        with pytest.raises(sqlite3.IntegrityError, match="successful bound run"):
            connection.execute(
                """
                INSERT INTO run_results (
                    run_id, evidence_hash, evidence_json, source_timestamp, ingested_at
                ) VALUES (?, ?, '{}', ?, ?)
                """,
                (
                    failed.run_id,
                    f"sha256:{'0' * 64}",
                    RUN_AT.isoformat(),
                    INGESTED_AT.isoformat(),
                ),
            )
    finally:
        connection.close()


@pytest.mark.parametrize(
    "invalid",
    ["percent_win_rate", "boolean_metric", "artifact_order", "unsafe_path"],
)
def test_result_contract_rejects_ambiguous_or_unsafe_values(
    vault: Path,
    invalid: str,
) -> None:
    run, snapshot = _run(vault)
    evidence = _evidence(run, snapshot)
    if invalid == "percent_win_rate":
        evidence["out_of_sample_metrics"]["win_rate"] = 52
    elif invalid == "boolean_metric":
        evidence["out_of_sample_metrics"]["sharpe"] = True
    elif invalid == "artifact_order":
        evidence["artifacts"].reverse()
    else:
        evidence["artifacts"][0]["relative_path"] = "../audit_results.json"

    with pytest.raises(ValidationError):
        MSMRunResultEvidence.model_validate(evidence)


@pytest.mark.parametrize(
    "source_timestamp",
    [RUN_AT - timedelta(seconds=1), INGESTED_AT + timedelta(seconds=1)],
)
def test_result_source_timestamp_is_bounded_by_execution_and_ingestion(
    vault: Path,
    source_timestamp: datetime,
) -> None:
    run, snapshot = _run(vault)
    evidence = _evidence(run, snapshot)
    evidence["source_timestamp"] = source_timestamp.isoformat()
    service = MSMResultIngestor(vault, clock=lambda: INGESTED_AT)

    with pytest.raises(IntegrityError, match="source_timestamp"):
        service.ingest({"evidence": evidence})


def test_run_result_registry_row_cannot_be_updated_or_deleted(vault: Path) -> None:
    run, snapshot = _run(vault)
    service = MSMResultIngestor(vault, clock=lambda: INGESTED_AT)
    service.ingest({"evidence": _evidence(run, snapshot)})
    connection = service.registry.connect()
    try:
        with pytest.raises(sqlite3.IntegrityError, match="write-once"):
            connection.execute(
                "UPDATE run_results SET evidence_hash = ? WHERE run_id = ?",
                (f"sha256:{'f' * 64}", run.run_id),
            )
        with pytest.raises(sqlite3.IntegrityError, match="permanent"):
            connection.execute("DELETE FROM run_results WHERE run_id = ?", (run.run_id,))
    finally:
        connection.close()


def test_preregistered_result_does_not_resolve_or_grade_prediction(
    vault: Path,
    valid_forecast: dict[str, object],
) -> None:
    snapshot = _snapshot()
    writer = LedgerWriter(vault)
    prediction = PredictionDraft.model_validate(
        {
            "prediction_id": "pred_result_01",
            "run_id": "run_result_01",
            "registration_status": "preregistered",
            "forecast": valid_forecast,
            "decision": "Run the frozen specification.",
            "snapshot": snapshot,
            "lineage": {"family_id": "fam_result_01"},
            "created_at": RUN_AT,
        }
    )
    writer.write(prediction)
    run_service = RunService(
        vault,
        writer=writer,
        clock=lambda: RUN_AT,
        executor=_executor(0),
        git_state_reader=_clean_git,
    )
    run = run_service.run_preregistered(
        PreregisteredRunRequest(
            idempotency_key="preregistered-result-01",
            prediction_id=prediction.prediction_id,
            command=("uv", "run", "msm", "orb"),
            working_directory=vault,
        )
    )

    MSMResultIngestor(vault, clock=lambda: INGESTED_AT).ingest(
        {"evidence": _evidence(run, snapshot)}
    )

    row = writer.registry.get_prediction(prediction.prediction_id)
    assert row is not None
    assert row["status"] == PredictionStatus.OPEN.value
    assert row["outcome_json"] is None
    assert row["grade_json"] is None
    assert row["resolution_metadata_json"] is None


def test_cli_ingests_results_without_loading_msm(
    vault: Path,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run, snapshot = _run(vault)
    evidence_path = tmp_path / "run-result.json"
    evidence_path.write_text(json.dumps(_evidence(run, snapshot)), encoding="utf-8")
    monkeypatch.setattr(
        cli,
        "_local_msm_snapshot_source",
        lambda: pytest.fail("result ingestion must not load MSM"),
    )

    exit_code = cli.run_cli(
        [
            "ingest-result",
            "--vault-root",
            str(vault),
            "--evidence",
            str(evidence_path),
        ]
    )

    assert exit_code == 0
    output = json.loads(capsys.readouterr().out)
    assert output["run_id"] == run.run_id
    assert output["registration_status"] == "exploratory"
    assert output["created"] is True
