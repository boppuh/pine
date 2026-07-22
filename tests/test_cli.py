from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

import ledger.cli as cli
from ledger.integrity import RegistrationStatus
from ledger.results import MSMResultIngestResult
from ledger.run import RunResult, RunState


def _result(
    *,
    prediction_id: str | None = None,
    state: RunState = RunState.COMPLETED,
    exit_code: int = 0,
    failure_note: str | None = None,
    executed: bool = True,
) -> RunResult:
    return RunResult(
        run_id="run_cli_01",
        prediction_id=prediction_id,
        registration_status=(
            RegistrationStatus.PREREGISTERED
            if prediction_id is not None
            else RegistrationStatus.EXPLORATORY
        ),
        strategy_id="msm-strat-orb-001",
        dataset_version=f"sha256:{'a' * 64}",
        envelope_hash=f"sha256:{'b' * 64}",
        state=state,
        exit_code=exit_code,
        failure_note=failure_note,
        executed=executed,
    )


def _ingested(*, created: bool) -> MSMResultIngestResult:
    timestamp = datetime(2026, 7, 22, 13, 0, tzinfo=UTC)
    return MSMResultIngestResult(
        run_id="run_cli_01",
        prediction_id=None,
        registration_status=RegistrationStatus.EXPLORATORY,
        strategy_id="msm-strat-orb-001",
        dataset_version=f"sha256:{'a' * 64}",
        envelope_hash=f"sha256:{'b' * 64}",
        evidence_hash=f"sha256:{'c' * 64}",
        source_timestamp=timestamp,
        ingested_at=timestamp,
        created=created,
    )


def test_cli_builds_exploratory_request_and_preserves_command_argv(
    vault: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    observed: dict[str, Any] = {}

    class FakeService:
        def __init__(self, vault_root: Path, source: object) -> None:
            observed["vault_root"] = vault_root
            observed["source"] = source

        def run_exploratory(self, request: object) -> RunResult:
            observed["request"] = request
            return _result()

    source = object()
    monkeypatch.setattr(cli, "RunService", FakeService)
    monkeypatch.setattr(cli, "_local_msm_snapshot_source", lambda: source)

    exit_code = cli.run_cli(
        [
            "run",
            "--vault",
            str(vault),
            "--idempotency-key",
            "cli-explore-01",
            "--strategy-id",
            "msm-strat-orb-001",
            "--in-sample-start",
            "2020-01-01",
            "--in-sample-end",
            "2023-12-31",
            "--out-of-sample-start",
            "2024-01-01",
            "--out-of-sample-end",
            "2025-12-31",
            "--",
            "uv",
            "run",
            "msm",
            "orb",
            "--output-dir",
            "results/test run",
        ]
    )

    assert exit_code == 0
    request = observed["request"]
    assert request.command == (
        "uv",
        "run",
        "msm",
        "orb",
        "--output-dir",
        "results/test run",
    )
    assert observed["source"] is source
    output = json.loads(capsys.readouterr().out)
    assert output["registration_status"] == "exploratory"


def test_cli_preregistered_path_does_not_construct_msm_snapshot_source(
    vault: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, Any] = {}

    class FakeService:
        def __init__(self, vault_root: Path) -> None:
            observed["vault_root"] = vault_root

        def run_preregistered(self, request: object) -> RunResult:
            observed["request"] = request
            return _result(prediction_id="pred_cli_01")

    monkeypatch.setattr(cli, "RunService", FakeService)
    monkeypatch.setattr(
        cli,
        "_local_msm_snapshot_source",
        lambda: pytest.fail("preregistered path must use its committed snapshot"),
    )

    exit_code = cli.run_cli(
        [
            "run",
            "--vault-root",
            str(vault),
            "--idempotency-key",
            "cli-preregistered-01",
            "--prediction-id",
            "pred_cli_01",
            "--",
            "uv",
            "run",
            "msm",
            "orb",
        ]
    )

    assert exit_code == 0
    assert observed["request"].prediction_id == "pred_cli_01"


def test_cli_rejects_exploratory_run_without_all_windows(vault: Path) -> None:
    with pytest.raises(SystemExit, match="2"):
        cli.run_cli(
            [
                "run",
                "--vault",
                str(vault),
                "--idempotency-key",
                "cli-invalid-01",
                "--strategy-id",
                "msm-strat-orb-001",
                "--",
                "uv",
                "run",
                "msm",
            ]
        )


def test_cli_returns_terminal_executor_failure_as_json(
    vault: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    class FakeService:
        def __init__(self, _vault_root: Path) -> None:
            pass

        def run_preregistered(self, _request: object) -> RunResult:
            return _result(
                prediction_id="pred_cli_failed",
                state=RunState.FAILED,
                exit_code=1,
                failure_note="OSError: runner missing",
                executed=False,
            )

    monkeypatch.setattr(cli, "RunService", FakeService)

    exit_code = cli.run_cli(
        [
            "run",
            "--vault",
            str(vault),
            "--idempotency-key",
            "cli-failed-01",
            "--prediction-id",
            "pred_cli_failed",
            "--",
            "missing-runner",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 1
    assert captured.err == ""
    output = json.loads(captured.out)
    assert output["state"] == "failed"
    assert output["failure_note"] == "OSError: runner missing"
    assert output["executed"] is False


def test_cli_exact_retry_automatically_ingests_child_result_evidence(
    vault: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    observed: dict[str, Any] = {}

    class FakeService:
        def __init__(self, _vault_root: Path, _source: object) -> None:
            pass

        def run_exploratory(self, request: object) -> RunResult:
            observed["request"] = request
            return _result(executed=False)

    def ingest(vault_root: Path, evidence_path: Path) -> MSMResultIngestResult:
        observed["ingest"] = (vault_root, evidence_path)
        return _ingested(created=False)

    monkeypatch.setattr(cli, "RunService", FakeService)
    monkeypatch.setattr(cli, "_local_msm_snapshot_source", object)
    monkeypatch.setattr(cli, "_ingest_result", ingest)

    exit_code = cli.run_cli(
        [
            "run",
            "--vault",
            str(vault),
            "--idempotency-key",
            "cli-auto-ingest-01",
            "--strategy-id",
            "msm-strat-orb-001",
            "--in-sample-start",
            "2020-01-01",
            "--in-sample-end",
            "2023-12-31",
            "--out-of-sample-start",
            "2024-01-01",
            "--out-of-sample-end",
            "2025-12-31",
            "--working-directory",
            str(tmp_path),
            "--result-evidence",
            "outputs/result.json",
            "--",
            "msm-ledger-result",
            "run",
        ]
    )

    expected_path = (tmp_path / "outputs/result.json").resolve()
    assert exit_code == 0
    assert observed["request"].result_evidence_path == expected_path
    assert observed["ingest"] == (vault, expected_path)
    output = json.loads(capsys.readouterr().out)
    assert output["executed"] is False
    assert output["result_ingestion"]["created"] is False
    assert output["result_ingestion"]["evidence_hash"] == f"sha256:{'c' * 64}"


def test_cli_does_not_ingest_result_evidence_after_failed_run(
    vault: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    class FakeService:
        def __init__(self, _vault_root: Path) -> None:
            pass

        def run_preregistered(self, _request: object) -> RunResult:
            return _result(
                prediction_id="pred_cli_failed",
                state=RunState.FAILED,
                exit_code=17,
                failure_note="child failed",
            )

    monkeypatch.setattr(cli, "RunService", FakeService)
    monkeypatch.setattr(
        cli,
        "_ingest_result",
        lambda *_args: pytest.fail("failed runs must not ingest result evidence"),
    )

    exit_code = cli.run_cli(
        [
            "run",
            "--vault",
            str(vault),
            "--idempotency-key",
            "cli-auto-ingest-failed",
            "--prediction-id",
            "pred_cli_failed",
            "--working-directory",
            str(tmp_path),
            "--result-evidence",
            "missing.json",
            "--",
            "false",
        ]
    )

    assert exit_code == 17
    output = json.loads(capsys.readouterr().out)
    assert "result_ingestion" not in output


def test_cli_reports_ingestion_failure_without_reclassifying_completed_run(
    vault: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    class FakeService:
        def __init__(self, _vault_root: Path) -> None:
            pass

        def run_preregistered(self, _request: object) -> RunResult:
            return _result(prediction_id="pred_cli_complete")

    monkeypatch.setattr(cli, "RunService", FakeService)

    exit_code = cli.run_cli(
        [
            "run",
            "--vault",
            str(vault),
            "--idempotency-key",
            "cli-auto-ingest-missing",
            "--prediction-id",
            "pred_cli_complete",
            "--working-directory",
            str(tmp_path),
            "--result-evidence",
            "missing.json",
            "--",
            "true",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 2
    assert captured.out == ""
    assert "run run_cli_01 completed but result evidence is invalid" in captured.err
