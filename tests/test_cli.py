from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

import ledger.cli as cli
from ledger.integrity import RegistrationStatus
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
