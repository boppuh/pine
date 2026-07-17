"""Command-line enforcement boundary for local MSM executions."""

from __future__ import annotations

import argparse
import importlib
import json
import sys
from collections.abc import Sequence
from datetime import date
from pathlib import Path
from typing import Any

from ledger.errors import LedgerError
from ledger.msm import SnapshotDateWindow
from ledger.run import (
    ExploratoryRunRequest,
    PreregisteredRunRequest,
    RunResult,
    RunService,
)


def build_parser() -> argparse.ArgumentParser:
    """Build the ``msm-ledger`` argument parser."""

    parser = argparse.ArgumentParser(
        prog="msm-ledger",
        description="Persist a ledger run envelope before invoking a local MSM command.",
    )
    subparsers = parser.add_subparsers(dest="subcommand", required=True)
    run = subparsers.add_parser("run", help="capture, register, and execute one MSM run")
    run.add_argument(
        "--vault-root",
        "--vault",
        dest="vault_root",
        type=Path,
        default=Path.cwd(),
        help="vault containing .ledger/ (default: current directory)",
    )
    run.add_argument(
        "--idempotency-key",
        required=True,
        help="stable caller-generated key; retries must reuse it",
    )
    mode = run.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--prediction-id",
        help="committed preregistered prediction whose allocated run should execute",
    )
    mode.add_argument(
        "--strategy-id",
        help="MSM strategy to capture in a new permanent exploratory envelope",
    )
    run.add_argument("--in-sample-start", type=date.fromisoformat)
    run.add_argument("--in-sample-end", type=date.fromisoformat)
    run.add_argument("--out-of-sample-start", type=date.fromisoformat)
    run.add_argument("--out-of-sample-end", type=date.fromisoformat)
    run.add_argument(
        "--working-directory",
        type=Path,
        help="directory in which MSM is invoked (default: current directory)",
    )
    run.add_argument(
        "command",
        nargs=argparse.REMAINDER,
        help="MSM command argv after --, for example: -- uv run msm ...",
    )
    return parser


def run_cli(argv: Sequence[str] | None = None) -> int:
    """Parse arguments, execute the wrapper, and return a shell exit code."""

    parser = build_parser()
    arguments = parser.parse_args(argv)
    if arguments.subcommand != "run":  # pragma: no cover - argparse enforces this
        parser.error("a subcommand is required")

    command = tuple(arguments.command)
    if command[:1] == ("--",):
        command = command[1:]
    if not command:
        parser.error("run requires an MSM command after --")

    try:
        if arguments.prediction_id is not None:
            request = PreregisteredRunRequest(
                idempotency_key=arguments.idempotency_key,
                prediction_id=arguments.prediction_id,
                command=command,
                working_directory=arguments.working_directory,
            )
            service = RunService(arguments.vault_root)
            result = service.run_preregistered(request)
        else:
            missing = [
                option
                for option, value in (
                    ("--in-sample-start", arguments.in_sample_start),
                    ("--in-sample-end", arguments.in_sample_end),
                    ("--out-of-sample-start", arguments.out_of_sample_start),
                    ("--out-of-sample-end", arguments.out_of_sample_end),
                )
                if value is None
            ]
            if missing:
                parser.error("exploratory runs require " + ", ".join(missing))
            request = ExploratoryRunRequest(
                idempotency_key=arguments.idempotency_key,
                strategy_id=arguments.strategy_id,
                in_sample_window=SnapshotDateWindow(
                    start=arguments.in_sample_start,
                    end=arguments.in_sample_end,
                ),
                out_of_sample_window=SnapshotDateWindow(
                    start=arguments.out_of_sample_start,
                    end=arguments.out_of_sample_end,
                ),
                command=command,
                working_directory=arguments.working_directory,
            )
            service = RunService(arguments.vault_root, _local_msm_snapshot_source())
            result = service.run_exploratory(request)
    except LedgerError as exc:
        print(f"msm-ledger: {exc}", file=sys.stderr)
        return 2
    except (ImportError, ValueError) as exc:
        print(f"msm-ledger: {exc}", file=sys.stderr)
        return 2

    print(json.dumps(_result_payload(result), sort_keys=True))
    if result.exit_code is None:
        return 0
    if result.exit_code < 0:
        return min(255, 128 + abs(result.exit_code))
    return result.exit_code


def main() -> None:
    """Console-script entry point."""

    raise SystemExit(run_cli())


def _local_msm_snapshot_source() -> Any:
    try:
        client_module = importlib.import_module("msm.ch.client")
        snapshot_module = importlib.import_module("msm.utils.ledger_snapshot")
    except ImportError as exc:
        raise ImportError(
            "exploratory runs require the local MSM package in this environment"
        ) from exc
    return snapshot_module.LedgerSnapshotSource(client_module.get_client())


def _result_payload(result: RunResult) -> dict[str, Any]:
    return {
        "run_id": result.run_id,
        "prediction_id": result.prediction_id,
        "registration_status": result.registration_status.value,
        "strategy_id": result.strategy_id,
        "dataset_version": result.dataset_version,
        "envelope_hash": result.envelope_hash,
        "state": result.state.value,
        "exit_code": result.exit_code,
        "failure_note": result.failure_note,
        "executed": result.executed,
    }
