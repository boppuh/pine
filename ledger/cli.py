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
from ledger.external import (
    ExternalRunEvidence,
    ExternalRunImportResult,
    ExternalRunIngestor,
    ExternalRunIngestRequest,
)
from ledger.msm import SnapshotDateWindow
from ledger.results import (
    MSMResultIngestor,
    MSMResultIngestRequest,
    MSMResultIngestResult,
    MSMRunResultEvidence,
)
from ledger.run import (
    ExploratoryRunRequest,
    PreregisteredRunRequest,
    RunResult,
    RunService,
    RunState,
)

MSM_RESULT_INTEGRATION_VERSION = 1


def build_parser() -> argparse.ArgumentParser:
    """Build the ``msm-ledger`` argument parser."""

    parser = argparse.ArgumentParser(
        prog="msm-ledger",
        description="Enforce or recover auditable local MSM run provenance.",
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
        "--result-evidence",
        type=Path,
        help=(
            "path the child will receive as LEDGER_RESULT_EVIDENCE_PATH; "
            "ingest it automatically after a successful run"
        ),
    )
    run.add_argument(
        "command",
        nargs=argparse.REMAINDER,
        help="MSM command argv after --, for example: -- uv run msm ...",
    )
    ingest = subparsers.add_parser(
        "ingest-external",
        help="import explicit evidence for a completed MSM run that bypassed the wrapper",
    )
    ingest.add_argument(
        "--vault-root",
        "--vault",
        dest="vault_root",
        type=Path,
        default=Path.cwd(),
        help="vault containing .ledger/ (default: current directory)",
    )
    ingest.add_argument(
        "--evidence",
        required=True,
        type=Path,
        help="JSON evidence document for one completed direct MSM run",
    )
    ingest_result = subparsers.add_parser(
        "ingest-result",
        help="attach immutable canonical result evidence to a successful bound MSM run",
    )
    ingest_result.add_argument(
        "--vault-root",
        "--vault",
        dest="vault_root",
        type=Path,
        default=Path.cwd(),
        help="vault containing .ledger/ (default: current directory)",
    )
    ingest_result.add_argument(
        "--evidence",
        required=True,
        type=Path,
        help="canonical JSON result evidence for one successful bound run",
    )
    return parser


def run_cli(argv: Sequence[str] | None = None) -> int:
    """Parse arguments, execute the wrapper, and return a shell exit code."""

    parser = build_parser()
    arguments = parser.parse_args(argv)
    if arguments.subcommand == "ingest-external":
        return _ingest_external_cli(arguments.vault_root, arguments.evidence)
    if arguments.subcommand == "ingest-result":
        return _ingest_result_cli(arguments.vault_root, arguments.evidence)
    if arguments.subcommand != "run":  # pragma: no cover - argparse enforces this
        parser.error("a subcommand is required")

    command = tuple(arguments.command)
    if command[:1] == ("--",):
        command = command[1:]
    if not command:
        parser.error("run requires an MSM command after --")

    working_directory = _working_directory(arguments.working_directory)
    result_evidence_path = _result_evidence_path(
        arguments.result_evidence,
        working_directory=working_directory,
    )

    try:
        if arguments.prediction_id is not None:
            request = PreregisteredRunRequest(
                idempotency_key=arguments.idempotency_key,
                prediction_id=arguments.prediction_id,
                command=command,
                working_directory=working_directory,
                result_evidence_path=result_evidence_path,
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
                working_directory=working_directory,
                result_evidence_path=result_evidence_path,
            )
            service = RunService(arguments.vault_root, _local_msm_snapshot_source())
            result = service.run_exploratory(request)
    except LedgerError as exc:
        print(f"msm-ledger: {exc}", file=sys.stderr)
        return 2
    except (ImportError, ValueError) as exc:
        print(f"msm-ledger: {exc}", file=sys.stderr)
        return 2

    ingested_result = None
    if result_evidence_path is not None and _is_successful_run(result):
        try:
            ingested_result = _ingest_result(arguments.vault_root, result_evidence_path)
        except LedgerError as exc:
            print(
                f"msm-ledger: run {result.run_id} completed but result ingestion failed: {exc}",
                file=sys.stderr,
            )
            return 2
        except (OSError, ValueError) as exc:
            print(
                f"msm-ledger: run {result.run_id} completed but result evidence is invalid: {exc}",
                file=sys.stderr,
            )
            return 2

    print(json.dumps(_result_payload(result, ingested_result), sort_keys=True))
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


def _result_payload(
    result: RunResult,
    ingested_result: MSMResultIngestResult | None = None,
) -> dict[str, Any]:
    payload = {
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
    if ingested_result is not None:
        payload["result_ingestion"] = _ingested_result_payload(ingested_result)
    return payload


def _working_directory(value: Path | None) -> Path:
    return (value or Path.cwd()).expanduser().resolve()


def _result_evidence_path(value: Path | None, *, working_directory: Path) -> Path | None:
    if value is None:
        return None
    expanded = value.expanduser()
    if not expanded.is_absolute():
        expanded = working_directory / expanded
    return expanded.resolve()


def _is_successful_run(result: RunResult) -> bool:
    return (
        result.state is RunState.COMPLETED
        and result.exit_code == 0
        and result.failure_note is None
    )


def _ingest_external_cli(vault_root: Path, evidence_path: Path) -> int:
    try:
        raw = json.loads(evidence_path.read_text(encoding="utf-8"))
        evidence = ExternalRunEvidence.model_validate(raw)
        result = ExternalRunIngestor(vault_root).ingest(ExternalRunIngestRequest(evidence=evidence))
    except LedgerError as exc:
        print(f"msm-ledger: {exc}", file=sys.stderr)
        return 2
    except (OSError, ValueError) as exc:
        print(f"msm-ledger: invalid external evidence: {exc}", file=sys.stderr)
        return 2

    print(json.dumps(_external_result_payload(result), sort_keys=True))
    return 0


def _external_result_payload(result: ExternalRunImportResult) -> dict[str, Any]:
    return {
        "run_id": result.run_id,
        "source_system": result.source_system,
        "source_run_id": result.source_run_id,
        "registration_status": result.registration_status.value,
        "strategy_id": result.strategy_id,
        "dataset_version": result.dataset_version,
        "evidence_hash": result.evidence_hash,
        "envelope_hash": result.envelope_hash,
        "state": result.state.value,
        "exit_code": result.exit_code,
        "failure_note": result.failure_note,
        "created": result.created,
    }


def _ingest_result_cli(vault_root: Path, evidence_path: Path) -> int:
    try:
        result = _ingest_result(vault_root, evidence_path)
    except LedgerError as exc:
        print(f"msm-ledger: {exc}", file=sys.stderr)
        return 2
    except (OSError, ValueError) as exc:
        print(f"msm-ledger: invalid result evidence: {exc}", file=sys.stderr)
        return 2

    print(json.dumps(_ingested_result_payload(result), sort_keys=True))
    return 0


def _ingest_result(vault_root: Path, evidence_path: Path) -> MSMResultIngestResult:
    raw = json.loads(evidence_path.read_text(encoding="utf-8"))
    evidence = MSMRunResultEvidence.model_validate(raw)
    return MSMResultIngestor(vault_root).ingest(MSMResultIngestRequest(evidence=evidence))


def _ingested_result_payload(result: MSMResultIngestResult) -> dict[str, Any]:
    return {
        "run_id": result.run_id,
        "prediction_id": result.prediction_id,
        "registration_status": result.registration_status.value,
        "strategy_id": result.strategy_id,
        "dataset_version": result.dataset_version,
        "envelope_hash": result.envelope_hash,
        "evidence_hash": result.evidence_hash,
        "source_timestamp": result.source_timestamp.isoformat(),
        "ingested_at": result.ingested_at.isoformat(),
        "created": result.created,
    }
