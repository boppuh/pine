from __future__ import annotations

import copy
import inspect
import json
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

import ledger.cli as cli
from ledger.errors import IdempotencyConflictError, IntegrityError
from ledger.external import (
    ExternalRunEvidence,
    ExternalRunIngestor,
    ExternalRunIngestRequest,
)
from ledger.integrity import RegistrationStatus
from ledger.json_utils import sha256_json
from ledger.run import RunState

STARTED_AT = datetime(2026, 7, 17, 19, 30, 11, tzinfo=UTC)
COMPLETED_AT = STARTED_AT + timedelta(minutes=3)
INGESTED_AT = COMPLETED_AT + timedelta(minutes=5)


def _snapshot() -> dict[str, Any]:
    manifest = {
        "database": "msm",
        "server_version": "26.1.3.52",
        "window": {"start": "2026-04-22", "end": "2026-04-22"},
        "tables": [
            {
                "table": "ohlcv_5m",
                "part_count": 1,
                "row_count": 84361,
                "min_date": "2026-04-01",
                "max_date": "2026-04-22",
                "parts": [
                    {
                        "name": "202604_1_1_0",
                        "partition_id": "202604",
                        "rows": 84361,
                        "bytes_on_disk": 4096,
                        "min_date": "2026-04-01",
                        "max_date": "2026-04-22",
                        "min_block_number": 1,
                        "max_block_number": 1,
                        "level": 0,
                        "data_version": 1,
                        "modification_epoch": int((STARTED_AT - timedelta(days=1)).timestamp()),
                    }
                ],
            }
        ],
    }
    return {
        "snapshot_format_version": 1,
        "strategy_id": "vwap_mr_v3.1",
        "strategy_spec_hash": f"sha256:{'a' * 64}",
        "git_commit": "b" * 40,
        "parameter_set": {"lookback": 40, "threshold": 0.5},
        "parameter_count": 2,
        "data_as_of_version": STARTED_AT.isoformat(),
        "dataset_version": sha256_json(manifest),
        "dataset_manifest": manifest,
        "universe_definition": {
            "mode": "explicit",
            "symbols": ["AAPL", "MSFT"],
            "excluded_symbols": [],
        },
        "in_sample_window": {"start": "2026-04-22", "end": "2026-04-22"},
        "out_of_sample_window": {"start": "2026-04-22", "end": "2026-04-22"},
        "cost_model_version": "msm.equity-costs:v1",
        "slippage_model_version": "msm.next-open:v1",
        "metric_definition_version": "msm.strategy-edge-metrics:v1",
        "engine_version": "msm.backtest:v1",
        "random_seed": 42,
        "captured_at": STARTED_AT.isoformat(),
    }


def _evidence(vault: Path, *, source_run_id: str = "msm-direct-20260717-01") -> dict[str, Any]:
    return {
        "evidence_format_version": 1,
        "source_system": "msm",
        "source_run_id": source_run_id,
        "strategy_id": "vwap_mr_v3.1",
        "started_at": STARTED_AT.isoformat(),
        "completed_at": COMPLETED_AT.isoformat(),
        "exit_code": 0,
        "failure_note": None,
        "command": [
            "uv",
            "run",
            "python",
            "scripts/frozen/frozen_vwap_mr_v3_1.py",
        ],
        "working_directory": str(vault.resolve()),
        "snapshot": _snapshot(),
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
        "metadata": {"output_root": "/tmp/msm-direct-output"},
    }


def test_direct_run_is_ingested_as_permanent_low_integrity(vault: Path) -> None:
    service = ExternalRunIngestor(vault, clock=lambda: INGESTED_AT)
    request = ExternalRunIngestRequest.model_validate({"evidence": _evidence(vault)})

    result = service.ingest(request)

    assert result.created is True
    assert result.registration_status is RegistrationStatus.UNREGISTERED_EXTERNAL
    assert result.state is RunState.COMPLETED
    assert result.exit_code == 0
    connection = service.registry.connect()
    try:
        assert connection.execute("SELECT COUNT(*) FROM predictions").fetchone()[0] == 0
        row = connection.execute(
            """
            SELECT runs.*, run_bindings.registration_status,
                   external_run_imports.source_system,
                   external_run_imports.source_run_id,
                   external_run_imports.evidence_hash
            FROM runs
            JOIN run_bindings USING (run_id)
            JOIN external_run_imports USING (run_id)
            """
        ).fetchone()
        assert row["run_id"] == result.run_id
        assert row["prediction_id"] is None
        assert row["state"] == "completed"
        assert row["registration_status"] == "unregistered_external"
        assert row["source_system"] == "msm"
        assert row["source_run_id"] == "msm-direct-20260717-01"
        assert row["evidence_hash"] == result.evidence_hash
    finally:
        connection.close()


def test_exact_external_evidence_retry_is_a_noop(vault: Path) -> None:
    service = ExternalRunIngestor(vault, clock=lambda: INGESTED_AT)
    request = ExternalRunIngestRequest.model_validate({"evidence": _evidence(vault)})

    first = service.ingest(request)
    second = service.ingest(request)

    assert second.run_id == first.run_id
    assert second.evidence_hash == first.evidence_hash
    assert second.envelope_hash == first.envelope_hash
    assert first.created is True
    assert second.created is False
    connection = service.registry.connect()
    try:
        assert connection.execute("SELECT COUNT(*) FROM runs").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM run_bindings").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM external_run_imports").fetchone()[0] == 1
    finally:
        connection.close()


def test_failed_direct_run_is_preserved_as_terminal_failed(vault: Path) -> None:
    evidence = _evidence(vault, source_run_id="msm-direct-failed-01")
    evidence["exit_code"] = 17
    evidence["failure_note"] = "process exited with code 17"
    service = ExternalRunIngestor(vault, clock=lambda: INGESTED_AT)

    result = service.ingest({"evidence": evidence})

    assert result.state is RunState.FAILED
    assert result.exit_code == 17
    assert result.failure_note == "process exited with code 17"
    row = service.registry.get_run(result.run_id)
    assert row is not None
    assert row["state"] == "failed"


def test_changed_evidence_for_same_source_run_fails_closed(vault: Path) -> None:
    service = ExternalRunIngestor(vault, clock=lambda: INGESTED_AT)
    original = _evidence(vault)
    service.ingest({"evidence": original})
    changed = copy.deepcopy(original)
    changed["artifacts"][1]["sha256"] = f"sha256:{'e' * 64}"

    with pytest.raises(IdempotencyConflictError, match="different evidence"):
        service.ingest({"evidence": changed})

    connection = service.registry.connect()
    try:
        assert connection.execute("SELECT COUNT(*) FROM runs").fetchone()[0] == 1
        stored = connection.execute("SELECT evidence_hash FROM external_run_imports").fetchone()[0]
        normalized = ExternalRunEvidence.model_validate(original).model_dump(mode="json")
        assert stored == sha256_json(normalized)
    finally:
        connection.close()


@pytest.mark.parametrize("mutation", ["dataset_hash", "artifact_order", "relative_escape"])
def test_invalid_external_evidence_creates_no_run(vault: Path, mutation: str) -> None:
    evidence = _evidence(vault)
    if mutation == "dataset_hash":
        evidence["snapshot"]["dataset_version"] = f"sha256:{'0' * 64}"
    elif mutation == "artifact_order":
        evidence["artifacts"].reverse()
    else:
        evidence["artifacts"][0]["relative_path"] = "../audit_results.json"
    service = ExternalRunIngestor(vault, clock=lambda: INGESTED_AT)

    with pytest.raises(ValidationError):
        service.ingest({"evidence": evidence})

    connection = service.registry.connect()
    try:
        assert connection.execute("SELECT COUNT(*) FROM runs").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM run_bindings").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM external_run_imports").fetchone()[0] == 0
    finally:
        connection.close()


def test_ingestion_clock_cannot_predate_external_run(vault: Path) -> None:
    service = ExternalRunIngestor(vault, clock=lambda: STARTED_AT)

    with pytest.raises(IntegrityError, match="cannot precede run completion"):
        service.ingest({"evidence": _evidence(vault)})

    connection = service.registry.connect()
    try:
        assert connection.execute("SELECT COUNT(*) FROM runs").fetchone()[0] == 0
    finally:
        connection.close()


def test_external_status_is_unrepresentable_and_registry_rows_are_write_once(
    vault: Path,
) -> None:
    service = ExternalRunIngestor(vault, clock=lambda: INGESTED_AT)
    result = service.ingest({"evidence": _evidence(vault)})

    assert "registration_status" not in ExternalRunEvidence.model_fields
    assert "registration_status" not in ExternalRunIngestRequest.model_fields
    assert (
        "registration_status"
        not in inspect.signature(service.registry.create_external_run_import).parameters
    )
    assert not hasattr(service, "promote")
    connection = service.registry.connect()
    try:
        with pytest.raises(sqlite3.IntegrityError, match="write-once"):
            connection.execute(
                "UPDATE run_bindings SET registration_status = 'preregistered' WHERE run_id = ?",
                (result.run_id,),
            )
        with pytest.raises(sqlite3.IntegrityError, match="write-once"):
            connection.execute(
                "UPDATE external_run_imports SET evidence_hash = ? WHERE run_id = ?",
                (f"sha256:{'f' * 64}", result.run_id),
            )
        with pytest.raises(sqlite3.IntegrityError, match="permanent"):
            connection.execute(
                "DELETE FROM external_run_imports WHERE run_id = ?", (result.run_id,)
            )
    finally:
        connection.close()


def test_corrupt_stored_external_envelope_is_rejected_on_retry(vault: Path) -> None:
    service = ExternalRunIngestor(vault, clock=lambda: INGESTED_AT)
    request = {"evidence": _evidence(vault)}
    result = service.ingest(request)
    connection = service.registry.connect()
    try:
        connection.execute("DROP TRIGGER run_bindings_write_once_update")
        connection.execute(
            "UPDATE run_bindings SET envelope_json = '{}' WHERE run_id = ?", (result.run_id,)
        )
    finally:
        connection.close()

    with pytest.raises(IntegrityError, match="immutable hash"):
        service.ingest(request)


def test_cli_ingests_external_evidence_without_loading_msm(
    vault: Path,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    evidence_path = tmp_path / "external-run.json"
    evidence_path.write_text(json.dumps(_evidence(vault)), encoding="utf-8")

    exit_code = cli.run_cli(
        [
            "ingest-external",
            "--vault-root",
            str(vault),
            "--evidence",
            str(evidence_path),
        ]
    )

    assert exit_code == 0
    output = json.loads(capsys.readouterr().out)
    assert output["registration_status"] == "unregistered_external"
    assert output["state"] == "completed"
    assert output["created"] is True
