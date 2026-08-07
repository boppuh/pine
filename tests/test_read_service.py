from __future__ import annotations

import copy
import hashlib
import json
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
import yaml
from fastapi.testclient import TestClient
from pydantic import JsonValue

from ledger.api import create_app
from ledger.capture import CaptureService
from ledger.errors import IntegrityError, ReadCursorError
from ledger.extraction import ExtractionService
from ledger.integrity import PredictionDraft, PredictionStatus, RegistrationStatus
from ledger.json_utils import sha256_json
from ledger.read_models import IntegrityReason, IntegrityState, ResultState
from ledger.read_service import LedgerReadService, PredictionListFilters
from ledger.record_integrity import ImmutableEvidenceVerifier
from ledger.results import MSMResultIngestor
from ledger.run import PreregisteredRunRequest, RunService
from ledger.snapshot import PendingPrediction
from ledger.writer import LedgerWriter

TOKEN = "read-api-token-" + "a" * 48
AUTHORIZATION = {"Authorization": f"Bearer {TOKEN}"}
GIT_COMMIT = "a" * 40
DECISION_AT = datetime(2026, 7, 17, 15, 0, tzinfo=UTC)


def _snapshot(strategy_id: str) -> dict[str, Any]:
    manifest = {
        "database": "msm",
        "server_version": "26.1.3.52",
        "window": {"start": "2020-01-01", "end": "2025-12-31"},
        "tables": [
            {
                "table": "ohlcv_5m",
                "part_count": 1,
                "row_count": 100,
                "min_date": "2020-01-01",
                "max_date": "2025-12-31",
                "parts": [
                    {
                        "name": "all_1_1_0",
                        "partition_id": "all",
                        "rows": 100,
                        "bytes_on_disk": 4096,
                        "min_date": "2020-01-01",
                        "max_date": "2025-12-31",
                        "min_block_number": 1,
                        "max_block_number": 1,
                        "level": 0,
                        "data_version": 1,
                        "modification_epoch": int(DECISION_AT.timestamp()),
                    }
                ],
            }
        ],
    }
    return {
        "snapshot_format_version": 1,
        "strategy_id": strategy_id,
        "strategy_spec_hash": f"sha256:{'b' * 64}",
        "git_commit": GIT_COMMIT,
        "parameter_set": {"lookback": 40, "threshold": 0.5},
        "parameter_count": 2,
        "data_as_of_version": DECISION_AT.isoformat(),
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
        "captured_at": DECISION_AT.isoformat(),
    }


def _draft(
    valid_forecast: dict[str, object],
    prediction_id: str,
    *,
    strategy_id: str = "msm-strat-orb-001",
) -> PredictionDraft:
    forecast = copy.deepcopy(valid_forecast)
    forecast["strategy_id"] = strategy_id
    return PredictionDraft.model_validate(
        {
            "prediction_id": prediction_id,
            "run_id": prediction_id.replace("pred_", "run_", 1),
            "registration_status": "preregistered",
            "forecast": forecast,
            "decision": "Run this frozen specification against the untouched OOS window.",
            "snapshot": _snapshot(strategy_id),
            "lineage": {"family_id": f"fam_{strategy_id}"},
            "body": f"# {strategy_id}\n\nMutable research context.",
            "created_at": DECISION_AT.isoformat(),
        }
    )


def _registry_state(registry) -> dict[str, list[tuple[Any, ...]] | int]:
    connection = registry.connect()
    try:
        tables = [
            row[0]
            for row in connection.execute(
                """
                SELECT name FROM sqlite_master
                WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
                ORDER BY name
                """
            )
        ]
        state: dict[str, list[tuple[Any, ...]] | int] = {
            "user_version": connection.execute("PRAGMA user_version").fetchone()[0]
        }
        for table in tables:
            state[table] = [
                tuple(row) for row in connection.execute(f'SELECT * FROM "{table}" ORDER BY rowid')
            ]
        return state
    finally:
        connection.close()


def _artifact_state(vault: Path) -> dict[str, str]:
    paths = list((vault / "predictions").glob("*.md"))
    paths.extend((vault / ".ledger" / "snapshots").glob("*.json"))
    return {
        path.relative_to(vault).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(paths)
    }


def _mutate_frontmatter(path: Path, field: str, value: object) -> None:
    content = path.read_text(encoding="utf-8").replace("\r\n", "\n")
    parts = content.split("---\n", maxsplit=2)
    frontmatter = yaml.safe_load(parts[1])
    frontmatter[field] = value
    rendered = yaml.safe_dump(frontmatter, sort_keys=False)
    path.write_text(f"---\n{rendered}---\n{parts[2]}", encoding="utf-8")


def test_verified_reads_return_typed_projections_without_mutation(
    vault: Path,
    valid_forecast: dict[str, object],
) -> None:
    writer = LedgerWriter(vault)
    draft = _draft(valid_forecast, "pred_read_01")
    written = writer.write(draft)
    writer.registry.update_resolution(
        draft.prediction_id,
        status=PredictionStatus.RESOLVED,
        outcome={"sharpe": 1.1},
        grade={"forecast_accuracy": 0.8},
        resolution_metadata={"source": "read-test"},
    )
    service = LedgerReadService(
        vault,
        cursor_secret=TOKEN,
        registry=writer.registry,
        schema_registry=writer.schema_registry,
    )
    before_registry = _registry_state(writer.registry)
    before_artifacts = _artifact_state(vault)

    page = service.list_predictions()
    detail = service.get_prediction(draft.prediction_id)
    status = service.get_status()

    assert len(page.items) == 1
    summary = page.items[0]
    assert summary.prediction_id == draft.prediction_id
    assert summary.strategy_id == draft.forecast.strategy_id
    assert summary.integrity_state is IntegrityState.VERIFIED
    assert summary.result_state is ResultState.ABSENT
    assert summary.run_state.value == "registered"
    assert detail.immutable_hash == written.immutable_hash
    assert detail.forecast == draft.forecast
    assert detail.snapshot.dataset_version == draft.snapshot["dataset_version"]
    assert detail.run.binding is None
    assert detail.body == draft.body
    assert detail.status is PredictionStatus.RESOLVED
    assert detail.outcome == {"sharpe": 1.1}
    assert detail.grade == {"forecast_accuracy": 0.8}
    assert status.registry_version == 7
    assert status.committed_predictions == 1
    assert status.quarantined_predictions == 0
    assert _registry_state(writer.registry) == before_registry
    assert _artifact_state(vault) == before_artifacts


@pytest.mark.parametrize("damage", ["record", "snapshot", "schema"])
def test_tampered_evidence_fails_closed_without_quarantine_side_effect(
    vault: Path,
    valid_forecast: dict[str, object],
    damage: str,
) -> None:
    writer = LedgerWriter(vault)
    draft = _draft(valid_forecast, f"pred_tamper_{damage}")
    written = writer.write(draft)
    if damage == "record":
        _mutate_frontmatter(written.record_path, "decision", "Changed after commitment.")
    elif damage == "snapshot":
        snapshot = json.loads(written.snapshot_path.read_text(encoding="utf-8"))
        snapshot["random_seed"] = 99
        written.snapshot_path.write_text(json.dumps(snapshot), encoding="utf-8")
    else:
        schema_path = vault / ".ledger" / "schemas" / "finance" / "strategy-edge.1.json"
        schema = json.loads(schema_path.read_text(encoding="utf-8"))
        schema["title"] = "Changed schema identity"
        schema_path.write_text(json.dumps(schema), encoding="utf-8")
    service = LedgerReadService(vault, cursor_secret=TOKEN, registry=writer.registry)
    before = _registry_state(writer.registry)

    page = service.list_predictions()

    assert len(page.items) == 1
    assert page.items[0].integrity_state is IntegrityState.FAILED
    assert page.items[0].strategy_id is None
    assert page.items[0].out_of_sample_window is None
    with pytest.raises(IntegrityError, match="failed verification"):
        service.get_prediction(draft.prediction_id)
    row = writer.registry.get_prediction(draft.prediction_id)
    assert row is not None
    assert row["status"] == "open"
    assert _registry_state(writer.registry) == before


def test_pure_artifact_verifier_reports_damage_without_writing_registry(
    vault: Path,
    valid_forecast: dict[str, object],
) -> None:
    writer = LedgerWriter(vault)
    draft = _draft(valid_forecast, "pred_pure_verifier")
    written = writer.write(draft)
    _mutate_frontmatter(written.record_path, "registration_status", "exploratory")
    row = writer.registry.get_prediction(draft.prediction_id)
    assert row is not None
    before = _registry_state(writer.registry)

    result = ImmutableEvidenceVerifier(vault).verify(written.record_path, row)

    assert result.verified is False
    assert set(result.violations) == {
        "immutable_payload",
        "registration_status",
    }
    assert _registry_state(writer.registry) == before


@pytest.mark.parametrize("artifact", ["record", "snapshot"])
def test_missing_committed_artifact_fails_closed_without_registry_mutation(
    vault: Path,
    valid_forecast: dict[str, object],
    artifact: str,
) -> None:
    writer = LedgerWriter(vault)
    draft = _draft(valid_forecast, f"pred_missing_{artifact}")
    written = writer.write(draft)
    missing = written.record_path if artifact == "record" else written.snapshot_path
    missing.unlink()
    before = _registry_state(writer.registry)
    service = LedgerReadService(vault, cursor_secret=TOKEN, registry=writer.registry)

    page = service.list_predictions()

    assert page.items[0].integrity_state is IntegrityState.FAILED
    with pytest.raises(IntegrityError, match="failed verification"):
        service.get_prediction(draft.prediction_id)
    assert _registry_state(writer.registry) == before


def test_cursor_pagination_and_filters_are_stable_and_bound(
    vault: Path,
    valid_forecast: dict[str, object],
) -> None:
    writer = LedgerWriter(vault)
    drafts = [
        _draft(valid_forecast, "pred_page_01", strategy_id="strategy-a"),
        _draft(valid_forecast, "pred_page_02", strategy_id="strategy-b"),
        _draft(valid_forecast, "pred_page_03", strategy_id="strategy-a"),
        _draft(valid_forecast, "pred_page_04", strategy_id="strategy-c"),
    ]
    for draft in drafts:
        writer.write(draft)
    service = LedgerReadService(vault, cursor_secret=TOKEN, registry=writer.registry)

    seen: list[str] = []
    cursor = None
    while True:
        page = service.list_predictions(limit=1, cursor=cursor)
        seen.extend(item.prediction_id for item in page.items)
        if page.next_cursor is None:
            break
        cursor = page.next_cursor

    assert len(seen) == 4
    assert set(seen) == {draft.prediction_id for draft in drafts}
    strategy_page = service.list_predictions(
        limit=10,
        filters=PredictionListFilters(strategy_id="strategy-a"),
    )
    assert {item.strategy_id for item in strategy_page.items} == {"strategy-a"}
    fully_filtered = service.list_predictions(
        limit=10,
        filters=PredictionListFilters(
            registration_status=RegistrationStatus.PREREGISTERED,
            status=PredictionStatus.OPEN,
            result_state=ResultState.ABSENT,
        ),
    )
    assert len(fully_filtered.items) == 4
    first = service.list_predictions(limit=1)
    assert first.next_cursor is not None
    with pytest.raises(ReadCursorError, match="does not match"):
        service.list_predictions(
            limit=1,
            cursor=first.next_cursor,
            filters=PredictionListFilters(strategy_id="strategy-a"),
        )
    replacement = "A" if first.next_cursor[-1] != "A" else "B"
    with pytest.raises(ReadCursorError, match="invalid"):
        service.list_predictions(limit=1, cursor=first.next_cursor[:-1] + replacement)


def test_quarantined_predictions_require_explicit_filter_and_remain_untrusted(
    vault: Path,
    valid_forecast: dict[str, object],
) -> None:
    writer = LedgerWriter(vault)
    draft = _draft(valid_forecast, "pred_quarantined")
    writer.write(draft)
    writer.registry.quarantine_prediction(
        draft.prediction_id,
        violations={"snapshot": "snapshot mutation observed"},
    )
    service = LedgerReadService(vault, cursor_secret=TOKEN, registry=writer.registry)

    default_page = service.list_predictions()
    quarantined_page = service.list_predictions(
        filters=PredictionListFilters(status=PredictionStatus.QUARANTINED)
    )

    assert default_page.items == ()
    assert len(quarantined_page.items) == 1
    item = quarantined_page.items[0]
    assert item.integrity_state is IntegrityState.FAILED
    assert item.integrity_reason is IntegrityReason.QUARANTINED
    assert item.strategy_id is None
    with pytest.raises(IntegrityError, match="quarantined"):
        service.get_prediction(draft.prediction_id)


def _clean_git(_working_directory: Path) -> tuple[str, bool]:
    return GIT_COMMIT, False


def _successful_executor(
    _command: Sequence[str],
    _working_directory: Path,
    _environment: Mapping[str, str],
) -> int:
    return 0


def _result_evidence(run, snapshot: Mapping[str, Any]) -> dict[str, Any]:
    metrics = {
        "sharpe": 1.0,
        "win_rate": 0.52,
        "max_drawdown": 0.12,
        "expectancy": 0.001,
        "total_return": 0.1,
        "trade_count": 50,
    }
    return {
        "result_format_version": 1,
        "source_system": "msm",
        "metric_units": "finance/strategy-edge:decimal-v1",
        "run_id": run.run_id,
        "prediction_id": run.prediction_id,
        "registration_status": run.registration_status.value,
        "strategy_id": run.strategy_id,
        "envelope_hash": run.envelope_hash,
        "dataset_version": run.dataset_version,
        "git_commit": snapshot["git_commit"],
        "metric_definition_version": snapshot["metric_definition_version"],
        "source_timestamp": (DECISION_AT + timedelta(seconds=3)).isoformat(),
        "in_sample_window": snapshot["in_sample_window"],
        "out_of_sample_window": snapshot["out_of_sample_window"],
        "in_sample_metrics": metrics,
        "out_of_sample_metrics": metrics,
        "regime_breakdown": [],
        "artifacts": [
            {
                "relative_path": "summary.csv",
                "sha256": f"sha256:{'c' * 64}",
                "size_bytes": 128,
            }
        ],
        "metadata": {},
    }


def _write_completed_result(
    vault: Path,
    valid_forecast: dict[str, object],
) -> tuple[LedgerWriter, PredictionDraft]:
    writer = LedgerWriter(vault)
    draft = _draft(valid_forecast, "pred_with_result")
    writer.write(draft)
    clock_values = iter(
        (
            DECISION_AT,
            DECISION_AT + timedelta(seconds=1),
            DECISION_AT + timedelta(seconds=2),
        )
    )
    run_service = RunService(
        vault,
        writer=writer,
        clock=lambda: next(clock_values),
        executor=_successful_executor,
        git_state_reader=_clean_git,
    )
    run = run_service.run_preregistered(
        PreregisteredRunRequest(
            idempotency_key="read-result-run",
            prediction_id=draft.prediction_id,
            command=("msm", "backtest"),
            working_directory=vault,
        )
    )
    MSMResultIngestor(
        vault,
        registry=writer.registry,
        clock=lambda: DECISION_AT + timedelta(seconds=4),
    ).ingest({"evidence": _result_evidence(run, draft.snapshot)})
    return writer, draft


def test_detail_verifies_bound_run_and_result_evidence(
    vault: Path,
    valid_forecast: dict[str, object],
) -> None:
    writer, draft = _write_completed_result(vault, valid_forecast)
    service = LedgerReadService(vault, cursor_secret=TOKEN, registry=writer.registry)

    page = service.list_predictions(filters=PredictionListFilters(result_state=ResultState.PRESENT))
    detail = service.get_prediction(draft.prediction_id)

    assert len(page.items) == 1
    assert page.items[0].result_state is ResultState.PRESENT
    assert page.items[0].run_state.value == "completed"
    assert detail.run.binding is not None
    assert detail.result is not None
    assert detail.result.out_of_sample_metrics.sharpe == 1.0
    assert "metadata" not in detail.result.model_dump(mode="json")


def test_tampered_stored_result_fails_closed(
    vault: Path,
    valid_forecast: dict[str, object],
) -> None:
    writer, draft = _write_completed_result(vault, valid_forecast)
    connection = writer.registry.connect()
    try:
        connection.execute("DROP TRIGGER run_results_write_once_update")
        row = writer.registry.get_run_result(draft.run_id, connection=connection)
        assert row is not None
        evidence = json.loads(row["evidence_json"])
        evidence["out_of_sample_metrics"]["sharpe"] = 9.0
        connection.execute(
            "UPDATE run_results SET evidence_json = ? WHERE run_id = ?",
            (json.dumps(evidence), draft.run_id),
        )
    finally:
        connection.close()
    before = _registry_state(writer.registry)
    service = LedgerReadService(vault, cursor_secret=TOKEN, registry=writer.registry)

    page = service.list_predictions()

    assert page.items[0].integrity_state is IntegrityState.FAILED
    assert page.items[0].integrity_reason is IntegrityReason.RESULT_UNVERIFIED
    with pytest.raises(IntegrityError, match="result_unverified"):
        service.get_prediction(draft.prediction_id)
    assert _registry_state(writer.registry) == before


class _FakeExtractor:
    async def extract(
        self,
        text: str,
        *,
        schema_id: str,
        schema: Mapping[str, Any],
    ) -> Mapping[str, Any] | None:
        del text, schema_id, schema
        return None


class _FakeSnapshotProvider:
    def capture_snapshot(
        self,
        prediction: PendingPrediction,
        at: datetime,
    ) -> Mapping[str, JsonValue]:
        del at
        return _snapshot(prediction.forecast.strategy_id)


def test_authenticated_read_api_exposes_list_detail_status_and_stable_errors(
    vault: Path,
    valid_forecast: dict[str, object],
) -> None:
    capture = CaptureService(vault, _FakeSnapshotProvider())
    extraction = ExtractionService(
        vault,
        _FakeExtractor(),
        schema_registry=capture.schema_registry,
        registry=capture.registry,
    )
    draft = _draft(valid_forecast, "pred_api_read")
    capture.writer.write(draft)
    app = create_app(
        extraction_service=extraction,
        capture_service=capture,
        token=TOKEN,
    )

    with TestClient(app) as client:
        unauthorized = client.get("/v1/predictions")
        unauthorized_detail = client.get(f"/v1/predictions/{draft.prediction_id}")
        unauthorized_status = client.get("/v1/status")
        listed = client.get("/v1/predictions?limit=1", headers=AUTHORIZATION)
        detail = client.get(f"/v1/predictions/{draft.prediction_id}", headers=AUTHORIZATION)
        status = client.get("/v1/status", headers=AUTHORIZATION)
        missing = client.get("/v1/predictions/pred_missing", headers=AUTHORIZATION)
        invalid_cursor = client.get(
            "/v1/predictions?cursor=not-a-valid-cursor",
            headers=AUTHORIZATION,
        )

    assert unauthorized.status_code == 401
    assert unauthorized_detail.status_code == 401
    assert unauthorized_status.status_code == 401
    assert listed.status_code == 200
    assert listed.json()["items"][0]["integrity_state"] == "verified"
    assert detail.status_code == 200
    assert detail.json()["prediction_id"] == draft.prediction_id
    assert "dataset_manifest" not in detail.json()["snapshot"]
    assert status.status_code == 200
    assert status.json()["api_version"] == "v1"
    assert missing.status_code == 404
    assert missing.json()["error"]["code"] == "prediction_not_found"
    assert invalid_cursor.status_code == 422
    assert invalid_cursor.json()["error"]["code"] == "invalid_cursor"


def test_invalid_registry_snapshot_path_fails_closed(
    vault: Path,
    valid_forecast: dict[str, object],
) -> None:
    writer = LedgerWriter(vault)
    draft = _draft(valid_forecast, "pred_unsafe_path")
    writer.write(draft)
    connection = writer.registry.connect()
    try:
        connection.execute("DROP TRIGGER predictions_write_once")
        connection.execute(
            "UPDATE predictions SET snapshot_ref = '../outside.json' WHERE prediction_id = ?",
            (draft.prediction_id,),
        )
    finally:
        connection.close()
    service = LedgerReadService(vault, cursor_secret=TOKEN, registry=writer.registry)

    page = service.list_predictions()

    assert len(page.items) == 1
    assert page.items[0].integrity_state is IntegrityState.FAILED
    with pytest.raises(IntegrityError, match="failed verification"):
        service.get_prediction(draft.prediction_id)
