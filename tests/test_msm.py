from __future__ import annotations

import copy
import json
from collections.abc import Mapping
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from ledger.capture import CaptureService
from ledger.errors import SnapshotCaptureError
from ledger.integrity import (
    FrozenDict,
    PreregisteredCaptureRequest,
    RegistrationStatus,
    StrategyEdgeForecast,
)
from ledger.json_utils import sha256_json
from ledger.msm import MSMSnapshotProvider, StrategySnapshot
from ledger.snapshot import PendingPrediction

DECISION_AT = datetime(2026, 7, 16, 13, 30, tzinfo=UTC)


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
        "git_commit": "b" * 40,
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


def _pending(valid_forecast: dict[str, object]) -> PendingPrediction:
    return PendingPrediction(
        prediction_id="pred_01",
        run_id="run_01",
        schema_id="finance/strategy-edge:1",
        registration_status=RegistrationStatus.PREREGISTERED,
        forecast=StrategyEdgeForecast.model_validate(valid_forecast),
        decision="Run the frozen strategy.",
        lineage=FrozenDict({"family_id": "fam_01"}),
        created_at=DECISION_AT,
    )


def test_provider_accepts_and_binds_complete_msm_snapshot(
    valid_forecast: dict[str, object],
) -> None:
    source = FakeMSMSource(_snapshot())
    provider = MSMSnapshotProvider(source)

    snapshot = provider.capture_snapshot(_pending(valid_forecast), DECISION_AT)

    assert snapshot["strategy_id"] == valid_forecast["strategy_id"]
    assert snapshot["dataset_version"].startswith("sha256:")
    assert len(source.calls) == 1
    assert source.calls[0] == {
        "strategy_id": "msm-strat-orb-001",
        "decision_at": DECISION_AT,
        "in_sample_window": {
            "start": date(2020, 1, 1),
            "end": date(2023, 12, 31),
        },
        "out_of_sample_window": {
            "start": date(2024, 1, 1),
            "end": date(2025, 12, 31),
        },
    }


@pytest.mark.parametrize(
    ("snapshot", "message"),
    [
        (_snapshot(strategy_id="different"), "strategy_id does not match"),
        (_snapshot(in_end="2023-12-30"), "in-sample window does not match"),
        (
            _snapshot(data_as_of=DECISION_AT - timedelta(seconds=1)),
            "data_as_of_version does not match",
        ),
    ],
)
def test_provider_rejects_snapshot_drift_from_pending_prediction(
    valid_forecast: dict[str, object],
    snapshot: dict[str, Any],
    message: str,
) -> None:
    provider = MSMSnapshotProvider(FakeMSMSource(snapshot))

    with pytest.raises(ValueError, match=message):
        provider.capture_snapshot(_pending(valid_forecast), DECISION_AT)


@pytest.mark.parametrize("mutation", ["missing", "extra", "digest", "part_count"])
def test_snapshot_contract_rejects_incomplete_or_inconsistent_state(mutation: str) -> None:
    snapshot = copy.deepcopy(_snapshot())
    if mutation == "missing":
        del snapshot["engine_version"]
    elif mutation == "extra":
        snapshot["backtest_result"] = {"sharpe": 99}
    elif mutation == "digest":
        snapshot["dataset_version"] = f"sha256:{'0' * 64}"
    else:
        snapshot["dataset_manifest"]["tables"][0]["part_count"] = 2

    with pytest.raises(ValidationError):
        StrategySnapshot.model_validate(snapshot)


def test_msm_provider_commits_validated_snapshot_through_atomic_capture(
    vault: Path,
    valid_forecast: dict[str, object],
) -> None:
    source = FakeMSMSource(_snapshot())
    service = CaptureService(vault, MSMSnapshotProvider(source), clock=lambda: DECISION_AT)
    request = PreregisteredCaptureRequest.model_validate(
        {
            "idempotency_key": "msm-confirm-01",
            "forecast": valid_forecast,
            "decision": "Run the frozen MSM strategy against untouched data.",
            "lineage": {"family_id": "fam_msm_01"},
        }
    )

    result = service.capture(request)

    persisted = json.loads(result.snapshot_path.read_text(encoding="utf-8"))
    assert persisted["strategy_id"] == "msm-strat-orb-001"
    assert persisted["dataset_manifest"]["tables"][0]["table"] == "ohlcv_1m"
    assert len(source.calls) == 1


def test_msm_source_failure_leaves_no_registry_rows_or_artifacts(
    vault: Path,
    valid_forecast: dict[str, object],
) -> None:
    source = FakeMSMSource(_snapshot(), error=ConnectionError("ClickHouse offline"))
    service = CaptureService(vault, MSMSnapshotProvider(source), clock=lambda: DECISION_AT)
    request = PreregisteredCaptureRequest.model_validate(
        {
            "idempotency_key": "msm-confirm-failure",
            "forecast": valid_forecast,
            "decision": "Run the frozen MSM strategy against untouched data.",
            "lineage": {"family_id": "fam_msm_02"},
        }
    )

    with pytest.raises(SnapshotCaptureError, match="snapshot capture failed"):
        service.capture(request)

    assert list((vault / "predictions").iterdir()) == []
    assert list((vault / ".ledger" / "snapshots").iterdir()) == []
    connection = service.registry.connect()
    try:
        assert connection.execute("SELECT COUNT(*) FROM predictions").fetchone()[0] == 0
    finally:
        connection.close()
