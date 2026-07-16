from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from ledger.integrity import PredictionDraft, RegistrationStatus


@pytest.fixture
def valid_forecast() -> dict[str, object]:
    return {
        "strategy_id": "msm-strat-orb-001",
        "expected_metrics": {
            "sharpe": 1.5,
            "win_rate": 0.55,
            "max_drawdown": 0.15,
            "expectancy": 0.012,
        },
        "in_sample_window": {"start": "2020-01-01", "end": "2023-12-31"},
        "out_of_sample_window": {"start": "2024-01-01", "end": "2025-12-31"},
        "invalidation": "OOS Sharpe < 0.5 or max drawdown > 0.25",
        "edge_source": "orb-breakout-momentum",
    }


@pytest.fixture
def vault(tmp_path: Path) -> Path:
    target = tmp_path / "vault"
    schema_dir = target / ".ledger" / "schemas" / "finance"
    schema_dir.mkdir(parents=True)
    source = Path(__file__).parents[1] / ".ledger" / "schemas" / "finance" / "strategy-edge.1.json"
    shutil.copy2(source, schema_dir / source.name)
    return target


@pytest.fixture
def draft(valid_forecast: dict[str, object]) -> PredictionDraft:
    return PredictionDraft(
        prediction_id="pred_01",
        run_id="run_01",
        registration_status=RegistrationStatus.PREREGISTERED,
        forecast=valid_forecast,
        decision="Run the frozen specification against the untouched OOS window.",
        snapshot={
            "strategy_spec_hash": "sha256:strategy",
            "parameter_count": 4,
            "random_seed": 42,
            "features": ["orb", {"regime": "low-vol"}],
        },
        lineage={"family_id": "fam_01", "parent_prediction_id": None},
        body="# ORB edge hypothesis\n\nThe breakout should persist out of sample.",
    )
