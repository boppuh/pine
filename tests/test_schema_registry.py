from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from ledger.schema_registry import SchemaRegistry


def test_valid_forecast_validates(valid_forecast: dict[str, object]) -> None:
    registry = SchemaRegistry(Path(__file__).parents[1] / ".ledger" / "schemas")

    valid, errors = registry.validate(valid_forecast, "finance/strategy-edge:1")

    assert valid is True
    assert errors == []


def test_invalid_forecast_is_rejected_with_reasons(valid_forecast: dict[str, object]) -> None:
    registry = SchemaRegistry(Path(__file__).parents[1] / ".ledger" / "schemas")
    invalid = dict(valid_forecast)
    invalid["expected_metrics"] = {
        "sharpe": "high",
        "win_rate": 1.2,
        "max_drawdown": 0.15,
    }
    invalid["out_of_sample_window"] = {"start": "not-a-date", "end": "2025-12-31"}

    valid, errors = registry.validate(invalid, "finance/strategy-edge:1")

    assert valid is False
    assert any("expectancy" in reason for reason in errors)
    assert any("win_rate" in reason and "greater than" in reason for reason in errors)
    assert any("sharpe" in reason and "not of type" in reason for reason in errors)
    assert any("out_of_sample_window.start" in reason and "date" in reason for reason in errors)


def test_schema_hash_is_stable_across_key_order_and_processes(tmp_path: Path) -> None:
    registry = SchemaRegistry(Path(__file__).parents[1] / ".ledger" / "schemas")
    schema = registry.load("finance/strategy-edge:1")
    reversed_schema = dict(reversed(list(schema.items())))

    expected = registry.hash(schema)
    assert expected == registry.hash(reversed_schema)
    assert expected.startswith("sha256:")
    assert len(expected) == len("sha256:") + 64

    schema_file = tmp_path / "schema.json"
    schema_file.write_text(json.dumps(schema), encoding="utf-8")
    command = (
        "import json,sys; "
        "from ledger.schema_registry import SchemaRegistry; "
        "print(SchemaRegistry.hash(json.load(open(sys.argv[1]))))"
    )
    actual = subprocess.check_output(
        [sys.executable, "-c", command, str(schema_file)],
        text=True,
    ).strip()
    assert actual == expected


def test_non_finite_numbers_are_rejected_with_reasons(
    valid_forecast: dict[str, object],
) -> None:
    registry = SchemaRegistry(Path(__file__).parents[1] / ".ledger" / "schemas")
    invalid = dict(valid_forecast)
    invalid["expected_metrics"] = {
        "sharpe": float("nan"),
        "win_rate": 0.5,
        "max_drawdown": float("inf"),
        "expectancy": 0.01,
    }

    valid, errors = registry.validate(invalid, "finance/strategy-edge:1")

    assert valid is False
    assert len(errors) == 1
    assert "finite JSON" in errors[0]
