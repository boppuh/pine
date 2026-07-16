from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from ledger.integrity import PredictionStatus
from ledger.registry import LedgerRegistry
from ledger.writer import LedgerWriter


def test_registry_uses_wal_and_has_migration_stamp(vault: Path) -> None:
    registry = LedgerRegistry(vault / ".ledger" / "registry.db")
    connection = registry.connect()
    try:
        assert connection.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 1
        assert connection.execute("SELECT version FROM schema_migrations").fetchone()[0] == 1
    finally:
        connection.close()


def test_registry_rejects_updates_to_write_once_columns(vault: Path, draft) -> None:
    writer = LedgerWriter(vault)
    writer.write(draft)

    connection = writer.registry.connect()
    try:
        with pytest.raises(sqlite3.IntegrityError, match="immutable prediction field"):
            connection.execute(
                "UPDATE predictions SET registration_status = 'exploratory' "
                "WHERE prediction_id = ?",
                (draft.prediction_id,),
            )
    finally:
        connection.close()


def test_lifecycle_fields_mutate_only_through_registry(vault: Path, draft) -> None:
    writer = LedgerWriter(vault)
    writer.write(draft)

    writer.registry.update_resolution(
        draft.prediction_id,
        status=PredictionStatus.RESOLVED,
        outcome={"sharpe": 1.3},
        grade={"forecast_accuracy": 0.8},
        resolution_metadata={"source": "test"},
    )

    row = writer.registry.get_prediction(draft.prediction_id)
    assert row is not None
    assert row["status"] == "resolved"
    assert row["outcome_json"] == '{"sharpe":1.3}'
