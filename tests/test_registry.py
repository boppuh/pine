from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from ledger.integrity import PredictionStatus
from ledger.registry import _MIGRATION_1, LedgerRegistry
from ledger.writer import LedgerWriter


def test_registry_uses_wal_and_has_migration_stamp(vault: Path) -> None:
    registry = LedgerRegistry(vault / ".ledger" / "registry.db")
    connection = registry.connect()
    try:
        assert connection.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 2
        versions = connection.execute(
            "SELECT version FROM schema_migrations ORDER BY version"
        ).fetchall()
        assert [row[0] for row in versions] == [1, 2]
    finally:
        connection.close()


def test_existing_version_one_registry_migrates_forward(vault: Path) -> None:
    db_path = vault / ".ledger" / "registry.db"
    connection = sqlite3.connect(db_path)
    try:
        connection.executescript(_MIGRATION_1)
        connection.execute(
            "INSERT INTO schema_migrations (version, applied_at) VALUES (1, '2026-01-01')"
        )
        connection.execute("PRAGMA user_version = 1")
        connection.commit()
    finally:
        connection.close()

    registry = LedgerRegistry(db_path)
    upgraded = registry.connect()
    try:
        assert upgraded.execute("PRAGMA user_version").fetchone()[0] == 2
        assert (
            upgraded.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'capture_requests'"
            ).fetchone()
            is not None
        )
    finally:
        upgraded.close()


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


@pytest.mark.parametrize(
    ("window_start", "window_end", "expected"),
    [
        ("2023-12-31", "2024-01-01", True),
        ("2024-12-31", "2025-01-01", True),
        ("2025-01-01", "2025-12-31", True),
        ("2023-01-01", "2023-12-30", False),
        ("2025-01-02", "2025-12-31", False),
    ],
)
def test_touched_window_overlap_is_inclusive(
    vault: Path,
    window_start: str,
    window_end: str,
    expected: bool,
) -> None:
    registry = LedgerRegistry(vault / ".ledger" / "registry.db")
    registry.mark_window_touched("fam_01", "2024-01-01", "2025-01-01")

    assert registry.window_overlaps_touched("fam_01", window_start, window_end) is expected
    assert registry.window_overlaps_touched("different-family", window_start, window_end) is False
