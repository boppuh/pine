from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from ledger.integrity import PredictionStatus
from ledger.registry import _MIGRATION_1, _MIGRATION_2, LedgerRegistry
from ledger.writer import LedgerWriter


def test_registry_uses_wal_and_has_migration_stamp(vault: Path) -> None:
    registry = LedgerRegistry(vault / ".ledger" / "registry.db")
    connection = registry.connect()
    try:
        assert connection.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 3
        versions = connection.execute(
            "SELECT version FROM schema_migrations ORDER BY version"
        ).fetchall()
        assert [row[0] for row in versions] == [1, 2, 3]
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
        assert upgraded.execute("PRAGMA user_version").fetchone()[0] == 3
        assert (
            upgraded.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'capture_requests'"
            ).fetchone()
            is not None
        )
        assert (
            upgraded.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'run_bindings'"
            ).fetchone()
            is not None
        )
    finally:
        upgraded.close()


def test_version_two_migration_preserves_allocated_prediction_run(vault: Path) -> None:
    db_path = vault / ".ledger" / "registry.db"
    connection = sqlite3.connect(db_path)
    try:
        connection.executescript(_MIGRATION_1)
        connection.executescript(_MIGRATION_2)
        connection.execute(
            """
            INSERT INTO predictions (
                prediction_id, run_id, schema_id, schema_hash, registration_status,
                snapshot_ref, lineage_json, status, transaction_state, immutable_hash,
                created_at, committed_at
            ) VALUES (
                'pred_legacy', 'run_legacy', 'finance/strategy-edge:1', 'sha256:schema',
                'preregistered', '.ledger/snapshots/pred_legacy.json', '{}', 'open',
                'committed', 'sha256:immutable', '2026-01-01T00:00:00+00:00',
                '2026-01-01T00:00:01+00:00'
            )
            """
        )
        connection.execute(
            """
            INSERT INTO runs (run_id, prediction_id, started_at, state)
            VALUES ('run_legacy', 'pred_legacy', '2026-01-01T00:00:00+00:00', 'registered')
            """
        )
        connection.executemany(
            "INSERT INTO schema_migrations (version, applied_at) VALUES (?, '2026-01-01')",
            [(1,), (2,)],
        )
        connection.execute("PRAGMA user_version = 2")
        connection.commit()
    finally:
        connection.close()

    registry = LedgerRegistry(db_path)
    row = registry.get_run("run_legacy")

    assert row is not None
    assert row["prediction_id"] == "pred_legacy"
    assert row["state"] == "registered"
    assert row["envelope_json"] is None


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


def test_registry_rejects_skipped_run_lifecycle_state(vault: Path, draft) -> None:
    writer = LedgerWriter(vault)
    writer.write(draft)

    connection = writer.registry.connect()
    try:
        with pytest.raises(sqlite3.IntegrityError, match="invalid run lifecycle transition"):
            connection.execute(
                """
                UPDATE runs
                SET state = 'completed', completed_at = ?, exit_code = 0
                WHERE run_id = ?
                """,
                ("2026-01-01T00:00:00+00:00", draft.run_id),
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
