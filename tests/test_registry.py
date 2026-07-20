from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import pytest

from ledger.errors import IntegrityError
from ledger.integrity import PredictionStatus
from ledger.registry import (
    _MIGRATION_1,
    _MIGRATION_2,
    _MIGRATION_3,
    _MIGRATION_4,
    _MIGRATION_5,
    _MIGRATION_6,
    LedgerRegistry,
)
from ledger.writer import LedgerWriter


def test_registry_uses_wal_and_has_migration_stamp(vault: Path) -> None:
    registry = LedgerRegistry(vault / ".ledger" / "registry.db")
    connection = registry.connect()
    try:
        assert connection.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 7
        versions = connection.execute(
            "SELECT version FROM schema_migrations ORDER BY version"
        ).fetchall()
        assert [row[0] for row in versions] == [1, 2, 3, 4, 5, 6, 7]
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
        assert upgraded.execute("PRAGMA user_version").fetchone()[0] == 7
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
        assert (
            upgraded.execute(
                """
                SELECT name FROM sqlite_master
                WHERE type = 'table' AND name = 'external_run_imports'
                """
            ).fetchone()
            is not None
        )
        assert (
            upgraded.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'run_results'"
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


def test_version_three_migration_preserves_existing_run_binding(vault: Path) -> None:
    db_path = vault / ".ledger" / "registry.db"
    connection = sqlite3.connect(db_path)
    try:
        connection.executescript(_MIGRATION_1)
        connection.executescript(_MIGRATION_2)
        connection.executescript(_MIGRATION_3)
        connection.execute(
            """
            INSERT INTO runs (run_id, prediction_id, started_at, state)
            VALUES ('run_explore_legacy', NULL, '2026-01-01T00:00:00+00:00', 'registered')
            """
        )
        connection.execute(
            """
            INSERT INTO run_bindings (
                run_id, idempotency_key, request_hash, registration_status,
                strategy_id, dataset_version, envelope_json, envelope_hash, bound_at
            ) VALUES (
                'run_explore_legacy', 'legacy-key', 'sha256:request', 'exploratory',
                'legacy-strategy', 'sha256:dataset', '{}', 'sha256:envelope',
                '2026-01-01T00:00:00+00:00'
            )
            """
        )
        connection.executemany(
            "INSERT INTO schema_migrations (version, applied_at) VALUES (?, '2026-01-01')",
            [(1,), (2,), (3,)],
        )
        connection.execute("PRAGMA user_version = 3")
        connection.commit()
    finally:
        connection.close()

    registry = LedgerRegistry(db_path)
    row = registry.get_run("run_explore_legacy")

    assert row is not None
    assert row["registration_status"] == "exploratory"
    assert row["idempotency_key"] == "legacy-key"


def _create_version_five_started_preregistered_registry(
    db_path: Path,
    *,
    lineage: dict[str, object],
) -> str:
    connection = sqlite3.connect(db_path)
    envelope = json.dumps(
        {
            "snapshot": {
                "out_of_sample_window": {
                    "start": "2026-04-22",
                    "end": "2026-04-22",
                }
            }
        }
    )
    execution_started_at = "2026-07-17T19:30:11+00:00"
    try:
        for migration in (
            _MIGRATION_1,
            _MIGRATION_2,
            _MIGRATION_3,
            _MIGRATION_4,
            _MIGRATION_5,
        ):
            connection.executescript(migration)
        connection.execute(
            """
            INSERT INTO predictions (
                prediction_id, run_id, schema_id, schema_hash, registration_status,
                snapshot_ref, lineage_json, status, transaction_state, immutable_hash,
                created_at, committed_at
            ) VALUES (
                'pred_started', 'run_started', 'finance/strategy-edge:1',
                'sha256:schema', 'preregistered',
                '.ledger/snapshots/pred_started.json', ?, 'open', 'committed',
                'sha256:immutable', '2026-07-17T19:00:00+00:00',
                '2026-07-17T19:00:01+00:00'
            )
            """,
            (json.dumps(lineage),),
        )
        connection.execute(
            """
            INSERT INTO runs (run_id, prediction_id, started_at, state)
            VALUES ('run_started', 'pred_started', '2026-07-17T19:00:00+00:00',
                    'registered')
            """
        )
        connection.execute(
            """
            INSERT INTO run_bindings (
                run_id, idempotency_key, request_hash, registration_status,
                strategy_id, dataset_version, envelope_json, envelope_hash, bound_at
            ) VALUES (
                'run_started', 'started-key', 'sha256:request', 'preregistered',
                'vwap_mr_v3.1', 'sha256:dataset', ?, 'sha256:envelope',
                '2026-07-17T19:00:00+00:00'
            )
            """,
            (envelope,),
        )
        connection.execute(
            """
            UPDATE runs
            SET state = 'running', execution_started_at = ?
            WHERE run_id = 'run_started'
            """,
            (execution_started_at,),
        )
        connection.executemany(
            "INSERT INTO schema_migrations (version, applied_at) VALUES (?, '2026-01-01')",
            [(1,), (2,), (3,), (4,), (5,)],
        )
        connection.execute("PRAGMA user_version = 5")
        connection.commit()
    finally:
        connection.close()
    return execution_started_at


def test_version_six_backfills_started_preregistered_run_window(vault: Path) -> None:
    db_path = vault / ".ledger" / "registry.db"
    execution_started_at = _create_version_five_started_preregistered_registry(
        db_path,
        lineage={"family_id": "fam_started"},
    )

    registry = LedgerRegistry(db_path)
    upgraded = registry.connect()
    try:
        row = upgraded.execute("SELECT * FROM touched_windows").fetchone()
        assert row is not None
        assert dict(row) == {
            "family_id": "fam_started",
            "window_start": "2026-04-22",
            "window_end": "2026-04-22",
            "touched_at": execution_started_at,
        }
        assert upgraded.execute("PRAGMA user_version").fetchone()[0] == 7
    finally:
        upgraded.close()


def test_version_six_rejects_started_preregistered_run_without_family(
    vault: Path,
) -> None:
    db_path = vault / ".ledger" / "registry.db"
    _create_version_five_started_preregistered_registry(db_path, lineage={})

    with pytest.raises(sqlite3.IntegrityError, match="migration_6_preregistered_guard"):
        LedgerRegistry(db_path)

    connection = sqlite3.connect(db_path)
    try:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 5
        assert connection.execute("SELECT COUNT(*) FROM touched_windows").fetchone()[0] == 0
        versions = connection.execute(
            "SELECT version FROM schema_migrations ORDER BY version"
        ).fetchall()
        assert [row[0] for row in versions] == [1, 2, 3, 4, 5]
    finally:
        connection.close()


def test_version_seven_adds_permanent_run_result_evidence(vault: Path) -> None:
    db_path = vault / ".ledger" / "registry.db"
    connection = sqlite3.connect(db_path)
    try:
        for migration in (
            _MIGRATION_1,
            _MIGRATION_2,
            _MIGRATION_3,
            _MIGRATION_4,
            _MIGRATION_5,
            _MIGRATION_6,
        ):
            connection.executescript(migration)
        connection.executemany(
            "INSERT INTO schema_migrations (version, applied_at) VALUES (?, '2026-01-01')",
            [(1,), (2,), (3,), (4,), (5,), (6,)],
        )
        connection.execute("PRAGMA user_version = 6")
        connection.commit()
    finally:
        connection.close()

    registry = LedgerRegistry(db_path)
    upgraded = registry.connect()
    try:
        assert upgraded.execute("PRAGMA user_version").fetchone()[0] == 7
        assert (
            upgraded.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'run_results'"
            ).fetchone()
            is not None
        )
        triggers = upgraded.execute(
            "SELECT name FROM sqlite_master WHERE type = 'trigger' AND name LIKE 'run_results_%'"
        ).fetchall()
        assert {row[0] for row in triggers} == {
            "run_results_permanent_delete",
            "run_results_require_successful_bound_run",
            "run_results_write_once_update",
        }
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


def test_quarantine_is_atomic_deduplicated_and_terminal(vault: Path, draft) -> None:
    writer = LedgerWriter(vault)
    writer.write(draft)
    registry = writer.registry
    registry.update_resolution(
        draft.prediction_id,
        status=PredictionStatus.RESOLVED,
        outcome={"sharpe": 1.3},
        grade={"forecast_accuracy": 0.8},
        resolution_metadata={"source": "test"},
    )
    with pytest.raises(IntegrityError, match="use quarantine_prediction"):
        registry.update_resolution(
            draft.prediction_id,
            status=PredictionStatus.QUARANTINED,
        )
    connection = registry.connect()
    try:
        with pytest.raises(sqlite3.IntegrityError, match="requires integrity violation"):
            connection.execute(
                "UPDATE predictions SET status = 'quarantined' WHERE prediction_id = ?",
                (draft.prediction_id,),
            )
    finally:
        connection.close()

    first_changed = registry.quarantine_prediction(
        draft.prediction_id,
        violations={"registration_status": "frontmatter differs from registry"},
    )
    second_changed = registry.quarantine_prediction(
        draft.prediction_id,
        violations={"registration_status": "frontmatter differs from registry"},
    )

    assert first_changed is True
    assert second_changed is False
    row = registry.get_prediction(draft.prediction_id)
    assert row is not None
    assert row["status"] == PredictionStatus.QUARANTINED.value
    assert row["outcome_json"] == '{"sharpe":1.3}'
    assert row["grade_json"] == '{"forecast_accuracy":0.8}'
    assert row["resolution_metadata_json"] == '{"source":"test"}'

    connection = registry.connect()
    try:
        violations = connection.execute(
            "SELECT field, note FROM integrity_violations WHERE prediction_id = ?",
            (draft.prediction_id,),
        ).fetchall()
        assert [tuple(item) for item in violations] == [
            ("registration_status", "frontmatter differs from registry")
        ]
        with pytest.raises(sqlite3.IntegrityError, match="status is terminal"):
            connection.execute(
                "UPDATE predictions SET status = 'resolved' WHERE prediction_id = ?",
                (draft.prediction_id,),
            )
        with pytest.raises(sqlite3.IntegrityError, match="evidence is write-once"):
            connection.execute(
                "UPDATE integrity_violations SET note = 'rewritten' WHERE prediction_id = ?",
                (draft.prediction_id,),
            )
        with pytest.raises(sqlite3.IntegrityError, match="evidence is permanent"):
            connection.execute(
                "DELETE FROM integrity_violations WHERE prediction_id = ?",
                (draft.prediction_id,),
            )
    finally:
        connection.close()

    with pytest.raises(sqlite3.IntegrityError, match="status is terminal"):
        registry.update_resolution(
            draft.prediction_id,
            status=PredictionStatus.OPEN,
        )


def test_quarantine_rejects_missing_or_empty_evidence(vault: Path) -> None:
    registry = LedgerRegistry(vault / ".ledger" / "registry.db")

    with pytest.raises(ValueError, match="at least one"):
        registry.quarantine_prediction("pred_missing", violations={})
    with pytest.raises(ValueError, match="non-empty"):
        registry.quarantine_prediction("pred_missing", violations={"record": ""})
    with pytest.raises(IntegrityError, match="unknown or uncommitted"):
        registry.quarantine_prediction(
            "pred_missing",
            violations={"record": "missing"},
        )


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


def test_touched_window_evidence_is_permanent(vault: Path) -> None:
    registry = LedgerRegistry(vault / ".ledger" / "registry.db")
    registry.mark_window_touched(
        "  fam_01  ",
        "2024-01-01",
        "2025-01-01",
        touched_at=datetime(2026, 7, 17, tzinfo=UTC),
    )
    assert registry.is_window_touched("fam_01", "2024-01-01", "2025-01-01")
    assert registry.window_overlaps_touched("  fam_01  ", "2024-12-31", "2025-01-02")

    connection = registry.connect()
    try:
        with pytest.raises(sqlite3.IntegrityError, match="write-once"):
            connection.execute("UPDATE touched_windows SET touched_at = '2027-01-01'")
        with pytest.raises(sqlite3.IntegrityError, match="permanent"):
            connection.execute("DELETE FROM touched_windows")
    finally:
        connection.close()
