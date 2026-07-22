from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from ledger.backup_cli import create_backup, verify_backup
from ledger.errors import IntegrityError
from ledger.registry import LedgerRegistry


def _vault(root: Path) -> Path:
    vault = root / "vault"
    schema = vault / ".ledger" / "schemas" / "finance" / "strategy-edge.1.json"
    schema.parent.mkdir(parents=True)
    schema.write_text('{"type":"object"}\n', encoding="utf-8")
    prediction = vault / "predictions" / "pred_test.md"
    prediction.parent.mkdir()
    prediction.write_text("# Immutable prediction\n", encoding="utf-8")
    snapshot = vault / ".ledger" / "snapshots" / "pred_test.json"
    snapshot.parent.mkdir()
    snapshot.write_text('{"strategy_id":"test"}\n', encoding="utf-8")
    evidence = vault / "runs" / "run_test" / "evidence.json"
    evidence.parent.mkdir(parents=True)
    evidence.write_text('{"result_format_version":1}\n', encoding="utf-8")
    connection = LedgerRegistry(vault / ".ledger" / "registry.db").connect()
    connection.close()
    (vault / ".ledger" / "backend.token").write_text("secret-not-backed-up", encoding="utf-8")
    (vault / ".ledger" / "backend.json").write_text("{}\n", encoding="utf-8")
    return vault


def test_create_backup_is_complete_private_and_verifiable(tmp_path: Path) -> None:
    vault = _vault(tmp_path)

    result = create_backup(vault, tmp_path / "backups")

    assert result["created"] is True
    assert result["verified"] is True
    backup = Path(result["backup"])
    manifest = json.loads((backup / "manifest.json").read_text(encoding="utf-8"))
    paths = [item["relative_path"] for item in manifest["files"]]
    assert paths == sorted(paths)
    assert "registry.db" in paths
    assert "vault/.ledger/schemas/finance/strategy-edge.1.json" in paths
    assert "vault/.ledger/snapshots/pred_test.json" in paths
    assert "vault/predictions/pred_test.md" in paths
    assert "vault/runs/run_test/evidence.json" in paths
    assert all("backend.token" not in path for path in paths)
    assert all("backend.json" not in path for path in paths)
    assert backup.stat().st_mode & 0o777 == 0o700
    assert all(path.stat().st_mode & 0o777 == 0o600 for path in backup.rglob("*") if path.is_file())
    assert verify_backup(backup)["verified"] is True


def test_verify_rejects_changed_file(tmp_path: Path) -> None:
    result = create_backup(_vault(tmp_path), tmp_path / "backups")
    backup = Path(result["backup"])
    (backup / "vault" / "predictions" / "pred_test.md").write_text("changed\n", encoding="utf-8")

    with pytest.raises(IntegrityError, match="identity mismatch"):
        verify_backup(backup)


def test_create_rejects_symlinked_managed_content(tmp_path: Path) -> None:
    vault = _vault(tmp_path)
    outside = tmp_path / "outside"
    outside.write_text("outside\n", encoding="utf-8")
    (vault / "predictions" / "unsafe.md").symlink_to(outside)

    with pytest.raises(IntegrityError, match="cannot be symlinks"):
        create_backup(vault, tmp_path / "backups")


def test_create_refuses_while_run_is_active(tmp_path: Path) -> None:
    vault = _vault(tmp_path)
    database = vault / ".ledger" / "registry.db"
    connection = sqlite3.connect(database)
    try:
        connection.execute(
            """
            INSERT INTO runs (
                run_id, prediction_id, started_at, state, execution_started_at,
                completed_at, exit_code, failure_note
            ) VALUES (?, NULL, ?, 'running', ?, NULL, NULL, NULL)
            """,
            ("run_active", "2026-07-22T00:00:00+00:00", "2026-07-22T00:00:01+00:00"),
        )
        connection.commit()
    finally:
        connection.close()

    with pytest.raises(IntegrityError, match="while an MSM run is active"):
        create_backup(vault, tmp_path / "backups")
