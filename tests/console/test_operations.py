from __future__ import annotations

import importlib.metadata
import json
import sqlite3
import stat
from pathlib import Path

import pytest

from ledger.console import migrations as console_migrations
from ledger.console.errors import ConsoleStateError
from ledger.console.migrations import (
    _MIGRATION_1,
    CONSOLE_SCHEMA_VERSION,
    _migration_statements,
)
from ledger.console.models import CaptureInput, WorkflowState
from ledger.console.operations import (
    create_console_state_backup,
    inspect_console_state,
    migrate_console_state,
    run_cli,
    verify_console_state_backup,
)
from ledger.console.state import ConsoleStateStore
from ledger.extraction import DraftProposal, ExtractionResult, ExtractionStatus


def _version_one_database(path: Path) -> None:
    path.parent.mkdir(mode=0o700)
    connection = sqlite3.connect(path)
    try:
        for statement in _migration_statements(_MIGRATION_1):
            connection.execute(statement)
        connection.execute(
            "INSERT INTO console_schema_migrations(version, applied_at) VALUES (1, 'v1')"
        )
        connection.execute("PRAGMA user_version = 1")
        connection.commit()
    finally:
        connection.close()
    path.chmod(0o600)


def _database_version(path: Path) -> int:
    connection = sqlite3.connect(path)
    try:
        return int(connection.execute("PRAGMA user_version").fetchone()[0])
    finally:
        connection.close()


def test_preflight_is_read_only_and_refuses_an_incompatible_release(tmp_path: Path) -> None:
    path = tmp_path / "state" / "console.db"
    store = ConsoleStateStore(path)
    workflow = store.create_workflow(
        user_id="operator@example.com",
        source_text="Synthetic operational state",
    )
    before = store.get_workflow(workflow.workflow_id, workflow.user_id)

    assert inspect_console_state(path) == {
        "compatible": True,
        "initialized": True,
        "schema_version": CONSOLE_SCHEMA_VERSION,
    }
    with pytest.raises(ConsoleStateError, match="incompatible"):
        inspect_console_state(path, maximum_schema_version=1)

    after = store.get_workflow(workflow.workflow_id, workflow.user_id)
    assert after == before


def test_compatible_rollback_preflight_preserves_uncertain_workflow(
    tmp_path: Path,
    proposal: DraftProposal,
    capture_input: CaptureInput,
) -> None:
    path = tmp_path / "state" / "console.db"
    store = ConsoleStateStore(path)
    editing = store.create_workflow(
        user_id="operator@example.com",
        source_text=proposal.body,
    )
    store.begin_extraction(editing.workflow_id, editing.user_id)
    reviewing = store.finish_extraction(
        editing.workflow_id,
        editing.user_id,
        ExtractionResult(status=ExtractionStatus.READY, proposal=proposal),
    )
    submitting = store.freeze_and_begin_submission(
        reviewing.workflow_id,
        reviewing.user_id,
        capture_input,
    )
    store.recover_abandoned_workflows()
    before = store.get_workflow(submitting.workflow_id, submitting.user_id)
    assert before.state is WorkflowState.UNCERTAIN

    assert (
        inspect_console_state(
            path,
            minimum_schema_version=1,
            maximum_schema_version=CONSOLE_SCHEMA_VERSION,
        )["compatible"]
        is True
    )

    assert store.get_workflow(submitting.workflow_id, submitting.user_id) == before


def test_missing_state_is_allowed_only_for_install_preflight(tmp_path: Path) -> None:
    path = tmp_path / "missing" / "console.db"

    assert inspect_console_state(path, allow_uninitialized=True) == {
        "compatible": True,
        "initialized": False,
        "schema_version": 0,
    }
    with pytest.raises(ConsoleStateError, match="not initialized"):
        inspect_console_state(path)
    assert not path.exists()


def test_operations_reject_state_or_backups_resolving_into_the_ledger(tmp_path: Path) -> None:
    ledger = tmp_path / "vault" / ".ledger"
    ledger.mkdir(parents=True)
    unsafe_state = ledger / "console.db"
    unsafe_state.touch(mode=0o600)
    state_link = tmp_path / "state-link"
    state_link.symlink_to(ledger, target_is_directory=True)

    with pytest.raises(ConsoleStateError, match="outside the authoritative ledger"):
        inspect_console_state(state_link / "console.db", allow_uninitialized=True)

    safe_state = tmp_path / "state" / "console.db"
    ConsoleStateStore(safe_state)
    with pytest.raises(ConsoleStateError, match="outside the authoritative ledger"):
        create_console_state_backup(safe_state, ledger / "backups")
    assert not (ledger / "backups").exists()


def test_online_backup_is_private_verified_and_detects_tampering(tmp_path: Path) -> None:
    path = tmp_path / "state" / "console.db"
    ConsoleStateStore(path).create_workflow(
        user_id="operator@example.com",
        source_text="Synthetic backup state",
    )

    created = create_console_state_backup(path, tmp_path / "backups")
    backup = Path(created["backup"])

    assert created["verified"] is True
    assert created["database_sha256"].startswith("sha256:")
    assert stat.S_IMODE(backup.stat().st_mode) == 0o700
    assert {item.name for item in backup.iterdir()} == {"console.db", "manifest.json"}
    assert all(stat.S_IMODE(item.stat().st_mode) == 0o600 for item in backup.iterdir())
    assert verify_console_state_backup(backup)["schema_version"] == CONSOLE_SCHEMA_VERSION

    with (backup / "console.db").open("ab") as handle:
        handle.write(b"tamper")
    with pytest.raises(ConsoleStateError, match="identity mismatch"):
        verify_console_state_backup(backup)


def test_migration_backs_up_version_one_before_advancing(tmp_path: Path) -> None:
    path = tmp_path / "state" / "console.db"
    _version_one_database(path)

    result = migrate_console_state(path, tmp_path / "backups")

    assert result["from_schema_version"] == 1
    assert result["schema_version"] == CONSOLE_SCHEMA_VERSION
    assert result["migrated"] is True
    assert result["backup"] is not None
    backup = verify_console_state_backup(result["backup"])
    assert backup["schema_version"] == 1
    assert _database_version(path) == CONSOLE_SCHEMA_VERSION


def test_failed_migration_preserves_source_and_verified_backup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "state" / "console.db"
    backup_root = tmp_path / "backups"
    _version_one_database(path)
    valid_migration = console_migrations._MIGRATIONS[2]
    monkeypatch.setitem(
        console_migrations._MIGRATIONS,
        2,
        valid_migration + "\nINSERT INTO missing_console_table(value) VALUES (1);\n",
    )

    with pytest.raises(ConsoleStateError, match="migration 2 failed"):
        migrate_console_state(path, backup_root)

    assert _database_version(path) == 1
    backups = [item for item in backup_root.iterdir() if item.is_dir()]
    assert len(backups) == 1
    assert verify_console_state_backup(backups[0])["schema_version"] == 1


def test_state_cli_reports_non_secret_evidence_and_has_a_wheel_entry_point(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    path = tmp_path / "state" / "console.db"
    ConsoleStateStore(path)

    assert run_cli(["preflight", "--state-path", str(path)]) == 0
    output = capsys.readouterr()
    assert json.loads(output.out) == {
        "compatible": True,
        "initialized": True,
        "schema_version": CONSOLE_SCHEMA_VERSION,
    }
    assert str(tmp_path) not in output.out + output.err

    distribution = importlib.metadata.distribution("decision-edge-ledger")
    scripts = {
        item.name: item.value
        for item in distribution.entry_points
        if item.group == "console_scripts"
    }
    assert scripts["pine-console-state"] == "ledger.console.operations:main"
