from __future__ import annotations

import queue
import time
from pathlib import Path

import pytest

from ledger.watcher import (
    FileChangeKind,
    LedgerRecordEvent,
    ManagedPathViolation,
    ManagedViolationReason,
    VaultWatcher,
)
from ledger.writer import LedgerWriter


def _record_text(prediction_id: str, body: str = "Observed strategy note.") -> str:
    return (
        "---\n"
        f"id: {prediction_id}\n"
        "type: prediction\n"
        "registration_status: exploratory\n"
        "---\n\n"
        f"{body}\n"
    )


def _watcher(
    vault: Path,
    records: queue.Queue[LedgerRecordEvent],
    violations: queue.Queue[ManagedPathViolation],
    *,
    debounce_seconds: float = 0.08,
) -> VaultWatcher:
    return VaultWatcher(
        vault,
        on_record=records.put,
        on_violation=violations.put,
        debounce_seconds=debounce_seconds,
        reconcile_interval=0.05,
    )


def _next_event(events: queue.Queue[object], *, timeout: float = 5.0) -> object:
    try:
        return events.get(timeout=timeout)
    except queue.Empty as exc:  # pragma: no cover - diagnostic for platform observer failures
        raise AssertionError("watcher did not emit the expected event") from exc


def _assert_no_event(events: queue.Queue[object], *, timeout: float = 0.3) -> None:
    with pytest.raises(queue.Empty):
        events.get(timeout=timeout)


def test_new_ledger_record_triggers_reindex_callback(vault: Path) -> None:
    records: queue.Queue[LedgerRecordEvent] = queue.Queue()
    violations: queue.Queue[ManagedPathViolation] = queue.Queue()
    notes = vault / "research"
    notes.mkdir()
    path = notes / "pred_watch_01.md"

    with _watcher(vault, records, violations):
        content = _record_text("pred_watch_01")
        path.write_text(content, encoding="utf-8")
        observed = _next_event(records)

    assert isinstance(observed, LedgerRecordEvent)
    assert observed.path == path
    assert observed.prediction_id == "pred_watch_01"
    assert observed.change is FileChangeKind.CREATED
    assert observed.registered is False
    assert path.read_text(encoding="utf-8") == content
    _assert_no_event(violations, timeout=0.05)


def test_watcher_lifecycle_is_single_use_and_stop_is_idempotent(vault: Path) -> None:
    records: queue.Queue[LedgerRecordEvent] = queue.Queue()
    violations: queue.Queue[ManagedPathViolation] = queue.Queue()
    watcher = _watcher(vault, records, violations)

    assert watcher.is_running is False
    watcher.start()
    assert watcher.is_running is True
    watcher.stop()
    watcher.stop()
    assert watcher.is_running is False
    with pytest.raises(RuntimeError, match="already been started"):
        watcher.start()


def test_rapid_record_saves_are_debounced_per_path(vault: Path) -> None:
    records: queue.Queue[LedgerRecordEvent] = queue.Queue()
    violations: queue.Queue[ManagedPathViolation] = queue.Queue()
    notes = vault / "predictions"
    notes.mkdir()
    path = notes / "pred_watch_02.md"
    path.write_text(_record_text("pred_watch_02", "initial"), encoding="utf-8")

    with _watcher(vault, records, violations, debounce_seconds=0.15):
        for revision in range(6):
            path.write_text(
                _record_text("pred_watch_02", f"revision {revision}"),
                encoding="utf-8",
            )
        observed = _next_event(records)
        time.sleep(0.25)

    assert isinstance(observed, LedgerRecordEvent)
    assert observed.change is FileChangeKind.MODIFIED
    assert records.empty()
    assert violations.empty()


def test_non_ledger_markdown_is_ignored(vault: Path) -> None:
    records: queue.Queue[LedgerRecordEvent] = queue.Queue()
    violations: queue.Queue[ManagedPathViolation] = queue.Queue()
    path = vault / "ordinary-note.md"

    with _watcher(vault, records, violations):
        path.write_text("# Ordinary note\n", encoding="utf-8")
        _assert_no_event(records)

    assert violations.empty()


def test_registered_record_still_triggers_after_frontmatter_is_damaged(
    vault: Path,
    draft,
) -> None:
    writer = LedgerWriter(vault)
    result = writer.write(draft)
    records: queue.Queue[LedgerRecordEvent] = queue.Queue()
    violations: queue.Queue[ManagedPathViolation] = queue.Queue()

    with _watcher(vault, records, violations):
        result.record_path.write_text("frontmatter removed\n", encoding="utf-8")
        observed = _next_event(records)

    assert isinstance(observed, LedgerRecordEvent)
    assert observed.prediction_id == draft.prediction_id
    assert observed.registered is True
    assert observed.change is FileChangeKind.MODIFIED


def test_schema_edit_is_reported_as_managed_violation(vault: Path) -> None:
    records: queue.Queue[LedgerRecordEvent] = queue.Queue()
    violations: queue.Queue[ManagedPathViolation] = queue.Queue()
    schema = vault / ".ledger" / "schemas" / "finance" / "strategy-edge.1.json"

    with _watcher(vault, records, violations):
        schema.write_text(schema.read_text(encoding="utf-8") + "\n", encoding="utf-8")
        observed = _next_event(violations)

    assert isinstance(observed, ManagedPathViolation)
    assert observed.path == schema
    assert observed.reason is ManagedViolationReason.SCHEMA_CHANGED
    assert observed.change is FileChangeKind.MODIFIED
    assert records.empty()


def test_unregistered_snapshot_is_reported(vault: Path) -> None:
    records: queue.Queue[LedgerRecordEvent] = queue.Queue()
    violations: queue.Queue[ManagedPathViolation] = queue.Queue()
    snapshots = vault / ".ledger" / "snapshots"
    snapshots.mkdir()
    rogue = snapshots / "pred_rogue.json"

    with _watcher(vault, records, violations):
        rogue.write_text('{"source":"hand-edit"}\n', encoding="utf-8")
        observed = _next_event(violations)

    assert isinstance(observed, ManagedPathViolation)
    assert observed.path == rogue
    assert observed.reason is ManagedViolationReason.UNREGISTERED_SNAPSHOT
    assert observed.change is FileChangeKind.CREATED


def test_writer_snapshot_append_is_allowed_but_rewrite_is_reported(
    vault: Path,
    draft,
) -> None:
    writer = LedgerWriter(vault)
    records: queue.Queue[LedgerRecordEvent] = queue.Queue()
    violations: queue.Queue[ManagedPathViolation] = queue.Queue()

    with _watcher(vault, records, violations):
        result = writer.write(draft)
        record_event = _next_event(records)
        _assert_no_event(violations)
        result.snapshot_path.write_text(
            result.snapshot_path.read_text(encoding="utf-8") + "\n",
            encoding="utf-8",
        )
        rewrite_violation = _next_event(violations)
        result.snapshot_path.unlink()
        delete_violation = _next_event(violations)

    assert isinstance(record_event, LedgerRecordEvent)
    assert record_event.registered is True
    assert isinstance(rewrite_violation, ManagedPathViolation)
    assert rewrite_violation.path == result.snapshot_path
    assert rewrite_violation.reason is ManagedViolationReason.SNAPSHOT_REWRITTEN
    assert rewrite_violation.change is FileChangeKind.MODIFIED
    assert isinstance(delete_violation, ManagedPathViolation)
    assert delete_violation.path == result.snapshot_path
    assert delete_violation.reason is ManagedViolationReason.SNAPSHOT_DELETED
    assert delete_violation.change is FileChangeKind.DELETED


def test_sqlite_runtime_churn_is_not_reported(vault: Path) -> None:
    records: queue.Queue[LedgerRecordEvent] = queue.Queue()
    violations: queue.Queue[ManagedPathViolation] = queue.Queue()
    registry = vault / ".ledger" / "registry.db"

    with _watcher(vault, records, violations):
        registry.write_bytes(b"runtime state")
        _assert_no_event(violations)

    assert records.empty()


def test_registry_recreation_after_settled_delete_is_reported(vault: Path) -> None:
    records: queue.Queue[LedgerRecordEvent] = queue.Queue()
    violations: queue.Queue[ManagedPathViolation] = queue.Queue()
    registry = vault / ".ledger" / "registry.db"
    registry.write_bytes(b"original registry")

    with _watcher(vault, records, violations):
        registry.unlink()
        removed = _next_event(violations)
        registry.write_bytes(b"replacement registry")
        replaced = _next_event(violations)

    assert isinstance(removed, ManagedPathViolation)
    assert removed.path == registry
    assert removed.reason is ManagedViolationReason.REGISTRY_REMOVED
    assert removed.change is FileChangeKind.DELETED
    assert isinstance(replaced, ManagedPathViolation)
    assert replaced.path == registry
    assert replaced.reason is ManagedViolationReason.REGISTRY_REPLACED
    assert replaced.change is FileChangeKind.CREATED
    assert records.empty()


def test_scalar_record_root_is_treated_as_one_path(vault: Path) -> None:
    records: queue.Queue[LedgerRecordEvent] = queue.Queue()
    violations: queue.Queue[ManagedPathViolation] = queue.Queue()
    watcher = VaultWatcher(
        vault,
        on_record=records.put,
        on_violation=violations.put,
        record_roots="research-records",
    )

    assert watcher.record_roots == ((vault / "research-records").resolve(),)
    watcher.stop()
