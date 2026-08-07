from __future__ import annotations

import json
import sqlite3
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
import yaml

import ledger.record_integrity as record_integrity
from ledger.integrity import PredictionStatus
from ledger.record_integrity import (
    IntegrityCheckState,
    RecordIntegrityChecker,
)
from ledger.watcher import (
    FileChangeKind,
    LedgerRecordEvent,
    ManagedPathViolation,
    ManagedViolationReason,
    VaultWatcher,
)
from ledger.writer import LedgerWriter


def _record_event(
    path: Path,
    prediction_id: str,
    *,
    registered: bool = True,
) -> LedgerRecordEvent:
    return LedgerRecordEvent(
        path=path,
        prediction_id=prediction_id,
        change=FileChangeKind.MODIFIED,
        registered=registered,
        observed_at=datetime.now(UTC),
    )


def _mutate_frontmatter(path: Path, mutation: Callable[[dict[str, Any]], None]) -> None:
    content = path.read_text(encoding="utf-8").replace("\r\n", "\n")
    parts = content.split("---\n", maxsplit=2)
    assert len(parts) == 3
    frontmatter = yaml.safe_load(parts[1])
    assert isinstance(frontmatter, dict)
    mutation(frontmatter)
    rendered = yaml.safe_dump(
        frontmatter,
        allow_unicode=True,
        default_flow_style=False,
        sort_keys=False,
    )
    path.write_text(f"---\n{rendered}---\n{parts[2]}", encoding="utf-8")


def _violation_fields(writer: LedgerWriter, prediction_id: str) -> list[str]:
    connection = writer.registry.connect()
    try:
        return [
            row["field"]
            for row in connection.execute(
                """
                SELECT field FROM integrity_violations
                WHERE prediction_id = ? ORDER BY field
                """,
                (prediction_id,),
            )
        ]
    finally:
        connection.close()


def test_clean_record_may_reindex(vault: Path, draft) -> None:
    writer = LedgerWriter(vault)
    written = writer.write(draft)
    checker = RecordIntegrityChecker(vault, registry=writer.registry)

    result = checker.check_record(_record_event(written.record_path, draft.prediction_id))

    assert result.state is IntegrityCheckState.CLEAN
    assert result.may_reindex is True
    assert result.violations == ()
    row = writer.registry.get_prediction(draft.prediction_id)
    assert row is not None
    assert row["status"] == PredictionStatus.OPEN.value


def test_body_and_mutable_projection_edits_do_not_quarantine(vault: Path, draft) -> None:
    writer = LedgerWriter(vault)
    written = writer.write(draft)
    checker = RecordIntegrityChecker(vault, registry=writer.registry)
    written.record_path.write_text(
        written.record_path.read_text(encoding="utf-8") + "\nAdditional research note.\n",
        encoding="utf-8",
    )
    _mutate_frontmatter(
        written.record_path,
        lambda frontmatter: frontmatter.__setitem__("status", "resolved"),
    )

    result = checker.check_record(_record_event(written.record_path, draft.prediction_id))

    assert result.state is IntegrityCheckState.CLEAN
    assert result.may_reindex is True


@pytest.mark.parametrize(
    ("mutation", "expected_fields"),
    [
        (
            lambda item: item.__setitem__("registration_status", "exploratory"),
            {"immutable_payload", "registration_status"},
        ),
        (
            lambda item: item["forecast"]["expected_metrics"].__setitem__("sharpe", 9.0),
            {"immutable_payload"},
        ),
        (
            lambda item: item.__setitem__("decision", "Use a different decision."),
            {"immutable_payload"},
        ),
        (
            lambda item: item["lineage"].__setitem__("family_id", "fam_tampered"),
            {"immutable_payload", "lineage"},
        ),
        (
            lambda item: item.__setitem__("schema_hash", "sha256:tampered"),
            {"immutable_payload", "schema_hash"},
        ),
        (
            lambda item: item.__setitem__("snapshot_ref", ".ledger/snapshots/other.json"),
            {"immutable_payload", "snapshot_ref"},
        ),
        (
            lambda item: item.__setitem__("created_at", "2020-01-01T00:00:00+00:00"),
            {"created_at"},
        ),
    ],
)
def test_immutable_frontmatter_edit_quarantines(
    vault: Path,
    draft,
    mutation: Callable[[dict[str, Any]], None],
    expected_fields: set[str],
) -> None:
    writer = LedgerWriter(vault)
    written = writer.write(draft)
    checker = RecordIntegrityChecker(vault, registry=writer.registry)
    original = writer.registry.get_prediction(draft.prediction_id)
    assert original is not None
    original_hash = original["immutable_hash"]
    original_status = original["registration_status"]
    _mutate_frontmatter(written.record_path, mutation)

    result = checker.check_record(_record_event(written.record_path, draft.prediction_id))

    assert result.state is IntegrityCheckState.QUARANTINED
    assert result.may_reindex is False
    assert set(result.violations) == expected_fields
    assert set(_violation_fields(writer, draft.prediction_id)) == expected_fields
    row = writer.registry.get_prediction(draft.prediction_id)
    assert row is not None
    assert row["status"] == PredictionStatus.QUARANTINED.value
    assert row["immutable_hash"] == original_hash
    assert row["registration_status"] == original_status


def test_snapshot_content_change_is_detected_by_record_hash(vault: Path, draft) -> None:
    writer = LedgerWriter(vault)
    written = writer.write(draft)
    checker = RecordIntegrityChecker(vault, registry=writer.registry)
    snapshot = json.loads(written.snapshot_path.read_text(encoding="utf-8"))
    snapshot["parameter_count"] = 99
    written.snapshot_path.write_text(json.dumps(snapshot), encoding="utf-8")

    result = checker.check_record(_record_event(written.record_path, draft.prediction_id))

    assert result.state is IntegrityCheckState.QUARANTINED
    assert result.violations == ("immutable_payload",)


def test_managed_snapshot_violation_quarantines_idempotently(vault: Path, draft) -> None:
    writer = LedgerWriter(vault)
    written = writer.write(draft)
    checker = RecordIntegrityChecker(vault, registry=writer.registry)
    violation = ManagedPathViolation(
        path=written.snapshot_path,
        change=FileChangeKind.MODIFIED,
        reason=ManagedViolationReason.SNAPSHOT_REWRITTEN,
        observed_at=datetime.now(UTC),
    )

    first = checker.handle_managed_violation(violation)
    second = checker.handle_managed_violation(violation)

    assert first is not None
    assert first.state is IntegrityCheckState.QUARANTINED
    assert first.changed is True
    assert second is not None
    assert second.state is IntegrityCheckState.QUARANTINED
    assert second.changed is False
    assert _violation_fields(writer, draft.prediction_id) == ["snapshot"]


@pytest.mark.parametrize("damage", ["malformed", "deleted"])
def test_missing_or_malformed_committed_record_quarantines(
    vault: Path,
    draft,
    damage: str,
) -> None:
    writer = LedgerWriter(vault)
    written = writer.write(draft)
    checker = RecordIntegrityChecker(vault, registry=writer.registry)
    if damage == "malformed":
        written.record_path.write_text("not frontmatter\n", encoding="utf-8")
    else:
        written.record_path.unlink()

    result = checker.check_record(_record_event(written.record_path, draft.prediction_id))

    assert result.state is IntegrityCheckState.QUARANTINED
    assert result.violations == ("record",)


def test_unregistered_record_does_not_create_registry_evidence(vault: Path) -> None:
    checker = RecordIntegrityChecker(vault)
    path = vault / "predictions" / "pred_rogue.md"
    path.parent.mkdir()
    path.write_text("---\nid: pred_rogue\ntype: prediction\n---\n", encoding="utf-8")

    result = checker.check_record(_record_event(path, "pred_rogue", registered=False))

    assert result.state is IntegrityCheckState.UNREGISTERED
    connection = checker.registry.connect()
    try:
        assert connection.execute("SELECT COUNT(*) FROM integrity_violations").fetchone()[0] == 0
    finally:
        connection.close()


def test_checker_error_never_allows_reindex(vault: Path, draft, monkeypatch) -> None:
    writer = LedgerWriter(vault)
    written = writer.write(draft)
    checker = RecordIntegrityChecker(vault, registry=writer.registry)

    def fail_registry_read(*_args, **_kwargs):
        raise sqlite3.OperationalError("registry unavailable")

    monkeypatch.setattr(checker.registry, "get_prediction", fail_registry_read)

    result = checker.check_record(_record_event(written.record_path, draft.prediction_id))

    assert result.state is IntegrityCheckState.ERROR
    assert result.may_reindex is False


def test_snapshot_read_limit_is_not_quarantined_as_damage(
    vault: Path,
    draft,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    writer = LedgerWriter(vault)
    written = writer.write(draft)
    checker = RecordIntegrityChecker(vault, registry=writer.registry)
    monkeypatch.setattr(record_integrity, "_MAX_SNAPSHOT_BYTES", 1)

    result = checker.check_record(_record_event(written.record_path, draft.prediction_id))

    assert result.state is IntegrityCheckState.ERROR
    assert result.may_reindex is False
    assert _violation_fields(writer, draft.prediction_id) == []
    row = writer.registry.get_prediction(draft.prediction_id)
    assert row is not None
    assert row["status"] == PredictionStatus.OPEN.value


def test_real_watcher_callback_quarantines_frontmatter_edit(vault: Path, draft) -> None:
    writer = LedgerWriter(vault)
    written = writer.write(draft)
    checker = RecordIntegrityChecker(vault, registry=writer.registry)

    with VaultWatcher(
        vault,
        on_record=checker.check_record,
        on_violation=checker.handle_managed_violation,
        debounce_seconds=0.05,
        reconcile_interval=0.05,
    ):
        _mutate_frontmatter(
            written.record_path,
            lambda frontmatter: frontmatter.__setitem__("registration_status", "exploratory"),
        )
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            row = writer.registry.get_prediction(draft.prediction_id)
            if row is not None and row["status"] == PredictionStatus.QUARANTINED.value:
                break
            time.sleep(0.02)
        else:  # pragma: no cover - diagnostic for platform observer failures
            raise AssertionError("watcher callback did not quarantine the edited record")

    assert set(_violation_fields(writer, draft.prediction_id)) == {
        "immutable_payload",
        "registration_status",
    }
