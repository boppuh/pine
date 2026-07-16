from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

from ledger.errors import ForecastValidationError, IntegrityError
from ledger.registry import LedgerRegistry
from ledger.writer import LedgerWriter


class SimulatedCrash(RuntimeError):
    pass


def test_atomic_write_creates_snapshot_note_and_registry(vault: Path, draft) -> None:
    writer = LedgerWriter(vault)

    result = writer.write(draft)

    assert result.created is True
    assert json.loads(result.snapshot_path.read_text(encoding="utf-8")) == draft.snapshot
    note = result.record_path.read_text(encoding="utf-8")
    _, yaml_text, body = note.split("---", maxsplit=2)
    frontmatter = yaml.safe_load(yaml_text)
    assert frontmatter["schema_id"] == "finance/strategy-edge:1"
    assert frontmatter["schema_hash"] == result.schema_hash
    assert frontmatter["snapshot_ref"] == f".ledger/snapshots/{draft.prediction_id}.json"
    assert "ORB edge hypothesis" in body

    row = writer.registry.get_prediction(draft.prediction_id)
    assert row is not None
    assert row["transaction_state"] == "committed"
    assert row["immutable_hash"] == result.immutable_hash


@pytest.mark.parametrize("phase", ["after_snapshot_publish", "after_record_publish"])
def test_crash_mid_write_leaves_no_partial_or_orphan(
    vault: Path,
    draft,
    phase: str,
) -> None:
    def crash(current_phase: str) -> None:
        if current_phase == phase:
            raise SimulatedCrash(phase)

    writer = LedgerWriter(vault, failure_injector=crash)

    with pytest.raises(SimulatedCrash, match=phase):
        writer.write(draft)

    assert list((vault / "predictions").iterdir()) == []
    assert list((vault / ".ledger" / "snapshots").iterdir()) == []
    assert writer.registry.get_prediction(draft.prediction_id) is None


def test_duplicate_prediction_id_is_an_idempotent_noop(vault: Path, draft) -> None:
    writer = LedgerWriter(vault)

    first = writer.write(draft)
    second = writer.write(draft)

    assert first.created is True
    assert second.created is False
    assert second.record_path == first.record_path
    assert second.snapshot_path == first.snapshot_path
    assert list((vault / "predictions").glob("*.md")) == [first.record_path]
    assert list((vault / ".ledger" / "snapshots").glob("*.json")) == [first.snapshot_path]


def test_invalid_mapping_fails_closed_before_any_artifact(vault: Path, draft) -> None:
    invalid = draft.model_dump(mode="json")
    invalid["forecast"]["expected_metrics"]["win_rate"] = 1.5
    writer = LedgerWriter(vault)

    with pytest.raises(ForecastValidationError, match="win_rate"):
        writer.write(invalid)

    assert list((vault / "predictions").iterdir()) == []
    assert list((vault / ".ledger" / "snapshots").iterdir()) == []


def test_hard_process_crash_is_recovered_on_next_start(vault: Path, draft) -> None:
    payload_path = vault / "draft.json"
    payload_path.write_text(json.dumps(draft.model_dump(mode="json")), encoding="utf-8")
    script = """
import json
import os
import sys
from ledger.writer import LedgerWriter

def hard_exit(phase):
    if phase == "after_snapshot_publish":
        os._exit(71)

payload = json.loads(open(sys.argv[2], encoding="utf-8").read())
LedgerWriter(sys.argv[1], failure_injector=hard_exit).write(payload)
"""

    completed = subprocess.run(
        [sys.executable, "-c", script, str(vault), str(payload_path)],
        check=False,
    )
    assert completed.returncode == 71

    # Physical paths in different directories cannot share a publication syscall.
    # The registry boundary keeps this uncommitted snapshot logically invisible.
    assert (vault / ".ledger" / "snapshots" / f"{draft.prediction_id}.json").is_file()
    assert not (vault / "predictions" / f"{draft.prediction_id}.md").exists()
    registry = LedgerRegistry(vault / ".ledger" / "registry.db")
    assert registry.get_prediction(draft.prediction_id) is None

    recovered = LedgerWriter(vault)
    assert list((vault / "predictions").iterdir()) == []
    assert list((vault / ".ledger" / "snapshots").iterdir()) == []
    assert recovered.registry.get_prediction(draft.prediction_id) is None


def test_concurrent_record_is_not_overwritten(vault: Path, draft) -> None:
    record_path = vault / "predictions" / f"{draft.prediction_id}.md"

    def create_concurrent_evidence(phase: str) -> None:
        if phase == "after_snapshot_publish":
            record_path.write_text("concurrent audit evidence", encoding="utf-8")

    writer = LedgerWriter(vault, failure_injector=create_concurrent_evidence)

    with pytest.raises(IntegrityError, match="refusing to overwrite"):
        writer.write(draft)

    assert record_path.read_text(encoding="utf-8") == "concurrent audit evidence"
    assert list((vault / ".ledger" / "snapshots").iterdir()) == []
    assert writer.registry.get_prediction(draft.prediction_id) is None


def test_concurrent_snapshot_is_not_overwritten(vault: Path, draft) -> None:
    snapshot_path = vault / ".ledger" / "snapshots" / f"{draft.prediction_id}.json"

    def create_concurrent_evidence(phase: str) -> None:
        if phase == "before_snapshot_publish":
            snapshot_path.write_text('{"external": true}\n', encoding="utf-8")

    writer = LedgerWriter(vault, failure_injector=create_concurrent_evidence)

    with pytest.raises(IntegrityError, match="refusing to overwrite"):
        writer.write(draft)

    assert snapshot_path.read_text(encoding="utf-8") == '{"external": true}\n'
    assert list((vault / "predictions").iterdir()) == []
    assert writer.registry.get_prediction(draft.prediction_id) is None


def test_abandoned_manifest_temp_is_removed_on_start(vault: Path) -> None:
    writer = LedgerWriter(vault)
    manifest_temp = writer.snapshots_dir / ".txn-pred-deadbeef.manifest-tmp"
    manifest_temp.write_text("partial", encoding="utf-8")

    LedgerWriter(vault)

    assert not manifest_temp.exists()
