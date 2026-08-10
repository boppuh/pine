from __future__ import annotations

import json
import threading
import time
from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
import yaml
from pydantic import JsonValue

from ledger.capture import CaptureService
from ledger.errors import (
    FreshWindowError,
    IdempotencyConflictError,
    IntegrityError,
    SnapshotCaptureError,
)
from ledger.integrity import PreregisteredCaptureRequest, RegistrationStatus
from ledger.snapshot import PendingPrediction
from ledger.writer import LedgerWriter

DECISION_AT = datetime(2026, 7, 16, 13, 30, tzinfo=UTC)
COMMITTED_AT = DECISION_AT + timedelta(seconds=5)


class FakeSnapshotProvider:
    def __init__(
        self,
        snapshot: Mapping[str, JsonValue] | None = None,
        *,
        error: Exception | None = None,
        delay: float = 0,
    ) -> None:
        self.snapshot = (
            snapshot
            if snapshot is not None
            else {
                "strategy_spec_hash": "sha256:strategy",
                "git_commit": "abc123",
                "parameter_set": {"lookback": 40},
                "parameter_count": 1,
                "data_as_of_version": DECISION_AT.isoformat(),
                "random_seed": 42,
            }
        )
        self.error = error
        self.delay = delay
        self.calls: list[tuple[PendingPrediction, datetime]] = []
        self._lock = threading.Lock()

    def capture_snapshot(
        self,
        prediction: PendingPrediction,
        at: datetime,
    ) -> Mapping[str, JsonValue]:
        with self._lock:
            self.calls.append((prediction, at))
        if self.delay:
            time.sleep(self.delay)
        if self.error is not None:
            raise self.error
        return self.snapshot


@pytest.fixture
def capture_request(valid_forecast: dict[str, object]) -> PreregisteredCaptureRequest:
    return PreregisteredCaptureRequest.model_validate(
        {
            "idempotency_key": "confirm-01",
            "forecast": valid_forecast,
            "decision": "Run the frozen specification against the untouched OOS window.",
            "lineage": {"family_id": "fam_01", "parent_prediction_id": None},
            "body": "# Confirmed edge hypothesis",
        }
    )


def test_capture_creates_preregistered_record_with_snapshot(
    vault: Path,
    capture_request: PreregisteredCaptureRequest,
) -> None:
    provider = FakeSnapshotProvider()
    service = CaptureService(vault, provider, clock=lambda: DECISION_AT)

    result = service.capture(capture_request)

    assert result.created is True
    assert result.prediction_id.startswith("pred_")
    assert result.run_id.startswith("run_")
    row = service.registry.get_prediction(result.prediction_id)
    assert row is not None
    assert row["registration_status"] == RegistrationStatus.PREREGISTERED.value
    assert row["transaction_state"] == "committed"
    assert row["created_at"] == DECISION_AT.isoformat()
    assert row["committed_at"] == DECISION_AT.isoformat()
    assert len(provider.calls) == 1
    pending, captured_at = provider.calls[0]
    assert pending.prediction_id == result.prediction_id
    assert pending.run_id == result.run_id
    assert pending.registration_status is RegistrationStatus.PREREGISTERED
    assert captured_at == DECISION_AT

    connection = service.registry.connect()
    try:
        binding = connection.execute("SELECT * FROM capture_requests").fetchone()
    finally:
        connection.close()
    assert binding is not None
    assert binding["idempotency_key"] == capture_request.idempotency_key
    assert binding["prediction_id"] == result.prediction_id


def test_capture_uses_injected_clock_for_decision_and_commit_times(
    vault: Path,
    capture_request: PreregisteredCaptureRequest,
) -> None:
    times = iter((DECISION_AT, COMMITTED_AT))
    provider = FakeSnapshotProvider(delay=0.01)
    service = CaptureService(vault, provider, clock=lambda: next(times))

    result = service.capture(capture_request)

    row = service.registry.get_prediction(result.prediction_id)
    assert row is not None
    assert row["created_at"] == DECISION_AT.isoformat()
    assert row["committed_at"] == COMMITTED_AT.isoformat()
    _, yaml_text, _ = result.record_path.read_text(encoding="utf-8").split("---", maxsplit=2)
    frontmatter = yaml.safe_load(yaml_text)
    assert frontmatter["created_at"] == DECISION_AT.isoformat()
    assert frontmatter["committed_at"] == COMMITTED_AT.isoformat()
    assert provider.calls[0][1] == DECISION_AT


def test_capture_refuses_touched_window_before_snapshot(
    vault: Path,
    capture_request: PreregisteredCaptureRequest,
) -> None:
    provider = FakeSnapshotProvider()
    service = CaptureService(vault, provider, clock=lambda: DECISION_AT)
    service.registry.mark_window_touched("fam_01", "2025-01-01", "2026-01-01")

    assert service.check_fresh_window(capture_request) is False
    with pytest.raises(FreshWindowError, match="overlaps previously observed data"):
        service.capture(capture_request)

    assert provider.calls == []
    assert list((vault / "predictions").iterdir()) == []
    assert list((vault / ".ledger" / "snapshots").iterdir()) == []


def test_capture_allows_nonoverlapping_adjacent_window(
    vault: Path,
    capture_request: PreregisteredCaptureRequest,
) -> None:
    provider = FakeSnapshotProvider()
    service = CaptureService(vault, provider, clock=lambda: DECISION_AT)
    service.registry.mark_window_touched("fam_01", "2023-01-01", "2023-12-31")

    assert service.check_fresh_window(capture_request) is True
    assert service.capture(capture_request).created is True


@pytest.mark.parametrize(
    "provider",
    [
        FakeSnapshotProvider(error=RuntimeError("MSM unavailable")),
        FakeSnapshotProvider(snapshot={}),
    ],
)
def test_snapshot_failure_rolls_back_every_resource(
    vault: Path,
    capture_request: PreregisteredCaptureRequest,
    provider: FakeSnapshotProvider,
) -> None:
    service = CaptureService(vault, provider, clock=lambda: DECISION_AT)

    with pytest.raises(SnapshotCaptureError, match="snapshot capture failed"):
        service.capture(capture_request)

    assert list((vault / "predictions").iterdir()) == []
    assert list((vault / ".ledger" / "snapshots").iterdir()) == []
    connection = service.registry.connect()
    try:
        assert connection.execute("SELECT COUNT(*) FROM predictions").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM capture_requests").fetchone()[0] == 0
    finally:
        connection.close()


def test_capture_retry_is_idempotent_before_recapturing_snapshot(
    vault: Path,
    capture_request: PreregisteredCaptureRequest,
) -> None:
    provider = FakeSnapshotProvider()
    service = CaptureService(vault, provider, clock=lambda: DECISION_AT)

    first = service.capture(capture_request)
    second = service.capture(capture_request)

    assert first.created is True
    assert second.created is False
    assert second.prediction_id == first.prediction_id
    assert second.run_id == first.run_id
    assert len(provider.calls) == 1
    assert list((vault / "predictions").glob("*.md")) == [first.record_path]
    assert list((vault / ".ledger" / "snapshots").glob("*.json")) == [first.snapshot_path]


def test_reused_idempotency_key_with_changed_request_is_rejected(
    vault: Path,
    capture_request: PreregisteredCaptureRequest,
) -> None:
    provider = FakeSnapshotProvider()
    service = CaptureService(vault, provider, clock=lambda: DECISION_AT)
    service.capture(capture_request)
    changed = capture_request.model_dump(mode="json")
    changed["decision"] = "A materially different decision."

    with pytest.raises(IdempotencyConflictError, match="different capture request"):
        service.capture(changed)

    assert len(provider.calls) == 1


def test_concurrent_duplicate_captures_serialize_to_one_record(
    vault: Path,
    capture_request: PreregisteredCaptureRequest,
) -> None:
    provider = FakeSnapshotProvider(delay=0.05)
    service = CaptureService(vault, provider, clock=lambda: DECISION_AT)

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(service.capture, [capture_request, capture_request]))

    assert sorted(result.created for result in results) == [False, True]
    assert len({result.prediction_id for result in results}) == 1
    assert len(provider.calls) == 1
    assert len(list((vault / "predictions").glob("*.md"))) == 1
    assert len(list((vault / ".ledger" / "snapshots").glob("*.json"))) == 1


def test_publication_failure_rolls_back_capture_binding_and_artifacts(
    vault: Path,
    capture_request: PreregisteredCaptureRequest,
) -> None:
    def fail_after_snapshot(phase: str) -> None:
        if phase == "after_snapshot_publish":
            raise RuntimeError("simulated publication failure")

    writer = LedgerWriter(vault, failure_injector=fail_after_snapshot)
    service = CaptureService(
        vault,
        FakeSnapshotProvider(),
        writer=writer,
        clock=lambda: DECISION_AT,
    )

    with pytest.raises(RuntimeError, match="simulated publication failure"):
        service.capture(capture_request)

    assert list((vault / "predictions").iterdir()) == []
    assert list((vault / ".ledger" / "snapshots").iterdir()) == []
    connection = service.registry.connect()
    try:
        assert connection.execute("SELECT COUNT(*) FROM predictions").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM capture_requests").fetchone()[0] == 0
    finally:
        connection.close()


def test_schema_drift_during_snapshot_capture_fails_closed(
    vault: Path,
    capture_request: PreregisteredCaptureRequest,
) -> None:
    schema_path = vault / ".ledger" / "schemas" / "finance" / "strategy-edge.1.json"

    class SchemaMutatingProvider(FakeSnapshotProvider):
        def capture_snapshot(
            self,
            prediction: PendingPrediction,
            at: datetime,
        ) -> Mapping[str, JsonValue]:
            snapshot = super().capture_snapshot(prediction, at)
            schema = json.loads(schema_path.read_text(encoding="utf-8"))
            schema["$comment"] = "changed during capture"
            schema_path.write_text(json.dumps(schema), encoding="utf-8")
            return snapshot

    service = CaptureService(vault, SchemaMutatingProvider(), clock=lambda: DECISION_AT)

    with pytest.raises(IntegrityError, match="schema changed during the capture transaction"):
        service.capture(capture_request)

    assert list((vault / "predictions").iterdir()) == []
    assert list((vault / ".ledger" / "snapshots").iterdir()) == []


def test_reviewed_schema_hash_drift_fails_before_snapshot(
    vault: Path,
    capture_request: PreregisteredCaptureRequest,
) -> None:
    provider = FakeSnapshotProvider()
    service = CaptureService(vault, provider, clock=lambda: DECISION_AT)
    reviewed_hash = service.schema_registry.hash(
        service.schema_registry.load(capture_request.schema_id)
    )
    request_data = capture_request.model_dump(mode="json")
    request_data["expected_schema_hash"] = reviewed_hash
    reviewed_request = PreregisteredCaptureRequest.model_validate(request_data)
    schema_path = vault / ".ledger" / "schemas" / "finance" / "strategy-edge.1.json"
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    schema["$comment"] = "changed after proposal review"
    schema_path.write_text(json.dumps(schema), encoding="utf-8")

    with pytest.raises(IntegrityError, match="reviewed schema hash"):
        service.capture(reviewed_request)

    assert provider.calls == []
    assert list((vault / "predictions").iterdir()) == []
    assert list((vault / ".ledger" / "snapshots").iterdir()) == []
    connection = service.registry.connect()
    try:
        assert connection.execute("SELECT COUNT(*) FROM predictions").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM capture_requests").fetchone()[0] == 0
    finally:
        connection.close()
