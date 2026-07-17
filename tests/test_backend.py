from __future__ import annotations

import json
import stat
import threading
import time
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx2
import pytest
from filelock import FileLock
from pydantic import JsonValue

from ledger.backend import (
    BackendDescriptor,
    BackendRuntimeFiles,
    BackendServer,
)
from ledger.capture import CaptureService
from ledger.errors import IntegrityError
from ledger.extraction import ExtractionService
from ledger.snapshot import PendingPrediction

STARTED_AT = datetime(2026, 7, 17, 16, 0, tzinfo=UTC)


class FakeExtractor:
    def __init__(self, candidate: Mapping[str, Any]) -> None:
        self.candidate = candidate

    async def extract(
        self,
        text: str,
        *,
        schema_id: str,
        schema: Mapping[str, Any],
    ) -> Mapping[str, Any]:
        del text, schema_id, schema
        return self.candidate


class FakeSnapshotProvider:
    def capture_snapshot(
        self,
        prediction: PendingPrediction,
        at: datetime,
    ) -> Mapping[str, JsonValue]:
        del prediction
        return {
            "strategy_spec_hash": "sha256:strategy",
            "data_as_of_version": at.isoformat(),
        }


def _server_services(
    vault: Path,
    valid_forecast: dict[str, object],
) -> tuple[ExtractionService, CaptureService]:
    capture = CaptureService(vault, FakeSnapshotProvider(), clock=lambda: STARTED_AT)
    extraction = ExtractionService(
        vault,
        FakeExtractor(
            {
                "forecast": valid_forecast,
                "decision": "Run the frozen strategy against the OOS window.",
                "lineage": {"family_id": "fam_backend"},
            }
        ),
        schema_registry=capture.schema_registry,
        registry=capture.registry,
    )
    return extraction, capture


def test_runtime_token_is_persistent_private_and_validated(vault: Path) -> None:
    runtime = BackendRuntimeFiles(vault)

    first = runtime.get_or_create_token()
    second = runtime.get_or_create_token()

    assert second == first
    assert len(first) >= 32
    assert stat.S_IMODE(runtime.token_path.stat().st_mode) == 0o600
    runtime.token_path.chmod(0o644)
    with pytest.raises(IntegrityError, match="permissions must be 0600"):
        runtime.get_or_create_token()


def test_discovery_publication_is_private_atomic_and_owner_aware(vault: Path) -> None:
    runtime = BackendRuntimeFiles(vault)
    first = BackendDescriptor(
        port=41111,
        pid=123,
        instance_id="first",
        started_at=STARTED_AT,
    )
    second = BackendDescriptor(
        port=42222,
        pid=123,
        instance_id="second",
        started_at=STARTED_AT,
    )

    runtime.publish(first)
    assert (
        BackendDescriptor.model_validate_json(runtime.discovery_path.read_text(encoding="utf-8"))
        == first
    )
    assert stat.S_IMODE(runtime.discovery_path.stat().st_mode) == 0o600
    runtime.publish(second)
    runtime.clear(first.instance_id)
    assert runtime.discovery_path.exists()
    runtime.clear(second.instance_id)
    assert not runtime.discovery_path.exists()
    assert list(runtime.ledger_dir.glob(".backend-*.tmp")) == []


def test_real_server_is_discoverable_on_loopback_and_cleans_up(
    vault: Path,
    valid_forecast: dict[str, object],
) -> None:
    extraction, capture = _server_services(vault, valid_forecast)
    server = BackendServer(
        vault,
        extraction_service=extraction,
        capture_service=capture,
        clock=lambda: STARTED_AT,
        log_level="error",
    )
    failures: list[BaseException] = []

    def run_server() -> None:
        try:
            server.run()
        except BaseException as exc:  # pragma: no cover - surfaced by assertion below
            failures.append(exc)

    thread = threading.Thread(target=run_server, daemon=True)
    thread.start()
    discovery_path = vault / ".ledger" / "backend.json"
    deadline = time.monotonic() + 5.0
    descriptor: BackendDescriptor | None = None
    while time.monotonic() < deadline:
        try:
            descriptor = BackendDescriptor.model_validate_json(
                discovery_path.read_text(encoding="utf-8")
            )
            break
        except (FileNotFoundError, ValueError):
            time.sleep(0.02)
    assert descriptor is not None
    token = (vault / descriptor.token_ref).read_text(encoding="utf-8")
    assert descriptor.host == "127.0.0.1"
    assert descriptor.started_at == STARTED_AT
    assert token not in discovery_path.read_text(encoding="utf-8")

    base_url = f"http://{descriptor.host}:{descriptor.port}"
    response: httpx2.Response | None = None
    while time.monotonic() < deadline:
        try:
            response = httpx2.get(f"{base_url}/health", timeout=0.5, trust_env=False)
            break
        except httpx2.TransportError:
            time.sleep(0.02)
    assert response is not None
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "api_version": "v1"}

    draft = httpx2.post(
        f"{base_url}/v1/drafts",
        headers={"Authorization": f"Bearer {token}"},
        json={"text": "A complete strategy hypothesis."},
        timeout=2.0,
        trust_env=False,
    )
    assert draft.status_code == 200
    assert draft.json()["status"] == "ready"

    server.stop()
    thread.join(5.0)
    assert not thread.is_alive()
    assert failures == []
    assert not discovery_path.exists()
    assert (vault / ".ledger" / "backend.token").exists()


def test_descriptor_never_serializes_token_material() -> None:
    descriptor = BackendDescriptor(
        port=41111,
        pid=123,
        instance_id="instance",
        started_at=STARTED_AT,
    )

    payload = json.loads(descriptor.model_dump_json())

    assert payload["token_ref"] == ".ledger/backend.token"
    assert "token" not in payload


def test_server_refuses_when_another_process_lock_is_held(
    vault: Path,
    valid_forecast: dict[str, object],
) -> None:
    extraction, capture = _server_services(vault, valid_forecast)
    server = BackendServer(
        vault,
        extraction_service=extraction,
        capture_service=capture,
    )
    competing_lock = FileLock(str(vault / ".ledger" / "backend.lock"), timeout=0)

    with competing_lock, pytest.raises(IntegrityError, match="already owns this vault"):
        server.run()

    assert not (vault / ".ledger" / "backend.json").exists()
