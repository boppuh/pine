from __future__ import annotations

import shutil
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient
from pydantic import JsonValue

from ledger.api import create_app
from ledger.capture import CaptureService
from ledger.extraction import ExtractionService
from ledger.integrity import PreregisteredCaptureRequest
from ledger.snapshot import PendingPrediction

TOKEN = "test-token-" + "a" * 48
AUTHORIZATION = {"Authorization": f"Bearer {TOKEN}"}
DECISION_AT = datetime(2026, 7, 17, 15, 0, tzinfo=UTC)


@pytest.fixture
def capture_request(
    valid_forecast: dict[str, object],
) -> PreregisteredCaptureRequest:
    return PreregisteredCaptureRequest.model_validate(
        {
            "idempotency_key": "api-confirm-01",
            "forecast": valid_forecast,
            "decision": "Run the frozen strategy against the untouched OOS window.",
            "lineage": {"family_id": "fam_01", "parent_prediction_id": None},
            "body": "# Confirmed API hypothesis",
        }
    )


class FakeExtractor:
    def __init__(self, candidate: Mapping[str, Any] | None) -> None:
        self.candidate = candidate

    async def extract(
        self,
        text: str,
        *,
        schema_id: str,
        schema: Mapping[str, Any],
    ) -> Mapping[str, Any] | None:
        del text, schema_id, schema
        return self.candidate


class FakeSnapshotProvider:
    def __init__(self, *, error: Exception | None = None) -> None:
        self.error = error

    def capture_snapshot(
        self,
        prediction: PendingPrediction,
        at: datetime,
    ) -> Mapping[str, JsonValue]:
        del prediction
        if self.error is not None:
            raise self.error
        return {
            "strategy_spec_hash": "sha256:strategy",
            "data_as_of_version": at.isoformat(),
            "parameter_count": 1,
        }


def _candidate(valid_forecast: dict[str, object]) -> dict[str, Any]:
    return {
        "forecast": valid_forecast,
        "decision": "Run the frozen strategy against the untouched OOS window.",
        "lineage": {"family_id": "fam_api", "parent_prediction_id": None},
    }


def _services(
    vault: Path,
    valid_forecast: dict[str, object],
    *,
    snapshot_error: Exception | None = None,
) -> tuple[ExtractionService, CaptureService]:
    capture = CaptureService(
        vault,
        FakeSnapshotProvider(error=snapshot_error),
        clock=lambda: DECISION_AT,
    )
    extraction = ExtractionService(
        vault,
        FakeExtractor(_candidate(valid_forecast)),
        schema_registry=capture.schema_registry,
        registry=capture.registry,
    )
    return extraction, capture


def test_health_is_public_but_draft_and_capture_require_token(
    vault: Path,
    valid_forecast: dict[str, object],
    capture_request,
) -> None:
    extraction, capture = _services(vault, valid_forecast)
    app = create_app(
        extraction_service=extraction,
        capture_service=capture,
        token=TOKEN,
    )

    with TestClient(app) as client:
        health = client.get("/health")
        missing = client.post("/v1/drafts", json={"text": "A hypothesis."})
        wrong = client.post(
            "/v1/captures",
            headers={"Authorization": "Bearer wrong"},
            json=capture_request.model_dump(mode="json"),
        )

    assert health.status_code == 200
    assert health.json() == {"status": "ok", "api_version": "v1"}
    assert missing.status_code == 401
    assert missing.json() == {
        "error": {
            "code": "unauthorized",
            "message": "a valid local backend token is required",
            "details": [],
        }
    }
    assert wrong.status_code == 401


def test_draft_endpoint_returns_validated_proposal(
    vault: Path,
    valid_forecast: dict[str, object],
) -> None:
    extraction, capture = _services(vault, valid_forecast)
    app = create_app(
        extraction_service=extraction,
        capture_service=capture,
        token=TOKEN,
    )

    with TestClient(app) as client:
        response = client.post(
            "/v1/drafts",
            headers=AUTHORIZATION,
            json={"text": "A complete VWAP mean-reversion hypothesis."},
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ready"
    assert payload["proposal"]["registration_status"] == "preregistered"
    assert payload["proposal"]["fresh_window"] is True
    assert payload["errors"] == []


def test_capture_endpoint_delegates_to_atomic_idempotent_service(
    vault: Path,
    valid_forecast: dict[str, object],
    capture_request,
) -> None:
    extraction, capture = _services(vault, valid_forecast)
    app = create_app(
        extraction_service=extraction,
        capture_service=capture,
        token=TOKEN,
    )
    request_json = capture_request.model_dump(mode="json")

    with TestClient(app) as client:
        first = client.post("/v1/captures", headers=AUTHORIZATION, json=request_json)
        second = client.post("/v1/captures", headers=AUTHORIZATION, json=request_json)

    assert first.status_code == 200
    assert second.status_code == 200
    first_payload = first.json()
    second_payload = second.json()
    assert first_payload["created"] is True
    assert second_payload["created"] is False
    assert second_payload["prediction_id"] == first_payload["prediction_id"]
    assert first_payload["record_ref"].startswith("predictions/")
    assert first_payload["snapshot_ref"].startswith(".ledger/snapshots/")
    row = capture.registry.get_prediction(first_payload["prediction_id"])
    assert row is not None
    assert row["transaction_state"] == "committed"


def test_invalid_request_and_domain_failures_use_stable_error_envelope(
    vault: Path,
    valid_forecast: dict[str, object],
    capture_request,
) -> None:
    extraction, capture = _services(vault, valid_forecast)
    app = create_app(
        extraction_service=extraction,
        capture_service=capture,
        token=TOKEN,
    )
    capture.registry.mark_window_touched("fam_01", "2025-01-01", "2026-01-01")

    with TestClient(app) as client:
        invalid = client.post("/v1/drafts", headers=AUTHORIZATION, json={"text": ""})
        unknown_schema = client.post(
            "/v1/drafts",
            headers=AUTHORIZATION,
            json={"text": "A hypothesis.", "schema_id": "finance/unknown:1"},
        )
        touched = client.post(
            "/v1/captures",
            headers=AUTHORIZATION,
            json=capture_request.model_dump(mode="json"),
        )

    assert invalid.status_code == 422
    assert invalid.json()["error"]["code"] == "invalid_request"
    assert invalid.json()["error"]["details"]
    assert unknown_schema.status_code == 404
    assert unknown_schema.json()["error"]["code"] == "schema_not_found"
    assert touched.status_code == 409
    assert touched.json()["error"]["code"] == "fresh_window_conflict"


def test_capture_endpoint_has_no_registration_status_input(
    vault: Path,
    valid_forecast: dict[str, object],
    capture_request,
) -> None:
    extraction, capture = _services(vault, valid_forecast)
    app = create_app(
        extraction_service=extraction,
        capture_service=capture,
        token=TOKEN,
    )
    request_json = capture_request.model_dump(mode="json")
    request_json["registration_status"] = "exploratory"

    with TestClient(app) as client:
        response = client.post(
            "/v1/captures",
            headers=AUTHORIZATION,
            json=request_json,
        )

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "invalid_request"
    connection = capture.registry.connect()
    try:
        assert connection.execute("SELECT COUNT(*) FROM predictions").fetchone()[0] == 0
    finally:
        connection.close()


def test_snapshot_failure_is_503_without_partial_capture(
    vault: Path,
    valid_forecast: dict[str, object],
    capture_request,
) -> None:
    extraction, capture = _services(
        vault,
        valid_forecast,
        snapshot_error=RuntimeError("MSM unavailable"),
    )
    app = create_app(
        extraction_service=extraction,
        capture_service=capture,
        token=TOKEN,
    )

    with TestClient(app) as client:
        response = client.post(
            "/v1/captures",
            headers=AUTHORIZATION,
            json=capture_request.model_dump(mode="json"),
        )

    assert response.status_code == 503
    assert response.json()["error"]["code"] == "snapshot_unavailable"
    connection = capture.registry.connect()
    try:
        assert connection.execute("SELECT COUNT(*) FROM predictions").fetchone()[0] == 0
    finally:
        connection.close()


def test_app_rejects_weak_token(
    vault: Path,
    valid_forecast: dict[str, object],
) -> None:
    extraction, capture = _services(vault, valid_forecast)

    with pytest.raises(ValueError, match="at least 32"):
        create_app(
            extraction_service=extraction,
            capture_service=capture,
            token="short",
        )


def test_app_rejects_cross_vault_service_composition(
    vault: Path,
    tmp_path: Path,
    valid_forecast: dict[str, object],
) -> None:
    extraction, capture = _services(vault, valid_forecast)
    other_vault = tmp_path / "other-vault"
    shutil.copytree(vault, other_vault)
    foreign_extraction = ExtractionService(
        other_vault,
        extraction.extractor,
    )

    with pytest.raises(ValueError, match="same vault root"):
        create_app(
            extraction_service=foreign_extraction,
            capture_service=capture,
            token=TOKEN,
        )
