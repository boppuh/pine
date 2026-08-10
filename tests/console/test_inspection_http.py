from __future__ import annotations

from pathlib import Path

from ledger.console.errors import BackendDomainError, BackendProtocolError
from ledger.console.migrations import CONSOLE_SCHEMA_VERSION
from ledger.read_models import (
    IntegrityReason,
    IntegrityState,
    PredictionPage,
    ResultEvidenceProjection,
    ResultState,
)
from ledger.run import RunState

from .conftest import FakeBackend, MutableClock
from .test_http_security import _build_app, _client, _identity_headers


def test_dashboard_uses_verified_recent_predictions_and_authoritative_counts(
    tmp_path: Path,
    fake_backend: FakeBackend,
    clock: MutableClock,
) -> None:
    app, config, store, _sessions = _build_app(tmp_path, fake_backend, clock)

    with _client(app, config) as client:
        response = client.get("/", headers=_identity_headers())

    assert response.status_code == 200
    assert "Decision evidence you can inspect" in response.text
    assert fake_backend.prediction_detail.forecast.strategy_id in response.text
    assert "1 committed prediction available" in response.text
    assert fake_backend.prediction_list_requests == [
        {
            "limit": 5,
            "cursor": None,
            "registration_status": None,
            "status": None,
            "strategy_id": None,
            "result_state": None,
        }
    ]
    assert fake_backend.capture_requests == []
    assert fake_backend.draft_requests == []
    assert store.get_status() == {"schema_version": CONSOLE_SCHEMA_VERSION}


def test_prediction_list_passes_filters_and_preserves_them_in_next_page(
    tmp_path: Path,
    fake_backend: FakeBackend,
    clock: MutableClock,
) -> None:
    strategy_id = fake_backend.prediction_detail.forecast.strategy_id
    fake_backend.prediction_page = fake_backend.prediction_page.model_copy(
        update={"next_cursor": "next+/="}
    )
    app, config, _store, _sessions = _build_app(tmp_path, fake_backend, clock)

    with _client(app, config) as client:
        response = client.get(
            "/predictions",
            headers=_identity_headers(),
            params={
                "registration_status": "preregistered",
                "status": "open",
                "strategy_id": strategy_id,
                "result_state": "absent",
            },
        )

    assert response.status_code == 200
    assert "Verified results" in response.text
    assert strategy_id in response.text
    assert "cursor=next%2B%2F%3D" in response.text
    assert "registration_status=preregistered" in response.text
    request = fake_backend.prediction_list_requests[-1]
    assert request["registration_status"] == "preregistered"
    assert request["status"] == "open"
    assert request["strategy_id"] == strategy_id
    assert request["result_state"] is ResultState.ABSENT


def test_failed_summary_does_not_render_unverified_forecast_fields(
    tmp_path: Path,
    fake_backend: FakeBackend,
    clock: MutableClock,
) -> None:
    trusted = fake_backend.prediction_page.items[0]
    failed = trusted.model_copy(
        update={
            "strategy_id": None,
            "out_of_sample_window": None,
            "integrity_state": IntegrityState.FAILED,
            "integrity_reason": IntegrityReason.SNAPSHOT_UNVERIFIED,
        }
    )
    fake_backend.prediction_page = PredictionPage(items=(failed,), next_cursor=None)
    app, config, _store, _sessions = _build_app(tmp_path, fake_backend, clock)

    with _client(app, config) as client:
        response = client.get("/predictions", headers=_identity_headers())

    assert response.status_code == 200
    assert "Verification failed" in response.text
    assert "snapshot unverified" in response.text
    assert "Not trusted" in response.text
    assert trusted.strategy_id not in response.text
    assert f'href="/predictions/{trusted.prediction_id}"' not in response.text


def test_prediction_detail_is_verified_plain_text_and_renders_result_evidence(
    tmp_path: Path,
    fake_backend: FakeBackend,
    clock: MutableClock,
) -> None:
    metrics = {
        "sharpe": 1.18,
        "win_rate": 0.54,
        "max_drawdown": 0.09,
        "expectancy": 0.04,
        "total_return": 0.11,
        "trade_count": 37,
    }
    result = ResultEvidenceProjection.model_validate(
        {
            "evidence_hash": f"sha256:{'f' * 64}",
            "source_timestamp": "2026-08-08T12:00:00Z",
            "ingested_at": "2026-08-08T12:01:00Z",
            "metric_units": "finance/strategy-edge:decimal-v1",
            "in_sample_metrics": metrics,
            "out_of_sample_metrics": {**metrics, "sharpe": 0.96, "trade_count": 12},
            "regime_breakdown": [],
            "artifacts": [
                {
                    "relative_path": "summary.csv",
                    "sha256": f"sha256:{'1' * 64}",
                    "size_bytes": 481,
                }
            ],
        }
    )
    detail = fake_backend.prediction_detail
    run = detail.run.model_copy(
        update={
            "state": RunState.COMPLETED,
            "completed_at": clock.value,
            "exit_code": 0,
        }
    )
    fake_backend.prediction_detail = detail.model_copy(
        update={
            "body": "<script>alert('record')</script>",
            "lineage": {"family_id": "<img src=x onerror=alert(1)>"},
            "run": run,
            "result": result,
        }
    )
    app, config, _store, _sessions = _build_app(tmp_path, fake_backend, clock)

    with _client(app, config) as client:
        response = client.get(
            f"/predictions/{detail.prediction_id}",
            headers=_identity_headers(),
        )

    assert response.status_code == 200
    assert "Evidence verified" in response.text
    assert "Out-of-sample result" in response.text
    assert "summary.csv" in response.text
    assert "&lt;script&gt;alert" in response.text
    assert "&lt;img src=x onerror=alert(1)&gt;" in response.text
    assert "<script>alert('record')</script>" not in response.text
    assert fake_backend.prediction_detail_requests == [detail.prediction_id]
    assert fake_backend.capture_requests == []


def test_prediction_detail_integrity_failure_is_a_blocking_page(
    tmp_path: Path,
    fake_backend: FakeBackend,
    clock: MutableClock,
) -> None:
    fake_backend.prediction_detail_outcomes.append(
        BackendDomainError(
            status_code=409,
            code="integrity_error",
            message="unsafe backend detail",
        )
    )
    app, config, _store, _sessions = _build_app(tmp_path, fake_backend, clock)

    with _client(app, config) as client:
        response = client.get(
            f"/predictions/{fake_backend.prediction_detail.prediction_id}",
            headers=_identity_headers(),
        )

    assert response.status_code == 409
    assert "Evidence could not be verified" in response.text
    assert "unsafe backend detail" not in response.text
    assert fake_backend.prediction_detail.forecast.strategy_id not in response.text


def test_prediction_protocol_failure_and_invalid_queries_fail_closed(
    tmp_path: Path,
    fake_backend: FakeBackend,
    clock: MutableClock,
) -> None:
    fake_backend.prediction_detail_outcomes.append(
        BackendProtocolError("backend prediction detail binding is invalid")
    )
    app, config, _store, _sessions = _build_app(tmp_path, fake_backend, clock)

    with _client(app, config) as client:
        integrity = client.get(
            f"/predictions/{fake_backend.prediction_detail.prediction_id}",
            headers=_identity_headers(),
        )
        unknown = client.get("/predictions?unknown=1", headers=_identity_headers())
        duplicate = client.get(
            "/predictions?status=open&status=resolved",
            headers=_identity_headers(),
        )

    assert integrity.status_code == 409
    assert unknown.status_code == 422
    assert duplicate.status_code == 422


def test_status_combines_console_and_non_secret_ledger_counts(
    tmp_path: Path,
    fake_backend: FakeBackend,
    clock: MutableClock,
) -> None:
    app, config, _store, _sessions = _build_app(tmp_path, fake_backend, clock)

    with _client(app, config) as client:
        response = client.get("/status", headers=_identity_headers())

    assert response.status_code == 200
    assert "Committed predictions" in response.text
    assert "Integrity violations" in response.text
    assert "Verified run results" in response.text
    assert f"Version {CONSOLE_SCHEMA_VERSION}" in response.text
