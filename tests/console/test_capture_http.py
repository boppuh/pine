from __future__ import annotations

import re
from collections.abc import Mapping
from html import unescape
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from ledger.console.errors import BackendDomainError, BackendTransportError
from ledger.console.models import CaptureInput, WorkflowState
from ledger.extraction import ExtractionResult, ExtractionStatus

from .conftest import FakeBackend, MutableClock
from .test_http_security import HOST, IDENTITY, _build_app, _client, _identity_headers


def _form_token(html: str, action: str) -> str:
    form = re.search(
        rf'<form[^>]+action="{re.escape(action)}"[^>]*>(.*?)</form>',
        html,
        flags=re.DOTALL,
    )
    assert form is not None
    token = re.search(r'name="csrf_token" value="([A-Za-z0-9_-]{43})"', form.group(1))
    assert token is not None
    return token.group(1)


def _hidden_value(html: str, name: str) -> str:
    match = re.search(rf'name="{re.escape(name)}" value="([^"]*)"', html)
    assert match is not None
    return unescape(match.group(1))


def _post_headers() -> dict[str, str]:
    return {**_identity_headers(), "Origin": f"https://{HOST}"}


def _start_ready_workflow(
    client: TestClient,
    fake_backend: FakeBackend,
    *,
    source: str = "A complete browser capture hypothesis.",
) -> tuple[str, str]:
    new_page = client.get("/hypotheses/new", headers=_identity_headers())
    assert new_page.status_code == 200
    fake_backend.proposal.body = source
    response = client.post(
        "/workflows",
        headers=_post_headers(),
        data={
            "csrf_token": _form_token(new_page.text, "/workflows"),
            "schema_id": "finance/strategy-edge:1",
            "source_text": source,
        },
        follow_redirects=False,
    )
    assert response.status_code == 303
    location = response.headers["location"]
    match = re.fullmatch(r"/workflows/([0-9a-f-]{36})/review", location)
    assert match is not None
    return match.group(1), location


def _review_data(html: str, workflow_id: str) -> dict[str, str]:
    action = f"/workflows/{workflow_id}/confirm"
    fields = {
        "version": _hidden_value(html, "version"),
        "schema_id": _hidden_value(html, "schema_id"),
        "strategy_id": "strat_console",
        "sharpe": "1.25",
        "win_rate": "0.56",
        "max_drawdown": "-0.12",
        "expectancy": "0.08",
        "in_sample_start": "2024-01-01",
        "in_sample_end": "2024-12-31",
        "out_of_sample_start": "2025-01-01",
        "out_of_sample_end": "2025-03-31",
        "invalidation": "OOS expectancy is zero or negative.",
        "edge_source": "Observed market microstructure behavior.",
        "decision": "Run the frozen specification against untouched OOS data.",
        "family_id": "fam_console",
        "csrf_token": _form_token(html, action),
    }
    return fields


def test_ready_validation_success_and_duplicate_confirmation_are_one_capture(
    tmp_path: Path,
    fake_backend: FakeBackend,
    clock: MutableClock,
) -> None:
    app, config, store, _sessions = _build_app(tmp_path, fake_backend, clock)

    with _client(app, config) as client:
        workflow_id, review_path = _start_ready_workflow(client, fake_backend)
        review = client.get(review_path, headers=_identity_headers())
        assert review.status_code == 200
        assert "Confirm strategy hypothesis" in review.text
        assert "preregistered" in review.text
        assert "Expected Sharpe" in review.text
        assert "Original source text" in review.text
        assert "not verified by the immutable hash" in review.text
        assert len(fake_backend.draft_requests) == 1
        assert fake_backend.capture_requests == []

        invalid = _review_data(review.text, workflow_id)
        invalid["win_rate"] = "1.2"
        validation = client.post(
            f"/workflows/{workflow_id}/confirm",
            headers=_post_headers(),
            data=invalid,
        )
        assert validation.status_code == 422
        assert "Check the highlighted fields" in validation.text
        assert 'id="win_rate"' in validation.text
        assert 'aria-invalid="true"' in validation.text
        assert fake_backend.capture_requests == []
        assert store.get_workflow(workflow_id, IDENTITY).state is WorkflowState.REVIEWING

        invalid_window = _review_data(validation.text, workflow_id)
        invalid_window["in_sample_start"] = "2024-12-31"
        invalid_window["in_sample_end"] = "2024-01-01"
        window_validation = client.post(
            f"/workflows/{workflow_id}/confirm",
            headers=_post_headers(),
            data=invalid_window,
        )
        assert window_validation.status_code == 422
        assert 'id="in_sample_end"' in window_validation.text
        assert "In-sample end must be on or after its start" in window_validation.text
        assert fake_backend.capture_requests == []

        current = client.get(review_path, headers=_identity_headers())
        valid = _review_data(current.text, workflow_id)
        committed = client.post(
            f"/workflows/{workflow_id}/confirm",
            headers=_post_headers(),
            data=valid,
            follow_redirects=False,
        )
        assert committed.status_code == 303
        assert committed.headers["location"] == f"/workflows/{workflow_id}/receipt"
        assert len(fake_backend.capture_requests) == 1

        duplicate = client.post(
            f"/workflows/{workflow_id}/confirm",
            headers=_post_headers(),
            data=valid,
            follow_redirects=False,
        )
        assert duplicate.status_code == 303
        assert len(fake_backend.capture_requests) == 1

        receipt = client.get(duplicate.headers["location"], headers=_identity_headers())
        assert receipt.status_code == 200
        assert "Preregistration committed" in receipt.text
        for value in (
            fake_backend.response.prediction_id,
            fake_backend.response.run_id,
            fake_backend.response.schema_hash,
            fake_backend.response.immutable_hash,
            fake_backend.response.record_ref,
            fake_backend.response.snapshot_ref,
        ):
            assert value in receipt.text
        assert "committed" in receipt.text
        assert fake_backend.receipt_requests == [fake_backend.response.prediction_id]


def test_unable_extraction_preserves_editable_source_and_reuses_transient_workflow(
    tmp_path: Path,
    fake_backend: FakeBackend,
    clock: MutableClock,
) -> None:
    app, config, store, _sessions = _build_app(tmp_path, fake_backend, clock)
    original = "Incomplete thesis without all required metrics."
    fake_backend.draft_outcomes.append(
        ExtractionResult(
            status=ExtractionStatus.UNABLE,
            errors=("$.forecast.expected_metrics: field required",),
        )
    )

    with _client(app, config) as client:
        new_page = client.get("/hypotheses/new", headers=_identity_headers())
        response = client.post(
            "/workflows",
            headers=_post_headers(),
            data={
                "csrf_token": _form_token(new_page.text, "/workflows"),
                "schema_id": "finance/strategy-edge:1",
                "source_text": original,
            },
            follow_redirects=False,
        )
        assert response.status_code == 303
        workflow_id = response.headers["location"].split("/")[2]
        unable = client.get(response.headers["location"], headers=_identity_headers())
        assert unable.status_code == 200
        assert "could not extract a complete proposal" in unable.text
        assert original in unable.text
        assert "field required" in unable.text
        assert fake_backend.capture_requests == []

        invalid_revision = client.post(
            "/workflows",
            headers=_post_headers(),
            data={
                "csrf_token": _form_token(unable.text, "/workflows"),
                "schema_id": "finance/strategy-edge:1",
                "source_text": "   ",
                "workflow_id": workflow_id,
                "version": _hidden_value(unable.text, "version"),
            },
        )
        assert invalid_revision.status_code == 422
        assert _hidden_value(invalid_revision.text, "workflow_id") == workflow_id
        assert _hidden_value(invalid_revision.text, "version") == _hidden_value(
            unable.text, "version"
        )
        assert store.get_workflow(workflow_id, IDENTITY).state is WorkflowState.EDITING

        revised = original + " Expected Sharpe 1.0 and win rate 0.55."
        fake_backend.proposal.body = revised
        retry = client.post(
            "/workflows",
            headers=_post_headers(),
            data={
                "csrf_token": _form_token(invalid_revision.text, "/workflows"),
                "schema_id": "finance/strategy-edge:1",
                "source_text": revised,
                "workflow_id": workflow_id,
                "version": _hidden_value(invalid_revision.text, "version"),
            },
            follow_redirects=False,
        )
        assert retry.status_code == 303
        assert retry.headers["location"] == f"/workflows/{workflow_id}/review"
        workflow = store.get_workflow(workflow_id, IDENTITY)
        assert workflow.state is WorkflowState.REVIEWING
        assert workflow.source_text == revised
        assert len(fake_backend.draft_requests) == 2
        assert fake_backend.capture_requests == []
        connection = store.connect()
        try:
            assert connection.execute("SELECT COUNT(*) FROM workflows").fetchone()[0] == 1
        finally:
            connection.close()


def test_submitting_status_does_not_display_a_generic_failure(
    tmp_path: Path,
    fake_backend: FakeBackend,
    clock: MutableClock,
    capture_input: CaptureInput,
) -> None:
    app, config, store, _sessions = _build_app(tmp_path, fake_backend, clock)

    with _client(app, config) as client:
        workflow_id, _review_path = _start_ready_workflow(client, fake_backend)
        reviewing = store.get_workflow(workflow_id, IDENTITY)
        submitting = store.freeze_and_begin_submission(
            workflow_id,
            IDENTITY,
            capture_input,
            expected_version=reviewing.version,
        )
        assert submitting.state is WorkflowState.SUBMITTING

        status = client.get(
            f"/workflows/{workflow_id}/status",
            headers=_identity_headers(),
        )

        assert status.status_code == 200
        assert "Submitting frozen request" in status.text
        assert "The request could not be completed safely" not in status.text


def test_cancel_discards_transient_content_without_capture(
    tmp_path: Path,
    fake_backend: FakeBackend,
    clock: MutableClock,
) -> None:
    app, config, store, _sessions = _build_app(tmp_path, fake_backend, clock)

    with _client(app, config) as client:
        workflow_id, review_path = _start_ready_workflow(client, fake_backend)
        review = client.get(review_path, headers=_identity_headers())
        cancel_path = f"/workflows/{workflow_id}/cancel"
        response = client.post(
            cancel_path,
            headers=_post_headers(),
            data={
                "csrf_token": _form_token(review.text, cancel_path),
                "version": _hidden_value(review.text, "version"),
            },
            follow_redirects=False,
        )
        assert response.status_code == 303
        workflow = store.get_workflow(workflow_id, IDENTITY)
        assert workflow.state is WorkflowState.CANCELLED
        assert workflow.source_text is None
        assert workflow.proposal is None
        assert fake_backend.capture_requests == []


def test_uncertain_retry_replays_exact_frozen_request(
    tmp_path: Path,
    fake_backend: FakeBackend,
    clock: MutableClock,
) -> None:
    app, config, store, _sessions = _build_app(tmp_path, fake_backend, clock)
    fake_backend.capture_outcomes.extend(
        [
            BackendTransportError("response lost"),
            fake_backend.response.model_copy(update={"created": False}),
        ]
    )

    with _client(app, config) as client:
        workflow_id, review_path = _start_ready_workflow(client, fake_backend)
        review = client.get(review_path, headers=_identity_headers())
        first = client.post(
            f"/workflows/{workflow_id}/confirm",
            headers=_post_headers(),
            data=_review_data(review.text, workflow_id),
            follow_redirects=False,
        )
        assert first.status_code == 303
        assert first.headers["location"] == f"/workflows/{workflow_id}/status"
        uncertain = client.get(first.headers["location"], headers=_identity_headers())
        assert "Commit outcome needs checking" in uncertain.text
        assert "Retry exact frozen request" in uncertain.text
        retry_path = f"/workflows/{workflow_id}/retry"
        retried = client.post(
            retry_path,
            headers=_post_headers(),
            data={
                "csrf_token": _form_token(uncertain.text, retry_path),
                "version": str(store.get_workflow(workflow_id, IDENTITY).version),
            },
            follow_redirects=False,
        )
        assert retried.status_code == 303
        assert retried.headers["location"] == f"/workflows/{workflow_id}/receipt"
        assert len(fake_backend.capture_requests) == 2
        assert fake_backend.capture_requests[0].model_dump(
            mode="json"
        ) == fake_backend.capture_requests[1].model_dump(mode="json")
        recovered = store.get_workflow(workflow_id, IDENTITY)
        assert recovered.capture_response is not None
        assert recovered.capture_response.created is False


@pytest.mark.parametrize(
    ("error", "heading", "retry_visible"),
    [
        (
            BackendDomainError(
                status_code=503,
                code="snapshot_unavailable",
                message="snapshot unavailable",
                details=("snapshot source is temporarily unavailable",),
            ),
            "Exact retry is available",
            True,
        ),
        (
            BackendDomainError(
                status_code=409,
                code="fresh_window_conflict",
                message="fresh window conflict",
                details=("family window overlaps prior research",),
            ),
            "Action required",
            False,
        ),
        (
            BackendDomainError(
                status_code=422,
                code="invalid_forecast",
                message="invalid forecast",
                details=("forecast no longer validates",),
            ),
            "Action required",
            False,
        ),
        (
            BackendDomainError(
                status_code=409,
                code="idempotency_conflict",
                message="idempotency conflict",
                details=("request binding differs",),
            ),
            "Action required",
            False,
        ),
    ],
)
def test_capture_failures_map_to_safe_retry_or_terminal_ui(
    tmp_path: Path,
    fake_backend: FakeBackend,
    clock: MutableClock,
    error: BackendDomainError,
    heading: str,
    retry_visible: bool,
) -> None:
    app, config, _store, _sessions = _build_app(tmp_path, fake_backend, clock)
    fake_backend.capture_outcomes.append(error)

    with _client(app, config) as client:
        workflow_id, review_path = _start_ready_workflow(client, fake_backend)
        review = client.get(review_path, headers=_identity_headers())
        response = client.post(
            f"/workflows/{workflow_id}/confirm",
            headers=_post_headers(),
            data=_review_data(review.text, workflow_id),
            follow_redirects=False,
        )
        status = client.get(response.headers["location"], headers=_identity_headers())
        assert heading in status.text
        assert ("Retry exact frozen request" in status.text) is retry_visible
        for detail in error.details:
            assert detail in status.text


def test_freshness_edit_invalidates_advisory_and_source_is_escaped(
    tmp_path: Path,
    fake_backend: FakeBackend,
    clock: MutableClock,
) -> None:
    app, config, _store, _sessions = _build_app(tmp_path, fake_backend, clock)
    source = '<script>alert("research")</script>'

    with _client(app, config) as client:
        workflow_id, review_path = _start_ready_workflow(client, fake_backend, source=source)
        review = client.get(review_path, headers=_identity_headers())
        assert "<script>alert" not in review.text
        assert "&lt;script&gt;" in review.text
        values: Mapping[str, str] = _review_data(review.text, workflow_id)
        changed = dict(values)
        changed["family_id"] = "fam_changed"
        changed["win_rate"] = "invalid"
        response = client.post(
            f"/workflows/{workflow_id}/confirm",
            headers=_post_headers(),
            data=changed,
        )
        assert response.status_code == 422
        assert "Changed since advisory check; Pine will re-check on confirmation." in response.text
        assert fake_backend.capture_requests == []


def test_receipt_does_not_infer_commit_when_authoritative_read_is_unavailable(
    tmp_path: Path,
    fake_backend: FakeBackend,
    clock: MutableClock,
) -> None:
    app, config, _store, _sessions = _build_app(tmp_path, fake_backend, clock)

    with _client(app, config) as client:
        workflow_id, review_path = _start_ready_workflow(client, fake_backend)
        review = client.get(review_path, headers=_identity_headers())
        confirmed = client.post(
            f"/workflows/{workflow_id}/confirm",
            headers=_post_headers(),
            data=_review_data(review.text, workflow_id),
            follow_redirects=False,
        )
        fake_backend.receipt_outcomes.append(BackendTransportError("read unavailable"))
        receipt = client.get(confirmed.headers["location"], headers=_identity_headers())

        assert receipt.status_code == 200
        assert "Capture response retained" in receipt.text
        assert "Transaction state is not being inferred" in receipt.text
        assert "Preregistration committed" not in receipt.text


@pytest.mark.parametrize("failure_kind", ["backend_integrity", "binding_mismatch"])
def test_receipt_integrity_failure_is_not_presented_as_a_transient_outage(
    tmp_path: Path,
    fake_backend: FakeBackend,
    clock: MutableClock,
    failure_kind: str,
) -> None:
    app, config, _store, _sessions = _build_app(tmp_path, fake_backend, clock)

    with _client(app, config) as client:
        workflow_id, review_path = _start_ready_workflow(client, fake_backend)
        review = client.get(review_path, headers=_identity_headers())
        confirmed = client.post(
            f"/workflows/{workflow_id}/confirm",
            headers=_post_headers(),
            data=_review_data(review.text, workflow_id),
            follow_redirects=False,
        )
        if failure_kind == "backend_integrity":
            fake_backend.receipt_outcomes.append(
                BackendDomainError(
                    status_code=409,
                    code="integrity_error",
                    message="receipt verification failed",
                )
            )
        else:
            authority = fake_backend.get_receipt(fake_backend.response.prediction_id)
            fake_backend.receipt_requests.clear()
            fake_backend.receipt_outcomes.append(
                authority.model_copy(update={"run_id": "run_mismatched"})
            )

        receipt = client.get(confirmed.headers["location"], headers=_identity_headers())

        assert receipt.status_code == 200
        assert "Receipt integrity check failed" in receipt.text
        assert "Do not rely on this receipt" in receipt.text
        assert "temporarily unavailable" not in receipt.text
        assert "Reload this page" not in receipt.text
        assert "Preregistration committed" not in receipt.text
