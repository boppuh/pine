from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest

from ledger.console.backend_client import ConsoleBackendClient
from ledger.console.config import ConsoleConfig
from ledger.console.errors import (
    BackendDomainError,
    BackendProtocolError,
    BackendTransportError,
    ConsoleConfigError,
    FailureDisposition,
)
from ledger.console.models import CaptureInput
from ledger.extraction import DraftProposal, HypothesisExtractionRequest
from ledger.integrity import PredictionStatus, RegistrationStatus
from ledger.json_utils import canonical_json
from ledger.read_models import ResultState

from .conftest import FakeBackend

TOKEN = "console-backend-token-" + "x" * 48


def _config(tmp_path: Path) -> ConsoleConfig:
    return ConsoleConfig(
        socket_path=tmp_path / "console.sock",
        state_path=tmp_path / "console.db",
        backend_credential_path=tmp_path / "backend-token",
        allowed_host="pine.example.ts.net",
        allowed_identities=("user@example.com",),
    )


def _client(
    tmp_path: Path,
    handler,
) -> ConsoleBackendClient:
    return ConsoleBackendClient(
        _config(tmp_path),
        token=TOKEN,
        client=httpx.Client(transport=httpx.MockTransport(handler)),
    )


def test_health_omits_token_and_accepts_additive_response_fields(tmp_path: Path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert "authorization" not in request.headers
        return httpx.Response(
            200,
            json={"status": "ok", "api_version": "v1", "future": True},
        )

    assert _client(tmp_path, handler).health().api_version == "v1"


def test_readiness_uses_cached_workflow_token_on_authenticated_status(
    tmp_path: Path,
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/v1/status"
        assert request.headers["authorization"] == f"Bearer {TOKEN}"
        return httpx.Response(
            200,
            json={
                "status": "ok",
                "api_version": "v1",
                "registry_version": 7,
            },
        )

    assert _client(tmp_path, handler).ready().api_version == "v1"


def test_capture_sends_canonical_bytes_once_and_accepts_additive_receipt(
    tmp_path: Path,
    capture_input: CaptureInput,
) -> None:
    request = capture_input.freeze("console-00000000-0000-4000-8000-000000000001")
    calls: list[bytes] = []

    def handler(http_request: httpx.Request) -> httpx.Response:
        calls.append(http_request.content)
        assert http_request.headers["authorization"] == f"Bearer {TOKEN}"
        return httpx.Response(
            200,
            json={
                "prediction_id": "pred_client",
                "run_id": "run_client",
                "record_ref": "predictions/pred_client.md",
                "snapshot_ref": ".ledger/snapshots/pred_client.json",
                "schema_id": "finance/strategy-edge:1",
                "schema_hash": f"sha256:{'a' * 64}",
                "immutable_hash": f"sha256:{'b' * 64}",
                "created": True,
                "future_field": {"allowed": True},
            },
        )

    result = _client(tmp_path, handler).capture(request)

    assert result.prediction_id == "pred_client"
    assert calls == [canonical_json(request.model_dump(mode="json")).encode()]


def test_authoritative_receipt_validates_committed_projection(
    tmp_path: Path,
    proposal: DraftProposal,
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/v1/predictions/pred_client"
        assert request.headers["authorization"] == f"Bearer {TOKEN}"
        return httpx.Response(
            200,
            json={
                "prediction_id": "pred_client",
                "run_id": "run_client",
                "registration_status": "preregistered",
                "forecast": proposal.forecast.model_dump(mode="json"),
                "decision": proposal.decision,
                "lineage": proposal.lineage,
                "status": "open",
                "transaction_state": "committed",
                "schema_id": "finance/strategy-edge:1",
                "schema_hash": f"sha256:{'a' * 64}",
                "immutable_hash": f"sha256:{'b' * 64}",
                "snapshot_ref": ".ledger/snapshots/pred_client.json",
                "committed_at": datetime(2026, 8, 7, 12, 1, tzinfo=UTC).isoformat(),
                "future_detail": True,
            },
        )

    receipt = _client(tmp_path, handler).get_receipt("pred_client")

    assert receipt.transaction_state == "committed"
    assert receipt.registration_status == "preregistered"
    assert receipt.committed_at.tzinfo is not None


def test_authoritative_receipt_rejects_wrong_prediction_binding(
    tmp_path: Path,
    proposal: DraftProposal,
) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "prediction_id": "different_prediction",
                "run_id": "run_client",
                "registration_status": "preregistered",
                "forecast": proposal.forecast.model_dump(mode="json"),
                "decision": proposal.decision,
                "lineage": proposal.lineage,
                "status": "open",
                "transaction_state": "committed",
                "schema_id": "finance/strategy-edge:1",
                "schema_hash": f"sha256:{'a' * 64}",
                "immutable_hash": f"sha256:{'b' * 64}",
                "snapshot_ref": ".ledger/snapshots/pred_client.json",
                "committed_at": datetime(2026, 8, 7, 12, 1, tzinfo=UTC).isoformat(),
            },
        )

    with pytest.raises(BackendProtocolError, match="prediction binding"):
        _client(tmp_path, handler).get_receipt("pred_client")


def test_prediction_list_encodes_filters_and_accepts_additive_fields(
    tmp_path: Path,
    fake_backend: FakeBackend,
) -> None:
    strategy_id = fake_backend.prediction_detail.forecast.strategy_id

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/v1/predictions"
        assert dict(request.url.params) == {
            "limit": "10",
            "cursor": "cursor+/=",
            "registration_status": "preregistered",
            "status": "open",
            "strategy_id": strategy_id,
            "result_state": "absent",
        }
        payload = fake_backend.prediction_page.model_dump(mode="json")
        payload["future_page"] = True
        payload["items"][0]["future_summary"] = True
        return httpx.Response(200, json=payload)

    page = _client(tmp_path, handler).list_predictions(
        limit=10,
        cursor="cursor+/=",
        registration_status=RegistrationStatus.PREREGISTERED,
        status=PredictionStatus.OPEN,
        strategy_id=strategy_id,
        result_state=ResultState.ABSENT,
    )

    assert page == fake_backend.prediction_page


def test_prediction_list_rejects_unverified_forecast_fields(
    tmp_path: Path,
    fake_backend: FakeBackend,
) -> None:
    payload = fake_backend.prediction_page.model_dump(mode="json")
    payload["items"][0]["integrity_state"] = "failed"
    payload["items"][0]["integrity_reason"] = "record_unverified"

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=payload)

    with pytest.raises(BackendProtocolError, match="exposes unverified data"):
        _client(tmp_path, handler).list_predictions()


def test_prediction_list_rejects_result_before_completed_run(
    tmp_path: Path,
    fake_backend: FakeBackend,
) -> None:
    payload = fake_backend.prediction_page.model_dump(mode="json")
    payload["items"][0]["result_state"] = "present"

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=payload)

    with pytest.raises(BackendProtocolError, match="result state"):
        _client(tmp_path, handler).list_predictions()


def test_prediction_detail_and_status_validate_authoritative_bindings(
    tmp_path: Path,
    fake_backend: FakeBackend,
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/status":
            payload = fake_backend.ledger_status.model_dump(mode="json")
            payload["future_status"] = True
            return httpx.Response(200, json=payload)
        payload = fake_backend.prediction_detail.model_dump(mode="json")
        payload["future_detail"] = True
        payload["snapshot"]["future_snapshot"] = True
        return httpx.Response(200, json=payload)

    client = _client(tmp_path, handler)

    assert (
        client.get_prediction(fake_backend.prediction_detail.prediction_id)
        == fake_backend.prediction_detail
    )
    assert client.get_status() == fake_backend.ledger_status


def test_status_rejects_more_quarantined_than_committed_predictions(
    tmp_path: Path,
    fake_backend: FakeBackend,
) -> None:
    payload = fake_backend.ledger_status.model_dump(mode="json")
    payload["quarantined_predictions"] = payload["committed_predictions"] + 1

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=payload)

    with pytest.raises(BackendProtocolError, match="status counters"):
        _client(tmp_path, handler).get_status()


def test_prediction_detail_rejects_wrong_run_binding(
    tmp_path: Path,
    fake_backend: FakeBackend,
) -> None:
    payload = fake_backend.prediction_detail.model_dump(mode="json")
    payload["run"]["run_id"] = "run_different"

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=payload)

    with pytest.raises(BackendProtocolError, match="detail binding"):
        _client(tmp_path, handler).get_prediction(fake_backend.prediction_detail.prediction_id)


def test_prediction_list_classifies_unsafe_response_ids_as_protocol_errors(
    tmp_path: Path,
    fake_backend: FakeBackend,
) -> None:
    payload = fake_backend.prediction_page.model_dump(mode="json")
    payload["items"][0]["prediction_id"] = "../unsafe"

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=payload)

    with pytest.raises(BackendProtocolError, match="response identifier"):
        _client(tmp_path, handler).list_predictions()


def test_prediction_detail_classifies_unsafe_response_ids_as_protocol_errors(
    tmp_path: Path,
    fake_backend: FakeBackend,
) -> None:
    payload = fake_backend.prediction_detail.model_dump(mode="json")
    payload["run_id"] = "../unsafe"

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=payload)

    with pytest.raises(BackendProtocolError, match="response identifier"):
        _client(tmp_path, handler).get_prediction(fake_backend.prediction_detail.prediction_id)


def test_verified_reads_use_the_dedicated_read_timeout(
    tmp_path: Path,
    fake_backend: FakeBackend,
) -> None:
    observed_timeout: dict[str, float] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        observed_timeout.update(request.extensions["timeout"])
        return httpx.Response(200, json=fake_backend.ledger_status.model_dump(mode="json"))

    config = _config(tmp_path).model_copy(
        update={"health_timeout_seconds": 1.0, "read_timeout_seconds": 17.0}
    )
    client = ConsoleBackendClient(
        config,
        token=TOKEN,
        client=httpx.Client(transport=httpx.MockTransport(handler)),
    )

    assert client.get_status() == fake_backend.ledger_status
    assert observed_timeout["read"] == 17.0
    assert observed_timeout["connect"] == config.connect_timeout_seconds


@pytest.mark.parametrize("prediction_id", [".", ".."])
def test_authoritative_receipt_rejects_dot_segment_before_request(
    tmp_path: Path,
    prediction_id: str,
) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(500)

    with pytest.raises(ValueError, match="identifier is unsafe"):
        _client(tmp_path, handler).get_receipt(prediction_id)

    assert requests == []


def test_frozen_capture_requires_canonical_console_uuid(
    capture_input: CaptureInput,
) -> None:
    with pytest.raises(ValueError, match="canonical UUIDv4"):
        capture_input.freeze("console-not-a-uuid")


def test_draft_response_is_strict_but_tolerates_additive_envelope_fields(
    tmp_path: Path,
    proposal: DraftProposal,
) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        payload = {
            "status": "ready",
            "proposal": proposal.model_dump(mode="json"),
            "errors": [],
            "future_field": "ignored",
        }
        payload["proposal"]["future_field"] = "ignored"
        return httpx.Response(200, json=payload)

    result = _client(tmp_path, handler).create_draft(
        HypothesisExtractionRequest(text=proposal.body)
    )

    assert result.proposal == proposal


def test_draft_response_must_bind_requested_schema(
    tmp_path: Path,
    proposal: DraftProposal,
) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        payload = proposal.model_dump(mode="json")
        payload["schema_id"] = "other/schema:1"
        return httpx.Response(
            200,
            json={"status": "ready", "proposal": payload, "errors": []},
        )

    with pytest.raises(BackendProtocolError, match="schema binding"):
        _client(tmp_path, handler).create_draft(HypothesisExtractionRequest(text="hypothesis"))


def test_draft_response_must_bind_original_source_text(
    tmp_path: Path,
    proposal: DraftProposal,
) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "status": "ready",
                "proposal": proposal.model_dump(mode="json"),
                "errors": [],
            },
        )

    with pytest.raises(BackendProtocolError, match="source binding"):
        _client(tmp_path, handler).create_draft(
            HypothesisExtractionRequest(text="different source text")
        )


def test_malformed_success_is_protocol_error_with_uncertain_capture_outcome(
    tmp_path: Path,
    capture_input: CaptureInput,
) -> None:
    request = capture_input.freeze("console-00000000-0000-4000-8000-000000000001")

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"prediction_id": "missing-everything-else"})

    with pytest.raises(BackendProtocolError, match="capture response"):
        _client(tmp_path, handler).capture(request)


def test_capture_receipt_must_bind_schema_and_artifact_names(
    tmp_path: Path,
    capture_input: CaptureInput,
) -> None:
    request = capture_input.freeze("console-00000000-0000-4000-8000-000000000001")

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "prediction_id": "pred_client",
                "run_id": "run_client",
                "record_ref": "predictions/different.md",
                "snapshot_ref": ".ledger/snapshots/pred_client.json",
                "schema_id": "other/schema:1",
                "schema_hash": f"sha256:{'a' * 64}",
                "immutable_hash": f"sha256:{'b' * 64}",
                "created": True,
            },
        )

    with pytest.raises(BackendProtocolError, match="binding"):
        _client(tmp_path, handler).capture(request)


def test_capture_receipt_must_bind_reviewed_schema_hash(
    tmp_path: Path,
    capture_input: CaptureInput,
) -> None:
    request = capture_input.freeze("console-00000000-0000-4000-8000-000000000001")

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "prediction_id": "pred_client",
                "run_id": "run_client",
                "record_ref": "predictions/pred_client.md",
                "snapshot_ref": ".ledger/snapshots/pred_client.json",
                "schema_id": "finance/strategy-edge:1",
                "schema_hash": f"sha256:{'c' * 64}",
                "immutable_hash": f"sha256:{'b' * 64}",
                "created": True,
            },
        )

    with pytest.raises(BackendProtocolError, match="binding"):
        _client(tmp_path, handler).capture(request)


def test_structured_error_is_classified_and_sensitive_details_are_redacted(
    tmp_path: Path,
    capture_input: CaptureInput,
) -> None:
    request = capture_input.freeze("console-00000000-0000-4000-8000-000000000001")

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            503,
            json={
                "error": {
                    "code": "snapshot_unavailable",
                    "message": "snapshot unavailable",
                    "details": [
                        "backend token was invalid",
                        "failure at /var/lib/pine/private.json",
                    ],
                    "future": "ignored",
                }
            },
        )

    with pytest.raises(BackendDomainError) as captured:
        _client(tmp_path, handler).capture(request)

    assert captured.value.disposition is FailureDisposition.RETRYABLE
    assert captured.value.details == (
        "[redacted sensitive backend detail]",
        "failure at [redacted path]",
    )


@pytest.mark.parametrize(
    ("status_code", "code"),
    [(500, "internal_error"), (418, "future_error")],
)
def test_internal_and_unknown_errors_have_uncertain_disposition(
    tmp_path: Path,
    capture_input: CaptureInput,
    status_code: int,
    code: str,
) -> None:
    request = capture_input.freeze("console-00000000-0000-4000-8000-000000000001")

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            status_code,
            json={"error": {"code": code, "message": "unverified", "details": []}},
        )

    with pytest.raises(BackendDomainError) as captured:
        _client(tmp_path, handler).capture(request)

    assert captured.value.disposition is FailureDisposition.UNCERTAIN


def test_empty_backend_message_gets_a_safe_placeholder(
    tmp_path: Path,
    capture_input: CaptureInput,
) -> None:
    request = capture_input.freeze("console-00000000-0000-4000-8000-000000000001")

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            418,
            json={"error": {"code": "future_error", "message": " ", "details": ["  "]}},
        )

    with pytest.raises(BackendDomainError) as captured:
        _client(tmp_path, handler).capture(request)

    assert captured.value.message == "[empty backend detail]"
    assert captured.value.details == ("[empty backend detail]",)


def test_known_error_code_with_wrong_http_status_is_unverified(
    tmp_path: Path,
    capture_input: CaptureInput,
) -> None:
    request = capture_input.freeze("console-00000000-0000-4000-8000-000000000001")

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            409,
            json={
                "error": {
                    "code": "snapshot_unavailable",
                    "message": "wrong status",
                    "details": [],
                }
            },
        )

    with pytest.raises(BackendProtocolError, match="status"):
        _client(tmp_path, handler).capture(request)


def test_transport_failure_is_not_retried_automatically(
    tmp_path: Path,
    capture_input: CaptureInput,
) -> None:
    request = capture_input.freeze("console-00000000-0000-4000-8000-000000000001")
    calls = 0

    def handler(http_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        raise httpx.ConnectError("connection refused", request=http_request)

    with pytest.raises(BackendTransportError):
        _client(tmp_path, handler).capture(request)
    assert calls == 1


def test_decoding_failure_is_classified_as_transport_failure(
    tmp_path: Path,
    capture_input: CaptureInput,
) -> None:
    request = capture_input.freeze("console-00000000-0000-4000-8000-000000000001")

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"content-encoding": "gzip"},
            content=b"not-a-gzip-stream",
        )

    with pytest.raises(BackendTransportError, match="not received"):
        _client(tmp_path, handler).capture(request)


def test_owned_client_disables_environment_proxy_inheritance(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    class ClientStub:
        def __init__(self, **kwargs: object) -> None:
            captured.update(kwargs)

        def close(self) -> None:
            return None

    monkeypatch.setattr(httpx, "Client", ClientStub)
    client = ConsoleBackendClient(_config(tmp_path), token=TOKEN)
    client.close()

    assert captured["follow_redirects"] is False
    assert captured["trust_env"] is False


def test_streamed_response_has_an_overall_deadline(tmp_path: Path) -> None:
    class Clock:
        value = 0.0

        def __call__(self) -> float:
            return self.value

    clock = Clock()

    class TrickleStream(httpx.SyncByteStream):
        def __iter__(self):
            clock.value += 2.0
            yield b'{"status":"ok",'
            clock.value += 2.0
            yield b'"api_version":"v1"}'

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=TrickleStream())

    client = ConsoleBackendClient(
        _config(tmp_path),
        token=TOKEN,
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        monotonic_clock=clock,
    )

    with pytest.raises(BackendTransportError, match="overall deadline"):
        client.health()


def test_config_loads_private_credential_and_rejects_unsafe_networks(
    tmp_path: Path,
) -> None:
    credential = tmp_path / "credential"
    credential.write_text(TOKEN + "\n", encoding="ascii")
    os.chmod(credential, 0o600)
    environment = {
        "PINE_CONSOLE_ALLOWED_HOST": "pine.example.ts.net",
        "PINE_CONSOLE_ALLOWED_IDENTITIES": "user@example.com",
        "PINE_CONSOLE_SOCKET_PATH": str(tmp_path / "console.sock"),
        "PINE_CONSOLE_STATE_PATH": str(tmp_path / "console.db"),
        "PINE_CONSOLE_BACKEND_CREDENTIAL_PATH": str(credential),
        "PINE_CONSOLE_READ_TIMEOUT_SECONDS": "17.5",
    }

    config = ConsoleConfig.from_env(environment)

    assert config.read_backend_token() == TOKEN
    assert config.backend_url == "http://127.0.0.1:8765"
    assert config.read_timeout_seconds == 17.5
    assert config.allowed_identities == ("user@example.com",)
    without_identities = {
        key: value for key, value in environment.items() if key != "PINE_CONSOLE_ALLOWED_IDENTITIES"
    }
    with pytest.raises(ConsoleConfigError, match="ALLOWED_IDENTITIES"):
        ConsoleConfig.from_env(without_identities)
    duplicates = dict(
        environment,
        PINE_CONSOLE_ALLOWED_IDENTITIES="User@Example.com,user@example.com",
    )
    with pytest.raises(ConsoleConfigError, match="invalid"):
        ConsoleConfig.from_env(duplicates)
    non_ascii = dict(
        environment,
        PINE_CONSOLE_ALLOWED_IDENTITIES="ßtrasse@example.com",
    )
    with pytest.raises(ConsoleConfigError, match="invalid"):
        ConsoleConfig.from_env(non_ascii)
    unsafe = dict(environment, PINE_CONSOLE_BACKEND_URL="https://public.example.com")
    with pytest.raises(ConsoleConfigError, match="invalid"):
        ConsoleConfig.from_env(unsafe)
    ledger_dir = tmp_path / "vault" / ".ledger"
    ledger_dir.mkdir(parents=True)
    state_link = tmp_path / "state-link"
    state_link.symlink_to(ledger_dir, target_is_directory=True)
    escaped = dict(environment, PINE_CONSOLE_STATE_PATH=str(state_link / "console.db"))
    with pytest.raises(ConsoleConfigError, match="invalid"):
        ConsoleConfig.from_env(escaped)
    credential.chmod(0o644)
    with pytest.raises(ConsoleConfigError, match="group/world"):
        config.read_backend_token()


def test_response_size_and_json_shape_are_bounded(tmp_path: Path) -> None:
    def oversized(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"{" + b"x" * (2 * 1024 * 1024) + b"}")

    with pytest.raises(BackendProtocolError, match="read limit"):
        _client(tmp_path, oversized).health()

    def array(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=json.dumps([]).encode())

    with pytest.raises(BackendProtocolError, match="JSON object"):
        _client(tmp_path, array).health()
