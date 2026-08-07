from __future__ import annotations

import json
import os
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
from ledger.json_utils import canonical_json

TOKEN = "console-backend-token-" + "x" * 48


def _config(tmp_path: Path) -> ConsoleConfig:
    return ConsoleConfig(
        socket_path=tmp_path / "console.sock",
        state_path=tmp_path / "console.db",
        backend_credential_path=tmp_path / "backend-token",
        allowed_host="pine.example.ts.net",
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
        "PINE_CONSOLE_SOCKET_PATH": str(tmp_path / "console.sock"),
        "PINE_CONSOLE_STATE_PATH": str(tmp_path / "console.db"),
        "PINE_CONSOLE_BACKEND_CREDENTIAL_PATH": str(credential),
    }

    config = ConsoleConfig.from_env(environment)

    assert config.read_backend_token() == TOKEN
    assert config.backend_url == "http://127.0.0.1:8765"
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
