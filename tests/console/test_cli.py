from __future__ import annotations

import importlib.metadata
import json
import os
from pathlib import Path

from fastapi.testclient import TestClient

from ledger.api import HealthResponse
from ledger.console.app import create_core_app
from ledger.console.config import ConsoleConfig
from ledger.console.errors import ConsoleConfigError
from ledger.console.models import CaptureInput, WorkflowState
from ledger.console.state import ConsoleStateStore
from ledger.console_cli import run_cli
from ledger.extraction import DraftProposal, ExtractionResult, ExtractionStatus

TOKEN = "console-cli-token-" + "q" * 48


class FakeClient:
    def __init__(self, _config: ConsoleConfig) -> None:
        self.closed = False
        self.health_calls = 0

    def health(self) -> HealthResponse:
        self.health_calls += 1
        return HealthResponse()

    def close(self) -> None:
        self.closed = True


def _environment(tmp_path: Path) -> dict[str, str]:
    credential = tmp_path / "credential"
    credential.write_text(TOKEN, encoding="ascii")
    os.chmod(credential, 0o600)
    return {
        "PINE_CONSOLE_ALLOWED_HOST": "pine.example.ts.net",
        "PINE_CONSOLE_SOCKET_PATH": str(tmp_path / "console.sock"),
        "PINE_CONSOLE_STATE_PATH": str(tmp_path / "state" / "console.db"),
        "PINE_CONSOLE_BACKEND_CREDENTIAL_PATH": str(credential),
    }


def test_check_reports_only_non_secret_readiness(
    tmp_path: Path,
    capsys,
) -> None:
    clients: list[FakeClient] = []

    def factory(config: ConsoleConfig):
        client = FakeClient(config)
        clients.append(client)
        return client

    environment = _environment(tmp_path)
    assert run_cli(["check"], environ=environment, backend_factory=factory) == 0

    output = capsys.readouterr()
    assert json.loads(output.out) == {
        "backend_api_version": "v1",
        "console_schema_version": 1,
        "ready": True,
    }
    assert TOKEN not in output.out + output.err
    assert str(tmp_path) not in output.out + output.err
    assert clients[0].closed is True


def test_serve_runs_recovery_before_injected_socket_runner(tmp_path: Path) -> None:
    environment = _environment(tmp_path)
    observed: dict[str, object] = {}

    def runner(config, store, backend) -> None:
        observed["socket"] = config.socket_path
        observed["status"] = store.get_status()
        observed["health"] = backend.health().status

    assert (
        run_cli(
            ["serve"],
            environ=environment,
            backend_factory=FakeClient,
            serve_runner=runner,
        )
        == 0
    )
    assert observed == {
        "socket": Path(environment["PINE_CONSOLE_SOCKET_PATH"]),
        "status": {"schema_version": 1},
        "health": "ok",
    }


def test_serve_recovers_submitting_before_backend_initialization(
    tmp_path: Path,
    proposal: DraftProposal,
    capture_input: CaptureInput,
) -> None:
    environment = _environment(tmp_path)
    store = ConsoleStateStore(environment["PINE_CONSOLE_STATE_PATH"])
    editing = store.create_workflow(user_id="user@example.com", source_text=proposal.body)
    store.begin_extraction(editing.workflow_id, editing.user_id)
    reviewing = store.finish_extraction(
        editing.workflow_id,
        editing.user_id,
        ExtractionResult(status=ExtractionStatus.READY, proposal=proposal),
    )
    submitting = store.freeze_and_begin_submission(
        reviewing.workflow_id,
        reviewing.user_id,
        capture_input,
    )

    def unavailable(_config: ConsoleConfig):
        raise ConsoleConfigError("credential unavailable")

    assert run_cli(["serve"], environ=environment, backend_factory=unavailable) == 2
    recovered = store.get_workflow(submitting.workflow_id, submitting.user_id)
    assert recovered.state is WorkflowState.UNCERTAIN


def test_core_app_exposes_health_only_and_fails_readiness_closed(tmp_path: Path) -> None:
    store = ConsoleStateStore(tmp_path / "console.db")
    backend = FakeClient(
        ConsoleConfig(
            socket_path=tmp_path / "console.sock",
            state_path=tmp_path / "console.db",
            backend_credential_path=tmp_path / "credential",
            allowed_host="pine.example.ts.net",
        )
    )
    app = create_core_app(store, backend)

    with TestClient(app) as client:
        health = client.get("/healthz")
        ready = client.get("/readyz")
        absent = client.get("/workflows")

    assert health.json() == {"status": "ok"}
    assert ready.json() == {
        "status": "ok",
        "backend_api_version": "v1",
        "console_schema_version": 1,
    }
    assert absent.status_code == 404


def test_wheel_entry_point_includes_console_cli() -> None:
    distribution = importlib.metadata.distribution("decision-edge-ledger")
    scripts = {
        item.name: item.value
        for item in distribution.entry_points
        if item.group == "console_scripts"
    }
    assert scripts["pine-research-console"] == "ledger.console_cli:main"
