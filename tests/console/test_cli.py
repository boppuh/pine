from __future__ import annotations

import importlib.metadata
import json
import os
import socket
import stat
from pathlib import Path
from uuid import uuid4

from fastapi.testclient import TestClient

from ledger.api import HealthResponse
from ledger.console.app import create_console_app
from ledger.console.config import ConsoleConfig
from ledger.console.errors import ConsoleConfigError
from ledger.console.models import CaptureInput, WorkflowState
from ledger.console.state import ConsoleStateStore
from ledger.console.unix_socket import CONSOLE_SOCKET_MODE
from ledger.console_cli import run_cli, run_console_app
from ledger.extraction import DraftProposal, ExtractionResult, ExtractionStatus

TOKEN = "console-cli-token-" + "q" * 48


class FakeClient:
    def __init__(self, _config: ConsoleConfig) -> None:
        self.closed = False
        self.health_calls = 0

    def health(self) -> HealthResponse:
        self.health_calls += 1
        return HealthResponse()

    def ready(self) -> HealthResponse:
        return HealthResponse()

    def close(self) -> None:
        self.closed = True


def _environment(tmp_path: Path) -> dict[str, str]:
    credential = tmp_path / "credential"
    credential.write_text(TOKEN, encoding="ascii")
    os.chmod(credential, 0o600)
    environment = {
        "PINE_CONSOLE_ALLOWED_HOST": "pine.example.ts.net",
        "PINE_CONSOLE_ALLOWED_IDENTITIES": "user@example.com",
        "PINE_CONSOLE_SOCKET_PATH": str(tmp_path / "console.sock"),
        "PINE_CONSOLE_STATE_PATH": str(tmp_path / "state" / "console.db"),
        "PINE_CONSOLE_BACKEND_CREDENTIAL_PATH": str(credential),
    }
    ConsoleStateStore(environment["PINE_CONSOLE_STATE_PATH"])
    return environment


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
        "console_schema_version": 2,
        "ready": True,
    }
    assert TOKEN not in output.out + output.err
    assert str(tmp_path) not in output.out + output.err
    assert clients[0].closed is True


def test_check_refuses_uninitialized_state_without_creating_it(
    tmp_path: Path,
    capsys,
) -> None:
    environment = _environment(tmp_path)
    state_path = Path(environment["PINE_CONSOLE_STATE_PATH"])
    for suffix in ("-wal", "-shm", ""):
        Path(f"{state_path}{suffix}").unlink(missing_ok=True)

    assert run_cli(["check"], environ=environment, backend_factory=FakeClient) == 2

    output = capsys.readouterr()
    assert "state" in output.err
    assert not state_path.exists()


def test_release_and_socket_preflights_are_non_secret_and_leave_no_socket(
    tmp_path: Path,
    capsys,
) -> None:
    release_root = Path(__file__).resolve().parents[3]
    assert run_cli(["release-check", "--release-root", str(release_root)]) == 0
    release_output = json.loads(capsys.readouterr().out)
    assert release_output == {
        "asset_count": 14,
        "assets_ready": True,
        "console_schema_maximum": 2,
        "console_schema_minimum": 1,
        "ready": True,
    }

    environment = _environment(tmp_path)
    probe_parent = Path("/tmp") / f"pine-console-probe-{uuid4().hex[:12]}"
    probe_parent.mkdir(mode=0o700)
    probe = probe_parent / "console.sock"
    environment["PINE_CONSOLE_SOCKET_PATH"] = str(probe_parent / "configured.sock")
    assert (
        run_cli(
            ["socket-check", "--socket-path", str(probe)],
            environ=environment,
            backend_factory=FakeClient,
        )
        == 0
    )
    socket_output = capsys.readouterr()
    assert json.loads(socket_output.out) == {"ready": True, "socket_mode": "0660"}
    assert TOKEN not in socket_output.out + socket_output.err
    assert not probe.exists()
    probe_parent.rmdir()


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
        "status": {"schema_version": 2},
        "health": "ok",
    }


def test_default_runner_passes_a_group_restricted_prebound_socket_to_uvicorn(
    tmp_path: Path,
    monkeypatch,
) -> None:
    environment = _environment(tmp_path)
    runtime_path = Path("/tmp") / f"pine-console-cli-{uuid4().hex[:12]}"
    runtime_path.mkdir(mode=0o700)
    environment["PINE_CONSOLE_SOCKET_PATH"] = str(runtime_path / "console.sock")
    config = ConsoleConfig.from_env(environment)
    store = ConsoleStateStore(config.state_path)
    observed: dict[str, object] = {}

    def fake_run(_app, **options) -> None:
        assert "uds" not in options
        duplicated = socket.fromfd(options["fd"], socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            socket_path = Path(duplicated.getsockname())
        finally:
            duplicated.close()
        observed["path"] = socket_path
        observed["mode"] = stat.S_IMODE(socket_path.stat().st_mode)

    monkeypatch.setattr("ledger.console_cli.uvicorn.run", fake_run)
    run_console_app(config, store, FakeClient(config))

    assert observed == {
        "path": config.socket_path,
        "mode": CONSOLE_SOCKET_MODE,
    }
    assert not config.socket_path.exists()
    runtime_path.rmdir()


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


def test_console_app_keeps_health_public_and_all_other_routes_default_denied(
    tmp_path: Path,
) -> None:
    store = ConsoleStateStore(tmp_path / "console.db")
    credential = tmp_path / "credential"
    credential.write_text(TOKEN, encoding="ascii")
    credential.chmod(0o600)
    config = ConsoleConfig(
        socket_path=tmp_path / "console.sock",
        state_path=tmp_path / "console.db",
        backend_credential_path=credential,
        allowed_host="pine.example.ts.net",
        allowed_identities=("user@example.com",),
    )
    backend = FakeClient(config)
    app = create_console_app(config, store, backend)

    with TestClient(app) as client:
        health = client.get("/healthz")
        ready = client.get("/readyz")
        denied = client.get("/workflows")

    assert health.json() == {"status": "ok"}
    assert ready.json() == {
        "status": "ok",
        "backend_api_version": "v1",
        "console_schema_version": 2,
    }
    assert denied.status_code == 403


def test_wheel_entry_point_includes_console_cli() -> None:
    distribution = importlib.metadata.distribution("decision-edge-ledger")
    scripts = {
        item.name: item.value
        for item in distribution.entry_points
        if item.group == "console_scripts"
    }
    assert scripts["pine-research-console"] == "ledger.console_cli:main"
