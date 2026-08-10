from __future__ import annotations

import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEPLOY = ROOT / "deploy"


def _unit(name: str) -> str:
    return (DEPLOY / name).read_text(encoding="utf-8")


def test_console_service_has_a_dedicated_identity_and_narrow_write_boundary() -> None:
    service = _unit("pine-console.service")

    assert "User=pine-console" in service
    assert "Group=pine-ingress" in service
    assert "LoadCredential=backend-token:/var/lib/pine/vault/.ledger/backend.token" in service
    assert "ReadWritePaths=/var/lib/pine/console /run/pine" in service
    assert "ReadOnlyPaths=/var/lib/pine/vault" in service
    assert "InaccessiblePaths=/var/lib/pine/vault/.ledger/backend.token" in service
    assert "RestrictAddressFamilies=AF_UNIX AF_INET" in service
    assert "IPAddressDeny=any" in service
    assert "IPAddressAllow=localhost" in service
    assert "RestartPreventExitStatus=2" in service
    assert "CapabilityBoundingSet=\n" in service

    process_arguments = "\n".join(
        line for line in service.splitlines() if line.startswith(("ExecStart=", "ExecStartPre="))
    )
    assert "backend.token" not in process_arguments
    assert "PINE_CONSOLE_SOCKET_PATH=/run/pine/console.sock" in process_arguments
    assert "PINE_CONSOLE_STATE_PATH=/var/lib/pine/console/console.db" in process_arguments
    assert "PINE_CONSOLE_BACKEND_URL=http://127.0.0.1:8765" in process_arguments
    assert "--host" not in process_arguments
    assert "--port" not in process_arguments


def test_console_readiness_is_credentialed_and_uses_the_read_only_check() -> None:
    service = _unit("pine-console-readiness.service")
    timer = _unit("pine-console-readiness.timer")

    assert "User=pine-console" in service
    assert "LoadCredential=backend-token:/var/lib/pine/vault/.ledger/backend.token" in service
    assert (
        "PINE_CONSOLE_BACKEND_CREDENTIAL_PATH="
        "/run/credentials/pine-console-readiness.service/backend-token" in service
    )
    assert "/opt/decision-edge/current/venv/bin/pine-research-console check" in service
    assert "ReadOnlyPaths=/var/lib/pine/console /var/lib/pine/vault" in service
    assert "ReadWritePaths=" not in service
    assert "OnUnitActiveSec=5min" in timer


def test_console_environment_example_contains_no_credential_value() -> None:
    environment = _unit("console.env.example")

    assert "PINE_CONSOLE_ALLOWED_HOST=" in environment
    assert "PINE_CONSOLE_ALLOWED_IDENTITIES=" in environment
    assert "backend-token" not in environment
    assert "backend.token" not in environment
    assert "TOKEN=" not in environment


def test_installer_runs_all_console_gates_before_selecting_the_release() -> None:
    installer = _unit("install-release.sh")
    switch = installer.index('mv -Tf "$temporary_link" "$current_link"')

    assert installer.index('pine-research-console" release-check') < switch
    assert installer.index('pine-research-console" socket-check') < switch
    assert installer.index('pine-console-state" migrate') < switch
    assert installer.index('pine-console-state" preflight') < switch
    assert installer.index("systemctl daemon-reload") > switch
    assert installer.index('cleanup_release=""', switch) > switch
    assert installer.index("systemctl stop pine-console.service") < switch
    assert 'install -d -m 0700 -o "$console_user"' in installer
    assert 'chmod 0755 "$release_root"' in installer
    assert "stat -c '%a:%U:%G' /run/pine/console.sock" in installer


def test_deployment_shell_script_has_valid_bash_syntax() -> None:
    subprocess.run(
        ("bash", "-n", str(DEPLOY / "install-release.sh")),
        check=True,
        capture_output=True,
        text=True,
    )


def test_backend_no_longer_owns_the_console_state_parent() -> None:
    backend = _unit("pine-backend.service")
    backup = _unit("pine-backup.service")

    assert "StateDirectory=pine" not in backend
    assert "ReadWritePaths=/var/lib/pine/vault" in backend
    assert "ReadWritePaths=/var/lib/pine/vault /var/backups/pine" in backup
