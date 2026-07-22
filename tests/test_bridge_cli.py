from __future__ import annotations

import stat
import subprocess
from datetime import UTC, datetime
from pathlib import Path

import pytest

from ledger.backend import BackendDescriptor
from ledger.bridge_cli import _validate_remote, fetch_remote_runtime, install_token
from ledger.errors import IntegrityError


def test_validate_remote_accepts_narrow_ssh_and_absolute_path() -> None:
    _validate_remote("ubuntu@example-host", "/var/lib/pine/vault")

    for destination in ("ubuntu@example;host", "ubuntu@example host", "-oProxyCommand=bad"):
        with pytest.raises(ValueError, match="SSH destination"):
            _validate_remote(destination, "/var/lib/pine/vault")
    for path in ("relative/path", "/var/lib/../secret", "/var//lib/pine", "/var/lib/pine/"):
        with pytest.raises(ValueError, match="remote vault root"):
            _validate_remote("ubuntu@example-host", path)


def test_install_token_is_atomic_private_and_rejects_symlink(tmp_path: Path) -> None:
    token_path = tmp_path / ".ledger" / "backend.token"
    first = "a" * 48
    second = "b" * 48

    install_token(token_path, first)
    install_token(token_path, second)

    assert token_path.read_text(encoding="utf-8") == second
    assert stat.S_IMODE(token_path.stat().st_mode) == 0o600
    token_path.unlink()
    token_path.symlink_to(tmp_path / "elsewhere")
    with pytest.raises(IntegrityError, match="path is unsafe"):
        install_token(token_path, first)


def test_fetch_remote_runtime_validates_without_printing_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    descriptor = BackendDescriptor(
        port=8765,
        pid=123,
        instance_id="remote-instance",
        started_at=datetime(2026, 7, 22, tzinfo=UTC),
    )
    token = "secret-token-" + "x" * 40

    def fake_run(arguments: tuple[str, ...], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        remote = arguments[-2]
        local = Path(arguments[-1])
        if remote.endswith("backend.json"):
            local.write_text(descriptor.model_dump_json(), encoding="utf-8")
        else:
            local.write_text(token, encoding="utf-8")
        return subprocess.CompletedProcess(arguments, 0)

    monkeypatch.setattr(subprocess, "run", fake_run)

    actual_descriptor, actual_token = fetch_remote_runtime(
        ssh_destination="ubuntu@example-host",
        remote_vault_root="/var/lib/pine/vault",
        scp_executable="/usr/bin/scp",
    )

    assert actual_descriptor == descriptor
    assert actual_token == token


def test_fetch_remote_runtime_rejects_short_token(monkeypatch: pytest.MonkeyPatch) -> None:
    descriptor = BackendDescriptor(
        port=8765,
        pid=123,
        instance_id="remote-instance",
        started_at=datetime(2026, 7, 22, tzinfo=UTC),
    )

    def fake_run(arguments: tuple[str, ...], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        remote = arguments[-2]
        local = Path(arguments[-1])
        local.write_text(
            descriptor.model_dump_json() if remote.endswith("backend.json") else "short",
            encoding="utf-8",
        )
        return subprocess.CompletedProcess(arguments, 0)

    monkeypatch.setattr(subprocess, "run", fake_run)

    with pytest.raises(IntegrityError, match="token is malformed"):
        fetch_remote_runtime(
            ssh_destination="ubuntu@example-host",
            remote_vault_root="/var/lib/pine/vault",
            scp_executable="/usr/bin/scp",
        )
