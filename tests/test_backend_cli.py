from __future__ import annotations

import json
import os
import signal
import subprocess
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from ledger import backend_cli
from ledger.errors import IntegrityError


def _schema_source(root: Path, content: str = '{"type":"object"}\n') -> Path:
    source = root / "source"
    schema = source / "finance" / "strategy-edge.1.json"
    schema.parent.mkdir(parents=True)
    schema.write_text(content, encoding="utf-8")
    return source


def _git_repo(root: Path) -> Path:
    repo = root / "msm"
    repo.mkdir()
    subprocess.run(("git", "init", "-q", str(repo)), check=True)
    subprocess.run(("git", "-C", str(repo), "config", "user.name", "Test"), check=True)
    subprocess.run(
        ("git", "-C", str(repo), "config", "user.email", "test@example.com"),
        check=True,
    )
    (repo / "tracked.txt").write_text("clean\n", encoding="utf-8")
    subprocess.run(("git", "-C", str(repo), "add", "tracked.txt"), check=True)
    subprocess.run(("git", "-C", str(repo), "commit", "-qm", "initial"), check=True)
    return repo


def test_install_schemas_is_idempotent_and_rejects_mutation(tmp_path: Path) -> None:
    source = _schema_source(tmp_path)
    destination = tmp_path / "vault" / ".ledger" / "schemas"

    backend_cli.install_schemas(source, destination)
    installed = destination / "finance" / "strategy-edge.1.json"
    assert installed.read_bytes() == (source / "finance" / installed.name).read_bytes()
    assert installed.stat().st_mode & 0o777 == 0o600

    backend_cli.install_schemas(source, destination)
    (source / "finance" / installed.name).write_text('{"type":"string"}\n', encoding="utf-8")
    with pytest.raises(IntegrityError, match="differs from release schema"):
        backend_cli.install_schemas(source, destination)


def test_install_schemas_rejects_symlinks(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    outside = tmp_path / "outside.json"
    outside.write_text("{}\n", encoding="utf-8")
    (source / "linked.json").symlink_to(outside)

    with pytest.raises(IntegrityError, match="regular files only"):
        backend_cli.install_schemas(source, tmp_path / "destination")


def test_require_clean_git_checkout_returns_full_commit(tmp_path: Path) -> None:
    repo = _git_repo(tmp_path)
    expected = subprocess.run(
        ("git", "-C", str(repo), "rev-parse", "HEAD"),
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()

    assert backend_cli.require_clean_git_checkout(repo) == expected

    (repo / "untracked.txt").write_text("dirty\n", encoding="utf-8")
    with pytest.raises(IntegrityError, match="must be clean"):
        backend_cli.require_clean_git_checkout(repo)


def test_require_safe_vault_rejects_symlinked_authority_paths(tmp_path: Path) -> None:
    actual = tmp_path / "actual"
    actual.mkdir()
    linked = tmp_path / "linked"
    linked.symlink_to(actual, target_is_directory=True)
    with pytest.raises(IntegrityError, match="vault root"):
        backend_cli.require_safe_vault(linked)

    ledger_target = tmp_path / "ledger-target"
    ledger_target.mkdir()
    (actual / ".ledger").symlink_to(ledger_target, target_is_directory=True)
    with pytest.raises(IntegrityError, match=".ledger path is unsafe"):
        backend_cli.require_safe_vault(actual)


def test_check_requires_openai_key_without_disclosing_configuration(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)

    result = backend_cli.run_cli(
        [
            "check",
            "--vault-root",
            str(tmp_path / "vault"),
            "--msm-repo-root",
            str(tmp_path / "msm"),
            "--schema-source",
            str(tmp_path / "schemas"),
        ]
    )

    assert result == 2
    assert capsys.readouterr().err == (
        "pine-ledger-backend: OPENAI_API_KEY is required for the production extractor\n"
    )


def test_check_reports_readiness_and_closes_clients(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    closed = False

    class Components:
        msm_commit = "a" * 40
        clickhouse_client = SimpleNamespace(
            query=lambda _query: SimpleNamespace(result_rows=[["26.1.3.52", "default"]])
        )

        def close(self) -> None:
            nonlocal closed
            closed = True

    monkeypatch.setattr(backend_cli, "_require_openai_key", lambda: None)
    monkeypatch.setattr(backend_cli, "build_components", lambda **_kwargs: Components())
    vault = tmp_path / "vault"

    assert (
        backend_cli.run_cli(
            [
                "check",
                "--vault-root",
                str(vault),
                "--msm-repo-root",
                str(tmp_path / "msm"),
                "--schema-source",
                str(tmp_path / "schemas"),
                "--port",
                "18765",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload == {
        "clickhouse_database": "default",
        "clickhouse_version": "26.1.3.52",
        "msm_commit": "a" * 40,
        "port": 18765,
        "ready": True,
        "vault_root": str(vault),
    }
    assert closed


def test_schema_install_uses_private_mode_independent_of_umask(tmp_path: Path) -> None:
    source = _schema_source(tmp_path)
    destination = tmp_path / "destination"
    previous = os.umask(0)
    try:
        backend_cli.install_schemas(source, destination)
    finally:
        os.umask(previous)
    assert (destination / "finance" / "strategy-edge.1.json").stat().st_mode & 0o777 == 0o600


def test_serve_with_signals_stops_worker_and_restores_handlers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handlers: dict[int, object] = {
        signal.SIGINT: object(),
        signal.SIGTERM: object(),
    }
    stopped = threading.Event()

    class Server:
        def run(self) -> None:
            assert stopped.wait(2.0)

        def stop(self) -> None:
            stopped.set()

    def fake_signal(signum: int, handler: object) -> object:
        prior = handlers[signum]
        handlers[signum] = handler
        return prior

    monkeypatch.setattr(signal, "signal", fake_signal)

    def request_shutdown() -> None:
        deadline = time.monotonic() + 2.0
        while not callable(handlers[signal.SIGTERM]) and time.monotonic() < deadline:
            time.sleep(0.01)
        handler = handlers[signal.SIGTERM]
        assert callable(handler)
        handler(signal.SIGTERM, None)

    shutdown = threading.Thread(target=request_shutdown)
    shutdown.start()
    backend_cli.serve_with_signals(Server())  # type: ignore[arg-type]
    shutdown.join()

    assert stopped.is_set()
    assert not callable(handlers[signal.SIGINT])
    assert not callable(handlers[signal.SIGTERM])


def test_serve_with_signals_propagates_worker_failure() -> None:
    class Server:
        def run(self) -> None:
            raise IntegrityError("worker failed")

        def stop(self) -> None:
            pass

    with pytest.raises(IntegrityError, match="worker failed"):
        backend_cli.serve_with_signals(Server())  # type: ignore[arg-type]
