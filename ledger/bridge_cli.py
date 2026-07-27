"""Desktop loopback bridge from the Obsidian plugin to an SSH-hosted Pine backend."""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import stat
import subprocess
import sys
import tempfile
import time
import urllib.request
import uuid
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any

from filelock import FileLock, Timeout

from ledger.backend import BackendDescriptor, BackendRuntimeFiles
from ledger.errors import IntegrityError, LedgerError

DEFAULT_LOCAL_PORT = 18765
_SSH_DESTINATION = re.compile(r"^(?:[A-Za-z0-9._-]+@)?[A-Za-z0-9.-]+$")
_REMOTE_PATH = re.compile(r"^/[A-Za-z0-9._/-]+$")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pine-obsidian-bridge",
        description="publish a local Obsidian discovery endpoint over an SSH tunnel",
    )
    parser.add_argument("--vault-root", type=Path, required=True)
    parser.add_argument("--ssh-destination", required=True)
    parser.add_argument("--remote-vault-root", required=True)
    parser.add_argument("--local-port", type=int, default=DEFAULT_LOCAL_PORT)
    parser.add_argument("--connect-timeout-seconds", type=float, default=15.0)
    parser.add_argument("--ssh-executable", default="ssh")
    parser.add_argument("--scp-executable", default="scp")
    return parser


def run_cli(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        run_bridge(
            vault_root=args.vault_root,
            ssh_destination=args.ssh_destination,
            remote_vault_root=args.remote_vault_root,
            local_port=args.local_port,
            connect_timeout_seconds=args.connect_timeout_seconds,
            ssh_executable=args.ssh_executable,
            scp_executable=args.scp_executable,
        )
        return 0
    except (LedgerError, OSError, subprocess.SubprocessError, ValueError) as exc:
        print(f"pine-obsidian-bridge: {exc}", file=sys.stderr)
        return 2


def run_bridge(
    *,
    vault_root: str | Path,
    ssh_destination: str,
    remote_vault_root: str,
    local_port: int = DEFAULT_LOCAL_PORT,
    connect_timeout_seconds: float = 15.0,
    ssh_executable: str = "ssh",
    scp_executable: str = "scp",
) -> None:
    """Run one exclusive local bridge until SSH exits or the process is interrupted."""

    vault_path = Path(vault_root).expanduser().absolute()
    if vault_path.is_symlink() or not vault_path.is_dir():
        raise IntegrityError(f"desktop vault does not exist or is unsafe: {vault_path}")
    vault = vault_path.resolve()
    ledger_dir = vault / ".ledger"
    if os.path.lexists(ledger_dir) and (ledger_dir.is_symlink() or not ledger_dir.is_dir()):
        raise IntegrityError("desktop vault .ledger path is unsafe")
    _validate_remote(ssh_destination, remote_vault_root)
    if not 1 <= local_port <= 65535:
        raise ValueError("local bridge port must be between 1 and 65535")
    if connect_timeout_seconds <= 0:
        raise ValueError("connect timeout must be positive")
    ssh_path = _find_executable(ssh_executable)
    scp_path = _find_executable(scp_executable)
    runtime = BackendRuntimeFiles(vault)
    process_lock = FileLock(str(runtime.ledger_dir / "backend.lock"), timeout=0)
    try:
        process_lock.acquire()
    except Timeout as exc:
        raise IntegrityError("another backend or bridge already owns this desktop vault") from exc

    tunnel: subprocess.Popen[bytes] | None = None
    instance_id = f"bridge-{uuid.uuid4().hex}"
    try:
        remote_descriptor, token = fetch_remote_runtime(
            ssh_destination=ssh_destination,
            remote_vault_root=remote_vault_root,
            scp_executable=scp_path,
        )
        forward = f"127.0.0.1:{local_port}:{remote_descriptor.host}:{remote_descriptor.port}"
        tunnel = subprocess.Popen(
            (
                ssh_path,
                "-N",
                "-T",
                "-o",
                "ExitOnForwardFailure=yes",
                "-o",
                "ServerAliveInterval=30",
                "-o",
                "ServerAliveCountMax=3",
                "-L",
                forward,
                "--",
                ssh_destination,
            ),
            stdin=subprocess.DEVNULL,
        )
        _wait_for_tunnel(tunnel, local_port, connect_timeout_seconds)
        install_token(runtime.token_path, token)
        descriptor = BackendDescriptor(
            port=local_port,
            pid=os.getpid(),
            instance_id=instance_id,
            started_at=datetime.now(UTC),
        )
        _publish_runtime(runtime, descriptor, token)
        print(
            json.dumps(
                {
                    "instance_id": instance_id,
                    "local_port": local_port,
                    "ready": True,
                    "remote_instance_id": remote_descriptor.instance_id,
                },
                sort_keys=True,
            ),
            flush=True,
        )
        return_code = tunnel.wait()
        if return_code != 0:
            raise IntegrityError(f"SSH tunnel exited with code {return_code}")
    except KeyboardInterrupt:
        return
    finally:
        runtime.clear(instance_id)
        if tunnel is not None and tunnel.poll() is None:
            tunnel.terminate()
            try:
                tunnel.wait(timeout=5)
            except subprocess.TimeoutExpired:
                tunnel.kill()
                tunnel.wait(timeout=5)
        process_lock.release()


def fetch_remote_runtime(
    *,
    ssh_destination: str,
    remote_vault_root: str,
    scp_executable: str,
) -> tuple[BackendDescriptor, str]:
    """Fetch and validate non-secret discovery plus the bearer token without logging it."""

    _validate_remote(ssh_destination, remote_vault_root)
    with tempfile.TemporaryDirectory(prefix="pine-bridge-") as directory:
        root = Path(directory)
        descriptor_path = root / "backend.json"
        token_path = root / "backend.token"
        for remote_name, local_path in (
            ("backend.json", descriptor_path),
            ("backend.token", token_path),
        ):
            source = f"{ssh_destination}:{remote_vault_root}/.ledger/{remote_name}"
            result = subprocess.run(
                (scp_executable, "-q", "--", source, str(local_path)),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            if result.returncode != 0:
                raise IntegrityError("unable to fetch the remote Pine runtime files")
        descriptor = BackendDescriptor.model_validate_json(
            descriptor_path.read_text(encoding="utf-8")
        )
        token = token_path.read_text(encoding="utf-8")
    valid_characters = all("!" <= character <= "~" for character in token)
    if len(token) < 32 or token.strip() != token or not valid_characters:
        raise IntegrityError("remote backend token is malformed")
    return descriptor, token


def install_token(path: Path, token: str) -> bytes | None:
    """Atomically install a private token and return its prior state."""

    path.parent.mkdir(parents=True, exist_ok=True)
    if path.is_symlink() or (path.exists() and not path.is_file()):
        raise IntegrityError("desktop backend token path is unsafe")
    previous = _read_token_state(path)
    try:
        _write_private_token(path, token.encode("utf-8"))
    except BaseException:
        _restore_token(path, previous)
        raise
    return previous


def _publish_runtime(
    runtime: BackendRuntimeFiles,
    descriptor: BackendDescriptor,
    token: str,
) -> None:
    previous_token = install_token(runtime.token_path, token)
    try:
        runtime.publish(descriptor)
    except BaseException:
        _restore_token(runtime.token_path, previous_token)
        raise


def _read_token_state(path: Path) -> bytes | None:
    if not os.path.lexists(path):
        return None
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise IntegrityError("desktop backend token must be a regular file")
        handle = os.fdopen(descriptor, "rb")
        descriptor = -1
        with handle:
            return handle.read()
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _write_private_token(path: Path, content: bytes) -> None:
    temporary = path.parent / f".backend-token-{uuid.uuid4().hex}.tmp"
    descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    try:
        handle = os.fdopen(descriptor, "wb")
        descriptor = -1
        with handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        os.chmod(path, 0o600)
        _fsync_directory(path.parent)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        temporary.unlink(missing_ok=True)


def _restore_token(path: Path, previous: bytes | None) -> None:
    if previous is None:
        path.unlink(missing_ok=True)
        _fsync_directory(path.parent)
        return
    _write_private_token(path, previous)


def _wait_for_tunnel(
    tunnel: subprocess.Popen[bytes],
    local_port: int,
    timeout_seconds: float,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        return_code = tunnel.poll()
        if return_code is not None:
            raise IntegrityError(f"SSH tunnel exited during startup with code {return_code}")
        try:
            payload = _read_health(local_port, timeout_seconds=0.5)
            if payload == {"api_version": "v1", "status": "ok"}:
                return
        except (OSError, ValueError):
            time.sleep(0.1)
    raise IntegrityError("SSH tunnel did not reach a healthy Pine backend")


def _read_health(port: int, *, timeout_seconds: float) -> dict[str, Any]:
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open(f"http://127.0.0.1:{port}/health", timeout=timeout_seconds) as response:
        payload = json.load(response)
    if not isinstance(payload, dict):
        raise ValueError("health response is not an object")
    return payload


def _validate_remote(ssh_destination: str, remote_vault_root: str) -> None:
    if _SSH_DESTINATION.fullmatch(ssh_destination) is None:
        raise ValueError("SSH destination contains unsupported characters")
    path = PurePosixPath(remote_vault_root)
    if (
        _REMOTE_PATH.fullmatch(remote_vault_root) is None
        or str(path) != remote_vault_root
        or ".." in path.parts
    ):
        raise ValueError("remote vault root must be a normalized absolute path")


def _find_executable(value: str) -> str:
    resolved = shutil.which(value)
    if resolved is None:
        raise IntegrityError(f"required executable is unavailable: {value}")
    return resolved


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def main() -> None:
    raise SystemExit(run_cli())
