"""Production composition and lifecycle commands for the local Pine backend."""

from __future__ import annotations

import argparse
import asyncio
import importlib
import json
import logging
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ledger.backend import BackendDescriptor, BackendServer
from ledger.capture import CaptureService
from ledger.errors import IntegrityError, LedgerError
from ledger.extraction import ExtractionService
from ledger.msm import MSMSnapshotProvider
from ledger.openai_extractor import OpenAIExtractorConfig, OpenAIHypothesisExtractor

DEFAULT_PORT = 8765


@dataclass(frozen=True, slots=True)
class BackendComponents:
    """Owned production services and their external clients."""

    server: BackendServer
    extractor: OpenAIHypothesisExtractor
    clickhouse_client: Any
    msm_commit: str

    def close(self) -> None:
        """Close provider clients after the blocking server exits."""

        try:
            asyncio.run(self.extractor.aclose())
        finally:
            close = getattr(self.clickhouse_client, "close", None)
            if callable(close):
                close()


def build_parser() -> argparse.ArgumentParser:
    """Build the production backend command parser."""

    parser = argparse.ArgumentParser(
        prog="pine-ledger-backend",
        description="run and inspect the loopback-only Pine capture backend",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    for command, help_text in (
        ("serve", "run the production backend until stopped"),
        ("check", "validate configuration, source state, schemas, and ClickHouse"),
    ):
        child = subparsers.add_parser(command, help=help_text)
        _add_runtime_arguments(child)

    health = subparsers.add_parser("health", help="query the discovered loopback backend")
    health.add_argument("--vault-root", type=Path, required=True)
    health.add_argument("--timeout-seconds", type=float, default=3.0)
    return parser


def _add_runtime_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--vault-root", type=Path, required=True)
    parser.add_argument("--msm-repo-root", type=Path, required=True)
    parser.add_argument("--schema-source", type=Path, required=True)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument(
        "--log-level",
        choices=("critical", "error", "warning", "info"),
        default="info",
    )


def run_cli(argv: list[str] | None = None) -> int:
    """Run one backend lifecycle command and return its process exit code."""

    args = build_parser().parse_args(argv)
    try:
        if args.command == "health":
            print(json.dumps(check_health(args.vault_root, args.timeout_seconds), sort_keys=True))
            return 0

        _require_openai_key()
        components = build_components(
            vault_root=args.vault_root,
            msm_repo_root=args.msm_repo_root,
            schema_source=args.schema_source,
            port=args.port,
            log_level=args.log_level,
        )
        try:
            if args.command == "check":
                clickhouse = components.clickhouse_client.query(
                    "SELECT version(), currentDatabase()"
                ).result_rows
                if len(clickhouse) != 1 or len(clickhouse[0]) != 2:
                    raise IntegrityError("ClickHouse readiness returned an invalid result")
                print(
                    json.dumps(
                        {
                            "clickhouse_database": str(clickhouse[0][1]),
                            "clickhouse_version": str(clickhouse[0][0]),
                            "msm_commit": components.msm_commit,
                            "port": args.port,
                            "ready": True,
                            "vault_root": str(args.vault_root.expanduser().resolve()),
                        },
                        sort_keys=True,
                    )
                )
                return 0
            serve_with_signals(components.server)
            return 0
        finally:
            components.close()
    except (ImportError, LedgerError, OSError, RuntimeError, ValueError) as exc:
        print(f"pine-ledger-backend: {exc}", file=sys.stderr)
        return 2


def build_components(
    *,
    vault_root: str | Path,
    msm_repo_root: str | Path,
    schema_source: str | Path,
    port: int = DEFAULT_PORT,
    log_level: str = "info",
) -> BackendComponents:
    """Compose the production backend from Pine and the installed MSM package."""

    resolved_vault = require_safe_vault(vault_root)
    resolved_msm = Path(msm_repo_root).expanduser().resolve()
    if not 1 <= port <= 65535:
        raise ValueError("production backend port must be between 1 and 65535")
    msm_commit = require_clean_git_checkout(resolved_msm)
    install_schemas(schema_source, resolved_vault / ".ledger" / "schemas")

    try:
        client_module = importlib.import_module("msm.ch.client")
        snapshot_module = importlib.import_module("msm.utils.ledger_snapshot")
    except ImportError as exc:
        raise ImportError("production backend requires MSM in the shared runtime") from exc

    clickhouse_client = client_module.get_client()
    extractor: OpenAIHypothesisExtractor | None = None
    try:
        source = snapshot_module.LedgerSnapshotSource(
            clickhouse_client,
            repo_root=resolved_msm,
        )
        capture = CaptureService(resolved_vault, MSMSnapshotProvider(source))
        extractor = OpenAIHypothesisExtractor(OpenAIExtractorConfig.from_env())
        extraction = ExtractionService(
            resolved_vault,
            extractor,
            schema_registry=capture.schema_registry,
            registry=capture.registry,
        )
        server = BackendServer(
            resolved_vault,
            extraction_service=extraction,
            capture_service=capture,
            port=port,
            log_level=log_level,
        )
    except BaseException:
        if extractor is not None:
            asyncio.run(extractor.aclose())
        close = getattr(clickhouse_client, "close", None)
        if callable(close):
            close()
        raise
    return BackendComponents(
        server=server,
        extractor=extractor,
        clickhouse_client=clickhouse_client,
        msm_commit=msm_commit,
    )


def require_clean_git_checkout(repo_root: str | Path) -> str:
    """Return the full commit for an exact, clean Git checkout."""

    root = Path(repo_root).resolve()
    if not root.is_dir():
        raise IntegrityError(f"MSM repository does not exist: {root}")
    try:
        top_level = _git(root, "rev-parse", "--show-toplevel")
        commit = _git(root, "rev-parse", "HEAD")
        status = _git(root, "status", "--porcelain", "--untracked-files=normal")
    except subprocess.CalledProcessError as exc:
        raise IntegrityError("MSM source must be a readable Git checkout") from exc
    if Path(top_level).resolve() != root:
        raise IntegrityError("MSM repository root must be the Git top level")
    if status:
        raise IntegrityError("MSM repository must be clean before backend startup")
    invalid_hex = any(character not in "0123456789abcdef" for character in commit)
    if len(commit) not in (40, 64) or invalid_hex:
        raise IntegrityError("MSM Git commit is not a full lowercase object ID")
    return commit


def require_safe_vault(vault_root: str | Path) -> Path:
    """Resolve an existing vault only after rejecting symlinked authority paths."""

    candidate = Path(vault_root).expanduser().absolute()
    if candidate.is_symlink() or not candidate.is_dir():
        raise IntegrityError("production vault root must be an existing regular directory")
    ledger_dir = candidate / ".ledger"
    if os.path.lexists(ledger_dir) and (ledger_dir.is_symlink() or not ledger_dir.is_dir()):
        raise IntegrityError("production vault .ledger path is unsafe")
    return candidate.resolve()


def serve_with_signals(server: BackendServer) -> None:
    """Run Uvicorn off-main-thread so Pine retains graceful signal cleanup."""

    stop_requested = threading.Event()
    failures: list[BaseException] = []

    def run_server() -> None:
        try:
            server.run()
        except BaseException as exc:
            failures.append(exc)

    def request_stop(_signum: int, _frame: Any) -> None:
        stop_requested.set()

    handled_signals = (signal.SIGINT, signal.SIGTERM)
    prior_handlers = {
        handled_signal: signal.signal(handled_signal, request_stop)
        for handled_signal in handled_signals
    }
    worker = threading.Thread(target=run_server, name="pine-backend", daemon=False)
    worker.start()
    try:
        while worker.is_alive():
            if stop_requested.is_set():
                server.stop()
            worker.join(0.1)
    finally:
        if worker.is_alive():
            server.stop()
            worker.join(5.0)
        for handled_signal, prior_handler in prior_handlers.items():
            signal.signal(handled_signal, prior_handler)
    if worker.is_alive():
        raise RuntimeError("backend worker did not stop cleanly")
    if failures:
        raise failures[0]


def install_schemas(source_root: str | Path, destination_root: str | Path) -> None:
    """Install missing versioned schemas and reject mutable or unsafe replacements."""

    source = Path(source_root).expanduser().resolve()
    destination = Path(destination_root).expanduser().resolve()
    if not source.is_dir():
        raise IntegrityError(f"schema source does not exist: {source}")
    schema_paths = sorted(source.rglob("*.json"))
    if not schema_paths:
        raise IntegrityError("schema source contains no JSON schemas")
    destination.mkdir(parents=True, exist_ok=True)
    for schema_path in schema_paths:
        if schema_path.is_symlink() or not schema_path.is_file():
            raise IntegrityError("schema source must contain regular files only")
        if not schema_path.resolve().is_relative_to(source):
            raise IntegrityError("schema source path escapes the schema root")
        relative = schema_path.relative_to(source)
        if any(
            (source / parent).is_symlink() for parent in relative.parents if parent != Path(".")
        ):
            raise IntegrityError("schema source directories cannot be symlinks")
        target = destination / relative
        if target.exists():
            if target.is_symlink() or not target.is_file():
                raise IntegrityError(f"installed schema is not a regular file: {relative}")
            if target.read_bytes() != schema_path.read_bytes():
                raise IntegrityError(f"installed schema differs from release schema: {relative}")
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            dir=target.parent,
            prefix=f".{target.name}.",
            delete=False,
        ) as handle:
            temporary = Path(handle.name)
            try:
                with schema_path.open("rb") as source_handle:
                    shutil.copyfileobj(source_handle, handle)
                handle.flush()
                os.fsync(handle.fileno())
                os.chmod(temporary, 0o600)
                try:
                    os.link(temporary, target)
                except FileExistsError as exc:
                    if target.is_symlink() or not target.is_file():
                        raise IntegrityError(
                            f"installed schema is not a regular file: {relative}"
                        ) from exc
                    if target.read_bytes() != schema_path.read_bytes():
                        raise IntegrityError(
                            f"installed schema differs from release schema: {relative}"
                        ) from exc
                _fsync_directory(target.parent)
            finally:
                temporary.unlink(missing_ok=True)


def check_health(vault_root: str | Path, timeout_seconds: float = 3.0) -> dict[str, Any]:
    """Validate discovery and query the backend without using ambient HTTP proxies."""

    if timeout_seconds <= 0:
        raise ValueError("health timeout must be positive")
    vault = Path(vault_root).expanduser().resolve()
    descriptor = BackendDescriptor.model_validate_json(
        (vault / ".ledger" / "backend.json").read_text(encoding="utf-8")
    )
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open(
        f"http://{descriptor.host}:{descriptor.port}/health",
        timeout=timeout_seconds,
    ) as response:
        payload = json.load(response)
    if payload != {"api_version": "v1", "status": "ok"}:
        raise IntegrityError("backend health response failed validation")
    return {
        "api_version": "v1",
        "instance_id": descriptor.instance_id,
        "pid": descriptor.pid,
        "port": descriptor.port,
        "status": "ok",
    }


def _require_openai_key() -> None:
    key = os.environ.get("OPENAI_API_KEY")
    if key is None or not key.strip():
        raise IntegrityError("OPENAI_API_KEY is required for the production extractor")


def _git(root: Path, *arguments: str) -> str:
    return subprocess.run(
        ("git", "-C", str(root), *arguments),
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def main() -> None:
    """Console-script entry point."""

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    raise SystemExit(run_cli())
