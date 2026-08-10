"""Lifecycle commands for the standalone Pine Research Console process."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Protocol

import uvicorn

from ledger.console.app import create_console_app
from ledger.console.assets import verify_packaged_assets
from ledger.console.backend_client import ConsoleBackend, ConsoleBackendClient
from ledger.console.config import ConsoleConfig
from ledger.console.errors import BackendError, ConsoleError
from ledger.console.migrations import (
    CONSOLE_SCHEMA_VERSION,
    MINIMUM_COMPATIBLE_SCHEMA_VERSION,
)
from ledger.console.state import ConsoleStateStore
from ledger.console.unix_socket import CONSOLE_SOCKET_MODE, secure_unix_socket


class ConsoleRunner(Protocol):
    def __call__(
        self,
        config: ConsoleConfig,
        store: ConsoleStateStore,
        backend: ConsoleBackend,
    ) -> None: ...


def build_parser() -> argparse.ArgumentParser:
    """Build the console lifecycle parser; configuration remains environment-only."""

    parser = argparse.ArgumentParser(
        prog="pine-research-console",
        description="check or run the private Pine Research Console",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser(
        "check",
        help="read-only validation of configuration, state, and backend readiness",
    )
    subparsers.add_parser("serve", help="recover workflows and serve the console Unix socket")
    release = subparsers.add_parser(
        "release-check",
        help="verify console code and assets come from one selected release",
    )
    release.add_argument("--release-root", type=Path, required=True)
    socket_check = subparsers.add_parser(
        "socket-check",
        help="validate configuration and prove a restricted Unix socket can be bound",
    )
    socket_check.add_argument("--socket-path", type=Path, required=True)
    return parser


def run_cli(
    argv: list[str] | None = None,
    *,
    environ: Mapping[str, str] | None = None,
    backend_factory: Callable[[ConsoleConfig], ConsoleBackendClient] = ConsoleBackendClient,
    serve_runner: ConsoleRunner | None = None,
) -> int:
    """Run a console lifecycle command without printing paths, identities, or secrets."""

    args = build_parser().parse_args(argv)
    backend: ConsoleBackendClient | None = None
    try:
        if args.command == "release-check":
            result = verify_packaged_assets(args.release_root)
            print(
                json.dumps(
                    {
                        **result,
                        "console_schema_maximum": CONSOLE_SCHEMA_VERSION,
                        "console_schema_minimum": MINIMUM_COMPATIBLE_SCHEMA_VERSION,
                        "ready": True,
                    },
                    sort_keys=True,
                )
            )
            return 0
        config = ConsoleConfig.from_env(environ)
        logging.basicConfig(
            level=getattr(logging, config.log_level.upper()),
            format="%(asctime)s %(levelname)s %(name)s %(message)s",
        )
        if args.command == "socket-check":
            probe_path = args.socket_path.expanduser().absolute()
            if probe_path.parent.resolve(strict=True) != config.socket_path.parent.resolve(
                strict=True
            ):
                raise ValueError("console socket probe must use the configured runtime directory")
            with secure_unix_socket(probe_path):
                pass
            print(
                json.dumps(
                    {"ready": True, "socket_mode": f"{CONSOLE_SOCKET_MODE:04o}"},
                    sort_keys=True,
                )
            )
            return 0
        store = ConsoleStateStore(
            config.state_path,
            ordinary_retention=config.ordinary_retention,
            receipt_retention=config.receipt_retention,
            migrate_schema=False,
        )
        if args.command == "serve":
            store.recover_abandoned_workflows()
            store.cleanup_expired()
        backend = backend_factory(config)
        health = backend.ready()
        if args.command == "check":
            status = store.get_status()
            print(
                json.dumps(
                    {
                        "backend_api_version": health.api_version,
                        "console_schema_version": status["schema_version"],
                        "ready": True,
                    },
                    sort_keys=True,
                )
            )
            return 0
        (serve_runner or run_console_app)(config, store, backend)
        return 0
    except BackendError:
        print("pine-research-console: backend readiness failed", file=sys.stderr)
        return 2
    except OSError:
        print("pine-research-console: operating system resource unavailable", file=sys.stderr)
        return 2
    except (ConsoleError, ValueError) as exc:
        print(f"pine-research-console: {exc}", file=sys.stderr)
        return 2
    finally:
        if backend is not None:
            backend.close()


def run_console_app(
    config: ConsoleConfig,
    store: ConsoleStateStore,
    backend: ConsoleBackend,
) -> None:
    """Serve the authenticated console exclusively through its Unix socket."""

    app = create_console_app(config, store, backend)
    with secure_unix_socket(config.socket_path) as listener:
        uvicorn.run(
            app,
            fd=listener.fileno(),
            log_level=config.log_level,
            access_log=False,
        )


def main() -> None:
    raise SystemExit(run_cli())


if __name__ == "__main__":
    main()
