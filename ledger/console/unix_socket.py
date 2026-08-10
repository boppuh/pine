"""Restricted Unix-socket binding for the trusted Tailscale ingress boundary."""

from __future__ import annotations

import os
import socket
import stat
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from ledger.console.errors import ConsoleConfigError

CONSOLE_SOCKET_MODE = 0o600


@contextmanager
def secure_unix_socket(path: Path) -> Iterator[socket.socket]:
    """Pre-bind an owner-only socket so Uvicorn cannot widen its permissions."""

    _validate_parent(path.parent)
    try:
        path.lstat()
    except FileNotFoundError:
        pass
    except OSError as exc:
        raise ConsoleConfigError("console socket path could not be inspected") from exc
    else:
        raise ConsoleConfigError("console socket path already exists")

    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    created_identity: tuple[int, int] | None = None
    try:
        listener.bind(str(path))
        bound_metadata = path.lstat()
        if not stat.S_ISSOCK(bound_metadata.st_mode) or bound_metadata.st_uid != os.geteuid():
            raise ConsoleConfigError("console socket ownership is unsafe")
        created_identity = (bound_metadata.st_dev, bound_metadata.st_ino)
        os.chmod(path, CONSOLE_SOCKET_MODE)
        metadata = path.lstat()
        if (
            not stat.S_ISSOCK(metadata.st_mode)
            or stat.S_IMODE(metadata.st_mode) != CONSOLE_SOCKET_MODE
            or metadata.st_uid != os.geteuid()
        ):
            raise ConsoleConfigError("console socket permissions are unsafe")
        yield listener
    except ConsoleConfigError:
        raise
    except OSError as exc:
        raise ConsoleConfigError("console socket could not be bound securely") from exc
    finally:
        listener.close()
        if created_identity is not None:
            _remove_created_socket(path, created_identity)


def _validate_parent(parent: Path) -> None:
    try:
        metadata = parent.lstat()
    except OSError as exc:
        raise ConsoleConfigError("console socket directory is unavailable") from exc
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or stat.S_IMODE(metadata.st_mode) & 0o022
        or metadata.st_uid not in {0, os.geteuid()}
    ):
        raise ConsoleConfigError("console socket directory permissions are unsafe")


def _remove_created_socket(path: Path, identity: tuple[int, int]) -> None:
    try:
        metadata = path.lstat()
        if (metadata.st_dev, metadata.st_ino) == identity and stat.S_ISSOCK(metadata.st_mode):
            path.unlink()
    except FileNotFoundError:
        return
    except OSError:
        return
