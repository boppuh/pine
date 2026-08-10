"""Verified backup, migration, and compatibility operations for console state."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sqlite3
import stat
import sys
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ledger.console.errors import ConsoleStateError
from ledger.console.migrations import (
    CONSOLE_SCHEMA_VERSION,
    MINIMUM_COMPATIBLE_SCHEMA_VERSION,
    schema_version,
)
from ledger.console.state import ConsoleStateStore
from ledger.json_utils import canonical_json

CONSOLE_BACKUP_FORMAT_VERSION = 1


def build_parser() -> argparse.ArgumentParser:
    """Build the console-state operations parser."""

    parser = argparse.ArgumentParser(
        prog="pine-console-state",
        description="inspect, migrate, back up, and verify Pine console state",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    preflight = subparsers.add_parser(
        "preflight",
        help="read console state without migrating it and enforce release compatibility",
    )
    preflight.add_argument("--state-path", type=Path, required=True)
    preflight.add_argument("--allow-uninitialized", action="store_true")
    preflight.add_argument(
        "--minimum-schema-version",
        type=int,
        default=MINIMUM_COMPATIBLE_SCHEMA_VERSION,
    )
    preflight.add_argument(
        "--maximum-schema-version",
        type=int,
        default=CONSOLE_SCHEMA_VERSION,
    )

    migrate = subparsers.add_parser(
        "migrate",
        help="back up older state before migrating it to this release",
    )
    migrate.add_argument("--state-path", type=Path, required=True)
    migrate.add_argument("--backup-root", type=Path, required=True)

    backup = subparsers.add_parser("backup", help="create and verify an online state backup")
    backup.add_argument("--state-path", type=Path, required=True)
    backup.add_argument("--backup-root", type=Path, required=True)

    verify = subparsers.add_parser("verify", help="verify a published console-state backup")
    verify.add_argument("--backup", type=Path, required=True)
    return parser


def run_cli(argv: list[str] | None = None) -> int:
    """Run one console-state operation and print only non-secret evidence."""

    args = build_parser().parse_args(argv)
    try:
        if args.command == "preflight":
            result = inspect_console_state(
                args.state_path,
                allow_uninitialized=args.allow_uninitialized,
                minimum_schema_version=args.minimum_schema_version,
                maximum_schema_version=args.maximum_schema_version,
            )
        elif args.command == "migrate":
            result = migrate_console_state(args.state_path, args.backup_root)
        elif args.command == "backup":
            result = create_console_state_backup(args.state_path, args.backup_root)
        else:
            result = verify_console_state_backup(args.backup)
        print(json.dumps(result, sort_keys=True))
        return 0
    except (ConsoleStateError, OSError, sqlite3.Error, ValueError) as exc:
        print(f"pine-console-state: {exc}", file=sys.stderr)
        return 2


def inspect_console_state(
    state_path: str | Path,
    *,
    allow_uninitialized: bool = False,
    minimum_schema_version: int = MINIMUM_COMPATIBLE_SCHEMA_VERSION,
    maximum_schema_version: int = CONSOLE_SCHEMA_VERSION,
) -> dict[str, Any]:
    """Inspect state read-only and refuse schemas unsupported by this release."""

    if minimum_schema_version < 1 or maximum_schema_version < minimum_schema_version:
        raise ValueError("console schema compatibility range is invalid")
    path = _state_path(state_path, must_exist=False)
    if not path.exists():
        if not allow_uninitialized:
            raise ConsoleStateError("console state is not initialized")
        return {
            "compatible": True,
            "initialized": False,
            "schema_version": 0,
        }

    connection = _read_only_connection(path)
    try:
        _require_quick_check(connection, "console state")
        version = schema_version(connection)
    finally:
        connection.close()
    if version == 0:
        if not allow_uninitialized:
            raise ConsoleStateError("console state is not initialized")
    elif not minimum_schema_version <= version <= maximum_schema_version:
        raise ConsoleStateError(
            f"console schema {version} is incompatible with this release "
            f"({minimum_schema_version}..{maximum_schema_version})"
        )
    return {
        "compatible": True,
        "initialized": version > 0,
        "schema_version": version,
    }


def migrate_console_state(
    state_path: str | Path,
    backup_root: str | Path,
) -> dict[str, Any]:
    """Back up compatible older state, migrate transactionally, and verify it."""

    before = inspect_console_state(state_path, allow_uninitialized=True)
    backup: dict[str, Any] | None = None
    if before["initialized"] and before["schema_version"] < CONSOLE_SCHEMA_VERSION:
        backup = create_console_state_backup(state_path, backup_root)

    try:
        ConsoleStateStore(state_path)
    except BaseException:
        if backup is not None:
            verify_console_state_backup(backup["backup"])
        raise

    after = inspect_console_state(state_path)
    return {
        "backup": None if backup is None else backup["backup"],
        "backup_sha256": None if backup is None else backup["database_sha256"],
        "from_schema_version": before["schema_version"],
        "migrated": before["schema_version"] != after["schema_version"],
        "schema_version": after["schema_version"],
        "verified": True,
    }


def create_console_state_backup(
    state_path: str | Path,
    backup_root: str | Path,
) -> dict[str, Any]:
    """Create an online-consistent private backup and publish it atomically."""

    source = _state_path(state_path, must_exist=True)
    destination = Path(backup_root).expanduser().absolute()
    if destination.is_symlink():
        raise ConsoleStateError("console backup root must not be a symlink")
    resolved_destination = destination.resolve(strict=False)
    if ".ledger" in resolved_destination.parts:
        raise ConsoleStateError("console backups must live outside the authoritative ledger")
    if source == resolved_destination or resolved_destination.is_relative_to(source.parent):
        raise ConsoleStateError("console backup root must live outside console state")
    destination.mkdir(parents=True, exist_ok=True, mode=0o700)
    destination = destination.resolve(strict=True)
    os.chmod(destination, 0o700)

    created_at = datetime.now(UTC)
    nonce = uuid.uuid4().hex
    temporary = destination / f".console-{nonce}.tmp"
    temporary.mkdir(mode=0o700)
    published: Path | None = None
    try:
        backup_database = temporary / "console.db"
        source_connection = _read_only_connection(source)
        backup_connection = sqlite3.connect(backup_database)
        try:
            source_connection.backup(backup_connection)
            _require_quick_check(backup_connection, "console state backup")
            version = schema_version(backup_connection)
            backup_connection.commit()
            journal = backup_connection.execute("PRAGMA journal_mode=DELETE").fetchone()
            if journal is None or str(journal[0]).lower() != "delete":
                raise ConsoleStateError("console state backup refused standalone journal mode")
        finally:
            backup_connection.close()
            source_connection.close()
        os.chmod(backup_database, 0o600)
        _fsync_file(backup_database)

        evidence = _file_evidence(backup_database)
        manifest = {
            "backup_format_version": CONSOLE_BACKUP_FORMAT_VERSION,
            "created_at": created_at.isoformat().replace("+00:00", "Z"),
            "database": evidence,
            "schema_version": version,
        }
        manifest_path = temporary / "manifest.json"
        _write_private_file(manifest_path, (canonical_json(manifest) + "\n").encode())
        _fsync_directory(temporary)
        verify_console_state_backup(temporary)

        name = created_at.strftime("console-%Y%m%dT%H%M%SZ-") + nonce[:12]
        published = destination / name
        os.replace(temporary, published)
        _fsync_directory(destination)
        verified = verify_console_state_backup(published)
        return {
            **verified,
            "backup": str(published),
            "created": True,
        }
    except BaseException:
        shutil.rmtree(temporary, ignore_errors=True)
        if published is not None and os.path.lexists(published):
            shutil.rmtree(published)
            _fsync_directory(destination)
        raise


def verify_console_state_backup(backup: str | Path) -> dict[str, Any]:
    """Verify a console backup's exact file set, hash, mode, and SQLite state."""

    root = Path(backup).expanduser().absolute()
    if root.is_symlink() or not root.is_dir():
        raise ConsoleStateError("console state backup is missing or unsafe")
    root = root.resolve(strict=True)
    if stat.S_IMODE(root.stat().st_mode) & 0o077:
        raise ConsoleStateError("console state backup directory must be private")
    manifest_path = root / "manifest.json"
    database_path = root / "console.db"
    expected = {manifest_path, database_path}
    actual = {path for path in root.iterdir()}
    if actual != expected or any(path.is_symlink() or not path.is_file() for path in expected):
        raise ConsoleStateError("console state backup file set is invalid")
    if any(stat.S_IMODE(path.stat().st_mode) != 0o600 for path in expected):
        raise ConsoleStateError("console state backup files must have mode 0600")
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ConsoleStateError("console state backup manifest is invalid JSON") from exc
    if not isinstance(manifest, dict) or set(manifest) != {
        "backup_format_version",
        "created_at",
        "database",
        "schema_version",
    }:
        raise ConsoleStateError("console state backup manifest is invalid")
    if manifest["backup_format_version"] != CONSOLE_BACKUP_FORMAT_VERSION:
        raise ConsoleStateError("console state backup version is unsupported")
    try:
        created_at = datetime.fromisoformat(str(manifest["created_at"]).replace("Z", "+00:00"))
    except ValueError as exc:
        raise ConsoleStateError("console state backup timestamp is invalid") from exc
    if created_at.tzinfo is None or created_at.utcoffset() is None:
        raise ConsoleStateError("console state backup timestamp is invalid")
    manifest_version = manifest["schema_version"]
    if not isinstance(manifest_version, int) or isinstance(manifest_version, bool):
        raise ConsoleStateError("console state backup schema version is invalid")
    evidence = manifest["database"]
    if not isinstance(evidence, dict) or evidence != _file_evidence(database_path):
        raise ConsoleStateError("console state backup database identity mismatch")
    connection = _read_only_connection(database_path)
    try:
        _require_quick_check(connection, "console state backup")
        version = schema_version(connection)
    finally:
        connection.close()
    if version != manifest_version:
        raise ConsoleStateError("console state backup schema stamp mismatch")
    return {
        "backup_format_version": CONSOLE_BACKUP_FORMAT_VERSION,
        "database_sha256": evidence["sha256"],
        "schema_version": version,
        "verified": True,
    }


def _state_path(state_path: str | Path, *, must_exist: bool) -> Path:
    candidate = Path(state_path).expanduser().absolute()
    if candidate.is_symlink():
        raise ConsoleStateError("console state path must not be a symlink")
    if ".ledger" in candidate.parts:
        raise ConsoleStateError("console state must live outside the authoritative ledger")
    if not candidate.exists():
        if must_exist:
            raise ConsoleStateError("console state is not initialized")
        resolved = candidate.resolve(strict=False)
        if ".ledger" in resolved.parts:
            raise ConsoleStateError("console state must live outside the authoritative ledger")
        return resolved
    if not candidate.is_file():
        raise ConsoleStateError("console state path must be a regular file")
    if stat.S_IMODE(candidate.stat().st_mode) != 0o600:
        raise ConsoleStateError("console state database must have mode 0600")
    resolved = candidate.resolve(strict=True)
    if ".ledger" in resolved.parts:
        raise ConsoleStateError("console state must live outside the authoritative ledger")
    return resolved


def _read_only_connection(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True, timeout=30.0)
    connection.execute("PRAGMA query_only = ON")
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute("PRAGMA busy_timeout = 30000")
    return connection


def _require_quick_check(connection: sqlite3.Connection, label: str) -> None:
    result = connection.execute("PRAGMA quick_check").fetchone()
    if result is None or result[0] != "ok":
        raise ConsoleStateError(f"{label} failed SQLite quick_check")


def _file_evidence(path: Path) -> dict[str, Any]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
            size += len(chunk)
    return {
        "relative_path": "console.db",
        "sha256": f"sha256:{digest.hexdigest()}",
        "size_bytes": size,
    }


def _write_private_file(path: Path, content: bytes) -> None:
    descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    try:
        with os.fdopen(descriptor, "wb", closefd=False) as handle:
            handle.write(content)
            handle.flush()
            os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _fsync_file(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def main() -> None:
    raise SystemExit(run_cli())


if __name__ == "__main__":
    main()
