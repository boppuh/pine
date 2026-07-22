"""Atomic, verifiable backups for one authoritative Pine vault."""

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
from pathlib import Path, PurePosixPath
from typing import Any

from ledger.errors import IntegrityError, LedgerError
from ledger.json_utils import canonical_json
from ledger.locking import ledger_lock
from ledger.registry import LedgerRegistry

BACKUP_FORMAT_VERSION = 1
_MANAGED_ROOTS = (
    Path(".ledger/schemas"),
    Path(".ledger/snapshots"),
    Path("predictions"),
    Path("runs"),
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pine-ledger-backup",
        description="create and verify atomic Pine vault backups",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    create = subparsers.add_parser("create", help="create and verify one new backup")
    create.add_argument("--vault-root", type=Path, required=True)
    create.add_argument("--backup-root", type=Path, required=True)
    verify = subparsers.add_parser("verify", help="verify a previously published backup")
    verify.add_argument("--backup", type=Path, required=True)
    return parser


def run_cli(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "create":
            result = create_backup(args.vault_root, args.backup_root)
        else:
            result = verify_backup(args.backup)
        print(json.dumps(result, sort_keys=True))
        return 0
    except (LedgerError, OSError, sqlite3.Error, ValueError) as exc:
        print(f"pine-ledger-backup: {exc}", file=sys.stderr)
        return 2


def create_backup(vault_root: str | Path, backup_root: str | Path) -> dict[str, Any]:
    """Create an online-consistent backup and publish it with one directory rename."""

    vault = Path(vault_root).expanduser().resolve()
    destination = Path(backup_root).expanduser().resolve()
    if not vault.is_dir():
        raise IntegrityError(f"vault does not exist: {vault}")
    if destination == vault or destination.is_relative_to(vault):
        raise IntegrityError("backup root must live outside the source vault")
    database = vault / ".ledger" / "registry.db"
    if database.is_symlink() or not database.is_file():
        raise IntegrityError("vault registry is missing or unsafe")

    destination.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(destination, 0o700)
    created_at = datetime.now(UTC)
    nonce = uuid.uuid4().hex
    temporary = destination / f".backup-{nonce}.tmp"
    temporary.mkdir(mode=0o700)
    try:
        with ledger_lock(vault / ".ledger", timeout=30.0):
            registry = LedgerRegistry(database)
            connection = registry.connect()
            try:
                running = connection.execute(
                    "SELECT count(*) FROM runs WHERE state = 'running'"
                ).fetchone()[0]
                if running:
                    raise IntegrityError("cannot back up the vault while an MSM run is active")
                backup_database = temporary / "registry.db"
                backup_connection = sqlite3.connect(backup_database)
                try:
                    connection.backup(backup_connection)
                    result = backup_connection.execute("PRAGMA quick_check").fetchone()
                    if result is None or result[0] != "ok":
                        raise IntegrityError("backup registry failed SQLite quick_check")
                    backup_connection.commit()
                    journal_mode = backup_connection.execute(
                        "PRAGMA journal_mode=DELETE"
                    ).fetchone()
                    if journal_mode is None or str(journal_mode[0]).lower() != "delete":
                        raise IntegrityError("backup registry refused standalone journal mode")
                finally:
                    backup_connection.close()
                os.chmod(backup_database, 0o600)
                _fsync_file(backup_database)
            finally:
                connection.close()

            copied = _copy_managed_files(vault, temporary / "vault")

        registry_entry = _file_evidence(temporary / "registry.db", "registry.db")
        files = [registry_entry, *copied]
        files.sort(key=lambda item: item["relative_path"])
        manifest = {
            "backup_format_version": BACKUP_FORMAT_VERSION,
            "created_at": created_at.isoformat().replace("+00:00", "Z"),
            "files": files,
            "source_vault": str(vault),
        }
        manifest_path = temporary / "manifest.json"
        _write_private_file(manifest_path, (canonical_json(manifest) + "\n").encode())
        _fsync_directory(temporary)
        verify_backup(temporary)

        name = created_at.strftime("pine-%Y%m%dT%H%M%SZ-") + nonce[:12]
        published = destination / name
        os.replace(temporary, published)
        _fsync_directory(destination)
        verified = verify_backup(published)
        return {
            **verified,
            "backup": str(published),
            "created": True,
        }
    except BaseException:
        shutil.rmtree(temporary, ignore_errors=True)
        raise


def verify_backup(backup: str | Path) -> dict[str, Any]:
    """Verify manifest shape, paths, file identities, and SQLite integrity."""

    root = Path(backup).expanduser().resolve()
    manifest_path = root / "manifest.json"
    if manifest_path.is_symlink() or not manifest_path.is_file():
        raise IntegrityError("backup manifest is missing or unsafe")
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise IntegrityError("backup manifest is invalid JSON") from exc
    if not isinstance(manifest, dict) or manifest.get("backup_format_version") != 1:
        raise IntegrityError("backup manifest version is unsupported")
    entries = manifest.get("files")
    if not isinstance(entries, list) or not entries:
        raise IntegrityError("backup manifest contains no files")

    seen: set[str] = set()
    prior = ""
    for entry in entries:
        if not isinstance(entry, dict) or set(entry) != {
            "relative_path",
            "sha256",
            "size_bytes",
        }:
            raise IntegrityError("backup manifest contains an invalid file entry")
        relative = entry["relative_path"]
        if not isinstance(relative, str):
            raise IntegrityError("backup file path must be a string")
        path = PurePosixPath(relative)
        unsafe_part = any(part in ("", ".", "..") for part in path.parts)
        if path.is_absolute() or str(path) != relative or unsafe_part:
            raise IntegrityError("backup file path is unsafe")
        if relative in seen or relative < prior:
            raise IntegrityError("backup file manifest must be unique and sorted")
        seen.add(relative)
        prior = relative
        file_path = root.joinpath(*path.parts)
        if file_path.is_symlink() or not file_path.is_file():
            raise IntegrityError(f"backup file is missing or unsafe: {relative}")
        actual = _file_evidence(file_path, relative)
        if actual != entry:
            raise IntegrityError(f"backup file identity mismatch: {relative}")

    expected_files: set[str] = set()
    for item in root.rglob("*"):
        if item.is_symlink():
            raise IntegrityError("backup directory cannot contain symlinks")
        if item.is_file() and item != manifest_path:
            expected_files.add(item.relative_to(root).as_posix())
    if expected_files != seen:
        raise IntegrityError("backup directory and manifest file sets differ")

    registry = root / "registry.db"
    connection = sqlite3.connect(f"file:{registry}?mode=ro&immutable=1", uri=True)
    try:
        result = connection.execute("PRAGMA quick_check").fetchone()
    finally:
        connection.close()
    if result is None or result[0] != "ok":
        raise IntegrityError("backup registry failed SQLite quick_check")
    return {
        "backup_format_version": BACKUP_FORMAT_VERSION,
        "file_count": len(entries),
        "verified": True,
    }


def _copy_managed_files(vault: Path, destination: Path) -> list[dict[str, Any]]:
    evidence: list[dict[str, Any]] = []
    for managed_root in _MANAGED_ROOTS:
        source_root = vault / managed_root
        if not os.path.lexists(source_root):
            continue
        if source_root.is_symlink() or not source_root.is_dir():
            raise IntegrityError(f"managed vault root is unsafe: {managed_root.as_posix()}")
        for current_root, directories, files in os.walk(source_root, followlinks=False):
            current = Path(current_root)
            for directory in directories:
                if (current / directory).is_symlink():
                    raise IntegrityError("managed vault directories cannot be symlinks")
            for filename in files:
                source = current / filename
                if source.is_symlink():
                    raise IntegrityError("managed vault files cannot be symlinks")
                relative = source.relative_to(vault)
                target_relative = Path("vault") / relative
                target = destination / relative
                target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
                os.chmod(target.parent, 0o700)
                _copy_private_regular_file(source, target)
                evidence.append(_file_evidence(target, target_relative.as_posix()))
    return evidence


def _copy_private_regular_file(source: Path, target: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    source_descriptor = os.open(source, flags)
    try:
        metadata = os.fstat(source_descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise IntegrityError("managed vault files must be regular files")
        target_descriptor = os.open(target, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        try:
            with os.fdopen(source_descriptor, "rb", closefd=False) as source_handle:
                with os.fdopen(target_descriptor, "wb", closefd=False) as target_handle:
                    shutil.copyfileobj(source_handle, target_handle)
                    target_handle.flush()
                    os.fsync(target_descriptor)
        finally:
            os.close(target_descriptor)
    finally:
        os.close(source_descriptor)


def _file_evidence(path: Path, relative_path: str) -> dict[str, Any]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
            size += len(chunk)
    return {
        "relative_path": relative_path,
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
