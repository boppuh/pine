"""Process-level advisory locking for multi-resource ledger writes."""

from __future__ import annotations

from pathlib import Path

from filelock import FileLock


def ledger_lock(ledger_dir: str | Path, *, timeout: float = 30.0) -> FileLock:
    """Return the shared advisory lock for the ledger capture critical section."""

    directory = Path(ledger_dir)
    directory.mkdir(parents=True, exist_ok=True)
    return FileLock(directory / "registry.lock", timeout=timeout)
