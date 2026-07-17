"""Process-level advisory locking for multi-resource ledger writes."""

from __future__ import annotations

import hashlib
from pathlib import Path

from filelock import FileLock


def ledger_lock(ledger_dir: str | Path, *, timeout: float = 30.0) -> FileLock:
    """Return the shared advisory lock for the ledger capture critical section."""

    directory = Path(ledger_dir)
    directory.mkdir(parents=True, exist_ok=True)
    return FileLock(directory / "registry.lock", timeout=timeout)


def run_execution_lock(
    ledger_dir: str | Path,
    run_id: str,
    *,
    timeout: float = 0.0,
) -> FileLock:
    """Return the process-owned lease used to distinguish live and orphaned runs."""

    directory = Path(ledger_dir) / "run-locks"
    directory.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256(run_id.encode("utf-8")).hexdigest()
    return FileLock(directory / f"{digest}.lock", timeout=timeout)
