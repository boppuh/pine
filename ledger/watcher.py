"""Read-only, debounced filesystem observation for a local ledger vault."""

from __future__ import annotations

import logging
import os
import sqlite3
import threading
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any, Generic, Protocol, TypeVar

import yaml
from watchdog.events import DirMovedEvent, FileMovedEvent, FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer
from watchdog.observers.api import BaseObserver

logger = logging.getLogger(__name__)

_MAX_FRONTMATTER_BYTES = 1024 * 1024
_RUNTIME_LEDGER_FILES = frozenset(
    {
        "registry.db",
        "registry.db-shm",
        "registry.db-wal",
        "registry.lock",
    }
)


class FileChangeKind(StrEnum):
    """Filesystem changes normalized across watchdog platform backends."""

    CREATED = "created"
    MODIFIED = "modified"
    MOVED = "moved"
    DELETED = "deleted"
    REPLACED = "replaced"


class ManagedViolationReason(StrEnum):
    """Actionable reasons a managed ledger path changed unexpectedly."""

    SCHEMA_CHANGED = "schema_changed"
    SNAPSHOT_REWRITTEN = "snapshot_rewritten"
    SNAPSHOT_DELETED = "snapshot_deleted"
    UNREGISTERED_SNAPSHOT = "unregistered_snapshot"
    REGISTRY_REMOVED = "registry_removed"
    REGISTRY_REPLACED = "registry_replaced"
    UNMANAGED_LEDGER_PATH = "unmanaged_ledger_path"


@dataclass(frozen=True, slots=True)
class LedgerRecordEvent:
    """A ledger Markdown record that should be revalidated and re-indexed."""

    path: Path
    prediction_id: str
    change: FileChangeKind
    registered: bool
    observed_at: datetime


@dataclass(frozen=True, slots=True)
class ManagedPathViolation:
    """An unexpected change beneath the watcher-owned ``.ledger`` directory."""

    path: Path
    change: FileChangeKind
    reason: ManagedViolationReason
    observed_at: datetime


class ReindexTrigger(Protocol):
    """Future indexing boundary invoked after a ledger record settles."""

    def __call__(self, event: LedgerRecordEvent, /) -> None:
        """Revalidate and enqueue one ledger record for indexing."""

        ...


class ManagedViolationReporter(Protocol):
    """Reporting boundary for managed-state integrity events."""

    def __call__(self, violation: ManagedPathViolation, /) -> None:
        """Report one unexpected managed-path change without mutating it."""

        ...


@dataclass(frozen=True, slots=True)
class _PendingPathEvent:
    path: Path
    change: FileChangeKind


_EventT = TypeVar("_EventT")


@dataclass(slots=True)
class _PendingTimer(Generic[_EventT]):
    event: _EventT
    timer: threading.Timer


class _Debouncer(Generic[_EventT]):
    """Thread-safe trailing-edge debounce with deterministic shutdown flushing."""

    def __init__(
        self,
        delay_seconds: float,
        dispatch: Callable[[_EventT], None],
        merge: Callable[[_EventT, _EventT], _EventT],
    ) -> None:
        if delay_seconds <= 0:
            raise ValueError("debounce_seconds must be positive")
        self.delay_seconds = delay_seconds
        self.dispatch = dispatch
        self.merge = merge
        self._lock = threading.Lock()
        self._pending: dict[str, _PendingTimer[_EventT]] = {}
        self._closed = False

    def submit(self, key: str, event: _EventT) -> None:
        """Replace one pending timer while retaining meaningful change history."""

        with self._lock:
            if self._closed:
                return
            existing = self._pending.get(key)
            if existing is not None:
                existing.timer.cancel()
                event = self.merge(existing.event, event)
            timer = threading.Timer(self.delay_seconds, self._fire, args=(key,))
            timer.daemon = True
            self._pending[key] = _PendingTimer(event=event, timer=timer)
            timer.start()

    def close(self, *, flush: bool) -> None:
        """Reject new work, cancel timers, and optionally dispatch settled state now."""

        with self._lock:
            if self._closed:
                return
            self._closed = True
            pending = list(self._pending.values())
            self._pending.clear()
            for item in pending:
                item.timer.cancel()
        if flush:
            for item in pending:
                self.dispatch(item.event)

    def _fire(self, key: str) -> None:
        with self._lock:
            pending = self._pending.pop(key, None)
            if pending is None or self._closed:
                return
        self.dispatch(pending.event)


class _VaultEventHandler(FileSystemEventHandler):
    """Route watchdog events through one per-path debounce window."""

    def __init__(
        self,
        vault_root: Path,
        *,
        on_record: ReindexTrigger,
        on_violation: ManagedViolationReporter,
        debounce_seconds: float,
        clock: Callable[[], datetime],
        record_roots: tuple[Path, ...],
    ) -> None:
        super().__init__()
        self.vault_root = vault_root
        self.ledger_dir = vault_root / ".ledger"
        self.registry_path = self.ledger_dir / "registry.db"
        self.on_record = on_record
        self.on_violation = on_violation
        self.clock = clock
        self.record_roots = record_roots
        self._known_lock = threading.Lock()
        self._known_files: dict[Path, tuple[int, int, int]] = {}
        self._debouncer = _Debouncer(
            debounce_seconds,
            self._dispatch,
            _merge_path_events,
        )

    def on_created(self, event: FileSystemEvent) -> None:
        self._submit(event, FileChangeKind.CREATED)

    def on_modified(self, event: FileSystemEvent) -> None:
        self._submit(event, FileChangeKind.MODIFIED)

    def on_deleted(self, event: FileSystemEvent) -> None:
        self._submit(event, FileChangeKind.DELETED)

    def on_moved(self, event: DirMovedEvent | FileMovedEvent) -> None:
        if event.is_directory:
            return
        self._submit_path(event.src_path, FileChangeKind.DELETED)
        self._submit_path(event.dest_path, FileChangeKind.MOVED)

    def close(self, *, flush: bool) -> None:
        self._debouncer.close(flush=flush)

    def seed_baseline(self) -> None:
        """Remember existing files so observer startup replays are not false changes."""

        baseline: dict[Path, tuple[int, int, int]] = {}
        for path in self._baseline_files():
            fingerprint = _file_fingerprint(path)
            if fingerprint is not None:
                baseline[path] = fingerprint
        with self._known_lock:
            self._known_files = baseline

    def reconcile(self) -> None:
        """Synthesize changes missed by a platform's native watchdog backend."""

        current: dict[Path, tuple[int, int, int]] = {}
        for path in self._reconcilable_files():
            fingerprint = _file_fingerprint(path)
            if fingerprint is not None:
                current[path] = fingerprint
        with self._known_lock:
            previous = {
                path: fingerprint
                for path, fingerprint in self._known_files.items()
                if self._is_reconcilable(path)
            }
        for path, fingerprint in current.items():
            if previous.get(path) != fingerprint:
                change = FileChangeKind.CREATED if path not in previous else FileChangeKind.MODIFIED
                self._submit_path(str(path), change)
        for path in previous.keys() - current.keys():
            self._submit_path(str(path), FileChangeKind.DELETED)

    def _baseline_files(self) -> set[Path]:
        files = {
            path
            for path in self.vault_root.rglob("*.md")
            if not path.is_relative_to(self.ledger_dir)
        }
        if self.ledger_dir.is_dir():
            files.update(path for path in self.ledger_dir.rglob("*") if path.is_file())
        return files

    def _reconcilable_files(self) -> set[Path]:
        files: set[Path] = set()
        for root in self.record_roots:
            if root.is_dir():
                files.update(root.rglob("*.md"))
        if self.ledger_dir.is_dir():
            files.update(path for path in self.ledger_dir.rglob("*") if path.is_file())
        return files

    def _is_reconcilable(self, path: Path) -> bool:
        return path.is_relative_to(self.ledger_dir) or any(
            path.is_relative_to(root) for root in self.record_roots
        )

    def _submit(self, event: FileSystemEvent, change: FileChangeKind) -> None:
        if not event.is_directory:
            self._submit_path(event.src_path, change)

    def _submit_path(self, raw_path: str | bytes, change: FileChangeKind) -> None:
        path = Path(os.path.abspath(os.fsdecode(raw_path)))
        if not path.is_relative_to(self.vault_root):
            return
        if not path.is_relative_to(self.ledger_dir) and path.suffix.lower() != ".md":
            return
        normalized = self._normalize_change(path, change)
        if normalized is None:
            return
        self._debouncer.submit(str(path), _PendingPathEvent(path=path, change=normalized))

    def _normalize_change(
        self,
        path: Path,
        change: FileChangeKind,
    ) -> FileChangeKind | None:
        fingerprint = _file_fingerprint(path)
        with self._known_lock:
            previous = self._known_files.get(path)
            if change is FileChangeKind.DELETED or fingerprint is None:
                self._known_files.pop(path, None)
                return FileChangeKind.DELETED if previous is not None else None
            if previous == fingerprint:
                return None
            self._known_files[path] = fingerprint
        if previous is None:
            return FileChangeKind.CREATED if change is not FileChangeKind.MOVED else change
        return FileChangeKind.MODIFIED

    def _dispatch(self, pending: _PendingPathEvent) -> None:
        try:
            if pending.path.is_relative_to(self.ledger_dir):
                self._dispatch_managed(pending)
            else:
                self._dispatch_record(pending)
        except Exception:
            logger.exception(
                "vault_watcher_dispatch_failed",
                extra={"path": str(pending.path), "change": pending.change.value},
            )

    def _dispatch_record(self, pending: _PendingPathEvent) -> None:
        candidate_id = _frontmatter_prediction_id(pending.path)
        registered_id = self._registered_prediction_id(pending.path.stem, candidate_id)
        if registered_id is None and candidate_id is None:
            return
        prediction_id = registered_id or candidate_id
        assert prediction_id is not None
        event = LedgerRecordEvent(
            path=pending.path,
            prediction_id=prediction_id,
            change=pending.change,
            registered=registered_id is not None,
            observed_at=self._clock_time(),
        )
        try:
            self.on_record(event)
        except Exception:
            logger.exception(
                "vault_watcher_reindex_callback_failed",
                extra={"path": str(event.path), "prediction_id": event.prediction_id},
            )
            return
        logger.info(
            "vault_ledger_record_observed",
            extra={
                "path": str(event.path),
                "prediction_id": event.prediction_id,
                "change": event.change.value,
                "registered": event.registered,
            },
        )

    def _dispatch_managed(self, pending: _PendingPathEvent) -> None:
        relative = pending.path.relative_to(self.ledger_dir)
        if not relative.parts:
            return
        top_level = relative.parts[0]
        if top_level == "run-locks" or _is_internal_temporary(relative):
            return
        if len(relative.parts) == 1 and top_level in _RUNTIME_LEDGER_FILES:
            if top_level == "registry.db":
                if pending.change is FileChangeKind.DELETED:
                    self._report_violation(pending, ManagedViolationReason.REGISTRY_REMOVED)
                elif pending.change is FileChangeKind.REPLACED:
                    self._report_violation(pending, ManagedViolationReason.REGISTRY_REPLACED)
            return
        if top_level == "schemas":
            self._report_violation(pending, ManagedViolationReason.SCHEMA_CHANGED)
            return
        if top_level == "snapshots":
            self._dispatch_snapshot(pending)
            return
        self._report_violation(pending, ManagedViolationReason.UNMANAGED_LEDGER_PATH)

    def _dispatch_snapshot(self, pending: _PendingPathEvent) -> None:
        if pending.change is FileChangeKind.DELETED:
            self._report_violation(pending, ManagedViolationReason.SNAPSHOT_DELETED)
            return
        if pending.change in (FileChangeKind.MODIFIED, FileChangeKind.REPLACED):
            self._report_violation(pending, ManagedViolationReason.SNAPSHOT_REWRITTEN)
            return
        if not pending.path.exists():
            with self._known_lock:
                self._known_files.pop(pending.path, None)
            return
        if pending.path.suffix.lower() != ".json" or not self._snapshot_is_registered(pending.path):
            self._report_violation(pending, ManagedViolationReason.UNREGISTERED_SNAPSHOT)

    def _registered_prediction_id(
        self,
        filename_id: str,
        frontmatter_id: str | None,
    ) -> str | None:
        candidates = dict.fromkeys(
            value for value in (filename_id, frontmatter_id) if value is not None
        )
        for prediction_id in candidates:
            row = self._prediction_row(prediction_id)
            if row is not None and row["transaction_state"] == "committed":
                return prediction_id
        return None

    def _snapshot_is_registered(self, path: Path) -> bool:
        row = self._prediction_row(path.stem)
        if row is None or row["transaction_state"] != "committed":
            return False
        expected_ref = path.relative_to(self.vault_root).as_posix()
        return row["snapshot_ref"] == expected_ref

    def _prediction_row(self, prediction_id: str) -> sqlite3.Row | None:
        if not self.registry_path.is_file():
            return None
        try:
            connection = sqlite3.connect(
                f"{self.registry_path.resolve().as_uri()}?mode=ro",
                uri=True,
                timeout=1.0,
            )
            connection.row_factory = sqlite3.Row
            try:
                return connection.execute(
                    """
                    SELECT prediction_id, snapshot_ref, transaction_state
                    FROM predictions
                    WHERE prediction_id = ?
                    """,
                    (prediction_id,),
                ).fetchone()
            finally:
                connection.close()
        except sqlite3.Error:
            logger.exception(
                "vault_watcher_registry_read_failed",
                extra={"prediction_id": prediction_id},
            )
            return None

    def _report_violation(
        self,
        pending: _PendingPathEvent,
        reason: ManagedViolationReason,
    ) -> None:
        violation = ManagedPathViolation(
            path=pending.path,
            change=pending.change,
            reason=reason,
            observed_at=self._clock_time(),
        )
        try:
            self.on_violation(violation)
        except Exception:
            logger.exception(
                "vault_watcher_violation_callback_failed",
                extra={"path": str(violation.path), "reason": violation.reason.value},
            )
            return
        logger.warning(
            "vault_managed_path_violation",
            extra={
                "path": str(violation.path),
                "change": violation.change.value,
                "reason": violation.reason.value,
            },
        )

    def _clock_time(self) -> datetime:
        value = self.clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("watcher clock must return a timezone-aware value")
        return value


class VaultWatcher:
    """Own the lifecycle of a recursive, read-only watchdog observer."""

    def __init__(
        self,
        vault_root: str | Path,
        *,
        on_record: ReindexTrigger,
        on_violation: ManagedViolationReporter,
        debounce_seconds: float = 0.25,
        reconcile_interval: float = 0.5,
        record_roots: Sequence[str | Path] | None = None,
        clock: Callable[[], datetime] | None = None,
        observer: BaseObserver | None = None,
    ) -> None:
        self.vault_root = Path(vault_root).resolve()
        if not self.vault_root.is_dir():
            raise ValueError(f"vault root does not exist: {self.vault_root}")
        if reconcile_interval <= 0:
            raise ValueError("reconcile_interval must be positive")
        self.clock = clock or (lambda: datetime.now(UTC))
        self.reconcile_interval = reconcile_interval
        roots = (Path("predictions"),) if record_roots is None else record_roots
        resolved_roots = tuple(
            (Path(root) if Path(root).is_absolute() else self.vault_root / root).resolve()
            for root in roots
        )
        if not resolved_roots:
            raise ValueError("record_roots must contain at least one directory")
        ledger_dir = self.vault_root / ".ledger"
        if any(
            not root.is_relative_to(self.vault_root) or root.is_relative_to(ledger_dir)
            for root in resolved_roots
        ):
            raise ValueError("record_roots must live inside the vault and outside .ledger")
        self._handler = _VaultEventHandler(
            self.vault_root,
            on_record=on_record,
            on_violation=on_violation,
            debounce_seconds=debounce_seconds,
            clock=self.clock,
            record_roots=resolved_roots,
        )
        self._observer = observer or Observer()
        self._reconcile_stop = threading.Event()
        self._reconcile_thread: threading.Thread | None = None
        self._started = False
        self._stopped = False

    @property
    def is_running(self) -> bool:
        """Return whether the underlying observer thread is alive."""

        return self._started and not self._stopped and self._observer.is_alive()

    def start(self) -> None:
        """Start recursive observation exactly once."""

        if self._started:
            raise RuntimeError("vault watcher has already been started")
        if self._stopped:
            raise RuntimeError("a stopped vault watcher cannot be restarted")
        self._handler.seed_baseline()
        try:
            self._observer.schedule(self._handler, str(self.vault_root), recursive=True)
            self._observer.start()
            self._started = True
            self._reconcile_thread = threading.Thread(
                target=self._reconcile_loop,
                name="ledger-vault-reconciler",
                daemon=True,
            )
            self._reconcile_thread.start()
        except BaseException:
            self._stopped = True
            self._reconcile_stop.set()
            if self._observer.is_alive():
                self._observer.stop()
                self._observer.join(5.0)
            self._handler.close(flush=False)
            raise
        logger.info("vault_watcher_started", extra={"vault_root": str(self.vault_root)})

    def stop(self, *, flush: bool = True, timeout: float = 5.0) -> None:
        """Stop observation and optionally flush the final debounced events."""

        if self._stopped:
            return
        if self._started:
            self._reconcile_stop.set()
            if self._reconcile_thread is not None:
                self._reconcile_thread.join(timeout)
            self._observer.stop()
            self._observer.join(timeout)
            reconciler_alive = (
                self._reconcile_thread is not None and self._reconcile_thread.is_alive()
            )
            observer_alive = self._observer.is_alive()
            if reconciler_alive or observer_alive:
                raise RuntimeError("vault watcher did not stop before the timeout")
            self._handler.close(flush=flush)
        else:
            self._handler.close(flush=flush)
        self._stopped = True
        logger.info("vault_watcher_stopped", extra={"vault_root": str(self.vault_root)})

    def _reconcile_loop(self) -> None:
        while not self._reconcile_stop.wait(self.reconcile_interval):
            try:
                self._handler.reconcile()
            except Exception:
                logger.exception(
                    "vault_watcher_reconciliation_failed",
                    extra={"vault_root": str(self.vault_root)},
                )

    def __enter__(self) -> VaultWatcher:
        self.start()
        return self

    def __exit__(self, *_exc: Any) -> None:
        self.stop()


def _frontmatter_prediction_id(path: Path) -> str | None:
    if not path.is_file():
        return None
    try:
        with path.open("rb") as handle:
            raw = handle.read(_MAX_FRONTMATTER_BYTES + 1)
    except OSError:
        return None
    if len(raw) > _MAX_FRONTMATTER_BYTES:
        return None
    try:
        content = raw.decode("utf-8").replace("\r\n", "\n")
        if not content.startswith("---\n"):
            return None
        parts = content.split("---\n", maxsplit=2)
        if len(parts) != 3:
            return None
        frontmatter = yaml.safe_load(parts[1])
    except (UnicodeDecodeError, yaml.YAMLError):
        return None
    if not isinstance(frontmatter, dict) or frontmatter.get("type") != "prediction":
        return None
    prediction_id = frontmatter.get("id")
    if not isinstance(prediction_id, str) or not prediction_id.strip():
        return None
    return prediction_id


def _merge_path_events(
    previous: _PendingPathEvent,
    current: _PendingPathEvent,
) -> _PendingPathEvent:
    if previous.path != current.path:
        raise ValueError("cannot merge events for different paths")
    if current.change is FileChangeKind.DELETED:
        change = FileChangeKind.DELETED
    elif previous.change is FileChangeKind.DELETED:
        change = FileChangeKind.REPLACED
    elif previous.change is FileChangeKind.CREATED:
        change = FileChangeKind.CREATED
    else:
        change = current.change
    return _PendingPathEvent(path=current.path, change=change)


def _is_internal_temporary(relative: Path) -> bool:
    name = relative.name
    return name.startswith(".txn-") or name.endswith((".tmp", ".manifest-tmp", ".lock"))


def _file_fingerprint(path: Path) -> tuple[int, int, int] | None:
    try:
        stat = path.stat(follow_symlinks=False)
    except (FileNotFoundError, OSError):
        return None
    if not path.is_file():
        return None
    return stat.st_ino, stat.st_size, stat.st_mtime_ns
