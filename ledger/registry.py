"""SQLite enforcement authority for prediction and run state."""

from __future__ import annotations

import logging
import sqlite3
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ledger.errors import IntegrityError
from ledger.integrity import CommittedPrediction, PredictionStatus
from ledger.json_utils import canonical_json

logger = logging.getLogger(__name__)

_MIGRATION_VERSION = 1

_MIGRATION_1 = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY,
    applied_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS predictions (
    prediction_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL UNIQUE,
    schema_id TEXT NOT NULL,
    schema_hash TEXT NOT NULL,
    registration_status TEXT NOT NULL CHECK (
        registration_status IN (
            'preregistered', 'exploratory', 'imported', 'unregistered_external'
        )
    ),
    snapshot_ref TEXT NOT NULL,
    lineage_json TEXT NOT NULL,
    status TEXT NOT NULL CHECK (
        status IN ('open', 'resolved', 'invalidated', 'quarantined')
    ),
    outcome_json TEXT,
    grade_json TEXT,
    resolution_metadata_json TEXT,
    transaction_state TEXT NOT NULL CHECK (
        transaction_state IN ('in_progress', 'committed', 'rolled_back')
    ),
    immutable_hash TEXT NOT NULL,
    created_at TEXT NOT NULL,
    committed_at TEXT
);

CREATE TABLE IF NOT EXISTS runs (
    run_id TEXT PRIMARY KEY,
    prediction_id TEXT NOT NULL,
    started_at TEXT NOT NULL,
    state TEXT NOT NULL,
    FOREIGN KEY (prediction_id) REFERENCES predictions(prediction_id) ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS touched_windows (
    family_id TEXT NOT NULL,
    window_start TEXT NOT NULL,
    window_end TEXT NOT NULL,
    touched_at TEXT NOT NULL,
    PRIMARY KEY (family_id, window_start, window_end)
);

CREATE TABLE IF NOT EXISTS integrity_violations (
    prediction_id TEXT NOT NULL,
    field TEXT NOT NULL,
    detected_at TEXT NOT NULL,
    note TEXT NOT NULL,
    FOREIGN KEY (prediction_id) REFERENCES predictions(prediction_id) ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS idx_runs_prediction_id ON runs(prediction_id);
CREATE INDEX IF NOT EXISTS idx_integrity_violations_prediction_id
    ON integrity_violations(prediction_id);

CREATE TRIGGER IF NOT EXISTS predictions_write_once
BEFORE UPDATE OF
    prediction_id,
    run_id,
    schema_id,
    schema_hash,
    registration_status,
    snapshot_ref,
    lineage_json,
    immutable_hash,
    created_at
ON predictions
BEGIN
    SELECT RAISE(ABORT, 'immutable prediction field');
END;

CREATE TRIGGER IF NOT EXISTS predictions_committed_at_write_once
BEFORE UPDATE OF committed_at ON predictions
WHEN OLD.committed_at IS NOT NULL
BEGIN
    SELECT RAISE(ABORT, 'committed_at is write-once');
END;
"""


class LedgerRegistry:
    """Own SQLite schema, transactions, and the only supported lifecycle mutations."""

    def __init__(self, db_path: str | Path = ".ledger/registry.db") -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._migrate()

    def connect(self) -> sqlite3.Connection:
        """Open a configured connection with WAL, foreign keys, and full sync."""

        connection = sqlite3.connect(self.db_path, timeout=30.0, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 30000")
        connection.execute("PRAGMA synchronous = FULL")
        journal_mode = connection.execute("PRAGMA journal_mode = WAL").fetchone()[0]
        if str(journal_mode).lower() != "wal":
            connection.close()
            raise IntegrityError("SQLite registry refused WAL mode")
        return connection

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        """Run an immediate transaction and roll it back on every BaseException."""

        connection = self.connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def get_prediction(
        self,
        prediction_id: str,
        *,
        connection: sqlite3.Connection | None = None,
    ) -> sqlite3.Row | None:
        """Return registry state for a prediction, if present."""

        owns_connection = connection is None
        active = connection or self.connect()
        try:
            return active.execute(
                "SELECT * FROM predictions WHERE prediction_id = ?", (prediction_id,)
            ).fetchone()
        finally:
            if owns_connection:
                active.close()

    def is_committed(self, prediction_id: str) -> bool:
        """Return whether the registry authoritatively committed this prediction."""

        row = self.get_prediction(prediction_id)
        return row is not None and row["transaction_state"] == "committed"

    def begin_prediction(
        self,
        prediction: CommittedPrediction,
        *,
        connection: sqlite3.Connection,
    ) -> None:
        """Stage prediction and run rows inside an existing capture transaction."""

        connection.execute(
            """
            INSERT INTO predictions (
                prediction_id, run_id, schema_id, schema_hash, registration_status,
                snapshot_ref, lineage_json, status, transaction_state, immutable_hash,
                created_at, committed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'in_progress', ?, ?, NULL)
            """,
            (
                prediction.prediction_id,
                prediction.run_id,
                prediction.schema_id,
                prediction.schema_hash,
                prediction.registration_status.value,
                prediction.snapshot_ref,
                canonical_json(prediction.lineage.to_dict()),
                prediction.status.value,
                prediction.immutable_hash,
                prediction.created_at.isoformat(),
            ),
        )
        connection.execute(
            """
            INSERT INTO runs (run_id, prediction_id, started_at, state)
            VALUES (?, ?, ?, 'registered')
            """,
            (prediction.run_id, prediction.prediction_id, prediction.created_at.isoformat()),
        )

    def commit_prediction(
        self,
        prediction_id: str,
        *,
        committed_at: datetime,
        connection: sqlite3.Connection,
    ) -> None:
        """Mark staged state committed after both artifacts have been renamed."""

        cursor = connection.execute(
            """
            UPDATE predictions
            SET transaction_state = 'committed', committed_at = ?
            WHERE prediction_id = ? AND transaction_state = 'in_progress'
            """,
            (committed_at.isoformat(), prediction_id),
        )
        if cursor.rowcount != 1:
            raise IntegrityError(f"prediction is not in progress: {prediction_id}")

    def update_resolution(
        self,
        prediction_id: str,
        *,
        status: PredictionStatus,
        outcome: Mapping[str, Any] | None = None,
        grade: Mapping[str, Any] | None = None,
        resolution_metadata: Mapping[str, Any] | None = None,
    ) -> None:
        """Persist the only mutable record fields through the enforcement authority."""

        with self.transaction() as connection:
            cursor = connection.execute(
                """
                UPDATE predictions
                SET status = ?, outcome_json = ?, grade_json = ?, resolution_metadata_json = ?
                WHERE prediction_id = ? AND transaction_state = 'committed'
                """,
                (
                    status.value,
                    _optional_json(outcome),
                    _optional_json(grade),
                    _optional_json(resolution_metadata),
                    prediction_id,
                ),
            )
            if cursor.rowcount != 1:
                raise IntegrityError(
                    f"cannot resolve unknown or uncommitted prediction: {prediction_id}"
                )

        logger.info(
            "ledger_resolution_updated",
            extra={"prediction_id": prediction_id, "status": status.value},
        )

    def mark_window_touched(
        self,
        family_id: str,
        window_start: str,
        window_end: str,
        *,
        touched_at: datetime | None = None,
    ) -> None:
        """Record an observed research window without overwriting its first touch."""

        timestamp = touched_at or datetime.now(UTC)
        with self.transaction() as connection:
            connection.execute(
                """
                INSERT OR IGNORE INTO touched_windows (
                    family_id, window_start, window_end, touched_at
                ) VALUES (?, ?, ?, ?)
                """,
                (family_id, window_start, window_end, timestamp.isoformat()),
            )

    def is_window_touched(self, family_id: str, window_start: str, window_end: str) -> bool:
        """Return whether a family has already observed the exact data window."""

        connection = self.connect()
        try:
            row = connection.execute(
                """
                SELECT 1 FROM touched_windows
                WHERE family_id = ? AND window_start = ? AND window_end = ?
                """,
                (family_id, window_start, window_end),
            ).fetchone()
            return row is not None
        finally:
            connection.close()

    def record_integrity_violation(
        self,
        prediction_id: str,
        *,
        field: str,
        note: str,
        detected_at: datetime | None = None,
    ) -> None:
        """Append an integrity event; never rewrite the authoritative value."""

        timestamp = detected_at or datetime.now(UTC)
        with self.transaction() as connection:
            connection.execute(
                """
                INSERT INTO integrity_violations (prediction_id, field, detected_at, note)
                VALUES (?, ?, ?, ?)
                """,
                (prediction_id, field, timestamp.isoformat(), note),
            )

    def _migrate(self) -> None:
        connection = self.connect()
        try:
            connection.executescript(
                "BEGIN IMMEDIATE;\n"
                + _MIGRATION_1
                + f"""
                INSERT OR IGNORE INTO schema_migrations (version, applied_at)
                VALUES (
                    {_MIGRATION_VERSION},
                    strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                );
                PRAGMA user_version = {_MIGRATION_VERSION};
                COMMIT;
                """
            )
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()


def _optional_json(value: Mapping[str, Any] | None) -> str | None:
    return None if value is None else canonical_json(dict(value))
