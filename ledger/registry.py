"""SQLite enforcement authority for prediction and run state."""

from __future__ import annotations

import json
import logging
import sqlite3
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from ledger.errors import IntegrityError
from ledger.integrity import CommittedPrediction, PredictionStatus
from ledger.json_utils import canonical_json, sha256_json

logger = logging.getLogger(__name__)

_MIGRATION_VERSION = 7

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

_MIGRATION_2 = """
CREATE TABLE IF NOT EXISTS capture_requests (
    idempotency_key TEXT PRIMARY KEY,
    request_hash TEXT NOT NULL,
    prediction_id TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL,
    FOREIGN KEY (prediction_id) REFERENCES predictions(prediction_id) ON DELETE RESTRICT
);

CREATE TRIGGER IF NOT EXISTS capture_requests_write_once_update
BEFORE UPDATE ON capture_requests
BEGIN
    SELECT RAISE(ABORT, 'capture request identity is write-once');
END;

CREATE TRIGGER IF NOT EXISTS capture_requests_write_once_delete
BEFORE DELETE ON capture_requests
BEGIN
    SELECT RAISE(ABORT, 'capture request identity is permanent');
END;

CREATE INDEX IF NOT EXISTS idx_touched_windows_overlap
    ON touched_windows(family_id, window_start, window_end);
"""

_MIGRATION_3 = """
ALTER TABLE runs RENAME TO runs_v2;

CREATE TABLE runs (
    run_id TEXT PRIMARY KEY,
    prediction_id TEXT UNIQUE,
    started_at TEXT NOT NULL,
    state TEXT NOT NULL CHECK (
        state IN ('registered', 'running', 'completed', 'failed')
    ),
    execution_started_at TEXT,
    completed_at TEXT,
    exit_code INTEGER,
    failure_note TEXT,
    FOREIGN KEY (prediction_id) REFERENCES predictions(prediction_id) ON DELETE RESTRICT
);

INSERT INTO runs (run_id, prediction_id, started_at, state)
SELECT run_id, prediction_id, started_at, state FROM runs_v2;

DROP TABLE runs_v2;

CREATE INDEX idx_runs_prediction_id ON runs(prediction_id);

CREATE TABLE run_bindings (
    run_id TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    request_hash TEXT NOT NULL,
    registration_status TEXT NOT NULL CHECK (
        registration_status IN ('preregistered', 'exploratory')
    ),
    strategy_id TEXT NOT NULL,
    dataset_version TEXT NOT NULL,
    envelope_json TEXT NOT NULL,
    envelope_hash TEXT NOT NULL,
    bound_at TEXT NOT NULL,
    FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE RESTRICT
);

CREATE TRIGGER runs_identity_write_once_update
BEFORE UPDATE OF run_id, prediction_id, started_at ON runs
BEGIN
    SELECT RAISE(ABORT, 'run identity is write-once');
END;

CREATE TRIGGER runs_lifecycle_transitions
BEFORE UPDATE ON runs
WHEN NOT (
    (
        OLD.state = 'registered'
        AND NEW.state = 'running'
        AND OLD.execution_started_at IS NULL
        AND NEW.execution_started_at IS NOT NULL
        AND NEW.completed_at IS NULL
        AND NEW.exit_code IS NULL
        AND NEW.failure_note IS NULL
    )
    OR
    (
        OLD.state = 'running'
        AND NEW.state IN ('completed', 'failed')
        AND NEW.execution_started_at = OLD.execution_started_at
        AND NEW.completed_at IS NOT NULL
        AND NEW.exit_code IS NOT NULL
        AND (
            (NEW.state = 'completed' AND NEW.exit_code = 0 AND NEW.failure_note IS NULL)
            OR
            (NEW.state = 'failed' AND (NEW.exit_code != 0 OR NEW.failure_note IS NOT NULL))
        )
    )
)
BEGIN
    SELECT RAISE(ABORT, 'invalid run lifecycle transition');
END;

CREATE TRIGGER runs_permanent_delete
BEFORE DELETE ON runs
BEGIN
    SELECT RAISE(ABORT, 'run identity is permanent');
END;

CREATE TRIGGER run_bindings_write_once_update
BEFORE UPDATE ON run_bindings
BEGIN
    SELECT RAISE(ABORT, 'run binding is write-once');
END;

CREATE TRIGGER run_bindings_permanent_delete
BEFORE DELETE ON run_bindings
BEGIN
    SELECT RAISE(ABORT, 'run binding is permanent');
END;
"""

_MIGRATION_4 = """
DROP TRIGGER run_bindings_write_once_update;
DROP TRIGGER run_bindings_permanent_delete;

ALTER TABLE run_bindings RENAME TO run_bindings_v3;

CREATE TABLE run_bindings (
    run_id TEXT PRIMARY KEY,
    idempotency_key TEXT NOT NULL UNIQUE,
    request_hash TEXT NOT NULL,
    registration_status TEXT NOT NULL CHECK (
        registration_status IN (
            'preregistered', 'exploratory', 'unregistered_external'
        )
    ),
    strategy_id TEXT NOT NULL,
    dataset_version TEXT NOT NULL,
    envelope_json TEXT NOT NULL,
    envelope_hash TEXT NOT NULL,
    bound_at TEXT NOT NULL,
    FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE RESTRICT
);

INSERT INTO run_bindings (
    run_id, idempotency_key, request_hash, registration_status,
    strategy_id, dataset_version, envelope_json, envelope_hash, bound_at
)
SELECT
    run_id, idempotency_key, request_hash, registration_status,
    strategy_id, dataset_version, envelope_json, envelope_hash, bound_at
FROM run_bindings_v3;

DROP TABLE run_bindings_v3;

CREATE TRIGGER run_bindings_write_once_update
BEFORE UPDATE ON run_bindings
BEGIN
    SELECT RAISE(ABORT, 'run binding is write-once');
END;

CREATE TRIGGER run_bindings_permanent_delete
BEFORE DELETE ON run_bindings
BEGIN
    SELECT RAISE(ABORT, 'run binding is permanent');
END;

CREATE TABLE external_run_imports (
    run_id TEXT PRIMARY KEY,
    source_system TEXT NOT NULL,
    source_run_id TEXT NOT NULL,
    evidence_hash TEXT NOT NULL UNIQUE,
    ingested_at TEXT NOT NULL,
    UNIQUE (source_system, source_run_id),
    FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE RESTRICT
);

CREATE TRIGGER external_run_imports_require_low_integrity_terminal_run
BEFORE INSERT ON external_run_imports
WHEN NOT EXISTS (
    SELECT 1
    FROM runs
    JOIN run_bindings USING (run_id)
    WHERE runs.run_id = NEW.run_id
      AND runs.prediction_id IS NULL
      AND runs.state IN ('completed', 'failed')
      AND run_bindings.registration_status = 'unregistered_external'
)
BEGIN
    SELECT RAISE(ABORT, 'external import requires a terminal low-integrity run');
END;

CREATE TRIGGER external_run_imports_write_once_update
BEFORE UPDATE ON external_run_imports
BEGIN
    SELECT RAISE(ABORT, 'external run evidence is write-once');
END;

CREATE TRIGGER external_run_imports_permanent_delete
BEFORE DELETE ON external_run_imports
BEGIN
    SELECT RAISE(ABORT, 'external run evidence is permanent');
END;
"""

_MIGRATION_5 = """
CREATE TRIGGER predictions_quarantine_requires_evidence
BEFORE UPDATE OF status ON predictions
WHEN NEW.status = 'quarantined'
  AND OLD.status != 'quarantined'
  AND NOT EXISTS (
      SELECT 1 FROM integrity_violations
      WHERE prediction_id = OLD.prediction_id
  )
BEGIN
    SELECT RAISE(ABORT, 'quarantine requires integrity violation evidence');
END;

CREATE TRIGGER predictions_quarantine_terminal
BEFORE UPDATE OF status ON predictions
WHEN OLD.status = 'quarantined' AND NEW.status != 'quarantined'
BEGIN
    SELECT RAISE(ABORT, 'quarantined prediction status is terminal');
END;

CREATE TRIGGER integrity_violations_write_once_update
BEFORE UPDATE ON integrity_violations
BEGIN
    SELECT RAISE(ABORT, 'integrity violation evidence is write-once');
END;

CREATE TRIGGER integrity_violations_permanent_delete
BEFORE DELETE ON integrity_violations
BEGIN
    SELECT RAISE(ABORT, 'integrity violation evidence is permanent');
END;
"""

_MIGRATION_6 = """
CREATE TEMP TABLE migration_6_preregistered_guard (
    transaction_state TEXT NOT NULL CHECK (transaction_state = 'committed'),
    family_type TEXT NOT NULL CHECK (family_type = 'text'),
    family_id TEXT NOT NULL CHECK (
        LENGTH(TRIM(family_id)) BETWEEN 1 AND 256
        AND INSTR(family_id, CHAR(0)) = 0
    )
);

INSERT INTO migration_6_preregistered_guard (
    transaction_state, family_type, family_id
)
SELECT
    predictions.transaction_state,
    json_type(predictions.lineage_json, '$.family_id'),
    json_extract(predictions.lineage_json, '$.family_id')
FROM runs
JOIN run_bindings USING (run_id)
LEFT JOIN predictions USING (prediction_id)
WHERE runs.execution_started_at IS NOT NULL
  AND run_bindings.registration_status = 'preregistered';

DROP TABLE migration_6_preregistered_guard;

INSERT OR IGNORE INTO touched_windows (
    family_id, window_start, window_end, touched_at
)
SELECT
    CASE
        WHEN run_bindings.registration_status = 'preregistered'
        THEN TRIM(json_extract(predictions.lineage_json, '$.family_id'))
        ELSE TRIM(run_bindings.strategy_id)
    END,
    json_extract(
        run_bindings.envelope_json,
        '$.snapshot.out_of_sample_window.start'
    ),
    json_extract(
        run_bindings.envelope_json,
        '$.snapshot.out_of_sample_window.end'
    ),
    runs.execution_started_at
FROM runs
JOIN run_bindings USING (run_id)
LEFT JOIN predictions USING (prediction_id)
WHERE runs.execution_started_at IS NOT NULL
  AND json_valid(run_bindings.envelope_json)
  AND json_type(
      run_bindings.envelope_json,
      '$.snapshot.out_of_sample_window.start'
  ) = 'text'
  AND json_type(
      run_bindings.envelope_json,
      '$.snapshot.out_of_sample_window.end'
  ) = 'text';

CREATE TRIGGER touched_windows_write_once_update
BEFORE UPDATE ON touched_windows
BEGIN
    SELECT RAISE(ABORT, 'touched window evidence is write-once');
END;

CREATE TRIGGER touched_windows_permanent_delete
BEFORE DELETE ON touched_windows
BEGIN
    SELECT RAISE(ABORT, 'touched window evidence is permanent');
END;
"""

_MIGRATION_7 = """
CREATE TABLE run_results (
    run_id TEXT PRIMARY KEY,
    evidence_hash TEXT NOT NULL UNIQUE,
    evidence_json TEXT NOT NULL CHECK (json_valid(evidence_json)),
    source_timestamp TEXT NOT NULL,
    ingested_at TEXT NOT NULL,
    FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE RESTRICT
);

CREATE TRIGGER run_results_require_successful_bound_run
BEFORE INSERT ON run_results
WHEN NOT EXISTS (
    SELECT 1
    FROM runs
    JOIN run_bindings USING (run_id)
    WHERE runs.run_id = NEW.run_id
      AND runs.state = 'completed'
      AND runs.exit_code = 0
      AND runs.failure_note IS NULL
)
BEGIN
    SELECT RAISE(ABORT, 'result evidence requires a successful bound run');
END;

CREATE TRIGGER run_results_write_once_update
BEFORE UPDATE ON run_results
BEGIN
    SELECT RAISE(ABORT, 'run result evidence is write-once');
END;

CREATE TRIGGER run_results_permanent_delete
BEFORE DELETE ON run_results
BEGIN
    SELECT RAISE(ABORT, 'run result evidence is permanent');
END;

CREATE INDEX idx_run_results_source_timestamp
    ON run_results(source_timestamp);
"""

_MIGRATIONS = {
    1: _MIGRATION_1,
    2: _MIGRATION_2,
    3: _MIGRATION_3,
    4: _MIGRATION_4,
    5: _MIGRATION_5,
    6: _MIGRATION_6,
    7: _MIGRATION_7,
}


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

    def get_prediction_by_idempotency_key(
        self,
        idempotency_key: str,
        *,
        connection: sqlite3.Connection | None = None,
    ) -> sqlite3.Row | None:
        """Return the prediction and request hash assigned to an idempotency key."""

        owns_connection = connection is None
        active = connection or self.connect()
        try:
            return active.execute(
                """
                SELECT predictions.*, capture_requests.request_hash,
                       capture_requests.idempotency_key
                FROM capture_requests
                JOIN predictions USING (prediction_id)
                WHERE capture_requests.idempotency_key = ?
                """,
                (idempotency_key,),
            ).fetchone()
        finally:
            if owns_connection:
                active.close()

    def get_run(
        self,
        run_id: str,
        *,
        connection: sqlite3.Connection | None = None,
    ) -> sqlite3.Row | None:
        """Return one run joined to its immutable execution binding, if present."""

        owns_connection = connection is None
        active = connection or self.connect()
        try:
            return active.execute(
                """
                SELECT runs.*, run_bindings.idempotency_key,
                       run_bindings.request_hash, run_bindings.registration_status,
                       run_bindings.strategy_id, run_bindings.dataset_version,
                       run_bindings.envelope_json, run_bindings.envelope_hash,
                       run_bindings.bound_at
                FROM runs
                LEFT JOIN run_bindings USING (run_id)
                WHERE runs.run_id = ?
                """,
                (run_id,),
            ).fetchone()
        finally:
            if owns_connection:
                active.close()

    def get_run_by_prediction_id(
        self,
        prediction_id: str,
        *,
        connection: sqlite3.Connection | None = None,
    ) -> sqlite3.Row | None:
        """Return the preallocated run for a prediction."""

        owns_connection = connection is None
        active = connection or self.connect()
        try:
            return active.execute(
                "SELECT * FROM runs WHERE prediction_id = ?",
                (prediction_id,),
            ).fetchone()
        finally:
            if owns_connection:
                active.close()

    def get_run_by_idempotency_key(
        self,
        idempotency_key: str,
        *,
        connection: sqlite3.Connection | None = None,
    ) -> sqlite3.Row | None:
        """Return a run and immutable binding assigned to an execution request."""

        owns_connection = connection is None
        active = connection or self.connect()
        try:
            return active.execute(
                """
                SELECT runs.*, run_bindings.idempotency_key,
                       run_bindings.request_hash, run_bindings.registration_status,
                       run_bindings.strategy_id, run_bindings.dataset_version,
                       run_bindings.envelope_json, run_bindings.envelope_hash,
                       run_bindings.bound_at
                FROM run_bindings
                JOIN runs USING (run_id)
                WHERE run_bindings.idempotency_key = ?
                """,
                (idempotency_key,),
            ).fetchone()
        finally:
            if owns_connection:
                active.close()

    def get_external_run_import(
        self,
        source_system: str,
        source_run_id: str,
        *,
        connection: sqlite3.Connection | None = None,
    ) -> sqlite3.Row | None:
        """Return a retroactively imported run by its permanent source identity."""

        owns_connection = connection is None
        active = connection or self.connect()
        try:
            return active.execute(
                """
                SELECT runs.*, run_bindings.idempotency_key,
                       run_bindings.request_hash, run_bindings.registration_status,
                       run_bindings.strategy_id, run_bindings.dataset_version,
                       run_bindings.envelope_json, run_bindings.envelope_hash,
                       run_bindings.bound_at, external_run_imports.source_system,
                       external_run_imports.source_run_id,
                       external_run_imports.evidence_hash,
                       external_run_imports.ingested_at
                FROM external_run_imports
                JOIN runs USING (run_id)
                JOIN run_bindings USING (run_id)
                WHERE external_run_imports.source_system = ?
                  AND external_run_imports.source_run_id = ?
                """,
                (source_system, source_run_id),
            ).fetchone()
        finally:
            if owns_connection:
                active.close()

    def get_run_result(
        self,
        run_id: str,
        *,
        connection: sqlite3.Connection | None = None,
    ) -> sqlite3.Row | None:
        """Return immutable result evidence previously ingested for one run."""

        owns_connection = connection is None
        active = connection or self.connect()
        try:
            return active.execute(
                "SELECT * FROM run_results WHERE run_id = ?",
                (run_id,),
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

    def register_capture_request(
        self,
        *,
        idempotency_key: str,
        request_hash: str,
        prediction_id: str,
        created_at: datetime,
        connection: sqlite3.Connection,
    ) -> None:
        """Bind a successful capture identity inside its prediction transaction."""

        connection.execute(
            """
            INSERT INTO capture_requests (
                idempotency_key, request_hash, prediction_id, created_at
            ) VALUES (?, ?, ?, ?)
            """,
            (idempotency_key, request_hash, prediction_id, created_at.isoformat()),
        )

    def create_exploratory_run(
        self,
        *,
        run_id: str,
        started_at: datetime,
        idempotency_key: str,
        request_hash: str,
        strategy_id: str,
        dataset_version: str,
        envelope_json: str,
        envelope_hash: str,
        connection: sqlite3.Connection,
    ) -> None:
        """Create an exploratory run and its permanent binding in one transaction."""

        connection.execute(
            """
            INSERT INTO runs (run_id, prediction_id, started_at, state)
            VALUES (?, NULL, ?, 'registered')
            """,
            (run_id, started_at.isoformat()),
        )
        self._bind_run(
            run_id=run_id,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            registration_status="exploratory",
            strategy_id=strategy_id,
            dataset_version=dataset_version,
            envelope_json=envelope_json,
            envelope_hash=envelope_hash,
            bound_at=started_at,
            connection=connection,
        )

    def create_external_run_import(
        self,
        *,
        run_id: str,
        source_system: str,
        source_run_id: str,
        evidence_hash: str,
        idempotency_key: str,
        strategy_id: str,
        dataset_version: str,
        envelope_json: str,
        envelope_hash: str,
        started_at: datetime,
        completed_at: datetime,
        ingested_at: datetime,
        exit_code: int,
        failure_note: str | None,
        connection: sqlite3.Connection,
    ) -> None:
        """Atomically register a terminal run recovered after wrapper bypass."""

        state = "completed" if exit_code == 0 and failure_note is None else "failed"
        connection.execute(
            """
            INSERT INTO runs (run_id, prediction_id, started_at, state)
            VALUES (?, NULL, ?, 'registered')
            """,
            (run_id, started_at.isoformat()),
        )
        self._bind_run(
            run_id=run_id,
            idempotency_key=idempotency_key,
            request_hash=evidence_hash,
            registration_status="unregistered_external",
            strategy_id=strategy_id,
            dataset_version=dataset_version,
            envelope_json=envelope_json,
            envelope_hash=envelope_hash,
            bound_at=ingested_at,
            connection=connection,
        )
        family, start, end = self._touch_identity_for_run(connection, run_id)
        connection.execute(
            """
            UPDATE runs
            SET state = 'running', execution_started_at = ?
            WHERE run_id = ? AND state = 'registered'
            """,
            (started_at.isoformat(), run_id),
        )
        self._insert_touched_window(
            connection,
            family_id=family,
            window_start=start,
            window_end=end,
            touched_at=started_at,
        )
        connection.execute(
            """
            UPDATE runs
            SET state = ?, completed_at = ?, exit_code = ?, failure_note = ?
            WHERE run_id = ? AND state = 'running'
            """,
            (state, completed_at.isoformat(), exit_code, failure_note, run_id),
        )
        connection.execute(
            """
            INSERT INTO external_run_imports (
                run_id, source_system, source_run_id, evidence_hash, ingested_at
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                run_id,
                source_system,
                source_run_id,
                evidence_hash,
                ingested_at.isoformat(),
            ),
        )

    def create_run_result(
        self,
        *,
        run_id: str,
        evidence_hash: str,
        evidence_json: str,
        source_timestamp: datetime,
        ingested_at: datetime,
        connection: sqlite3.Connection,
    ) -> None:
        """Bind immutable result evidence to one successful terminal run."""

        cursor = connection.execute(
            """
            INSERT INTO run_results (
                run_id, evidence_hash, evidence_json, source_timestamp, ingested_at
            )
            SELECT ?, ?, ?, ?, ?
            WHERE EXISTS (
                SELECT 1
                FROM runs
                JOIN run_bindings USING (run_id)
                WHERE runs.run_id = ?
                  AND runs.state = 'completed'
                  AND runs.exit_code = 0
                  AND runs.failure_note IS NULL
            )
            """,
            (
                run_id,
                evidence_hash,
                evidence_json,
                source_timestamp.isoformat(),
                ingested_at.isoformat(),
                run_id,
            ),
        )
        if cursor.rowcount != 1:
            raise IntegrityError(f"result evidence requires a successful bound run: {run_id}")

    def bind_preregistered_run(
        self,
        *,
        run_id: str,
        idempotency_key: str,
        request_hash: str,
        strategy_id: str,
        dataset_version: str,
        envelope_json: str,
        envelope_hash: str,
        bound_at: datetime,
        connection: sqlite3.Connection,
    ) -> None:
        """Bind only the allocated run of a committed preregistered prediction."""

        eligible = connection.execute(
            """
            SELECT 1
            FROM runs
            JOIN predictions USING (prediction_id)
            WHERE runs.run_id = ?
              AND runs.state = 'registered'
              AND predictions.transaction_state = 'committed'
              AND predictions.registration_status = 'preregistered'
            """,
            (run_id,),
        ).fetchone()
        if eligible is None:
            raise IntegrityError(f"run is not allocated to a preregistered prediction: {run_id}")
        self._bind_run(
            run_id=run_id,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            registration_status="preregistered",
            strategy_id=strategy_id,
            dataset_version=dataset_version,
            envelope_json=envelope_json,
            envelope_hash=envelope_hash,
            bound_at=bound_at,
            connection=connection,
        )

    @staticmethod
    def _bind_run(
        *,
        run_id: str,
        idempotency_key: str,
        request_hash: str,
        registration_status: str,
        strategy_id: str,
        dataset_version: str,
        envelope_json: str,
        envelope_hash: str,
        bound_at: datetime,
        connection: sqlite3.Connection,
    ) -> None:
        """Insert the shared immutable binding after a typed public-path check."""

        cursor = connection.execute(
            """
            INSERT INTO run_bindings (
                run_id, idempotency_key, request_hash, registration_status,
                strategy_id, dataset_version, envelope_json, envelope_hash, bound_at
            )
            SELECT run_id, ?, ?, ?, ?, ?, ?, ?, ?
            FROM runs
            WHERE run_id = ? AND state = 'registered'
            """,
            (
                idempotency_key,
                request_hash,
                registration_status,
                strategy_id,
                dataset_version,
                envelope_json,
                envelope_hash,
                bound_at.isoformat(),
                run_id,
            ),
        )
        if cursor.rowcount != 1:
            raise IntegrityError(f"cannot bind unknown or started run: {run_id}")

    def start_run(
        self,
        run_id: str,
        *,
        started_at: datetime,
    ) -> None:
        """Atomically claim a run and permanently touch its observed OOS window."""

        if started_at.tzinfo is None or started_at.utcoffset() is None:
            raise ValueError("started_at must be timezone-aware")

        with self.transaction() as connection:
            family, start, end = self._touch_identity_for_run(connection, run_id)
            cursor = connection.execute(
                """
                UPDATE runs
                SET state = 'running', execution_started_at = ?
                WHERE run_id = ? AND state = 'registered'
                  AND EXISTS (
                      SELECT 1 FROM run_bindings WHERE run_bindings.run_id = runs.run_id
                  )
                """,
                (started_at.isoformat(), run_id),
            )
            if cursor.rowcount != 1:
                raise IntegrityError(f"run is not available to start: {run_id}")
            self._insert_touched_window(
                connection,
                family_id=family,
                window_start=start,
                window_end=end,
                touched_at=started_at,
            )

    @staticmethod
    def _touch_identity_for_run(
        connection: sqlite3.Connection,
        run_id: str,
    ) -> tuple[str, str, str]:
        """Derive touched evidence from one immutable run binding."""

        row = connection.execute(
            """
            SELECT runs.prediction_id, run_bindings.registration_status,
                   run_bindings.strategy_id, run_bindings.envelope_json,
                   run_bindings.envelope_hash, predictions.lineage_json,
                   predictions.transaction_state
            FROM runs
            JOIN run_bindings USING (run_id)
            LEFT JOIN predictions USING (prediction_id)
            WHERE runs.run_id = ? AND runs.state = 'registered'
            """,
            (run_id,),
        ).fetchone()
        if row is None:
            raise IntegrityError(f"run has no registered immutable binding: {run_id}")

        try:
            envelope = json.loads(row["envelope_json"])
        except (TypeError, json.JSONDecodeError) as exc:
            raise IntegrityError("run binding envelope is not valid JSON") from exc
        if not isinstance(envelope, dict) or sha256_json(envelope) != row["envelope_hash"]:
            raise IntegrityError("run binding envelope does not match its immutable hash")
        if envelope.get("registration_status") != row["registration_status"]:
            raise IntegrityError("run binding registration status does not match its envelope")
        if envelope.get("strategy_id") != row["strategy_id"]:
            raise IntegrityError("run binding strategy does not match its envelope")

        snapshot = envelope.get("snapshot")
        window = snapshot.get("out_of_sample_window") if isinstance(snapshot, dict) else None
        if not isinstance(window, dict):
            raise IntegrityError("run binding is missing its out-of-sample window")
        try:
            start, end = _normalize_window(window["start"], window["end"])
        except (KeyError, TypeError, ValueError) as exc:
            raise IntegrityError("run binding has an invalid out-of-sample window") from exc

        family_id: object = row["strategy_id"]
        if row["registration_status"] == "preregistered":
            if row["prediction_id"] is None or row["transaction_state"] != "committed":
                raise IntegrityError("preregistered run is missing its committed prediction")
            try:
                lineage = json.loads(row["lineage_json"])
            except (TypeError, json.JSONDecodeError) as exc:
                raise IntegrityError("preregistered run lineage is not valid JSON") from exc
            family_id = lineage.get("family_id") if isinstance(lineage, dict) else None
        try:
            family = _normalize_family_id(family_id)
        except (TypeError, ValueError) as exc:
            raise IntegrityError("run binding has an invalid strategy family") from exc
        return family, start, end

    def finish_run(
        self,
        run_id: str,
        *,
        completed_at: datetime,
        exit_code: int,
        failure_note: str | None = None,
    ) -> None:
        """Persist the terminal process state for a claimed run."""

        state = "completed" if exit_code == 0 and failure_note is None else "failed"
        with self.transaction() as connection:
            cursor = connection.execute(
                """
                UPDATE runs
                SET state = ?, completed_at = ?, exit_code = ?, failure_note = ?
                WHERE run_id = ? AND state = 'running'
                """,
                (state, completed_at.isoformat(), exit_code, failure_note, run_id),
            )
            if cursor.rowcount != 1:
                raise IntegrityError(f"run is not active: {run_id}")

        logger.info(
            "ledger_run_finished",
            extra={"run_id": run_id, "state": state, "exit_code": exit_code},
        )

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

        if status is PredictionStatus.QUARANTINED:
            raise IntegrityError(
                "quarantine requires violation evidence; use quarantine_prediction"
            )

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
        window_start: date | str,
        window_end: date | str,
        *,
        touched_at: datetime | None = None,
    ) -> None:
        """Record an observed research window without overwriting its first touch."""

        family = _normalize_family_id(family_id)
        start, end = _normalize_window(window_start, window_end)
        timestamp = touched_at or datetime.now(UTC)
        if timestamp.tzinfo is None or timestamp.utcoffset() is None:
            raise ValueError("touched_at must be timezone-aware")
        with self.transaction() as connection:
            self._insert_touched_window(
                connection,
                family_id=family,
                window_start=start,
                window_end=end,
                touched_at=timestamp,
            )

    @staticmethod
    def _insert_touched_window(
        connection: sqlite3.Connection,
        *,
        family_id: str,
        window_start: str,
        window_end: str,
        touched_at: datetime,
    ) -> None:
        connection.execute(
            """
            INSERT OR IGNORE INTO touched_windows (
                family_id, window_start, window_end, touched_at
            ) VALUES (?, ?, ?, ?)
            """,
            (family_id, window_start, window_end, touched_at.isoformat()),
        )

    def is_window_touched(
        self,
        family_id: str,
        window_start: date | str,
        window_end: date | str,
    ) -> bool:
        """Return whether a family has already observed the exact data window."""

        family = _normalize_family_id(family_id)
        start, end = _normalize_window(window_start, window_end)
        connection = self.connect()
        try:
            row = connection.execute(
                """
                SELECT 1 FROM touched_windows
                WHERE family_id = ? AND window_start = ? AND window_end = ?
                """,
                (family, start, end),
            ).fetchone()
            return row is not None
        finally:
            connection.close()

    def find_touched_window_overlap(
        self,
        family_id: str,
        window_start: date | str,
        window_end: date | str,
        *,
        connection: sqlite3.Connection | None = None,
    ) -> sqlite3.Row | None:
        """Return the first inclusive overlap for a strategy family's data window."""

        family = _normalize_family_id(family_id)
        start, end = _normalize_window(window_start, window_end)
        owns_connection = connection is None
        active = connection or self.connect()
        try:
            return active.execute(
                """
                SELECT family_id, window_start, window_end, touched_at
                FROM touched_windows
                WHERE family_id = ?
                  AND window_start <= ?
                  AND window_end >= ?
                ORDER BY touched_at, window_start, window_end
                LIMIT 1
                """,
                (family, end, start),
            ).fetchone()
        finally:
            if owns_connection:
                active.close()

    def window_overlaps_touched(
        self,
        family_id: str,
        window_start: date | str,
        window_end: date | str,
        *,
        connection: sqlite3.Connection | None = None,
    ) -> bool:
        """Return whether any previously observed family window overlaps inclusively."""

        return (
            self.find_touched_window_overlap(
                family_id,
                window_start,
                window_end,
                connection=connection,
            )
            is not None
        )

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

    def quarantine_prediction(
        self,
        prediction_id: str,
        *,
        violations: Mapping[str, str],
        detected_at: datetime | None = None,
    ) -> bool:
        """Permanently quarantine a committed prediction and append new evidence.

        Identical ``(prediction_id, field, note)`` evidence is recorded once so
        duplicate filesystem notifications remain idempotent. The return value is
        true when the status changed or at least one new violation was appended.
        """

        if not violations:
            raise ValueError("violations must contain at least one field")
        if any(not field or not note for field, note in violations.items()):
            raise ValueError("violation fields and notes must be non-empty")
        timestamp = detected_at or datetime.now(UTC)
        if timestamp.tzinfo is None or timestamp.utcoffset() is None:
            raise ValueError("detected_at must be timezone-aware")
        changed = False
        with self.transaction() as connection:
            row = self.get_prediction(prediction_id, connection=connection)
            if row is None or row["transaction_state"] != "committed":
                raise IntegrityError(
                    f"cannot quarantine unknown or uncommitted prediction: {prediction_id}"
                )
            for field, note in violations.items():
                cursor = connection.execute(
                    """
                    INSERT INTO integrity_violations (
                        prediction_id, field, detected_at, note
                    )
                    SELECT ?, ?, ?, ?
                    WHERE NOT EXISTS (
                        SELECT 1 FROM integrity_violations
                        WHERE prediction_id = ? AND field = ? AND note = ?
                    )
                    """,
                    (
                        prediction_id,
                        field,
                        timestamp.isoformat(),
                        note,
                        prediction_id,
                        field,
                        note,
                    ),
                )
                changed = changed or cursor.rowcount == 1
            if row["status"] != PredictionStatus.QUARANTINED.value:
                connection.execute(
                    "UPDATE predictions SET status = ? WHERE prediction_id = ?",
                    (PredictionStatus.QUARANTINED.value, prediction_id),
                )
                changed = True

        logger.warning(
            "ledger_prediction_quarantined",
            extra={
                "prediction_id": prediction_id,
                "fields": sorted(violations),
                "changed": changed,
            },
        )
        return changed

    def _migrate(self) -> None:
        connection = self.connect()
        try:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    version INTEGER PRIMARY KEY,
                    applied_at TEXT NOT NULL
                )
                """
            )
            applied = {
                row[0] for row in connection.execute("SELECT version FROM schema_migrations")
            }
            for version, migration in _MIGRATIONS.items():
                if version in applied:
                    continue
                connection.executescript(
                    "BEGIN IMMEDIATE;\n"
                    + migration
                    + f"""
                    INSERT OR IGNORE INTO schema_migrations (version, applied_at)
                    VALUES ({version}, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'));
                    PRAGMA user_version = {version};
                    COMMIT;
                    """
                )
            actual_version = connection.execute("PRAGMA user_version").fetchone()[0]
            if actual_version != _MIGRATION_VERSION:
                raise IntegrityError(
                    f"unsupported registry version {actual_version}; expected {_MIGRATION_VERSION}"
                )
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()


def _optional_json(value: Mapping[str, Any] | None) -> str | None:
    return None if value is None else canonical_json(dict(value))


def _normalize_family_id(family_id: object) -> str:
    if not isinstance(family_id, str):
        raise TypeError("family_id must be a string")
    normalized = family_id.strip()
    if not normalized or len(normalized) > 256 or "\x00" in normalized:
        raise ValueError("family_id must be 1 to 256 non-NUL characters")
    return normalized


def _normalize_window(window_start: date | str, window_end: date | str) -> tuple[str, str]:
    start = window_start if isinstance(window_start, date) else date.fromisoformat(window_start)
    end = window_end if isinstance(window_end, date) else date.fromisoformat(window_end)
    if end < start:
        raise ValueError("window_end must be on or after window_start")
    return start.isoformat(), end.isoformat()
