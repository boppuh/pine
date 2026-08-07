"""Versioned schema for non-authoritative console workflow state."""

from __future__ import annotations

import sqlite3

from ledger.console.errors import ConsoleStateError

CONSOLE_SCHEMA_VERSION = 1
MINIMUM_COMPATIBLE_SCHEMA_VERSION = 1

_MIGRATION_1 = """
CREATE TABLE console_schema_migrations (
    version INTEGER PRIMARY KEY,
    applied_at TEXT NOT NULL
);

CREATE TABLE workflows (
    workflow_id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN (
        'editing', 'extracting', 'reviewing', 'frozen', 'submitting',
        'uncertain', 'retryable_failure', 'terminal_failure',
        'committed', 'cancelled'
    )),
    schema_id TEXT NOT NULL,
    source_text TEXT,
    proposal_json TEXT CHECK (
        proposal_json IS NULL OR json_valid(proposal_json)
    ),
    idempotency_key TEXT NOT NULL UNIQUE,
    frozen_request_json TEXT CHECK (
        frozen_request_json IS NULL OR json_valid(frozen_request_json)
    ),
    capture_response_json TEXT CHECK (
        capture_response_json IS NULL OR json_valid(capture_response_json)
    ),
    error_code TEXT,
    error_details_json TEXT CHECK (
        error_details_json IS NULL OR json_valid(error_details_json)
    ),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    expires_at TEXT,
    version INTEGER NOT NULL DEFAULT 0 CHECK (version >= 0),
    CHECK (LENGTH(workflow_id) = 36),
    CHECK (LENGTH(user_id) BETWEEN 1 AND 320),
    CHECK (LENGTH(schema_id) BETWEEN 1 AND 256),
    CHECK (LENGTH(idempotency_key) = 44),
    CHECK (source_text IS NULL OR LENGTH(source_text) BETWEEN 1 AND 200000)
);

CREATE INDEX workflows_user_updated
    ON workflows(user_id, updated_at DESC, workflow_id DESC);
CREATE INDEX workflows_expiry
    ON workflows(expires_at, state);

CREATE TRIGGER workflows_insert_starts_editing
BEFORE INSERT ON workflows
WHEN NEW.state != 'editing'
  OR NEW.source_text IS NULL
  OR NEW.proposal_json IS NOT NULL
  OR NEW.frozen_request_json IS NOT NULL
  OR NEW.capture_response_json IS NOT NULL
  OR NEW.error_code IS NOT NULL
  OR NEW.error_details_json IS NOT NULL
  OR NEW.expires_at IS NULL
  OR NEW.version != 0
BEGIN
    SELECT RAISE(ABORT, 'console workflow must begin in editing state');
END;

CREATE TRIGGER workflows_identity_write_once
BEFORE UPDATE OF workflow_id, user_id, schema_id, idempotency_key, created_at ON workflows
BEGIN
    SELECT RAISE(ABORT, 'console workflow identity is write-once');
END;

CREATE TRIGGER workflows_version_increments
BEFORE UPDATE ON workflows
WHEN NEW.version != OLD.version + 1
BEGIN
    SELECT RAISE(ABORT, 'console workflow version must increment exactly once');
END;

CREATE TRIGGER workflows_legal_state_transition
BEFORE UPDATE OF state ON workflows
WHEN NEW.state != OLD.state AND NOT (
    (OLD.state = 'editing' AND NEW.state IN ('extracting', 'cancelled')) OR
    (OLD.state = 'extracting' AND NEW.state IN ('editing', 'reviewing')) OR
    (OLD.state = 'reviewing' AND NEW.state IN ('cancelled', 'frozen')) OR
    (OLD.state = 'frozen' AND NEW.state = 'submitting') OR
    (OLD.state = 'submitting' AND NEW.state IN (
        'committed', 'uncertain', 'retryable_failure', 'terminal_failure'
    )) OR
    (OLD.state IN ('uncertain', 'retryable_failure') AND NEW.state = 'submitting')
)
BEGIN
    SELECT RAISE(ABORT, 'illegal console workflow transition');
END;

CREATE TRIGGER workflows_frozen_request_write_once
BEFORE UPDATE OF frozen_request_json ON workflows
WHEN OLD.frozen_request_json IS NOT NULL
 AND NEW.frozen_request_json IS NOT OLD.frozen_request_json
BEGIN
    SELECT RAISE(ABORT, 'frozen console request is write-once');
END;

CREATE TRIGGER workflows_frozen_request_first_set
BEFORE UPDATE OF frozen_request_json ON workflows
WHEN OLD.frozen_request_json IS NULL
 AND NEW.frozen_request_json IS NOT NULL
 AND NOT (OLD.state = 'reviewing' AND NEW.state = 'frozen')
BEGIN
    SELECT RAISE(ABORT, 'frozen request requires reviewing-to-frozen transition');
END;

CREATE TRIGGER workflows_capture_response_write_once
BEFORE UPDATE OF capture_response_json ON workflows
WHEN OLD.capture_response_json IS NOT NULL
 AND NEW.capture_response_json IS NOT OLD.capture_response_json
BEGIN
    SELECT RAISE(ABORT, 'console capture response is write-once');
END;

CREATE TRIGGER workflows_capture_response_first_set
BEFORE UPDATE OF capture_response_json ON workflows
WHEN OLD.capture_response_json IS NULL
 AND NEW.capture_response_json IS NOT NULL
 AND NOT (OLD.state = 'submitting' AND NEW.state = 'committed')
BEGIN
    SELECT RAISE(ABORT, 'capture response requires submitting-to-committed transition');
END;

CREATE TRIGGER workflows_frozen_state_requires_request
BEFORE UPDATE ON workflows
WHEN NEW.state IN (
    'frozen', 'submitting', 'uncertain', 'retryable_failure',
    'terminal_failure', 'committed'
) AND NEW.frozen_request_json IS NULL
BEGIN
    SELECT RAISE(ABORT, 'frozen workflow state requires exact request');
END;

CREATE TRIGGER workflows_committed_requires_response
BEFORE UPDATE ON workflows
WHEN (NEW.state = 'committed') != (NEW.capture_response_json IS NOT NULL)
BEGIN
    SELECT RAISE(ABORT, 'capture response is valid only for committed workflows');
END;

CREATE TRIGGER workflows_reconciliation_never_expires
BEFORE UPDATE ON workflows
WHEN NEW.state IN ('frozen', 'submitting', 'uncertain', 'retryable_failure')
 AND NEW.expires_at IS NOT NULL
BEGIN
    SELECT RAISE(ABORT, 'reconciliation workflow cannot expire');
END;

CREATE TRIGGER workflows_terminal_failure_expires
BEFORE UPDATE ON workflows
WHEN NEW.state = 'terminal_failure' AND NEW.expires_at IS NULL
BEGIN
    SELECT RAISE(ABORT, 'terminal failure workflow must expire');
END;

CREATE TRIGGER workflows_cancelled_discards_content
BEFORE UPDATE ON workflows
WHEN NEW.state = 'cancelled'
 AND (NEW.source_text IS NOT NULL OR NEW.proposal_json IS NOT NULL)
BEGIN
    SELECT RAISE(ABORT, 'cancelled workflow must discard transient content');
END;

CREATE TRIGGER workflows_protected_from_cleanup
BEFORE DELETE ON workflows
WHEN OLD.state NOT IN (
    'editing', 'reviewing', 'terminal_failure', 'committed', 'cancelled'
)
BEGIN
    SELECT RAISE(ABORT, 'workflow state is protected from automatic cleanup');
END;
"""

_MIGRATIONS = {1: _MIGRATION_1}


def migrate(connection: sqlite3.Connection) -> int:
    """Migrate an empty or supported console database to the current version."""

    version = 0
    try:
        connection.execute("BEGIN IMMEDIATE")
        current = schema_version(connection)
        if current > CONSOLE_SCHEMA_VERSION:
            raise ConsoleStateError(
                f"console schema {current} is newer than supported {CONSOLE_SCHEMA_VERSION}"
            )
        for version in range(current + 1, CONSOLE_SCHEMA_VERSION + 1):
            for statement in _migration_statements(_MIGRATIONS[version]):
                connection.execute(statement)
            connection.execute(
                "INSERT INTO console_schema_migrations (version, applied_at) "
                "VALUES (?, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))",
                (version,),
            )
            connection.execute(f"PRAGMA user_version = {version}")
        connection.commit()
    except ConsoleStateError:
        connection.rollback()
        raise
    except sqlite3.DatabaseError as exc:
        connection.rollback()
        raise ConsoleStateError(f"console schema migration {version} failed") from exc
    return CONSOLE_SCHEMA_VERSION


def _migration_statements(script: str) -> tuple[str, ...]:
    """Split a trusted migration script without breaking trigger bodies."""

    statements: list[str] = []
    pending = ""
    for line in script.splitlines(keepends=True):
        pending += line
        if sqlite3.complete_statement(pending):
            statement = pending.strip()
            if statement:
                statements.append(statement)
            pending = ""
    if pending.strip():
        raise ConsoleStateError("console migration contains an incomplete statement")
    return tuple(statements)


def schema_version(connection: sqlite3.Connection) -> int:
    """Return the dedicated console migration version without guessing."""

    table = connection.execute(
        """
        SELECT 1 FROM sqlite_master
        WHERE type = 'table' AND name = 'console_schema_migrations'
        """
    ).fetchone()
    if table is None:
        unrelated = connection.execute(
            """
            SELECT name FROM sqlite_master
            WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
            LIMIT 1
            """
        ).fetchone()
        if unrelated is not None:
            raise ConsoleStateError("unversioned console database is not safe to migrate")
        return 0
    row = connection.execute("SELECT MAX(version) FROM console_schema_migrations").fetchone()
    if row is None or row[0] is None:
        raise ConsoleStateError("console migration table is empty")
    version = int(row[0])
    pragma_version = int(connection.execute("PRAGMA user_version").fetchone()[0])
    if pragma_version != version:
        raise ConsoleStateError("console migration stamps disagree")
    return version
