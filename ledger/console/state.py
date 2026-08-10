"""SQLite authority for transient, non-ledger console workflows."""

from __future__ import annotations

import json
import os
import sqlite3
import stat
import time
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

from pydantic import JsonValue, ValidationError

from ledger.api import CaptureResponse
from ledger.console.auth import normalize_user_identity
from ledger.console.errors import ConsoleStateError, WorkflowConflictError, WorkflowNotFoundError
from ledger.console.migrations import CONSOLE_SCHEMA_VERSION, migrate, schema_version
from ledger.console.models import (
    CaptureInput,
    ConsoleWorkflow,
    FrozenCaptureRequest,
    FrozenCaptureResponse,
    WorkflowState,
)
from ledger.extraction import (
    DraftProposal,
    ExtractionResult,
    ExtractionStatus,
    HypothesisExtractionRequest,
)
from ledger.json_utils import canonical_json


class ConsoleStateStore:
    """Persist recoverable workflow state outside the authoritative ledger vault."""

    def __init__(
        self,
        db_path: str | Path,
        *,
        ordinary_retention: timedelta = timedelta(hours=24),
        receipt_retention: timedelta = timedelta(hours=24),
        clock: Callable[[], datetime] | None = None,
        uuid_factory: Callable[[], UUID] | None = None,
    ) -> None:
        requested_path = Path(db_path).expanduser().absolute()
        if requested_path.is_symlink():
            raise ConsoleStateError("console state path must not be a symlink")
        try:
            self.db_path = requested_path.resolve(strict=False)
        except OSError as exc:
            raise ConsoleStateError("console state path could not be resolved safely") from exc
        if ".ledger" in self.db_path.parts:
            raise ConsoleStateError("console state must live outside the authoritative ledger")
        self.ordinary_retention = ordinary_retention
        self.receipt_retention = receipt_retention
        self.clock = clock or (lambda: datetime.now(UTC))
        self.uuid_factory = uuid_factory or uuid4
        if ordinary_retention <= timedelta(0) or receipt_retention <= timedelta(0):
            raise ValueError("console retention durations must be positive")
        self._prepare_path()
        connection = self.connect()
        try:
            migrate(connection)
        finally:
            connection.close()

    def connect(self) -> sqlite3.Connection:
        """Open a hardened WAL connection to console state only."""

        connection = sqlite3.connect(self.db_path, timeout=30.0, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout = 30000")
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA synchronous = FULL")
        self._enable_wal(connection)
        return connection

    @staticmethod
    def _enable_wal(connection: sqlite3.Connection) -> None:
        """Enable WAL with bounded retry during concurrent process startup."""

        deadline = time.monotonic() + 30.0
        delay = 0.001
        last_error: sqlite3.OperationalError | None = None
        while True:
            try:
                journal_mode = connection.execute("PRAGMA journal_mode = WAL").fetchone()[0]
            except sqlite3.OperationalError as exc:
                last_error = exc
            else:
                if str(journal_mode).lower() == "wal":
                    return
            if time.monotonic() >= deadline:
                connection.close()
                raise ConsoleStateError("console database refused WAL mode") from last_error
            time.sleep(delay)
            delay = min(delay * 2, 0.05)

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        """Serialize workflow transitions and roll back every exceptional exit."""

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

    def get_workflow(self, workflow_id: str, user_id: str) -> ConsoleWorkflow:
        """Return one user-scoped workflow without revealing cross-user existence."""

        connection = self.connect()
        try:
            row = self._get_row(connection, workflow_id, normalize_user_identity(user_id))
            return self._workflow_from_row(row)
        finally:
            connection.close()

    def create_workflow(
        self,
        *,
        user_id: str,
        source_text: str,
        schema_id: str = "finance/strategy-edge:1",
    ) -> ConsoleWorkflow:
        """Create an editing workflow with server-owned identities and expiry."""

        request = HypothesisExtractionRequest(text=source_text, schema_id=schema_id)
        identity = normalize_user_identity(user_id)
        workflow_id = str(self.uuid_factory())
        idempotency_key = f"console-{self.uuid_factory()}"
        now = self._now()
        expires_at = now + self.ordinary_retention
        with self.transaction() as connection:
            try:
                connection.execute(
                    """
                    INSERT INTO workflows (
                        workflow_id, user_id, state, schema_id, source_text,
                        proposal_json, idempotency_key, frozen_request_json,
                        capture_response_json, error_code, error_details_json,
                        created_at, updated_at, expires_at, version
                    ) VALUES (?, ?, 'editing', ?, ?, NULL, ?, NULL, NULL, NULL, NULL,
                              ?, ?, ?, 0)
                    """,
                    (
                        workflow_id,
                        identity,
                        request.schema_id,
                        request.text,
                        idempotency_key,
                        _timestamp(now),
                        _timestamp(now),
                        _timestamp(expires_at),
                    ),
                )
            except sqlite3.IntegrityError as exc:
                raise ConsoleStateError("console workflow identity collision") from exc
            row = self._get_row(connection, workflow_id, identity)
        return self._workflow_from_row(row)

    def begin_extraction(
        self,
        workflow_id: str,
        user_id: str,
        *,
        expected_version: int | None = None,
    ) -> ConsoleWorkflow:
        """Move an editing workflow to extracting before the backend call."""

        identity = normalize_user_identity(user_id)
        now = self._now()
        with self.transaction() as connection:
            row = self._get_row(connection, workflow_id, identity)
            self._require_state(row, WorkflowState.EDITING)
            self._require_version(row, expected_version)
            self._update(
                connection,
                workflow_id,
                """
                state = 'extracting', error_code = NULL, error_details_json = NULL,
                updated_at = ?, version = version + 1
                """,
                (_timestamp(now),),
            )
            updated = self._get_row(connection, workflow_id, identity)
        return self._workflow_from_row(updated)

    def revise_source(
        self,
        workflow_id: str,
        user_id: str,
        *,
        source_text: str,
        schema_id: str,
        expected_version: int | None = None,
    ) -> ConsoleWorkflow:
        """Replace transient source after a failed extraction, before trying again."""

        request = HypothesisExtractionRequest(text=source_text, schema_id=schema_id)
        identity = normalize_user_identity(user_id)
        now = self._now()
        with self.transaction() as connection:
            row = self._get_row(connection, workflow_id, identity)
            self._require_state(row, WorkflowState.EDITING)
            self._require_version(row, expected_version)
            if request.schema_id != row["schema_id"]:
                raise WorkflowConflictError("workflow schema cannot be changed")
            self._update(
                connection,
                workflow_id,
                """
                source_text = ?, proposal_json = NULL, error_code = NULL,
                error_details_json = NULL, updated_at = ?, expires_at = ?,
                version = version + 1
                """,
                (
                    request.text,
                    _timestamp(now),
                    _timestamp(now + self.ordinary_retention),
                ),
            )
            updated = self._get_row(connection, workflow_id, identity)
        return self._workflow_from_row(updated)

    def finish_extraction(
        self,
        workflow_id: str,
        user_id: str,
        result: ExtractionResult,
    ) -> ConsoleWorkflow:
        """Persist a ready proposal or return a clean extraction failure to editing."""

        identity = normalize_user_identity(user_id)
        now = self._now()
        expires_at = now + self.ordinary_retention
        with self.transaction() as connection:
            row = self._get_row(connection, workflow_id, identity, reject_expired=False)
            self._require_state(row, WorkflowState.EXTRACTING)
            if result.status is ExtractionStatus.READY:
                if result.proposal is None:
                    raise ConsoleStateError("ready extraction result lacks a proposal")
                self._update(
                    connection,
                    workflow_id,
                    """
                    state = 'reviewing', proposal_json = ?, error_code = NULL,
                    error_details_json = NULL, updated_at = ?, expires_at = ?,
                    version = version + 1
                    """,
                    (
                        _model_json(result.proposal),
                        _timestamp(now),
                        _timestamp(expires_at),
                    ),
                )
            else:
                self._update(
                    connection,
                    workflow_id,
                    """
                    state = 'editing', proposal_json = NULL, error_code = 'extraction_unable',
                    error_details_json = ?, updated_at = ?, expires_at = ?,
                    version = version + 1
                    """,
                    (
                        canonical_json({"details": list(result.errors)}),
                        _timestamp(now),
                        _timestamp(expires_at),
                    ),
                )
            updated = self._get_row(connection, workflow_id, identity)
        return self._workflow_from_row(updated)

    def fail_extraction(
        self,
        workflow_id: str,
        user_id: str,
        *,
        code: str,
        details: tuple[str, ...],
    ) -> ConsoleWorkflow:
        """Return a side-effect-free backend failure to editing."""

        identity = normalize_user_identity(user_id)
        now = self._now()
        with self.transaction() as connection:
            row = self._get_row(connection, workflow_id, identity, reject_expired=False)
            self._require_state(row, WorkflowState.EXTRACTING)
            self._update(
                connection,
                workflow_id,
                """
                state = 'editing', proposal_json = NULL, error_code = ?,
                error_details_json = ?, updated_at = ?, expires_at = ?,
                version = version + 1
                """,
                (
                    code,
                    canonical_json({"details": list(details)}),
                    _timestamp(now),
                    _timestamp(now + self.ordinary_retention),
                ),
            )
            updated = self._get_row(connection, workflow_id, identity)
        return self._workflow_from_row(updated)

    def freeze_and_begin_submission(
        self,
        workflow_id: str,
        user_id: str,
        capture: CaptureInput | Mapping[str, Any],
        *,
        expected_version: int | None = None,
    ) -> ConsoleWorkflow:
        """Freeze the first request and enter submitting in one immediate transaction."""

        identity = normalize_user_identity(user_id)
        now = self._now()
        with self.transaction() as connection:
            row = self._get_row(connection, workflow_id, identity)
            if row["state"] == WorkflowState.COMMITTED.value:
                return self._workflow_from_row(row)
            self._require_state(row, WorkflowState.REVIEWING)
            self._require_version(row, expected_version)
            capture_input = (
                capture
                if isinstance(capture, CaptureInput)
                else CaptureInput.model_validate(capture)
            )
            if capture_input.schema_id != row["schema_id"]:
                raise WorkflowConflictError("capture schema differs from the workflow schema")
            request = capture_input.freeze(row["idempotency_key"])
            self._update(
                connection,
                workflow_id,
                """
                state = 'frozen', frozen_request_json = ?, expires_at = NULL,
                error_code = NULL, error_details_json = NULL, updated_at = ?,
                version = version + 1
                """,
                (_model_json(request), _timestamp(now)),
            )
            self._update(
                connection,
                workflow_id,
                """
                state = 'submitting', updated_at = ?, version = version + 1
                """,
                (_timestamp(now),),
            )
            updated = self._get_row(connection, workflow_id, identity)
        return self._workflow_from_row(updated)

    def begin_retry(
        self,
        workflow_id: str,
        user_id: str,
        *,
        expected_version: int | None = None,
    ) -> ConsoleWorkflow:
        """Enter submitting while preserving the exact frozen request."""

        identity = normalize_user_identity(user_id)
        now = self._now()
        with self.transaction() as connection:
            row = self._get_row(connection, workflow_id, identity)
            if row["state"] not in {
                WorkflowState.UNCERTAIN.value,
                WorkflowState.RETRYABLE_FAILURE.value,
            }:
                raise WorkflowConflictError("workflow is not eligible for exact retry")
            self._require_version(row, expected_version)
            self._update(
                connection,
                workflow_id,
                """
                state = 'submitting', error_code = NULL, error_details_json = NULL,
                updated_at = ?, expires_at = NULL, version = version + 1
                """,
                (_timestamp(now),),
            )
            updated = self._get_row(connection, workflow_id, identity)
        return self._workflow_from_row(updated)

    def record_capture_response(
        self,
        workflow_id: str,
        user_id: str,
        response: CaptureResponse,
    ) -> ConsoleWorkflow:
        """Persist the first validated receipt and make it immutable."""

        identity = normalize_user_identity(user_id)
        now = self._now()
        with self.transaction() as connection:
            row = self._get_row(connection, workflow_id, identity)
            self._require_state(row, WorkflowState.SUBMITTING)
            self._update(
                connection,
                workflow_id,
                """
                state = 'committed', capture_response_json = ?, error_code = NULL,
                error_details_json = NULL, updated_at = ?, expires_at = ?,
                version = version + 1
                """,
                (
                    _model_json(response),
                    _timestamp(now),
                    _timestamp(now + self.receipt_retention),
                ),
            )
            updated = self._get_row(connection, workflow_id, identity)
        return self._workflow_from_row(updated)

    def record_capture_failure(
        self,
        workflow_id: str,
        user_id: str,
        *,
        state: WorkflowState,
        code: str,
        details: tuple[str, ...],
    ) -> ConsoleWorkflow:
        """Persist a classified capture failure without altering the frozen request."""

        if state not in {
            WorkflowState.UNCERTAIN,
            WorkflowState.RETRYABLE_FAILURE,
            WorkflowState.TERMINAL_FAILURE,
        }:
            raise ValueError("capture failure requires a failure state")
        identity = normalize_user_identity(user_id)
        now = self._now()
        with self.transaction() as connection:
            row = self._get_row(connection, workflow_id, identity)
            self._require_state(row, WorkflowState.SUBMITTING)
            self._update(
                connection,
                workflow_id,
                """
                state = ?, error_code = ?, error_details_json = ?, updated_at = ?,
                expires_at = ?, version = version + 1
                """,
                (
                    state.value,
                    code,
                    canonical_json({"details": list(details)}),
                    _timestamp(now),
                    (
                        _timestamp(now + self.ordinary_retention)
                        if state is WorkflowState.TERMINAL_FAILURE
                        else None
                    ),
                ),
            )
            updated = self._get_row(connection, workflow_id, identity)
        return self._workflow_from_row(updated)

    def cancel_workflow(
        self,
        workflow_id: str,
        user_id: str,
        *,
        expected_version: int | None = None,
    ) -> ConsoleWorkflow:
        """Cancel pre-confirmation work and remove its source and proposal."""

        identity = normalize_user_identity(user_id)
        now = self._now()
        with self.transaction() as connection:
            row = self._get_row(connection, workflow_id, identity)
            if row["state"] not in {
                WorkflowState.EDITING.value,
                WorkflowState.REVIEWING.value,
            }:
                raise WorkflowConflictError("workflow can no longer be cancelled")
            self._require_version(row, expected_version)
            self._update(
                connection,
                workflow_id,
                """
                state = 'cancelled', source_text = NULL, proposal_json = NULL,
                error_code = NULL, error_details_json = NULL, updated_at = ?,
                expires_at = ?, version = version + 1
                """,
                (_timestamp(now), _timestamp(now + self.ordinary_retention)),
            )
            updated = self._get_row(connection, workflow_id, identity)
        return self._workflow_from_row(updated)

    def recover_abandoned_workflows(self) -> dict[str, int]:
        """Convert interrupted writes to uncertain and extraction to safe editing."""

        now = self._now()
        with self.transaction() as connection:
            submitting = connection.execute(
                """
                UPDATE workflows
                SET state = 'uncertain', error_code = 'console_restarted',
                    error_details_json = '{"details":["capture outcome requires exact replay"]}',
                    updated_at = ?, expires_at = NULL, version = version + 1
                WHERE state = 'submitting'
                """,
                (_timestamp(now),),
            ).rowcount
            extracting = connection.execute(
                """
                UPDATE workflows
                SET state = 'editing', error_code = 'console_restarted',
                    error_details_json = '{"details":["extraction may be started again"]}',
                    updated_at = ?, expires_at = ?, version = version + 1
                WHERE state = 'extracting'
                """,
                (
                    _timestamp(now),
                    _timestamp(now + self.ordinary_retention),
                ),
            ).rowcount
        return {"submitting_to_uncertain": submitting, "extracting_to_editing": extracting}

    def cleanup_expired(self) -> int:
        """Delete only expired states explicitly approved by the retention contract."""

        now = _timestamp(self._now())
        with self.transaction() as connection:
            return connection.execute(
                """
                DELETE FROM workflows
                WHERE expires_at IS NOT NULL AND expires_at <= ?
                  AND state IN (
                      'editing', 'reviewing', 'terminal_failure', 'committed', 'cancelled'
                  )
                """,
                (now,),
            ).rowcount

    def get_status(self) -> dict[str, int]:
        """Return non-secret schema and workflow counts for readiness checks."""

        connection = self.connect()
        try:
            rows = connection.execute(
                "SELECT state, COUNT(*) AS count FROM workflows GROUP BY state"
            ).fetchall()
            result = {"schema_version": schema_version(connection)}
            result.update({str(row["state"]): int(row["count"]) for row in rows})
            return result
        finally:
            connection.close()

    def check_writable(self) -> None:
        """Prove the durable console database can complete a write transaction."""

        connection = self.connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            cursor = connection.execute(
                "UPDATE console_schema_migrations SET applied_at = applied_at WHERE version = ?",
                (CONSOLE_SCHEMA_VERSION,),
            )
            if cursor.rowcount != 1:
                raise ConsoleStateError("console schema write check found no current stamp")
            connection.commit()
        except ConsoleStateError:
            connection.rollback()
            raise
        except sqlite3.DatabaseError as exc:
            connection.rollback()
            raise ConsoleStateError("console database is not writable") from exc
        finally:
            connection.close()

    def _prepare_path(self) -> None:
        parent = self.db_path.parent
        if parent.is_symlink():
            raise ConsoleStateError("console state directory must not be a symlink")
        parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        if not parent.is_dir():
            raise ConsoleStateError("console state parent is not a directory")
        if stat.S_IMODE(parent.stat().st_mode) & 0o077:
            raise ConsoleStateError("console state directory must not be group/world accessible")
        if self.db_path.is_symlink() or (self.db_path.exists() and not self.db_path.is_file()):
            raise ConsoleStateError("console state path must be a regular file")
        if not self.db_path.exists():
            try:
                descriptor = os.open(self.db_path, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600)
            except FileExistsError:
                pass
            else:
                os.close(descriptor)
        if self.db_path.is_symlink() or not self.db_path.is_file():
            raise ConsoleStateError("console state path must be a regular file")
        mode = stat.S_IMODE(self.db_path.stat().st_mode)
        if mode & 0o077:
            raise ConsoleStateError("console state database must not be group/world accessible")

    def _now(self) -> datetime:
        value = self.clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise ConsoleStateError("console clock returned a naive timestamp")
        return value.astimezone(UTC)

    def _get_row(
        self,
        connection: sqlite3.Connection,
        workflow_id: str,
        user_id: str,
        *,
        reject_expired: bool = True,
    ) -> sqlite3.Row:
        row = connection.execute(
            "SELECT * FROM workflows WHERE workflow_id = ? AND user_id = ?",
            (workflow_id, user_id),
        ).fetchone()
        if row is None:
            raise WorkflowNotFoundError("console workflow was not found")
        if reject_expired and row["expires_at"] is not None:
            try:
                expires_at = datetime.fromisoformat(row["expires_at"])
            except (TypeError, ValueError) as exc:
                raise ConsoleStateError("console workflow expiry is invalid") from exc
            if expires_at <= self._now():
                raise WorkflowNotFoundError("console workflow was not found")
        return row

    @staticmethod
    def _require_state(row: sqlite3.Row, expected: WorkflowState) -> None:
        if row["state"] != expected.value:
            raise WorkflowConflictError(f"workflow is {row['state']}; expected {expected.value}")

    @staticmethod
    def _require_version(row: sqlite3.Row, expected: int | None) -> None:
        if expected is not None and int(row["version"]) != expected:
            raise WorkflowConflictError("workflow version is stale")

    @staticmethod
    def _update(
        connection: sqlite3.Connection,
        workflow_id: str,
        assignments: str,
        parameters: tuple[Any, ...],
    ) -> None:
        try:
            cursor = connection.execute(
                f"UPDATE workflows SET {assignments} WHERE workflow_id = ?",
                (*parameters, workflow_id),
            )
        except sqlite3.IntegrityError as exc:
            raise WorkflowConflictError("console workflow transition was rejected") from exc
        if cursor.rowcount != 1:
            raise WorkflowNotFoundError("console workflow was not found")

    @staticmethod
    def _workflow_from_row(row: sqlite3.Row) -> ConsoleWorkflow:
        try:
            proposal = (
                None
                if row["proposal_json"] is None
                else DraftProposal.model_validate_json(row["proposal_json"])
            )
            frozen = (
                None
                if row["frozen_request_json"] is None
                else FrozenCaptureRequest.model_validate_json(row["frozen_request_json"])
            )
            response = (
                None
                if row["capture_response_json"] is None
                else FrozenCaptureResponse.model_validate_json(row["capture_response_json"])
            )
            error_details = _optional_json_object(row["error_details_json"])
            return ConsoleWorkflow(
                workflow_id=row["workflow_id"],
                user_id=row["user_id"],
                state=row["state"],
                schema_id=row["schema_id"],
                source_text=row["source_text"],
                proposal=proposal,
                idempotency_key=row["idempotency_key"],
                frozen_request=frozen,
                capture_response=response,
                error_code=row["error_code"],
                error_details=error_details,
                created_at=datetime.fromisoformat(row["created_at"]),
                updated_at=datetime.fromisoformat(row["updated_at"]),
                expires_at=(
                    None if row["expires_at"] is None else datetime.fromisoformat(row["expires_at"])
                ),
                version=row["version"],
            )
        except (KeyError, TypeError, ValueError, ValidationError) as exc:
            raise ConsoleStateError("console workflow row failed validation") from exc


def _model_json(value: Any) -> str:
    return canonical_json(value.model_dump(mode="json"))


def _timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="microseconds")


def _optional_json_object(value: object) -> dict[str, JsonValue] | None:
    if value is None:
        return None
    if not isinstance(value, str | bytes | bytearray):
        raise ConsoleStateError("console error details are invalid")
    parsed = json.loads(value)
    if not isinstance(parsed, dict):
        raise ConsoleStateError("console error details must be an object")
    canonical_json(parsed)
    return parsed


assert CONSOLE_SCHEMA_VERSION >= 1
