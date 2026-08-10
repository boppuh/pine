from __future__ import annotations

import json
import sqlite3
import stat
import threading
from datetime import timedelta
from pathlib import Path

import pytest

from ledger.api import CaptureResponse
from ledger.console.errors import ConsoleStateError, WorkflowConflictError, WorkflowNotFoundError
from ledger.console.migrations import (
    _MIGRATION_1,
    CONSOLE_SCHEMA_VERSION,
    _migration_statements,
)
from ledger.console.models import CaptureInput, WorkflowState
from ledger.console.state import ConsoleStateStore
from ledger.extraction import DraftProposal, ExtractionResult, ExtractionStatus

from .conftest import MutableClock


def _reviewing(
    store: ConsoleStateStore,
    proposal: DraftProposal,
    *,
    user_id: str = "Ryan@Example.com",
):
    workflow = store.create_workflow(user_id=user_id, source_text=proposal.body)
    store.begin_extraction(workflow.workflow_id, user_id)
    return store.finish_extraction(
        workflow.workflow_id,
        user_id,
        ExtractionResult(status=ExtractionStatus.READY, proposal=proposal),
    )


def test_new_store_is_private_wal_versioned_and_reopenable(tmp_path: Path) -> None:
    path = tmp_path / "state" / "console.db"
    store = ConsoleStateStore(path)
    connection = store.connect()
    try:
        assert connection.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        assert connection.execute("PRAGMA user_version").fetchone()[0] == CONSOLE_SCHEMA_VERSION
        assert connection.execute("PRAGMA quick_check").fetchone()[0] == "ok"
    finally:
        connection.close()
    assert stat.S_IMODE(path.stat().st_mode) == 0o600

    reopened = ConsoleStateStore(path)
    assert reopened.get_status() == {"schema_version": CONSOLE_SCHEMA_VERSION}


def test_unversioned_or_newer_database_fails_closed(tmp_path: Path) -> None:
    unversioned = tmp_path / "unversioned.db"
    unversioned.touch(mode=0o600)
    connection = sqlite3.connect(unversioned)
    connection.execute("CREATE TABLE unknown(value TEXT)")
    connection.commit()
    connection.close()
    with pytest.raises(ConsoleStateError, match="unversioned"):
        ConsoleStateStore(unversioned)

    current = tmp_path / "current.db"
    store = ConsoleStateStore(current)
    connection = store.connect()
    connection.execute(
        "INSERT INTO console_schema_migrations(version, applied_at) VALUES (99, 'now')"
    )
    connection.execute("PRAGMA user_version = 99")
    connection.close()
    with pytest.raises(ConsoleStateError, match="newer"):
        ConsoleStateStore(current)


def test_concurrent_initialization_serializes_migration(tmp_path: Path) -> None:
    path = tmp_path / "concurrent" / "console.db"
    path.parent.mkdir(mode=0o700)
    path.touch(mode=0o600)
    barrier = threading.Barrier(2)
    results: list[ConsoleStateStore | BaseException] = []

    def initialize() -> None:
        barrier.wait()
        try:
            results.append(ConsoleStateStore(path))
        except BaseException as exc:
            results.append(exc)

    workers = [threading.Thread(target=initialize) for _ in range(2)]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join(5)

    assert len(results) == 2
    failures = [result for result in results if isinstance(result, BaseException)]
    assert failures == []
    assert ConsoleStateStore(path).get_status() == {"schema_version": CONSOLE_SCHEMA_VERSION}


def test_released_v1_database_migrates_sessions_without_reinitialization(tmp_path: Path) -> None:
    path = tmp_path / "v1" / "console.db"
    path.parent.mkdir(mode=0o700)
    connection = sqlite3.connect(path)
    for statement in _migration_statements(_MIGRATION_1):
        connection.execute(statement)
    connection.execute(
        "INSERT INTO console_schema_migrations(version, applied_at) VALUES (1, 'v1')"
    )
    connection.execute("PRAGMA user_version = 1")
    connection.commit()
    connection.close()
    path.chmod(0o600)

    store = ConsoleStateStore(path)
    migrated = store.connect()
    try:
        assert migrated.execute("PRAGMA user_version").fetchone()[0] == 2
        assert (
            migrated.execute(
                "SELECT COUNT(*) FROM sqlite_master "
                "WHERE type = 'table' AND name = 'console_sessions'"
            ).fetchone()[0]
            == 1
        )
    finally:
        migrated.close()


def test_state_path_rejects_ledger_symlink_and_permissive_parent(tmp_path: Path) -> None:
    ledger_dir = tmp_path / "vault" / ".ledger"
    ledger_dir.mkdir(parents=True)
    symlink = tmp_path / "state-link"
    symlink.symlink_to(ledger_dir, target_is_directory=True)

    with pytest.raises(ConsoleStateError, match="outside the authoritative ledger"):
        ConsoleStateStore(symlink / "console.db")
    assert not (ledger_dir / "console.db").exists()

    permissive = tmp_path / "permissive"
    permissive.mkdir(mode=0o755)
    permissive.chmod(0o755)
    with pytest.raises(ConsoleStateError, match="group/world"):
        ConsoleStateStore(permissive / "console.db")


def test_create_extract_review_and_cancel_discards_transient_content(
    console_store: ConsoleStateStore,
    proposal: DraftProposal,
) -> None:
    created = console_store.create_workflow(
        user_id=" Ryan@Example.COM ",
        source_text=proposal.body,
    )
    assert created.user_id == "ryan@example.com"
    assert created.state is WorkflowState.EDITING
    assert created.idempotency_key.startswith("console-")
    assert created.expires_at is not None

    extracting = console_store.begin_extraction(
        created.workflow_id,
        created.user_id,
        expected_version=created.version,
    )
    reviewing = console_store.finish_extraction(
        created.workflow_id,
        created.user_id,
        ExtractionResult(status=ExtractionStatus.READY, proposal=proposal),
    )
    with pytest.raises(WorkflowConflictError, match="stale"):
        console_store.cancel_workflow(
            created.workflow_id,
            created.user_id,
            expected_version=created.version,
        )
    cancelled = console_store.cancel_workflow(
        created.workflow_id,
        created.user_id,
        expected_version=reviewing.version,
    )

    assert extracting.state is WorkflowState.EXTRACTING
    assert reviewing.state is WorkflowState.REVIEWING
    assert reviewing.proposal == proposal
    assert cancelled.state is WorkflowState.CANCELLED
    assert cancelled.source_text is None
    assert cancelled.proposal is None


def test_ready_extraction_without_proposal_fails_with_typed_state_error(
    console_store: ConsoleStateStore,
) -> None:
    created = console_store.create_workflow(user_id="user@example.com", source_text="text")
    extracting = console_store.begin_extraction(created.workflow_id, created.user_id)
    malformed = ExtractionResult.model_construct(
        status=ExtractionStatus.READY,
        proposal=None,
        errors=(),
    )

    with pytest.raises(ConsoleStateError, match="lacks a proposal"):
        console_store.finish_extraction(
            extracting.workflow_id,
            extracting.user_id,
            malformed,
        )

    assert (
        console_store.get_workflow(extracting.workflow_id, extracting.user_id).state
        is WorkflowState.EXTRACTING
    )


def test_user_scope_does_not_reveal_foreign_workflow(
    console_store: ConsoleStateStore,
) -> None:
    created = console_store.create_workflow(user_id="owner@example.com", source_text="text")
    with pytest.raises(WorkflowNotFoundError, match="not found"):
        console_store.get_workflow(created.workflow_id, "other@example.com")


def test_expired_review_cannot_transition_before_cleanup(
    tmp_path: Path,
    clock: MutableClock,
    proposal: DraftProposal,
    capture_input: CaptureInput,
) -> None:
    store = ConsoleStateStore(
        tmp_path / "console.db",
        clock=clock,
        ordinary_retention=timedelta(hours=1),
    )
    reviewing = _reviewing(store, proposal)
    clock.advance(timedelta(hours=2))

    with pytest.raises(WorkflowNotFoundError, match="not found"):
        store.freeze_and_begin_submission(
            reviewing.workflow_id,
            reviewing.user_id,
            capture_input,
        )

    connection = store.connect()
    try:
        state = connection.execute(
            "SELECT state FROM workflows WHERE workflow_id = ?",
            (reviewing.workflow_id,),
        ).fetchone()[0]
    finally:
        connection.close()
    assert state == WorkflowState.REVIEWING.value


def test_failed_extraction_source_can_be_revised_only_while_editing(
    console_store: ConsoleStateStore,
    proposal: DraftProposal,
) -> None:
    created = console_store.create_workflow(
        user_id="owner@example.com",
        source_text="incomplete source",
    )
    extracting = console_store.begin_extraction(
        created.workflow_id,
        created.user_id,
        expected_version=created.version,
    )
    failed = console_store.fail_extraction(
        extracting.workflow_id,
        extracting.user_id,
        code="extraction_unable",
        details=("missing expected metrics",),
    )

    revised = console_store.revise_source(
        failed.workflow_id,
        failed.user_id,
        source_text="complete revised source",
        schema_id=failed.schema_id,
        expected_version=failed.version,
    )

    assert revised.state is WorkflowState.EDITING
    assert revised.source_text == "complete revised source"
    assert revised.error_code is None
    assert revised.error_details is None
    reviewing = console_store.begin_extraction(
        revised.workflow_id,
        revised.user_id,
        expected_version=revised.version,
    )
    reviewing = console_store.finish_extraction(
        reviewing.workflow_id,
        reviewing.user_id,
        ExtractionResult(status=ExtractionStatus.READY, proposal=proposal),
    )
    with pytest.raises(WorkflowConflictError, match="expected editing"):
        console_store.revise_source(
            reviewing.workflow_id,
            reviewing.user_id,
            source_text="too late",
            schema_id=reviewing.schema_id,
        )


def test_database_triggers_reject_identity_payload_and_transition_bypasses(
    console_store: ConsoleStateStore,
    proposal: DraftProposal,
    capture_input: CaptureInput,
) -> None:
    reviewing = _reviewing(console_store, proposal)
    submitting = console_store.freeze_and_begin_submission(
        reviewing.workflow_id,
        reviewing.user_id,
        capture_input,
    )
    assert submitting.frozen_request is not None
    original_frozen = submitting.frozen_request.model_dump(mode="json")
    connection = console_store.connect()
    try:
        attempts = (
            (
                "UPDATE workflows SET idempotency_key = ?, version = version + 1 "
                "WHERE workflow_id = ?",
                (f"console-{'0' * 36}", submitting.workflow_id),
            ),
            (
                "UPDATE workflows SET schema_id = ?, version = version + 1 WHERE workflow_id = ?",
                ("other/schema:1", submitting.workflow_id),
            ),
            (
                "UPDATE workflows SET frozen_request_json = ?, version = version + 1 "
                "WHERE workflow_id = ?",
                (json.dumps({"changed": True}), submitting.workflow_id),
            ),
            (
                "UPDATE workflows SET state = 'editing', version = version + 1 "
                "WHERE workflow_id = ?",
                (submitting.workflow_id,),
            ),
            (
                "UPDATE workflows SET state = 'terminal_failure', expires_at = NULL, "
                "version = version + 1 WHERE workflow_id = ?",
                (submitting.workflow_id,),
            ),
            (
                "DELETE FROM workflows WHERE workflow_id = ?",
                (submitting.workflow_id,),
            ),
        )
        for statement, parameters in attempts:
            with pytest.raises(sqlite3.IntegrityError):
                connection.execute(statement, parameters)
    finally:
        connection.close()
    observed = console_store.get_workflow(submitting.workflow_id, submitting.user_id)
    assert observed.frozen_request is not None
    assert observed.frozen_request.model_dump(mode="json") == original_frozen


def test_cleanup_removes_only_expired_approved_states(
    tmp_path: Path,
    clock: MutableClock,
    proposal: DraftProposal,
    capture_input: CaptureInput,
    capture_response: CaptureResponse,
) -> None:
    store = ConsoleStateStore(
        tmp_path / "console.db",
        clock=clock,
        ordinary_retention=timedelta(hours=1),
        receipt_retention=timedelta(hours=1),
    )
    editing = store.create_workflow(user_id="user@example.com", source_text="editing")
    reviewing = _reviewing(store, proposal, user_id="user@example.com")
    cancelled = store.create_workflow(user_id="user@example.com", source_text="cancelled")
    store.cancel_workflow(cancelled.workflow_id, cancelled.user_id)
    committed_review = _reviewing(store, proposal, user_id="user@example.com")
    committed_submit = store.freeze_and_begin_submission(
        committed_review.workflow_id,
        committed_review.user_id,
        capture_input,
    )
    store.record_capture_response(
        committed_submit.workflow_id,
        committed_submit.user_id,
        capture_response,
    )
    protected_review = _reviewing(store, proposal, user_id="user@example.com")
    protected = store.freeze_and_begin_submission(
        protected_review.workflow_id,
        protected_review.user_id,
        capture_input,
    )
    protected = store.record_capture_failure(
        protected.workflow_id,
        protected.user_id,
        state=WorkflowState.UNCERTAIN,
        code="timeout",
        details=("exact replay required",),
    )
    terminal_review = _reviewing(store, proposal, user_id="user@example.com")
    terminal_submit = store.freeze_and_begin_submission(
        terminal_review.workflow_id,
        terminal_review.user_id,
        capture_input,
    )
    terminal = store.record_capture_failure(
        terminal_submit.workflow_id,
        terminal_submit.user_id,
        state=WorkflowState.TERMINAL_FAILURE,
        code="invalid_forecast",
        details=("new workflow required",),
    )
    assert terminal.expires_at is not None
    clock.advance(timedelta(hours=2))

    assert store.cleanup_expired() == 5
    with pytest.raises(WorkflowNotFoundError):
        store.get_workflow(editing.workflow_id, editing.user_id)
    with pytest.raises(WorkflowNotFoundError):
        store.get_workflow(reviewing.workflow_id, reviewing.user_id)
    preserved = store.get_workflow(protected.workflow_id, protected.user_id)
    assert preserved.state is WorkflowState.UNCERTAIN
    assert preserved.expires_at is None
    connection = store.connect()
    try:
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM workflows WHERE workflow_id = ?",
                (terminal.workflow_id,),
            ).fetchone()[0]
            == 0
        )
    finally:
        connection.close()
