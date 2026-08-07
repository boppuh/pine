from __future__ import annotations

import hashlib
import logging
import sqlite3
import threading
from pathlib import Path

import pytest
from pydantic import ValidationError

from ledger.api import CaptureResponse
from ledger.console.errors import (
    BackendDomainError,
    BackendTransportError,
    WorkflowConflictError,
)
from ledger.console.models import CaptureInput, WorkflowState
from ledger.console.state import ConsoleStateStore
from ledger.console.workflow import WorkflowService
from ledger.errors import IntegrityError
from ledger.extraction import DraftProposal
from ledger.integrity import PreregisteredCaptureRequest
from ledger.json_utils import canonical_json

from .conftest import FakeBackend


def _ready_workflow(
    service: WorkflowService,
    proposal: DraftProposal,
    *,
    user_id: str = "user@example.com",
):
    created = service.create(user_id=user_id, source_text=proposal.body)
    return service.extract(
        created.workflow_id,
        user_id,
        expected_version=created.version,
    )


def test_successful_confirmation_freezes_request_and_receipt_once(
    console_store: ConsoleStateStore,
    fake_backend: FakeBackend,
    proposal: DraftProposal,
    capture_input: CaptureInput,
    capture_response: CaptureResponse,
) -> None:
    service = WorkflowService(console_store, fake_backend)
    reviewing = _ready_workflow(service, proposal)

    committed = service.confirm(
        reviewing.workflow_id,
        reviewing.user_id,
        capture_input,
        expected_version=reviewing.version,
    )
    repeated = service.confirm(
        reviewing.workflow_id,
        reviewing.user_id,
        {"this": "is ignored after commit"},
    )

    assert committed.state is WorkflowState.COMMITTED
    assert committed.capture_response is not None
    assert committed.capture_response.model_dump(mode="json") == capture_response.model_dump(
        mode="json"
    )
    assert committed.frozen_request is not None
    assert committed.frozen_request.idempotency_key == reviewing.idempotency_key
    with pytest.raises(IntegrityError, match="immutable"):
        committed.frozen_request.lineage["family_id"] = "changed"
    with pytest.raises(ValidationError, match="frozen"):
        committed.capture_response.created = False
    with pytest.raises(IntegrityError, match="field updates"):
        committed.model_copy(update={"idempotency_key": "console-changed"})
    assert committed.expires_at is not None
    assert repeated == committed
    assert len(fake_backend.capture_requests) == 1
    connection = console_store.connect()
    try:
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                """
                UPDATE workflows
                SET capture_response_json = ?, version = version + 1
                WHERE workflow_id = ?
                """,
                ('{"changed":true}', committed.workflow_id),
            )
    finally:
        connection.close()


def test_lost_response_retry_replays_byte_equivalent_frozen_request(
    console_store: ConsoleStateStore,
    fake_backend: FakeBackend,
    proposal: DraftProposal,
    capture_input: CaptureInput,
    capture_response: CaptureResponse,
) -> None:
    fake_backend.capture_outcomes = [
        BackendTransportError("connection reset"),
        capture_response,
    ]
    service = WorkflowService(console_store, fake_backend)
    reviewing = _ready_workflow(service, proposal)

    uncertain = service.confirm(reviewing.workflow_id, reviewing.user_id, capture_input)
    retried = service.retry(
        reviewing.workflow_id,
        reviewing.user_id,
        expected_version=uncertain.version,
    )

    assert uncertain.state is WorkflowState.UNCERTAIN
    assert uncertain.expires_at is None
    assert retried.state is WorkflowState.COMMITTED
    assert len(fake_backend.capture_requests) == 2
    request_bytes = [
        canonical_json(item.model_dump(mode="json")).encode()
        for item in fake_backend.capture_requests
    ]
    assert request_bytes[0] == request_bytes[1]
    assert uncertain.frozen_request == retried.frozen_request


@pytest.mark.parametrize(
    ("code", "expected_state", "retry_allowed"),
    [
        ("snapshot_unavailable", WorkflowState.RETRYABLE_FAILURE, True),
        ("unauthorized", WorkflowState.RETRYABLE_FAILURE, True),
        ("invalid_forecast", WorkflowState.TERMINAL_FAILURE, False),
        ("fresh_window_conflict", WorkflowState.TERMINAL_FAILURE, False),
        ("idempotency_conflict", WorkflowState.TERMINAL_FAILURE, False),
        ("integrity_error", WorkflowState.TERMINAL_FAILURE, False),
        ("internal_error", WorkflowState.UNCERTAIN, True),
        ("future_unknown_code", WorkflowState.UNCERTAIN, True),
    ],
)
def test_backend_failures_are_classified_without_changing_frozen_request(
    console_store: ConsoleStateStore,
    fake_backend: FakeBackend,
    proposal: DraftProposal,
    capture_input: CaptureInput,
    code: str,
    expected_state: WorkflowState,
    retry_allowed: bool,
) -> None:
    fake_backend.capture_outcomes = [
        BackendDomainError(
            status_code=500,
            code=code,
            message="safe message",
            details=("safe detail",),
        )
    ]
    service = WorkflowService(console_store, fake_backend)
    reviewing = _ready_workflow(service, proposal)

    failed = service.confirm(reviewing.workflow_id, reviewing.user_id, capture_input)

    assert failed.state is expected_state
    assert failed.error_code == code
    assert failed.frozen_request is not None
    if retry_allowed:
        fake_backend.capture_outcomes = [fake_backend.response]
        assert service.retry(failed.workflow_id, failed.user_id).state is WorkflowState.COMMITTED
    else:
        with pytest.raises(WorkflowConflictError, match="not eligible"):
            service.retry(failed.workflow_id, failed.user_id)


def test_unexpected_capture_failure_logs_a_redacted_traceback(
    console_store: ConsoleStateStore,
    fake_backend: FakeBackend,
    proposal: DraftProposal,
    capture_input: CaptureInput,
    caplog: pytest.LogCaptureFixture,
) -> None:
    unsafe_message = "backend-token-sensitive-value"
    fake_backend.capture_outcomes = [RuntimeError(unsafe_message)]
    service = WorkflowService(console_store, fake_backend)
    reviewing = _ready_workflow(service, proposal)

    with caplog.at_level(logging.ERROR, logger="ledger.console.workflow"):
        failed = service.confirm(reviewing.workflow_id, reviewing.user_id, capture_input)

    assert failed.state is WorkflowState.UNCERTAIN
    assert unsafe_message not in caplog.text
    assert "RuntimeError: details redacted" in caplog.text


def test_process_interruption_is_recovered_to_uncertain_without_backend_lookup(
    console_store: ConsoleStateStore,
    fake_backend: FakeBackend,
    proposal: DraftProposal,
    capture_input: CaptureInput,
) -> None:
    fake_backend.capture_outcomes = [KeyboardInterrupt()]
    service = WorkflowService(console_store, fake_backend)
    reviewing = _ready_workflow(service, proposal)

    with pytest.raises(KeyboardInterrupt):
        service.confirm(reviewing.workflow_id, reviewing.user_id, capture_input)
    interrupted = console_store.get_workflow(reviewing.workflow_id, reviewing.user_id)
    assert interrupted.frozen_request is not None
    frozen_json = canonical_json(interrupted.frozen_request.model_dump(mode="json"))
    assert interrupted.state is WorkflowState.SUBMITTING

    recovered_counts = console_store.recover_abandoned_workflows()
    recovered = console_store.get_workflow(reviewing.workflow_id, reviewing.user_id)

    assert recovered_counts["submitting_to_uncertain"] == 1
    assert recovered.state is WorkflowState.UNCERTAIN
    assert recovered.expires_at is None
    assert recovered.frozen_request is not None
    assert canonical_json(recovered.frozen_request.model_dump(mode="json")) == frozen_json
    assert len(fake_backend.capture_requests) == 1


def test_concurrent_double_confirm_makes_one_backend_call(
    console_store: ConsoleStateStore,
    proposal: DraftProposal,
    capture_input: CaptureInput,
    capture_response: CaptureResponse,
) -> None:
    started = threading.Event()
    release = threading.Event()

    class BlockingBackend(FakeBackend):
        def capture(self, request: PreregisteredCaptureRequest) -> CaptureResponse:
            self.capture_requests.append(request)
            started.set()
            assert release.wait(5)
            return self.response

    backend = BlockingBackend(proposal, capture_response)
    service = WorkflowService(console_store, backend)
    reviewing = _ready_workflow(service, proposal)
    results: list[object] = []

    def first_confirm() -> None:
        try:
            results.append(
                service.confirm(
                    reviewing.workflow_id,
                    reviewing.user_id,
                    capture_input,
                    expected_version=reviewing.version,
                )
            )
        except BaseException as exc:
            results.append(exc)

    worker = threading.Thread(target=first_confirm)
    worker.start()
    assert started.wait(5)
    with pytest.raises(WorkflowConflictError, match="expected reviewing"):
        service.confirm(
            reviewing.workflow_id,
            reviewing.user_id,
            capture_input,
            expected_version=reviewing.version,
        )
    release.set()
    worker.join(5)

    assert len(backend.capture_requests) == 1
    assert len(results) == 1
    assert not isinstance(results[0], BaseException)
    assert results[0].state is WorkflowState.COMMITTED  # type: ignore[union-attr]


def test_console_workflow_never_writes_the_vault(
    tmp_path: Path,
    vault: Path,
    fake_backend: FakeBackend,
    proposal: DraftProposal,
    capture_input: CaptureInput,
) -> None:
    before = {
        item.relative_to(vault).as_posix(): hashlib.sha256(item.read_bytes()).hexdigest()
        for item in vault.rglob("*")
        if item.is_file()
    }
    store = ConsoleStateStore(tmp_path / "outside-vault" / "console.db")
    service = WorkflowService(store, fake_backend)
    reviewing = _ready_workflow(service, proposal)

    service.confirm(reviewing.workflow_id, reviewing.user_id, capture_input)

    after = {
        item.relative_to(vault).as_posix(): hashlib.sha256(item.read_bytes()).hexdigest()
        for item in vault.rglob("*")
        if item.is_file()
    }
    assert after == before
