"""Recoverable orchestration over the console store and loopback backend."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any

from ledger.console.backend_client import ConsoleBackend
from ledger.console.errors import (
    BackendDomainError,
    BackendError,
    FailureDisposition,
)
from ledger.console.models import CaptureInput, ConsoleWorkflow, WorkflowState
from ledger.console.state import ConsoleStateStore
from ledger.extraction import HypothesisExtractionRequest

logger = logging.getLogger(__name__)


class WorkflowService:
    """Coordinate exact capture attempts without acquiring ledger authority."""

    def __init__(self, store: ConsoleStateStore, backend: ConsoleBackend) -> None:
        self.store = store
        self.backend = backend

    def create(
        self,
        *,
        user_id: str,
        source_text: str,
        schema_id: str = "finance/strategy-edge:1",
    ) -> ConsoleWorkflow:
        """Create an expiring editing workflow."""

        return self.store.create_workflow(
            user_id=user_id,
            source_text=source_text,
            schema_id=schema_id,
        )

    def extract(
        self,
        workflow_id: str,
        user_id: str,
        *,
        expected_version: int | None = None,
    ) -> ConsoleWorkflow:
        """Run side-effect-free extraction and persist only its proposal or safe error."""

        workflow = self.store.begin_extraction(
            workflow_id,
            user_id,
            expected_version=expected_version,
        )
        if workflow.source_text is None:
            raise RuntimeError("editing workflow unexpectedly lacks source text")
        request = HypothesisExtractionRequest(
            text=workflow.source_text,
            schema_id=workflow.schema_id,
        )
        try:
            result = self.backend.create_draft(request)
        except BackendDomainError as exc:
            logger.warning(
                "console_extraction_backend_rejected",
                extra={"workflow_id": workflow_id, "error_code": exc.code},
            )
            return self.store.fail_extraction(
                workflow_id,
                user_id,
                code=exc.code,
                details=exc.details,
            )
        except BackendError:
            logger.warning(
                "console_extraction_backend_unavailable",
                extra={"workflow_id": workflow_id},
            )
            return self.store.fail_extraction(
                workflow_id,
                user_id,
                code="backend_unavailable",
                details=("extraction may be started again",),
            )
        except Exception as exc:
            logger.exception(
                "console_extraction_unexpected_failure",
                extra={"workflow_id": workflow_id},
                exc_info=_redacted_exception(exc),
            )
            return self.store.fail_extraction(
                workflow_id,
                user_id,
                code="console_internal_error",
                details=("extraction may be started again",),
            )
        return self.store.finish_extraction(workflow_id, user_id, result)

    def confirm(
        self,
        workflow_id: str,
        user_id: str,
        capture: CaptureInput | Mapping[str, Any],
        *,
        expected_version: int | None = None,
    ) -> ConsoleWorkflow:
        """Freeze the first request, call the backend once, and durably classify outcome."""

        workflow = self.store.freeze_and_begin_submission(
            workflow_id,
            user_id,
            capture,
            expected_version=expected_version,
        )
        if workflow.state is WorkflowState.COMMITTED:
            return workflow
        return self._submit(workflow, user_id, retry=False)

    def retry(
        self,
        workflow_id: str,
        user_id: str,
        *,
        expected_version: int | None = None,
    ) -> ConsoleWorkflow:
        """Replay the stored request exactly; caller-supplied capture fields are impossible."""

        workflow = self.store.begin_retry(
            workflow_id,
            user_id,
            expected_version=expected_version,
        )
        return self._submit(workflow, user_id, retry=True)

    def cancel(
        self,
        workflow_id: str,
        user_id: str,
        *,
        expected_version: int | None = None,
    ) -> ConsoleWorkflow:
        """Cancel a pre-confirmation workflow without contacting the backend."""

        return self.store.cancel_workflow(
            workflow_id,
            user_id,
            expected_version=expected_version,
        )

    def _submit(
        self,
        workflow: ConsoleWorkflow,
        user_id: str,
        *,
        retry: bool,
    ) -> ConsoleWorkflow:
        request = workflow.frozen_request
        if request is None:
            raise RuntimeError("submitting workflow unexpectedly lacks a frozen request")
        logger.info(
            "console_capture_attempt",
            extra={"workflow_id": workflow.workflow_id, "retry": retry},
        )
        try:
            response = self.backend.capture(request.to_backend_request())
        except BackendDomainError as exc:
            state = _failure_state(exc.disposition)
            logger.warning(
                "console_capture_backend_failure",
                extra={
                    "workflow_id": workflow.workflow_id,
                    "error_code": exc.code,
                    "outcome": state.value,
                },
            )
            return self.store.record_capture_failure(
                workflow.workflow_id,
                user_id,
                state=state,
                code=exc.code,
                details=exc.details,
            )
        except BackendError:
            logger.warning(
                "console_capture_outcome_uncertain",
                extra={"workflow_id": workflow.workflow_id},
            )
            return self.store.record_capture_failure(
                workflow.workflow_id,
                user_id,
                state=WorkflowState.UNCERTAIN,
                code="backend_response_unverified",
                details=("capture outcome requires exact replay",),
            )
        except Exception as exc:
            logger.exception(
                "console_capture_unexpected_failure",
                extra={"workflow_id": workflow.workflow_id},
                exc_info=_redacted_exception(exc),
            )
            return self.store.record_capture_failure(
                workflow.workflow_id,
                user_id,
                state=WorkflowState.UNCERTAIN,
                code="console_internal_error",
                details=("capture outcome requires exact replay",),
            )
        committed = self.store.record_capture_response(workflow.workflow_id, user_id, response)
        logger.info(
            "console_capture_receipt_persisted",
            extra={
                "workflow_id": workflow.workflow_id,
                "prediction_id": response.prediction_id,
                "run_id": response.run_id,
            },
        )
        return committed


def _failure_state(disposition: FailureDisposition) -> WorkflowState:
    if disposition is FailureDisposition.TERMINAL:
        return WorkflowState.TERMINAL_FAILURE
    if disposition is FailureDisposition.RETRYABLE:
        return WorkflowState.RETRYABLE_FAILURE
    return WorkflowState.UNCERTAIN


def _redacted_exception(exc: Exception) -> RuntimeError:
    """Retain an internal traceback without logging an exception's unsafe message."""

    sanitized = RuntimeError(f"{type(exc).__name__}: details redacted")
    return sanitized.with_traceback(exc.__traceback__)
