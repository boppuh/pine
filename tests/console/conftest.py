from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from ledger.api import CaptureResponse, HealthResponse
from ledger.console.backend_client import AuthoritativeReceipt
from ledger.console.models import CaptureInput
from ledger.console.state import ConsoleStateStore
from ledger.extraction import (
    DraftProposal,
    ExtractionResult,
    ExtractionStatus,
    HypothesisExtractionRequest,
)
from ledger.integrity import PredictionStatus, PreregisteredCaptureRequest, RegistrationStatus


class MutableClock:
    def __init__(self) -> None:
        self.value = datetime(2026, 8, 7, 12, 0, tzinfo=UTC)

    def __call__(self) -> datetime:
        return self.value

    def advance(self, value: timedelta) -> None:
        self.value += value


class FakeBackend:
    def __init__(self, proposal: DraftProposal, response: CaptureResponse) -> None:
        self.proposal = proposal
        self.response = response
        self.capture_outcomes: list[CaptureResponse | BaseException] = []
        self.draft_outcomes: list[ExtractionResult | BaseException] = []
        self.ready_outcomes: list[HealthResponse | BaseException] = []
        self.capture_requests: list[PreregisteredCaptureRequest] = []
        self.draft_requests: list[HypothesisExtractionRequest] = []
        self.receipt_requests: list[str] = []
        self.receipt_outcomes: list[AuthoritativeReceipt | BaseException] = []

    def health(self) -> HealthResponse:
        return HealthResponse()

    def ready(self) -> HealthResponse:
        if self.ready_outcomes:
            outcome = self.ready_outcomes.pop(0)
            if isinstance(outcome, BaseException):
                raise outcome
            assert isinstance(outcome, HealthResponse)
            return outcome
        return HealthResponse()

    def create_draft(self, request: HypothesisExtractionRequest) -> ExtractionResult:
        self.draft_requests.append(request)
        if self.draft_outcomes:
            outcome = self.draft_outcomes.pop(0)
            if isinstance(outcome, BaseException):
                raise outcome
            assert isinstance(outcome, ExtractionResult)
            return outcome
        return ExtractionResult(status=ExtractionStatus.READY, proposal=self.proposal)

    def capture(self, request: PreregisteredCaptureRequest) -> CaptureResponse:
        self.capture_requests.append(request)
        if self.capture_outcomes:
            outcome = self.capture_outcomes.pop(0)
            if isinstance(outcome, BaseException):
                raise outcome
            assert isinstance(outcome, CaptureResponse)
            return outcome
        return self.response

    def get_receipt(self, prediction_id: str) -> AuthoritativeReceipt:
        self.receipt_requests.append(prediction_id)
        if self.receipt_outcomes:
            outcome = self.receipt_outcomes.pop(0)
            if isinstance(outcome, BaseException):
                raise outcome
            return outcome
        return AuthoritativeReceipt(
            prediction_id=self.response.prediction_id,
            run_id=self.response.run_id,
            registration_status=RegistrationStatus.PREREGISTERED,
            status=PredictionStatus.OPEN,
            transaction_state="committed",
            schema_id=self.response.schema_id,
            schema_hash=self.response.schema_hash,
            immutable_hash=self.response.immutable_hash,
            snapshot_ref=self.response.snapshot_ref,
            committed_at=datetime(2026, 8, 7, 12, 1, tzinfo=UTC),
        )


@pytest.fixture
def clock() -> MutableClock:
    return MutableClock()


@pytest.fixture
def console_store(tmp_path: Path, clock: MutableClock) -> ConsoleStateStore:
    return ConsoleStateStore(tmp_path / "console-state" / "console.db", clock=clock)


@pytest.fixture
def proposal(valid_forecast: Mapping[str, Any]) -> DraftProposal:
    return DraftProposal.model_validate(
        {
            "schema_id": "finance/strategy-edge:1",
            "schema_hash": f"sha256:{'a' * 64}",
            "forecast": valid_forecast,
            "decision": "Run the frozen specification against untouched OOS data.",
            "lineage": {"family_id": "fam_console"},
            "body": "# Console hypothesis\n\nResearch context.",
            "fresh_window": True,
        }
    )


@pytest.fixture
def capture_input(proposal: DraftProposal) -> CaptureInput:
    return CaptureInput(
        schema_id=proposal.schema_id,
        schema_hash=proposal.schema_hash,
        forecast=proposal.forecast,
        decision=proposal.decision,
        lineage=proposal.lineage,
        body=proposal.body,
    )


@pytest.fixture
def capture_response() -> CaptureResponse:
    return CaptureResponse(
        prediction_id="pred_console_01",
        run_id="run_console_01",
        record_ref="predictions/pred_console_01.md",
        snapshot_ref=".ledger/snapshots/pred_console_01.json",
        schema_id="finance/strategy-edge:1",
        schema_hash=f"sha256:{'a' * 64}",
        immutable_hash=f"sha256:{'b' * 64}",
        created=True,
    )


@pytest.fixture
def fake_backend(
    proposal: DraftProposal,
    capture_response: CaptureResponse,
) -> FakeBackend:
    return FakeBackend(proposal, capture_response)
