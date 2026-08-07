from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from ledger.api import CaptureResponse, HealthResponse
from ledger.console.models import CaptureInput
from ledger.console.state import ConsoleStateStore
from ledger.extraction import (
    DraftProposal,
    ExtractionResult,
    ExtractionStatus,
    HypothesisExtractionRequest,
)
from ledger.integrity import PreregisteredCaptureRequest


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
        self.capture_requests: list[PreregisteredCaptureRequest] = []
        self.draft_requests: list[HypothesisExtractionRequest] = []

    def health(self) -> HealthResponse:
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
