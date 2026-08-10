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
from ledger.read_models import (
    IntegrityState,
    LedgerStatus,
    PredictionDetail,
    PredictionPage,
    PredictionSummary,
    ResultState,
)
from ledger.run import RunState


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
        committed_at = datetime(2026, 8, 7, 12, 1, tzinfo=UTC)
        summary = PredictionSummary(
            prediction_id=response.prediction_id,
            run_id=response.run_id,
            registration_status=RegistrationStatus.PREREGISTERED,
            status=PredictionStatus.OPEN,
            transaction_state="committed",
            strategy_id=proposal.forecast.strategy_id,
            schema_id=response.schema_id,
            out_of_sample_window=proposal.forecast.out_of_sample_window.model_dump(mode="json"),
            created_at=datetime(2026, 8, 7, 12, 0, tzinfo=UTC),
            committed_at=committed_at,
            run_state=RunState.REGISTERED,
            result_state=ResultState.ABSENT,
            integrity_state=IntegrityState.VERIFIED,
        )
        self.prediction_page = PredictionPage(items=(summary,), next_cursor=None)
        self.prediction_detail = PredictionDetail.model_validate(
            {
                "prediction_id": response.prediction_id,
                "run_id": response.run_id,
                "schema_id": response.schema_id,
                "schema_hash": response.schema_hash,
                "registration_status": "preregistered",
                "forecast": proposal.forecast.model_dump(mode="json"),
                "decision": proposal.decision,
                "snapshot_ref": response.snapshot_ref,
                "snapshot": {
                    "strategy_id": proposal.forecast.strategy_id,
                    "strategy_spec_hash": f"sha256:{'c' * 64}",
                    "git_commit": "d" * 40,
                    "parameter_count": 4,
                    "data_as_of_version": "2026-08-07T11:59:00Z",
                    "dataset_version": f"sha256:{'e' * 64}",
                    "in_sample_window": proposal.forecast.in_sample_window.model_dump(mode="json"),
                    "out_of_sample_window": proposal.forecast.out_of_sample_window.model_dump(
                        mode="json"
                    ),
                    "cost_model_version": "cost-v1",
                    "slippage_model_version": "slippage-v1",
                    "metric_definition_version": "metrics-v1",
                    "engine_version": "msm-v1",
                    "random_seed": 17,
                    "captured_at": "2026-08-07T12:00:30Z",
                },
                "lineage": proposal.lineage,
                "immutable_hash": response.immutable_hash,
                "body": proposal.body,
                "status": "open",
                "outcome": None,
                "grade": None,
                "resolution_metadata": None,
                "transaction_state": "committed",
                "created_at": "2026-08-07T12:00:00Z",
                "committed_at": committed_at,
                "run": {
                    "run_id": response.run_id,
                    "prediction_id": response.prediction_id,
                    "started_at": "2026-08-07T12:00:00Z",
                    "state": "registered",
                    "execution_started_at": None,
                    "completed_at": None,
                    "exit_code": None,
                    "failure_note": None,
                    "binding": None,
                },
                "result": None,
                "integrity_violations": [],
                "integrity_state": "verified",
            }
        )
        self.ledger_status = LedgerStatus(
            registry_version=7,
            committed_predictions=1,
            quarantined_predictions=0,
            integrity_violations=0,
            run_results=0,
        )
        self.prediction_list_requests: list[dict[str, object]] = []
        self.prediction_detail_requests: list[str] = []
        self.prediction_list_outcomes: list[PredictionPage | BaseException] = []
        self.prediction_detail_outcomes: list[PredictionDetail | BaseException] = []
        self.status_outcomes: list[LedgerStatus | BaseException] = []

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
        capture = self.capture_requests[-1] if self.capture_requests else None
        return AuthoritativeReceipt(
            prediction_id=self.response.prediction_id,
            run_id=self.response.run_id,
            registration_status=RegistrationStatus.PREREGISTERED,
            forecast=self.proposal.forecast if capture is None else capture.forecast,
            decision=self.proposal.decision if capture is None else capture.decision,
            lineage=self.proposal.lineage if capture is None else capture.lineage,
            status=PredictionStatus.OPEN,
            transaction_state="committed",
            schema_id=self.response.schema_id,
            schema_hash=self.response.schema_hash,
            immutable_hash=self.response.immutable_hash,
            snapshot_ref=self.response.snapshot_ref,
            committed_at=datetime(2026, 8, 7, 12, 1, tzinfo=UTC),
        )

    def list_predictions(
        self,
        *,
        limit: int = 25,
        cursor: str | None = None,
        registration_status: RegistrationStatus | None = None,
        status: PredictionStatus | None = None,
        strategy_id: str | None = None,
        result_state: ResultState | None = None,
    ) -> PredictionPage:
        self.prediction_list_requests.append(
            {
                "limit": limit,
                "cursor": cursor,
                "registration_status": registration_status,
                "status": status,
                "strategy_id": strategy_id,
                "result_state": result_state,
            }
        )
        if self.prediction_list_outcomes:
            outcome = self.prediction_list_outcomes.pop(0)
            if isinstance(outcome, BaseException):
                raise outcome
            return outcome
        return self.prediction_page

    def get_prediction(self, prediction_id: str) -> PredictionDetail:
        self.prediction_detail_requests.append(prediction_id)
        if self.prediction_detail_outcomes:
            outcome = self.prediction_detail_outcomes.pop(0)
            if isinstance(outcome, BaseException):
                raise outcome
            return outcome
        return self.prediction_detail

    def get_status(self) -> LedgerStatus:
        if self.status_outcomes:
            outcome = self.status_outcomes.pop(0)
            if isinstance(outcome, BaseException):
                raise outcome
            return outcome
        return self.ledger_status


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
