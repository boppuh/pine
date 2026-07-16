"""Atomic preregistered capture transaction."""

from __future__ import annotations

import logging
import uuid
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pydantic import JsonValue, TypeAdapter, ValidationError

from ledger.errors import (
    ForecastValidationError,
    FreshWindowError,
    IdempotencyConflictError,
    SnapshotCaptureError,
)
from ledger.integrity import (
    FrozenDict,
    PredictionDraft,
    PredictionStatus,
    PreregisteredCaptureRequest,
    RegistrationStatus,
)
from ledger.json_utils import sha256_json
from ledger.locking import ledger_lock
from ledger.snapshot import PendingPrediction, SnapshotProvider
from ledger.writer import LedgerWriter, StagedWrite, WriteResult

logger = logging.getLogger(__name__)
_SNAPSHOT_ADAPTER = TypeAdapter(dict[str, JsonValue])


class CaptureService:
    """Create a preregistered ledger envelope before any backtest executes."""

    def __init__(
        self,
        vault_root: str | Path,
        snapshot_provider: SnapshotProvider,
        *,
        writer: LedgerWriter | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.writer = writer or LedgerWriter(vault_root)
        resolved_root = Path(vault_root).resolve()
        if self.writer.vault_root != resolved_root:
            raise ValueError("writer and capture service must use the same vault root")
        self.snapshot_provider = snapshot_provider
        self.registry = self.writer.registry
        self.schema_registry = self.writer.schema_registry
        self.clock = clock or (lambda: datetime.now(UTC))

    def check_fresh_window(
        self,
        request: PreregisteredCaptureRequest | Mapping[str, Any],
    ) -> bool:
        """Perform the advisory, pre-confirmation fresh-window check."""

        capture = self._coerce_request(request)
        window = capture.forecast.out_of_sample_window
        return not self.registry.window_overlaps_touched(
            self._family_id(capture),
            window.start,
            window.end,
        )

    def capture(
        self,
        request: PreregisteredCaptureRequest | Mapping[str, Any],
    ) -> WriteResult:
        """Capture and commit a confirmed hypothesis as one serialized transaction."""

        capture = self._coerce_request(request)
        request_hash = self._request_hash(capture)

        with ledger_lock(self.writer.ledger_dir, timeout=self.writer.lock_timeout):
            self.writer.recover_unfinished_transactions_locked()
            connection = self.registry.connect()
            staged: StagedWrite | None = None
            try:
                connection.execute("BEGIN IMMEDIATE")
                existing = self.registry.get_prediction_by_idempotency_key(
                    capture.idempotency_key,
                    connection=connection,
                )
                if existing is not None:
                    if existing["request_hash"] != request_hash:
                        raise IdempotencyConflictError(
                            "idempotency key is already bound to a different capture request"
                        )
                    result = self.writer.result_for_row(existing)
                    connection.commit()
                    return result

                schema = self.schema_registry.load(capture.schema_id)
                forecast = capture.forecast.model_dump(mode="json")
                valid, errors = self.schema_registry.validate_schema(forecast, schema)
                if not valid:
                    raise ForecastValidationError(errors)
                schema_hash = self.schema_registry.hash(schema)

                window = capture.forecast.out_of_sample_window
                overlap = self.registry.find_touched_window_overlap(
                    self._family_id(capture),
                    window.start,
                    window.end,
                    connection=connection,
                )
                if overlap is not None:
                    raise FreshWindowError(
                        "out-of-sample window overlaps previously observed data "
                        f"[{overlap['window_start']}, {overlap['window_end']}]"
                    )

                decision_at = self._clock_time()
                prediction_id = f"pred_{uuid.uuid4().hex}"
                run_id = f"run_{uuid.uuid4().hex}"
                pending = PendingPrediction(
                    prediction_id=prediction_id,
                    run_id=run_id,
                    schema_id=capture.schema_id,
                    registration_status=RegistrationStatus.PREREGISTERED,
                    forecast=capture.forecast,
                    decision=capture.decision,
                    lineage=FrozenDict(capture.lineage),
                    created_at=decision_at,
                )
                snapshot = self._capture_snapshot(pending, decision_at)
                committed_at = self._clock_time()
                draft = PredictionDraft(
                    prediction_id=prediction_id,
                    run_id=run_id,
                    schema_id=capture.schema_id,
                    registration_status=RegistrationStatus.PREREGISTERED,
                    forecast=capture.forecast,
                    decision=capture.decision,
                    snapshot=snapshot,
                    lineage=capture.lineage,
                    body=capture.body,
                    status=PredictionStatus.OPEN,
                    created_at=decision_at,
                )
                staged = self.writer.stage_in_transaction(
                    draft,
                    connection=connection,
                    expected_schema_hash=schema_hash,
                    committed_at=committed_at,
                )
                self.registry.register_capture_request(
                    idempotency_key=capture.idempotency_key,
                    request_hash=request_hash,
                    prediction_id=prediction_id,
                    created_at=decision_at,
                    connection=connection,
                )
                connection.commit()
            except BaseException:
                if connection.in_transaction:
                    connection.rollback()
                if staged is not None:
                    try:
                        committed = self.registry.is_committed(staged.result.prediction_id)
                    except Exception:
                        committed = None
                    if committed is True:
                        staged.finalize()
                    elif committed is False:
                        staged.rollback()
                logger.exception(
                    "preregistered_capture_failed",
                    extra={"idempotency_key": capture.idempotency_key},
                )
                raise
            else:
                if staged is None:  # pragma: no cover - defensive invariant
                    raise RuntimeError("capture committed without a staged write")
                staged.finalize()
                logger.info(
                    "preregistered_capture_committed",
                    extra={
                        "prediction_id": staged.result.prediction_id,
                        "run_id": staged.result.run_id,
                    },
                )
                return staged.result
            finally:
                connection.close()

    def _capture_snapshot(
        self,
        prediction: PendingPrediction,
        decision_at: datetime,
    ) -> dict[str, JsonValue]:
        try:
            snapshot = self.snapshot_provider.capture_snapshot(prediction, decision_at)
            if not isinstance(snapshot, Mapping):
                raise TypeError("snapshot provider must return a mapping")
            validated = _SNAPSHOT_ADAPTER.validate_python(dict(snapshot))
            if not validated:
                raise ValueError("snapshot provider returned an empty snapshot")
            return validated
        except SnapshotCaptureError:
            raise
        except Exception as exc:
            raise SnapshotCaptureError("snapshot capture failed") from exc

    def _clock_time(self) -> datetime:
        value = self.clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise SnapshotCaptureError("capture clock must return a timezone-aware value")
        return value

    @staticmethod
    def _coerce_request(
        request: PreregisteredCaptureRequest | Mapping[str, Any],
    ) -> PreregisteredCaptureRequest:
        if isinstance(request, PreregisteredCaptureRequest):
            return request
        try:
            return PreregisteredCaptureRequest.model_validate(request)
        except ValidationError as exc:
            errors = [
                f"$.{'.'.join(str(item) for item in error['loc'])}: {error['msg']}"
                for error in exc.errors()
            ]
            raise ForecastValidationError(errors) from exc

    @staticmethod
    def _family_id(request: PreregisteredCaptureRequest) -> str:
        family_id = request.lineage["family_id"]
        assert isinstance(family_id, str)
        return family_id

    @staticmethod
    def _request_hash(request: PreregisteredCaptureRequest) -> str:
        return sha256_json(
            {
                "schema_id": request.schema_id,
                "registration_status": RegistrationStatus.PREREGISTERED.value,
                "forecast": request.forecast.model_dump(mode="json"),
                "decision": request.decision,
                "lineage": request.lineage,
                "body": request.body,
            }
        )
