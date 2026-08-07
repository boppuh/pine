"""Verifier-backed, side-effect-free projections for the ledger read API."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import sqlite3
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from pydantic import JsonValue, ValidationError

from ledger.errors import (
    IntegrityError,
    PredictionNotFoundError,
    ReadCursorError,
    SchemaNotFoundError,
)
from ledger.integrity import PredictionStatus, RegistrationStatus
from ledger.json_utils import canonical_json, sha256_json
from ledger.msm import SnapshotDateWindow, StrategySnapshot
from ledger.read_models import (
    IntegrityReason,
    IntegrityState,
    LedgerStatus,
    PredictionDetail,
    PredictionPage,
    PredictionSummary,
    ResultEvidenceProjection,
    ResultState,
    RunBindingProjection,
    RunProjection,
    SnapshotProvenance,
)
from ledger.record_integrity import (
    ImmutableEvidenceVerifier,
    SnapshotReadLimitError,
    VerifiedRecordEvidence,
)
from ledger.registry import LedgerRegistry
from ledger.results import MSMResultIngestor, VerifiedStoredRunResult
from ledger.run import RunState
from ledger.schema_registry import SchemaRegistry

logger = logging.getLogger(__name__)

_CURSOR_VERSION = 1
_MAX_CURSOR_BYTES = 4096
_CURSOR_CONTEXT = b"pine-prediction-read-cursor-v1\x00"


@dataclass(frozen=True, slots=True)
class PredictionListFilters:
    """Validated filters bound into an opaque pagination cursor."""

    registration_status: RegistrationStatus | None = None
    status: PredictionStatus | None = None
    strategy_id: str | None = None
    result_state: ResultState | None = None

    def __post_init__(self) -> None:
        if self.strategy_id is not None and (
            not self.strategy_id
            or len(self.strategy_id) > 256
            or self.strategy_id != self.strategy_id.strip()
            or "\x00" in self.strategy_id
        ):
            raise ValueError("strategy_id must be 1 to 256 normalized characters")

    def payload(self) -> dict[str, str | None]:
        """Return the canonical filter identity stored in a cursor."""

        return {
            "registration_status": (
                None if self.registration_status is None else self.registration_status.value
            ),
            "status": None if self.status is None else self.status.value,
            "strategy_id": self.strategy_id,
            "result_state": None if self.result_state is None else self.result_state.value,
        }


@dataclass(frozen=True, slots=True)
class _VerifiedBundle:
    prediction: sqlite3.Row
    evidence: VerifiedRecordEvidence
    snapshot: StrategySnapshot
    run: RunProjection
    result: VerifiedStoredRunResult | None


class _ProjectionFailure(Exception):
    def __init__(self, reason: IntegrityReason) -> None:
        super().__init__(reason.value)
        self.reason = reason


class _CursorCodec:
    """Authenticate opaque keyset cursors with server-held key material."""

    def __init__(self, secret: str | bytes) -> None:
        raw = secret.encode() if isinstance(secret, str) else secret
        if len(raw) < 32:
            raise ValueError("cursor secret must contain at least 32 bytes")
        self._key = hashlib.sha256(_CURSOR_CONTEXT + raw).digest()

    def encode(
        self,
        *,
        committed_at: str,
        prediction_id: str,
        filters: PredictionListFilters,
    ) -> str:
        payload = canonical_json(
            {
                "v": _CURSOR_VERSION,
                "committed_at": committed_at,
                "prediction_id": prediction_id,
                "filters": filters.payload(),
            }
        ).encode()
        encoded = _urlsafe_encode(payload)
        signature = _urlsafe_encode(hmac.digest(self._key, payload, "sha256"))
        return f"{encoded}.{signature}"

    def decode(
        self,
        cursor: str,
        *,
        filters: PredictionListFilters,
    ) -> tuple[str, str]:
        if not cursor or len(cursor.encode()) > _MAX_CURSOR_BYTES:
            raise ReadCursorError("prediction cursor is invalid")
        try:
            encoded, encoded_signature = cursor.split(".", maxsplit=1)
            payload = _urlsafe_decode(encoded)
            signature = _urlsafe_decode(encoded_signature)
        except (UnicodeEncodeError, ValueError) as exc:
            raise ReadCursorError("prediction cursor is invalid") from exc
        expected = hmac.digest(self._key, payload, "sha256")
        if not hmac.compare_digest(signature, expected):
            raise ReadCursorError("prediction cursor is invalid")
        try:
            decoded = json.loads(payload)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ReadCursorError("prediction cursor is invalid") from exc
        if not isinstance(decoded, dict) or set(decoded) != {
            "v",
            "committed_at",
            "prediction_id",
            "filters",
        }:
            raise ReadCursorError("prediction cursor is invalid")
        committed_at = decoded["committed_at"]
        prediction_id = decoded["prediction_id"]
        if (
            decoded["v"] != _CURSOR_VERSION
            or not isinstance(committed_at, str)
            or not committed_at
            or not isinstance(prediction_id, str)
            or not prediction_id
            or decoded["filters"] != filters.payload()
        ):
            raise ReadCursorError("prediction cursor does not match this query")
        return committed_at, prediction_id


class LedgerReadService:
    """Build trusted projections without mutating ledger state or artifacts."""

    def __init__(
        self,
        vault_root: str | Path,
        *,
        cursor_secret: str | bytes,
        records_dir: str | Path | None = None,
        registry: LedgerRegistry | None = None,
        schema_registry: SchemaRegistry | None = None,
    ) -> None:
        self.vault_root = Path(vault_root).resolve()
        if not self.vault_root.is_dir():
            raise ValueError(f"vault root does not exist: {self.vault_root}")
        self.ledger_dir = self.vault_root / ".ledger"
        self.records_dir = (
            Path(records_dir).resolve()
            if records_dir is not None
            else self.vault_root / "predictions"
        )
        if not self.records_dir.is_relative_to(self.vault_root) or self.records_dir.is_relative_to(
            self.ledger_dir
        ):
            raise ValueError("records_dir must live inside the vault outside .ledger")
        self.registry = registry or LedgerRegistry(self.ledger_dir / "registry.db")
        if self.registry.db_path.resolve() != (self.ledger_dir / "registry.db").resolve():
            raise ValueError("registry and read service must use the same vault root")
        self.schema_registry = schema_registry or SchemaRegistry(self.ledger_dir / "schemas")
        self.verifier = ImmutableEvidenceVerifier(self.vault_root)
        self.cursor_codec = _CursorCodec(cursor_secret)

    def list_predictions(
        self,
        *,
        limit: int = 25,
        cursor: str | None = None,
        filters: PredictionListFilters | None = None,
    ) -> PredictionPage:
        """Return a verifier-backed committed prediction page."""

        if not 1 <= limit <= 100:
            raise ValueError("prediction read limit must be between 1 and 100")
        active_filters = filters or PredictionListFilters()
        after_committed_at: str | None = None
        after_prediction_id: str | None = None
        if cursor is not None:
            after_committed_at, after_prediction_id = self.cursor_codec.decode(
                cursor,
                filters=active_filters,
            )

        matched: list[tuple[PredictionSummary, str, str]] = []
        exhausted = False
        while len(matched) <= limit and not exhausted:
            batch_size = min(100, max(25, (limit + 1 - len(matched)) * 2))
            rows = self.registry.list_committed_predictions(
                limit=batch_size,
                registration_status=(
                    None
                    if active_filters.registration_status is None
                    else active_filters.registration_status.value
                ),
                status=(None if active_filters.status is None else active_filters.status.value),
                result_state=(
                    None
                    if active_filters.result_state is None
                    else active_filters.result_state.value
                ),
                after_committed_at=after_committed_at,
                after_prediction_id=after_prediction_id,
            )
            if not rows:
                break
            for row in rows:
                summary = self._summary(row)
                if (
                    active_filters.strategy_id is None
                    or summary.strategy_id == active_filters.strategy_id
                ):
                    matched.append(
                        (
                            summary,
                            str(row["committed_sort_key"]),
                            str(row["prediction_id"]),
                        )
                    )
                    if len(matched) > limit:
                        break
            last = rows[-1]
            after_committed_at = str(last["committed_sort_key"])
            after_prediction_id = str(last["prediction_id"])
            exhausted = len(rows) < batch_size

        page_items = matched[:limit]
        next_cursor = None
        if len(matched) > limit and page_items:
            _summary, committed_at, prediction_id = page_items[-1]
            next_cursor = self.cursor_codec.encode(
                committed_at=committed_at,
                prediction_id=prediction_id,
                filters=active_filters,
            )
        return PredictionPage(
            items=tuple(item[0] for item in page_items),
            next_cursor=next_cursor,
        )

    def get_prediction(self, prediction_id: str) -> PredictionDetail:
        """Return one fully verified detail projection or fail closed."""

        row = self.registry.get_prediction(prediction_id)
        if row is None or row["transaction_state"] != "committed":
            raise PredictionNotFoundError("committed prediction was not found")
        try:
            bundle = self._verify_bundle(row)
            row = bundle.prediction
            created_at = _parse_time(row["created_at"], "prediction created_at")
            committed_at = _parse_time(row["committed_at"], "prediction committed_at")
            outcome = _optional_json_object(row["outcome_json"], "prediction outcome")
            grade = _optional_json_object(row["grade_json"], "prediction grade")
            resolution_metadata = _optional_json_object(
                row["resolution_metadata_json"],
                "prediction resolution metadata",
            )
        except _ProjectionFailure as exc:
            logger.warning(
                "ledger_prediction_detail_unverified",
                extra={"prediction_id": prediction_id, "reason": exc.reason.value},
            )
            raise IntegrityError(
                f"prediction evidence failed verification: {exc.reason.value}"
            ) from exc
        except (KeyError, TypeError, ValueError, ValidationError) as exc:
            raise IntegrityError("prediction registry projection is invalid") from exc

        result = None
        if bundle.result is not None:
            result = ResultEvidenceProjection(
                evidence_hash=bundle.result.evidence_hash,
                source_timestamp=bundle.result.source_timestamp,
                ingested_at=bundle.result.ingested_at,
                metric_units=bundle.result.evidence.metric_units,
                in_sample_metrics=bundle.result.evidence.in_sample_metrics,
                out_of_sample_metrics=bundle.result.evidence.out_of_sample_metrics,
                regime_breakdown=bundle.result.evidence.regime_breakdown,
                artifacts=bundle.result.evidence.artifacts,
            )
        return PredictionDetail(
            prediction_id=row["prediction_id"],
            run_id=row["run_id"],
            schema_id=row["schema_id"],
            schema_hash=row["schema_hash"],
            registration_status=RegistrationStatus(row["registration_status"]),
            forecast=bundle.evidence.forecast,
            decision=bundle.evidence.decision,
            snapshot_ref=row["snapshot_ref"],
            snapshot=_snapshot_provenance(bundle.snapshot),
            lineage=bundle.evidence.lineage,
            immutable_hash=row["immutable_hash"],
            body=bundle.evidence.body,
            status=PredictionStatus(row["status"]),
            outcome=outcome,
            grade=grade,
            resolution_metadata=resolution_metadata,
            transaction_state="committed",
            created_at=created_at,
            committed_at=committed_at,
            run=bundle.run,
            result=result,
            integrity_violations=(),
        )

    def get_status(self) -> LedgerStatus:
        """Return non-secret registry readiness metadata."""

        return LedgerStatus.model_validate(self.registry.get_read_status())

    def _summary(self, row: sqlite3.Row) -> PredictionSummary:
        try:
            bundle = self._verify_bundle(row)
        except _ProjectionFailure as exc:
            return self._failed_summary(row, exc.reason)
        except Exception:
            logger.exception(
                "ledger_prediction_summary_verification_failed",
                extra={"prediction_id": row["prediction_id"]},
            )
            return self._failed_summary(row, IntegrityReason.REGISTRY_UNVERIFIED)
        row = bundle.prediction
        return PredictionSummary(
            prediction_id=row["prediction_id"],
            run_id=row["run_id"],
            registration_status=RegistrationStatus(row["registration_status"]),
            status=PredictionStatus(row["status"]),
            transaction_state="committed",
            strategy_id=bundle.evidence.forecast.strategy_id,
            schema_id=row["schema_id"],
            out_of_sample_window=SnapshotDateWindow(
                start=bundle.evidence.forecast.out_of_sample_window.start,
                end=bundle.evidence.forecast.out_of_sample_window.end,
            ),
            created_at=_optional_time(row["created_at"]),
            committed_at=_optional_time(row["committed_at"]),
            run_state=bundle.run.state,
            result_state=(ResultState.PRESENT if bundle.result is not None else ResultState.ABSENT),
            integrity_state=IntegrityState.VERIFIED,
        )

    def _failed_summary(
        self,
        row: sqlite3.Row,
        reason: IntegrityReason,
    ) -> PredictionSummary:
        logger.warning(
            "ledger_prediction_summary_unverified",
            extra={"prediction_id": row["prediction_id"], "reason": reason.value},
        )
        try:
            registration_status = RegistrationStatus(row["registration_status"])
            prediction_status = PredictionStatus(row["status"])
        except (TypeError, ValueError) as exc:
            raise IntegrityError("prediction registry identity is invalid") from exc
        run_state = None
        try:
            if row["run_state"] is not None:
                run_state = RunState(row["run_state"])
        except (TypeError, ValueError):
            reason = IntegrityReason.REGISTRY_UNVERIFIED
        return PredictionSummary(
            prediction_id=str(row["prediction_id"]),
            run_id=str(row["run_id"]),
            registration_status=registration_status,
            status=prediction_status,
            transaction_state="committed",
            strategy_id=None,
            schema_id=str(row["schema_id"]),
            out_of_sample_window=None,
            created_at=_optional_time(row["created_at"]),
            committed_at=_optional_time(row["committed_at"]),
            run_state=run_state,
            result_state=(ResultState.PRESENT if bool(row["has_result"]) else ResultState.ABSENT),
            integrity_state=IntegrityState.FAILED,
            integrity_reason=reason,
        )

    def _verify_bundle(self, row: sqlite3.Row) -> _VerifiedBundle:
        with self.registry.read_transaction() as connection:
            prediction = self.registry.get_prediction(
                row["prediction_id"],
                connection=connection,
            )
            if prediction is None or prediction["transaction_state"] != "committed":
                raise _ProjectionFailure(IntegrityReason.REGISTRY_UNVERIFIED)
            return self._verify_bundle_in_snapshot(prediction, connection=connection)

    def _verify_bundle_in_snapshot(
        self,
        row: sqlite3.Row,
        *,
        connection: sqlite3.Connection,
    ) -> _VerifiedBundle:
        if row["status"] == PredictionStatus.QUARANTINED.value:
            raise _ProjectionFailure(IntegrityReason.QUARANTINED)
        if self.registry.list_integrity_violations(
            row["prediction_id"],
            connection=connection,
        ):
            raise _ProjectionFailure(IntegrityReason.QUARANTINED)

        record_path = self.records_dir / f"{row['prediction_id']}.md"
        try:
            verification = self.verifier.verify(record_path, row)
        except SnapshotReadLimitError:
            raise _ProjectionFailure(IntegrityReason.SNAPSHOT_UNVERIFIED) from None
        except (OSError, TypeError, ValueError):
            raise _ProjectionFailure(IntegrityReason.RECORD_UNVERIFIED) from None
        if not verification.verified or verification.evidence is None:
            raise _ProjectionFailure(_record_failure_reason(verification.violations))
        evidence = verification.evidence

        try:
            schema = self.schema_registry.load(row["schema_id"])
            if self.schema_registry.hash(schema) != row["schema_hash"]:
                raise _ProjectionFailure(IntegrityReason.SCHEMA_UNVERIFIED)
            valid, _errors = self.schema_registry.validate_schema(
                evidence.forecast.model_dump(mode="json"),
                schema,
            )
            if not valid:
                raise _ProjectionFailure(IntegrityReason.SCHEMA_UNVERIFIED)
        except SchemaNotFoundError:
            raise _ProjectionFailure(IntegrityReason.SCHEMA_UNVERIFIED) from None

        try:
            snapshot = StrategySnapshot.model_validate(evidence.snapshot)
            decision_at = _parse_time(row["created_at"], "prediction created_at")
        except (IntegrityError, ValidationError):
            raise _ProjectionFailure(IntegrityReason.SNAPSHOT_UNVERIFIED) from None
        expected_is = SnapshotDateWindow(
            start=evidence.forecast.in_sample_window.start,
            end=evidence.forecast.in_sample_window.end,
        )
        expected_oos = SnapshotDateWindow(
            start=evidence.forecast.out_of_sample_window.start,
            end=evidence.forecast.out_of_sample_window.end,
        )
        if (
            snapshot.strategy_id != evidence.forecast.strategy_id
            or snapshot.in_sample_window != expected_is
            or snapshot.out_of_sample_window != expected_oos
            or snapshot.data_as_of_version.astimezone(UTC) != decision_at
        ):
            raise _ProjectionFailure(IntegrityReason.SNAPSHOT_UNVERIFIED)

        run_row = self.registry.get_run(row["run_id"], connection=connection)
        try:
            run = _verify_run(row, run_row, snapshot)
        except (IntegrityError, KeyError, TypeError, ValueError, ValidationError):
            raise _ProjectionFailure(IntegrityReason.RUN_UNVERIFIED) from None

        result_row = self.registry.get_run_result(row["run_id"], connection=connection)
        result = None
        if result_row is not None:
            try:
                result = MSMResultIngestor.verify_stored_result(
                    result_row,
                    run_row,
                )
                if canonical_json(result.snapshot.model_dump(mode="json")) != canonical_json(
                    snapshot.model_dump(mode="json")
                ):
                    raise IntegrityError("result snapshot differs from prediction snapshot")
            except IntegrityError:
                raise _ProjectionFailure(IntegrityReason.RESULT_UNVERIFIED) from None
        return _VerifiedBundle(
            prediction=row,
            evidence=evidence,
            snapshot=snapshot,
            run=run,
            result=result,
        )


def _verify_run(
    prediction: sqlite3.Row,
    run: sqlite3.Row | None,
    snapshot: StrategySnapshot,
) -> RunProjection:
    if (
        run is None
        or run["run_id"] != prediction["run_id"]
        or run["prediction_id"] != prediction["prediction_id"]
    ):
        raise IntegrityError("prediction is missing its allocated run")
    state = RunState(run["state"])
    started_at = _parse_time(run["started_at"], "run started_at")
    execution_started_at = _parse_optional_time(
        run["execution_started_at"],
        "run execution_started_at",
    )
    completed_at = _parse_optional_time(run["completed_at"], "run completed_at")

    binding_fields = (
        "idempotency_key",
        "request_hash",
        "registration_status",
        "strategy_id",
        "dataset_version",
        "envelope_json",
        "envelope_hash",
        "bound_at",
    )
    binding_present = run["envelope_json"] is not None
    binding = None
    if not binding_present:
        if any(run[field] is not None for field in binding_fields):
            raise IntegrityError("allocated run has a partial binding")
        if (
            state is not RunState.REGISTERED
            or execution_started_at is not None
            or completed_at is not None
            or run["exit_code"] is not None
            or run["failure_note"] is not None
        ):
            raise IntegrityError("unbound run has invalid lifecycle state")
    else:
        if any(run[field] is None for field in binding_fields):
            raise IntegrityError("allocated run has a partial binding")
        try:
            envelope = json.loads(run["envelope_json"])
        except (TypeError, json.JSONDecodeError) as exc:
            raise IntegrityError("run envelope is invalid") from exc
        if not isinstance(envelope, dict) or sha256_json(envelope) != run["envelope_hash"]:
            raise IntegrityError("run envelope hash is invalid")
        envelope_snapshot = StrategySnapshot.model_validate(envelope.get("snapshot"))
        if canonical_json(envelope_snapshot.model_dump(mode="json")) != canonical_json(
            snapshot.model_dump(mode="json")
        ):
            raise IntegrityError("run snapshot differs from committed prediction snapshot")
        registration_status = RegistrationStatus(run["registration_status"])
        if not all(
            (
                envelope.get("run_id") == run["run_id"],
                envelope.get("prediction_id") == run["prediction_id"],
                envelope.get("registration_status") == registration_status.value,
                envelope.get("strategy_id") == run["strategy_id"] == snapshot.strategy_id,
                run["dataset_version"] == snapshot.dataset_version,
                registration_status.value == prediction["registration_status"],
            )
        ):
            raise IntegrityError("run binding provenance is invalid")
        binding = RunBindingProjection(
            idempotency_key=run["idempotency_key"],
            request_hash=run["request_hash"],
            registration_status=registration_status,
            strategy_id=run["strategy_id"],
            dataset_version=run["dataset_version"],
            envelope_hash=run["envelope_hash"],
            bound_at=_parse_time(run["bound_at"], "run bound_at"),
        )
        if state is RunState.REGISTERED:
            if execution_started_at is not None or completed_at is not None:
                raise IntegrityError("registered run has execution timestamps")
        elif state is RunState.RUNNING:
            if execution_started_at is None or completed_at is not None:
                raise IntegrityError("running run has invalid timestamps")
        else:
            if execution_started_at is None or completed_at is None or run["exit_code"] is None:
                raise IntegrityError("terminal run has incomplete lifecycle evidence")
            if state is RunState.COMPLETED and (
                run["exit_code"] != 0 or run["failure_note"] is not None
            ):
                raise IntegrityError("completed run has invalid terminal evidence")
            if state is RunState.FAILED and (run["exit_code"] == 0 and run["failure_note"] is None):
                raise IntegrityError("failed run has invalid terminal evidence")

    return RunProjection(
        run_id=run["run_id"],
        prediction_id=run["prediction_id"],
        started_at=started_at,
        state=state,
        execution_started_at=execution_started_at,
        completed_at=completed_at,
        exit_code=run["exit_code"],
        failure_note=run["failure_note"],
        binding=binding,
    )


def _record_failure_reason(violations: Mapping[str, str]) -> IntegrityReason:
    fields = set(violations)
    if "snapshot" in fields:
        return IntegrityReason.SNAPSHOT_UNVERIFIED
    if fields & {"schema_id", "schema_hash"}:
        return IntegrityReason.SCHEMA_UNVERIFIED
    return IntegrityReason.RECORD_UNVERIFIED


def _snapshot_provenance(snapshot: StrategySnapshot) -> SnapshotProvenance:
    return SnapshotProvenance(
        strategy_id=snapshot.strategy_id,
        strategy_spec_hash=snapshot.strategy_spec_hash,
        git_commit=snapshot.git_commit,
        parameter_count=snapshot.parameter_count,
        data_as_of_version=snapshot.data_as_of_version,
        dataset_version=snapshot.dataset_version,
        in_sample_window=snapshot.in_sample_window,
        out_of_sample_window=snapshot.out_of_sample_window,
        cost_model_version=snapshot.cost_model_version,
        slippage_model_version=snapshot.slippage_model_version,
        metric_definition_version=snapshot.metric_definition_version,
        engine_version=snapshot.engine_version,
        random_seed=snapshot.random_seed,
        captured_at=snapshot.captured_at,
    )


def _optional_json_object(value: object, name: str) -> dict[str, JsonValue] | None:
    if value is None:
        return None
    if not isinstance(value, str | bytes | bytearray):
        raise IntegrityError(f"{name} is invalid")
    try:
        parsed = json.loads(value)
        canonical_json(parsed)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise IntegrityError(f"{name} is invalid") from exc
    if not isinstance(parsed, dict):
        raise IntegrityError(f"{name} must be an object")
    return parsed


def _parse_time(value: object, name: str) -> datetime:
    try:
        if isinstance(value, datetime):
            parsed = value
        elif isinstance(value, str):
            parsed = datetime.fromisoformat(value)
        else:
            raise TypeError
    except (TypeError, ValueError) as exc:
        raise IntegrityError(f"{name} is invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise IntegrityError(f"{name} must be timezone-aware")
    return parsed.astimezone(UTC)


def _optional_time(value: object) -> datetime | None:
    if value is None:
        return None
    try:
        return _parse_time(value, "timestamp")
    except IntegrityError:
        return None


def _parse_optional_time(value: object, name: str) -> datetime | None:
    if value is None:
        return None
    return _parse_time(value, name)


def _urlsafe_encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _urlsafe_decode(value: str) -> bytes:
    alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
    if not value or any(character not in alphabet for character in value):
        raise ValueError("invalid base64url value")
    padding = "=" * (-len(value) % 4)
    decoded = base64.b64decode(value + padding, altchars=b"-_", validate=True)
    if _urlsafe_encode(decoded) != value:
        raise ValueError("non-canonical base64url value")
    return decoded
