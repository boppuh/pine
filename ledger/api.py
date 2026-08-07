"""Loopback HTTP API over extraction and atomic capture services."""

from __future__ import annotations

import logging
import secrets
from dataclasses import dataclass
from typing import Annotated, Any

from fastapi import Depends, FastAPI, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict

from ledger.capture import CaptureService
from ledger.errors import (
    ForecastValidationError,
    FreshWindowError,
    IdempotencyConflictError,
    IntegrityError,
    PredictionNotFoundError,
    ReadCursorError,
    SchemaNotFoundError,
    SnapshotCaptureError,
)
from ledger.extraction import (
    ExtractionResult,
    ExtractionService,
    HypothesisExtractionRequest,
)
from ledger.integrity import (
    PredictionStatus,
    PreregisteredCaptureRequest,
    RegistrationStatus,
)
from ledger.read_models import LedgerStatus, PredictionDetail, PredictionPage, ResultState
from ledger.read_service import LedgerReadService, PredictionListFilters
from ledger.writer import WriteResult

logger = logging.getLogger(__name__)

API_VERSION = "v1"


class HealthResponse(BaseModel):
    """Unauthenticated liveness response used during local discovery."""

    model_config = ConfigDict(extra="forbid")

    status: str = "ok"
    api_version: str = API_VERSION


class CaptureResponse(BaseModel):
    """Stable, vault-relative representation of one atomic capture result."""

    model_config = ConfigDict(extra="forbid")

    prediction_id: str
    run_id: str
    record_ref: str
    snapshot_ref: str
    schema_id: str
    schema_hash: str
    immutable_hash: str
    created: bool


class APIErrorBody(BaseModel):
    """Machine-readable local API error payload."""

    model_config = ConfigDict(extra="forbid")

    code: str
    message: str
    details: tuple[str, ...] = ()


class APIErrorResponse(BaseModel):
    """Envelope shared by authentication, validation, and domain errors."""

    model_config = ConfigDict(extra="forbid")

    error: APIErrorBody


@dataclass(frozen=True, slots=True)
class _APIException(Exception):
    status_code: int
    code: str
    message: str
    details: tuple[str, ...] = ()


def create_app(
    *,
    extraction_service: ExtractionService,
    capture_service: CaptureService,
    token: str,
    read_service: LedgerReadService | None = None,
) -> FastAPI:
    """Create the local ASGI application with closed-by-default write access."""

    if len(token) < 32 or not token.isascii() or any(character.isspace() for character in token):
        raise ValueError("backend token must be at least 32 non-whitespace ASCII characters")
    if extraction_service.vault_root != capture_service.writer.vault_root:
        raise ValueError("extraction and capture services must use the same vault root")
    reads = read_service or LedgerReadService(
        capture_service.writer.vault_root,
        cursor_secret=token,
        records_dir=capture_service.writer.records_dir,
        registry=capture_service.registry,
        schema_registry=capture_service.schema_registry,
    )
    if reads.vault_root != capture_service.writer.vault_root:
        raise ValueError("read and capture services must use the same vault root")
    if reads.records_dir != capture_service.writer.records_dir:
        raise ValueError("read and capture services must use the same record directory")
    app = FastAPI(
        title="Decision Edge Ledger",
        version=API_VERSION,
        docs_url="/docs",
        redoc_url=None,
    )

    async def require_token(request: Request) -> None:
        supplied = request.headers.get("authorization", "")
        expected = f"Bearer {token}"
        if not secrets.compare_digest(supplied.encode(), expected.encode()):
            raise _APIException(
                status_code=401,
                code="unauthorized",
                message="a valid local backend token is required",
            )

    @app.exception_handler(_APIException)
    async def api_exception_handler(_request: Request, exc: _APIException) -> JSONResponse:
        return _error_response(exc.status_code, exc.code, exc.message, exc.details)

    @app.exception_handler(RequestValidationError)
    async def request_validation_handler(
        _request: Request,
        exc: RequestValidationError,
    ) -> JSONResponse:
        details = tuple(
            f"$.{'.'.join(str(item) for item in error['loc'])}: {error['msg']}"
            for error in exc.errors()
        )
        return _error_response(422, "invalid_request", "request validation failed", details)

    @app.exception_handler(SchemaNotFoundError)
    async def schema_not_found_handler(
        _request: Request,
        exc: SchemaNotFoundError,
    ) -> JSONResponse:
        return _error_response(404, "schema_not_found", str(exc))

    @app.exception_handler(ForecastValidationError)
    async def forecast_validation_handler(
        _request: Request,
        exc: ForecastValidationError,
    ) -> JSONResponse:
        return _error_response(
            422,
            "invalid_forecast",
            "forecast validation failed",
            tuple(exc.errors),
        )

    @app.exception_handler(FreshWindowError)
    async def fresh_window_handler(_request: Request, exc: FreshWindowError) -> JSONResponse:
        return _error_response(409, "fresh_window_conflict", str(exc))

    @app.exception_handler(IdempotencyConflictError)
    async def idempotency_handler(
        _request: Request,
        exc: IdempotencyConflictError,
    ) -> JSONResponse:
        return _error_response(409, "idempotency_conflict", str(exc))

    @app.exception_handler(SnapshotCaptureError)
    async def snapshot_handler(_request: Request, exc: SnapshotCaptureError) -> JSONResponse:
        return _error_response(503, "snapshot_unavailable", str(exc))

    @app.exception_handler(IntegrityError)
    async def integrity_handler(_request: Request, exc: IntegrityError) -> JSONResponse:
        return _error_response(409, "integrity_error", str(exc))

    @app.exception_handler(PredictionNotFoundError)
    async def prediction_not_found_handler(
        _request: Request,
        exc: PredictionNotFoundError,
    ) -> JSONResponse:
        return _error_response(404, "prediction_not_found", str(exc))

    @app.exception_handler(ReadCursorError)
    async def read_cursor_handler(_request: Request, exc: ReadCursorError) -> JSONResponse:
        return _error_response(422, "invalid_cursor", str(exc))

    @app.exception_handler(Exception)
    async def unexpected_handler(request: Request, exc: Exception) -> JSONResponse:
        logger.exception(
            "local_backend_request_failed",
            extra={"method": request.method, "path": request.url.path},
        )
        return _error_response(500, "internal_error", "local backend request failed")

    @app.get("/health", response_model=HealthResponse)
    async def health() -> HealthResponse:
        return HealthResponse()

    @app.post(
        "/v1/drafts",
        response_model=ExtractionResult,
        dependencies=[Depends(require_token)],
    )
    async def create_draft(request: HypothesisExtractionRequest) -> ExtractionResult:
        return await extraction_service.propose(request)

    @app.post(
        "/v1/captures",
        response_model=CaptureResponse,
        dependencies=[Depends(require_token)],
    )
    def create_capture(request: PreregisteredCaptureRequest) -> CaptureResponse:
        result = capture_service.capture(request)
        return _capture_response(result, capture_service)

    @app.get(
        "/v1/predictions",
        response_model=PredictionPage,
        dependencies=[Depends(require_token)],
    )
    def list_predictions(
        limit: Annotated[int, Query(ge=1, le=100)] = 25,
        cursor: Annotated[str | None, Query(max_length=4096)] = None,
        registration_status: RegistrationStatus | None = None,
        status: PredictionStatus | None = None,
        strategy_id: Annotated[str | None, Query(min_length=1, max_length=256)] = None,
        result_state: ResultState | None = None,
    ) -> PredictionPage:
        try:
            filters = PredictionListFilters(
                registration_status=registration_status,
                status=status,
                strategy_id=strategy_id,
                result_state=result_state,
            )
        except ValueError as exc:
            raise _APIException(
                status_code=422,
                code="invalid_request",
                message="request validation failed",
                details=(str(exc),),
            ) from exc
        return reads.list_predictions(limit=limit, cursor=cursor, filters=filters)

    @app.get(
        "/v1/predictions/{prediction_id}",
        response_model=PredictionDetail,
        dependencies=[Depends(require_token)],
    )
    def get_prediction(prediction_id: str) -> PredictionDetail:
        return reads.get_prediction(prediction_id)

    @app.get(
        "/v1/status",
        response_model=LedgerStatus,
        dependencies=[Depends(require_token)],
    )
    def get_status() -> LedgerStatus:
        return reads.get_status()

    return app


def _capture_response(result: WriteResult, service: CaptureService) -> CaptureResponse:
    vault_root = service.writer.vault_root
    return CaptureResponse(
        prediction_id=result.prediction_id,
        run_id=result.run_id,
        record_ref=result.record_path.relative_to(vault_root).as_posix(),
        snapshot_ref=result.snapshot_path.relative_to(vault_root).as_posix(),
        schema_id=result.schema_id,
        schema_hash=result.schema_hash,
        immutable_hash=result.immutable_hash,
        created=result.created,
    )


def _error_response(
    status_code: int,
    code: str,
    message: str,
    details: tuple[str, ...] = (),
) -> JSONResponse:
    content: dict[str, Any] = APIErrorResponse(
        error=APIErrorBody(code=code, message=message, details=details)
    ).model_dump(mode="json")
    return JSONResponse(status_code=status_code, content=content)
