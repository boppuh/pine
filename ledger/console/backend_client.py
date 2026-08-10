"""Server-only, no-retry client for the loopback Pine backend."""

from __future__ import annotations

import json
import re
import time
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from pathlib import PurePosixPath
from typing import Any, Literal, Protocol
from urllib.parse import quote

import httpx
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from ledger.api import CaptureResponse, HealthResponse
from ledger.console.config import ConsoleConfig
from ledger.console.errors import (
    BackendDomainError,
    BackendProtocolError,
    BackendTransportError,
)
from ledger.extraction import (
    DraftProposal,
    ExtractionResult,
    ExtractionStatus,
    HypothesisExtractionRequest,
)
from ledger.integrity import PredictionStatus, PreregisteredCaptureRequest, RegistrationStatus
from ledger.json_utils import canonical_json

_MAX_RESPONSE_BYTES = 2 * 1024 * 1024
_SECRET_PATTERN = re.compile(r"(?i)(bearer\s+|password|secret|token|api[_-]?key|authorization)")
_ABSOLUTE_PATH_PATTERN = re.compile(r"(?<![\w.])/(?:[^\s:;,]+/)*[^\s:;,]+")
_EXPECTED_ERROR_STATUSES = {
    "unauthorized": 401,
    "invalid_request": 422,
    "schema_not_found": 404,
    "invalid_forecast": 422,
    "fresh_window_conflict": 409,
    "idempotency_conflict": 409,
    "snapshot_unavailable": 503,
    "integrity_error": 409,
    "prediction_not_found": 404,
    "internal_error": 500,
}


class ConsoleBackend(Protocol):
    """Backend operations used by the console workflow core."""

    def health(self) -> HealthResponse:
        """Return loopback backend liveness."""

        ...

    def ready(self) -> HealthResponse:
        """Verify the cached workflow credential against the backend."""

        ...

    def create_draft(self, request: HypothesisExtractionRequest) -> ExtractionResult:
        """Return a side-effect-free extraction result."""

        ...

    def capture(self, request: PreregisteredCaptureRequest) -> CaptureResponse:
        """Submit one exact preregistration request without automatic retry."""

        ...

    def get_receipt(self, prediction_id: str) -> AuthoritativeReceipt:
        """Return verified committed fields for a capture receipt."""

        ...


class AuthoritativeReceipt(BaseModel):
    """Small verified projection required to call a capture committed."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    prediction_id: str = Field(min_length=1, max_length=128)
    run_id: str = Field(min_length=1, max_length=128)
    registration_status: Literal[RegistrationStatus.PREREGISTERED]
    status: PredictionStatus
    transaction_state: Literal["committed"]
    schema_id: str = Field(min_length=1, max_length=256)
    schema_hash: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    immutable_hash: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    snapshot_ref: str
    committed_at: datetime

    @field_validator("prediction_id", "run_id")
    @classmethod
    def identifiers_are_not_paths(cls, value: str) -> str:
        return _safe_identifier(value)

    @field_validator("snapshot_ref")
    @classmethod
    def snapshot_reference_is_safe(cls, value: str) -> str:
        return _safe_relative_reference(value)

    @field_validator("committed_at")
    @classmethod
    def committed_at_is_timezone_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("backend receipt timestamp must be timezone-aware")
        return value.astimezone(UTC)


class _WireHealth(BaseModel):
    model_config = ConfigDict(extra="ignore")

    status: str
    api_version: str


class _WireProposal(BaseModel):
    model_config = ConfigDict(extra="ignore")

    schema_id: str
    schema_hash: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    registration_status: str
    forecast: dict[str, Any]
    decision: str
    lineage: dict[str, Any]
    body: str
    fresh_window: bool


class _WireExtraction(BaseModel):
    model_config = ConfigDict(extra="ignore")

    status: ExtractionStatus
    proposal: _WireProposal | None = None
    errors: tuple[str, ...] = ()


class _WireCapture(BaseModel):
    model_config = ConfigDict(extra="ignore")

    prediction_id: str = Field(min_length=1, max_length=128)
    run_id: str = Field(min_length=1, max_length=128)
    record_ref: str
    snapshot_ref: str
    schema_id: str = Field(min_length=1, max_length=256)
    schema_hash: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    immutable_hash: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    created: bool

    @field_validator("prediction_id", "run_id")
    @classmethod
    def identifiers_are_not_paths(cls, value: str) -> str:
        return _safe_identifier(value)

    @field_validator("record_ref", "snapshot_ref")
    @classmethod
    def references_are_safe_relative_paths(cls, value: str) -> str:
        return _safe_relative_reference(value)


class _WireReceipt(BaseModel):
    model_config = ConfigDict(extra="ignore")

    prediction_id: str
    run_id: str
    registration_status: str
    status: str
    transaction_state: str
    schema_id: str
    schema_hash: str
    immutable_hash: str
    snapshot_ref: str
    committed_at: datetime


class _WireErrorBody(BaseModel):
    model_config = ConfigDict(extra="ignore")

    code: str = Field(min_length=1, max_length=128)
    message: str = Field(min_length=1, max_length=2000)
    details: tuple[str, ...] = ()


class _WireError(BaseModel):
    model_config = ConfigDict(extra="ignore")

    error: _WireErrorBody


class ConsoleBackendClient:
    """Call only the configured loopback backend with a server-held token."""

    def __init__(
        self,
        config: ConsoleConfig,
        *,
        token: str | None = None,
        client: httpx.Client | None = None,
        monotonic_clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.config = config
        self._token = token if token is not None else config.read_backend_token()
        if (
            len(self._token) < 32
            or len(self._token) > 4096
            or not self._token.isascii()
            or any(character.isspace() for character in self._token)
        ):
            raise ValueError("backend token format is invalid")
        self._client = client or httpx.Client(follow_redirects=False, trust_env=False)
        self._owns_client = client is None
        self._monotonic = monotonic_clock

    def close(self) -> None:
        """Close the owned HTTP connection pool."""

        if self._owns_client:
            self._client.close()

    def health(self) -> HealthResponse:
        """Validate API-v1 health without sending the bearer token."""

        payload = self._request(
            "GET",
            "/health",
            timeout=self.config.health_timeout_seconds,
            authenticated=False,
        )
        return _validated_health(payload, operation="health")

    def ready(self) -> HealthResponse:
        """Validate API-v1 readiness through an authenticated read endpoint."""

        payload = self._request(
            "GET",
            "/v1/status",
            timeout=self.config.health_timeout_seconds,
        )
        return _validated_health(payload, operation="readiness")

    def create_draft(self, request: HypothesisExtractionRequest) -> ExtractionResult:
        """Submit extraction once and validate its complete discriminated response."""

        payload = self._request(
            "POST",
            "/v1/drafts",
            timeout=self.config.extraction_timeout_seconds,
            body=request.model_dump(mode="json"),
        )
        try:
            wire = _WireExtraction.model_validate(payload)
            proposal = (
                None
                if wire.proposal is None
                else DraftProposal.model_validate(wire.proposal.model_dump(mode="json"))
            )
            result = ExtractionResult(
                status=wire.status,
                proposal=proposal,
                errors=sanitize_backend_details(wire.errors),
            )
        except ValidationError as exc:
            raise BackendProtocolError("backend extraction response is invalid") from exc
        if proposal is not None and proposal.schema_id != request.schema_id:
            raise BackendProtocolError("backend extraction schema binding is invalid")
        if proposal is not None and proposal.body != request.text:
            raise BackendProtocolError("backend extraction source binding is invalid")
        return result

    def capture(self, request: PreregisteredCaptureRequest) -> CaptureResponse:
        """Submit canonical request bytes exactly once and validate the receipt."""

        payload = self._request(
            "POST",
            "/v1/captures",
            timeout=self.config.capture_timeout_seconds,
            body=request.model_dump(mode="json"),
        )
        try:
            wire = _WireCapture.model_validate(payload)
            response = CaptureResponse.model_validate(wire.model_dump(mode="json"))
        except ValidationError as exc:
            raise BackendProtocolError("backend capture response is invalid") from exc
        if (
            response.schema_id != request.schema_id
            or (
                request.expected_schema_hash is not None
                and response.schema_hash != request.expected_schema_hash
            )
            or response.record_ref != f"predictions/{response.prediction_id}.md"
            or response.snapshot_ref != f".ledger/snapshots/{response.prediction_id}.json"
        ):
            raise BackendProtocolError("backend capture receipt binding is invalid")
        return response

    def get_receipt(self, prediction_id: str) -> AuthoritativeReceipt:
        """Read verified committed fields and reject a mismatched prediction binding."""

        safe_prediction_id = _safe_identifier(prediction_id)
        payload = self._request(
            "GET",
            f"/v1/predictions/{quote(safe_prediction_id, safe='')}",
            timeout=self.config.health_timeout_seconds,
        )
        try:
            wire = _WireReceipt.model_validate(payload)
            receipt = AuthoritativeReceipt.model_validate(wire.model_dump(mode="json"))
        except ValidationError as exc:
            raise BackendProtocolError("backend receipt response is invalid") from exc
        if receipt.prediction_id != safe_prediction_id:
            raise BackendProtocolError("backend receipt prediction binding is invalid")
        return receipt

    def _request(
        self,
        method: str,
        path: str,
        *,
        timeout: float,
        body: Mapping[str, Any] | None = None,
        authenticated: bool = True,
    ) -> dict[str, Any]:
        headers = {"accept": "application/json"}
        if authenticated:
            headers["authorization"] = f"Bearer {self._token}"
        content = None
        if body is not None:
            headers["content-type"] = "application/json"
            content = canonical_json(body).encode("utf-8")
        deadline = self._monotonic() + timeout
        try:
            with self._client.stream(
                method,
                f"{self.config.backend_url.rstrip('/')}{path}",
                headers=headers,
                content=content,
                timeout=httpx.Timeout(timeout, connect=self.config.connect_timeout_seconds),
            ) as response:
                status_code = response.status_code
                is_error = response.is_error
                raw = _read_response_bytes(
                    response,
                    deadline=deadline,
                    monotonic_clock=self._monotonic,
                )
        except httpx.RequestError as exc:
            raise BackendTransportError("backend response was not received") from exc
        payload = _response_object(raw)
        if is_error:
            try:
                wire_error = _WireError.model_validate(payload).error
            except ValidationError as exc:
                raise BackendProtocolError("backend error response is invalid") from exc
            expected_status = _EXPECTED_ERROR_STATUSES.get(wire_error.code)
            if expected_status is not None and status_code != expected_status:
                raise BackendProtocolError("backend error status does not match its code")
            raise BackendDomainError(
                status_code=status_code,
                code=wire_error.code,
                message=sanitize_backend_details((wire_error.message,))[0],
                details=sanitize_backend_details(wire_error.details),
            )
        if not 200 <= status_code < 300:
            raise BackendProtocolError("backend returned an unexpected status")
        return payload


def _validated_health(payload: Mapping[str, Any], *, operation: str) -> HealthResponse:
    """Validate the shared API-version contract for health and readiness probes."""

    try:
        wire = _WireHealth.model_validate(payload)
        response = HealthResponse(status=wire.status, api_version=wire.api_version)
    except ValidationError as exc:
        raise BackendProtocolError(f"backend {operation} response is invalid") from exc
    if response.status != "ok" or response.api_version != "v1":
        raise BackendProtocolError(f"backend {operation} contract is incompatible")
    return response


def _safe_identifier(value: str) -> str:
    if (
        not value
        or len(value) > 128
        or any(character in value for character in ("/", "\\", "\x00"))
    ):
        raise ValueError("backend identifier is unsafe")
    return value


def _safe_relative_reference(value: str) -> str:
    path = PurePosixPath(value)
    if (
        not value
        or len(value) > 4096
        or path.is_absolute()
        or str(path) != value
        or any(part in {"", ".", ".."} for part in path.parts)
    ):
        raise ValueError("backend artifact reference is unsafe")
    return value


def sanitize_backend_details(details: tuple[str, ...]) -> tuple[str, ...]:
    """Bound validation details and remove values that resemble secrets or host paths."""

    sanitized: list[str] = []
    for detail in details[:20]:
        normalized = detail.strip()[:500]
        if not normalized:
            sanitized.append("[empty backend detail]")
            continue
        if _SECRET_PATTERN.search(normalized):
            sanitized.append("[redacted sensitive backend detail]")
            continue
        sanitized.append(_ABSOLUTE_PATH_PATTERN.sub("[redacted path]", normalized))
    return tuple(sanitized)


def _read_response_bytes(
    response: httpx.Response,
    *,
    deadline: float,
    monotonic_clock: Callable[[], float],
) -> bytes:
    if monotonic_clock() >= deadline:
        raise BackendTransportError("backend response exceeded the overall deadline")
    content_length = response.headers.get("content-length")
    if content_length is not None:
        try:
            declared_length = int(content_length)
        except ValueError as exc:
            raise BackendProtocolError("backend content length is invalid") from exc
        if declared_length < 0 or declared_length > _MAX_RESPONSE_BYTES:
            raise BackendProtocolError("backend response exceeds the read limit")
    raw = bytearray()
    for chunk in response.iter_bytes():
        if monotonic_clock() >= deadline:
            raise BackendTransportError("backend response exceeded the overall deadline")
        raw.extend(chunk)
        if len(raw) > _MAX_RESPONSE_BYTES:
            raise BackendProtocolError("backend response exceeds the read limit")
    if monotonic_clock() >= deadline:
        raise BackendTransportError("backend response exceeded the overall deadline")
    return bytes(raw)


def _response_object(raw: bytes) -> dict[str, Any]:
    try:
        payload = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise BackendProtocolError("backend response is not valid JSON") from exc
    if not isinstance(payload, dict):
        raise BackendProtocolError("backend response must be a JSON object")
    return payload
