"""Server-only, no-retry client for the loopback Pine backend."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from pathlib import PurePosixPath
from typing import Any, Protocol

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
from ledger.integrity import PreregisteredCaptureRequest
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
    "internal_error": 500,
}


class ConsoleBackend(Protocol):
    """Backend operations used by the console workflow core."""

    def health(self) -> HealthResponse:
        """Return loopback backend liveness."""

        ...

    def create_draft(self, request: HypothesisExtractionRequest) -> ExtractionResult:
        """Return a side-effect-free extraction result."""

        ...

    def capture(self, request: PreregisteredCaptureRequest) -> CaptureResponse:
        """Submit one exact preregistration request without automatic retry."""

        ...


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
        if any(character in value for character in ("/", "\\", "\x00")):
            raise ValueError("backend identifier is unsafe")
        return value

    @field_validator("record_ref", "snapshot_ref")
    @classmethod
    def references_are_safe_relative_paths(cls, value: str) -> str:
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
        self._client = client or httpx.Client(follow_redirects=False)
        self._owns_client = client is None

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
        try:
            wire = _WireHealth.model_validate(payload)
            response = HealthResponse(status=wire.status, api_version=wire.api_version)
        except ValidationError as exc:
            raise BackendProtocolError("backend health response is invalid") from exc
        if response.status != "ok" or response.api_version != "v1":
            raise BackendProtocolError("backend health contract is incompatible")
        return response

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
            or response.record_ref != f"predictions/{response.prediction_id}.md"
            or response.snapshot_ref != f".ledger/snapshots/{response.prediction_id}.json"
        ):
            raise BackendProtocolError("backend capture receipt binding is invalid")
        return response

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
                raw = _read_response_bytes(response)
        except (httpx.TimeoutException, httpx.TransportError) as exc:
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


def sanitize_backend_details(details: tuple[str, ...]) -> tuple[str, ...]:
    """Bound validation details and remove values that resemble secrets or host paths."""

    sanitized: list[str] = []
    for detail in details[:20]:
        normalized = detail.strip()[:500]
        if _SECRET_PATTERN.search(normalized):
            sanitized.append("[redacted sensitive backend detail]")
            continue
        sanitized.append(_ABSOLUTE_PATH_PATTERN.sub("[redacted path]", normalized))
    return tuple(sanitized)


def _read_response_bytes(response: httpx.Response) -> bytes:
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
        raw.extend(chunk)
        if len(raw) > _MAX_RESPONSE_BYTES:
            raise BackendProtocolError("backend response exceeds the read limit")
    return bytes(raw)


def _response_object(raw: bytes) -> dict[str, Any]:
    try:
        payload = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise BackendProtocolError("backend response is not valid JSON") from exc
    if not isinstance(payload, dict):
        raise BackendProtocolError("backend response must be a JSON object")
    return payload
