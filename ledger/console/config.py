"""Strict environment configuration for the standalone console process."""

from __future__ import annotations

import os
import stat
from collections.abc import Mapping
from datetime import timedelta
from pathlib import Path
from typing import Self
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from ledger.console.auth import normalize_user_identity
from ledger.console.errors import ConsoleConfigError


class ConsoleConfig(BaseModel):
    """Validated server-only configuration with no browser-exposed secrets."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    socket_path: Path = Path("/run/pine/console.sock")
    state_path: Path = Path("/var/lib/pine/console/console.db")
    backend_url: str = "http://127.0.0.1:8765"
    backend_credential_path: Path = Path("/run/credentials/pine-console.service/backend-token")
    allowed_host: str
    allowed_identities: tuple[str, ...]
    connect_timeout_seconds: float = Field(default=3.0, gt=0, le=30)
    health_timeout_seconds: float = Field(default=3.0, gt=0, le=30)
    read_timeout_seconds: float = Field(default=30.0, gt=0, le=300)
    extraction_timeout_seconds: float = Field(default=90.0, gt=0, le=300)
    capture_timeout_seconds: float = Field(default=120.0, gt=0, le=600)
    ordinary_retention_hours: int = Field(default=24, ge=1, le=24 * 30)
    receipt_retention_hours: int = Field(default=24, ge=1, le=24 * 30)
    session_absolute_minutes: int = Field(default=8 * 60, ge=5, le=24 * 60)
    session_idle_minutes: int = Field(default=30, ge=1, le=8 * 60)
    max_request_bytes: int = Field(default=1024 * 1024, ge=1024, le=1024 * 1024)
    session_attempt_limit: int = Field(default=20, ge=1, le=1000)
    extraction_attempt_limit: int = Field(default=10, ge=1, le=1000)
    confirmation_attempt_limit: int = Field(default=5, ge=1, le=1000)
    retry_attempt_limit: int = Field(default=10, ge=1, le=1000)
    rate_limit_window_seconds: int = Field(default=10 * 60, ge=60, le=24 * 60 * 60)
    log_level: str = "info"

    @field_validator("socket_path", "state_path", "backend_credential_path")
    @classmethod
    def paths_are_absolute(cls, value: Path) -> Path:
        expanded = value.expanduser()
        if not expanded.is_absolute():
            raise ValueError("console paths must be absolute")
        if "\x00" in str(expanded):
            raise ValueError("console paths cannot contain NUL")
        return expanded

    @field_validator("allowed_host")
    @classmethod
    def allowed_host_is_canonical(cls, value: str) -> str:
        normalized = value.strip().lower()
        if (
            not normalized
            or len(normalized) > 253
            or normalized != value
            or any(
                character not in "abcdefghijklmnopqrstuvwxyz0123456789-."
                for character in normalized
            )
            or normalized.startswith(".")
            or normalized.endswith(".")
            or ".." in normalized
        ):
            raise ValueError("allowed_host must be one normalized DNS hostname")
        return normalized

    @field_validator("allowed_identities")
    @classmethod
    def allowed_identities_are_exact(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        normalized = tuple(normalize_user_identity(value) for value in values)
        if not normalized or len(normalized) > 32:
            raise ValueError("allowed_identities must contain between one and 32 identities")
        if len(set(normalized)) != len(normalized):
            raise ValueError("allowed_identities must not contain duplicates")
        return normalized

    @field_validator("log_level")
    @classmethod
    def log_level_is_supported(cls, value: str) -> str:
        normalized = value.strip().lower()
        if normalized not in {"critical", "error", "warning", "info"}:
            raise ValueError("unsupported console log level")
        return normalized

    @model_validator(mode="after")
    def configuration_is_safe(self) -> Self:
        parsed = urlsplit(self.backend_url)
        if (
            parsed.scheme != "http"
            or parsed.hostname != "127.0.0.1"
            or parsed.port is None
            or parsed.username is not None
            or parsed.password is not None
            or parsed.path not in {"", "/"}
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError("backend_url must be loopback HTTP with an explicit port")
        if self.socket_path.suffix != ".sock":
            raise ValueError("console socket path must end in .sock")
        if self.state_path.suffix != ".db":
            raise ValueError("console state path must end in .db")
        try:
            resolved_state_path = self.state_path.resolve(strict=False)
            resolved_paths = {
                self.socket_path.resolve(strict=False),
                resolved_state_path,
                self.backend_credential_path.resolve(strict=False),
            }
        except OSError as exc:
            raise ValueError("console paths could not be resolved safely") from exc
        if ".ledger" in resolved_state_path.parts:
            raise ValueError("console state must live outside the authoritative ledger")
        if len(resolved_paths) != 3:
            raise ValueError("console socket, state, and credential paths must be distinct")
        if self.session_idle_minutes >= self.session_absolute_minutes:
            raise ValueError("session idle lifetime must be shorter than absolute lifetime")
        return self

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> ConsoleConfig:
        """Load the allowlisted console environment and reject malformed values."""

        values = os.environ if environ is None else environ
        allowed_host = values.get("PINE_CONSOLE_ALLOWED_HOST")
        if allowed_host is None:
            raise ConsoleConfigError("PINE_CONSOLE_ALLOWED_HOST is required")
        allowed_identities = values.get("PINE_CONSOLE_ALLOWED_IDENTITIES")
        if allowed_identities is None:
            raise ConsoleConfigError("PINE_CONSOLE_ALLOWED_IDENTITIES is required")
        raw_identities = tuple(item.strip() for item in allowed_identities.split(","))
        if any(not item for item in raw_identities):
            raise ConsoleConfigError("console environment configuration is invalid")
        raw: dict[str, object] = {
            "allowed_host": allowed_host,
            "allowed_identities": raw_identities,
        }
        mappings: dict[str, tuple[str, type[str] | type[int] | type[float] | type[Path]]] = {
            "PINE_CONSOLE_SOCKET_PATH": ("socket_path", Path),
            "PINE_CONSOLE_STATE_PATH": ("state_path", Path),
            "PINE_CONSOLE_BACKEND_URL": ("backend_url", str),
            "PINE_CONSOLE_BACKEND_CREDENTIAL_PATH": ("backend_credential_path", Path),
            "PINE_CONSOLE_CONNECT_TIMEOUT_SECONDS": ("connect_timeout_seconds", float),
            "PINE_CONSOLE_HEALTH_TIMEOUT_SECONDS": ("health_timeout_seconds", float),
            "PINE_CONSOLE_READ_TIMEOUT_SECONDS": ("read_timeout_seconds", float),
            "PINE_CONSOLE_EXTRACTION_TIMEOUT_SECONDS": (
                "extraction_timeout_seconds",
                float,
            ),
            "PINE_CONSOLE_CAPTURE_TIMEOUT_SECONDS": ("capture_timeout_seconds", float),
            "PINE_CONSOLE_ORDINARY_RETENTION_HOURS": ("ordinary_retention_hours", int),
            "PINE_CONSOLE_RECEIPT_RETENTION_HOURS": ("receipt_retention_hours", int),
            "PINE_CONSOLE_SESSION_ABSOLUTE_MINUTES": ("session_absolute_minutes", int),
            "PINE_CONSOLE_SESSION_IDLE_MINUTES": ("session_idle_minutes", int),
            "PINE_CONSOLE_MAX_REQUEST_BYTES": ("max_request_bytes", int),
            "PINE_CONSOLE_SESSION_ATTEMPT_LIMIT": ("session_attempt_limit", int),
            "PINE_CONSOLE_EXTRACTION_ATTEMPT_LIMIT": ("extraction_attempt_limit", int),
            "PINE_CONSOLE_CONFIRMATION_ATTEMPT_LIMIT": (
                "confirmation_attempt_limit",
                int,
            ),
            "PINE_CONSOLE_RETRY_ATTEMPT_LIMIT": ("retry_attempt_limit", int),
            "PINE_CONSOLE_RATE_LIMIT_WINDOW_SECONDS": (
                "rate_limit_window_seconds",
                int,
            ),
            "PINE_CONSOLE_LOG_LEVEL": ("log_level", str),
        }
        try:
            for environment_name, (field_name, converter) in mappings.items():
                if environment_name in values:
                    raw[field_name] = converter(values[environment_name])
            return cls.model_validate(raw)
        except (TypeError, ValueError) as exc:
            raise ConsoleConfigError("console environment configuration is invalid") from exc

    @property
    def ordinary_retention(self) -> timedelta:
        return timedelta(hours=self.ordinary_retention_hours)

    @property
    def receipt_retention(self) -> timedelta:
        return timedelta(hours=self.receipt_retention_hours)

    @property
    def session_absolute_lifetime(self) -> timedelta:
        return timedelta(minutes=self.session_absolute_minutes)

    @property
    def session_idle_lifetime(self) -> timedelta:
        return timedelta(minutes=self.session_idle_minutes)

    def read_backend_token(self) -> str:
        """Read a bounded systemd credential without exposing its value."""

        path = self.backend_credential_path
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open(path, flags)
        except OSError as exc:
            raise ConsoleConfigError("backend credential is unreadable") from exc
        try:
            metadata = os.fstat(descriptor)
            if not stat.S_ISREG(metadata.st_mode):
                raise ConsoleConfigError("backend credential must be a regular file")
            if stat.S_IMODE(metadata.st_mode) & 0o077:
                raise ConsoleConfigError("backend credential must not be group/world accessible")
            raw = os.read(descriptor, 4097)
        finally:
            os.close(descriptor)
        if not 32 <= len(raw) <= 4096:
            raise ConsoleConfigError("backend credential length is invalid")
        try:
            token = raw.decode("ascii").strip()
        except UnicodeDecodeError as exc:
            raise ConsoleConfigError("backend credential must be ASCII") from exc
        if len(token) < 32 or any(character.isspace() for character in token):
            raise ConsoleConfigError("backend credential format is invalid")
        return token
