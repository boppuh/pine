"""Fail-closed Tailscale Serve identity adapter."""

from __future__ import annotations

import hmac
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

TAILSCALE_LOGIN_HEADER = b"tailscale-user-login"


@dataclass(frozen=True, slots=True)
class AuthenticatedIdentity:
    """One normalized ingress identity approved for this console."""

    user_id: str


def normalize_user_identity(value: str) -> str:
    """Normalize an ASCII ingress identity without accepting ambiguous whitespace."""

    normalized = value.strip().casefold()
    if (
        not normalized
        or len(normalized) > 320
        or any(ord(character) < 0x21 or ord(character) > 0x7E for character in normalized)
    ):
        raise ValueError("authenticated identity is invalid")
    return normalized


def authenticate_tailscale_identity(
    scope: Mapping[str, Any],
    *,
    socket_path: Path,
    allowed_identities: tuple[str, ...],
) -> AuthenticatedIdentity | None:
    """Trust Tailscale's login claim only on the configured Unix socket."""

    if not is_configured_unix_socket(scope, socket_path):
        return None
    raw = _single_header(scope, TAILSCALE_LOGIN_HEADER)
    if raw is None:
        return None
    try:
        claimed = normalize_user_identity(raw.decode("ascii"))
    except (UnicodeDecodeError, ValueError):
        return None
    if not any(hmac.compare_digest(claimed, allowed) for allowed in allowed_identities):
        return None
    return AuthenticatedIdentity(user_id=claimed)


def is_configured_unix_socket(scope: Mapping[str, Any], socket_path: Path) -> bool:
    """Distinguish Uvicorn Unix-socket scope metadata from every TCP peer."""

    if scope.get("client") is not None:
        return False
    server = scope.get("server")
    if not isinstance(server, tuple) or len(server) != 2 or server[1] is not None:
        return False
    try:
        return Path(str(server[0])) == socket_path
    except (OSError, ValueError):
        return False


def single_header(scope: Mapping[str, Any], name: str) -> str | None:
    """Return one ASCII header value and reject duplicates or malformed bytes."""

    raw = _single_header(scope, name.lower().encode("ascii"))
    if raw is None:
        return None
    try:
        return raw.decode("ascii")
    except UnicodeDecodeError:
        return None


def _single_header(scope: Mapping[str, Any], name: bytes) -> bytes | None:
    headers = scope.get("headers")
    if not isinstance(headers, list):
        return None
    matches = [value for key, value in headers if key.lower() == name]
    if len(matches) != 1:
        return None
    return matches[0]
