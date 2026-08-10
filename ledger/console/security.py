"""ASGI security boundary for the browser-facing console."""

from __future__ import annotations

import json
import logging
import math
import secrets
import time
from collections.abc import Callable
from http import HTTPStatus
from typing import Any
from urllib.parse import parse_qs

from anyio import CapacityLimiter
from anyio.to_thread import run_sync
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from ledger.console.auth import authenticate_tailscale_identity, single_header
from ledger.console.config import ConsoleConfig
from ledger.console.errors import ConsoleError, RateLimitExceeded
from ledger.console.rate_limit import ConsoleAbuseControls
from ledger.console.sessions import (
    SESSION_COOKIE_NAME,
    ConsoleSession,
    ConsoleSessionStore,
    SessionLookupStatus,
    hash_user_identity,
)

LOGGER = logging.getLogger("ledger.console.http")
PUBLIC_HEALTH_PATHS = frozenset({"/healthz", "/livez", "/readyz"})
CONTENT_SECURITY_POLICY = (
    "default-src 'none'; base-uri 'none'; frame-ancestors 'none'; "
    "form-action 'self'; object-src 'none'; script-src 'self'; "
    "style-src 'self'; img-src 'self' data:; connect-src 'self';"
)
SECURITY_HEADERS: tuple[tuple[bytes, bytes], ...] = (
    (b"content-security-policy", CONTENT_SECURITY_POLICY.encode("ascii")),
    (b"x-content-type-options", b"nosniff"),
    (b"x-frame-options", b"DENY"),
    (b"referrer-policy", b"no-referrer"),
    (
        b"permissions-policy",
        b"camera=(), microphone=(), geolocation=(), payment=(), usb=(), "
        b"accelerometer=(), gyroscope=(), magnetometer=()",
    ),
    (b"cross-origin-opener-policy", b"same-origin"),
    (b"cross-origin-resource-policy", b"same-origin"),
    (b"strict-transport-security", b"max-age=31536000"),
    (b"cache-control", b"no-store"),
)
_SESSION_DATABASE_CONCURRENCY = 4


class _RequestDisconnected(Exception):
    """Stop a state-changing request whose body never arrived completely."""


class ConsoleSecurityMiddleware:
    """Authenticate, constrain, and instrument every HTTP request fail closed."""

    def __init__(
        self,
        app: ASGIApp,
        *,
        config: ConsoleConfig,
        sessions: ConsoleSessionStore,
        abuse_controls: ConsoleAbuseControls,
        monotonic_clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.app = app
        self.config = config
        self.sessions = sessions
        self.abuse_controls = abuse_controls
        self.monotonic_clock = monotonic_clock
        self.session_database_limiter = CapacityLimiter(_SESSION_DATABASE_CONCURRENCY)

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        request_id = secrets.token_hex(16)
        started = self.monotonic_clock()
        status_code = HTTPStatus.INTERNAL_SERVER_ERROR
        identity_hash: str | None = None
        response_cookie: tuple[str, int] | None = None
        clear_cookie = False
        response_started = False

        async def secured_send(message: Message) -> None:
            nonlocal response_started, status_code
            if message["type"] == "http.response.start":
                response_started = True
                status_code = int(message["status"])
                headers = _replace_security_headers(list(message.get("headers", [])))
                headers.append((b"x-request-id", request_id.encode("ascii")))
                should_clear = clear_cookie or bool(
                    _request_state(scope).get("pine.clear_session_cookie")
                )
                if should_clear:
                    headers.append((b"set-cookie", _expired_cookie()))
                elif response_cookie is not None:
                    cookie_value, max_age_seconds = response_cookie
                    headers.append(
                        (
                            b"set-cookie",
                            _session_cookie(
                                cookie_value,
                                max_age_seconds=max_age_seconds,
                            ),
                        )
                    )
                message["headers"] = headers
            await send(message)

        try:
            path = str(scope.get("path", ""))
            if path in PUBLIC_HEALTH_PATHS:
                await self.app(scope, receive, secured_send)
                return

            identity = authenticate_tailscale_identity(
                scope,
                socket_path=self.config.socket_path,
                allowed_identities=self.config.allowed_identities,
            )
            if identity is None:
                await _send_plain(secured_send, HTTPStatus.FORBIDDEN, "Forbidden")
                return
            identity_hash = hash_user_identity(identity.user_id)

            if not _valid_host(scope, self.config.allowed_host):
                await _send_plain(secured_send, HTTPStatus.BAD_REQUEST, "Bad Request")
                return

            method = str(scope.get("method", "GET")).upper()
            state_changing = method not in {"GET", "HEAD", "OPTIONS"}
            if state_changing and not _valid_origin(scope, self.config.allowed_host):
                await _send_plain(secured_send, HTTPStatus.FORBIDDEN, "Forbidden")
                return

            content_length = _content_length(scope)
            if content_length is not None and content_length > self.config.max_request_bytes:
                await _send_plain(secured_send, HTTPStatus.REQUEST_ENTITY_TOO_LARGE, "Too Large")
                return

            csrf_header = single_header(scope, "x-pine-csrf-token")
            if state_changing and csrf_header is None and not _is_urlencoded(scope):
                await _send_plain(secured_send, HTTPStatus.FORBIDDEN, "Forbidden")
                return

            cookie_value = _session_cookie_value(scope)
            lookup = await run_sync(
                self.sessions.lookup,
                cookie_value,
                identity.user_id,
                limiter=self.session_database_limiter,
            )
            if lookup.status is SessionLookupStatus.IDENTITY_MISMATCH:
                clear_cookie = True
                await _send_plain(secured_send, HTTPStatus.FORBIDDEN, "Forbidden")
                return
            if lookup.status is SessionLookupStatus.VALID:
                session = lookup.session
                if session is None:
                    raise RuntimeError("valid console session lookup lacked state")
                if cookie_value is None:
                    raise RuntimeError("valid console session lacked a browser cookie")
                response_cookie = (cookie_value, _session_cookie_max_age(session))
            else:
                if state_changing:
                    clear_cookie = lookup.status is SessionLookupStatus.EXPIRED
                    await _send_plain(secured_send, HTTPStatus.FORBIDDEN, "Forbidden")
                    return
                if not _valid_session_establishment(scope, method):
                    clear_cookie = lookup.status is SessionLookupStatus.EXPIRED
                    await _send_plain(secured_send, HTTPStatus.FORBIDDEN, "Forbidden")
                    return
                self.abuse_controls.session_establishment(identity_hash)
                await run_sync(
                    self.sessions.cleanup_expired,
                    limiter=self.session_database_limiter,
                )
                created = await run_sync(
                    self.sessions.create,
                    identity.user_id,
                    limiter=self.session_database_limiter,
                )
                session = created.session
                response_cookie = (
                    created.cookie_value,
                    _session_cookie_max_age(created.session),
                )

            state = _request_state(scope)
            state["pine.identity"] = identity.user_id
            state["pine.identity_hash"] = identity_hash
            state["pine.session"] = session

            if state_changing:
                if csrf_header is not None and not session.validates_csrf(
                    csrf_header, method, path
                ):
                    await _send_plain(secured_send, HTTPStatus.FORBIDDEN, "Forbidden")
                    return

            body = await _read_bounded_body(receive, self.config.max_request_bytes)
            if body is None:
                await _send_plain(secured_send, HTTPStatus.REQUEST_ENTITY_TOO_LARGE, "Too Large")
                return

            if state_changing and _is_urlencoded(scope):
                form_token, form_field_names, form_values = _form_csrf(body)
                state["pine.form_field_names"] = form_field_names
                state["pine.form_values"] = form_values
                if csrf_header is None and (
                    form_token is None or not session.validates_csrf(form_token, method, path)
                ):
                    await _send_plain(secured_send, HTTPStatus.FORBIDDEN, "Forbidden")
                    return

            await self.app(scope, _replay_body(body), secured_send)
        except _RequestDisconnected:
            status_code = 499
        except RateLimitExceeded as exc:
            if response_started:
                raise
            await _send_plain(
                secured_send,
                HTTPStatus.TOO_MANY_REQUESTS,
                "Too Many Requests",
                extra_headers=((b"retry-after", str(exc.retry_after_seconds).encode("ascii")),),
            )
        except ConsoleError:
            LOGGER.error(
                _log_event(
                    "console_request_failed_closed",
                    request_id=request_id,
                    route=str(scope.get("path", "")),
                    status=HTTPStatus.SERVICE_UNAVAILABLE,
                    identity_hash=identity_hash,
                )
            )
            if response_started:
                raise
            await _send_plain(secured_send, HTTPStatus.SERVICE_UNAVAILABLE, "Unavailable")
        except Exception:
            LOGGER.error(
                _log_event(
                    "console_request_error",
                    request_id=request_id,
                    route=str(scope.get("path", "")),
                    status=HTTPStatus.INTERNAL_SERVER_ERROR,
                    identity_hash=identity_hash,
                ),
                exc_info=True,
            )
            if response_started:
                raise
            await _send_plain(secured_send, HTTPStatus.INTERNAL_SERVER_ERROR, "Error")
        finally:
            duration_ms = max(0, round((self.monotonic_clock() - started) * 1000))
            LOGGER.info(
                _log_event(
                    "console_http_request",
                    request_id=request_id,
                    route=str(scope.get("path", "")),
                    status=int(status_code),
                    identity_hash=identity_hash,
                    duration_ms=duration_ms,
                )
            )


def require_session(scope: Scope) -> ConsoleSession:
    """Return middleware-authenticated session state to route handlers."""

    session = _request_state(scope).get("pine.session")
    if not isinstance(session, ConsoleSession):
        raise RuntimeError("authenticated console session is unavailable")
    return session


def require_identity(scope: Scope) -> str:
    """Return the normalized owner for user-scoped workflow calls."""

    identity = _request_state(scope).get("pine.identity")
    if not isinstance(identity, str):
        raise RuntimeError("authenticated console identity is unavailable")
    return identity


def require_form_fields(scope: Scope) -> frozenset[str]:
    """Return the middleware-validated form field names for a route."""

    field_names = _request_state(scope).get("pine.form_field_names", frozenset())
    if not isinstance(field_names, frozenset) or not all(
        isinstance(field, str) for field in field_names
    ):
        raise RuntimeError("validated console form fields are unavailable")
    return field_names


def require_form_values(scope: Scope) -> dict[str, tuple[str, ...]]:
    """Return bounded form values parsed once by the security boundary."""

    values = _request_state(scope).get("pine.form_values")
    if not isinstance(values, dict) or not all(
        isinstance(field, str)
        and isinstance(items, tuple)
        and all(isinstance(item, str) for item in items)
        for field, items in values.items()
    ):
        raise RuntimeError("validated console form values are unavailable")
    return values


def clear_session_cookie(scope: Scope) -> None:
    """Tell the response boundary to expire the authenticated browser cookie."""

    _request_state(scope)["pine.clear_session_cookie"] = True


def _request_state(scope: Scope) -> dict[str, Any]:
    state = scope.setdefault("state", {})
    if not isinstance(state, dict):
        raise RuntimeError("ASGI request state is invalid")
    return state


def _valid_host(scope: Scope, allowed_host: str) -> bool:
    value = single_header(scope, "host")
    return value is not None and secrets.compare_digest(value.lower(), allowed_host)


def _valid_origin(scope: Scope, allowed_host: str) -> bool:
    value = single_header(scope, "origin")
    expected = f"https://{allowed_host}"
    return value is not None and secrets.compare_digest(value, expected)


def _valid_session_establishment(scope: Scope, method: str) -> bool:
    """Allow a new browser session only for a direct or same-origin document load."""

    fetch_site = single_header(scope, "sec-fetch-site")
    fetch_mode = single_header(scope, "sec-fetch-mode")
    fetch_destination = single_header(scope, "sec-fetch-dest")
    return (
        method == "GET"
        and fetch_site in {"none", "same-origin"}
        and fetch_mode == "navigate"
        and fetch_destination == "document"
    )


def _session_cookie_value(scope: Scope) -> str | None:
    headers = scope.get("headers", [])
    raw_values = [value for name, value in headers if name.lower() == b"cookie"]
    if not raw_values:
        return None
    try:
        combined = b";".join(raw_values).decode("ascii")
    except UnicodeDecodeError:
        return None
    matches: list[str] = []
    for item in combined.split(";"):
        name, separator, value = item.strip().partition("=")
        if separator and name == SESSION_COOKIE_NAME:
            matches.append(value)
    return matches[0] if len(matches) == 1 else None


def _content_length(scope: Scope) -> int | None:
    value = single_header(scope, "content-length")
    if value is None:
        return None
    try:
        parsed = int(value, 10)
    except ValueError:
        return None
    return parsed if parsed >= 0 else None


def _is_urlencoded(scope: Scope) -> bool:
    content_type = single_header(scope, "content-type")
    if content_type is None:
        return False
    return content_type.partition(";")[0].strip().lower() == "application/x-www-form-urlencoded"


def _form_csrf(
    body: bytes,
) -> tuple[str | None, frozenset[str], dict[str, tuple[str, ...]]]:
    try:
        parsed = parse_qs(
            body.decode("utf-8"),
            keep_blank_values=True,
            strict_parsing=False,
            max_num_fields=64,
        )
    except (UnicodeDecodeError, ValueError):
        return None, frozenset(), {}
    values = parsed.get("csrf_token")
    token = values[0] if values is not None and len(values) == 1 else None
    frozen_values = {field: tuple(items) for field, items in parsed.items()}
    return token, frozenset(parsed), frozen_values


async def _read_bounded_body(receive: Receive, limit: int) -> bytes | None:
    chunks: list[bytes] = []
    size = 0
    while True:
        message = await receive()
        if message["type"] == "http.disconnect":
            raise _RequestDisconnected
        if message["type"] != "http.request":
            continue
        chunk = message.get("body", b"")
        size += len(chunk)
        if size > limit:
            return None
        chunks.append(chunk)
        if not message.get("more_body", False):
            return b"".join(chunks)


def _replay_body(body: bytes) -> Receive:
    sent = False

    async def receive() -> Message:
        nonlocal sent
        if sent:
            return {"type": "http.request", "body": b"", "more_body": False}
        sent = True
        return {"type": "http.request", "body": body, "more_body": False}

    return receive


async def _send_plain(
    send: Send,
    status: int,
    message: str,
    *,
    extra_headers: tuple[tuple[bytes, bytes], ...] = (),
) -> None:
    body = message.encode("ascii")
    await send(
        {
            "type": "http.response.start",
            "status": int(status),
            "headers": [
                (b"content-type", b"text/plain; charset=utf-8"),
                (b"content-length", str(len(body)).encode("ascii")),
                *extra_headers,
            ],
        }
    )
    await send({"type": "http.response.body", "body": body})


def _replace_security_headers(headers: list[tuple[bytes, bytes]]) -> list[tuple[bytes, bytes]]:
    locked = {name for name, _value in SECURITY_HEADERS}
    filtered = [(name, value) for name, value in headers if name.lower() not in locked]
    filtered.extend(SECURITY_HEADERS)
    return filtered


def _session_cookie(value: str, *, max_age_seconds: int) -> bytes:
    return (
        f"{SESSION_COOKIE_NAME}={value}; Path=/; Max-Age={max_age_seconds}; "
        "Secure; HttpOnly; SameSite=Strict"
    ).encode("ascii")


def _session_cookie_max_age(session: ConsoleSession) -> int:
    remaining = session.idle_expires_at - session.last_seen_at
    return max(1, math.ceil(remaining.total_seconds()))


def _expired_cookie() -> bytes:
    return (
        f"{SESSION_COOKIE_NAME}=; Path=/; Max-Age=0; "
        "Expires=Thu, 01 Jan 1970 00:00:00 GMT; Secure; HttpOnly; SameSite=Strict"
    ).encode("ascii")


def _log_event(
    event: str,
    *,
    request_id: str,
    route: str,
    status: int,
    identity_hash: str | None,
    duration_ms: int | None = None,
) -> str:
    fields: dict[str, str | int] = {
        "event": event,
        "request_id": request_id,
        "route": route,
        "status": int(status),
    }
    if identity_hash is not None:
        fields["identity_hash"] = identity_hash
    if duration_ms is not None:
        fields["duration_ms"] = duration_ms
    return json.dumps(fields, sort_keys=True, separators=(",", ":"))
