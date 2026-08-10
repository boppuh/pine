"""Opaque, server-side browser sessions for the private console."""

from __future__ import annotations

import base64
import hashlib
import hmac
import re
import secrets
import sqlite3
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import StrEnum

from ledger.console.auth import normalize_user_identity
from ledger.console.errors import ConsoleSessionError
from ledger.console.state import ConsoleStateStore

SESSION_COOKIE_NAME = "__Host-pine_session"
_OPAQUE_TOKEN = re.compile(r"^[A-Za-z0-9_-]{43}$")


class SessionLookupStatus(StrEnum):
    """Non-secret result of looking up a presented session cookie."""

    VALID = "valid"
    MISSING = "missing"
    EXPIRED = "expired"
    IDENTITY_MISMATCH = "identity_mismatch"


@dataclass(frozen=True, slots=True)
class ConsoleSession:
    """Validated server-side session state attached to one request."""

    session_hash: str
    user_id: str
    created_at: datetime
    last_seen_at: datetime
    absolute_expires_at: datetime
    idle_expires_at: datetime
    csrf_secret: str = field(repr=False)

    def csrf_token(self, method: str, path: str) -> str:
        """Derive a method-and-path-bound token without exposing the session secret."""

        secret = _decode_secret(self.csrf_secret)
        message = f"{method.upper()}\n{path}".encode()
        digest = hmac.digest(secret, message, "sha256")
        return base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")

    def validates_csrf(self, token: str, method: str, path: str) -> bool:
        """Compare a submitted token in constant time."""

        if not _OPAQUE_TOKEN.fullmatch(token):
            return False
        return hmac.compare_digest(token, self.csrf_token(method, path))


@dataclass(frozen=True, slots=True)
class CreatedSession:
    """A new server session and its one-time browser cookie value."""

    session: ConsoleSession
    cookie_value: str = field(repr=False)


@dataclass(frozen=True, slots=True)
class SessionLookup:
    """Validated session lookup result."""

    status: SessionLookupStatus
    session: ConsoleSession | None = None


class ConsoleSessionStore:
    """Persist only hashes of opaque cookie values in the console database."""

    def __init__(
        self,
        store: ConsoleStateStore,
        *,
        absolute_lifetime: timedelta = timedelta(hours=8),
        idle_lifetime: timedelta = timedelta(minutes=30),
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if absolute_lifetime <= timedelta(0) or idle_lifetime <= timedelta(0):
            raise ValueError("session lifetimes must be positive")
        if idle_lifetime >= absolute_lifetime:
            raise ValueError("session idle lifetime must be shorter than absolute lifetime")
        self.store = store
        self.absolute_lifetime = absolute_lifetime
        self.idle_lifetime = idle_lifetime
        self.clock = clock or (lambda: datetime.now(UTC))

    def create(self, user_id: str) -> CreatedSession:
        """Create a rotated session with at least 256 bits of random entropy."""

        identity = normalize_user_identity(user_id)
        now = self._now()
        absolute_expires_at = now + self.absolute_lifetime
        idle_expires_at = min(now + self.idle_lifetime, absolute_expires_at)
        for _attempt in range(5):
            cookie_value = secrets.token_urlsafe(32)
            csrf_secret = secrets.token_urlsafe(32)
            session_hash = hash_session_id(cookie_value)
            try:
                with self.store.transaction() as connection:
                    connection.execute(
                        """
                        INSERT INTO console_sessions (
                            session_hash, user_id, csrf_secret, created_at, last_seen_at,
                            absolute_expires_at, idle_expires_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            session_hash,
                            identity,
                            csrf_secret,
                            _timestamp(now),
                            _timestamp(now),
                            _timestamp(absolute_expires_at),
                            _timestamp(idle_expires_at),
                        ),
                    )
            except sqlite3.IntegrityError:
                continue
            except sqlite3.DatabaseError as exc:
                raise ConsoleSessionError("console session could not be created") from exc
            session = ConsoleSession(
                session_hash=session_hash,
                user_id=identity,
                csrf_secret=csrf_secret,
                created_at=now,
                last_seen_at=now,
                absolute_expires_at=absolute_expires_at,
                idle_expires_at=idle_expires_at,
            )
            return CreatedSession(session=session, cookie_value=cookie_value)
        raise ConsoleSessionError("console session identity collision")

    def lookup(self, cookie_value: str | None, user_id: str) -> SessionLookup:
        """Validate ownership and expiry, then atomically advance idle expiry."""

        if cookie_value is None or not _OPAQUE_TOKEN.fullmatch(cookie_value):
            return SessionLookup(SessionLookupStatus.MISSING)
        identity = normalize_user_identity(user_id)
        session_hash = hash_session_id(cookie_value)
        now = self._now()
        try:
            with self.store.transaction() as connection:
                row = connection.execute(
                    "SELECT * FROM console_sessions WHERE session_hash = ?",
                    (session_hash,),
                ).fetchone()
                if row is None:
                    return SessionLookup(SessionLookupStatus.MISSING)
                session = _session_from_row(row)
                if not hmac.compare_digest(session.user_id, identity):
                    connection.execute(
                        "DELETE FROM console_sessions WHERE session_hash = ?",
                        (session_hash,),
                    )
                    return SessionLookup(SessionLookupStatus.IDENTITY_MISMATCH)
                if now >= session.absolute_expires_at or now >= session.idle_expires_at:
                    connection.execute(
                        "DELETE FROM console_sessions WHERE session_hash = ?",
                        (session_hash,),
                    )
                    return SessionLookup(SessionLookupStatus.EXPIRED)
                idle_expires_at = min(now + self.idle_lifetime, session.absolute_expires_at)
                connection.execute(
                    """
                    UPDATE console_sessions
                    SET last_seen_at = ?, idle_expires_at = ?
                    WHERE session_hash = ?
                    """,
                    (_timestamp(now), _timestamp(idle_expires_at), session_hash),
                )
        except ConsoleSessionError:
            raise
        except (sqlite3.DatabaseError, TypeError, ValueError) as exc:
            raise ConsoleSessionError("console session state failed validation") from exc
        return SessionLookup(
            SessionLookupStatus.VALID,
            ConsoleSession(
                session_hash=session.session_hash,
                user_id=session.user_id,
                csrf_secret=session.csrf_secret,
                created_at=session.created_at,
                last_seen_at=now,
                absolute_expires_at=session.absolute_expires_at,
                idle_expires_at=idle_expires_at,
            ),
        )

    def delete_hash(self, session_hash: str) -> None:
        """Delete an already authenticated session by its non-secret durable hash."""

        if not re.fullmatch(r"sha256:[0-9a-f]{64}", session_hash):
            raise ConsoleSessionError("console session hash is invalid")
        try:
            with self.store.transaction() as connection:
                connection.execute(
                    "DELETE FROM console_sessions WHERE session_hash = ?",
                    (session_hash,),
                )
        except sqlite3.DatabaseError as exc:
            raise ConsoleSessionError("console session could not be deleted") from exc

    def cleanup_expired(self) -> int:
        """Remove expired sessions without touching workflow or ledger state."""

        now = _timestamp(self._now())
        try:
            with self.store.transaction() as connection:
                cursor = connection.execute(
                    """
                    DELETE FROM console_sessions
                    WHERE absolute_expires_at <= ? OR idle_expires_at <= ?
                    """,
                    (now, now),
                )
                return cursor.rowcount
        except sqlite3.DatabaseError as exc:
            raise ConsoleSessionError("expired console sessions could not be removed") from exc

    def _now(self) -> datetime:
        value = self.clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise ConsoleSessionError("console session clock returned a naive timestamp")
        return value.astimezone(UTC)


def hash_session_id(cookie_value: str) -> str:
    """Hash an opaque browser value before any durable lookup or storage."""

    return f"sha256:{hashlib.sha256(cookie_value.encode('ascii')).hexdigest()}"


def hash_user_identity(user_id: str) -> str:
    """Return a non-email key suitable for logs and abuse controls."""

    normalized = normalize_user_identity(user_id)
    return f"sha256:{hashlib.sha256(normalized.encode('ascii')).hexdigest()}"


def _session_from_row(row: sqlite3.Row) -> ConsoleSession:
    session_hash = str(row["session_hash"])
    user_id = normalize_user_identity(str(row["user_id"]))
    csrf_secret = str(row["csrf_secret"])
    created_at = datetime.fromisoformat(str(row["created_at"]))
    last_seen_at = datetime.fromisoformat(str(row["last_seen_at"]))
    idle_expires_at = datetime.fromisoformat(str(row["idle_expires_at"]))
    absolute_expires_at = datetime.fromisoformat(str(row["absolute_expires_at"]))
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", session_hash):
        raise ConsoleSessionError("console session hash is invalid")
    _decode_secret(csrf_secret)
    timestamps = (created_at, last_seen_at, idle_expires_at, absolute_expires_at)
    if any(value.tzinfo is None or value.utcoffset() is None for value in timestamps):
        raise ConsoleSessionError("console session timestamps are invalid")
    if not created_at <= last_seen_at <= idle_expires_at <= absolute_expires_at:
        raise ConsoleSessionError("console session timestamp order is invalid")
    return ConsoleSession(
        session_hash=session_hash,
        user_id=user_id,
        csrf_secret=csrf_secret,
        created_at=created_at,
        last_seen_at=last_seen_at,
        idle_expires_at=idle_expires_at,
        absolute_expires_at=absolute_expires_at,
    )


def _decode_secret(value: str) -> bytes:
    if not _OPAQUE_TOKEN.fullmatch(value):
        raise ConsoleSessionError("console CSRF secret is invalid")
    try:
        decoded = base64.b64decode(value + "=", altchars=b"-_", validate=True)
    except (ValueError, TypeError) as exc:
        raise ConsoleSessionError("console CSRF secret is invalid") from exc
    if len(decoded) != 32:
        raise ConsoleSessionError("console CSRF secret has invalid entropy")
    return decoded


def _timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="microseconds")
