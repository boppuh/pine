from __future__ import annotations

import sqlite3
from datetime import timedelta

import pytest

from ledger.console.errors import ConsoleSessionError
from ledger.console.sessions import (
    ConsoleSessionStore,
    SessionLookupStatus,
    hash_session_id,
)
from ledger.console.state import ConsoleStateStore

from .conftest import MutableClock


def test_session_persists_only_cookie_hash_and_advances_idle_expiry(
    console_store: ConsoleStateStore,
    clock: MutableClock,
) -> None:
    sessions = ConsoleSessionStore(console_store, clock=clock)
    created = sessions.create(" User@Example.com ")

    connection = console_store.connect()
    try:
        row = connection.execute("SELECT * FROM console_sessions").fetchone()
    finally:
        connection.close()

    assert row is not None
    assert row["session_hash"] == hash_session_id(created.cookie_value)
    assert created.cookie_value not in tuple(str(value) for value in row)
    assert row["user_id"] == "user@example.com"
    initial_idle_expiry = created.session.idle_expires_at

    clock.advance(timedelta(minutes=10))
    lookup = sessions.lookup(created.cookie_value, "USER@example.com")

    assert lookup.status is SessionLookupStatus.VALID
    assert lookup.session is not None
    assert lookup.session.idle_expires_at > initial_idle_expiry


def test_session_idle_and_absolute_expiry_fail_closed(
    tmp_path,
    clock: MutableClock,
) -> None:
    store = ConsoleStateStore(tmp_path / "sessions" / "console.db", clock=clock)
    sessions = ConsoleSessionStore(store, clock=clock)
    idle = sessions.create("user@example.com")

    clock.advance(timedelta(minutes=31))
    assert (
        sessions.lookup(idle.cookie_value, "user@example.com").status is SessionLookupStatus.EXPIRED
    )

    absolute = sessions.create("user@example.com")
    for _ in range(23):
        clock.advance(timedelta(minutes=20))
        assert (
            sessions.lookup(absolute.cookie_value, "user@example.com").status
            is SessionLookupStatus.VALID
        )
    clock.advance(timedelta(minutes=21))
    assert (
        sessions.lookup(absolute.cookie_value, "user@example.com").status
        is SessionLookupStatus.EXPIRED
    )


def test_identity_mismatch_revokes_presented_session(
    console_store: ConsoleStateStore,
    clock: MutableClock,
) -> None:
    sessions = ConsoleSessionStore(console_store, clock=clock)
    created = sessions.create("owner@example.com")

    mismatch = sessions.lookup(created.cookie_value, "other@example.com")
    repeated = sessions.lookup(created.cookie_value, "owner@example.com")

    assert mismatch.status is SessionLookupStatus.IDENTITY_MISMATCH
    assert repeated.status is SessionLookupStatus.MISSING


def test_session_csrf_is_bound_to_method_and_path(
    console_store: ConsoleStateStore,
    clock: MutableClock,
) -> None:
    session = ConsoleSessionStore(console_store, clock=clock).create("user@example.com").session
    token = session.csrf_token("POST", "/workflow")

    assert session.validates_csrf(token, "POST", "/workflow")
    assert not session.validates_csrf(token, "POST", "/other")
    assert not session.validates_csrf(token, "DELETE", "/workflow")
    assert not session.validates_csrf("not-a-token", "POST", "/workflow")


def test_corrupt_session_state_is_never_authenticated(
    console_store: ConsoleStateStore,
    clock: MutableClock,
) -> None:
    sessions = ConsoleSessionStore(console_store, clock=clock)
    created = sessions.create("user@example.com")
    connection = console_store.connect()
    try:
        connection.execute("DROP TRIGGER console_sessions_identity_write_once")
        connection.execute("PRAGMA ignore_check_constraints = ON")
        connection.execute(
            "UPDATE console_sessions SET csrf_secret = ? WHERE session_hash = ?",
            ("!" * 43, created.session.session_hash),
        )
    finally:
        connection.close()

    with pytest.raises(ConsoleSessionError, match="CSRF secret is invalid"):
        sessions.lookup(created.cookie_value, "user@example.com")


def test_session_schema_rejects_identity_and_secret_rewrites(
    console_store: ConsoleStateStore,
    clock: MutableClock,
) -> None:
    created = ConsoleSessionStore(console_store, clock=clock).create("user@example.com")
    connection = console_store.connect()
    try:
        with pytest.raises(sqlite3.IntegrityError, match="write-once"):
            connection.execute(
                "UPDATE console_sessions SET user_id = ? WHERE session_hash = ?",
                ("other@example.com", created.session.session_hash),
            )
    finally:
        connection.close()
