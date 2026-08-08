from __future__ import annotations

import asyncio
import logging
import re
import threading
import time
from datetime import timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

import httpx
import uvicorn
from fastapi.testclient import TestClient

from ledger.console.app import _templates, create_console_app
from ledger.console.config import ConsoleConfig
from ledger.console.security import CONTENT_SECURITY_POLICY
from ledger.console.sessions import (
    SESSION_COOKIE_NAME,
    ConsoleSessionStore,
    hash_session_id,
)
from ledger.console.state import ConsoleStateStore

from .conftest import FakeBackend, MutableClock

IDENTITY = "operator@example.com"
SECOND_IDENTITY = "second@example.com"
HOST = "pine.example.ts.net"
TOKEN = "readiness-token-" + "s" * 48


class UnixSocketScope:
    """Make TestClient exercise the same ASGI metadata Uvicorn emits for UDS."""

    def __init__(self, app: Any, socket_path: Path) -> None:
        self.app = app
        self.socket_path = socket_path

    async def __call__(self, scope, receive, send) -> None:
        if scope["type"] == "http":
            scope = dict(scope)
            scope["client"] = None
            scope["server"] = (str(self.socket_path), None)
        await self.app(scope, receive, send)


def _build_app(
    tmp_path: Path,
    backend: FakeBackend,
    clock: MutableClock,
    *,
    allowed_identities: tuple[str, ...] = (IDENTITY,),
    socket_path: Path | None = None,
):
    socket_path = socket_path or tmp_path / "console.sock"
    state_path = tmp_path / "state" / "console.db"
    credential = tmp_path / "backend-token"
    credential.write_text(TOKEN, encoding="ascii")
    credential.chmod(0o600)
    config = ConsoleConfig(
        socket_path=socket_path,
        state_path=state_path,
        backend_credential_path=credential,
        allowed_host=HOST,
        allowed_identities=allowed_identities,
    )
    store = ConsoleStateStore(state_path, clock=clock)
    sessions = ConsoleSessionStore(
        store,
        absolute_lifetime=config.session_absolute_lifetime,
        idle_lifetime=config.session_idle_lifetime,
        clock=clock,
    )
    app = create_console_app(config, store, backend, sessions=sessions)
    return app, config, store, sessions


def _client(app, config: ConsoleConfig, *, unix: bool = True) -> TestClient:
    target = UnixSocketScope(app, config.socket_path) if unix else app
    return TestClient(target, base_url=f"https://{HOST}")


def _identity_headers(identity: str = IDENTITY) -> dict[str, str]:
    return {"Tailscale-User-Login": identity}


def _csrf_from_html(content: str) -> str:
    match = re.search(r'name="csrf_token" value="([A-Za-z0-9_-]{43})"', content)
    assert match is not None
    return match.group(1)


def test_identity_is_missing_unapproved_malformed_or_tcp_spoofed(
    tmp_path: Path,
    fake_backend: FakeBackend,
    clock: MutableClock,
) -> None:
    app, config, _store, _sessions = _build_app(tmp_path, fake_backend, clock)

    with _client(app, config) as client:
        assert client.get("/").status_code == 403
        assert client.get("/", headers=_identity_headers("other@example.com")).status_code == 403
        assert client.get("/", headers=_identity_headers("bad identity")).status_code == 403
        duplicate = client.get(
            "/",
            headers=[
                ("Tailscale-User-Login", IDENTITY),
                ("Tailscale-User-Login", IDENTITY),
            ],
        )
        assert duplicate.status_code == 403

    with _client(app, config, unix=False) as tcp_client:
        assert tcp_client.get("/", headers=_identity_headers()).status_code == 403


def test_real_uvicorn_unix_socket_scope_establishes_identity(
    tmp_path: Path,
    fake_backend: FakeBackend,
    clock: MutableClock,
) -> None:
    socket_path = Path("/tmp") / f"pine-console-test-{uuid4().hex}.sock"
    app, config, _store, _sessions = _build_app(
        tmp_path,
        fake_backend,
        clock,
        socket_path=socket_path,
    )
    server = uvicorn.Server(
        uvicorn.Config(
            app,
            uds=str(config.socket_path),
            access_log=False,
            log_level="error",
        )
    )
    worker = threading.Thread(target=server.run, daemon=True)
    worker.start()
    deadline = time.monotonic() + 5
    while not config.socket_path.exists() and time.monotonic() < deadline:
        time.sleep(0.01)
    assert config.socket_path.exists()
    try:
        transport = httpx.HTTPTransport(uds=str(config.socket_path))
        with httpx.Client(transport=transport, base_url=f"http://{HOST}") as client:
            response = client.get("/", headers=_identity_headers())
        assert response.status_code == 200
        assert SESSION_COOKIE_NAME in response.headers["set-cookie"]
    finally:
        server.should_exit = True
        worker.join(5)
        socket_path.unlink(missing_ok=True)
    assert not worker.is_alive()


def test_route_inventory_is_public_only_for_health(
    tmp_path: Path,
    fake_backend: FakeBackend,
    clock: MutableClock,
) -> None:
    app, config, _store, _sessions = _build_app(tmp_path, fake_backend, clock)

    with _client(app, config) as client:
        for public_path in ("/healthz", "/livez", "/readyz"):
            assert client.get(public_path).status_code == 200
        for protected_path in ("/", "/status", "/assets/console.css", "/unknown"):
            assert client.get(protected_path).status_code == 403
        assert client.post("/session/logout").status_code == 403


def test_authenticated_shell_sets_exact_cookie_headers_and_local_assets(
    tmp_path: Path,
    fake_backend: FakeBackend,
    clock: MutableClock,
) -> None:
    app, config, store, _sessions = _build_app(tmp_path, fake_backend, clock)

    with _client(app, config) as client:
        response = client.get("/", headers=_identity_headers())
        cookie = response.cookies.get(SESSION_COOKIE_NAME)
        assert cookie is not None
        set_cookie = response.headers["set-cookie"]
        assert set_cookie.startswith(f"{SESSION_COOKIE_NAME}=")
        assert "Secure" in set_cookie
        assert "HttpOnly" in set_cookie
        assert "SameSite=Strict" in set_cookie
        assert "Path=/" in set_cookie
        assert "Domain=" not in set_cookie
        assert response.headers["content-security-policy"] == CONTENT_SECURITY_POLICY
        assert response.headers["x-content-type-options"] == "nosniff"
        assert response.headers["x-frame-options"] == "DENY"
        assert response.headers["referrer-policy"] == "no-referrer"
        assert response.headers["cross-origin-opener-policy"] == "same-origin"
        assert response.headers["cross-origin-resource-policy"] == "same-origin"
        assert response.headers["strict-transport-security"] == "max-age=31536000"
        assert "camera=()" in response.headers["permissions-policy"]
        assert "microphone=()" in response.headers["permissions-policy"]
        assert "geolocation=()" in response.headers["permissions-policy"]
        assert response.headers["cache-control"] == "no-store"
        assert re.fullmatch(r"[0-9a-f]{32}", response.headers["x-request-id"])
        assert "<header" in response.text
        assert "<nav" in response.text
        assert '<main id="main-content"' in response.text
        assert "focus-visible" not in response.text

        repeated = client.get("/", headers=_identity_headers())
        assert repeated.cookies.get(SESSION_COOKIE_NAME) == cookie
        assert "Max-Age=1800" in repeated.headers["set-cookie"]
        status = client.get("/status", headers=_identity_headers())
        assert status.status_code == 200
        assert "Console status" in status.text
        assert "Version 2" in status.text
        asset = client.get("/assets/console.css", headers=_identity_headers())
        assert asset.status_code == 200
        assert ":focus-visible" in asset.text
        assert "@media (max-width: 48rem)" in asset.text
        assert "https://" not in asset.text
        assert "http://" not in asset.text
        assert "@import" not in asset.text

    connection = store.connect()
    try:
        row = connection.execute("SELECT session_hash FROM console_sessions").fetchone()
    finally:
        connection.close()
    assert row is not None
    assert row["session_hash"] == hash_session_id(cookie)
    assert row["session_hash"] != cookie


def test_active_session_refreshes_cookie_and_preserves_original_csrf(
    tmp_path: Path,
    fake_backend: FakeBackend,
    clock: MutableClock,
) -> None:
    app, config, _store, _sessions = _build_app(tmp_path, fake_backend, clock)

    with _client(app, config) as client:
        home = client.get("/", headers=_identity_headers())
        cookie = home.cookies.get(SESSION_COOKIE_NAME)
        csrf = _csrf_from_html(home.text)
        assert cookie is not None

        clock.advance(timedelta(minutes=20))
        refreshed = client.get("/status", headers=_identity_headers())
        assert refreshed.cookies.get(SESSION_COOKIE_NAME) == cookie
        assert "Max-Age=1800" in refreshed.headers["set-cookie"]

        clock.advance(timedelta(minutes=20))
        logged_out = client.post(
            "/session/logout",
            headers={**_identity_headers(), "Origin": f"https://{HOST}"},
            data={"csrf_token": csrf},
        )
        assert logged_out.status_code == 200


def test_sessionless_writes_are_denied_without_creating_or_rotating_session(
    tmp_path: Path,
    fake_backend: FakeBackend,
    clock: MutableClock,
) -> None:
    app, config, store, _sessions = _build_app(tmp_path, fake_backend, clock)

    with _client(app, config) as client:
        cross_origin = client.post(
            "/session/logout",
            headers={**_identity_headers(), "Origin": "https://evil.example"},
            data={"csrf_token": "x" * 43},
        )
        same_origin = client.post(
            "/session/logout",
            headers={**_identity_headers(), "Origin": f"https://{HOST}"},
            data={"csrf_token": "x" * 43},
        )

    assert cross_origin.status_code == 403
    assert same_origin.status_code == 403
    assert "set-cookie" not in cross_origin.headers
    assert "set-cookie" not in same_origin.headers
    connection = store.connect()
    try:
        assert connection.execute("SELECT COUNT(*) FROM console_sessions").fetchone()[0] == 0
    finally:
        connection.close()


def test_session_database_wait_does_not_block_liveness(
    tmp_path: Path,
    fake_backend: FakeBackend,
    clock: MutableClock,
    monkeypatch,
) -> None:
    app, config, _store, sessions = _build_app(tmp_path, fake_backend, clock)
    started = threading.Event()
    release = threading.Event()
    original_lookup = sessions.lookup

    def blocking_lookup(cookie_value, user_id):
        started.set()
        assert release.wait(5)
        return original_lookup(cookie_value, user_id)

    monkeypatch.setattr(sessions, "lookup", blocking_lookup)

    async def exercise() -> tuple[httpx.Response, httpx.Response]:
        transport = httpx.ASGITransport(app=UnixSocketScope(app, config.socket_path))
        async with httpx.AsyncClient(
            transport=transport,
            base_url=f"https://{HOST}",
        ) as client:
            protected = asyncio.create_task(client.get("/", headers=_identity_headers()))
            assert await asyncio.to_thread(started.wait, 2)
            live = await asyncio.wait_for(client.get("/livez"), timeout=1)
            release.set()
            return await protected, live

    try:
        protected, live = asyncio.run(exercise())
    finally:
        release.set()
    assert protected.status_code == 200
    assert live.status_code == 200


def test_session_establishment_cleans_abandoned_expired_rows(
    tmp_path: Path,
    fake_backend: FakeBackend,
    clock: MutableClock,
) -> None:
    app, config, store, sessions = _build_app(tmp_path, fake_backend, clock)
    orphaned = sessions.create(IDENTITY)
    clock.advance(timedelta(minutes=31))

    with _client(app, config) as client:
        established = client.get("/", headers=_identity_headers())

    assert established.status_code == 200
    connection = store.connect()
    try:
        rows = connection.execute("SELECT session_hash FROM console_sessions").fetchall()
    finally:
        connection.close()
    assert len(rows) == 1
    assert rows[0]["session_hash"] != orphaned.session.session_hash


def test_unknown_cookie_cannot_fix_a_session_and_expiry_rotates_it(
    tmp_path: Path,
    fake_backend: FakeBackend,
    clock: MutableClock,
) -> None:
    app, config, _store, _sessions = _build_app(tmp_path, fake_backend, clock)

    with _client(app, config) as client:
        attacker_value = "A" * 43
        client.cookies.set(SESSION_COOKIE_NAME, attacker_value, domain=HOST, path="/")
        fixed = client.get("/", headers=_identity_headers())
        first = fixed.cookies.get(SESSION_COOKIE_NAME)
        assert first is not None and first != attacker_value

        clock.advance(timedelta(minutes=31))
        rotated = client.get("/", headers=_identity_headers())
        second = rotated.cookies.get(SESSION_COOKIE_NAME)
        assert second is not None and second != first


def test_identity_mismatch_revokes_cookie_without_switching_owner(
    tmp_path: Path,
    fake_backend: FakeBackend,
    clock: MutableClock,
) -> None:
    app, config, store, _sessions = _build_app(
        tmp_path,
        fake_backend,
        clock,
        allowed_identities=(IDENTITY, SECOND_IDENTITY),
    )

    with _client(app, config) as client:
        assert client.get("/", headers=_identity_headers()).status_code == 200
        mismatch = client.get("/", headers=_identity_headers(SECOND_IDENTITY))
        assert mismatch.status_code == 403
        assert "Max-Age=0" in mismatch.headers["set-cookie"]

    connection = store.connect()
    try:
        assert connection.execute("SELECT COUNT(*) FROM console_sessions").fetchone()[0] == 0
    finally:
        connection.close()


def test_csrf_origin_host_and_logout_are_enforced(
    tmp_path: Path,
    fake_backend: FakeBackend,
    clock: MutableClock,
) -> None:
    app, config, store, _sessions = _build_app(tmp_path, fake_backend, clock)

    with _client(app, config) as client:
        home = client.get("/", headers=_identity_headers())
        csrf = _csrf_from_html(home.text)
        form = {"csrf_token": csrf}
        missing_origin = client.post("/session/logout", headers=_identity_headers(), data=form)
        assert missing_origin.status_code == 403
        assert (
            client.post(
                "/session/logout",
                headers={**_identity_headers(), "Origin": "https://evil.example"},
                data=form,
            ).status_code
            == 403
        )
        unknown_field = client.post(
            "/session/logout",
            headers={**_identity_headers(), "Origin": f"https://{HOST}"},
            data={"csrf_token": csrf, "unexpected": "value"},
        )
        assert unknown_field.status_code == 422
        assert (
            client.post(
                "/session/logout",
                headers={**_identity_headers(), "Origin": f"https://{HOST}"},
                data={"csrf_token": "x" * 43},
            ).status_code
            == 403
        )
        assert (
            client.post(
                "/session/logout",
                headers={
                    **_identity_headers(),
                    "Host": "wrong.example",
                    "Origin": f"https://{HOST}",
                },
                data=form,
            ).status_code
            == 400
        )
        logged_out = client.post(
            "/session/logout",
            headers={**_identity_headers(), "Origin": f"https://{HOST}"},
            data=form,
        )
        assert logged_out.status_code == 200
        assert "Max-Age=0" in logged_out.headers["set-cookie"]

    connection = store.connect()
    try:
        assert connection.execute("SELECT COUNT(*) FROM console_sessions").fetchone()[0] == 0
    finally:
        connection.close()


def test_oversized_body_is_rejected_before_any_backend_operation(
    tmp_path: Path,
    fake_backend: FakeBackend,
    clock: MutableClock,
) -> None:
    app, config, _store, sessions = _build_app(tmp_path, fake_backend, clock)

    with _client(app, config) as client:
        home = client.get("/", headers=_identity_headers())
        cookie = home.cookies.get(SESSION_COOKIE_NAME)
        assert cookie is not None
        lookup = sessions.lookup(cookie, IDENTITY)
        assert lookup.session is not None
        csrf = lookup.session.csrf_token("POST", "/not-a-route")
        response = client.post(
            "/not-a-route",
            headers={
                **_identity_headers(),
                "Origin": f"https://{HOST}",
                "X-Pine-CSRF-Token": csrf,
                "Content-Type": "text/plain",
            },
            content=b"x" * (config.max_request_bytes + 1),
        )

    assert response.status_code == 413
    assert fake_backend.draft_requests == []
    assert fake_backend.capture_requests == []


def test_templates_escape_plain_text_and_logs_omit_secrets(
    tmp_path: Path,
    fake_backend: FakeBackend,
    clock: MutableClock,
    caplog,
) -> None:
    payload = '<script>alert("stored")</script><img src=x onerror=alert(1)>'
    rendered = _templates().env.get_template("plain_text.html").render(value=payload)
    assert "<script>" not in rendered
    assert "<img" not in rendered
    assert "&lt;script&gt;" in rendered
    app, config, _store, _sessions = _build_app(tmp_path, fake_backend, clock)

    with caplog.at_level(logging.INFO, logger="ledger.console.http"):
        with _client(app, config) as client:
            response = client.get("/", headers=_identity_headers())
            cookie = response.cookies.get(SESSION_COOKIE_NAME)

    assert response.status_code == 200
    assert TOKEN not in response.text + caplog.text
    assert IDENTITY not in response.text + caplog.text
    assert cookie is not None and cookie not in caplog.text
    assert "csrf_token" not in caplog.text


def test_health_is_public_but_contains_no_secret_and_readiness_fails_closed(
    tmp_path: Path,
    fake_backend: FakeBackend,
    clock: MutableClock,
) -> None:
    app, config, _store, _sessions = _build_app(tmp_path, fake_backend, clock)

    with _client(app, config, unix=False) as client:
        live = client.get("/livez")
        ready = client.get("/readyz")

    assert live.json() == {"status": "ok"}
    assert ready.json() == {
        "status": "ok",
        "backend_api_version": "v1",
        "console_schema_version": 2,
    }
    assert TOKEN not in live.text + ready.text

    Path(config.backend_credential_path).chmod(0o644)
    with _client(app, config, unix=False) as client:
        unavailable = client.get("/readyz")
    assert unavailable.status_code == 503
    assert unavailable.json() == {"status": "unavailable"}
