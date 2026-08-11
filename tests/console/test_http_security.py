from __future__ import annotations

import asyncio
import logging
import re
import stat
import threading
import time
from datetime import timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

import httpx
import pytest
import uvicorn
from fastapi.testclient import TestClient

from ledger.console.app import _templates, create_console_app
from ledger.console.config import ConsoleConfig
from ledger.console.errors import BackendTransportError
from ledger.console.rate_limit import ConsoleAbuseControls, ConsoleRateLimiter
from ledger.console.security import CONTENT_SECURITY_POLICY, ConsoleSecurityMiddleware
from ledger.console.sessions import (
    SESSION_COOKIE_NAME,
    ConsoleSessionStore,
    hash_session_id,
)
from ledger.console.state import ConsoleStateStore
from ledger.console.unix_socket import CONSOLE_SOCKET_MODE, secure_unix_socket

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
    retention_sweep_interval_seconds: float = 300.0,
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
    app = create_console_app(
        config,
        store,
        backend,
        sessions=sessions,
        retention_sweep_interval_seconds=retention_sweep_interval_seconds,
    )
    return app, config, store, sessions


def _client(app, config: ConsoleConfig, *, unix: bool = True) -> TestClient:
    target = UnixSocketScope(app, config.socket_path) if unix else app
    return TestClient(target, base_url=f"https://{HOST}")


def _identity_headers(identity: str = IDENTITY) -> dict[str, str]:
    return {
        "Host": "localhost",
        "Tailscale-User-Login": identity,
        "X-Forwarded-Host": HOST,
        "X-Forwarded-Proto": "https",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Dest": "document",
    }


def test_long_running_console_continuously_removes_expired_workflows(
    tmp_path: Path,
    fake_backend: FakeBackend,
    clock: MutableClock,
) -> None:
    app, config, store, _sessions = _build_app(
        tmp_path,
        fake_backend,
        clock,
        retention_sweep_interval_seconds=0.01,
    )

    with _client(app, config) as client:
        assert client.get("/", headers=_identity_headers()).status_code == 200
        store.create_workflow(user_id=IDENTITY, source_text="Temporary hypothesis")
        clock.advance(timedelta(hours=25))
        deadline = time.monotonic() + 2
        remaining = 1
        while remaining and time.monotonic() < deadline:
            connection = store.connect()
            try:
                remaining = connection.execute("SELECT COUNT(*) FROM workflows").fetchone()[0]
            finally:
                connection.close()
            if remaining:
                time.sleep(0.01)

    assert remaining == 0


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


def test_tailscale_proxy_destination_is_exact_and_fails_closed(
    tmp_path: Path,
    fake_backend: FakeBackend,
    clock: MutableClock,
) -> None:
    app, config, _store, _sessions = _build_app(tmp_path, fake_backend, clock)

    with _client(app, config) as client:
        assert client.get("/", headers=_identity_headers()).status_code == 200
        for override in (
            {"Host": HOST},
            {"X-Forwarded-Host": "wrong.example"},
            {"X-Forwarded-Host": f"{HOST}:8443"},
            {"X-Forwarded-Proto": "http"},
        ):
            assert client.get("/", headers={**_identity_headers(), **override}).status_code == 400

        missing_forwarded_host = _identity_headers()
        del missing_forwarded_host["X-Forwarded-Host"]
        assert client.get("/", headers=missing_forwarded_host).status_code == 400

        duplicate_forwarded_host = client.get(
            "/",
            headers=[
                *_identity_headers().items(),
                ("X-Forwarded-Host", HOST),
            ],
        )
        assert duplicate_forwarded_host.status_code == 400

        assert (
            client.get(
                "/",
                headers={**_identity_headers(), "X-Forwarded-Host": f"{HOST}:443"},
            ).status_code
            == 200
        )


def test_real_uvicorn_unix_socket_scope_establishes_identity(
    tmp_path: Path,
    fake_backend: FakeBackend,
    clock: MutableClock,
) -> None:
    runtime_path = Path("/tmp") / f"pine-console-test-{uuid4().hex}"
    runtime_path.mkdir(mode=0o700)
    socket_path = runtime_path / "console.sock"
    app, config, _store, _sessions = _build_app(
        tmp_path,
        fake_backend,
        clock,
        socket_path=socket_path,
    )
    with secure_unix_socket(config.socket_path) as listener:
        server = uvicorn.Server(
            uvicorn.Config(
                app,
                fd=listener.fileno(),
                access_log=False,
                log_level="error",
            )
        )
        worker = threading.Thread(target=server.run, daemon=True)
        worker.start()
        deadline = time.monotonic() + 5
        while not server.started and time.monotonic() < deadline:
            time.sleep(0.01)
        assert server.started
        assert stat.S_IMODE(config.socket_path.stat().st_mode) == CONSOLE_SOCKET_MODE
        try:
            transport = httpx.HTTPTransport(uds=str(config.socket_path))
            with httpx.Client(transport=transport, base_url=f"http://{HOST}") as client:
                response = client.get("/", headers=_identity_headers())
            assert response.status_code == 200
            assert SESSION_COOKIE_NAME in response.headers["set-cookie"]
        finally:
            server.should_exit = True
            worker.join(5)
        assert not worker.is_alive()
    assert not socket_path.exists()
    runtime_path.rmdir()


def test_route_inventory_is_public_only_for_health(
    tmp_path: Path,
    fake_backend: FakeBackend,
    clock: MutableClock,
) -> None:
    app, config, _store, _sessions = _build_app(tmp_path, fake_backend, clock)

    with _client(app, config) as client:
        for public_path in ("/healthz", "/livez", "/readyz"):
            assert client.get(public_path).status_code == 200
        for protected_path in (
            "/",
            "/hypotheses/new",
            "/predictions",
            "/predictions/pred_console_01",
            "/status",
            "/assets/console.css",
            "/assets/console.js",
            "/workflows/00000000-0000-4000-8000-000000000001/review",
            "/workflows/00000000-0000-4000-8000-000000000001/status",
            "/workflows/00000000-0000-4000-8000-000000000001/receipt",
            "/unknown",
        ):
            assert client.get(protected_path).status_code == 403
        for protected_path in (
            "/session/logout",
            "/workflows",
            "/workflows/00000000-0000-4000-8000-000000000001/confirm",
            "/workflows/00000000-0000-4000-8000-000000000001/retry",
            "/workflows/00000000-0000-4000-8000-000000000001/cancel",
        ):
            assert client.post(protected_path).status_code == 403


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
        assert 'href="/predictions"' in response.text
        assert "(not yet available)" not in response.text
        assert ".visually-hidden" in asset.text

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


def test_cross_site_safe_request_cannot_establish_or_consume_a_session(
    tmp_path: Path,
    fake_backend: FakeBackend,
    clock: MutableClock,
) -> None:
    app, config, store, _sessions = _build_app(tmp_path, fake_backend, clock)
    embedded_headers = {
        **_identity_headers(),
        "Sec-Fetch-Site": "cross-site",
        "Sec-Fetch-Mode": "no-cors",
        "Sec-Fetch-Dest": "image",
    }

    with _client(app, config) as client:
        for _attempt in range(config.session_attempt_limit + 1):
            response = client.get("/", headers=embedded_headers)
            assert response.status_code == 403
            assert "set-cookie" not in response.headers

        direct_navigation = client.get("/", headers=_identity_headers())

    assert direct_navigation.status_code == 200
    connection = store.connect()
    try:
        assert connection.execute("SELECT COUNT(*) FROM console_sessions").fetchone()[0] == 1
    finally:
        connection.close()


def test_session_establishment_limit_is_enforced_at_http_boundary(
    tmp_path: Path,
    fake_backend: FakeBackend,
    clock: MutableClock,
) -> None:
    app, config, _store, _sessions = _build_app(tmp_path, fake_backend, clock)

    with _client(app, config) as client:
        for _attempt in range(config.session_attempt_limit):
            client.cookies.clear()
            assert client.get("/", headers=_identity_headers()).status_code == 200
        client.cookies.clear()
        limited = client.get("/", headers=_identity_headers())

    assert limited.status_code == 429
    assert int(limited.headers["retry-after"]) >= 1


def test_session_database_waits_are_bounded_and_do_not_block_liveness(
    tmp_path: Path,
    fake_backend: FakeBackend,
    clock: MutableClock,
    monkeypatch,
) -> None:
    app, config, _store, sessions = _build_app(tmp_path, fake_backend, clock)
    at_capacity = threading.Event()
    release = threading.Event()
    counter_lock = threading.Lock()
    started = 0
    original_lookup = sessions.lookup

    def blocking_lookup(cookie_value, user_id):
        nonlocal started
        with counter_lock:
            started += 1
            if started == 4:
                at_capacity.set()
        assert release.wait(5)
        return original_lookup(cookie_value, user_id)

    monkeypatch.setattr(sessions, "lookup", blocking_lookup)

    async def exercise() -> tuple[list[httpx.Response], httpx.Response]:
        transport = httpx.ASGITransport(app=UnixSocketScope(app, config.socket_path))
        async with httpx.AsyncClient(
            transport=transport,
            base_url=f"https://{HOST}",
        ) as client:
            protected = [
                asyncio.create_task(client.get("/", headers=_identity_headers()))
                for _attempt in range(8)
            ]
            assert await asyncio.to_thread(at_capacity.wait, 2)
            await asyncio.sleep(0.05)
            with counter_lock:
                assert started == 4
            live = await asyncio.wait_for(client.get("/livez"), timeout=1)
            release.set()
            return await asyncio.gather(*protected), live

    try:
        protected, live = asyncio.run(exercise())
    finally:
        release.set()
    assert all(response.status_code == 200 for response in protected)
    assert live.status_code == 200


def test_failure_after_response_start_does_not_emit_a_second_start(
    tmp_path: Path,
    fake_backend: FakeBackend,
    clock: MutableClock,
) -> None:
    _app, config, _store, sessions = _build_app(tmp_path, fake_backend, clock)
    controls = ConsoleAbuseControls(
        ConsoleRateLimiter(),
        session_limit=config.session_attempt_limit,
        extraction_limit=config.extraction_attempt_limit,
        confirmation_limit=config.confirmation_attempt_limit,
        retry_limit=config.retry_attempt_limit,
        window_seconds=config.rate_limit_window_seconds,
    )

    async def started_then_failed(_scope, _receive, send) -> None:
        await send({"type": "http.response.start", "status": 200, "headers": []})
        raise RuntimeError("route failed after response start")

    middleware = ConsoleSecurityMiddleware(
        started_then_failed,
        config=config,
        sessions=sessions,
        abuse_controls=controls,
    )
    headers = {
        **_identity_headers(),
    }
    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "https",
        "path": "/",
        "raw_path": b"/",
        "query_string": b"",
        "root_path": "",
        "headers": [
            (name.lower().encode("ascii"), value.encode("ascii")) for name, value in headers.items()
        ],
        "client": None,
        "server": (str(config.socket_path), None),
        "state": {},
    }
    messages: list[dict[str, Any]] = []

    async def receive() -> dict[str, Any]:
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message: dict[str, Any]) -> None:
        messages.append(message)

    with pytest.raises(RuntimeError, match="after response start"):
        asyncio.run(middleware(scope, receive, send))  # type: ignore[arg-type]

    assert [message["type"] for message in messages] == ["http.response.start"]


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

    Path(config.backend_credential_path).chmod(0o600)
    fake_backend.ready_outcomes.append(BackendTransportError("cached token was rejected"))
    with _client(app, config, unix=False) as client:
        unauthorized = client.get("/readyz")
    assert unauthorized.status_code == 503
    assert unauthorized.json() == {"status": "unavailable"}
