from __future__ import annotations

import os
import socket
import threading
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
import uvicorn
from playwright.sync_api import BrowserType, Page, Playwright, expect, sync_playwright

from ledger.console.app import create_console_app
from ledger.console.config import ConsoleConfig
from ledger.console.sessions import ConsoleSessionStore
from ledger.console.state import ConsoleStateStore

from .conftest import FakeBackend, MutableClock
from .test_http_security import HOST, IDENTITY, TOKEN

pytestmark = pytest.mark.browser


class _TrustedIngress:
    """Represent the header and socket metadata supplied by Tailscale Serve."""

    def __init__(self, app: Any, socket_path: Path) -> None:
        self.app = app
        self.socket_path = socket_path

    async def __call__(self, scope, receive, send) -> None:
        async def local_http_send(message) -> None:
            if message["type"] == "http.response.start":
                message = dict(message)
                message["headers"] = [
                    (
                        name,
                        value.replace(b"__Host-pine_session", b"pine_test_session").replace(
                            b"; Secure", b""
                        )
                        if name.lower() == b"set-cookie"
                        else value,
                    )
                    for name, value in message.get("headers", [])
                ]
            await send(message)

        if scope["type"] == "http":
            scope = dict(scope)
            scope["client"] = None
            scope["server"] = (str(self.socket_path), None)
            stripped = {
                b"host",
                b"origin",
                b"tailscale-user-login",
                b"sec-fetch-site",
                b"sec-fetch-mode",
                b"sec-fetch-dest",
            }
            headers = [
                (
                    name,
                    value.replace(b"pine_test_session", b"__Host-pine_session")
                    if name.lower() == b"cookie"
                    else value,
                )
                for name, value in scope.get("headers", [])
                if name.lower() not in stripped
            ]
            headers.extend(
                [
                    (b"host", HOST.encode("ascii")),
                    (b"tailscale-user-login", IDENTITY.encode("ascii")),
                    (b"sec-fetch-site", b"same-origin"),
                    (b"sec-fetch-mode", b"navigate"),
                    (b"sec-fetch-dest", b"document"),
                ]
            )
            if str(scope.get("method", "GET")).upper() not in {"GET", "HEAD", "OPTIONS"}:
                headers.append((b"origin", f"https://{HOST}".encode("ascii")))
            scope["headers"] = headers
        await self.app(scope, receive, local_http_send)


@pytest.fixture(scope="module")
def playwright_runtime() -> Iterator[Playwright]:
    if os.environ.get("PINE_RUN_BROWSER_TESTS") != "1":
        pytest.skip("set PINE_RUN_BROWSER_TESTS=1 after installing Playwright browsers")
    with sync_playwright() as runtime:
        yield runtime


@pytest.fixture
def browser_server(
    tmp_path: Path,
    fake_backend: FakeBackend,
    clock: MutableClock,
) -> Iterator[tuple[str, FakeBackend]]:
    socket_path = tmp_path / "console.sock"
    credential = tmp_path / "backend-token"
    credential.write_text(TOKEN, encoding="ascii")
    credential.chmod(0o600)
    config = ConsoleConfig(
        socket_path=socket_path,
        state_path=tmp_path / "state" / "console.db",
        backend_credential_path=credential,
        allowed_host=HOST,
        allowed_identities=(IDENTITY,),
    )
    store = ConsoleStateStore(config.state_path, clock=clock)
    sessions = ConsoleSessionStore(
        store,
        absolute_lifetime=config.session_absolute_lifetime,
        idle_lifetime=config.session_idle_lifetime,
        clock=clock,
    )
    app = _TrustedIngress(
        create_console_app(config, store, fake_backend, sessions=sessions),
        socket_path,
    )
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen(128)
    port = listener.getsockname()[1]
    server = uvicorn.Server(
        uvicorn.Config(app, fd=listener.fileno(), access_log=False, log_level="error")
    )
    worker = threading.Thread(target=server.run, daemon=True)
    worker.start()
    deadline = time.monotonic() + 5
    while not server.started and time.monotonic() < deadline:
        time.sleep(0.01)
    assert server.started
    try:
        yield f"http://localhost:{port}", fake_backend
    finally:
        server.should_exit = True
        worker.join(5)
        listener.close()
        assert not worker.is_alive()


@pytest.mark.parametrize("engine", ["chromium", "webkit"])
def test_keyboard_capture_flow_in_chromium_and_webkit(
    playwright_runtime: Playwright,
    browser_server: tuple[str, FakeBackend],
    tmp_path: Path,
    engine: str,
) -> None:
    base_url, backend = browser_server
    browser_type: BrowserType = getattr(playwright_runtime, engine)
    browser = browser_type.launch()
    evidence_root = Path(
        os.environ.get("PINE_BROWSER_EVIDENCE_DIR", str(tmp_path / "browser-evidence"))
    )
    trace_path = evidence_root / f"capture-{engine}.zip"
    trace_path.parent.mkdir(parents=True, exist_ok=True)
    context = browser.new_context(base_url=base_url, viewport={"width": 1280, "height": 900})
    context.tracing.start(screenshots=True, snapshots=True, sources=True)
    page = context.new_page()
    browser_errors: list[str] = []
    page.on("pageerror", lambda error: browser_errors.append(str(error)))
    page.on(
        "console",
        lambda message: browser_errors.append(message.text) if message.type == "error" else None,
    )
    try:
        page.goto("/hypotheses/new")
        expect(page.get_by_role("heading", name="New strategy hypothesis")).to_be_visible()
        backend.proposal.body = "Keyboard-only operational hypothesis."
        page.get_by_label("Hypothesis source").focus()
        page.keyboard.type(backend.proposal.body)
        page.get_by_role("button", name="Extract proposal").focus()
        page.keyboard.press("Enter")
        page.wait_for_url("**/review")
        expect(page.get_by_role("heading", name="Confirm strategy hypothesis")).to_be_visible()
        assert backend.capture_requests == []

        page.get_by_label("Research family ID").focus()
        page.keyboard.press("ControlOrMeta+A")
        page.keyboard.type("fam_console_keyboard")
        expect(page.get_by_text("Changed since advisory check", exact=False)).to_be_visible()
        page.get_by_role("button", name="Confirm preregistration").focus()
        page.keyboard.press("Enter")
        page.wait_for_url("**/receipt")
        expect(page.get_by_role("heading", name="Preregistration committed")).to_be_visible()
        expect(page.get_by_text(backend.response.prediction_id, exact=True)).to_be_visible()
        assert len(backend.capture_requests) == 1
        assert browser_errors == []
    finally:
        context.tracing.stop(path=trace_path)
        context.close()
        browser.close()
    assert trace_path.is_file()
    assert trace_path.stat().st_size > 0


def test_capture_layout_has_no_horizontal_overflow_at_supported_widths(
    playwright_runtime: Playwright,
    browser_server: tuple[str, FakeBackend],
) -> None:
    base_url, _backend = browser_server
    browser = playwright_runtime.chromium.launch()
    try:
        for width, height in ((390, 844), (768, 1024), (1440, 1000)):
            context = browser.new_context(
                base_url=base_url,
                viewport={"width": width, "height": height},
            )
            page: Page = context.new_page()
            page.goto("/hypotheses/new")
            overflow = page.evaluate(
                "document.documentElement.scrollWidth > document.documentElement.clientWidth"
            )
            assert overflow is False, f"horizontal overflow at {width}x{height}"
            context.close()
    finally:
        browser.close()
