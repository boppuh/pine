from __future__ import annotations

import json
import logging
import os
import socket
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit
from zipfile import ZipFile

import pytest
import uvicorn
from playwright.sync_api import (
    BrowserContext,
    BrowserType,
    Page,
    Playwright,
    expect,
    sync_playwright,
)

from ledger.console.app import create_console_app
from ledger.console.config import ConsoleConfig
from ledger.console.errors import BackendTransportError
from ledger.console.sessions import ConsoleSessionStore
from ledger.console.state import ConsoleStateStore

from .conftest import FakeBackend, MutableClock
from .test_http_security import HOST, IDENTITY, TOKEN

pytestmark = pytest.mark.browser


@dataclass(frozen=True, slots=True)
class _BrowserProbe:
    page: Page
    context: BrowserContext
    errors: list[str]
    requests: list[str]
    trace_path: Path


@dataclass(frozen=True, slots=True)
class _BrowserServer:
    base_url: str
    backend: FakeBackend
    credential_path: Path


@contextmanager
def _browser_probe(
    playwright_runtime: Playwright,
    *,
    base_url: str,
    tmp_path: Path,
    name: str,
    engine: str = "chromium",
    color_scheme: str = "light",
    evidence_root: Path | None = None,
    har_path: Path | None = None,
    after_context: Callable[[_BrowserProbe], None] | None = None,
    trace_sources: bool = True,
) -> Iterator[_BrowserProbe]:
    """Run an instrumented context and retain a trace on in- or post-context failure."""

    browser_type: BrowserType = getattr(playwright_runtime, engine)
    browser = browser_type.launch()
    trace_root = evidence_root or Path(
        os.environ.get("PINE_BROWSER_EVIDENCE_DIR", str(tmp_path / "browser-evidence"))
    )
    trace_path = trace_root / f"{name}-{engine}.zip"
    trace_path.unlink(missing_ok=True)
    context_options: dict[str, Any] = {
        "base_url": base_url,
        "viewport": {"width": 1280, "height": 900},
        "color_scheme": color_scheme,
    }
    if har_path is not None:
        har_path.unlink(missing_ok=True)
        context_options.update(
            {
                "record_har_path": str(har_path),
                "record_har_content": "embed",
                "record_har_mode": "full",
            }
        )
    context = browser.new_context(**context_options)
    context.tracing.start(screenshots=True, snapshots=True, sources=trace_sources)
    page = context.new_page()
    errors: list[str] = []
    requests: list[str] = []
    page.on("pageerror", lambda error: errors.append(str(error)))
    page.on(
        "console",
        lambda message: errors.append(message.text) if message.type == "error" else None,
    )
    page.on("request", lambda request: requests.append(request.url))
    page.add_init_script(
        """
        window.__pineCspViolations = [];
        document.addEventListener("securitypolicyviolation", (event) => {
          window.__pineCspViolations.push({
            blockedURI: event.blockedURI,
            directive: event.effectiveDirective,
          });
        });
        """
    )
    context_closed = False
    try:
        try:
            probe = _BrowserProbe(
                page=page,
                context=context,
                errors=errors,
                requests=requests,
                trace_path=trace_path,
            )
            yield probe
        except BaseException:
            trace_path.parent.mkdir(parents=True, exist_ok=True)
            try:
                context.tracing.stop(path=trace_path)
            except Exception:
                pass
            raise
        else:
            if after_context is None:
                context.tracing.stop()
            else:
                trace_path.parent.mkdir(parents=True, exist_ok=True)
                context.tracing.stop(path=trace_path)
                try:
                    context.close()
                finally:
                    context_closed = True
                after_context(probe)
                trace_path.unlink()
    finally:
        if not context_closed:
            context.close()
        browser.close()


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
) -> Iterator[_BrowserServer]:
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
        yield _BrowserServer(
            base_url=f"http://localhost:{port}",
            backend=fake_backend,
            credential_path=credential,
        )
    finally:
        server.should_exit = True
        worker.join(5)
        listener.close()
        assert not worker.is_alive()


@pytest.mark.parametrize("engine", ["chromium", "webkit"])
def test_keyboard_capture_flow_in_chromium_and_webkit(
    playwright_runtime: Playwright,
    browser_server: _BrowserServer,
    tmp_path: Path,
    engine: str,
) -> None:
    base_url = browser_server.base_url
    backend = browser_server.backend
    with _browser_probe(
        playwright_runtime,
        base_url=base_url,
        tmp_path=tmp_path,
        name="capture",
        engine=engine,
    ) as probe:
        page = probe.page
        page.goto("/hypotheses/new")
        expect(page.get_by_role("heading", name="New strategy hypothesis")).to_be_visible()
        backend.proposal.body = "Keyboard-only operational hypothesis."
        page.get_by_label("Hypothesis source").focus()
        page.keyboard.type("   ")
        page.get_by_role("button", name="Extract proposal").click()
        expect(page.get_by_role("heading", name="Check the source text")).to_be_visible()
        expect(page.locator("[data-error-summary]")).to_be_focused()
        assert backend.draft_requests == []
        assert probe.errors and all("422" in message for message in probe.errors)
        probe.errors.clear()
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
        page.get_by_label("Research family ID").fill("fam_console")
        expect(page.get_by_text("Window was fresh at extraction", exact=True)).to_be_visible()
        expect(
            page.get_by_text(
                "No touched family window overlapped at the advisory check.",
                exact=True,
            )
        ).to_be_visible()
        page.get_by_role("button", name="Confirm preregistration").focus()
        page.keyboard.press("Enter")
        page.wait_for_url("**/receipt")
        expect(page.get_by_role("heading", name="Preregistration committed")).to_be_visible()
        expect(page.get_by_text(backend.response.prediction_id, exact=True)).to_be_visible()
        assert len(backend.capture_requests) == 1
        assert probe.errors == []
    assert not probe.trace_path.exists()


def test_capture_layout_has_no_horizontal_overflow_at_supported_widths(
    playwright_runtime: Playwright,
    browser_server: _BrowserServer,
) -> None:
    base_url = browser_server.base_url
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


@pytest.mark.parametrize("engine", ["chromium", "webkit"])
def test_verified_inspection_flow_in_chromium_and_webkit(
    playwright_runtime: Playwright,
    browser_server: _BrowserServer,
    tmp_path: Path,
    engine: str,
) -> None:
    base_url = browser_server.base_url
    backend = browser_server.backend
    strategy_id = backend.prediction_detail.forecast.strategy_id
    with _browser_probe(
        playwright_runtime,
        base_url=base_url,
        tmp_path=tmp_path,
        name="inspection",
        engine=engine,
    ) as probe:
        page = probe.page
        page.goto("/")
        expect(
            page.get_by_role("heading", name="Decision evidence you can inspect")
        ).to_be_visible()
        page.get_by_role("link", name="Predictions").first.click()
        expect(page.get_by_role("heading", name="Predictions", exact=True)).to_be_visible()
        page.get_by_label("Strategy ID").fill(strategy_id)
        page.get_by_role("button", name="Apply filters").click()
        expect(page.get_by_role("link", name=strategy_id)).to_be_visible()
        assert backend.prediction_list_requests[-1]["strategy_id"] == strategy_id
        page.get_by_role("link", name=strategy_id).click()
        expect(page.get_by_role("heading", name=strategy_id, exact=True)).to_be_visible()
        expect(page.get_by_text("Evidence verified", exact=True)).to_be_visible()
        expect(page.get_by_text("No result evidence attached", exact=True)).to_be_visible()
        assert backend.capture_requests == []
        assert probe.errors == []
    assert not probe.trace_path.exists()


def test_inspection_layout_has_no_horizontal_overflow_at_supported_widths(
    playwright_runtime: Playwright,
    browser_server: _BrowserServer,
) -> None:
    base_url = browser_server.base_url
    backend = browser_server.backend
    browser = playwright_runtime.chromium.launch()
    paths = (
        "/",
        "/predictions",
        f"/predictions/{backend.prediction_detail.prediction_id}",
        "/status",
    )
    try:
        for width, height in ((390, 844), (768, 1024), (1440, 1000)):
            context = browser.new_context(
                base_url=base_url,
                viewport={"width": width, "height": height},
            )
            page: Page = context.new_page()
            for path in paths:
                page.goto(path)
                overflow = page.evaluate(
                    "document.documentElement.scrollWidth > document.documentElement.clientWidth"
                )
                assert overflow is False, f"horizontal overflow for {path} at {width}x{height}"
            context.close()
    finally:
        browser.close()


def test_inspection_dark_mode_uses_a_distinct_background(
    playwright_runtime: Playwright,
    browser_server: _BrowserServer,
    tmp_path: Path,
) -> None:
    base_url = browser_server.base_url
    backend = browser_server.backend
    backgrounds: dict[str, str] = {}
    for color_scheme in ("light", "dark"):
        with _browser_probe(
            playwright_runtime,
            base_url=base_url,
            tmp_path=tmp_path,
            name=f"inspection-theme-{color_scheme}",
            color_scheme=color_scheme,
        ) as probe:
            probe.page.goto(f"/predictions/{backend.prediction_detail.prediction_id}")
            backgrounds[color_scheme] = probe.page.evaluate(
                "getComputedStyle(document.body).backgroundColor"
            )
        assert not probe.trace_path.exists()

    assert backgrounds["dark"] != backgrounds["light"]


def _dom_accessibility_issues(page: Page) -> list[str]:
    issues = page.evaluate(
        """
        () => {
          const issues = [];
          const main = document.querySelectorAll("main");
          if (main.length !== 1) issues.push(`expected one main landmark, found ${main.length}`);
          const headings = document.querySelectorAll("main h1");
          if (headings.length !== 1) issues.push(`expected one main h1, found ${headings.length}`);
          for (const landmark of document.querySelectorAll("nav")) {
            if (!landmark.getAttribute("aria-label") && !landmark.getAttribute("aria-labelledby")) {
              issues.push("navigation landmark lacks an accessible name");
            }
          }
          const ids = new Set();
          for (const element of document.querySelectorAll("[id]")) {
            if (ids.has(element.id)) issues.push(`duplicate id: ${element.id}`);
            ids.add(element.id);
          }
          for (const control of document.querySelectorAll(
            'input:not([type="hidden"]), select, textarea'
          )) {
            const named = control.labels?.length > 0 ||
              control.hasAttribute("aria-label") || control.hasAttribute("aria-labelledby");
            if (!named) issues.push(`unlabelled control: ${control.tagName.toLowerCase()}`);
          }
          for (const control of document.querySelectorAll("a[href], button")) {
            const name = control.getAttribute("aria-label") ||
              control.getAttribute("aria-labelledby") || control.textContent?.trim() ||
              control.getAttribute("title");
            if (!name) issues.push(`unnamed interactive element: ${control.tagName.toLowerCase()}`);
          }
          for (const element of document.querySelectorAll("[tabindex]")) {
            if (element.tabIndex > 0) {
              issues.push(`positive tabindex: ${element.tagName.toLowerCase()}`);
            }
          }
          for (const region of document.querySelectorAll("pre.structured-text")) {
            if (region.tabIndex !== 0 || !region.getAttribute("aria-label")) {
              issues.push("scrollable evidence region is not keyboard named and focusable");
            }
          }
          return issues;
        }
        """
    )
    return [str(issue) for issue in issues]


def _contrast_ratio(page: Page, foreground: str, background: str) -> float:
    return float(
        page.evaluate(
            """
            ({ foreground, background }) => {
              const foregroundElement = document.querySelector(foreground);
              const backgroundElement = document.querySelector(background);
              if (!foregroundElement || !backgroundElement) {
                throw new Error("contrast target missing");
              }
              const canvas = document.createElement("canvas");
              canvas.width = 1;
              canvas.height = 1;
              const context = canvas.getContext("2d", { willReadFrequently: true });
              const sample = (color) => {
                context.clearRect(0, 0, 1, 1);
                context.fillStyle = color;
                context.fillRect(0, 0, 1, 1);
                return Array.from(context.getImageData(0, 0, 1, 1).data).slice(0, 3);
              };
              const luminance = (rgb) => {
                const channels = rgb.map((value) => {
                  const normalized = value / 255;
                  return normalized <= 0.04045
                    ? normalized / 12.92
                    : ((normalized + 0.055) / 1.055) ** 2.4;
                });
                return 0.2126 * channels[0] + 0.7152 * channels[1] + 0.0722 * channels[2];
              };
              const foregroundLuminance = luminance(
                sample(getComputedStyle(foregroundElement).color)
              );
              const backgroundLuminance = luminance(
                sample(getComputedStyle(backgroundElement).backgroundColor)
              );
              const lighter = Math.max(foregroundLuminance, backgroundLuminance);
              const darker = Math.min(foregroundLuminance, backgroundLuminance);
              return (lighter + 0.05) / (darker + 0.05);
            }
            """,
            {"foreground": foreground, "background": background},
        )
    )


def _assert_accessible_page(page: Page, label: str) -> None:
    assert _dom_accessibility_issues(page) == [], label
    assert _contrast_ratio(page, "body", "body") >= 4.5, label
    for foreground, background in (
        (".lede", "body"),
        (".freshness-notice.is-warning", ".freshness-notice.is-warning"),
        (".notice.is-warning", ".notice.is-warning"),
        (".error-summary", ".error-summary"),
    ):
        if page.locator(foreground).count():
            assert _contrast_ratio(page, foreground, background) >= 4.5, f"{label}: {foreground}"


@pytest.mark.parametrize("color_scheme", ["light", "dark"])
def test_console_pages_pass_automated_accessibility_checks(
    playwright_runtime: Playwright,
    browser_server: _BrowserServer,
    tmp_path: Path,
    color_scheme: str,
) -> None:
    base_url = browser_server.base_url
    backend = browser_server.backend
    paths = (
        "/",
        "/hypotheses/new",
        "/predictions",
        f"/predictions/{backend.prediction_detail.prediction_id}",
        "/status",
    )
    with _browser_probe(
        playwright_runtime,
        base_url=base_url,
        tmp_path=tmp_path,
        name=f"accessibility-{color_scheme}",
        color_scheme=color_scheme,
    ) as probe:
        for path in paths:
            response = probe.page.goto(path)
            assert response is not None
            assert "default-src 'none'" in response.headers["content-security-policy"]
            _assert_accessible_page(probe.page, path)
            assert probe.page.evaluate("window.__pineCspViolations") == []

        probe.page.goto("/")
        probe.page.keyboard.press("Tab")
        expect(probe.page.get_by_role("link", name="Skip to content")).to_be_focused()
        probe.page.keyboard.press("Enter")
        expect(probe.page.locator("main")).to_be_focused()

        probe.page.goto("/hypotheses/new")
        probe.page.get_by_label("Hypothesis source").fill("   ")
        probe.page.get_by_role("button", name="Extract proposal").click()
        error_summary = probe.page.locator("[data-error-summary]")
        expect(error_summary).to_be_focused()
        _assert_accessible_page(probe.page, "capture validation error")
        assert probe.errors and all("422" in message for message in probe.errors)
        probe.errors.clear()
        assert probe.page.evaluate("window.__pineCspViolations") == []

        probe.page.get_by_label("Hypothesis source").fill(backend.proposal.body)
        probe.page.get_by_role("button", name="Extract proposal").click()
        probe.page.wait_for_url("**/review")
        probe.page.get_by_label("Research family ID").fill("fam_console_accessibility_changed")
        expect(probe.page.get_by_text("Changed since advisory check", exact=False)).to_be_visible()
        _assert_accessible_page(probe.page, "capture review")

        backend.capture_outcomes.append(BackendTransportError("synthetic uncertain outcome"))
        probe.page.get_by_role("button", name="Confirm preregistration").click()
        probe.page.wait_for_url("**/status")
        _assert_accessible_page(probe.page, "workflow status")

        probe.page.get_by_role("button", name="Retry exact frozen request").click()
        probe.page.wait_for_url("**/receipt")
        _assert_accessible_page(probe.page, "capture receipt")
        assert probe.errors == []
        assert probe.page.evaluate("window.__pineCspViolations") == []
    assert not probe.trace_path.exists()


def test_browser_traffic_storage_and_assets_exclude_server_secrets(
    playwright_runtime: Playwright,
    browser_server: _BrowserServer,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    base_url = browser_server.base_url
    backend = browser_server.backend
    har_path = tmp_path / "browser-security.har"
    browser_material_parts: list[str] = []
    forbidden_values = (
        TOKEN,
        "Authorization: Bearer",
        '"name": "authorization"',
        str(browser_server.credential_path),
        "/run/credentials/pine-console.service/backend-token",
    )
    caplog.set_level(logging.INFO)

    def assert_no_server_secrets(_probe: _BrowserProbe) -> None:
        assert har_path.is_file()
        assert _probe.trace_path.is_file()
        browser_material = "\n".join(
            [
                *browser_material_parts,
                har_path.read_text(encoding="utf-8"),
                caplog.text,
            ]
        )
        for forbidden in forbidden_values:
            assert forbidden not in browser_material
        with ZipFile(_probe.trace_path) as archive:
            assert archive.testzip() is None
            for name in archive.namelist():
                material = archive.read(name)
                for forbidden in forbidden_values:
                    assert forbidden.encode("utf-8") not in material, name
        har_path.unlink()

    with _browser_probe(
        playwright_runtime,
        base_url=base_url,
        tmp_path=tmp_path,
        name="browser-security",
        har_path=har_path,
        after_context=assert_no_server_secrets,
        trace_sources=False,
    ) as probe:
        for path in (
            "/",
            "/hypotheses/new",
            "/predictions",
            f"/predictions/{backend.prediction_detail.prediction_id}",
            "/status",
        ):
            response = probe.page.goto(path)
            assert response is not None and response.ok
            browser_material_parts.append(probe.page.content())
            assert probe.page.evaluate("window.__pineCspViolations") == []

        readiness = probe.page.request.get(f"{base_url}/readyz")
        javascript = probe.page.request.get(f"{base_url}/assets/console.js")
        stylesheet = probe.page.request.get(f"{base_url}/assets/console.css")
        assert readiness.ok and javascript.ok and stylesheet.ok
        browser_material_parts.extend(
            [
                readiness.text(),
                javascript.text(),
                stylesheet.text(),
                json.dumps(probe.context.storage_state(), sort_keys=True),
            ]
        )
        assert probe.page.evaluate("localStorage.length") == 0
        assert probe.page.evaluate("sessionStorage.length") == 0
        assert probe.errors == []

        expected_origin = urlsplit(base_url)
        external_requests = {
            request_url
            for request_url in probe.requests
            if (
                urlsplit(request_url).scheme,
                urlsplit(request_url).hostname,
                urlsplit(request_url).port,
            )
            != (expected_origin.scheme, expected_origin.hostname, expected_origin.port)
        }
        assert external_requests == set()

    assert not probe.trace_path.exists()


@pytest.mark.parametrize("failure_stage", ["in-context", "post-context"])
def test_browser_trace_is_retained_for_a_failed_journey(
    playwright_runtime: Playwright,
    browser_server: _BrowserServer,
    tmp_path: Path,
    failure_stage: str,
) -> None:
    base_url = browser_server.base_url
    failure_evidence = tmp_path / "synthetic-failure-evidence"
    trace_path = failure_evidence / f"synthetic-{failure_stage}-chromium.zip"

    def fail_after_context(_probe: _BrowserProbe) -> None:
        raise RuntimeError("synthetic post-context failure")

    expected_failure = f"synthetic {failure_stage} failure"
    with pytest.raises(RuntimeError, match=expected_failure):
        with _browser_probe(
            playwright_runtime,
            base_url=base_url,
            tmp_path=tmp_path,
            name=f"synthetic-{failure_stage}",
            evidence_root=failure_evidence,
            after_context=fail_after_context if failure_stage == "post-context" else None,
        ) as probe:
            probe.page.goto("/")
            if failure_stage == "in-context":
                raise RuntimeError(expected_failure)

    assert trace_path.is_file()
    assert trace_path.stat().st_size > 0
    with ZipFile(trace_path) as archive:
        assert archive.testzip() is None
        assert all(TOKEN.encode("ascii") not in archive.read(name) for name in archive.namelist())
    trace_path.unlink()
    assert list(failure_evidence.glob("*.zip")) == []
