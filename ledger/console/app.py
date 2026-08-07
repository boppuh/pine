"""Authenticated, server-rendered Pine Research Console shell."""

from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from jinja2 import Environment, FileSystemLoader, StrictUndefined, select_autoescape
from starlette.templating import Jinja2Templates

from ledger.console.backend_client import ConsoleBackend
from ledger.console.config import ConsoleConfig
from ledger.console.rate_limit import ConsoleAbuseControls, ConsoleRateLimiter
from ledger.console.security import ConsoleSecurityMiddleware, require_identity, require_session
from ledger.console.sessions import ConsoleSessionStore
from ledger.console.state import ConsoleStateStore

_PACKAGE_ROOT = Path(__file__).resolve().parent
_TEMPLATE_ROOT = _PACKAGE_ROOT / "templates"
_STATIC_ROOT = _PACKAGE_ROOT / "static"
_REQUIRED_FILES = (
    _TEMPLATE_ROOT / "base.html",
    _TEMPLATE_ROOT / "home.html",
    _TEMPLATE_ROOT / "status.html",
    _TEMPLATE_ROOT / "signed_out.html",
    _TEMPLATE_ROOT / "plain_text.html",
    _STATIC_ROOT / "console.css",
)


def create_console_app(
    config: ConsoleConfig,
    store: ConsoleStateStore,
    backend: ConsoleBackend,
    *,
    sessions: ConsoleSessionStore | None = None,
    limiter: ConsoleRateLimiter | None = None,
) -> FastAPI:
    """Create the private shell with a locked transport and browser boundary."""

    session_store = sessions or ConsoleSessionStore(
        store,
        absolute_lifetime=config.session_absolute_lifetime,
        idle_lifetime=config.session_idle_lifetime,
    )
    rate_limiter = limiter or ConsoleRateLimiter()
    abuse_controls = ConsoleAbuseControls(
        rate_limiter,
        session_limit=config.session_attempt_limit,
        extraction_limit=config.extraction_attempt_limit,
        confirmation_limit=config.confirmation_attempt_limit,
        retry_limit=config.retry_attempt_limit,
        window_seconds=config.rate_limit_window_seconds,
    )
    session_store.cleanup_expired()
    templates = _templates()
    app = FastAPI(
        title="Pine Research Console",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )
    app.state.console_sessions = session_store
    app.state.console_limiter = rate_limiter
    app.state.console_abuse_controls = abuse_controls

    app.mount(
        "/assets",
        StaticFiles(directory=_STATIC_ROOT, check_dir=True),
        name="console_asset",
    )

    @app.get("/healthz")
    @app.get("/livez")
    def live() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/readyz")
    def ready() -> JSONResponse:
        try:
            config.read_backend_token()
            _verify_packaged_assets()
            store.check_writable()
            backend_health = backend.health()
            status = store.get_status()
        except Exception:
            return JSONResponse(status_code=503, content={"status": "unavailable"})
        return JSONResponse(
            content={
                "status": "ok",
                "backend_api_version": backend_health.api_version,
                "console_schema_version": status["schema_version"],
            }
        )

    @app.get("/", response_class=HTMLResponse)
    def home(request: Request) -> HTMLResponse:
        session = require_session(request.scope)
        require_identity(request.scope)
        return templates.TemplateResponse(
            request=request,
            name="home.html",
            context={
                "page_title": "Research Console",
                "active_page": "home",
                "csrf_token": session.csrf_token("POST", "/session/logout"),
            },
        )

    @app.get("/status", response_class=HTMLResponse)
    def console_status(request: Request) -> HTMLResponse:
        session = require_session(request.scope)
        require_identity(request.scope)
        state_status = store.get_status()
        try:
            backend_status = backend.health().status
        except Exception:
            backend_status = "unavailable"
        return templates.TemplateResponse(
            request=request,
            name="status.html",
            context={
                "page_title": "System status",
                "active_page": "status",
                "csrf_token": session.csrf_token("POST", "/session/logout"),
                "backend_status": backend_status,
                "schema_version": state_status["schema_version"],
            },
        )

    @app.post("/session/logout", response_class=HTMLResponse)
    def logout(request: Request) -> HTMLResponse:
        session = require_session(request.scope)
        require_identity(request.scope)
        if request.scope["state"].get("pine.form_field_names") != frozenset({"csrf_token"}):
            raise HTTPException(status_code=422, detail="invalid form fields")
        session_store.delete_hash(session.session_hash)
        request.scope["state"]["pine.clear_session_cookie"] = True
        return templates.TemplateResponse(
            request=request,
            name="signed_out.html",
            context={"page_title": "Session ended", "active_page": ""},
        )

    app.add_middleware(
        ConsoleSecurityMiddleware,
        config=config,
        sessions=session_store,
        abuse_controls=abuse_controls,
    )
    return app


def _templates() -> Jinja2Templates:
    environment = Environment(
        loader=FileSystemLoader(_TEMPLATE_ROOT),
        autoescape=select_autoescape(enabled_extensions=("html",), default_for_string=True),
        undefined=StrictUndefined,
        enable_async=False,
    )
    return Jinja2Templates(env=environment)


def _verify_packaged_assets() -> None:
    for path in _REQUIRED_FILES:
        if not path.is_file() or path.is_symlink() or path.stat().st_size < 1:
            raise RuntimeError("required console asset is unavailable")
