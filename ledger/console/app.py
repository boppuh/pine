"""Health-only ASGI surface used until the authenticated shell lands in PR 3."""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from ledger.console.backend_client import ConsoleBackend
from ledger.console.state import ConsoleStateStore


def create_core_app(store: ConsoleStateStore, backend: ConsoleBackend) -> FastAPI:
    """Create a non-browser health surface with no workflow mutation routes."""

    app = FastAPI(
        title="Pine Research Console Core",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )

    @app.get("/healthz")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/readyz")
    def ready() -> JSONResponse:
        try:
            backend_health = backend.health()
            status = store.get_status()
        except Exception:
            return JSONResponse(
                status_code=503,
                content={"status": "unavailable"},
            )
        return JSONResponse(
            content={
                "status": "ok",
                "backend_api_version": backend_health.api_version,
                "console_schema_version": status["schema_version"],
            }
        )

    return app
