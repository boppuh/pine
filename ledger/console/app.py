"""Authenticated, server-rendered Pine Research Console."""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any
from uuid import UUID

import anyio
from anyio.to_thread import run_sync
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from jinja2 import Environment, FileSystemLoader, StrictUndefined, select_autoescape
from pydantic import ValidationError
from starlette.templating import Jinja2Templates

from ledger.console.backend_client import (
    AuthoritativeReceipt,
    ConsoleBackend,
)
from ledger.console.config import ConsoleConfig
from ledger.console.errors import (
    BackendDomainError,
    BackendError,
    BackendProtocolError,
    WorkflowConflictError,
    WorkflowNotFoundError,
)
from ledger.console.forms import (
    HYPOTHESIS_FIELDS,
    HYPOTHESIS_REVISION_FIELDS,
    REVIEW_FIELDS,
    VERSION_FIELDS,
    HypothesisForm,
    ReviewForm,
)
from ledger.console.models import ConsoleWorkflow, WorkflowState
from ledger.console.rate_limit import ConsoleAbuseControls, ConsoleRateLimiter
from ledger.console.security import (
    ConsoleSecurityMiddleware,
    clear_session_cookie,
    require_form_fields,
    require_form_values,
    require_identity,
    require_session,
)
from ledger.console.sessions import ConsoleSessionStore, hash_user_identity
from ledger.console.state import ConsoleStateStore
from ledger.console.workflow import WorkflowService

_PACKAGE_ROOT = Path(__file__).resolve().parent
_TEMPLATE_ROOT = _PACKAGE_ROOT / "templates"
_STATIC_ROOT = _PACKAGE_ROOT / "static"
LOGGER = logging.getLogger("ledger.console.app")
_REQUIRED_FILES = (
    _TEMPLATE_ROOT / "base.html",
    _TEMPLATE_ROOT / "home.html",
    _TEMPLATE_ROOT / "status.html",
    _TEMPLATE_ROOT / "signed_out.html",
    _TEMPLATE_ROOT / "plain_text.html",
    _TEMPLATE_ROOT / "hypothesis_new.html",
    _TEMPLATE_ROOT / "hypothesis_review.html",
    _TEMPLATE_ROOT / "workflow_status.html",
    _TEMPLATE_ROOT / "hypothesis_receipt.html",
    _TEMPLATE_ROOT / "error.html",
    _STATIC_ROOT / "console.css",
    _STATIC_ROOT / "console.js",
)


def create_console_app(
    config: ConsoleConfig,
    store: ConsoleStateStore,
    backend: ConsoleBackend,
    *,
    sessions: ConsoleSessionStore | None = None,
    limiter: ConsoleRateLimiter | None = None,
    retention_sweep_interval_seconds: float = 300.0,
) -> FastAPI:
    """Create the private console with its capture workflow and locked boundary."""

    if retention_sweep_interval_seconds <= 0:
        raise ValueError("retention sweep interval must be positive")

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
    workflows = WorkflowService(store, backend)
    session_store.cleanup_expired()
    templates = _templates()

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        async with anyio.create_task_group() as task_group:
            task_group.start_soon(
                _retention_cleanup_loop,
                store,
                retention_sweep_interval_seconds,
            )
            yield
            task_group.cancel_scope.cancel()

    app = FastAPI(
        title="Pine Research Console",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
        lifespan=lifespan,
    )
    app.state.console_sessions = session_store
    app.state.console_limiter = rate_limiter
    app.state.console_abuse_controls = abuse_controls
    app.state.console_workflows = workflows
    app.state.console_backend = backend

    app.mount(
        "/assets",
        StaticFiles(directory=_STATIC_ROOT, check_dir=True),
        name="console_asset",
    )

    @app.exception_handler(WorkflowNotFoundError)
    def workflow_not_found(request: Request, _exc: WorkflowNotFoundError) -> HTMLResponse:
        return _error_response(
            templates,
            request,
            status_code=404,
            title="Workflow not found",
            message="This workflow is unavailable or has expired.",
        )

    @app.exception_handler(WorkflowConflictError)
    def workflow_conflict(request: Request, _exc: WorkflowConflictError) -> HTMLResponse:
        return _error_response(
            templates,
            request,
            status_code=409,
            title="Workflow changed",
            message="Reload this workflow before taking another action.",
        )

    @app.get("/healthz")
    @app.get("/livez")
    async def live() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/readyz")
    def ready() -> JSONResponse:
        try:
            config.read_backend_token()
            _verify_packaged_assets()
            store.check_writable()
            backend_health = backend.ready()
            status = store.get_status()
        except Exception:
            LOGGER.error("console_readiness_failed", exc_info=True)
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
        return templates.TemplateResponse(
            request=request,
            name="home.html",
            context=_page_context(request, page_title="Research Console", active_page="home"),
        )

    @app.get("/hypotheses/new", response_class=HTMLResponse)
    def new_hypothesis(request: Request) -> HTMLResponse:
        return _render_new(templates, request)

    @app.post("/workflows", response_class=HTMLResponse)
    def create_workflow(request: Request) -> Response:
        identity = require_identity(request.scope)
        field_names = require_form_fields(request.scope)
        expected = (
            HYPOTHESIS_REVISION_FIELDS
            if "workflow_id" in field_names or "version" in field_names
            else HYPOTHESIS_FIELDS
        )
        try:
            values = _strict_form(request, expected)
            form = HypothesisForm.model_validate(values)
        except (HTTPException, ValidationError, ValueError) as exc:
            source_text = _first_form_value(request, "source_text")
            errors = _form_errors(exc)
            workflow = _preserved_revision_workflow(
                request,
                store,
                identity,
                field_names=field_names,
            )
            return _render_new(
                templates,
                request,
                workflow=workflow,
                source_text=source_text,
                errors=errors,
                status_code=422,
            )

        identity_key = hash_user_identity(identity)
        with abuse_controls.extraction(identity_key):
            if form.workflow_id is None:
                workflow = workflows.create(
                    user_id=identity,
                    source_text=form.source_text,
                    schema_id=form.schema_id,
                )
                workflow = workflows.extract(
                    workflow.workflow_id,
                    identity,
                    expected_version=workflow.version,
                )
            else:
                workflow = workflows.revise_and_extract(
                    form.workflow_id,
                    identity,
                    source_text=form.source_text,
                    schema_id=form.schema_id,
                    expected_version=form.version,
                )
        return _workflow_redirect(workflow)

    @app.get("/workflows/{workflow_id}/review", response_class=HTMLResponse)
    def review_workflow(request: Request, workflow_id: str) -> Response:
        workflow = store.get_workflow(_uuid4(workflow_id), require_identity(request.scope))
        if workflow.state is WorkflowState.COMMITTED:
            return RedirectResponse(
                f"/workflows/{workflow.workflow_id}/receipt",
                status_code=303,
            )
        if workflow.state is not WorkflowState.REVIEWING:
            return RedirectResponse(
                f"/workflows/{workflow.workflow_id}/status",
                status_code=303,
            )
        return _render_review(templates, request, workflow)

    @app.post("/workflows/{workflow_id}/confirm", response_class=HTMLResponse)
    def confirm_workflow(request: Request, workflow_id: str) -> Response:
        identity = require_identity(request.scope)
        workflow = store.get_workflow(_uuid4(workflow_id), identity)
        if workflow.state is WorkflowState.COMMITTED:
            _strict_form(request, REVIEW_FIELDS)
            return RedirectResponse(
                f"/workflows/{workflow.workflow_id}/receipt",
                status_code=303,
            )
        if workflow.state is not WorkflowState.REVIEWING or workflow.proposal is None:
            raise WorkflowConflictError("workflow cannot be confirmed")
        try:
            values = _strict_form(request, REVIEW_FIELDS)
            form = ReviewForm.model_validate(values)
            capture = form.to_capture_input(
                body=workflow.proposal.body,
                schema_hash=workflow.proposal.schema_hash,
                lineage_context=workflow.proposal.lineage,
            )
        except (HTTPException, ValidationError, ValueError) as exc:
            submitted = _submitted_review_values(request, workflow)
            return _render_review(
                templates,
                request,
                workflow,
                values=submitted,
                errors=_form_errors(exc),
                status_code=422,
            )
        abuse_controls.confirmation(workflow.workflow_id)
        result = workflows.confirm(
            workflow.workflow_id,
            identity,
            capture,
            expected_version=form.version,
        )
        return _workflow_redirect(result)

    @app.post("/workflows/{workflow_id}/retry", response_class=HTMLResponse)
    def retry_workflow(request: Request, workflow_id: str) -> RedirectResponse:
        values = _strict_form(request, VERSION_FIELDS)
        version = _version(values)
        identity = require_identity(request.scope)
        canonical_id = _uuid4(workflow_id)
        abuse_controls.retry(canonical_id)
        workflow = workflows.retry(
            canonical_id,
            identity,
            expected_version=version,
        )
        return _workflow_redirect(workflow)

    @app.post("/workflows/{workflow_id}/cancel", response_class=HTMLResponse)
    def cancel_workflow(request: Request, workflow_id: str) -> RedirectResponse:
        values = _strict_form(request, VERSION_FIELDS)
        workflows.cancel(
            _uuid4(workflow_id),
            require_identity(request.scope),
            expected_version=_version(values),
        )
        return RedirectResponse("/hypotheses/new?cancelled=1", status_code=303)

    @app.get("/workflows/{workflow_id}/status", response_class=HTMLResponse)
    def workflow_status(request: Request, workflow_id: str) -> Response:
        workflow = store.get_workflow(_uuid4(workflow_id), require_identity(request.scope))
        if workflow.state is WorkflowState.REVIEWING:
            return RedirectResponse(
                f"/workflows/{workflow.workflow_id}/review",
                status_code=303,
            )
        if workflow.state is WorkflowState.COMMITTED:
            return RedirectResponse(
                f"/workflows/{workflow.workflow_id}/receipt",
                status_code=303,
            )
        if workflow.state is WorkflowState.EDITING:
            return _render_new(
                templates,
                request,
                workflow=workflow,
                source_text=workflow.source_text or "",
                errors=_workflow_errors(workflow),
            )
        if workflow.state is WorkflowState.CANCELLED:
            return RedirectResponse("/hypotheses/new", status_code=303)
        return templates.TemplateResponse(
            request=request,
            name="workflow_status.html",
            context={
                **_page_context(
                    request,
                    page_title="Capture status",
                    active_page="capture",
                ),
                "workflow": workflow,
                "retry_allowed": workflow.state
                in {WorkflowState.UNCERTAIN, WorkflowState.RETRYABLE_FAILURE},
                "retry_csrf_token": require_session(request.scope).csrf_token(
                    "POST", f"/workflows/{workflow.workflow_id}/retry"
                ),
                "error_details": _workflow_errors(workflow),
            },
        )

    @app.get("/workflows/{workflow_id}/receipt", response_class=HTMLResponse)
    def workflow_receipt(request: Request, workflow_id: str) -> Response:
        workflow = store.get_workflow(_uuid4(workflow_id), require_identity(request.scope))
        if workflow.state is not WorkflowState.COMMITTED or workflow.capture_response is None:
            return RedirectResponse(
                f"/workflows/{workflow.workflow_id}/status",
                status_code=303,
            )
        verification = _verified_receipt(backend, workflow)
        return templates.TemplateResponse(
            request=request,
            name="hypothesis_receipt.html",
            context={
                **_page_context(
                    request,
                    page_title="Capture receipt",
                    active_page="capture",
                ),
                "workflow": workflow,
                "receipt": workflow.capture_response,
                "authority": verification.authority,
                "verification_state": verification.state.value,
            },
        )

    @app.get("/status", response_class=HTMLResponse)
    def console_status(request: Request) -> HTMLResponse:
        state_status = store.get_status()
        try:
            backend_status = backend.health().status
        except Exception:
            backend_status = "unavailable"
        return templates.TemplateResponse(
            request=request,
            name="status.html",
            context={
                **_page_context(
                    request,
                    page_title="System status",
                    active_page="status",
                ),
                "backend_status": backend_status,
                "schema_version": state_status["schema_version"],
            },
        )

    @app.post("/session/logout", response_class=HTMLResponse)
    def logout(request: Request) -> HTMLResponse:
        session = require_session(request.scope)
        require_identity(request.scope)
        _strict_form(request, frozenset({"csrf_token"}))
        session_store.delete_hash(session.session_hash)
        clear_session_cookie(request.scope)
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


def _page_context(request: Request, *, page_title: str, active_page: str) -> dict[str, Any]:
    session = require_session(request.scope)
    require_identity(request.scope)
    return {
        "page_title": page_title,
        "active_page": active_page,
        "csrf_token": session.csrf_token("POST", "/session/logout"),
    }


def _render_new(
    templates: Jinja2Templates,
    request: Request,
    *,
    workflow: ConsoleWorkflow | None = None,
    source_text: str = "",
    errors: Mapping[str, str] | None = None,
    status_code: int = 200,
) -> HTMLResponse:
    session = require_session(request.scope)
    return templates.TemplateResponse(
        request=request,
        name="hypothesis_new.html",
        status_code=status_code,
        context={
            **_page_context(
                request,
                page_title="New hypothesis",
                active_page="capture",
            ),
            "workflow": workflow,
            "source_text": source_text,
            "errors": dict(errors or {}),
            "form_csrf_token": session.csrf_token("POST", "/workflows"),
        },
    )


def _render_review(
    templates: Jinja2Templates,
    request: Request,
    workflow: ConsoleWorkflow,
    *,
    values: Mapping[str, str] | None = None,
    errors: Mapping[str, str] | None = None,
    status_code: int = 200,
) -> HTMLResponse:
    if workflow.proposal is None:
        raise WorkflowConflictError("reviewing workflow lacks a proposal")
    session = require_session(request.scope)
    review_values = dict(values or _proposal_values(workflow))
    return templates.TemplateResponse(
        request=request,
        name="hypothesis_review.html",
        status_code=status_code,
        context={
            **_page_context(
                request,
                page_title="Confirm strategy hypothesis",
                active_page="capture",
            ),
            "workflow": workflow,
            "proposal": workflow.proposal,
            "values": review_values,
            "errors": dict(errors or {}),
            "freshness_changed": _freshness_changed(workflow, review_values),
            "confirm_csrf_token": session.csrf_token(
                "POST", f"/workflows/{workflow.workflow_id}/confirm"
            ),
            "cancel_csrf_token": session.csrf_token(
                "POST", f"/workflows/{workflow.workflow_id}/cancel"
            ),
        },
    )


def _error_response(
    templates: Jinja2Templates,
    request: Request,
    *,
    status_code: int,
    title: str,
    message: str,
) -> HTMLResponse:
    return templates.TemplateResponse(
        request=request,
        name="error.html",
        status_code=status_code,
        context={
            **_page_context(request, page_title=title, active_page=""),
            "heading": title,
            "message": message,
        },
    )


def _strict_form(request: Request, expected: frozenset[str]) -> dict[str, str]:
    if require_form_fields(request.scope) != expected:
        raise HTTPException(status_code=422, detail="invalid form fields")
    raw = require_form_values(request.scope)
    if frozenset(raw) != expected or any(len(items) != 1 for items in raw.values()):
        raise HTTPException(status_code=422, detail="invalid form fields")
    return {field: items[0] for field, items in raw.items() if field != "csrf_token"}


def _first_form_value(request: Request, field: str) -> str:
    try:
        values = require_form_values(request.scope).get(field, ())
    except RuntimeError:
        return ""
    return values[0] if len(values) == 1 else ""


def _preserved_revision_workflow(
    request: Request,
    store: ConsoleStateStore,
    identity: str,
    *,
    field_names: frozenset[str],
) -> ConsoleWorkflow | None:
    """Recover only a valid, owner-bound revision after source validation fails."""

    if field_names != HYPOTHESIS_REVISION_FIELDS:
        return None
    raw = require_form_values(request.scope)
    if frozenset(raw) != HYPOTHESIS_REVISION_FIELDS or any(
        len(items) != 1 for items in raw.values()
    ):
        return None
    try:
        workflow_id = _uuid4(raw["workflow_id"][0])
        version = _version({"version": raw["version"][0]})
        workflow = store.get_workflow(workflow_id, identity)
    except (HTTPException, WorkflowNotFoundError):
        return None
    if (
        workflow.state is not WorkflowState.EDITING
        or workflow.version != version
        or workflow.schema_id != raw["schema_id"][0]
    ):
        return None
    return workflow


def _submitted_review_values(
    request: Request,
    workflow: ConsoleWorkflow,
) -> dict[str, str]:
    fallback = _proposal_values(workflow)
    try:
        raw = require_form_values(request.scope)
    except RuntimeError:
        return fallback
    for field in ReviewForm.model_fields:
        values = raw.get(field, ())
        if len(values) == 1:
            fallback[field] = values[0]
    return fallback


def _proposal_values(workflow: ConsoleWorkflow) -> dict[str, str]:
    proposal = workflow.proposal
    if proposal is None:
        raise WorkflowConflictError("reviewing workflow lacks a proposal")
    forecast = proposal.forecast
    metrics = forecast.expected_metrics
    family_id = proposal.lineage.get("family_id")
    if not isinstance(family_id, str):
        raise WorkflowConflictError("proposal family binding is invalid")
    return {
        "version": str(workflow.version),
        "schema_id": proposal.schema_id,
        "strategy_id": forecast.strategy_id,
        "sharpe": str(metrics.sharpe),
        "win_rate": str(metrics.win_rate),
        "max_drawdown": str(metrics.max_drawdown),
        "expectancy": str(metrics.expectancy),
        "in_sample_start": forecast.in_sample_window.start.isoformat(),
        "in_sample_end": forecast.in_sample_window.end.isoformat(),
        "out_of_sample_start": forecast.out_of_sample_window.start.isoformat(),
        "out_of_sample_end": forecast.out_of_sample_window.end.isoformat(),
        "invalidation": forecast.invalidation,
        "edge_source": forecast.edge_source,
        "decision": proposal.decision,
        "family_id": family_id,
    }


def _freshness_changed(workflow: ConsoleWorkflow, values: Mapping[str, str]) -> bool:
    original = _proposal_values(workflow)
    return any(
        values.get(field) != original[field]
        for field in ("family_id", "out_of_sample_start", "out_of_sample_end")
    )


def _form_errors(exc: Exception) -> dict[str, str]:
    if isinstance(exc, ValidationError):
        errors: dict[str, str] = {}
        for error in exc.errors():
            location = error.get("loc", ())
            field = str(location[0]) if location else "form"
            errors.setdefault(field, str(error.get("msg", "Invalid value")))
        return errors
    return {"form": "Check the form fields and try again."}


def _workflow_errors(workflow: ConsoleWorkflow) -> dict[str, str]:
    if workflow.error_code is None and workflow.error_details is None:
        return {}
    details = workflow.error_details or {}
    values = details.get("details", [])
    if not isinstance(values, list):
        return {"form": "The request could not be completed safely."}
    errors = {
        f"detail_{index}": value
        for index, value in enumerate(values)
        if isinstance(value, str) and value
    }
    return errors or {"form": "The request could not be completed safely."}


def _version(values: Mapping[str, str]) -> int:
    try:
        value = int(values["version"], 10)
    except (KeyError, ValueError) as exc:
        raise HTTPException(status_code=422, detail="invalid workflow version") from exc
    if value < 0:
        raise HTTPException(status_code=422, detail="invalid workflow version")
    return value


def _uuid4(value: str) -> str:
    try:
        parsed = UUID(value)
    except ValueError as exc:
        raise WorkflowNotFoundError("console workflow was not found") from exc
    if parsed.version != 4 or str(parsed) != value:
        raise WorkflowNotFoundError("console workflow was not found")
    return value


def _workflow_redirect(workflow: ConsoleWorkflow) -> RedirectResponse:
    if workflow.state is WorkflowState.REVIEWING:
        suffix = "review"
    elif workflow.state is WorkflowState.COMMITTED:
        suffix = "receipt"
    else:
        suffix = "status"
    return RedirectResponse(
        f"/workflows/{workflow.workflow_id}/{suffix}",
        status_code=303,
    )


class _ReceiptVerificationState(StrEnum):
    VERIFIED = "verified"
    UNAVAILABLE = "unavailable"
    INTEGRITY_FAILURE = "integrity_failure"


@dataclass(frozen=True, slots=True)
class _ReceiptVerification:
    state: _ReceiptVerificationState
    authority: AuthoritativeReceipt | None = None


def _verified_receipt(
    backend: ConsoleBackend,
    workflow: ConsoleWorkflow,
) -> _ReceiptVerification:
    response = workflow.capture_response
    frozen = workflow.frozen_request
    if response is None or frozen is None:
        return _ReceiptVerification(_ReceiptVerificationState.INTEGRITY_FAILURE)
    try:
        authority = backend.get_receipt(response.prediction_id)
        if (
            authority.prediction_id != response.prediction_id
            or authority.run_id != response.run_id
            or authority.schema_id != response.schema_id
            or authority.schema_hash != response.schema_hash
            or authority.immutable_hash != response.immutable_hash
            or authority.snapshot_ref != response.snapshot_ref
            or authority.forecast.model_dump(mode="json") != frozen.forecast.model_dump(mode="json")
            or authority.decision != frozen.decision
            or authority.lineage != frozen.lineage.to_dict()
        ):
            raise BackendProtocolError("authoritative receipt does not match capture response")
    except BackendProtocolError:
        LOGGER.error(
            "console_receipt_integrity_failed",
            extra={"workflow_id": workflow.workflow_id},
        )
        return _ReceiptVerification(_ReceiptVerificationState.INTEGRITY_FAILURE)
    except BackendDomainError as exc:
        if exc.code in {"integrity_error", "prediction_not_found"}:
            LOGGER.error(
                "console_receipt_integrity_failed",
                extra={"workflow_id": workflow.workflow_id},
            )
            return _ReceiptVerification(_ReceiptVerificationState.INTEGRITY_FAILURE)
        LOGGER.warning(
            "console_receipt_authority_unavailable",
            extra={"workflow_id": workflow.workflow_id},
        )
        return _ReceiptVerification(_ReceiptVerificationState.UNAVAILABLE)
    except BackendError:
        LOGGER.warning(
            "console_receipt_authority_unavailable",
            extra={"workflow_id": workflow.workflow_id},
        )
        return _ReceiptVerification(_ReceiptVerificationState.UNAVAILABLE)
    return _ReceiptVerification(_ReceiptVerificationState.VERIFIED, authority)


async def _retention_cleanup_loop(
    store: ConsoleStateStore,
    interval_seconds: float,
) -> None:
    """Continuously enforce workflow expiry for a long-running console process."""

    while True:
        try:
            removed = await run_sync(store.cleanup_expired)
        except Exception:
            LOGGER.error("console_retention_cleanup_failed", exc_info=True)
        else:
            if removed:
                LOGGER.info(
                    "console_retention_cleanup_completed",
                    extra={"removed_count": removed},
                )
        await anyio.sleep(interval_seconds)


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
