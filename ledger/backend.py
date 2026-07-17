"""Lifecycle and secure runtime discovery for the loopback ledger API."""

from __future__ import annotations

import json
import logging
import os
import secrets
import socket
import stat
import threading
import uuid
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

import uvicorn
from filelock import FileLock, Timeout
from pydantic import BaseModel, ConfigDict, Field, field_validator

from ledger.api import API_VERSION, create_app
from ledger.capture import CaptureService
from ledger.errors import IntegrityError
from ledger.extraction import ExtractionService

logger = logging.getLogger(__name__)

LOOPBACK_HOST = "127.0.0.1"
TOKEN_REF = ".ledger/backend.token"
DISCOVERY_REF = ".ledger/backend.json"


class BackendDescriptor(BaseModel):
    """Non-secret connection details published for local clients."""

    model_config = ConfigDict(extra="forbid")

    api_version: Literal["v1"] = API_VERSION
    host: Literal["127.0.0.1"] = LOOPBACK_HOST
    port: int = Field(ge=1, le=65535)
    pid: int = Field(ge=1)
    instance_id: str = Field(min_length=1)
    token_ref: Literal[".ledger/backend.token"] = TOKEN_REF
    started_at: datetime

    @field_validator("started_at")
    @classmethod
    def started_at_is_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("started_at must be timezone-aware")
        return value


class BackendRuntimeFiles:
    """Own token creation and atomic publication of one backend descriptor."""

    def __init__(self, vault_root: str | Path) -> None:
        self.vault_root = Path(vault_root).resolve()
        self.ledger_dir = self.vault_root / ".ledger"
        self.ledger_dir.mkdir(parents=True, exist_ok=True)
        self.token_path = self.vault_root / TOKEN_REF
        self.discovery_path = self.vault_root / DISCOVERY_REF

    def get_or_create_token(self) -> str:
        """Return a persistent user-only bearer token, creating it without races."""

        token = secrets.token_urlsafe(48)
        try:
            descriptor = os.open(
                self.token_path,
                os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                0o600,
            )
        except FileExistsError:
            return self._read_existing_token()
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                handle.write(token)
                handle.flush()
                os.fsync(handle.fileno())
            os.chmod(self.token_path, 0o600)
            _fsync_directory(self.ledger_dir)
        except BaseException:
            self.token_path.unlink(missing_ok=True)
            raise
        return token

    def publish(self, descriptor: BackendDescriptor) -> None:
        """Atomically publish non-secret discovery metadata with user-only mode."""

        temporary = self.ledger_dir / f".backend-{descriptor.instance_id}.tmp"
        content = json.dumps(
            descriptor.model_dump(mode="json"),
            allow_nan=False,
            ensure_ascii=False,
            sort_keys=True,
        )
        file_descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        try:
            with os.fdopen(file_descriptor, "w", encoding="utf-8") as handle:
                handle.write(f"{content}\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, self.discovery_path)
            os.chmod(self.discovery_path, 0o600)
            _fsync_directory(self.ledger_dir)
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise

    def clear(self, instance_id: str) -> None:
        """Remove only the discovery file still owned by this backend instance."""

        try:
            current = BackendDescriptor.model_validate_json(
                self.discovery_path.read_text(encoding="utf-8")
            )
        except (FileNotFoundError, OSError, ValueError):
            return
        if current.instance_id != instance_id:
            return
        self.discovery_path.unlink(missing_ok=True)
        _fsync_directory(self.ledger_dir)

    def _read_existing_token(self) -> str:
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open(self.token_path, flags)
        except OSError as exc:
            raise IntegrityError("backend token must be a readable regular file") from exc
        try:
            metadata = os.fstat(descriptor)
            if not stat.S_ISREG(metadata.st_mode):
                raise IntegrityError("backend token must be a regular file")
            if stat.S_IMODE(metadata.st_mode) != 0o600:
                raise IntegrityError("backend token permissions must be 0600")
            with os.fdopen(descriptor, "r", encoding="utf-8") as handle:
                descriptor = -1
                token = handle.read()
        finally:
            if descriptor >= 0:
                os.close(descriptor)
        if len(token) < 32 or token.strip() != token:
            raise IntegrityError("backend token is malformed")
        return token


class BackendServer:
    """Run the authenticated ledger API on a pre-bound loopback socket."""

    def __init__(
        self,
        vault_root: str | Path,
        *,
        extraction_service: ExtractionService,
        capture_service: CaptureService,
        port: int = 0,
        clock: Callable[[], datetime] | None = None,
        log_level: str = "warning",
    ) -> None:
        self.vault_root = Path(vault_root).resolve()
        if extraction_service.vault_root != self.vault_root:
            raise ValueError("extraction service and backend must use the same vault root")
        if capture_service.writer.vault_root != self.vault_root:
            raise ValueError("capture service and backend must use the same vault root")
        if not 0 <= port <= 65535:
            raise ValueError("port must be between 0 and 65535")
        self.extraction_service = extraction_service
        self.capture_service = capture_service
        self.port = port
        self.clock = clock or (lambda: datetime.now(UTC))
        self.log_level = log_level
        self.runtime_files = BackendRuntimeFiles(self.vault_root)
        self._process_lock = FileLock(str(self.ledger_dir / "backend.lock"), timeout=0)
        self._state_lock = threading.Lock()
        self._server: uvicorn.Server | None = None

    @property
    def ledger_dir(self) -> Path:
        """Return the managed runtime directory for this backend."""

        return self.runtime_files.ledger_dir

    def run(self) -> None:
        """Serve until stopped, publishing discovery only for this process lifetime."""

        try:
            self._process_lock.acquire()
        except Timeout as exc:
            raise IntegrityError("another backend already owns this vault") from exc
        try:
            self._run_locked()
        finally:
            self._process_lock.release()

    def _run_locked(self) -> None:
        """Serve while the per-vault backend process lock is held."""

        with self._state_lock:
            if self._server is not None:
                raise RuntimeError("backend server is already running")
            token = self.runtime_files.get_or_create_token()
            app = create_app(
                extraction_service=self.extraction_service,
                capture_service=self.capture_service,
                token=token,
            )
            configuration = uvicorn.Config(
                app,
                host=LOOPBACK_HOST,
                port=self.port,
                log_level=self.log_level,
                access_log=False,
                proxy_headers=False,
                server_header=False,
            )
            server = uvicorn.Server(configuration)
            self._server = server

        listening_socket: socket.socket | None = None
        instance_id = uuid.uuid4().hex
        try:
            listening_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            listening_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listening_socket.bind((LOOPBACK_HOST, self.port))
            listening_socket.listen(configuration.backlog)
            actual_port = int(listening_socket.getsockname()[1])
            started_at = self.clock()
            descriptor = BackendDescriptor(
                port=actual_port,
                pid=os.getpid(),
                instance_id=instance_id,
                started_at=started_at,
            )
            self.runtime_files.publish(descriptor)
            logger.info(
                "local_backend_started",
                extra={"host": LOOPBACK_HOST, "port": actual_port, "pid": descriptor.pid},
            )
            server.run(sockets=[listening_socket])
        finally:
            self.runtime_files.clear(instance_id)
            if listening_socket is not None:
                listening_socket.close()
            with self._state_lock:
                self._server = None
            logger.info("local_backend_stopped", extra={"instance_id": instance_id})

    def stop(self) -> None:
        """Request graceful shutdown; safe before start and after exit."""

        with self._state_lock:
            if self._server is not None:
                self._server.should_exit = True


def _fsync_directory(path: Path) -> None:
    try:
        descriptor = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
