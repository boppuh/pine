"""Small in-process abuse controls for the single-instance console."""

from __future__ import annotations

import math
import threading
import time
from collections import deque
from collections.abc import Callable, Iterator
from contextlib import contextmanager

from ledger.console.errors import RateLimitExceeded


class ConsoleRateLimiter:
    """Bound sliding-window attempts and mutually exclusive operations."""

    def __init__(
        self,
        *,
        monotonic_clock: Callable[[], float] = time.monotonic,
        max_keys: int = 4096,
    ) -> None:
        if max_keys < 1:
            raise ValueError("rate limiter max_keys must be positive")
        self._clock = monotonic_clock
        self._max_keys = max_keys
        self._events: dict[tuple[str, str], deque[float]] = {}
        self._windows: dict[tuple[str, str], int] = {}
        self._in_flight: set[tuple[str, str]] = set()
        self._lock = threading.Lock()

    def hit(self, bucket: str, key: str, *, limit: int, window_seconds: int) -> None:
        """Record an allowed attempt or raise before its protected operation."""

        if not bucket or not key or limit < 1 or window_seconds < 1:
            raise ValueError("rate limit parameters are invalid")
        now = self._clock()
        item = (bucket, key)
        with self._lock:
            self._prune(now)
            events = self._events.get(item)
            if events is None:
                if len(self._events) >= self._max_keys:
                    raise RateLimitExceeded(window_seconds)
                events = deque()
                self._events[item] = events
                self._windows[item] = window_seconds
            elif self._windows[item] != window_seconds:
                raise ValueError("rate limit window cannot change for an existing bucket key")
            cutoff = now - window_seconds
            while events and events[0] <= cutoff:
                events.popleft()
            if len(events) >= limit:
                retry_after = math.ceil(window_seconds - (now - events[0]))
                raise RateLimitExceeded(retry_after)
            events.append(now)

    @contextmanager
    def one_in_flight(self, operation: str, identity_key: str) -> Iterator[None]:
        """Permit one concurrent protected operation for an opaque identity key."""

        if not operation or not identity_key:
            raise ValueError("in-flight operation parameters are invalid")
        item = (operation, identity_key)
        with self._lock:
            if item in self._in_flight:
                raise RateLimitExceeded(1)
            self._in_flight.add(item)
        try:
            yield
        finally:
            with self._lock:
                self._in_flight.discard(item)

    def _prune(self, now: float) -> None:
        empty: list[tuple[str, str]] = []
        for key, events in self._events.items():
            cutoff = now - self._windows[key]
            while events and events[0] <= cutoff:
                events.popleft()
            if not events:
                empty.append(key)
        for key in empty:
            del self._events[key]
            del self._windows[key]


class ConsoleAbuseControls:
    """Named policy gates for each browser operation introduced across console PRs."""

    def __init__(
        self,
        limiter: ConsoleRateLimiter,
        *,
        session_limit: int,
        extraction_limit: int,
        confirmation_limit: int,
        retry_limit: int,
        window_seconds: int,
    ) -> None:
        if (
            min(
                session_limit,
                extraction_limit,
                confirmation_limit,
                retry_limit,
                window_seconds,
            )
            < 1
        ):
            raise ValueError("console abuse-control policy must be positive")
        self.limiter = limiter
        self.session_limit = session_limit
        self.extraction_limit = extraction_limit
        self.confirmation_limit = confirmation_limit
        self.retry_limit = retry_limit
        self.window_seconds = window_seconds

    def session_establishment(self, identity_key: str) -> None:
        """Bound rotations and first-session creation per hashed identity."""

        self.limiter.hit(
            "session",
            identity_key,
            limit=self.session_limit,
            window_seconds=self.window_seconds,
        )

    @contextmanager
    def extraction(self, identity_key: str) -> Iterator[None]:
        """Bound attempts and permit one extraction at a time per identity."""

        self.limiter.hit(
            "extraction",
            identity_key,
            limit=self.extraction_limit,
            window_seconds=self.window_seconds,
        )
        with self.limiter.one_in_flight("extraction", identity_key):
            yield

    def confirmation(self, workflow_key: str) -> None:
        """Bound new confirmation attempts independently per opaque workflow ID."""

        self.limiter.hit(
            "confirmation",
            workflow_key,
            limit=self.confirmation_limit,
            window_seconds=self.window_seconds,
        )

    def retry(self, workflow_key: str) -> None:
        """Bound exact recovery retries separately from new confirmations."""

        self.limiter.hit(
            "retry",
            workflow_key,
            limit=self.retry_limit,
            window_seconds=self.window_seconds,
        )
