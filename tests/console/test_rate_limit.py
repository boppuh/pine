from __future__ import annotations

import pytest

from ledger.console.errors import RateLimitExceeded
from ledger.console.rate_limit import ConsoleAbuseControls, ConsoleRateLimiter


class MonotonicClock:
    def __init__(self) -> None:
        self.value = 100.0

    def __call__(self) -> float:
        return self.value


def test_sliding_window_is_scoped_and_reports_retry_after() -> None:
    clock = MonotonicClock()
    limiter = ConsoleRateLimiter(monotonic_clock=clock)
    limiter.hit("extraction", "identity-a", limit=2, window_seconds=600)
    limiter.hit("extraction", "identity-a", limit=2, window_seconds=600)

    with pytest.raises(RateLimitExceeded) as exceeded:
        limiter.hit("extraction", "identity-a", limit=2, window_seconds=600)

    assert exceeded.value.retry_after_seconds == 600
    limiter.hit("extraction", "identity-b", limit=2, window_seconds=600)
    limiter.hit("confirmation", "identity-a", limit=2, window_seconds=600)
    clock.value += 601
    limiter.hit("extraction", "identity-a", limit=2, window_seconds=600)


def test_only_one_operation_is_in_flight_per_identity() -> None:
    limiter = ConsoleRateLimiter()

    with limiter.one_in_flight("extraction", "identity-hash"):
        with pytest.raises(RateLimitExceeded):
            with limiter.one_in_flight("extraction", "identity-hash"):
                pass
        with limiter.one_in_flight("extraction", "different-identity"):
            pass

    with limiter.one_in_flight("extraction", "identity-hash"):
        pass


def test_named_controls_keep_confirmation_and_retry_budgets_separate() -> None:
    controls = ConsoleAbuseControls(
        ConsoleRateLimiter(),
        session_limit=1,
        extraction_limit=1,
        confirmation_limit=1,
        retry_limit=1,
        window_seconds=600,
    )
    controls.confirmation("workflow-id")
    controls.retry("workflow-id")

    with pytest.raises(RateLimitExceeded):
        controls.confirmation("workflow-id")
    with pytest.raises(RateLimitExceeded):
        controls.retry("workflow-id")


def test_named_extraction_control_combines_window_and_in_flight_gate() -> None:
    controls = ConsoleAbuseControls(
        ConsoleRateLimiter(),
        session_limit=2,
        extraction_limit=2,
        confirmation_limit=2,
        retry_limit=2,
        window_seconds=600,
    )

    with controls.extraction("identity-hash"):
        with pytest.raises(RateLimitExceeded):
            with controls.extraction("identity-hash"):
                pass
