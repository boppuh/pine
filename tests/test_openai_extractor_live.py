from __future__ import annotations

import asyncio
import os
from pathlib import Path

import pytest

from ledger.extraction import ExtractionService, ExtractionStatus
from ledger.openai_extractor import OpenAIHypothesisExtractor

pytestmark = pytest.mark.live_openai


def test_real_openai_strategy_hypothesis_smoke(vault: Path) -> None:
    """Opt-in contract smoke; never runs in the normal offline suite."""

    if os.environ.get("PINE_RUN_OPENAI_SMOKE") != "1":
        pytest.skip("set PINE_RUN_OPENAI_SMOKE=1 to call the real OpenAI API")
    if not os.environ.get("OPENAI_API_KEY"):
        pytest.skip("OPENAI_API_KEY is required for the live OpenAI smoke")

    note = """
Strategy ID: vwap_mr_v3.1.
Family ID: vwap-mean-reversion-us-equities.
Expected metrics: Sharpe 1.50, win rate 0.56, max drawdown 0.12, and expectancy 0.002.
In-sample window: 2025-01-01 through 2025-12-31 inclusive.
Out-of-sample window: 2026-01-01 through 2026-03-31 inclusive.
Invalidation: invalidate if out-of-sample Sharpe is below 1.0.
Edge source: short-horizon VWAP dislocation and intraday mean reversion.
Decision: implement the frozen specification and run it once against the untouched
out-of-sample window.
""".strip()

    async def run() -> None:
        extractor = OpenAIHypothesisExtractor()
        try:
            service = ExtractionService(vault, extractor)
            result = await service.propose({"text": note})
            assert result.status is ExtractionStatus.READY, result.errors
            assert result.proposal is not None
            assert result.proposal.forecast.strategy_id == "vwap_mr_v3.1"
            assert result.proposal.lineage["family_id"] == "vwap-mean-reversion-us-equities"
            provenance = result.proposal.lineage["extraction"]
            assert isinstance(provenance, dict)
            assert provenance["provider"] == "openai"
            with service.registry.connect() as connection:
                assert connection.execute("SELECT COUNT(*) FROM predictions").fetchone()[0] == 0
            snapshots = vault / ".ledger" / "snapshots"
            assert not snapshots.exists() or list(snapshots.iterdir()) == []
        finally:
            await extractor.aclose()

    asyncio.run(run())
