from __future__ import annotations

import asyncio
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pytest

from ledger.errors import SchemaNotFoundError
from ledger.extraction import (
    ExtractionService,
    ExtractionStatus,
    HypothesisExtractionRequest,
)
from ledger.integrity import RegistrationStatus


class FakeExtractor:
    def __init__(
        self,
        result: Mapping[str, Any] | None,
        *,
        error: Exception | None = None,
    ) -> None:
        self.result = result
        self.error = error
        self.calls: list[tuple[str, str, Mapping[str, Any]]] = []

    async def extract(
        self,
        text: str,
        *,
        schema_id: str,
        schema: Mapping[str, Any],
    ) -> Mapping[str, Any] | None:
        self.calls.append((text, schema_id, schema))
        if self.error is not None:
            raise self.error
        return self.result


class SchemaMutatingExtractor(FakeExtractor):
    async def extract(
        self,
        text: str,
        *,
        schema_id: str,
        schema: Mapping[str, Any],
    ) -> Mapping[str, Any] | None:
        assert isinstance(schema, dict)
        schema["title"] = "provider mutation"
        return await super().extract(text, schema_id=schema_id, schema=schema)


def _candidate(valid_forecast: dict[str, object]) -> dict[str, Any]:
    return {
        "forecast": valid_forecast,
        "decision": "Run the frozen strategy against the untouched OOS window.",
        "lineage": {"family_id": "fam_extraction", "parent_prediction_id": None},
    }


def test_extraction_returns_valid_side_effect_free_proposal(
    vault: Path,
    valid_forecast: dict[str, object],
) -> None:
    extractor = FakeExtractor(_candidate(valid_forecast))
    service = ExtractionService(vault, extractor)
    note = "VWAP mean reversion should retain a Sharpe above 1.5 out of sample."

    result = asyncio.run(service.propose(HypothesisExtractionRequest(text=note)))

    assert result.status is ExtractionStatus.READY
    assert result.errors == ()
    assert result.proposal is not None
    assert result.proposal.registration_status is RegistrationStatus.PREREGISTERED
    assert result.proposal.schema_id == "finance/strategy-edge:1"
    assert result.proposal.schema_hash.startswith("sha256:")
    assert result.proposal.body == note
    assert result.proposal.fresh_window is True
    assert len(extractor.calls) == 1
    _, schema_id, schema = extractor.calls[0]
    assert schema_id == result.proposal.schema_id
    assert str(schema["$id"]).endswith(result.proposal.schema_id)

    connection = service.registry.connect()
    try:
        assert connection.execute("SELECT COUNT(*) FROM predictions").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM capture_requests").fetchone()[0] == 0
    finally:
        connection.close()
    snapshots = vault / ".ledger" / "snapshots"
    assert not snapshots.exists() or list(snapshots.iterdir()) == []


def test_extraction_reports_touched_window_as_advisory(
    vault: Path,
    valid_forecast: dict[str, object],
) -> None:
    service = ExtractionService(vault, FakeExtractor(_candidate(valid_forecast)))
    service.registry.mark_window_touched(
        "fam_extraction",
        "2025-01-01",
        "2026-01-01",
    )

    result = asyncio.run(service.propose({"text": "A complete strategy hypothesis."}))

    assert result.status is ExtractionStatus.READY
    assert result.proposal is not None
    assert result.proposal.fresh_window is False


@pytest.mark.parametrize(
    ("extractor", "error_fragment"),
    [
        (FakeExtractor(None), "unable to extract"),
        (FakeExtractor({"forecast": {}}), "Field required"),
        (FakeExtractor(None, error=RuntimeError("provider offline")), "provider failed"),
    ],
)
def test_extraction_failures_are_explicit_and_never_ready(
    vault: Path,
    extractor: FakeExtractor,
    error_fragment: str,
) -> None:
    service = ExtractionService(vault, extractor)

    result = asyncio.run(service.propose({"text": "An incomplete hypothesis."}))

    assert result.status is ExtractionStatus.UNABLE
    assert result.proposal is None
    assert any(error_fragment in error for error in result.errors)


def test_unknown_extraction_schema_is_rejected_before_provider_call(vault: Path) -> None:
    extractor = FakeExtractor(None)
    service = ExtractionService(vault, extractor)

    with pytest.raises(SchemaNotFoundError):
        asyncio.run(
            service.propose(
                {
                    "text": "A complete hypothesis.",
                    "schema_id": "finance/unknown:1",
                }
            )
        )

    assert extractor.calls == []


def test_provider_cannot_choose_registration_status(
    vault: Path,
    valid_forecast: dict[str, object],
) -> None:
    candidate = _candidate(valid_forecast)
    candidate["registration_status"] = "exploratory"
    service = ExtractionService(vault, FakeExtractor(candidate))

    result = asyncio.run(service.propose({"text": "A complete strategy hypothesis."}))

    assert result.status is ExtractionStatus.UNABLE
    assert result.proposal is None
    assert any("Extra inputs are not permitted" in error for error in result.errors)


def test_provider_cannot_mutate_authoritative_schema(
    vault: Path,
    valid_forecast: dict[str, object],
) -> None:
    extractor = SchemaMutatingExtractor(_candidate(valid_forecast))
    service = ExtractionService(vault, extractor)
    authoritative = service.schema_registry.load("finance/strategy-edge:1")
    expected_hash = service.schema_registry.hash(authoritative)

    result = asyncio.run(service.propose({"text": "A complete strategy hypothesis."}))

    assert result.status is ExtractionStatus.READY
    assert result.proposal is not None
    assert result.proposal.schema_hash == expected_hash
    assert service.schema_registry.load("finance/strategy-edge:1")["title"] != "provider mutation"
