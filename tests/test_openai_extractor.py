from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

import httpx
import pytest
from openai import AsyncOpenAI

from ledger.extraction import ExtractionService, ExtractionStatus
from ledger.openai_extractor import (
    OpenAIExtractionError,
    OpenAIExtractorConfig,
    OpenAIHypothesisExtractor,
)

SCHEMA_ID = "finance/strategy-edge:1"
NOTE = "Complete strategy hypothesis with private marker NOTE_SECRET_9f31."
API_KEY = "sk-test-SECRET_2b07"


def _ready_output(forecast: Mapping[str, object]) -> dict[str, object]:
    return {
        "status": "ready",
        "hypothesis": {
            "forecast": forecast,
            "decision": "Run the frozen specification against the untouched OOS window.",
            "family_id": "fam_openai_extraction",
        },
        "unable_reason": None,
    }


def _response(
    output: Mapping[str, object] | str | None,
    *,
    status: str = "completed",
    refusal: str | None = None,
) -> httpx.Response:
    if refusal is not None:
        content: list[dict[str, object]] = [{"type": "refusal", "refusal": refusal}]
    elif output is None:
        content = []
    else:
        text = output if isinstance(output, str) else json.dumps(output)
        content = [{"type": "output_text", "annotations": [], "text": text}]
    response_output: list[dict[str, object]] = []
    if content:
        response_output.append(
            {
                "id": "msg_test",
                "type": "message",
                "role": "assistant",
                "status": "completed",
                "content": content,
            }
        )
    return httpx.Response(
        200,
        json={
            "id": "resp_test",
            "object": "response",
            "created_at": 1_784_331_600,
            "status": status,
            "error": None,
            "incomplete_details": (
                {"reason": "max_output_tokens"} if status == "incomplete" else None
            ),
            "instructions": None,
            "max_output_tokens": 4_096,
            "model": "gpt-5.6-2026-07-16",
            "output": response_output,
            "parallel_tool_calls": True,
            "previous_response_id": None,
            "reasoning": {"effort": "low", "summary": None},
            "store": False,
            "temperature": 1.0,
            "text": {"format": {"type": "text"}},
            "tool_choice": "auto",
            "tools": [],
            "top_p": 1.0,
            "truncation": "disabled",
            "usage": None,
        },
    )


def _schema() -> dict[str, Any]:
    path = Path(__file__).parents[1] / ".ledger" / "schemas" / "finance" / "strategy-edge.1.json"
    value = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(value, dict)
    return value


async def _extract_with_handler(
    handler: Callable[[httpx.Request], Any],
    *,
    config: OpenAIExtractorConfig | None = None,
) -> Mapping[str, Any] | None:
    http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    client = AsyncOpenAI(
        api_key=API_KEY,
        base_url="https://openai.invalid/v1",
        http_client=http_client,
        max_retries=(config.max_retries if config is not None else 0),
    )
    try:
        extractor = OpenAIHypothesisExtractor(config, client=client)
        return await extractor.extract(NOTE, schema_id=SCHEMA_ID, schema=_schema())
    finally:
        await client.close()


def test_openai_extractor_uses_strict_private_request_and_stamps_provenance(
    valid_forecast: dict[str, object],
    caplog: pytest.LogCaptureFixture,
) -> None:
    requests: list[dict[str, Any]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["authorization"] == f"Bearer {API_KEY}"
        payload = json.loads(request.content)
        assert isinstance(payload, dict)
        requests.append(payload)
        return _response(_ready_output(valid_forecast))

    caplog.set_level(logging.INFO)
    result = asyncio.run(_extract_with_handler(handler))

    assert result is not None
    assert result["forecast"] == valid_forecast
    lineage = result["lineage"]
    assert isinstance(lineage, dict)
    assert lineage == {
        "family_id": "fam_openai_extraction",
        "extraction": {
            "provider": "openai",
            "configured_model": "gpt-5.6",
            "response_model": "gpt-5.6-2026-07-16",
            "prompt_version": "finance-strategy-edge-extraction:v1",
            "schema_id": SCHEMA_ID,
        },
    }
    assert len(requests) == 1
    payload = requests[0]
    assert payload["model"] == "gpt-5.6"
    assert payload["input"] == NOTE
    assert payload["store"] is False
    assert payload["truncation"] == "disabled"
    assert payload["text"]["format"]["type"] == "json_schema"
    assert "Authoritative forecast JSON Schema:" in payload["instructions"]
    assert API_KEY not in caplog.text
    assert NOTE not in caplog.text


@pytest.mark.parametrize(
    "response",
    [
        _response(
            {
                "status": "unable",
                "hypothesis": None,
                "unable_reason": "The out-of-sample window is missing.",
            }
        ),
        _response(None, status="incomplete"),
        _response(None, refusal="I cannot process this request."),
        _response('{"status":"ready","hypothesis":null,"unable_reason":null}'),
    ],
    ids=["explicit-unable", "truncated", "refusal", "schema-invalid"],
)
def test_openai_extractor_maps_non_ready_output_to_clean_unable(response: httpx.Response) -> None:
    result = asyncio.run(_extract_with_handler(lambda _: response))

    assert result is None


def test_openai_extractor_retries_transient_failure_only(
    valid_forecast: dict[str, object],
) -> None:
    calls = 0

    def handler(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(
                500,
                headers={"retry-after-ms": "0"},
                json={"error": {"message": "temporary", "type": "server_error"}},
            )
        return _response(_ready_output(valid_forecast))

    config = OpenAIExtractorConfig(max_retries=1)
    result = asyncio.run(_extract_with_handler(handler, config=config))

    assert result is not None
    assert calls == 2


def test_openai_extractor_does_not_retry_non_transient_failure() -> None:
    calls = 0

    def handler(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(
            400,
            json={"error": {"message": "bad request", "type": "invalid_request_error"}},
        )

    with pytest.raises(OpenAIExtractionError, match="hypothesis extraction failed"):
        asyncio.run(_extract_with_handler(handler, config=OpenAIExtractorConfig(max_retries=3)))

    assert calls == 1


def test_openai_provider_failure_is_sanitized_before_service_logging(
    vault: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    async def run() -> None:
        def handler(_: httpx.Request) -> httpx.Response:
            return httpx.Response(
                400,
                json={
                    "error": {
                        "message": f"upstream echoed {NOTE} and {API_KEY}",
                        "type": "invalid_request_error",
                    }
                },
            )

        http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        client = AsyncOpenAI(
            api_key=API_KEY,
            base_url="https://openai.invalid/v1",
            http_client=http_client,
            max_retries=0,
        )
        try:
            service = ExtractionService(vault, OpenAIHypothesisExtractor(client=client))
            result = await service.propose({"text": NOTE})
            assert result.status is ExtractionStatus.UNABLE
            assert result.errors == ("extraction provider failed",)
        finally:
            await client.close()

    caplog.set_level(logging.INFO)
    asyncio.run(run())

    assert NOTE not in caplog.text
    assert API_KEY not in caplog.text


def test_openai_extractor_is_cancellable() -> None:
    async def run() -> None:
        started = asyncio.Event()
        never = asyncio.Event()

        async def handler(_: httpx.Request) -> httpx.Response:
            started.set()
            await never.wait()
            raise AssertionError("unreachable")

        http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        client = AsyncOpenAI(
            api_key=API_KEY,
            base_url="https://openai.invalid/v1",
            http_client=http_client,
            max_retries=0,
        )
        try:
            extractor = OpenAIHypothesisExtractor(client=client)
            task = asyncio.create_task(
                extractor.extract(NOTE, schema_id=SCHEMA_ID, schema=_schema())
            )
            await started.wait()
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        finally:
            await client.close()

    asyncio.run(run())


def test_openai_extractor_rejects_unsupported_schema_before_network() -> None:
    async def run() -> None:
        def handler(_: httpx.Request) -> httpx.Response:
            raise AssertionError("unsupported schemas must not reach the provider")

        http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        client = AsyncOpenAI(
            api_key=API_KEY,
            base_url="https://openai.invalid/v1",
            http_client=http_client,
            max_retries=0,
        )
        try:
            extractor = OpenAIHypothesisExtractor(client=client)
            result = await extractor.extract(
                NOTE,
                schema_id="finance/future:1",
                schema={},
            )
            assert result is None
        finally:
            await client.close()

    asyncio.run(run())


def test_openai_extractor_config_loads_only_non_secret_environment() -> None:
    config = OpenAIExtractorConfig.from_env(
        {
            "OPENAI_API_KEY": "must-not-enter-config",
            "PINE_OPENAI_MODEL": "gpt-test",
            "PINE_OPENAI_PROMPT_VERSION": "prompt:v9",
            "PINE_OPENAI_TIMEOUT_SECONDS": "12.5",
            "PINE_OPENAI_MAX_RETRIES": "1",
            "PINE_OPENAI_MAX_OUTPUT_TOKENS": "2048",
        }
    )

    assert config.model == "gpt-test"
    assert config.prompt_version == "prompt:v9"
    assert config.timeout_seconds == 12.5
    assert config.max_retries == 1
    assert config.max_output_tokens == 2_048
    assert "api_key" not in config.model_dump()


def test_openai_extraction_service_returns_valid_proposal_without_side_effects(
    vault: Path,
    valid_forecast: dict[str, object],
) -> None:
    async def run() -> None:
        async def handler(_: httpx.Request) -> httpx.Response:
            return _response(_ready_output(valid_forecast))

        http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        client = AsyncOpenAI(
            api_key=API_KEY,
            base_url="https://openai.invalid/v1",
            http_client=http_client,
            max_retries=0,
        )
        try:
            service = ExtractionService(vault, OpenAIHypothesisExtractor(client=client))
            result = await service.propose({"text": NOTE})
            assert result.status is ExtractionStatus.READY
            assert result.proposal is not None
            assert result.proposal.lineage["family_id"] == "fam_openai_extraction"
            assert result.proposal.lineage["extraction"] == {
                "provider": "openai",
                "configured_model": "gpt-5.6",
                "response_model": "gpt-5.6-2026-07-16",
                "prompt_version": "finance-strategy-edge-extraction:v1",
                "schema_id": SCHEMA_ID,
            }
            with service.registry.connect() as connection:
                assert connection.execute("SELECT COUNT(*) FROM predictions").fetchone()[0] == 0
            snapshots = vault / ".ledger" / "snapshots"
            assert not snapshots.exists() or list(snapshots.iterdir()) == []
        finally:
            await client.close()

    asyncio.run(run())
