"""Tests for ExchangeProtocol and KalshiClient. Mock HTTP via custom transport."""
from __future__ import annotations

import pytest
import httpx

from btc_kalshi.exchange.exchange_protocol import ExchangeProtocol
from btc_kalshi.exchange.kalshi_client import KalshiClient


def test_implements_protocol():
    """KalshiClient must be a concrete implementation of ExchangeProtocol."""
    client = KalshiClient(
        base_url="https://api.kalshi.com",
        api_key="test-key",
        api_secret="test-secret",
    )
    assert isinstance(client, ExchangeProtocol)


@pytest.mark.asyncio
async def test_retry_on_503():
    """Client retries on 503 with backoff/jitter."""
    call_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            return httpx.Response(503, text="Service Unavailable")
        return httpx.Response(200, json={"markets": [], "cursor": None})

    transport = httpx.MockTransport(handler)
    client = KalshiClient(
        base_url="https://api.kalshi.com",
        api_key="key",
        api_secret="secret",
        transport=transport,
    )
    result = await client.get_btc_contracts()
    assert result == []
    assert call_count == 3


@pytest.mark.asyncio
async def test_no_retry_on_400():
    """Client does not retry on 400."""
    call_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        return httpx.Response(400, json={"error": "Bad request"})

    transport = httpx.MockTransport(handler)
    client = KalshiClient(
        base_url="https://api.kalshi.com",
        api_key="key",
        api_secret="secret",
        transport=transport,
    )
    with pytest.raises(httpx.HTTPStatusError):
        await client.get_btc_contracts()
    assert call_count == 1


@pytest.mark.asyncio
async def test_systemic_failure_detection():
    """api_healthy becomes False after 5xx response."""
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="Internal Server Error")

    transport = httpx.MockTransport(handler)
    client = KalshiClient(
        base_url="https://api.kalshi.com",
        api_key="key",
        api_secret="secret",
        transport=transport,
    )
    assert client.api_healthy is True
    with pytest.raises(httpx.HTTPStatusError):
        await client.get_btc_contracts()
    assert client.api_healthy is False


@pytest.mark.asyncio
async def test_get_contracts_parses_response():
    """get_btc_contracts parses API response into list of contract dicts."""
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "markets": [
                    {
                        "ticker": "BTC-25JAN01",
                        "title": "BTC above 60000?",
                        "event_ticker": "BTC",
                        "close_time": "2025-01-01T00:00:00Z",
                    }
                ],
                "cursor": None,
            },
        )

    transport = httpx.MockTransport(handler)
    client = KalshiClient(
        base_url="https://api.kalshi.com",
        api_key="key",
        api_secret="secret",
        transport=transport,
    )
    contracts = await client.get_btc_contracts()
    assert len(contracts) == 1
    assert contracts[0]["ticker"] == "BTC-25JAN01"
    assert contracts[0]["title"] == "BTC above 60000?"
