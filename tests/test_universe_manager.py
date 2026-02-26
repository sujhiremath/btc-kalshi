"""Tests for UniverseManager: refresh, soft-block, near-expiry removal."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pytest

from btc_kalshi.exchange.exchange_protocol import ExchangeProtocol
from btc_kalshi.exchange.universe_manager import UniverseManager


def _dt(iso: str) -> datetime:
    return datetime.fromisoformat(iso.replace("Z", "+00:00"))


class MockExchange(ExchangeProtocol):
    """Mock ExchangeProtocol for universe tests."""

    def __init__(
        self,
        contracts: List[Dict[str, Any]],
        orderbooks: Dict[str, Dict[str, Any]],
    ) -> None:
        self._contracts = contracts
        self._orderbooks = orderbooks
        self._get_btc_contracts_called = 0

    async def get_btc_contracts(self) -> List[Dict[str, Any]]:
        self._get_btc_contracts_called += 1
        return list(self._contracts)

    async def get_contract(self, contract_id: str) -> Optional[Dict[str, Any]]:
        for c in self._contracts:
            if c.get("ticker") == contract_id or c.get("id") == contract_id:
                return c
        return None

    async def get_orderbook(self, contract_id: str) -> Dict[str, Any]:
        return dict(self._orderbooks.get(contract_id, {}))

    async def place_order(
        self,
        contract_id: str,
        side: str,
        count: int,
        price_cents: Optional[int] = None,
        type: str = "limit",
        client_order_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        return {}

    async def cancel_order(self, order_id: str) -> Dict[str, Any]:
        return {}

    async def get_order(self, order_id: str) -> Optional[Dict[str, Any]]:
        return None

    async def get_open_orders(
        self, contract_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        return []

    async def get_positions(self) -> List[Dict[str, Any]]:
        return []

    async def get_balance(self) -> Dict[str, Any]:
        return {}


@pytest.fixture
def eligible_contracts_and_books():
    """Contracts and orderbooks that pass eligibility at btc=60600, now=noon."""
    now = _dt("2025-01-01T12:00:00Z")
    btc = 60600.0
    contracts = [
        {
            "title": "BTC above $61200?",
            "ticker": "BTC-25JAN01-61200",
            "close_time": "2025-01-01T14:00:00Z",  # 120 min
            "volume": 1000,
            "open_interest": 800,
        },
        {
            "title": "BTC above $61080?",
            "ticker": "BTC-25JAN01-61080",
            "close_time": "2025-01-01T14:00:00Z",
            "volume": 1000,
            "open_interest": 800,
        },
    ]
    orderbooks = {
        "BTC-25JAN01-61200": {
            "asks": [{"price": 60, "quantity": 100}],
            "bids": [{"price": 58, "quantity": 80}],
        },
        "BTC-25JAN01-61080": {
            "asks": [{"price": 55, "quantity": 100}],
            "bids": [{"price": 53, "quantity": 80}],
        },
    }
    return contracts, orderbooks, btc, now


@pytest.mark.asyncio
async def test_refresh_filters_contracts(eligible_contracts_and_books):
    """Refresh fetches from exchange and filters to eligible canonical contracts."""
    contracts, orderbooks, btc, now = eligible_contracts_and_books
    exchange = MockExchange(contracts, orderbooks)

    def get_price():
        return btc

    manager = UniverseManager(exchange, get_btc_price=get_price)
    await manager.refresh(now=now)

    universe = manager.get_universe()
    assert len(universe) == 2
    tickers = {c["ticker"] for c in universe}
    assert "BTC-25JAN01-61200" in tickers
    assert "BTC-25JAN01-61080" in tickers
    assert exchange._get_btc_contracts_called >= 1


@pytest.mark.asyncio
async def test_soft_block_applied(eligible_contracts_and_books):
    """Soft-blocked contract is excluded from universe and is_soft_blocked returns True."""
    contracts, orderbooks, btc, now = eligible_contracts_and_books
    exchange = MockExchange(contracts, orderbooks)
    manager = UniverseManager(exchange, get_btc_price=lambda: btc)
    await manager.refresh(now=now)

    manager.add_soft_block("BTC-25JAN01-61200", now=now)
    assert manager.is_soft_blocked("BTC-25JAN01-61200", now=now) is True

    universe = manager.get_universe(now=now)
    tickers = {c["ticker"] for c in universe}
    assert "BTC-25JAN01-61200" not in tickers
    assert "BTC-25JAN01-61080" in tickers


@pytest.mark.asyncio
async def test_soft_block_expires(eligible_contracts_and_books):
    """After 10 minutes, soft block expires and contract reappears in universe."""
    contracts, orderbooks, btc, now = eligible_contracts_and_books
    exchange = MockExchange(contracts, orderbooks)
    manager = UniverseManager(exchange, get_btc_price=lambda: btc)
    await manager.refresh(now=now)

    manager.add_soft_block("BTC-25JAN01-61200", now=now)
    assert manager.is_soft_blocked("BTC-25JAN01-61200", now=now) is True

    # Simulate 11 minutes later; block should be expired
    from datetime import timedelta
    later = now + timedelta(minutes=11)
    assert manager.is_soft_blocked("BTC-25JAN01-61200", now=later) is False

    universe = manager.get_universe(now=later)
    tickers = {c["ticker"] for c in universe}
    assert "BTC-25JAN01-61200" in tickers


@pytest.mark.asyncio
async def test_removes_near_expiry():
    """Contracts with expiry too soon are not included in universe after refresh."""
    now = _dt("2025-01-01T12:00:00Z")
    btc = 60600.0
    # One contract closes in 30 min (too soon), one in 120 min
    contracts = [
        {
            "title": "BTC above $61200?",
            "ticker": "BTC-25JAN01-61200-NEAR",
            "close_time": "2025-01-01T12:30:00Z",
            "volume": 1000,
            "open_interest": 800,
        },
        {
            "title": "BTC above $61080?",
            "ticker": "BTC-25JAN01-61080",
            "close_time": "2025-01-01T14:00:00Z",
            "volume": 1000,
            "open_interest": 800,
        },
    ]
    orderbooks = {
        "BTC-25JAN01-61200-NEAR": {
            "asks": [{"price": 60, "quantity": 100}],
            "bids": [{"price": 58, "quantity": 80}],
        },
        "BTC-25JAN01-61080": {
            "asks": [{"price": 55, "quantity": 100}],
            "bids": [{"price": 53, "quantity": 80}],
        },
    }
    exchange = MockExchange(contracts, orderbooks)
    manager = UniverseManager(exchange, get_btc_price=lambda: btc)
    await manager.refresh(now=now)

    universe = manager.get_universe()
    tickers = {c["ticker"] for c in universe}
    assert "BTC-25JAN01-61200-NEAR" not in tickers
    assert "BTC-25JAN01-61080" in tickers
