import asyncio
from datetime import datetime, timezone

import pytest

from btc_kalshi.feeds.coinbase_feed import PriceTick
from btc_kalshi.feeds import feed_manager as fm_module
from btc_kalshi.feeds.feed_manager import FeedManager


class DummyFeed:
    def __init__(self, name: str, on_tick):
        self.name = name
        self._on_tick = on_tick
        self.healthy = True
        self._last_tick_ts = None

    async def connect(self):
        return None

    async def disconnect(self):
        return None

    def is_healthy(self) -> bool:
        return self.healthy

    @property
    def last_tick_ts(self):
        return self._last_tick_ts

    async def emit(self, price: float):
        now = datetime.now(timezone.utc)
        tick = PriceTick(
            source=self.name,
            price=price,
            volume=1.0,
            timestamp=now,
        )
        self._last_tick_ts = now
        await self._on_tick(tick)


@pytest.mark.asyncio
async def test_uses_primary_by_default(monkeypatch):
    feeds = {}

    def fake_coinbase(url, on_tick):
        feeds["primary"] = DummyFeed("coinbase", on_tick)
        return feeds["primary"]

    def fake_binance(url, on_tick):
        feeds["backup"] = DummyFeed("binance", on_tick)
        return feeds["backup"]

    monkeypatch.setattr(fm_module, "CoinbaseFeed", fake_coinbase)
    monkeypatch.setattr(fm_module, "BinanceFeed", fake_binance)

    manager = FeedManager("ws-primary", "ws-backup")
    await manager.start()

    primary = feeds["primary"]
    backup = feeds["backup"]

    await primary.emit(100.0)
    await backup.emit(100.2)

    price = manager.get_current_price()
    assert price == pytest.approx(100.0)
    assert manager.entries_suspended is False


@pytest.mark.asyncio
async def test_failover_to_backup(monkeypatch):
    feeds = {}

    def fake_coinbase(url, on_tick):
        feeds["primary"] = DummyFeed("coinbase", on_tick)
        return feeds["primary"]

    def fake_binance(url, on_tick):
        feeds["backup"] = DummyFeed("binance", on_tick)
        return feeds["backup"]

    monkeypatch.setattr(fm_module, "CoinbaseFeed", fake_coinbase)
    monkeypatch.setattr(fm_module, "BinanceFeed", fake_binance)

    manager = FeedManager("ws-primary", "ws-backup")
    await manager.start()

    primary = feeds["primary"]
    backup = feeds["backup"]

    await primary.emit(100.0)
    await backup.emit(101.0)

    # Mark primary as unhealthy; backup still healthy.
    primary.healthy = False

    price = manager.get_current_price()
    assert price == pytest.approx(101.0)
    assert manager.entries_suspended is False


@pytest.mark.asyncio
async def test_suspend_when_both_down(monkeypatch):
    feeds = {}

    def fake_coinbase(url, on_tick):
        feeds["primary"] = DummyFeed("coinbase", on_tick)
        return feeds["primary"]

    def fake_binance(url, on_tick):
        feeds["backup"] = DummyFeed("binance", on_tick)
        return feeds["backup"]

    monkeypatch.setattr(fm_module, "CoinbaseFeed", fake_coinbase)
    monkeypatch.setattr(fm_module, "BinanceFeed", fake_binance)

    manager = FeedManager("ws-primary", "ws-backup")
    await manager.start()

    primary = feeds["primary"]
    backup = feeds["backup"]

    primary.healthy = False
    backup.healthy = False

    price = manager.get_current_price()
    assert price is None
    assert manager.entries_suspended is True


@pytest.mark.asyncio
async def test_price_divergence_suspend(monkeypatch):
    feeds = {}

    def fake_coinbase(url, on_tick):
        feeds["primary"] = DummyFeed("coinbase", on_tick)
        return feeds["primary"]

    def fake_binance(url, on_tick):
        feeds["backup"] = DummyFeed("binance", on_tick)
        return feeds["backup"]

    monkeypatch.setattr(fm_module, "CoinbaseFeed", fake_coinbase)
    monkeypatch.setattr(fm_module, "BinanceFeed", fake_binance)

    manager = FeedManager("ws-primary", "ws-backup")
    await manager.start()

    primary = feeds["primary"]
    backup = feeds["backup"]

    # Both healthy but with >0.3% price divergence.
    await primary.emit(100.0)
    await backup.emit(101.0)

    price = manager.get_current_price()
    assert price is None
    assert manager.entries_suspended is True

