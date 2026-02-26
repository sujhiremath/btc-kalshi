import asyncio
import json
from datetime import datetime, timedelta, timezone

import pytest

from btc_kalshi.feeds.coinbase_feed import CoinbaseFeed, PriceTick


@pytest.mark.asyncio
async def test_tick_parsing():
    received: list[PriceTick] = []

    async def on_tick(tick: PriceTick) -> None:
        received.append(tick)

    feed = CoinbaseFeed("wss://example", on_tick=on_tick)

    message = json.dumps(
        {
            "product_id": "BTC-USD",
            "events": [
                {
                    "type": "snapshot",
                    "trades": [
                        {
                            "price": "42000.5",
                            "size": "0.01",
                        }
                    ],
                }
            ],
        }
    )

    await feed._handle_message(message)

    assert len(received) == 1
    tick = received[0]
    assert tick.source == "coinbase"
    assert tick.price == pytest.approx(42000.5)
    assert tick.volume == pytest.approx(0.01)


def test_health_check_healthy(monkeypatch):
    feed = CoinbaseFeed("wss://example", on_tick=lambda t: None)

    now = datetime.now(timezone.utc)
    feed._last_tick_ts = now

    monkeypatch.setattr(
        "btc_kalshi.feeds.coinbase_feed.CoinbaseFeed._now",
        staticmethod(lambda: now + timedelta(seconds=5)),
    )

    assert feed.is_healthy() is True


def test_health_check_stale(monkeypatch):
    feed = CoinbaseFeed("wss://example", on_tick=lambda t: None)

    now = datetime.now(timezone.utc)
    feed._last_tick_ts = now - timedelta(seconds=20)

    monkeypatch.setattr(
        "btc_kalshi.feeds.coinbase_feed.CoinbaseFeed._now",
        staticmethod(lambda: now),
    )

    assert feed.is_healthy() is False


@pytest.mark.asyncio
async def test_reconnect_on_disconnect(monkeypatch):
    async def on_tick(_: PriceTick) -> None:
        return

    feed = CoinbaseFeed("wss://example", on_tick=on_tick)

    class FakeContextManager:
        def __init__(self) -> None:
            self.calls = 0

        async def __aenter__(self):
            self.calls += 1
            if self.calls == 1:
                # Simulate a disconnect/failure on first connect.
                raise RuntimeError("disconnect")
            # On second call, stop the feed immediately with an empty iterator.
            feed._stopped = True

            class DummyWS:
                def __aiter__(self):
                    return self

                async def __anext__(self):
                    raise StopAsyncIteration

            return DummyWS()

        async def __aexit__(self, exc_type, exc, tb):
            return False

    cm = FakeContextManager()

    def fake_connect(url: str):
        return cm

    monkeypatch.setattr(
        "btc_kalshi.feeds.coinbase_feed.websockets.connect",
        fake_connect,
    )

    await feed.run()

    # We expect at least two connection attempts: original + one reconnect.
    assert cm.calls >= 2

