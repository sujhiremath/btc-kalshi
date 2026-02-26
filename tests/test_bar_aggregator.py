from datetime import datetime, timedelta, timezone

import pytest

from btc_kalshi.feeds.bar_aggregator import BarAggregator
from btc_kalshi.feeds.coinbase_feed import PriceTick


class DummyFeedManager:
    def __init__(self) -> None:
        self.callbacks = []

    def subscribe(self, cb):
        self.callbacks.append(cb)


def _dt(iso: str) -> datetime:
    return datetime.fromisoformat(iso).replace(tzinfo=timezone.utc)


@pytest.mark.asyncio
async def test_single_bar_aggregation(tmp_path):
    fm = DummyFeedManager()
    csv_path = tmp_path / "bars.csv"
    agg = BarAggregator(fm, csv_path)

    ts = _dt("2025-01-01T00:00:01")
    ticks = [
        PriceTick(source="coinbase", price=100.0, volume=1.0, timestamp=ts),
        PriceTick(
            source="coinbase",
            price=101.0,
            volume=2.0,
            timestamp=ts + timedelta(seconds=1),
        ),
        PriceTick(
            source="coinbase",
            price=99.0,
            volume=3.0,
            timestamp=ts + timedelta(seconds=2),
        ),
    ]

    for t in ticks:
        await agg._on_tick(t)

    bar = agg.get_current_incomplete_bar()
    assert bar is not None
    assert bar.open == pytest.approx(100.0)
    assert bar.high == pytest.approx(101.0)
    assert bar.low == pytest.approx(99.0)
    assert bar.close == pytest.approx(99.0)
    assert bar.volume == pytest.approx(1.0 + 2.0 + 3.0)
    assert bar.tick_count == 3


@pytest.mark.asyncio
async def test_bar_emitted_on_window_close(tmp_path):
    fm = DummyFeedManager()
    csv_path = tmp_path / "bars.csv"
    agg = BarAggregator(fm, csv_path)

    received = []

    async def on_bar(bar):
        received.append(bar)

    agg.subscribe(on_bar)

    ts = _dt("2025-01-01T00:00:01")  # bucket 00:00:00
    ts_next = _dt("2025-01-01T00:00:06")  # bucket 00:00:05

    await agg._on_tick(
        PriceTick(source="coinbase", price=100.0, volume=1.0, timestamp=ts)
    )
    # This tick starts a new window and should close the previous bar.
    await agg._on_tick(
        PriceTick(source="coinbase", price=101.0, volume=1.0, timestamp=ts_next)
    )

    assert len(received) == 1
    bar = received[0]
    assert bar.timestamp == datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    assert bar.open == pytest.approx(100.0)


@pytest.mark.asyncio
async def test_rolling_buffer_limit(tmp_path):
    fm = DummyFeedManager()
    csv_path = tmp_path / "bars.csv"
    agg = BarAggregator(fm, csv_path)

    base = _dt("2025-01-01T00:00:00")

    # Produce 510 consecutive windows with one tick each.
    for i in range(510):
        ts = base + timedelta(seconds=i * 5)
        await agg._on_tick(
            PriceTick(source="coinbase", price=100.0 + i, volume=1.0, timestamp=ts)
        )
        # Next tick in next window to close previous, except last iteration.
        if i < 509:
            ts_next = base + timedelta(seconds=i * 5 + 5)
            await agg._on_tick(
                PriceTick(
                    source="coinbase", price=100.0 + i, volume=1.0, timestamp=ts_next
                )
            )

    bars = agg.get_bars(1000)
    assert len(bars) == 500
    # Oldest bar should correspond to window index 10 (0-based).
    assert bars[0].timestamp == base + timedelta(seconds=10 * 5)


@pytest.mark.asyncio
async def test_csv_append(tmp_path):
    fm = DummyFeedManager()
    csv_path = tmp_path / "bars.csv"
    agg = BarAggregator(fm, csv_path)

    ts = _dt("2025-01-01T00:00:01")
    ts2 = _dt("2025-01-01T00:00:06")

    await agg._on_tick(
        PriceTick(source="coinbase", price=100.0, volume=1.0, timestamp=ts)
    )
    await agg._on_tick(
        PriceTick(source="coinbase", price=101.0, volume=1.0, timestamp=ts2)
    )

    assert csv_path.exists()
    content = csv_path.read_text().strip().splitlines()
    # header + one completed bar
    assert len(content) == 2
    assert content[0].startswith("timestamp,open,high,low,close,volume,tick_count")


@pytest.mark.asyncio
async def test_no_bar_on_empty_window(tmp_path):
    fm = DummyFeedManager()
    csv_path = tmp_path / "bars.csv"
    agg = BarAggregator(fm, csv_path)

    ts = _dt("2025-01-01T00:00:01")  # bucket 00:00:00
    ts_gap = _dt("2025-01-01T00:00:21")  # bucket 00:00:20, with empty windows in between

    await agg._on_tick(
        PriceTick(source="coinbase", price=100.0, volume=1.0, timestamp=ts)
    )
    await agg._on_tick(
        PriceTick(source="coinbase", price=102.0, volume=1.0, timestamp=ts_gap)
    )

    bars = agg.get_bars(10)
    # Only one completed bar (for the first window); later windows remain incomplete.
    assert len(bars) == 1
    assert bars[0].timestamp == datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

