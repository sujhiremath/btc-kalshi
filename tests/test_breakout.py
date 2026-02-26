"""Tests for breakout detection and false-breakout confirmation."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from btc_kalshi.feeds.bar_aggregator import Bar
from btc_kalshi.strategy.breakout import (
    confirm_breakout,
    detect_breakout,
)


def _make_bars(prices, start_ts=None):
    from tests.conftest import make_bars as _mk
    return _mk(prices, start_ts)


def _one_bar(ts, open_, high, low, close):
    return Bar(timestamp=ts, open=open_, high=high, low=low, close=close, volume=1.0, tick_count=1)


def test_bullish_breakout_detected(make_bars):
    """Close above 15-min high (body break, not wick) is detected as bullish breakout."""
    # 181 bars: first 180 at 100, last bar close 101 (above 15-min high 100)
    prices = [100.0] * 180 + [101.0]
    bars = make_bars(prices)
    result = detect_breakout(bars)
    assert result is not None
    direction, level = result
    assert direction == "up"
    assert level == pytest.approx(100.0)


def test_bearish_breakout_detected(make_bars):
    """Close below 15-min low is detected as bearish breakout."""
    prices = [100.0] * 180 + [99.0]
    bars = make_bars(prices)
    result = detect_breakout(bars)
    assert result is not None
    direction, level = result
    assert direction == "down"
    assert level == pytest.approx(100.0)


def test_no_breakout_within_range(make_bars):
    """Close within 15-min range does not trigger breakout."""
    # 15-min window: mix so high=101, low=99; last bar close 100 (inside range)
    prices = [99.0, 101.0] * 89 + [99.0, 100.0, 100.0]  # 181 bars; 15-min high=101 low=99, last close 100
    bars = make_bars(prices)
    result = detect_breakout(bars)
    assert result is None


def test_wick_only_not_counted(make_bars):
    """High wick above 15-min high but close below does not count as breakout."""
    # 180 bars at 100, then one bar: high 102, close 100 (wick only)
    prices = [100.0] * 180
    bars = make_bars(prices)
    start = bars[0].timestamp
    last_ts = start + timedelta(seconds=179 * 5)
    bars[-1] = _one_bar(last_ts, 100.0, 102.0, 99.0, 100.0)
    result = detect_breakout(bars)
    assert result is None


def test_false_breakout_confirmed(make_bars):
    """Next 2 bars stay beyond level -> breakout confirmed."""
    # Breakout bar closed at 101 (above 100). Then 2 bars both close above 100.
    prices = [100.0] * 178 + [101.0, 101.5, 102.0]  # breakout + 2 confirmations
    bars = make_bars(prices)
    ok = confirm_breakout(bars, "up", 100.0)
    assert ok is True


def test_false_breakout_rejected(make_bars):
    """One of next 2 bars comes back inside -> not confirmed."""
    # Breakout at 101, then 101.5, then 99.5 (back below 100)
    prices = [100.0] * 178 + [101.0, 101.5, 99.5]
    bars = make_bars(prices)
    ok = confirm_breakout(bars, "up", 100.0)
    assert ok is False
