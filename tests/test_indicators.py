"""Tests for strategy indicators (ROC, EMA, volatility, 15min high/low)."""
from __future__ import annotations

import pytest

from btc_kalshi.strategy.indicators import (
    calculate_ema,
    calculate_roc,
    calculate_volatility,
    get_15min_high_low,
)


def test_roc_positive(make_bars):
    """ROC is positive when close increases over the window."""
    # 11 bars: first close 100, last close 105 -> roc = (105-100)/100 = 0.05
    prices = [100.0] * 10 + [105.0]
    bars = make_bars(prices)
    roc = calculate_roc(bars, window=10)
    assert roc is not None
    assert roc > 0


def test_roc_negative(make_bars):
    """ROC is negative when close decreases over the window."""
    prices = [100.0] * 10 + [95.0]
    bars = make_bars(prices)
    roc = calculate_roc(bars, window=10)
    assert roc is not None
    assert roc < 0


def test_ema_converges(make_bars):
    """EMA converges toward constant price over many bars."""
    prices = [100.0] * 60  # 60 bars of constant 100
    bars = make_bars(prices)
    ema = calculate_ema(bars, period=50)
    assert ema is not None
    assert abs(ema - 100.0) < 1.0


def test_volatility_zero(make_bars):
    """Volatility is zero when all closes are the same."""
    prices = [100.0] * 25
    bars = make_bars(prices)
    vol = calculate_volatility(bars, window=20)
    assert vol is not None
    assert vol == pytest.approx(0.0, abs=1e-9)


def test_volatility_high(make_bars):
    """Volatility is higher when prices swing."""
    # Alternating high/low
    prices = [100.0, 102.0] * 15  # 30 bars
    bars = make_bars(prices)
    vol = calculate_volatility(bars, window=20)
    assert vol is not None
    assert vol > 0


def test_15min_high_low(make_bars):
    """get_15min_high_low returns high and low over last 15 minutes of bars (180 bars)."""
    # 200 bars: first 20 with low values, last 180 with range 50-150
    prices = [10.0] * 20 + [50.0 + (i % 101) for i in range(180)]
    bars = make_bars(prices)
    high, low = get_15min_high_low(bars)
    assert high == 150.0
    assert low == 50.0
