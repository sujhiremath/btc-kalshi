"""Technical indicator calculators for 5-second OHLC bars."""
from __future__ import annotations

import math
from typing import Any, List, Optional, Sequence

# 5-second bars: 15 min = 180 bars, 1 hour = 720 bars
BARS_PER_15MIN = 15 * 60 // 5
BARS_PER_HOUR = 60 * 60 // 5


def _closes(bars: Sequence[Any]) -> List[float]:
    return [float(b.close) for b in bars]


def calculate_roc(bars: Sequence[Any], window: int = 10) -> Optional[float]:
    """
    Rate of change over the last `window` bars: (close_now - close_past) / close_past.
    Returns None if insufficient bars (need at least window + 1).
    """
    if len(bars) < window + 1:
        return None
    closes = _closes(bars)
    past = closes[-(window + 1)]
    now = closes[-1]
    if past == 0:
        return None
    return (now - past) / past


def calculate_ema(bars: Sequence[Any], period: int = 50) -> Optional[float]:
    """
    Exponential moving average of close prices. Returns the most recent EMA.
    Needs at least one bar; first EMA value is close[0], then EMA = mult * close + (1-mult) * EMA
    with mult = 2 / (period + 1).
    """
    if not bars:
        return None
    closes = _closes(bars)
    mult = 2.0 / (period + 1)
    ema = closes[0]
    for c in closes[1:]:
        ema = c * mult + ema * (1.0 - mult)
    return ema


def calculate_volatility(bars: Sequence[Any], window: int = 20) -> Optional[float]:
    """
    Standard deviation of close-to-close returns over the last `window` bars (i.e. window returns).
    Needs at least window + 1 bars. Returns 0 if returns are constant.
    """
    if len(bars) < window + 1:
        return None
    closes = _closes(bars)
    returns: List[float] = []
    for i in range(len(closes) - 1):
        if closes[i] != 0:
            returns.append(closes[i + 1] / closes[i] - 1.0)
    if len(returns) < window:
        return None
    ret_slice = returns[-window:]
    n = len(ret_slice)
    mean = sum(ret_slice) / n
    variance = sum((r - mean) ** 2 for r in ret_slice) / n
    return math.sqrt(variance)


def calculate_hourly_avg_volatility(bars: Sequence[Any]) -> Optional[float]:
    """
    Volatility (std of returns) over the last hour of 5-second bars (720 bars).
    Returns None if fewer than 720 bars.
    """
    if len(bars) < BARS_PER_HOUR:
        return None
    last_hour = bars[-BARS_PER_HOUR:]
    closes = _closes(last_hour)
    returns: List[float] = []
    for i in range(len(closes) - 1):
        if closes[i] != 0:
            returns.append(closes[i + 1] / closes[i] - 1.0)
    if not returns:
        return 0.0
    n = len(returns)
    mean = sum(returns) / n
    variance = sum((r - mean) ** 2 for r in returns) / n
    return math.sqrt(variance)


def get_15min_high_low(bars: Sequence[Any]) -> tuple[float, float]:
    """
    High and low over the last 15 minutes of bars (180 bars of 5s).
    If fewer than 180 bars, uses all bars.
    """
    slice_bars = bars[-BARS_PER_15MIN:] if len(bars) >= BARS_PER_15MIN else bars
    if not slice_bars:
        return 0.0, 0.0
    highs = [float(b.high) for b in slice_bars]
    lows = [float(b.low) for b in slice_bars]
    return max(highs), min(lows)
