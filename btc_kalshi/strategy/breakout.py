"""Breakout detection: closed bar vs 15-min high/low, with false-breakout filter."""
from __future__ import annotations

from typing import Any, Optional, Sequence, Tuple

from btc_kalshi.core.constants import (
    FALSE_BREAKOUT_BARS,
    PRICE_MOVE_THRESHOLD,
    PRICE_MOVE_WINDOW_MAX,
    PRICE_MOVE_WINDOW_MIN,
)
from btc_kalshi.strategy.indicators import BARS_PER_15MIN, get_15min_high_low


def detect_breakout(bars: Sequence[Any]) -> Optional[Tuple[str, float]]:
    """
    Detect breakout using the most recent closed bar's close vs 15-min high/low.
    Wick does not count: only close is compared to the level.
    Returns (direction, level) where direction is "up" or "down", or None if no breakout.
    Needs at least BARS_PER_15MIN + 1 bars (181 for 5s bars).
    """
    if len(bars) < BARS_PER_15MIN + 1:
        return None
    high_15, low_15 = get_15min_high_low(bars[:-1])
    last = bars[-1]
    close = float(last.close)
    if close > high_15:
        return ("up", high_15)
    if close < low_15:
        return ("down", low_15)
    return None


def confirm_breakout(
    bars: Sequence[Any],
    direction: str,
    level: float,
) -> bool:
    """
    Confirm breakout: the next FALSE_BREAKOUT_BARS (2) bars must stay beyond the level.
    Uses close only. Returns True if both confirmation bars stay beyond level.
    """
    if len(bars) < FALSE_BREAKOUT_BARS:
        return False
    n = FALSE_BREAKOUT_BARS
    confirmation_bars = bars[-n:]
    if direction == "up":
        return all(float(b.close) > level for b in confirmation_bars)
    if direction == "down":
        return all(float(b.close) < level for b in confirmation_bars)
    return False


def check_price_move(bars: Sequence[Any], direction: str) -> bool:
    """
    Check for a PRICE_MOVE_THRESHOLD (0.5%) move within a window of
    PRICE_MOVE_WINDOW_MIN to PRICE_MOVE_WINDOW_MAX bars (5–15 min for 5s bars).
    """
    if len(bars) < PRICE_MOVE_WINDOW_MAX:
        return False
    closes = [float(b.close) for b in bars]
    end_close = closes[-1]
    for start_idx in range(-PRICE_MOVE_WINDOW_MAX, -PRICE_MOVE_WINDOW_MIN + 1):
        start_close = closes[start_idx]
        if start_close <= 0:
            continue
        ret = (end_close - start_close) / start_close
        if direction == "up" and ret >= PRICE_MOVE_THRESHOLD:
            return True
        if direction == "down" and ret <= -PRICE_MOVE_THRESHOLD:
            return True
    return False
