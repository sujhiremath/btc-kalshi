"""Tests for signal engine 8-filter gating and direction mapping."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List
from unittest.mock import patch

import pytest

from btc_kalshi.strategy.signal_engine import Signal, SignalEngine


def _contract(ticker: str = "BTC-25JAN01-60600", title: str = "BTC above $60600?") -> Dict[str, Any]:
    return {"ticker": ticker, "title": title}


def _orderbook(ask_cents: int = 60, bid_cents: int = 58) -> Dict[str, Any]:
    return {
        "asks": [{"price": ask_cents, "quantity": 100}],
        "bids": [{"price": bid_cents, "quantity": 80}],
    }


def _bars_200():
    """Minimal bar list (200 bars) so filters have enough data."""
    from tests.conftest import make_bars_fn
    return make_bars_fn([60000.0 + (i * 2) for i in range(200)])


@pytest.fixture
def engine():
    return SignalEngine()


def test_all_filters_pass(engine, monkeypatch):
    """When all 8 filters pass, signal has all_passed True and no rejection_reason."""
    bars = _bars_200()
    contract = _contract()
    orderbook = _orderbook(60, 58)
    btc_price = 60600.0

    with patch.object(engine, "_check_price_move", return_value=True), \
         patch.object(engine, "_check_breakout", return_value=("up", 60500.0)), \
         patch.object(engine, "_check_false_breakout", return_value=True), \
         patch.object(engine, "_check_roc", return_value=(True, 0.005)), \
         patch.object(engine, "_check_volatility", return_value=(True, 0.5)), \
         patch.object(engine, "_check_ema_trend", return_value=True), \
         patch.object(engine, "_check_ask", return_value=True), \
         patch.object(engine, "_check_spread", return_value=True):
        signal = engine.evaluate(contract, orderbook, bars, btc_price)

    assert signal is not None
    assert signal.all_passed is True
    assert signal.rejection_reason is None or signal.rejection_reason == ""
    assert signal.direction == "up"
    assert signal.side == "YES"


def test_roc_filter_fails(engine):
    """When ROC is below 0.4%, signal fails with rejection_reason mentioning ROC."""
    bars = _bars_200()
    contract = _contract()
    orderbook = _orderbook(60, 58)
    btc_price = 60600.0

    with patch.object(engine, "_check_price_move", return_value=True), \
         patch.object(engine, "_check_breakout", return_value=("up", 60500.0)), \
         patch.object(engine, "_check_false_breakout", return_value=True), \
         patch("btc_kalshi.strategy.signal_engine.calculate_roc", return_value=0.002):
        signal = engine.evaluate(contract, orderbook, bars, btc_price)

    assert signal is not None
    assert signal.all_passed is False
    assert "roc" in signal.rejection_reason.lower()


def test_volatility_too_high(engine):
    """When volatility ratio exceeds threshold, signal fails."""
    bars = _bars_200()
    contract = _contract()
    orderbook = _orderbook(60, 58)
    btc_price = 60600.0

    with patch.object(engine, "_check_price_move", return_value=True), \
         patch.object(engine, "_check_breakout", return_value=("up", 60500.0)), \
         patch.object(engine, "_check_false_breakout", return_value=True), \
         patch.object(engine, "_check_roc", return_value=(True, 0.005)), \
         patch("btc_kalshi.strategy.signal_engine.calculate_volatility", return_value=0.01), \
         patch("btc_kalshi.strategy.signal_engine.calculate_hourly_avg_volatility", return_value=0.002):
        signal = engine.evaluate(contract, orderbook, bars, btc_price)

    assert signal is not None
    assert signal.all_passed is False
    assert "volatility" in signal.rejection_reason.lower()


def test_spread_too_wide(engine):
    """When spread > 3.5¢, signal fails."""
    contract = _contract()
    orderbook = _orderbook(70, 30)  # spread 40¢
    bars = _bars_200()
    btc_price = 60600.0

    with patch.object(engine, "_check_price_move", return_value=True), \
         patch.object(engine, "_check_breakout", return_value=("up", 60500.0)), \
         patch.object(engine, "_check_false_breakout", return_value=True), \
         patch.object(engine, "_check_roc", return_value=(True, 0.005)), \
         patch.object(engine, "_check_volatility", return_value=(True, 0.5)), \
         patch.object(engine, "_check_ema_trend", return_value=True), \
         patch.object(engine, "_check_ask", return_value=True):
        signal = engine.evaluate(contract, orderbook, bars, btc_price)

    assert signal is not None
    assert signal.all_passed is False
    assert "spread" in signal.rejection_reason.lower()


def test_direction_mapping(engine):
    """Bullish -> YES, bearish -> NO."""
    bars = _bars_200()
    contract = _contract()
    orderbook = _orderbook(60, 58)
    btc_price = 60600.0

    with patch.object(engine, "_check_price_move", return_value=True), \
         patch.object(engine, "_check_breakout", return_value=("up", 60500.0)), \
         patch.object(engine, "_check_false_breakout", return_value=True), \
         patch.object(engine, "_check_roc", return_value=(True, 0.005)), \
         patch.object(engine, "_check_volatility", return_value=(True, 0.5)), \
         patch.object(engine, "_check_ema_trend", return_value=True), \
         patch.object(engine, "_check_ask", return_value=True), \
         patch.object(engine, "_check_spread", return_value=True):
        signal_up = engine.evaluate(contract, orderbook, bars, btc_price)
    assert signal_up.side == "YES" and signal_up.direction == "up"

    with patch.object(engine, "_check_price_move", return_value=True), \
         patch.object(engine, "_check_breakout", return_value=("down", 60500.0)), \
         patch.object(engine, "_check_false_breakout", return_value=True), \
         patch.object(engine, "_check_roc", return_value=(True, -0.005)), \
         patch.object(engine, "_check_volatility", return_value=(True, 0.5)), \
         patch.object(engine, "_check_ema_trend", return_value=True), \
         patch.object(engine, "_check_ask", return_value=True), \
         patch.object(engine, "_check_spread", return_value=True):
        signal_down = engine.evaluate(contract, orderbook, bars, btc_price)
    assert signal_down.side == "NO" and signal_down.direction == "down"
