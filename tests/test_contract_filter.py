"""Tests for canonical contract format and eligibility filter."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from btc_kalshi.exchange.contract_filter import (
    check_eligibility,
    filter_universe,
    is_canonical_format,
)


def _dt(iso: str) -> datetime:
    return datetime.fromisoformat(iso.replace("Z", "+00:00"))


def test_canonical_format_above():
    """Contract 'above $X at time T' is canonical."""
    contract = {"title": "BTC above $60000?", "ticker": "BTC-25JAN01-60000"}
    assert is_canonical_format(contract) is True

    contract2 = {"title": "BTC above $95000 at 4pm ET?"}
    assert is_canonical_format(contract2) is True


def test_canonical_format_below_rejected():
    """Contract 'below $X' is not canonical."""
    contract = {"title": "BTC below $60000?", "ticker": "BTC-25JAN01-60000B"}
    assert is_canonical_format(contract) is False


def test_canonical_format_malformed():
    """Missing title or non-BTC above pattern is rejected."""
    assert is_canonical_format({}) is False
    assert is_canonical_format({"ticker": "BTC-25JAN01"}) is False
    assert is_canonical_format({"title": "ETH above $3000?"}) is False
    assert is_canonical_format({"title": "BTC above"}) is False  # no price


def test_eligibility_all_pass():
    """Contract passes when strike, expiry, volume, OI, depth, ask, spread are within bounds."""
    now = _dt("2025-01-01T12:00:00Z")
    btc_price = 60000.0
    # Strike 61200 = 2% above, within 0.6%–1.2% we need 60600–60720 for MIN/MAX 0.006/0.012
    # So use strike 60660 (1% above) -> distance 0.011, in [0.006, 0.012]
    contract = {
        "title": "BTC above $60660?",
        "ticker": "BTC-25JAN01-60660",
        "close_time": "2025-01-01T14:00:00Z",  # 120 min from now
        "volume": 1000,
        "open_interest": 800,
    }
    orderbook = {
        "asks": [{"price": 65, "quantity": 100}],  # 0.65 ask, 100 contracts
        "bids": [{"price": 62, "quantity": 80}],   # spread 3 cents = 0.03
    }
    ok, reason = check_eligibility(contract, btc_price, orderbook, now)
    assert ok is True, reason
    assert reason is None or reason == ""


def test_eligibility_strike_too_far():
    """Contract rejected when strike distance outside [MIN, MAX]."""
    now = _dt("2025-01-01T12:00:00Z")
    btc_price = 60000.0
    # Strike 65000 = 8.33% above -> way above STRIKE_DISTANCE_MAX 0.012
    contract = {
        "title": "BTC above $65000?",
        "ticker": "BTC-25JAN01-65000",
        "close_time": "2025-01-01T14:00:00Z",
        "volume": 1000,
        "open_interest": 800,
    }
    orderbook = {"asks": [{"price": 50, "quantity": 100}], "bids": [{"price": 48, "quantity": 80}]}
    ok, reason = check_eligibility(contract, btc_price, orderbook, now)
    assert ok is False
    assert "strike" in reason.lower()


def test_eligibility_expiry_too_soon():
    """Contract rejected when expiry is sooner than MIN_EXPIRY_MINUTES."""
    now = _dt("2025-01-01T12:00:00Z")
    btc_price = 60000.0
    # Close in 30 minutes (less than MIN_EXPIRY_MINUTES 45)
    contract = {
        "title": "BTC above $60600?",
        "ticker": "BTC-25JAN01-60600",
        "close_time": "2025-01-01T12:30:00Z",
        "volume": 1000,
        "open_interest": 800,
    }
    orderbook = {"asks": [{"price": 55, "quantity": 100}], "bids": [{"price": 52, "quantity": 80}]}
    ok, reason = check_eligibility(contract, btc_price, orderbook, now)
    assert ok is False
    assert "expir" in reason.lower()


def test_filter_universe_returns_only_eligible():
    """filter_universe returns only canonical and eligible contracts."""
    now = _dt("2025-01-01T12:00:00Z")
    btc_price = 60600.0
    contracts = [
        {
            "title": "BTC below $60000?",
            "ticker": "BTC-25JAN01-60K-B",
            "close_time": "2025-01-01T14:00:00Z",
            "volume": 1000,
            "open_interest": 800,
        },
        {
            "title": "BTC above $61200?",
            "ticker": "BTC-25JAN01-61200",
            "close_time": "2025-01-01T14:00:00Z",
            "volume": 1000,
            "open_interest": 800,
        },
    ]
    orderbooks = {
        "BTC-25JAN01-60K-B": {"asks": [{"price": 50, "quantity": 100}], "bids": [{"price": 48, "quantity": 80}]},
        "BTC-25JAN01-61200": {"asks": [{"price": 60, "quantity": 100}], "bids": [{"price": 58, "quantity": 80}]},
    }
    # 61200/60600 - 1 = 0.0099, in [0.006, 0.012]
    result = filter_universe(contracts, btc_price, orderbooks, now)
    assert len(result) == 1
    assert result[0]["ticker"] == "BTC-25JAN01-61200"
