"""
Tests for TradingWindowEnforcer (ET trading day, active window, entry/exit rules).
"""
from datetime import datetime

import pytest

from btc_kalshi.risk.trading_window import TradingWindowEnforcer


def _et(year: int, month: int, day: int, hour: int = 0, minute: int = 0) -> datetime:
    """Naive datetime in ET (enforcer treats naive as ET)."""
    return datetime(year, month, day, hour, minute)


@pytest.fixture
def enforcer(tmp_path):
    """Enforcer using a minimal macro calendar in tmp_path."""
    calendar = tmp_path / "macro_calendar.json"
    calendar.write_text("[]")
    return TradingWindowEnforcer(calendar_path=str(calendar))


def test_weekday_is_trading_day(enforcer):
    """Weekday is a trading day."""
    # Monday 2025-01-06 10:00 ET
    assert enforcer.is_trading_day(_et(2025, 1, 6, 10, 0)) is True
    # Wednesday
    assert enforcer.is_trading_day(_et(2025, 1, 8, 12, 0)) is True


def test_weekend_not_trading_day(enforcer):
    """Saturday and Sunday are not trading days."""
    assert enforcer.is_trading_day(_et(2025, 1, 4, 12, 0)) is False   # Saturday
    assert enforcer.is_trading_day(_et(2025, 1, 5, 12, 0)) is False   # Sunday


def test_active_window(enforcer):
    """Active window is 9:30 AM - 8:00 PM ET."""
    # Inside
    assert enforcer.is_in_active_window(_et(2025, 1, 6, 10, 0)) is True
    assert enforcer.is_in_active_window(_et(2025, 1, 6, 9, 30)) is True
    assert enforcer.is_in_active_window(_et(2025, 1, 6, 19, 59)) is True
    # Outside: before open
    assert enforcer.is_in_active_window(_et(2025, 1, 6, 9, 0)) is False
    assert enforcer.is_in_active_window(_et(2025, 1, 6, 9, 29)) is False
    # Outside: at/after 8 PM
    assert enforcer.is_in_active_window(_et(2025, 1, 6, 20, 0)) is False
    assert enforcer.is_in_active_window(_et(2025, 1, 6, 20, 1)) is False


def test_entry_cutoff(enforcer):
    """Entry allowed before 7 PM ET, not at or after 7 PM."""
    # Before 7 PM
    assert enforcer.is_entry_allowed(_et(2025, 1, 6, 18, 59)) is True
    # At 7 PM
    assert enforcer.is_entry_allowed(_et(2025, 1, 6, 19, 0)) is False
    # After 7 PM
    assert enforcer.is_entry_allowed(_et(2025, 1, 6, 19, 30)) is False


def test_avoid_window_lunch(enforcer):
    """Entry not allowed during lunch avoid window 12:00-1:00 PM ET."""
    # Just before lunch
    assert enforcer.is_entry_allowed(_et(2025, 1, 6, 11, 59)) is True
    # During lunch
    assert enforcer.is_entry_allowed(_et(2025, 1, 6, 12, 0)) is False
    assert enforcer.is_entry_allowed(_et(2025, 1, 6, 12, 30)) is False
    assert enforcer.is_entry_allowed(_et(2025, 1, 6, 12, 59)) is False
    # Just after lunch
    assert enforcer.is_entry_allowed(_et(2025, 1, 6, 13, 0)) is True
