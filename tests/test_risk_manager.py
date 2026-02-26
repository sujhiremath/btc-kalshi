"""
Tests for RiskManager: state, window, feed, re-entry, sizing, daily stop, profit protection.
"""
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from btc_kalshi.risk.risk_manager import RiskManager
from btc_kalshi.strategy.signal_engine import Signal


def _make_signal(
    contract_id: str = "BTC-20250115",
    entry_price: float = 0.50,
    stop_price: float = 0.10,
    roc_value: float | None = 0.01,
) -> Signal:
    return Signal(
        timestamp=datetime.now(timezone.utc),
        contract_id=contract_id,
        contract_title="BTC above $100000?",
        direction="bullish",
        side="YES",
        btc_price=100000.0,
        strike_price=100000.0,
        entry_price=entry_price,
        stop_price=stop_price,
        take_profit_price=0.90,
        roc_value=roc_value,
        ema_value=99000.0,
        volatility_ratio=1.0,
        breakout_level=100500.0,
        filter_results={},
        all_passed=True,
        rejection_reason="",
    )


@pytest.fixture
def risk_deps(state_manager, tmp_path):
    """State machine, trading window, position sizer, streak manager for RiskManager."""
    from btc_kalshi.core.state_machine import LifecycleStateMachine
    from btc_kalshi.risk.position_sizer import PositionSizer
    from btc_kalshi.risk.streak_manager import StreakManager
    from btc_kalshi.risk.trading_window import TradingWindowEnforcer

    cal = tmp_path / "macro.json"
    cal.write_text("[]")
    sm = LifecycleStateMachine(db=state_manager)
    window = TradingWindowEnforcer(calendar_path=str(cal))
    sizer = PositionSizer(db=state_manager)
    streak = StreakManager(
        sqlite_manager=state_manager,
        state_machine=sm,
        event_logger=None,
        mode="live",
    )
    return {
        "state_machine": sm,
        "trading_window": window,
        "position_sizer": sizer,
        "streak_manager": streak,
        "sqlite_manager": state_manager,
        "event_logger": None,
    }


@pytest.mark.asyncio
async def test_signal_approved_all_clear(risk_deps, state_manager):
    """When state is ACTIVE, window allows, feed healthy, no re-entry block: approved with size."""
    await state_manager.update_bot_state(
        mode="live",
        lifecycle_state="ACTIVE",
        starting_bankroll=1000.0,
        daily_pnl_net=0.0,
        size_multiplier=1.0,
        trading_date="2025-01-15",
    )
    # Mock window and feed so they allow (window uses "now" so may fail on weekend; mock for reliability)
    window = MagicMock()
    window.is_entry_allowed = MagicMock(return_value=True)
    feed_healthy = MagicMock(return_value=True)
    risk_deps["trading_window"] = window

    rm = RiskManager(
        state_machine=risk_deps["state_machine"],
        trading_window=window,
        position_sizer=risk_deps["position_sizer"],
        streak_manager=risk_deps["streak_manager"],
        sqlite_manager=state_manager,
        event_logger=None,
        mode="live",
        get_feed_healthy=feed_healthy,
    )
    signal = _make_signal()
    approved, reason, size = await rm.evaluate_signal(
        signal, expiry_ts="2025-01-15T20:00:00Z"
    )
    assert approved is True
    assert reason == ""
    assert size >= 1


@pytest.mark.asyncio
async def test_rejected_wrong_state(risk_deps, state_manager):
    """Live mode: rejected when state is not ACTIVE (e.g. PAUSED)."""
    await state_manager.update_bot_state(
        mode="live",
        lifecycle_state="PAUSED",
        starting_bankroll=1000.0,
        daily_pnl_net=0.0,
        size_multiplier=1.0,
        trading_date="2025-01-15",
    )
    window = MagicMock()
    window.is_entry_allowed = MagicMock(return_value=True)
    feed_healthy = MagicMock(return_value=True)

    rm = RiskManager(
        state_machine=risk_deps["state_machine"],
        trading_window=window,
        position_sizer=risk_deps["position_sizer"],
        streak_manager=risk_deps["streak_manager"],
        sqlite_manager=state_manager,
        event_logger=None,
        mode="live",
        get_feed_healthy=feed_healthy,
    )
    signal = _make_signal()
    approved, reason, size = await rm.evaluate_signal(
        signal, expiry_ts="2025-01-15T20:00:00Z"
    )
    assert approved is False
    assert "state" in reason.lower()
    assert size == 0


@pytest.mark.asyncio
async def test_paper_mode_skips_state_check(risk_deps, state_manager):
    """Paper mode: entries allowed without checking lifecycle state."""
    await state_manager.update_bot_state(
        mode="paper",
        lifecycle_state="DISARMED",  # not ACTIVE
        starting_bankroll=1000.0,
        daily_pnl_net=0.0,
        size_multiplier=1.0,
        trading_date="2025-01-15",
    )
    window = MagicMock()
    window.is_entry_allowed = MagicMock(return_value=True)
    feed_healthy = MagicMock(return_value=True)
    # Paper streak manager
    from btc_kalshi.risk.streak_manager import StreakManager
    paper_streak = StreakManager(
        sqlite_manager=state_manager,
        state_machine=risk_deps["state_machine"],
        event_logger=None,
        mode="paper",
    )

    rm = RiskManager(
        state_machine=risk_deps["state_machine"],
        trading_window=window,
        position_sizer=risk_deps["position_sizer"],
        streak_manager=paper_streak,
        sqlite_manager=state_manager,
        event_logger=None,
        mode="paper",
        get_feed_healthy=feed_healthy,
    )
    signal = _make_signal()
    approved, reason, size = await rm.evaluate_signal(
        signal, expiry_ts="2025-01-15T20:00:00Z"
    )
    assert approved is True
    assert size >= 1


@pytest.mark.asyncio
async def test_rejected_outside_window(risk_deps, state_manager):
    """Rejected when trading window does not allow entry."""
    await state_manager.update_bot_state(
        mode="live",
        lifecycle_state="ACTIVE",
        starting_bankroll=1000.0,
        daily_pnl_net=0.0,
        size_multiplier=1.0,
        trading_date="2025-01-15",
    )
    window = MagicMock()
    window.is_entry_allowed = MagicMock(return_value=False)
    feed_healthy = MagicMock(return_value=True)

    rm = RiskManager(
        state_machine=risk_deps["state_machine"],
        trading_window=window,
        position_sizer=risk_deps["position_sizer"],
        streak_manager=risk_deps["streak_manager"],
        sqlite_manager=state_manager,
        event_logger=None,
        mode="live",
        get_feed_healthy=feed_healthy,
    )
    signal = _make_signal()
    approved, reason, size = await rm.evaluate_signal(
        signal, expiry_ts="2025-01-15T20:00:00Z"
    )
    assert approved is False
    assert "window" in reason.lower()
    assert size == 0


@pytest.mark.asyncio
async def test_reentry_blocked_max_entries(risk_deps, state_manager):
    """Rejected when contract already has 2 entries today (max 2/contract/day)."""
    await state_manager.update_bot_state(
        mode="live",
        lifecycle_state="ACTIVE",
        starting_bankroll=1000.0,
        daily_pnl_net=0.0,
        size_multiplier=1.0,
        trading_date="2025-01-15",
    )
    # Create 2 entry orders for same contract today
    for i in range(2):
        await state_manager.create_order(
            client_order_id=f"ord-{i}",
            position_id=f"pos-{i}",
            contract_id="BTC-20250115",
            purpose="entry",
            side="YES",
            intended_price=0.50,
            intended_size=2,
            created_ts="2025-01-15T14:00:00Z",
            mode="live",
        )
    window = MagicMock()
    window.is_entry_allowed = MagicMock(return_value=True)
    feed_healthy = MagicMock(return_value=True)

    rm = RiskManager(
        state_machine=risk_deps["state_machine"],
        trading_window=window,
        position_sizer=risk_deps["position_sizer"],
        streak_manager=risk_deps["streak_manager"],
        sqlite_manager=state_manager,
        event_logger=None,
        mode="live",
        get_feed_healthy=feed_healthy,
    )
    signal = _make_signal(contract_id="BTC-20250115")
    approved, reason, size = await rm.evaluate_signal(
        signal, expiry_ts="2025-01-15T20:00:00Z"
    )
    assert approved is False
    assert ("entry" in reason.lower() or "max" in reason.lower() or "reentry" in reason.lower())
    assert size == 0
