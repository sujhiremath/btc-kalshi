"""
Tests for StreakManager: loss/win streaks, daily stop, drawdown floors, profit protection.
"""
import pytest

from btc_kalshi.core.state_machine import LifecycleStateMachine
from btc_kalshi.risk.streak_manager import StreakManager


@pytest.fixture
def streak_manager(state_manager):
    """StreakManager for live mode with state_machine (only live uses it)."""
    sm = LifecycleStateMachine(db=state_manager)
    return StreakManager(
        sqlite_manager=state_manager,
        state_machine=sm,
        event_logger=None,
        mode="live",
    )


@pytest.mark.asyncio
async def test_single_loss_continues(streak_manager, state_manager):
    """One loss: streak = 1, action is continue (no reduction)."""
    await state_manager.update_bot_state(
        mode="live",
        starting_bankroll=1000.0,
        daily_pnl_net=0.0,
        intraday_peak_equity=1000.0,
        current_streak_type=None,
        current_streak_count=0,
        size_multiplier=1.0,
    )
    action = await streak_manager.record_trade_result(pnl=-10.0, is_win=False)
    assert action.get("size_multiplier") == 1.0
    assert action.get("transition") is None
    state = await state_manager.get_bot_state(mode="live")
    assert state["current_streak_type"] == "loss"
    assert state["current_streak_count"] == 1


@pytest.mark.asyncio
async def test_two_losses_reduce(streak_manager, state_manager):
    """Two losses: size_multiplier reduced to 0.75."""
    await state_manager.update_bot_state(
        mode="live",
        starting_bankroll=1000.0,
        daily_pnl_net=0.0,
        intraday_peak_equity=1000.0,
        current_streak_type="loss",
        current_streak_count=1,
        size_multiplier=1.0,
    )
    action = await streak_manager.record_trade_result(pnl=-5.0, is_win=False)
    assert action.get("size_multiplier") == 0.75
    state = await state_manager.get_bot_state(mode="live")
    assert state["current_streak_count"] == 2


@pytest.mark.asyncio
async def test_five_losses_stop(streak_manager, state_manager):
    """Five losses: transition to STOPPED (live)."""
    await state_manager.update_bot_state(
        mode="live",
        starting_bankroll=1000.0,
        daily_pnl_net=0.0,
        intraday_peak_equity=1000.0,
        current_streak_type="loss",
        current_streak_count=4,
        size_multiplier=0.5,
        lifecycle_state="ACTIVE",
    )
    action = await streak_manager.record_trade_result(pnl=-5.0, is_win=False)
    assert action.get("transition") == "STOPPED"
    state = await state_manager.get_bot_state(mode="live")
    assert state["current_streak_count"] == 5
    assert state["lifecycle_state"] == "STOPPED"


@pytest.mark.asyncio
async def test_win_streak_increase(streak_manager, state_manager):
    """Win streak 2–3: size_multiplier 1.15."""
    await state_manager.update_bot_state(
        mode="live",
        starting_bankroll=1000.0,
        daily_pnl_net=0.0,
        intraday_peak_equity=1000.0,
        current_streak_type="win",
        current_streak_count=1,
        size_multiplier=1.0,
    )
    action = await streak_manager.record_trade_result(pnl=20.0, is_win=True)
    assert action.get("size_multiplier") == 1.15
    state = await state_manager.get_bot_state(mode="live")
    assert state["current_streak_count"] == 2


@pytest.mark.asyncio
async def test_daily_stop_triggered(streak_manager, state_manager):
    """check_daily_stop returns True when daily loss >= 5% of starting bankroll."""
    await state_manager.update_bot_state(
        mode="live",
        starting_bankroll=1000.0,
        daily_pnl_net=-60.0,  # -6% > -5% threshold
        intraday_peak_equity=1000.0,
    )
    triggered = await streak_manager.check_daily_stop()
    assert triggered is True
    # -50 is exactly 5%, so -50 should trigger (>= 5% loss)
    await state_manager.update_bot_state(mode="live", daily_pnl_net=-50.0)
    triggered = await streak_manager.check_daily_stop()
    assert triggered is True


@pytest.mark.asyncio
async def test_drawdown_floor_from_peak(streak_manager, state_manager):
    """check_drawdown_floors triggers when drawdown from intraday peak exceeds floor."""
    await state_manager.update_bot_state(
        mode="live",
        starting_bankroll=1000.0,
        daily_pnl_net=-50.0,   # equity 950
        intraday_peak_equity=1000.0,  # drawdown (1000-950)/1000 = 5%
    )
    # 5% < 8% floor, so not triggered yet
    triggered = await streak_manager.check_drawdown_floors()
    assert triggered is False

    await state_manager.update_bot_state(
        mode="live",
        daily_pnl_net=-100.0,  # equity 900, drawdown 10% > 8%
        intraday_peak_equity=1000.0,
    )
    triggered = await streak_manager.check_drawdown_floors()
    assert triggered is True
