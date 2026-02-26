import pytest

from btc_kalshi.db.sqlite_manager import SQLiteStateManager


@pytest.mark.asyncio
async def test_init_creates_tables(state_manager: SQLiteStateManager):
    # If we can read both modes without error and get a dict, tables exist.
    live_state = await state_manager.get_bot_state(mode="live")
    paper_state = await state_manager.get_bot_state(mode="paper")

    assert isinstance(live_state, dict)
    assert isinstance(paper_state, dict)
    assert "armed" in live_state
    assert "armed" in paper_state


@pytest.mark.asyncio
async def test_default_bot_state(state_manager: SQLiteStateManager):
    state = await state_manager.get_bot_state(mode="live")

    # Initial state should be disarmed.
    assert state["armed"] == 0
    assert state["lifecycle_state"] == "DISARMED"
    assert state["mode"] == "live"


@pytest.mark.asyncio
async def test_update_bot_state(state_manager: SQLiteStateManager):
    await state_manager.update_bot_state(
        mode="live",
        armed=1,
        lifecycle_state="ARMED",
        current_streak_type="win",
        current_streak_count=3,
    )

    state = await state_manager.get_bot_state(mode="live")
    assert state["armed"] == 1
    assert state["lifecycle_state"] == "ARMED"
    assert state["current_streak_type"] == "win"
    assert state["current_streak_count"] == 3


@pytest.mark.asyncio
async def test_update_preserves_other_fields(state_manager: SQLiteStateManager):
    # Establish some baseline values.
    await state_manager.update_bot_state(
        mode="live",
        starting_bankroll=500.0,
        daily_pnl_net=10.0,
    )
    before = await state_manager.get_bot_state(mode="live")

    # Update only one field and ensure others are preserved.
    await state_manager.update_bot_state(mode="live", armed=1)
    after = await state_manager.get_bot_state(mode="live")

    assert after["armed"] == 1
    assert after["starting_bankroll"] == before["starting_bankroll"]
    assert after["daily_pnl_net"] == before["daily_pnl_net"]


@pytest.mark.asyncio
async def test_reset_daily_state(state_manager: SQLiteStateManager):
    await state_manager.update_bot_state(
        mode="live",
        armed=1,
        lifecycle_state="ARMED",
        daily_pnl_gross=5.0,
        daily_pnl_net=4.0,
        intraday_peak_equity=110.0,
        current_streak_type="win",
        current_streak_count=2,
    )

    await state_manager.reset_daily_state(
        trading_date="2025-01-01",
        starting_bankroll=100.0,
        mode="live",
    )

    state = await state_manager.get_bot_state(mode="live")

    assert state["trading_date"] == "2025-01-01"
    assert state["starting_bankroll"] == 100.0
    assert state["daily_pnl_gross"] == 0.0
    assert state["daily_pnl_net"] == 0.0
    assert state["intraday_peak_equity"] == 100.0
    assert state["current_streak_type"] is None
    assert state["current_streak_count"] == 0
    assert state["armed"] == 0
    assert state["lifecycle_state"] == "DISARMED"


@pytest.mark.asyncio
async def test_paper_state_independent(state_manager: SQLiteStateManager):
    # Reset both states with different bankrolls.
    await state_manager.reset_daily_state(
        trading_date="2025-01-01",
        starting_bankroll=100.0,
        mode="live",
    )
    await state_manager.reset_daily_state(
        trading_date="2025-01-01",
        starting_bankroll=200.0,
        mode="paper",
    )

    # Update paper_state only.
    await state_manager.update_bot_state(
        mode="paper",
        daily_pnl_net=15.0,
    )

    live_state = await state_manager.get_bot_state(mode="live")
    paper_state = await state_manager.get_bot_state(mode="paper")

    assert live_state["starting_bankroll"] == 100.0
    assert paper_state["starting_bankroll"] == 200.0

    # PnL changes in paper state should not affect live state.
    assert live_state["daily_pnl_net"] == 0.0
    assert paper_state["daily_pnl_net"] == 15.0

