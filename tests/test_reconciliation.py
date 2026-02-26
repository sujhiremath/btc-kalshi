"""
Tests for Reconciler (live: Kalshi vs SQLite) and kill switch (live only).
"""
from unittest.mock import AsyncMock, MagicMock

import pytest

from btc_kalshi.execution.kill_switch import execute_kill_switch
from btc_kalshi.execution.reconciliation import Reconciler


@pytest.mark.asyncio
async def test_reconcile_matched(state_manager):
    """When Kalshi positions match local live positions, reconcile passes."""
    await state_manager.open_position(
        position_id="pos-1",
        contract_id="BTC-20250115",
        contract_title="BTC > 100k?",
        expiry_ts="2025-01-15T20:00:00Z",
        side="YES",
        entry_order_client_id="ord-1",
        entry_price_intended=0.50,
        entry_price_filled=0.50,
        stop_price=0.08,
        take_profit_price=0.90,
        intended_size=10,
        filled_size=10,
        opened_ts="2025-01-15T12:00:00Z",
        mode="live",
    )
    exchange = MagicMock()
    exchange.get_positions = AsyncMock(
        return_value=[
            {"ticker": "BTC-20250115", "position": 10, "side": "yes", "contract_id": "BTC-20250115"},
        ]
    )
    reconciler = Reconciler(exchange=exchange, sqlite_manager=state_manager)
    passed = await reconciler.reconcile()
    assert passed is True


@pytest.mark.asyncio
async def test_reconcile_unknown_position(state_manager):
    """When Kalshi has a position not in SQLite (unknown), we close it and still pass if no gaps."""
    exchange = MagicMock()
    exchange.get_positions = AsyncMock(
        return_value=[
            {"ticker": "BTC-UNKNOWN", "position": 5, "side": "yes", "contract_id": "BTC-UNKNOWN"},
        ]
    )
    exchange.place_order = AsyncMock(return_value={"order": {"id": "ex-1", "status": "filled"}})
    reconciler = Reconciler(exchange=exchange, sqlite_manager=state_manager)
    passed = await reconciler.reconcile()
    exchange.place_order.assert_called()
    call_kw = exchange.place_order.call_args[1]
    assert call_kw["contract_id"] == "BTC-UNKNOWN"
    assert call_kw.get("type") == "market" or call_kw.get("price_cents") is None
    assert passed is True


@pytest.mark.asyncio
async def test_reconcile_gap(state_manager):
    """When SQLite has a live position that Kalshi does not (gap), reconcile fails."""
    await state_manager.open_position(
        position_id="pos-1",
        contract_id="BTC-20250115",
        contract_title="BTC > 100k?",
        expiry_ts="2025-01-15T20:00:00Z",
        side="YES",
        entry_order_client_id="ord-1",
        entry_price_intended=0.50,
        entry_price_filled=0.50,
        stop_price=0.08,
        take_profit_price=0.90,
        intended_size=10,
        filled_size=10,
        opened_ts="2025-01-15T12:00:00Z",
        mode="live",
    )
    exchange = MagicMock()
    exchange.get_positions = AsyncMock(return_value=[])
    reconciler = Reconciler(exchange=exchange, sqlite_manager=state_manager)
    passed = await reconciler.reconcile()
    assert passed is False


@pytest.mark.asyncio
async def test_kill_switch_sequence(state_manager):
    """execute_kill_switch: suspend signals, cancel orders, market-close positions, set KILLED."""
    from btc_kalshi.core.state_machine import LifecycleStateMachine
    await state_manager.update_bot_state(mode="live", lifecycle_state="ACTIVE")
    exchange = MagicMock()
    exchange.get_positions = AsyncMock(return_value=[
        {"ticker": "BTC-20250115", "position": 10, "side": "yes", "contract_id": "BTC-20250115"},
    ])
    exchange.get_open_orders = AsyncMock(return_value=[
        {"id": "ord-1", "ticker": "BTC-20250115"},
    ])
    exchange.cancel_order = AsyncMock(return_value={"order": {"status": "cancelled"}})
    exchange.place_order = AsyncMock(return_value={"order": {"status": "filled"}})
    state_machine = LifecycleStateMachine(db=state_manager)
    suspend_signals = AsyncMock()

    await execute_kill_switch(
        exchange=exchange,
        sqlite_manager=state_manager,
        state_machine=state_machine,
        suspend_signals=suspend_signals,
    )

    suspend_signals.assert_called_once()
    exchange.cancel_order.assert_called()
    exchange.place_order.assert_called()
    state = await state_manager.get_bot_state(mode="live")
    assert state["lifecycle_state"] == "KILLED"


@pytest.mark.asyncio
async def test_kill_switch_sets_killed(state_manager):
    """Kill switch sets lifecycle to KILLED."""
    await state_manager.update_bot_state(mode="live", lifecycle_state="ACTIVE")
    exchange = MagicMock()
    exchange.get_positions = AsyncMock(return_value=[])
    exchange.get_open_orders = AsyncMock(return_value=[])
    from btc_kalshi.core.state_machine import LifecycleStateMachine
    sm = LifecycleStateMachine(db=state_manager)

    await execute_kill_switch(
        exchange=exchange,
        sqlite_manager=state_manager,
        state_machine=sm,
        suspend_signals=AsyncMock(),
    )

    state = await state_manager.get_bot_state(mode="live")
    assert state["lifecycle_state"] == "KILLED"


@pytest.mark.asyncio
async def test_kill_switch_independent_of_risk_manager(state_manager):
    """Kill switch does not use risk manager; it uses exchange and state_machine directly."""
    exchange = MagicMock()
    exchange.get_positions = AsyncMock(return_value=[])
    exchange.get_open_orders = AsyncMock(return_value=[])
    state_machine = MagicMock()
    state_machine.transition = AsyncMock(return_value=True)
    suspend_signals = AsyncMock()

    await execute_kill_switch(
        exchange=exchange,
        sqlite_manager=state_manager,
        state_machine=state_machine,
        suspend_signals=suspend_signals,
    )

    state_machine.transition.assert_called_with("KILLED", "kill_switch")
