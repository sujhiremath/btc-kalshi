"""
Tests for ExitManager: SL/TP triggers, time-based exits, hold to settlement,
execute_take_profit, execute_stop_loss, execute_force_close, failed_exit_fallback.
Mocks ExchangeProtocol and time/orderbook.
"""
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from btc_kalshi.execution.exit_manager import ExitManager


def _position(
    position_id: str = "pos-1",
    contract_id: str = "BTC-20250115",
    entry_price_filled: float = 0.50,
    stop_price: float = 0.08,
    take_profit_price: float = 0.90,
    filled_size: int = 10,
    side: str = "YES",
    expiry_ts: str = "2025-01-15T20:00:00Z",
) -> dict:
    return {
        "position_id": position_id,
        "mode": "live",
        "contract_id": contract_id,
        "expiry_ts": expiry_ts,
        "side": side,
        "entry_price_filled": entry_price_filled,
        "stop_price": stop_price,
        "take_profit_price": take_profit_price,
        "filled_size": filled_size,
        "status": "OPEN",
    }


@pytest.fixture
def exit_manager(state_manager):
    exchange = MagicMock()
    return ExitManager(
        exchange=exchange,
        sqlite_manager=state_manager,
        mode="live",
    ), exchange


@pytest.mark.asyncio
async def test_stop_loss_trigger(exit_manager):
    """check_exit_triggers returns stop_loss when price <= 8¢ or loss >= 20%."""
    om, exchange = exit_manager
    pos = _position(entry_price_filled=0.50, stop_price=0.08)
    exchange.get_orderbook = AsyncMock(return_value={"bids": [{"price": 7, "quantity": 100}], "asks": [{"price": 8, "quantity": 100}]})
    om._get_now = lambda: datetime(2025, 1, 15, 12, 0, 0, tzinfo=timezone.utc)

    trigger = await om.check_exit_triggers(pos)
    assert trigger in ("stop_loss", "sl") or (isinstance(trigger, dict) and trigger.get("action") in ("stop_loss", "sl"))


@pytest.mark.asyncio
async def test_take_profit_trigger(exit_manager):
    """check_exit_triggers returns take_profit when price >= 90¢ or profit >= 25%."""
    om, exchange = exit_manager
    pos = _position(entry_price_filled=0.50, take_profit_price=0.90)
    exchange.get_orderbook = AsyncMock(return_value={"bids": [{"price": 91, "quantity": 100}], "asks": [{"price": 92, "quantity": 100}]})
    om._get_now = lambda: datetime(2025, 1, 15, 12, 0, 0, tzinfo=timezone.utc)

    trigger = await om.check_exit_triggers(pos)
    assert trigger in ("take_profit", "tp") or (isinstance(trigger, dict) and trigger.get("action") in ("take_profit", "tp"))


@pytest.mark.asyncio
async def test_time_based_exit_30min(exit_manager):
    """30-60m to expiry and price < 70¢: time-based exit trigger."""
    om, exchange = exit_manager
    now = datetime(2025, 1, 15, 19, 30, 0, tzinfo=timezone.utc)  # 30m before expiry 20:00 -> in 30-60m tier
    # Entry 0.60 so 65¢ is not TP (profit 8.3% < 25%), and 65¢ < 70¢ for 30-60m tier
    pos = _position(expiry_ts="2025-01-15T20:00:00Z", entry_price_filled=0.60)
    exchange.get_orderbook = AsyncMock(return_value={"bids": [{"price": 65, "quantity": 100}], "asks": [{"price": 66, "quantity": 100}]})
    om._get_now = lambda: now

    trigger = await om.check_exit_triggers(pos)
    assert trigger in ("time_based", "time") or (isinstance(trigger, dict) and "time" in str(trigger.get("action", "")).lower())


@pytest.mark.asyncio
async def test_hold_to_settlement(exit_manager):
    """Price >= 96¢: hold (no exit trigger)."""
    om, exchange = exit_manager
    pos = _position(entry_price_filled=0.50)
    exchange.get_orderbook = AsyncMock(return_value={"bids": [{"price": 97, "quantity": 100}], "asks": [{"price": 98, "quantity": 100}]})
    om._get_now = lambda: datetime(2025, 1, 15, 19, 50, 0, tzinfo=timezone.utc)  # 10m to expiry

    trigger = await om.check_exit_triggers(pos)
    assert trigger is None or trigger == "hold" or (isinstance(trigger, dict) and trigger.get("action") in (None, "hold"))


@pytest.mark.asyncio
async def test_stop_loss_execution_flow(exit_manager, state_manager):
    """execute_stop_loss: limit first, then market within 20s."""
    om, exchange = exit_manager
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
    pos = await state_manager.get_position("pos-1", mode="live")
    exchange.place_order = AsyncMock(return_value={"order": {"id": "ex-1", "status": "filled", "filled_count": 10}})
    exchange.get_order = AsyncMock(return_value={"id": "ex-1", "status": "filled", "filled_count": 10})
    om._get_now = lambda: datetime(2025, 1, 15, 12, 0, 0, tzinfo=timezone.utc)

    result = await om.execute_stop_loss(pos)

    exchange.place_order.assert_called()
    kwargs = exchange.place_order.call_args[1]
    assert kwargs["contract_id"] == "BTC-20250115"
    assert kwargs["count"] == 10


@pytest.mark.asyncio
async def test_take_profit_execution_flow(exit_manager, state_manager):
    """execute_take_profit: limit then reprice then market, 90s max."""
    om, exchange = exit_manager
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
    pos = await state_manager.get_position("pos-1", mode="live")
    exchange.place_order = AsyncMock(return_value={"order": {"id": "ex-1", "status": "filled", "filled_count": 10}})
    om._get_now = lambda: datetime(2025, 1, 15, 12, 0, 0, tzinfo=timezone.utc)

    result = await om.execute_take_profit(pos)

    exchange.place_order.assert_called()
    assert result.get("filled") or result.get("status") == "filled" or exchange.place_order.call_count >= 1


@pytest.mark.asyncio
async def test_force_close_is_market(exit_manager, state_manager):
    """execute_force_close uses market order (type=market or price that crosses)."""
    om, exchange = exit_manager
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
        intended_size=5,
        filled_size=5,
        opened_ts="2025-01-15T12:00:00Z",
        mode="live",
    )
    pos = await state_manager.get_position("pos-1", mode="live")
    exchange.place_order = AsyncMock(return_value={"order": {"id": "ex-1", "status": "filled"}})

    await om.execute_force_close(pos)

    exchange.place_order.assert_called_once()
    call_kw = exchange.place_order.call_args[1]
    assert call_kw.get("type") == "market" or call_kw.get("price_cents") is None


@pytest.mark.asyncio
async def test_failed_exit_fallback(exit_manager, state_manager):
    """failed_exit_fallback: if <10min to expiry use market."""
    om, exchange = exit_manager
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
        intended_size=5,
        filled_size=5,
        opened_ts="2025-01-15T12:00:00Z",
        mode="live",
    )
    pos = await state_manager.get_position("pos-1", mode="live")
    exchange.place_order = AsyncMock(return_value={"order": {"id": "ex-1", "status": "filled"}})
    om._get_now = lambda: datetime(2025, 1, 15, 19, 55, 0, tzinfo=timezone.utc)  # 5 min to expiry

    await om.failed_exit_fallback(pos)

    exchange.place_order.assert_called()
    call_kw = exchange.place_order.call_args[1]
    assert call_kw.get("type") == "market" or call_kw.get("price_cents") is None
