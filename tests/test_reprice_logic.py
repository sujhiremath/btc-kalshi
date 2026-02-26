"""
Tests for monitor_entry_fill: phase 1 (poll 0-45s), phase 2 (reprice at 45s), phase 3 (cancel 90s),
partial fill >=60% accept, <60% cancel+top-up. Mocks ExchangeProtocol and time.
"""
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from btc_kalshi.execution.order_manager import OrderManager


def _order_row(
    client_order_id: str = "live-BTC-1-123-YES",
    contract_id: str = "BTC-20250115",
    intended_size: int = 10,
    intended_price: float = 0.50,
    created_ts: str = "2025-01-15T14:00:00Z",
) -> dict:
    return {
        "client_order_id": client_order_id,
        "mode": "live",
        "position_id": None,
        "contract_id": contract_id,
        "purpose": "entry",
        "side": "YES",
        "intended_price": intended_price,
        "intended_size": intended_size,
        "filled_price": None,
        "filled_size": 0,
        "current_status": "NEW",
        "created_ts": created_ts,
        "last_update_ts": None,
    }


def _exchange_order(status: str = "resting", filled_size: int = 0, id: str = "ex-1"):
    return {"id": id, "status": status, "filled_count": filled_size, "count": 10}


@pytest.fixture
def order_manager(state_manager):
    exchange = MagicMock()
    om = OrderManager(
        exchange=exchange,
        sqlite_manager=state_manager,
        event_logger=None,
        mode="live",
    )
    return om, exchange


@pytest.mark.asyncio
async def test_fill_within_45s(order_manager, state_manager):
    """Order fills within 45s: no reprice, outcome filled."""
    om, exchange = order_manager
    om._get_now = lambda: datetime(2025, 1, 15, 14, 0, 30, tzinfo=timezone.utc)  # 30s elapsed
    client_order_id = "live-BTC-20250115-1736956800000-YES"
    created_ts = "2025-01-15T14:00:00Z"
    await state_manager.create_order(
        client_order_id=client_order_id,
        position_id=None,
        contract_id="BTC-20250115",
        purpose="entry",
        side="YES",
        intended_price=0.50,
        intended_size=10,
        created_ts=created_ts,
        mode="live",
    )

    exchange.get_order = AsyncMock(
        return_value={"id": "ex-1", "status": "filled", "filled_count": 10, "count": 10}
    )

    result = await om.monitor_entry_fill(client_order_id)

    assert result.get("outcome") == "filled"
    assert result.get("filled_size") == 10
    exchange.cancel_order.assert_not_called()


@pytest.mark.asyncio
async def test_reprice_after_45s(order_manager, state_manager):
    """After 45s we reprice once to ask+2¢ (cancel + place at new price)."""
    om, exchange = order_manager
    om._get_now = lambda: datetime(2025, 1, 15, 14, 1, 0, tzinfo=timezone.utc)  # 60s elapsed
    client_order_id = "live-BTC-20250115-1736956800000-YES"
    created_ts = "2025-01-15T14:00:00Z"
    await state_manager.create_order(
        client_order_id=client_order_id,
        position_id=None,
        contract_id="BTC-20250115",
        purpose="entry",
        side="YES",
        intended_price=0.50,
        intended_size=10,
        created_ts=created_ts,
        mode="live",
    )

    exchange.get_order = AsyncMock(
        return_value={"id": "ex-1", "status": "resting", "filled_count": 0, "count": 10}
    )
    exchange.get_orderbook = AsyncMock(
        return_value={"asks": [{"price": 52, "quantity": 100}], "bids": [{"price": 51, "quantity": 50}]}  # ask 52¢
    )
    exchange.cancel_order = AsyncMock(return_value={"order": {"status": "cancelled"}})
    exchange.place_order = AsyncMock(
        return_value={"order": {"id": "ex-2", "status": "resting"}}
    )

    result = await om.monitor_entry_fill(client_order_id)

    exchange.cancel_order.assert_called_once()
    exchange.place_order.assert_called_once()
    call_kw = exchange.place_order.call_args[1]
    assert call_kw["price_cents"] == 54  # ask 52¢ + 2¢
    assert call_kw["count"] == 10
    assert result.get("outcome") == "repriced"


@pytest.mark.asyncio
async def test_cancel_after_90s(order_manager, state_manager):
    """After 90s: cancel and soft block."""
    om, exchange = order_manager
    om._get_now = lambda: datetime(2025, 1, 15, 14, 2, 0, tzinfo=timezone.utc)  # 120s elapsed
    client_order_id = "live-BTC-20250115-1736956800000-YES"
    created_ts = "2025-01-15T14:00:00Z"
    await state_manager.create_order(
        client_order_id=client_order_id,
        position_id=None,
        contract_id="BTC-20250115",
        purpose="entry",
        side="YES",
        intended_price=0.50,
        intended_size=10,
        created_ts=created_ts,
        mode="live",
    )

    exchange.get_order = AsyncMock(
        return_value={"id": "ex-1", "status": "resting", "filled_count": 0, "count": 10}
    )
    exchange.cancel_order = AsyncMock(return_value={"order": {"status": "cancelled"}})

    result = await om.monitor_entry_fill(client_order_id)

    assert result.get("outcome") == "cancelled"
    assert result.get("soft_block") is True
    exchange.cancel_order.assert_called_once()


@pytest.mark.asyncio
async def test_partial_fill_above_60pct(order_manager, state_manager):
    """Partial fill >=60%: accept (no cancel)."""
    om, exchange = order_manager
    om._get_now = lambda: datetime(2025, 1, 15, 14, 0, 20, tzinfo=timezone.utc)
    client_order_id = "live-BTC-20250115-1736956800000-YES"
    created_ts = "2025-01-15T14:00:00Z"
    await state_manager.create_order(
        client_order_id=client_order_id,
        position_id=None,
        contract_id="BTC-20250115",
        purpose="entry",
        side="YES",
        intended_price=0.50,
        intended_size=10,
        created_ts=created_ts,
        mode="live",
    )

    exchange.get_order = AsyncMock(
        return_value={"id": "ex-1", "status": "filled", "filled_count": 7, "count": 10}  # 70% >= 60%
    )

    result = await om.monitor_entry_fill(client_order_id)

    assert result.get("outcome") == "filled"
    assert result.get("filled_size") == 7
    exchange.cancel_order.assert_not_called()


@pytest.mark.asyncio
async def test_partial_fill_below_60pct_with_topup(order_manager, state_manager):
    """Partial fill <60%: cancel and optional top-up."""
    om, exchange = order_manager
    client_order_id = "live-BTC-20250115-1736956800000-YES"
    created_ts = "2025-01-15T14:00:00Z"
    await state_manager.create_order(
        client_order_id=client_order_id,
        position_id=None,
        contract_id="BTC-20250115",
        purpose="entry",
        side="YES",
        intended_price=0.50,
        intended_size=10,
        created_ts=created_ts,
        mode="live",
    )

    # 4 filled of 10 = 40% < 60%
    exchange.get_order = AsyncMock(
        return_value={"id": "ex-1", "status": "resting", "filled_count": 4, "count": 10}
    )
    exchange.cancel_order = AsyncMock(return_value={"order": {"status": "cancelled"}})
    exchange.get_orderbook = AsyncMock(return_value={"asks": [{"price": 50, "quantity": 100}], "bids": [{"price": 49, "quantity": 50}]})
    exchange.place_order = AsyncMock(
        return_value={"order": {"id": "ex-2", "status": "resting"}}
    )

    om._get_now = lambda: datetime(2025, 1, 15, 14, 0, 30, tzinfo=timezone.utc)

    result = await om.monitor_entry_fill(client_order_id)

    assert result.get("outcome") == "cancelled"
    assert result.get("partial_fill_size") == 4
    exchange.cancel_order.assert_called_once()
    assert result.get("top_up_placed") is True
    exchange.place_order.assert_called_once()
