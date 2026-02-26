"""
Tests for OrderManager: client order ID, persist-before-send, entry order, ambiguous retry, cancel, same ID on retry.
"""
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from btc_kalshi.execution.order_manager import OrderManager, generate_client_order_id
from btc_kalshi.strategy.signal_engine import Signal


def _make_signal(
    contract_id: str = "BTC-20250115",
    entry_price: float = 0.50,
    side: str = "YES",
) -> Signal:
    return Signal(
        timestamp=datetime(2025, 1, 15, 14, 30, 0, tzinfo=timezone.utc),
        contract_id=contract_id,
        contract_title="BTC above $100000?",
        direction="bullish",
        side=side,
        btc_price=100000.0,
        strike_price=100000.0,
        entry_price=entry_price,
        stop_price=0.10,
        take_profit_price=0.90,
        roc_value=0.01,
        ema_value=99000.0,
        volatility_ratio=1.0,
        breakout_level=100500.0,
        filter_results={},
        all_passed=True,
        rejection_reason="",
    )


def test_client_order_id_includes_mode():
    """generate_client_order_id includes mode, contract_id, ts_int, side."""
    ts = datetime(2025, 1, 15, 14, 30, 0, tzinfo=timezone.utc)
    oid = generate_client_order_id("BTC-20250115", ts, "YES", mode="live")
    assert oid.startswith("live-")
    assert "BTC-20250115" in oid
    assert "YES" in oid
    # ts_int (e.g. epoch ms)
    parts = oid.split("-")
    assert len(parts) >= 4
    assert parts[0] == "live"
    assert parts[-1] == "YES"

    oid_paper = generate_client_order_id("BTC-20250115", ts, "NO", mode="paper")
    assert oid_paper.startswith("paper-")
    assert "NO" in oid_paper


@pytest.mark.asyncio
async def test_order_persisted_before_send(state_manager):
    """Order is written to SQLite before exchange.place_order is called."""
    exchange = MagicMock()
    captured_client_order_id = []

    async def place_mock(*args, **kwargs):
        # When exchange.place_order is called, order must already be in DB
        cid = kwargs.get("client_order_id")
        captured_client_order_id.append(cid)
        row = await state_manager.get_order(client_order_id=cid, mode="live")
        assert row is not None, "Order must be persisted before place_order is called"
        assert row["contract_id"] == "BTC-20250115"
        assert row["purpose"] == "entry"
        assert row["intended_size"] == 2
        return {"order": {"id": "ex-1", "status": "resting"}}

    exchange.place_order = AsyncMock(side_effect=place_mock)

    om = OrderManager(
        exchange=exchange,
        sqlite_manager=state_manager,
        event_logger=None,
        mode="live",
    )
    signal = _make_signal()
    await om.place_entry_order(signal, size=2)

    assert len(captured_client_order_id) == 1
    order_in_db = await state_manager.get_order(
        client_order_id=captured_client_order_id[0],
        mode="live",
    )
    assert order_in_db is not None
    assert order_in_db["contract_id"] == "BTC-20250115"
    assert order_in_db["purpose"] == "entry"
    assert order_in_db["intended_size"] == 2


@pytest.mark.asyncio
async def test_entry_order_success(state_manager):
    """place_entry_order succeeds and order record has correct mode tag."""
    exchange = MagicMock()
    exchange.place_order = AsyncMock(
        return_value={"order": {"id": "ex-456", "status": "resting"}}
    )

    om = OrderManager(
        exchange=exchange,
        sqlite_manager=state_manager,
        event_logger=None,
        mode="paper",
    )
    signal = _make_signal(contract_id="BTC-20250120")
    result = await om.place_entry_order(signal, size=3)

    assert result is not None
    assert exchange.place_order.called
    kwargs = exchange.place_order.call_args[1]
    assert kwargs["contract_id"] == "BTC-20250120"
    assert kwargs["count"] == 3
    assert kwargs["client_order_id"].startswith("paper-")

    # DB record has mode = paper
    client_order_id = kwargs["client_order_id"]
    row = await state_manager.get_order(client_order_id=client_order_id, mode="paper")
    assert row is not None
    assert row["mode"] == "paper"


@pytest.mark.asyncio
async def test_ambiguous_response_retries(state_manager):
    """On ambiguous response, manager waits and queries by client_order_id, then retries if needed."""
    exchange = MagicMock()
    # First place returns ambiguous (no order id), then get_order returns the order
    exchange.place_order = AsyncMock(return_value={})  # ambiguous
    exchange.get_order = AsyncMock(return_value=None)

    async def get_after_delay(client_order_id):
        # Simulate: after 5s wait, order appears
        return {"id": "ex-789", "client_order_id": client_order_id, "status": "resting"}

    # First get_order returns None, so we retry place with same id; second get returns order
    call_count = {"get": 0}

    async def get_order_side_effect(order_id):
        call_count["get"] += 1
        if call_count["get"] >= 2:
            return {"id": "ex-789", "client_order_id": order_id, "status": "resting"}
        return None

    exchange.get_order = AsyncMock(side_effect=get_order_side_effect)
    place_calls = []

    async def place_side_effect(*args, **kwargs):
        place_calls.append(kwargs.get("client_order_id"))
        return {}  # ambiguous

    exchange.place_order = AsyncMock(side_effect=place_side_effect)

    om = OrderManager(
        exchange=exchange,
        sqlite_manager=state_manager,
        event_logger=None,
        mode="live",
    )
    signal = _make_signal()
    await om.place_entry_order(signal, size=1)

    # Should have called place at least once, then get_order, then possibly retry place with same id
    assert len(place_calls) >= 1
    if len(place_calls) >= 2:
        assert place_calls[0] == place_calls[1]  # same client_order_id on retry


@pytest.mark.asyncio
async def test_cancel_order(state_manager):
    """cancel_order calls exchange.cancel_order with the order id."""
    exchange = MagicMock()
    exchange.get_order = AsyncMock(
        return_value={"id": "ex-cancel-me", "client_order_id": "live-BTC-1-123-YES", "status": "resting"}
    )
    exchange.cancel_order = AsyncMock(return_value={"order": {"status": "cancelled"}})

    om = OrderManager(
        exchange=exchange,
        sqlite_manager=state_manager,
        event_logger=None,
        mode="live",
    )
    await om.cancel_order("live-BTC-1-123-YES")

    exchange.get_order.assert_called_once_with("live-BTC-1-123-YES")
    exchange.cancel_order.assert_called_once_with("ex-cancel-me")


@pytest.mark.asyncio
async def test_same_id_on_retry(state_manager):
    """On retry after ambiguous response, same client_order_id is sent (idempotent)."""
    exchange = MagicMock()
    client_order_ids_used = []

    async def place_capture(*args, **kwargs):
        client_order_ids_used.append(kwargs.get("client_order_id"))
        return {}  # ambiguous both times

    exchange.place_order = AsyncMock(side_effect=place_capture)
    exchange.get_order = AsyncMock(return_value=None)  # never find it, so we retry place

    om = OrderManager(
        exchange=exchange,
        sqlite_manager=state_manager,
        event_logger=None,
        mode="live",
    )
    signal = _make_signal()
    await om.place_entry_order(signal, size=1)

    # Same client_order_id used on all place_order calls
    assert len(client_order_ids_used) >= 2
    assert len(set(client_order_ids_used)) == 1
