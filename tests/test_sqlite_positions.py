import pytest

from btc_kalshi.db.sqlite_manager import SQLiteStateManager


@pytest.mark.asyncio
async def test_open_position(state_manager: SQLiteStateManager):
    await state_manager.open_position(
        position_id="pos-1",
        contract_id="BTC-2025",
        contract_title="BTC > 60000?",
        expiry_ts="2025-01-01T00:00:00Z",
        side="YES",
        entry_order_client_id="ord-1",
        entry_price_intended=0.45,
        entry_price_filled=0.46,
        stop_price=0.10,
        take_profit_price=0.90,
        intended_size=5,
        opened_ts="2024-12-31T12:00:00Z",
    )

    pos = await state_manager.get_position("pos-1", mode="live")
    assert pos is not None
    assert pos["position_id"] == "pos-1"
    assert pos["mode"] == "live"
    assert pos["status"] == "OPEN"
    assert pos["intended_size"] == 5


@pytest.mark.asyncio
async def test_get_open_positions_excludes_closed(
    state_manager: SQLiteStateManager,
):
    await state_manager.open_position(
        position_id="pos-open",
        contract_id="BTC-2025",
        contract_title="BTC > 60000?",
        expiry_ts="2025-01-01T00:00:00Z",
        side="YES",
        entry_order_client_id="ord-open",
        entry_price_intended=0.45,
        entry_price_filled=0.45,
        stop_price=0.10,
        take_profit_price=0.90,
        intended_size=3,
        opened_ts="2024-12-31T12:00:00Z",
    )
    await state_manager.open_position(
        position_id="pos-closed",
        contract_id="BTC-2025",
        contract_title="BTC > 60000?",
        expiry_ts="2025-01-01T00:00:00Z",
        side="YES",
        entry_order_client_id="ord-closed",
        entry_price_intended=0.50,
        entry_price_filled=0.50,
        stop_price=0.10,
        take_profit_price=0.90,
        intended_size=2,
        opened_ts="2024-12-31T12:05:00Z",
    )

    await state_manager.close_position("pos-closed", mode="live")

    open_positions = await state_manager.get_open_positions(mode="live")
    ids = {p["position_id"] for p in open_positions}

    assert "pos-open" in ids
    assert "pos-closed" not in ids


@pytest.mark.asyncio
async def test_count_open_positions(state_manager: SQLiteStateManager):
    await state_manager.open_position(
        position_id="pos-1",
        contract_id="BTC-2025",
        contract_title="BTC > 60000?",
        expiry_ts="2025-01-01T00:00:00Z",
        side="YES",
        entry_order_client_id="ord-1",
        entry_price_intended=0.45,
        entry_price_filled=0.45,
        stop_price=0.10,
        take_profit_price=0.90,
        intended_size=1,
        opened_ts="2024-12-31T12:00:00Z",
    )
    await state_manager.open_position(
        position_id="pos-2",
        contract_id="BTC-2025",
        contract_title="BTC > 60000?",
        expiry_ts="2025-01-01T00:00:00Z",
        side="NO",
        entry_order_client_id="ord-2",
        entry_price_intended=0.55,
        entry_price_filled=0.55,
        stop_price=0.10,
        take_profit_price=0.90,
        intended_size=2,
        opened_ts="2024-12-31T12:05:00Z",
    )

    count = await state_manager.count_open_positions(mode="live")
    assert count == 2


@pytest.mark.asyncio
async def test_has_expiry_conflict_true(state_manager: SQLiteStateManager):
    await state_manager.open_position(
        position_id="pos-1",
        contract_id="BTC-2025",
        contract_title="BTC > 60000?",
        expiry_ts="2025-01-01T00:00:00Z",
        side="YES",
        entry_order_client_id="ord-1",
        entry_price_intended=0.45,
        entry_price_filled=0.45,
        stop_price=0.10,
        take_profit_price=0.90,
        intended_size=1,
        opened_ts="2024-12-31T12:00:00Z",
    )

    assert await state_manager.has_expiry_conflict(
        expiry_ts="2025-01-01T00:00:00Z",
        side="YES",
        mode="live",
    )


@pytest.mark.asyncio
async def test_has_expiry_conflict_false(state_manager: SQLiteStateManager):
    await state_manager.open_position(
        position_id="pos-1",
        contract_id="BTC-2025",
        contract_title="BTC > 60000?",
        expiry_ts="2025-01-01T00:00:00Z",
        side="YES",
        entry_order_client_id="ord-1",
        entry_price_intended=0.45,
        entry_price_filled=0.45,
        stop_price=0.10,
        take_profit_price=0.90,
        intended_size=1,
        opened_ts="2024-12-31T12:00:00Z",
    )

    # Different expiry should not conflict.
    assert not await state_manager.has_expiry_conflict(
        expiry_ts="2025-01-02T00:00:00Z",
        side="YES",
        mode="live",
    )


@pytest.mark.asyncio
async def test_mode_isolation(state_manager: SQLiteStateManager):
    # Live position
    await state_manager.open_position(
        position_id="live-pos",
        contract_id="BTC-2025",
        contract_title="BTC > 60000?",
        expiry_ts="2025-01-01T00:00:00Z",
        side="YES",
        entry_order_client_id="ord-live",
        entry_price_intended=0.45,
        entry_price_filled=0.45,
        stop_price=0.10,
        take_profit_price=0.90,
        intended_size=1,
        opened_ts="2024-12-31T12:00:00Z",
        mode="live",
    )
    # Paper position
    await state_manager.open_position(
        position_id="paper-pos",
        contract_id="BTC-2025",
        contract_title="BTC > 60000?",
        expiry_ts="2025-01-01T00:00:00Z",
        side="YES",
        entry_order_client_id="ord-paper",
        entry_price_intended=0.45,
        entry_price_filled=0.45,
        stop_price=0.10,
        take_profit_price=0.90,
        intended_size=1,
        opened_ts="2024-12-31T12:00:00Z",
        mode="paper",
    )

    live_positions = await state_manager.get_open_positions(mode="live")
    paper_positions = await state_manager.get_open_positions(mode="paper")

    live_ids = {p["position_id"] for p in live_positions}
    paper_ids = {p["position_id"] for p in paper_positions}

    assert "live-pos" in live_ids
    assert "paper-pos" not in live_ids
    assert "paper-pos" in paper_ids
    assert "live-pos" not in paper_ids


@pytest.mark.asyncio
async def test_create_and_update_order(state_manager: SQLiteStateManager):
    await state_manager.create_order(
        client_order_id="ord-1",
        position_id="pos-1",
        contract_id="BTC-2025",
        purpose="entry",
        side="YES",
        intended_price=0.45,
        intended_size=5,
        created_ts="2024-12-31T12:00:00Z",
    )

    await state_manager.update_order(
        client_order_id="ord-1",
        mode="live",
        current_status="FILLED",
        filled_price=0.46,
        filled_size=5,
        last_update_ts="2024-12-31T12:01:00Z",
    )

    order = await state_manager.get_order("ord-1", mode="live")
    assert order is not None
    assert order["client_order_id"] == "ord-1"
    assert order["current_status"] == "FILLED"
    assert order["filled_size"] == 5
    assert order["filled_price"] == 0.46
    assert order["last_update_ts"] == "2024-12-31T12:01:00Z"


@pytest.mark.asyncio
async def test_get_orders_for_position(state_manager: SQLiteStateManager):
    await state_manager.create_order(
        client_order_id="ord-a",
        position_id="pos-1",
        contract_id="BTC-2025",
        purpose="entry",
        side="YES",
        intended_price=0.45,
        intended_size=5,
        created_ts="2024-12-31T12:00:00Z",
    )
    await state_manager.create_order(
        client_order_id="ord-b",
        position_id="pos-1",
        contract_id="BTC-2025",
        purpose="take-profit",
        side="SELL",
        intended_price=0.90,
        intended_size=5,
        created_ts="2024-12-31T12:05:00Z",
    )
    await state_manager.create_order(
        client_order_id="ord-other",
        position_id="pos-2",
        contract_id="BTC-OTHER",
        purpose="entry",
        side="YES",
        intended_price=0.30,
        intended_size=1,
        created_ts="2024-12-31T12:10:00Z",
    )

    orders = await state_manager.get_orders_for_position("pos-1", mode="live")
    ids = [o["client_order_id"] for o in orders]

    assert ids == ["ord-a", "ord-b"]

