"""
Tests for PositionSizer: sizing, max loss, can_open_position, exposure (mode-aware).
"""
import pytest

from btc_kalshi.risk.position_sizer import PositionSizer


def test_normal_sizing(state_manager):
    """calculate_size returns a positive integer for valid inputs."""
    sizer = PositionSizer(db=state_manager)
    size = sizer.calculate_size(bankroll=1000.0, entry=0.50, stop=0.10, multiplier=1.0)
    assert isinstance(size, int)
    assert size >= 1


def test_size_multiplier_reduces(state_manager):
    """Lower multiplier reduces contract size."""
    sizer = PositionSizer(db=state_manager)
    full = sizer.calculate_size(bankroll=1000.0, entry=0.50, stop=0.10, multiplier=1.0)
    half = sizer.calculate_size(bankroll=1000.0, entry=0.50, stop=0.10, multiplier=0.5)
    assert half <= full
    assert half >= 0


def test_max_loss_calculation(state_manager):
    """calculate_max_loss(entry, stop, size) = (entry - stop) * size in dollars."""
    sizer = PositionSizer(db=state_manager)
    loss = sizer.calculate_max_loss(entry=0.50, stop=0.10, size=10)
    assert loss == pytest.approx(4.0)  # (0.50 - 0.10) * 10


@pytest.mark.asyncio
async def test_can_open_within_limits(state_manager):
    """When count < 3, no expiry conflict, and exposure within cap, can open."""
    sizer = PositionSizer(db=state_manager)
    ok = await sizer.can_open_position(
        entry=0.50, stop=0.10, size=5, expiry_ts="2025-01-15T00:00:00Z", mode="live"
    )
    assert ok is True


@pytest.mark.asyncio
async def test_blocked_by_position_limit(state_manager):
    """When already 3 open positions, can_open_position returns False."""
    sizer = PositionSizer(db=state_manager)
    for i in range(3):
        await state_manager.open_position(
            position_id=f"pos-{i}",
            contract_id=f"BTC-2025-{i}",
            contract_title="BTC > 60k?",
            expiry_ts="2025-01-01T00:00:00Z",
            side="YES",
            entry_order_client_id=f"ord-{i}",
            entry_price_intended=0.45,
            entry_price_filled=0.45,
            stop_price=0.10,
            take_profit_price=0.90,
            intended_size=1,
            opened_ts="2024-12-31T12:00:00Z",
        )
    ok = await sizer.can_open_position(
        entry=0.50, stop=0.10, size=1, expiry_ts="2025-01-20T00:00:00Z", mode="live"
    )
    assert ok is False


@pytest.mark.asyncio
async def test_mode_isolation(state_manager):
    """Three paper positions do not block opening a live position."""
    for i in range(3):
        await state_manager.open_position(
            position_id=f"paper-pos-{i}",
            contract_id=f"BTC-2025-{i}",
            contract_title="BTC > 60k?",
            expiry_ts="2025-01-01T00:00:00Z",
            side="YES",
            entry_order_client_id=f"paper-ord-{i}",
            entry_price_intended=0.45,
            entry_price_filled=0.45,
            stop_price=0.10,
            take_profit_price=0.90,
            intended_size=1,
            opened_ts="2024-12-31T12:00:00Z",
            mode="paper",
        )
    sizer = PositionSizer(db=state_manager)
    ok = await sizer.can_open_position(
        entry=0.50, stop=0.10, size=1, expiry_ts="2025-01-20T00:00:00Z", mode="live"
    )
    assert ok is True
    exposure_live = await sizer.get_current_exposure(mode="live")
    assert exposure_live == 0.0
