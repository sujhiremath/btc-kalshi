"""
Tests for FillSimulator and PaperExchangeAdapter. Paper uses real orderbook from
universe manager; fills simulated locally. Balance = starting - exposure + realized P&L.
"""
from unittest.mock import MagicMock, patch

import pytest

from btc_kalshi.exchange.exchange_protocol import ExchangeProtocol
from btc_kalshi.exchange.fill_simulator import FillSimulator
from btc_kalshi.exchange.paper_adapter import PaperExchangeAdapter


# --- FillSimulator (shared) ---


def test_implements_protocol():
    """PaperExchangeAdapter implements ExchangeProtocol."""
    um = MagicMock()
    um.get_universe.return_value = []
    um.get_orderbook.return_value = None
    adapter = PaperExchangeAdapter(
        universe_manager=um,
        fill_simulator=FillSimulator(),
        starting_balance=100.0,
    )
    assert isinstance(adapter, ExchangeProtocol)


@pytest.mark.asyncio
async def test_entry_fill_uses_real_orderbook():
    """place_order fetches orderbook from universe_manager and uses it for fill."""
    um = MagicMock()
    um.get_universe.return_value = [
        {"ticker": "BTC-20250115", "id": "BTC-20250115", "title": "BTC > 100k?"},
    ]
    # Real orderbook shape: asks with price (cents), quantity
    um.get_orderbook.return_value = {
        "asks": [{"price": 52, "quantity": 100}, {"price": 53, "quantity": 200}],
        "bids": [{"price": 50, "quantity": 80}],
    }
    sim = FillSimulator()
    adapter = PaperExchangeAdapter(universe_manager=um, fill_simulator=sim, starting_balance=100.0)

    result = await adapter.place_order(
        contract_id="BTC-20250115",
        side="yes",
        count=10,
        price_cents=53,
        type="limit",
    )

    um.get_orderbook.assert_called_with("BTC-20250115")
    assert result.get("order", {}).get("status") == "filled"


def test_partial_fill_thin_book():
    """When book_depth_3c < 50, entry fill is 60% partial."""
    sim = FillSimulator()
    # depth 30 < 50 → partial
    result = sim.simulate_entry_fill(ask_price=0.52, book_depth_3c=30, elapsed_seconds=10)
    assert result["filled"] is True
    assert result.get("fill_size_pct") == 0.6


def test_no_fill_after_90s():
    """When elapsed_seconds > 90, no fill."""
    sim = FillSimulator()
    result = sim.simulate_entry_fill(ask_price=0.52, book_depth_3c=100, elapsed_seconds=91)
    assert result["filled"] is False


def test_exit_slippage_applied():
    """Exit fill applies EXIT_SLIPPAGE_BUFFER (2¢)."""
    sim = FillSimulator()
    result = sim.simulate_exit_fill(bid_price=0.55, exit_type="market")
    assert result["fill_price"] == pytest.approx(0.53, abs=0.001)
    assert result.get("slippage") == 0.02


@pytest.mark.asyncio
async def test_balance_tracks_pnl():
    """get_balance() = starting - exposure + realized P&L."""
    um = MagicMock()
    um.get_universe.return_value = [{"ticker": "BTC-20250115", "id": "BTC-20250115"}]
    um.get_orderbook.return_value = {
        "asks": [{"price": 50, "quantity": 200}],
        "bids": [{"price": 48, "quantity": 200}],
    }
    adapter = PaperExchangeAdapter(universe_manager=um, fill_simulator=FillSimulator(), starting_balance=100.0)

    await adapter.place_order(contract_id="BTC-20250115", side="yes", count=10, price_cents=51, type="limit")
    balance = await adapter.get_balance()
    # After one entry: exposure = 10 * 0.51 = 5.1, realized = 0 → balance = 100 + 0 - 5.1 = 94.9
    assert balance["balance"] == pytest.approx(94.9, abs=0.02)


def test_pnl_uses_shared_fee_function():
    """calculate_simulated_pnl uses calculate_fee from constants."""
    sim = FillSimulator()
    with patch("btc_kalshi.exchange.fill_simulator.calculate_fee") as m_fee:
        m_fee.return_value = 0.5
        pnl = sim.calculate_simulated_pnl(entry=0.50, exit_price=0.90, contracts=10, won=True)
        m_fee.assert_called_once_with(0.50, 0.90, 10, True)
        assert pnl == pytest.approx((0.90 - 0.50) * 10 - 0.5, abs=0.01)
