"""
Fill simulator: shared between paper trading and future backtesting.
Uses real orderbook data; simulates entry/exit fills and P&L with shared fee model.
"""
from __future__ import annotations

import random
from typing import Any, Dict

from btc_kalshi.core.constants import EXIT_SLIPPAGE_BUFFER, MIN_FILL_PCT, calculate_fee


class FillSimulator:
    """
    0-45s → ask+1¢, 45-90s → ask+2¢, >90s → no fill.
    <50 book_depth_3c → 60% partial. Exit: apply EXIT_SLIPPAGE_BUFFER (2¢).
    """

    def simulate_entry_fill(
        self,
        ask_price: float,
        book_depth_3c: int,
        elapsed_seconds: float,
    ) -> Dict[str, Any]:
        """
        Returns {filled, fill_price, fill_size_pct, latency_ms}.
        0-45s → fill at ask+1¢; 45-90s → ask+2¢; >90s → no fill.
        book_depth_3c < 50 → fill_size_pct = 0.6.
        """
        if elapsed_seconds > 90:
            return {
                "filled": False,
                "fill_price": ask_price,
                "fill_size_pct": 0.0,
                "latency_ms": 0,
            }
        if elapsed_seconds <= 45:
            fill_price = ask_price + 0.01
        else:
            fill_price = ask_price + 0.02
        fill_size_pct = MIN_FILL_PCT if book_depth_3c < 50 else 1.0
        latency_ms = random.randint(40, 120) if (elapsed_seconds <= 90) else 0
        return {
            "filled": True,
            "fill_price": fill_price,
            "fill_size_pct": fill_size_pct,
            "latency_ms": latency_ms,
        }

    def simulate_exit_fill(self, bid_price: float, exit_type: str = "market") -> Dict[str, Any]:
        """Apply EXIT_SLIPPAGE_BUFFER (2¢). Returns {fill_price, slippage}."""
        slippage = EXIT_SLIPPAGE_BUFFER
        fill_price = max(0.0, bid_price - slippage)
        return {"fill_price": fill_price, "slippage": slippage}

    def calculate_simulated_pnl(
        self,
        entry: float,
        exit_price: float,
        contracts: int,
        won: bool,
    ) -> float:
        """Uses calculate_fee from constants."""
        if won:
            gross = (exit_price - entry) * contracts
        else:
            gross = -entry * contracts
        fee = calculate_fee(entry, exit_price, contracts, won)
        return gross - fee
