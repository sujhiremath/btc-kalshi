"""
Mode-aware position sizing and exposure caps. Live and paper each have a $90 cap.
"""
from __future__ import annotations

from math import floor
from typing import TYPE_CHECKING, Any

from btc_kalshi.core.constants import (
    MAX_OPEN_EXPOSURE,
    MAX_OPEN_POSITIONS,
    calculate_position_size as _base_position_size,
)

if TYPE_CHECKING:
    from btc_kalshi.db.sqlite_manager import SQLiteStateManager


class PositionSizer:
    """
    Mode-aware position sizer: calculate_size, max loss, can_open_position (count < 3,
    no expiry conflict, exposure < $90), and get_current_exposure. Live and paper
    each have their own $90 exposure cap.
    """

    def __init__(self, db: SQLiteStateManager | None) -> None:
        self._db = db

    def calculate_size(
        self,
        bankroll: float,
        entry: float,
        stop: float,
        multiplier: float,
    ) -> int:
        """Base size from bankroll/entry/stop, then scaled by multiplier (floor)."""
        base = _base_position_size(bankroll, entry, stop)
        if base <= 0:
            return 0
        return max(0, floor(base * multiplier))

    def calculate_max_loss(self, entry: float, stop: float, size: int) -> float:
        """Max loss in dollars if position moves from entry to stop (contracts at $1)."""
        if size <= 0 or entry <= stop:
            return 0.0
        return (entry - stop) * size

    async def get_current_exposure(self, mode: str = "live") -> float:
        """Sum of max loss (entry-to-stop) for all open positions in the given mode."""
        if self._db is None:
            return 0.0
        positions = await self._db.get_open_positions(mode=mode)
        total = 0.0
        for p in positions:
            entry = p.get("entry_price_filled") or p.get("entry_price_intended") or 0.0
            stop = p.get("stop_price") or 0.0
            size = p.get("filled_size") or p.get("intended_size") or 0
            total += self.calculate_max_loss(entry, stop, int(size))
        return total

    async def can_open_position(
        self,
        entry: float,
        stop: float,
        size: int,
        expiry_ts: str,
        mode: str = "live",
        side: str = "YES",
    ) -> bool:
        """
        True if: open count < 3, no expiry conflict, and current exposure + new
        max loss stays under $90 for that mode.
        """
        if self._db is None:
            return False
        count = await self._db.count_open_positions(mode=mode)
        if count >= MAX_OPEN_POSITIONS:
            return False
        if await self._db.has_expiry_conflict(expiry_ts=expiry_ts, side=side, mode=mode):
            return False
        current = await self.get_current_exposure(mode=mode)
        new_loss = self.calculate_max_loss(entry, stop, size)
        if current + new_loss >= MAX_OPEN_EXPOSURE:
            return False
        return True
