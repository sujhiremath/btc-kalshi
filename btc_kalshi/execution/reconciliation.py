"""
Reconciliation: LIVE only. Compare real Kalshi positions vs SQLite mode='live'.
Unknown (on Kalshi, not local) → close on exchange. Gap (local, not on Kalshi) → log, fail.
Match → load (sync). Bot can't become READY until reconcile() passes.
"""
from __future__ import annotations

from typing import Any

from btc_kalshi.core.logger import get_logger

LIVE_MODE = "live"


def _kalshi_key(p: dict[str, Any]) -> str:
    """Canonical key for matching Kalshi position."""
    c = (p.get("ticker") or p.get("contract_id") or "").strip()
    side = (p.get("side") or "yes").lower()
    return f"{c}:{side}"


def _local_key(p: dict[str, Any]) -> str:
    """Canonical key for local position."""
    c = (p.get("contract_id") or "").strip()
    side = (p.get("side") or "YES").lower()
    return f"{c}:{side}"


async def _close_position_on_exchange(exchange: Any, p: dict[str, Any]) -> None:
    """Market-close a position on the exchange (for unknown)."""
    contract_id = p.get("ticker") or p.get("contract_id") or ""
    size = int(p.get("position") or p.get("size") or 0)
    side = (p.get("side") or "yes").lower()
    exit_side = "no" if side == "yes" else "yes"
    if contract_id and size > 0:
        await exchange.place_order(
            contract_id=contract_id,
            side=exit_side,
            count=size,
            type="market",
        )


class Reconciler:
    """
    Reconcile Kalshi (exchange) vs SQLite live positions. Only applies to LIVE.
    Returns True only when no gaps; unknowns are closed on exchange.
    """

    def __init__(self, exchange: Any, sqlite_manager: Any) -> None:
        self._exchange = exchange
        self._db = sqlite_manager
        self._logger = get_logger("reconciler")

    async def reconcile(self) -> bool:
        """
        Match Kalshi vs SQLite live positions. Unknown → close on exchange.
        Gap (local not on Kalshi) → log and return False. Match → load (sync).
        Bot can't become READY until this passes (returns True).
        """
        kalshi_pos = await self._exchange.get_positions()
        local_pos = await self._db.get_open_positions(mode=LIVE_MODE)

        k_keys = {_kalshi_key(p): p for p in kalshi_pos}
        l_keys = {_local_key(p): p for p in local_pos}

        # Unknown: on Kalshi but not in local → close on exchange
        for key, p in list(k_keys.items()):
            if key not in l_keys:
                self._logger.warning(
                    "Reconcile: unknown position on Kalshi, closing",
                    extra={"contract_id": p.get("ticker") or p.get("contract_id"), "key": key},
                )
                await _close_position_on_exchange(self._exchange, p)

        # Gap: in local but not on Kalshi → log and fail
        for key in l_keys:
            if key not in k_keys:
                self._logger.error(
                    "Reconcile: gap — local position not on Kalshi",
                    extra={"key": key, "mode": LIVE_MODE},
                )
                return False

        # Match: both have same keys → load (sync) — no-op for now beyond pass
        return True
