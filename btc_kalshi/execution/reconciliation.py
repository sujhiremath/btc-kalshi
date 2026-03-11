"""
Reconciliation: LIVE only. Compare real Kalshi positions vs SQLite mode='live'.
Unknown (on Kalshi, not local) → close on exchange. Gap (local, not on Kalshi) → log, fail.
Match → load (sync). Bot can't become READY until reconcile() passes.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any, Callable

import httpx

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


async def _close_position_on_exchange(exchange: Any, p: dict[str, Any], logger: Any) -> None:
    """Market-close a position on the exchange (for unknown). Sell the position side."""
    contract_id = p.get("ticker") or p.get("contract_id") or ""
    size = int(p.get("position") or p.get("size") or 0)
    side = (p.get("side") or "yes").lower()
    if not contract_id or size <= 0:
        return
    try:
        await exchange.place_order(
            contract_id=contract_id,
            side=side,
            count=size,
            type="market",
            action="sell",
        )
    except httpx.HTTPStatusError as e:
        logger.warning(
            "Reconcile: could not close position on exchange",
            extra={"contract_id": contract_id, "status": e.response.status_code, "body": e.response.text},
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
                await _close_position_on_exchange(self._exchange, p, self._logger)

        # Gap: in local but not on Kalshi
        # If the contract has already expired → auto-close locally (bot was down at expiry)
        # If not expired → genuine gap, fail so operator can investigate
        now = datetime.now(timezone.utc)
        for key, p in l_keys.items():
            if key not in k_keys:
                expiry_ts = p.get("expiry_ts")
                is_expired = False
                if expiry_ts:
                    try:
                        exp = datetime.fromisoformat(expiry_ts)
                        if exp.tzinfo is None:
                            exp = exp.replace(tzinfo=timezone.utc)
                        is_expired = exp < now
                    except ValueError:
                        pass

                if is_expired:
                    self._logger.warning(
                        "Reconcile: gap — contract expired while bot was down, auto-closing locally",
                        extra={"key": key, "expiry_ts": expiry_ts},
                    )
                    await self._db.close_position(p["position_id"], mode=LIVE_MODE)
                else:
                    self._logger.error(
                        "Reconcile: gap — local position not on Kalshi",
                        extra={"key": key, "mode": LIVE_MODE},
                    )
                    return False

        # Match: both have same keys → load (sync) — no-op for now beyond pass
        return True

    async def run_reconciliation_loop(
        self,
        db: Any,
        interval_seconds: int = 300,
        get_shutdown: Callable[[], bool] = lambda: False,
    ) -> None:
        """
        Periodic live reconciliation. Sleeps interval_seconds, then reconciles and
        writes last_reconciliation_ts to db. Runs until get_shutdown() returns True.
        """
        while not get_shutdown():
            try:
                await asyncio.sleep(interval_seconds)
                if get_shutdown():
                    break
                passed = await self.reconcile()
                now_ts = datetime.now(timezone.utc).isoformat()
                await db.update_bot_state(mode=LIVE_MODE, last_reconciliation_ts=now_ts)
                self._logger.info("Periodic reconcile", extra={"passed": passed})
            except asyncio.CancelledError:
                break
            except Exception as e:
                self._logger.exception("reconciliation_loop: %s", e)
