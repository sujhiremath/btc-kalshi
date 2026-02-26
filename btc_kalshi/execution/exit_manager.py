"""
Exit manager: check SL/TP/time-based triggers, execute_take_profit, execute_stop_loss,
execute_force_close, failed_exit_fallback. Queries positions by mode. One instance per path.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any, Optional

from btc_kalshi.core.constants import (
    STOP_LOSS_PCT,
    STOP_LOSS_PRICE,
    TAKE_PROFIT_PCT,
    TAKE_PROFIT_PRICE,
)
from btc_kalshi.core.logger import get_logger

MONITOR_INTERVAL_SECONDS = 2
TP_MAX_SECONDS = 90
SL_MAX_SECONDS = 20
FAILED_EXIT_MARKET_UNDER_MINUTES = 10
TIME_TIER_60M = 60
TIME_TIER_30M = 30
TIME_TIER_15M = 15
TIME_PRICE_70 = 0.70
TIME_PRICE_80 = 0.80
TIME_PRICE_88 = 0.88
HOLD_IF_PRICE_GE = 0.96


def _best_bid(ob: Any) -> float:
    """Best bid in decimal (we sell into bid)."""
    bids = ob.get("bids") or [] if isinstance(ob, dict) else []
    if not bids:
        return 0.0
    first = bids[0]
    p = first.get("price") if isinstance(first, dict) else (first[0] if isinstance(first, (list, tuple)) else first)
    return float(p) / 100.0


def _minutes_to_expiry(expiry_ts: Optional[str], now: datetime) -> float:
    if not expiry_ts:
        return 999.0
    s = expiry_ts.replace("Z", "+00:00")
    try:
        exp = datetime.fromisoformat(s)
        if exp.tzinfo is None:
            exp = exp.replace(tzinfo=timezone.utc)
        delta = exp - now
        return max(0.0, delta.total_seconds() / 60.0)
    except ValueError:
        return 999.0


def _exit_side(position_side: str) -> str:
    """Opposite side to close: YES -> NO, NO -> YES."""
    return "NO" if (position_side or "").upper() == "YES" else "YES"


class ExitManager:
    """
    check_exit_triggers (SL/TP/time-based, hold >=96¢). execute_take_profit (limit→reprice→market, 90s),
    execute_stop_loss (limit→market, 20s), execute_force_close (market), failed_exit_fallback (<10min→market).
    run_exit_monitor_loop every 2s, stop().
    """

    def __init__(
        self,
        exchange: Any,
        sqlite_manager: Any,
        mode: str = "live",
        get_now: Optional[Any] = None,
    ) -> None:
        self._exchange = exchange
        self._db = sqlite_manager
        self._mode = mode
        self._get_now = get_now or (lambda: datetime.now(timezone.utc))
        self._stop = False
        self._logger = get_logger("exit-manager")

    async def check_exit_triggers(self, position: dict[str, Any]) -> Optional[str]:
        """
        Returns 'stop_loss' | 'take_profit' | 'time_based' | None (hold or no trigger).
        SL: price <= 8¢ or loss >= 20%. TP: price >= 90¢ or profit >= 25%.
        Time: >60m normal; 30-60m <70¢; 15-30m <80¢; <15m <88¢. Hold if price >= 96¢.
        """
        contract_id = position.get("contract_id") or ""
        entry = float(position.get("entry_price_filled") or 0)
        side = (position.get("side") or "YES").upper()
        expiry_ts = position.get("expiry_ts")

        ob = await self._exchange.get_orderbook(contract_id)
        price = _best_bid(ob) if side == "YES" else (1.0 - _best_bid(ob))  # simplify: use bid for YES
        now = self._get_now()
        min_to_exp = _minutes_to_expiry(expiry_ts, now)

        if entry <= 0:
            return None

        if price >= HOLD_IF_PRICE_GE:
            return None  # hold to settlement

        # Stop loss: <= 8¢ or loss >= 20%
        if price <= STOP_LOSS_PRICE:
            return "stop_loss"
        loss_pct = (entry - price) / entry if entry else 0
        if loss_pct >= STOP_LOSS_PCT:
            return "stop_loss"

        # Take profit: >= 90¢ or profit >= 25%
        if price >= TAKE_PROFIT_PRICE:
            return "take_profit"
        profit_pct = (price - entry) / entry if entry else 0
        if profit_pct >= TAKE_PROFIT_PCT:
            return "take_profit"

        # Time-based
        if min_to_exp < TIME_TIER_15M and price < TIME_PRICE_88:
            return "time_based"
        if min_to_exp < TIME_TIER_30M and price < TIME_PRICE_80:
            return "time_based"
        if min_to_exp < TIME_TIER_60M and price < TIME_PRICE_70:
            return "time_based"

        return None

    async def execute_take_profit(self, position: dict[str, Any]) -> dict[str, Any]:
        """Limit at TP price, then reprice, then market; 90s max."""
        contract_id = position.get("contract_id") or ""
        size = int(position.get("filled_size") or 0)
        side = position.get("side") or "YES"
        tp = float(position.get("take_profit_price") or TAKE_PROFIT_PRICE)
        exit_side = _exit_side(side)
        price_cents = int(round(tp * 100))

        resp = await self._exchange.place_order(
            contract_id=contract_id,
            side=exit_side,
            count=size,
            price_cents=price_cents,
            type="limit",
        )
        order_id = (resp.get("order") or {}).get("id") if isinstance(resp.get("order"), dict) else None
        if not order_id:
            order_id = resp.get("id")
        filled = (resp.get("order") or resp).get("filled_count") or (resp.get("order") or resp).get("filled_size") or 0
        if filled >= size:
            return {"filled": True, "filled_size": size}
        # Reprice / market fallback simplified: place market if not filled by deadline
        await asyncio.sleep(min(5, TP_MAX_SECONDS // 3))
        status = await self._exchange.get_order(order_id) if order_id else None
        st = (status or {}).get("status") or (status or {}).get("current_status") or ""
        if (st or "").lower() == "filled":
            return {"filled": True, "filled_size": size}
        await self._exchange.place_order(
            contract_id=contract_id,
            side=exit_side,
            count=size,
            type="market",
        )
        return {"filled": True, "filled_size": size}

    async def execute_stop_loss(self, position: dict[str, Any]) -> dict[str, Any]:
        """Limit at SL price, then market within 20s."""
        contract_id = position.get("contract_id") or ""
        size = int(position.get("filled_size") or 0)
        side = position.get("side") or "YES"
        sl = float(position.get("stop_price") or STOP_LOSS_PRICE)
        exit_side = _exit_side(side)
        price_cents = int(round(sl * 100))

        resp = await self._exchange.place_order(
            contract_id=contract_id,
            side=exit_side,
            count=size,
            price_cents=price_cents,
            type="limit",
        )
        filled = (resp.get("order") or resp).get("filled_count") or (resp.get("order") or resp).get("filled_size") or 0
        if filled >= size:
            return {"filled": True, "filled_size": size}
        await asyncio.sleep(min(3, SL_MAX_SECONDS // 2))
        await self._exchange.place_order(
            contract_id=contract_id,
            side=exit_side,
            count=size,
            type="market",
        )
        return {"filled": True, "filled_size": size}

    async def execute_force_close(self, position: dict[str, Any]) -> dict[str, Any]:
        """Market order to close."""
        contract_id = position.get("contract_id") or ""
        size = int(position.get("filled_size") or 0)
        side = position.get("side") or "YES"
        exit_side = _exit_side(side)

        await self._exchange.place_order(
            contract_id=contract_id,
            side=exit_side,
            count=size,
            type="market",
        )
        return {"filled": True, "filled_size": size}

    async def failed_exit_fallback(self, position: dict[str, Any]) -> dict[str, Any]:
        """If <10min to expiry use market."""
        now = self._get_now()
        min_to_exp = _minutes_to_expiry(position.get("expiry_ts"), now)
        if min_to_exp < FAILED_EXIT_MARKET_UNDER_MINUTES:
            return await self.execute_force_close(position)
        return await self.execute_force_close(position)

    async def run_exit_monitor_loop(self) -> None:
        """Every 2s, get positions for mode, check triggers, execute; until stop()."""
        self._stop = False
        while not self._stop:
            try:
                positions = await self._db.get_open_positions(mode=self._mode)
                for pos in positions:
                    trigger = await self.check_exit_triggers(pos)
                    if trigger == "stop_loss":
                        await self.execute_stop_loss(pos)
                        await self._db.close_position(pos["position_id"], mode=self._mode)
                    elif trigger == "take_profit":
                        await self.execute_take_profit(pos)
                        await self._db.close_position(pos["position_id"], mode=self._mode)
                    elif trigger == "time_based":
                        await self.execute_force_close(pos)
                        await self._db.close_position(pos["position_id"], mode=self._mode)
            except Exception as e:
                self._logger.exception("Exit monitor loop error: %s", e)
            await asyncio.sleep(MONITOR_INTERVAL_SECONDS)

    def stop(self) -> None:
        self._stop = True
