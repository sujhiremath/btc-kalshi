"""
Exchange-agnostic order manager: persist before send, client order IDs, ambiguous retry.
Accepts any ExchangeProtocol (KalshiClient live, PaperExchangeAdapter paper). Mode tags SQLite records.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any, Callable, Optional

from btc_kalshi.core.constants import (
    ENTRY_REPRICE_OFFSET,
    ENTRY_WAIT_SECONDS,
    MIN_FILL_PCT,
    REPRICE_AFTER_SECONDS,
)
from btc_kalshi.core.logger import get_logger

AMBIGUOUS_WAIT_SECONDS = 5


def generate_client_order_id(
    contract_id: str,
    signal_ts: datetime,
    side: str,
    mode: str = "live",
) -> str:
    """Format: {mode}-{contract_id}-{ts_int}-{side}."""
    ts_int = int(signal_ts.timestamp() * 1000)
    return f"{mode}-{contract_id}-{ts_int}-{side}"


def _best_ask_from_orderbook(ob: Any) -> float:
    """Best ask in decimal (e.g. 0.52). Orderbook asks: list of dicts with price in cents or list [price, qty]."""
    asks = ob.get("asks") or [] if isinstance(ob, dict) else []
    if not asks:
        return 1.0
    first = asks[0]
    p = first.get("price") if isinstance(first, dict) else (first[0] if isinstance(first, (list, tuple)) else first)
    return float(p) / 100.0


def _order_id_from_response(resp: Any) -> Optional[str]:
    """Extract exchange order id from place_order/get_order response. None if ambiguous."""
    if not resp or not isinstance(resp, dict):
        return None
    order = resp.get("order") if isinstance(resp.get("order"), dict) else resp
    if not isinstance(order, dict):
        return None
    return order.get("id") or order.get("order_id")


class OrderManager:
    """
    Persist order BEFORE sending to exchange. On ambiguous response: query by client_order_id,
    wait 5s, retry same ID. Uses mode to tag SQLite records.
    """

    def __init__(
        self,
        exchange: Any,
        sqlite_manager: Any,
        event_logger: Optional[Any] = None,
        mode: str = "live",
        get_now: Optional[Callable[[], datetime]] = None,
    ) -> None:
        self._exchange = exchange
        self._db = sqlite_manager
        self._event_logger = event_logger
        self._mode = mode
        self._get_now = get_now or (lambda: datetime.now(timezone.utc))
        self._logger = get_logger("order-manager")

    async def place_entry_order(self, signal: Any, size: int) -> Optional[dict[str, Any]]:
        """
        Persist order to SQLite first, then send to exchange. On ambiguous response,
        wait 5s, get_order(client_order_id); if not found, retry place with same client_order_id.
        """
        contract_id = getattr(signal, "contract_id", "")
        side = getattr(signal, "side", "YES")
        entry_price = getattr(signal, "entry_price", 0.5)
        signal_ts = getattr(signal, "timestamp", datetime.now(timezone.utc))
        if hasattr(signal_ts, "isoformat"):
            created_ts = signal_ts.isoformat()
        else:
            created_ts = datetime.now(timezone.utc).isoformat()

        client_order_id = generate_client_order_id(
            contract_id, signal_ts, side, mode=self._mode
        )

        # Persist BEFORE sending
        await self._db.create_order(
            client_order_id=client_order_id,
            position_id=None,
            contract_id=contract_id,
            purpose="entry",
            side=side,
            intended_price=entry_price,
            intended_size=size,
            created_ts=created_ts,
            mode=self._mode,
        )

        price_cents = int(round(entry_price * 100))

        resp = await self._exchange.place_order(
            contract_id=contract_id,
            side=side,
            count=size,
            price_cents=price_cents,
            client_order_id=client_order_id,
        )

        order_id = _order_id_from_response(resp)
        if order_id is not None:
            await self._update_order_from_response(client_order_id, resp)
            return resp

        # Ambiguous: wait 5s, query by client_order_id
        await asyncio.sleep(AMBIGUOUS_WAIT_SECONDS)
        resp2 = await self._exchange.get_order(client_order_id)
        if resp2 and _order_id_from_response(resp2):
            await self._update_order_from_response(client_order_id, resp2)
            return resp2

        # Retry place with same client_order_id (idempotent)
        resp3 = await self._exchange.place_order(
            contract_id=contract_id,
            side=side,
            count=size,
            price_cents=price_cents,
            client_order_id=client_order_id,
        )
        order_id3 = _order_id_from_response(resp3)
        if order_id3 is not None:
            await self._update_order_from_response(client_order_id, resp3)
            return resp3

        await asyncio.sleep(AMBIGUOUS_WAIT_SECONDS)
        resp4 = await self._exchange.get_order(client_order_id)
        if resp4:
            await self._update_order_from_response(client_order_id, resp4)
            return resp4

        return resp3

    async def _update_order_from_response(self, client_order_id: str, resp: Any) -> None:
        """Update SQLite order row from exchange response if we have status."""
        order = resp.get("order") if isinstance(resp, dict) else resp
        if isinstance(order, dict):
            status = order.get("status") or order.get("current_status")
            if status:
                self._logger.info(
                    "Order status from exchange",
                    extra={"client_order_id": client_order_id, "status": status},
                )
                await self._db.update_order(
                    client_order_id,
                    mode=self._mode,
                    current_status=status,
                )

    async def cancel_order(self, client_order_id: str) -> dict[str, Any]:
        """Resolve exchange order id via get_order(client_order_id), then cancel."""
        order = await self._exchange.get_order(client_order_id)
        if order is None:
            return {"cancelled": False, "reason": "order_not_found"}
        o = order.get("order") if isinstance(order, dict) and isinstance(order.get("order"), dict) else order
        if not isinstance(o, dict):
            return {"cancelled": False, "reason": "invalid_response"}
        exchange_id = o.get("id") or o.get("order_id") or client_order_id
        result = await self._exchange.cancel_order(exchange_id)
        return result

    async def get_order_status(self, client_order_id: str) -> Optional[dict[str, Any]]:
        """Get order status from exchange by client_order_id."""
        return await self._exchange.get_order(client_order_id)

    def _parse_created_ts(self, created_ts: Optional[str]) -> datetime:
        if not created_ts:
            return datetime.now(timezone.utc)
        s = created_ts.replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(s)
        except ValueError:
            return datetime.now(timezone.utc)

    async def monitor_entry_fill(self, client_order_id: str) -> dict[str, Any]:
        """
        Phase 1 (0-45s): poll at initial price. Phase 2 (45-90s): reprice once to ask+2¢.
        Phase 3 (90s+): cancel, soft block. Partial >=60% accept, <60% cancel+optional top-up.
        """
        now = self._get_now()
        row = await self._db.get_order(client_order_id=client_order_id, mode=self._mode)
        if not row:
            return {"outcome": "error", "reason": "order_not_found"}

        contract_id = row.get("contract_id") or ""
        intended_size = int(row.get("intended_size") or 0)
        side = row.get("side") or "YES"
        created_ts = self._parse_created_ts(row.get("created_ts"))
        elapsed = (now - created_ts).total_seconds()

        ex_order = await self._exchange.get_order(client_order_id)
        if not ex_order:
            return {"outcome": "pending", "elapsed": elapsed}

        o = ex_order.get("order") if isinstance(ex_order.get("order"), dict) else ex_order
        if not isinstance(o, dict):
            return {"outcome": "pending", "elapsed": elapsed}

        status = (o.get("status") or o.get("current_status") or "").lower()
        filled = int(o.get("filled_count") or o.get("filled_size") or 0)
        if intended_size <= 0:
            intended_size = int(o.get("count") or 1)

        # Full fill
        if status == "filled" or filled >= intended_size:
            await self._db.update_order(
                client_order_id, mode=self._mode,
                current_status="filled", filled_size=filled,
            )
            return {"outcome": "filled", "filled_size": filled}

        # Partial fill: >=60% accept
        if filled >= intended_size * MIN_FILL_PCT:
            await self._db.update_order(
                client_order_id, mode=self._mode,
                current_status="filled", filled_size=filled,
            )
            return {"outcome": "filled", "filled_size": filled}

        # Partial fill <60%: cancel + optional top-up
        if filled > 0 and filled < intended_size * MIN_FILL_PCT:
            await self.cancel_order(client_order_id)
            remaining = intended_size - filled
            top_up_placed = False
            if remaining > 0 and hasattr(self._exchange, "get_orderbook"):
                ob = await self._exchange.get_orderbook(contract_id)
                best_ask = _best_ask_from_orderbook(ob)
                new_price_cents = int(round((best_ask + ENTRY_REPRICE_OFFSET) * 100))
                await self._exchange.place_order(
                    contract_id=contract_id,
                    side=side,
                    count=remaining,
                    price_cents=new_price_cents,
                    client_order_id=client_order_id + "-topup",
                )
                top_up_placed = True
            return {
                "outcome": "cancelled",
                "partial_fill_size": filled,
                "top_up_placed": top_up_placed,
            }

        # Phase 3: 90s+ cancel, soft block
        if elapsed >= ENTRY_WAIT_SECONDS:
            await self.cancel_order(client_order_id)
            return {"outcome": "cancelled", "soft_block": True}

        # Phase 2: 45-90s reprice once (ask+2¢)
        if elapsed >= REPRICE_AFTER_SECONDS:
            ob = await self._exchange.get_orderbook(contract_id)
            best_ask = _best_ask_from_orderbook(ob)
            new_price_cents = int(round((best_ask + ENTRY_REPRICE_OFFSET) * 100))
            await self.cancel_order(client_order_id)
            await self._exchange.place_order(
                contract_id=contract_id,
                side=side,
                count=intended_size,
                price_cents=new_price_cents,
                client_order_id=client_order_id,
            )
            now_iso = self._get_now().isoformat()
            await self._db.update_order(client_order_id, mode=self._mode, created_ts=now_iso)
            return {"outcome": "repriced"}

        return {"outcome": "pending", "elapsed": elapsed}
