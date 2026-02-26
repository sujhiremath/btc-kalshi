"""
Paper exchange adapter: implements ExchangeProtocol using universe_manager
for real orderbook data and FillSimulator for local fills. No Kalshi demo site.
"""
from __future__ import annotations

import uuid
from typing import Any, Dict, List, Optional

from btc_kalshi.exchange.exchange_protocol import ExchangeProtocol
from btc_kalshi.exchange.fill_simulator import FillSimulator


def _best_ask_and_depth(ob: Dict[str, Any]) -> tuple[float, int]:
    """Best ask in decimal, and book depth within 3 cents. Returns (0.0, 0) if no asks."""
    asks = ob.get("asks") or []
    if not asks:
        return 0.0, 0
    first = asks[0]
    best_ask_cents = float(first.get("price") if isinstance(first, dict) else first)
    best_ask = best_ask_cents / 100.0
    depth_cut = best_ask_cents + 3
    depth = 0
    for level in asks:
        p = level.get("price") if isinstance(level, dict) else level
        if float(p) <= depth_cut:
            q = level.get("quantity") if isinstance(level, dict) else (level[1] if isinstance(level, (list, tuple)) else 0)
            depth += int(q) if q is not None else 0
    return best_ask, depth


def _best_bid(ob: Dict[str, Any]) -> float:
    bids = ob.get("bids") or []
    if not bids:
        return 0.0
    first = bids[0]
    p = first.get("price") if isinstance(first, dict) else first
    return float(p) / 100.0


class PaperExchangeAdapter(ExchangeProtocol):
    """
    Delegates contract/orderbook to universe_manager; place_order uses FillSimulator
    with real current orderbook. Internal state: _orders, _positions, _balance, _trade_history.
    get_balance() = starting - exposure + realized P&L.
    """

    def __init__(
        self,
        universe_manager: Any,
        fill_simulator: FillSimulator,
        starting_balance: float = 100.0,
    ) -> None:
        self._universe_manager = universe_manager
        self._fill_simulator = fill_simulator
        self._starting_balance = starting_balance
        self._orders: Dict[str, Dict[str, Any]] = {}
        self._positions: List[Dict[str, Any]] = []
        self._trade_history: List[Dict[str, Any]] = []

    async def get_btc_contracts(self) -> List[Dict[str, Any]]:
        return self._universe_manager.get_universe()

    async def get_contract(self, contract_id: str) -> Optional[Dict[str, Any]]:
        for c in self._universe_manager.get_universe():
            if (c.get("ticker") or c.get("id") or "") == contract_id:
                return c
        return None

    async def get_orderbook(self, contract_id: str) -> Dict[str, Any]:
        ob = self._universe_manager.get_orderbook(contract_id)
        return dict(ob) if ob else {}

    async def place_order(
        self,
        contract_id: str,
        side: str,
        count: int,
        price_cents: Optional[int] = None,
        type: str = "limit",
        client_order_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        ob = self._universe_manager.get_orderbook(contract_id) or {}
        side_lower = (side or "yes").lower()
        order_id = str(uuid.uuid4().hex[:12])

        # Check for closing an existing position (exit)
        pos = self._find_position(contract_id, "yes" if side_lower == "no" else "no")
        if pos is not None and pos["position"] >= count:
            # Exit
            bid_price = _best_bid(ob)
            exit_result = self._fill_simulator.simulate_exit_fill(bid_price, type or "market")
            fill_price = exit_result["fill_price"]
            entry_price = pos["entry_price"]
            won = fill_price > entry_price
            pnl = self._fill_simulator.calculate_simulated_pnl(entry_price, fill_price, count, won)
            self._trade_history.append({
                "contract_id": contract_id,
                "entry": entry_price,
                "exit": fill_price,
                "contracts": count,
                "won": won,
                "pnl": pnl,
            })
            pos["position"] -= count
            if pos["position"] <= 0:
                self._positions.remove(pos)
            self._orders[order_id] = {
                "id": order_id,
                "ticker": contract_id,
                "status": "filled",
                "side": side,
                "count": count,
                "fill_price": fill_price,
            }
            return {"order": self._orders[order_id]}

        # Entry
        if not ob.get("asks"):
            self._orders[order_id] = {"id": order_id, "status": "resting", "ticker": contract_id}
            return {"order": self._orders[order_id]}
        ask_price, book_depth_3c = _best_ask_and_depth(ob)
        elapsed = 0.0
        fill_result = self._fill_simulator.simulate_entry_fill(ask_price, book_depth_3c, elapsed)
        if not fill_result["filled"]:
            self._orders[order_id] = {"id": order_id, "status": "resting", "ticker": contract_id}
            return {"order": self._orders[order_id]}
        fill_price = fill_result["fill_price"]
        fill_size_pct = fill_result.get("fill_size_pct", 1.0)
        filled_count = max(1, int(round(count * fill_size_pct)))
        existing = self._find_position(contract_id, side_lower)
        if existing is not None:
            total_size = existing["position"] + filled_count
            existing["entry_price"] = (existing["entry_price"] * existing["position"] + fill_price * filled_count) / total_size
            existing["position"] = total_size
        else:
            self._positions.append({
                "ticker": contract_id,
                "contract_id": contract_id,
                "position": filled_count,
                "side": side_lower,
                "entry_price": fill_price,
            })
        self._orders[order_id] = {
            "id": order_id,
            "ticker": contract_id,
            "status": "filled",
            "side": side,
            "count": filled_count,
            "fill_price": fill_price,
        }
        return {"order": self._orders[order_id]}

    def _find_position(self, contract_id: str, side: str) -> Optional[Dict[str, Any]]:
        for p in self._positions:
            if (p.get("contract_id") or p.get("ticker")) == contract_id and (p.get("side") or "yes") == side:
                return p
        return None

    async def cancel_order(self, order_id: str) -> Dict[str, Any]:
        if order_id in self._orders:
            self._orders[order_id]["status"] = "cancelled"
        return {"order": self._orders.get(order_id, {"id": order_id, "status": "cancelled"})}

    async def get_order(self, order_id: str) -> Optional[Dict[str, Any]]:
        return self._orders.get(order_id)

    async def get_open_orders(self, contract_id: Optional[str] = None) -> List[Dict[str, Any]]:
        open_list = [o for o in self._orders.values() if o.get("status") == "resting"]
        if contract_id:
            open_list = [o for o in open_list if (o.get("ticker") or o.get("contract_id")) == contract_id]
        return open_list

    async def get_positions(self) -> List[Dict[str, Any]]:
        return [
            {
                "ticker": p.get("contract_id") or p.get("ticker"),
                "contract_id": p.get("contract_id") or p.get("ticker"),
                "position": p["position"],
                "side": (p.get("side") or "yes").lower(),
            }
            for p in self._positions
            if p["position"] > 0
        ]

    async def get_balance(self) -> Dict[str, Any]:
        exposure = sum(
            (p["position"] * p["entry_price"]) for p in self._positions
        )
        realized_pnl = sum(t["pnl"] for t in self._trade_history)
        balance = self._starting_balance + realized_pnl - exposure
        return {"balance": balance, "cash": balance, "realized_pnl": realized_pnl, "exposure": exposure}
