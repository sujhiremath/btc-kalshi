"""Universe manager: shared contract/orderbook cache and soft-block tracking."""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional

from btc_kalshi.core.logger import get_logger
from btc_kalshi.exchange.contract_filter import filter_universe
from btc_kalshi.exchange.exchange_protocol import ExchangeProtocol

SOFT_BLOCK_MINUTES = 10
REFRESH_INTERVAL_SECONDS = 60


class UniverseManager:
    """
    Holds the shared universe of real contracts and cached orderbooks for both
    live and paper paths. Accepts ExchangeProtocol (not KalshiClient).
    """

    def __init__(
        self,
        exchange: ExchangeProtocol,
        *,
        get_btc_price: Callable[[], float],
    ) -> None:
        self._exchange = exchange
        self._get_btc_price = get_btc_price
        self._logger = get_logger("universe-manager")
        self._universe: List[Dict[str, Any]] = []
        self._orderbook_cache: Dict[str, Dict[str, Any]] = {}
        self._soft_blocks: Dict[str, datetime] = {}  # contract_id -> block expiry (utc)
        self._refresh_task: Optional[asyncio.Task[None]] = None
        self._stop_loop = False

    @staticmethod
    def _now() -> datetime:
        return datetime.now(timezone.utc)

    def _is_soft_blocked_at(self, contract_id: str, now: datetime) -> bool:
        expiry = self._soft_blocks.get(contract_id)
        if expiry is None:
            return False
        if now >= expiry:
            return False
        return True

    def is_soft_blocked(self, contract_id: str, now: Optional[datetime] = None) -> bool:
        """Return True if the contract is currently within its 10-minute soft block."""
        t = now if now is not None else self._now()
        return self._is_soft_blocked_at(contract_id, t)

    def add_soft_block(self, contract_id: str, now: Optional[datetime] = None) -> None:
        """Add a 10-minute soft block for the contract."""
        t = now if now is not None else self._now()
        self._soft_blocks[contract_id] = t + timedelta(minutes=SOFT_BLOCK_MINUTES)
        self._logger.info(
            "Soft block added",
            extra={"contract_id": contract_id, "expires_at": self._soft_blocks[contract_id].isoformat()},
        )

    async def refresh(self, now: Optional[datetime] = None) -> None:
        """
        Fetch contracts and orderbooks from the exchange, filter to eligible
        canonical contracts, and update cached universe and orderbooks.
        """
        t = now if now is not None else self._now()
        btc_price = self._get_btc_price()
        raw = await self._exchange.get_btc_contracts()
        orderbooks: Dict[str, Dict[str, Any]] = {}
        for c in raw:
            ticker = c.get("ticker") or c.get("id") or ""
            if not ticker:
                continue
            try:
                ob = await self._exchange.get_orderbook(ticker)
                orderbooks[ticker] = ob
            except Exception as exc:  # pragma: no cover
                self._logger.debug(
                    "Failed to fetch orderbook",
                    extra={"ticker": ticker, "error": str(exc)},
                )
        filtered = filter_universe(raw, btc_price, orderbooks, t)
        self._universe = filtered
        self._orderbook_cache = orderbooks
        # Prune expired soft blocks
        self._soft_blocks = {
            k: v for k, v in self._soft_blocks.items() if v > t
        }

    def get_universe(self, now: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """
        Return the current eligible universe, excluding any contract that is
        currently soft-blocked.
        """
        t = now if now is not None else self._now()
        return [
            c for c in self._universe
            if not self._is_soft_blocked_at(c.get("ticker") or c.get("id") or "", t)
        ]

    def get_orderbook(self, contract_id: str) -> Optional[Dict[str, Any]]:
        """Return cached orderbook for the contract, or None if not cached."""
        ob = self._orderbook_cache.get(contract_id)
        return dict(ob) if ob is not None else None

    async def _refresh_loop(self) -> None:
        while not self._stop_loop:
            try:
                await self.refresh()
            except Exception as exc:  # pragma: no cover
                self._logger.critical(
                    "Universe refresh failed",
                    extra={"error": str(exc)},
                )
            await asyncio.sleep(REFRESH_INTERVAL_SECONDS)

    def start_refresh_loop(self) -> None:
        """Start the background refresh loop (every 60s)."""
        if self._refresh_task is not None and not self._refresh_task.done():
            return
        self._stop_loop = False
        self._refresh_task = asyncio.create_task(self._refresh_loop())
        self._logger.info("Universe refresh loop started")

    async def stop(self) -> None:
        """Stop the refresh loop and wait for it to finish."""
        self._stop_loop = True
        if self._refresh_task is not None:
            self._refresh_task.cancel()
            try:
                await self._refresh_task
            except asyncio.CancelledError:
                pass
            self._refresh_task = None
        self._logger.info("Universe refresh loop stopped")
