from __future__ import annotations

import asyncio
import inspect
from datetime import datetime, timezone
from typing import Awaitable, Callable, Dict, List, Optional

from btc_kalshi.core.logger import get_logger
from btc_kalshi.feeds.binance_feed import BinanceFeed
from btc_kalshi.feeds.coinbase_feed import CoinbaseFeed, PriceTick

PriceCallback = Callable[[PriceTick], Awaitable[None]] | Callable[[PriceTick], None]


class FeedManager:
    """
    Orchestrates primary (Coinbase) and backup (Binance) feeds with
    health-based failover and divergence-based suspension.
    """

    def __init__(self, primary_ws_url: str, backup_ws_url: str) -> None:
        self._logger = get_logger("feed-manager")
        self._subscribers: List[PriceCallback] = []
        self._entries_suspended: bool = False

        self._latest_primary: Optional[PriceTick] = None
        self._latest_backup: Optional[PriceTick] = None

        self._primary = CoinbaseFeed(primary_ws_url, self._on_primary_tick)
        self._backup = BinanceFeed(backup_ws_url, self._on_backup_tick)

    @staticmethod
    def _now() -> datetime:
        return datetime.now(timezone.utc)

    @property
    def entries_suspended(self) -> bool:
        return self._entries_suspended

    async def _notify_subscribers(self, tick: PriceTick) -> None:
        for cb in list(self._subscribers):
            try:
                result = cb(tick)
                if inspect.isawaitable(result):
                    await result  # type: ignore[func-returns-value]
            except Exception as exc:  # pragma: no cover - defensive
                self._logger.critical(
                    "Error in feed subscriber callback",
                    extra={"error": str(exc)},
                )

    def subscribe(self, callback: PriceCallback) -> None:
        self._subscribers.append(callback)

    async def _on_primary_tick(self, tick: PriceTick) -> None:
        self._latest_primary = tick
        self._update_entries_suspended()
        await self._notify_subscribers(tick)

    async def _on_backup_tick(self, tick: PriceTick) -> None:
        self._latest_backup = tick
        self._update_entries_suspended()
        await self._notify_subscribers(tick)

    def _update_entries_suspended(self) -> None:
        primary_ok = self._primary.is_healthy()
        backup_ok = self._backup.is_healthy()

        # Suspend if both feeds are down.
        if not primary_ok and not backup_ok:
            self._entries_suspended = True
            return

        # If both healthy and we have prices, check divergence.
        if (
            primary_ok
            and backup_ok
            and self._latest_primary is not None
            and self._latest_backup is not None
        ):
            p1 = self._latest_primary.price
            p2 = self._latest_backup.price
            if p1 > 0 and p2 > 0:
                mid = (p1 + p2) / 2.0
                divergence = abs(p1 - p2) / mid
                if divergence > 0.003:  # 0.3%
                    self._entries_suspended = True
                    return

        # Otherwise, entries are allowed.
        self._entries_suspended = False

    async def start(self) -> None:
        """
        Start both feeds.
        """
        await asyncio.gather(self._primary.connect(), self._backup.connect())

    async def stop(self) -> None:
        """
        Stop both feeds.
        """
        await asyncio.gather(self._primary.disconnect(), self._backup.disconnect())

    def get_current_price(self) -> Optional[float]:
        """
        Return the current reference price, using primary by default,
        then failing over to backup when primary appears stale.

        Returns None when suspended or no healthy feeds available.
        """
        self._update_entries_suspended()
        if self._entries_suspended:
            return None

        if self._primary.is_healthy() and self._latest_primary is not None:
            return self._latest_primary.price

        if self._backup.is_healthy() and self._latest_backup is not None:
            return self._latest_backup.price

        return None

    def get_feed_status(self) -> Dict[str, Dict[str, object]]:
        """
        Lightweight status snapshot for monitoring / debugging.
        """
        primary_healthy = self._primary.is_healthy()
        backup_healthy = self._backup.is_healthy()

        active: Optional[str]
        if not self._entries_suspended:
            if primary_healthy and self._latest_primary is not None:
                active = "primary"
            elif backup_healthy and self._latest_backup is not None:
                active = "backup"
            else:
                active = None
        else:
            active = None

        return {
            "primary": {
                "healthy": primary_healthy,
                "last_tick_ts": self._primary.last_tick_ts,
            },
            "backup": {
                "healthy": backup_healthy,
                "last_tick_ts": self._backup.last_tick_ts,
            },
            "entries_suspended": self._entries_suspended,
            "active_feed": active,
        }

