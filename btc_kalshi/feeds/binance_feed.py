from __future__ import annotations

import asyncio
import inspect
import json
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import websockets

from btc_kalshi.core.logger import get_logger
from btc_kalshi.feeds.coinbase_feed import OnTickCallback, PriceTick


class BinanceFeed:
    """
    Backup BTC price feed backed by Binance websocket.
    """

    def __init__(self, ws_url: str, on_tick: OnTickCallback) -> None:
        self._ws_url = ws_url
        self._on_tick = on_tick
        self._logger = get_logger("binance-feed")
        self._last_tick_ts: Optional[datetime] = None
        self._stopped: bool = False
        self._task: Optional[asyncio.Task[None]] = None

    @staticmethod
    def _now() -> datetime:
        return datetime.now(timezone.utc)

    @property
    def last_tick_ts(self) -> Optional[datetime]:
        return self._last_tick_ts

    def is_healthy(self) -> bool:
        """
        A feed is healthy if we've seen a tick within the last 10 seconds.
        """
        if self._last_tick_ts is None:
            return False
        age = self._now() - self._last_tick_ts
        return age <= timedelta(seconds=10)

    async def _emit_tick(self, tick: PriceTick) -> None:
        self._last_tick_ts = tick.timestamp
        try:
            result = self._on_tick(tick)
            if inspect.isawaitable(result):
                await result  # type: ignore[func-returns-value]
        except Exception as exc:  # pragma: no cover - defensive logging
            self._logger.critical(
                "Error in on_tick callback",
                extra={"error": str(exc)},
            )

    async def _handle_message(self, message: str) -> None:
        """
        Parse a Binance BTCUSDT trade message into PriceTicks.
        """
        try:
            data: dict[str, Any] = json.loads(message)
        except json.JSONDecodeError:
            self._logger.critical(
                "Failed to decode Binance message as JSON",
                extra={"raw": message[:256]},
            )
            return

        # Typical Binance trade stream symbol key is "s": "BTCUSDT"
        if data.get("s") != "BTCUSDT":
            return

        try:
            price = float(data["p"])
            volume = float(data.get("q", 0.0))
        except (KeyError, ValueError, TypeError):
            return

        tick = PriceTick(
            source="binance",
            price=price,
            volume=volume,
            timestamp=self._now(),
        )
        await self._emit_tick(tick)

    async def connect(self) -> None:
        """
        Start the feed in the background.
        """
        if self._task is None or self._task.done():
            self._stopped = False
            self._task = asyncio.create_task(self.run())

    async def run(self) -> None:
        """
        Main websocket loop with reconnect and backoff.
        """
        backoffs = [1, 2, 4]
        attempts = 0

        while not self._stopped and attempts <= len(backoffs):
            try:
                async with websockets.connect(self._ws_url) as ws:
                    attempts = 0

                    async for message in ws:
                        await self._handle_message(message)

            except Exception as exc:
                attempts += 1
                self._logger.critical(
                    "Binance feed websocket failure",
                    extra={"error": str(exc), "attempt": attempts},
                )
                if attempts > len(backoffs):
                    break
                backoff = backoffs[attempts - 1]
                await asyncio.sleep(backoff)

        self._logger.info(
            "Binance feed stopped",
            extra={"attempts": attempts},
        )

    async def disconnect(self) -> None:
        """
        Request the feed to stop and wait for the background task to finish.
        """
        self._stopped = True
        if self._task is not None:
            await self._task

