from __future__ import annotations

import inspect
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Awaitable, Callable, List, Optional

from btc_kalshi.core.logger import get_logger
from btc_kalshi.feeds.coinbase_feed import PriceTick


@dataclass
class Bar:
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    tick_count: int


BarCallback = Callable[[Bar], Awaitable[None]] | Callable[[Bar], None]


class BarAggregator:
    """
    Aggregates ticks from the FeedManager into fixed 5-second OHLC bars.
    """

    def __init__(self, feed_manager, csv_path: str | Path) -> None:
        self._logger = get_logger("bar-aggregator")
        self._feed_manager = feed_manager
        self._csv_path = Path(csv_path)
        self._csv_path.parent.mkdir(parents=True, exist_ok=True)

        self._bars: List[Bar] = []
        self._current_bar: Optional[Bar] = None
        self._current_window_start: Optional[datetime] = None
        self._subscribers: List[BarCallback] = []

        # Subscribe to ticks from the feed manager.
        self._feed_manager.subscribe(self._on_tick)

    @staticmethod
    def _now() -> datetime:
        return datetime.now(timezone.utc)

    @staticmethod
    def _window_start(ts: datetime) -> datetime:
        # Aligns timestamps to 5-second buckets based on Unix epoch.
        epoch_seconds = int(ts.timestamp())
        bucket_start = (epoch_seconds // 5) * 5
        return datetime.fromtimestamp(bucket_start, tz=timezone.utc)

    def subscribe(self, callback: BarCallback) -> None:
        """
        Subscribe to completed bars.
        """
        self._subscribers.append(callback)

    async def _notify_subscribers(self, bar: Bar) -> None:
        for cb in list(self._subscribers):
            try:
                result = cb(bar)
                if inspect.isawaitable(result):
                    await result  # type: ignore[func-returns-value]
            except Exception as exc:  # pragma: no cover - defensive
                self._logger.critical(
                    "Error in bar subscriber callback",
                    extra={"error": str(exc)},
                )

    def _append_csv(self, bar: Bar) -> None:
        is_new_file = not self._csv_path.exists() or self._csv_path.stat().st_size == 0
        line = ",".join(
            [
                bar.timestamp.isoformat(),
                f"{bar.open:.8f}",
                f"{bar.high:.8f}",
                f"{bar.low:.8f}",
                f"{bar.close:.8f}",
                f"{bar.volume:.8f}",
                str(bar.tick_count),
            ]
        )
        with self._csv_path.open("a", encoding="utf-8") as f:
            if is_new_file:
                f.write("timestamp,open,high,low,close,volume,tick_count\n")
            f.write(line + "\n")

    async def _finalize_current_bar(self) -> None:
        if self._current_bar is None:
            return

        bar = self._current_bar
        self._bars.append(bar)
        if len(self._bars) > 500:
            self._bars = self._bars[-500:]

        self._append_csv(bar)
        await self._notify_subscribers(bar)

        self._current_bar = None
        self._current_window_start = None

    async def _on_tick(self, tick: PriceTick) -> None:
        """
        FeedManager callback for each incoming price tick.
        """
        ts = tick.timestamp
        window_start = self._window_start(ts)

        if self._current_window_start is None:
            # Start first bar.
            self._current_window_start = window_start
            self._current_bar = Bar(
                timestamp=window_start,
                open=tick.price,
                high=tick.price,
                low=tick.price,
                close=tick.price,
                volume=tick.volume,
                tick_count=1,
            )
            return

        if window_start == self._current_window_start:
            # Update current bar within same window.
            bar = self._current_bar
            if bar is None:
                return
            bar.high = max(bar.high, tick.price)
            bar.low = min(bar.low, tick.price)
            bar.close = tick.price
            bar.volume += tick.volume
            bar.tick_count += 1
            return

        # New window: finalize previous bar and start a new one.
        await self._finalize_current_bar()

        self._current_window_start = window_start
        self._current_bar = Bar(
            timestamp=window_start,
            open=tick.price,
            high=tick.price,
            low=tick.price,
            close=tick.price,
            volume=tick.volume,
            tick_count=1,
        )

    def get_bars(self, n: int) -> list[Bar]:
        """
        Return up to the last n completed bars (most recent last).
        """
        if n <= 0:
            return []
        return list(self._bars[-n:])

    def get_current_incomplete_bar(self) -> Optional[Bar]:
        """
        Return the current, not-yet-closed bar, if any.
        """
        return self._current_bar

