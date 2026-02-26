"""Shared signal engine: one signal stream for both live and paper. No mode awareness."""
from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

from btc_kalshi.core.constants import (
    EMA_PERIOD,
    MAX_CONTRACT_ASK,
    MAX_SPREAD,
    ROC_ENTRY_THRESHOLD,
    STOP_LOSS_PRICE,
    TAKE_PROFIT_PRICE,
    VOL_HIGH_MULTIPLIER,
    VOLATILITY_WINDOW,
)
from btc_kalshi.core.logger import get_logger
from btc_kalshi.strategy.breakout import check_price_move, confirm_breakout, detect_breakout
from btc_kalshi.strategy.indicators import (
    BARS_PER_HOUR,
    calculate_ema,
    calculate_hourly_avg_volatility,
    calculate_roc,
    calculate_volatility,
)


def _parse_strike(title: str) -> Optional[float]:
    if not title:
        return None
    m = re.search(r"above\s+\$([\d,]+)", title, re.IGNORECASE)
    if not m:
        return None
    try:
        return float(m.group(1).replace(",", ""))
    except ValueError:
        return None


def _best_ask(orderbook: Dict[str, Any]) -> float:
    asks = orderbook.get("asks") or []
    if not asks:
        return 1.0
    p = asks[0].get("price") if isinstance(asks[0], dict) else asks[0]
    return float(p) / 100.0


def _best_bid(orderbook: Dict[str, Any]) -> float:
    bids = orderbook.get("bids") or []
    if not bids:
        return 0.0
    p = bids[0].get("price") if isinstance(bids[0], dict) else bids[0]
    return float(p) / 100.0


@dataclass
class Signal:
    timestamp: datetime
    contract_id: str
    contract_title: str
    direction: str
    side: str
    btc_price: float
    strike_price: float
    entry_price: float
    stop_price: float
    take_profit_price: float
    roc_value: Optional[float]
    ema_value: Optional[float]
    volatility_ratio: Optional[float]
    breakout_level: Optional[float]
    filter_results: Dict[str, bool]
    all_passed: bool
    rejection_reason: str


class SignalEngine:
    """
    Shared signal engine: produces one signal stream consumed by both live and paper.
    Does not know about modes.
    """

    def __init__(self) -> None:
        self._logger = get_logger("signal-engine")
        self._signal_queue: asyncio.Queue[Signal] = asyncio.Queue()
        self._stop = False
        self._scan_task: Optional[asyncio.Task[None]] = None

    def _check_price_move(self, bars: Sequence[Any], direction: str) -> bool:
        return check_price_move(bars, direction)

    def _check_breakout(self, bars: Sequence[Any]) -> Tuple[Optional[str], Optional[float]]:
        out = detect_breakout(bars)
        if out is None:
            return None, None
        return out[0], out[1]

    def _check_false_breakout(self, bars: Sequence[Any], direction: str, level: float) -> bool:
        return confirm_breakout(bars, direction, level)

    def _check_roc(self, bars: Sequence[Any], direction: str) -> Tuple[bool, Optional[float]]:
        roc = calculate_roc(bars)
        if roc is None:
            return False, None
        if direction == "up":
            return roc >= ROC_ENTRY_THRESHOLD, roc
        return roc <= -ROC_ENTRY_THRESHOLD, roc

    def _check_volatility(self, bars: Sequence[Any]) -> Tuple[bool, Optional[float]]:
        vol = calculate_volatility(bars, window=VOLATILITY_WINDOW)
        hourly = calculate_hourly_avg_volatility(bars)
        if vol is None:
            return False, None
        if hourly is None or hourly == 0:
            return True, 0.0
        ratio = vol / hourly
        return ratio <= VOL_HIGH_MULTIPLIER, ratio

    def _check_ema_trend(self, bars: Sequence[Any], direction: str) -> bool:
        ema = calculate_ema(bars, period=EMA_PERIOD)
        if not bars or ema is None:
            return False
        close = float(bars[-1].close)
        if direction == "up":
            return close > ema
        return close < ema

    def _check_ask(self, orderbook: Dict[str, Any]) -> bool:
        return _best_ask(orderbook) <= MAX_CONTRACT_ASK

    def _check_spread(self, orderbook: Dict[str, Any]) -> bool:
        ask = _best_ask(orderbook)
        bid = _best_bid(orderbook)
        return (ask - bid) <= MAX_SPREAD

    def evaluate(
        self,
        contract: Dict[str, Any],
        orderbook: Dict[str, Any],
        bars: Sequence[Any],
        btc_price: float,
    ) -> Signal:
        """
        Run all 8 filters. Direction mapping: bullish -> YES, bearish -> NO.
        """
        now = datetime.now(timezone.utc)
        contract_id = contract.get("ticker") or contract.get("id") or ""
        contract_title = contract.get("title") or ""
        strike_price = _parse_strike(contract_title) or 0.0
        entry_price = _best_ask(orderbook)
        stop_price = STOP_LOSS_PRICE
        take_profit_price = TAKE_PROFIT_PRICE

        filter_results: Dict[str, bool] = {}
        rejection_reason = ""
        all_passed = False
        roc_v, ema_v, vol_ratio = None, None, None

        # 1) Breakout gives direction
        direction, breakout_level = self._check_breakout(bars)
        if direction is None or breakout_level is None:
            filter_results["breakout"] = False
            rejection_reason = "no_breakout"
            return Signal(
                timestamp=now,
                contract_id=contract_id,
                contract_title=contract_title,
                direction="up",
                side="YES",
                btc_price=btc_price,
                strike_price=strike_price,
                entry_price=entry_price,
                stop_price=stop_price,
                take_profit_price=take_profit_price,
                roc_value=None,
                ema_value=None,
                volatility_ratio=None,
                breakout_level=None,
                filter_results=filter_results,
                all_passed=False,
                rejection_reason=rejection_reason,
            )
        filter_results["breakout"] = True

        # 2) Price move
        filter_results["price_move"] = self._check_price_move(bars, direction)
        if not filter_results["price_move"]:
            rejection_reason = "price_move"
            all_passed = False
            roc_v, ema_v, vol_ratio = None, None, None
        else:
            # 3) False breakout
            filter_results["false_breakout"] = self._check_false_breakout(bars, direction, breakout_level)
            if not filter_results["false_breakout"]:
                rejection_reason = "false_breakout"
                all_passed = False
                roc_v, ema_v, vol_ratio = None, None, None
            else:
                roc_ok, roc_v = self._check_roc(bars, direction)
                filter_results["roc"] = roc_ok
                if not roc_ok:
                    rejection_reason = "roc"
                    all_passed = False
                    ema_v, vol_ratio = None, None
                else:
                    vol_ok, vol_ratio = self._check_volatility(bars)
                    filter_results["volatility"] = vol_ok
                    if not vol_ok:
                        rejection_reason = "volatility"
                        all_passed = False
                        ema_v = None
                    else:
                        filter_results["ema_trend"] = self._check_ema_trend(bars, direction)
                        ema_v = calculate_ema(bars, period=EMA_PERIOD)
                        if not filter_results["ema_trend"]:
                            rejection_reason = "ema_trend"
                            all_passed = False
                        else:
                            filter_results["ask"] = self._check_ask(orderbook)
                            if not filter_results["ask"]:
                                rejection_reason = "ask"
                                all_passed = False
                            else:
                                filter_results["spread"] = self._check_spread(orderbook)
                                if not filter_results["spread"]:
                                    rejection_reason = "spread"
                                    all_passed = False
                                else:
                                    all_passed = True

        if roc_v is None and "roc" not in filter_results:
            roc_v = calculate_roc(bars)
        if ema_v is None and "ema_trend" in filter_results:
            ema_v = calculate_ema(bars, period=EMA_PERIOD)
        if vol_ratio is None and "volatility" not in filter_results:
            _, vol_ratio = self._check_volatility(bars)

        side = "YES" if direction == "up" else "NO"

        return Signal(
            timestamp=now,
            contract_id=contract_id,
            contract_title=contract_title,
            direction=direction,
            side=side,
            btc_price=btc_price,
            strike_price=strike_price,
            entry_price=entry_price,
            stop_price=stop_price,
            take_profit_price=take_profit_price,
            roc_value=roc_v,
            ema_value=ema_v,
            volatility_ratio=vol_ratio,
            breakout_level=breakout_level,
            filter_results=filter_results,
            all_passed=all_passed,
            rejection_reason=rejection_reason,
        )

    @property
    def signal_queue(self) -> asyncio.Queue[Signal]:
        return self._signal_queue

    async def run_scan_loop(
        self,
        get_universe: Any,
        get_orderbook: Any,
        get_bars: Any,
        get_btc_price: Any,
    ) -> None:
        """Emit signals to signal_queue. get_universe(), get_orderbook(ticker), get_bars(), get_btc_price()."""
        self._stop = False
        while not self._stop:
            try:
                universe = get_universe() if callable(get_universe) else get_universe
                bars = get_bars() if callable(get_bars) else []
                btc_price = get_btc_price() if callable(get_btc_price) else 0.0
                if not bars or btc_price <= 0:
                    await asyncio.sleep(1)
                    continue
                for contract in universe:
                    ticker = contract.get("ticker") or contract.get("id")
                    if not ticker:
                        continue
                    ob = get_orderbook(ticker) if callable(get_orderbook) else (get_orderbook.get(ticker) if isinstance(get_orderbook, dict) else None)
                    if not ob:
                        continue
                    signal = self.evaluate(contract, ob, bars, btc_price)
                    if signal.all_passed:
                        await self._signal_queue.put(signal)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                self._logger.critical("scan_loop_error", extra={"error": str(exc)})
            await asyncio.sleep(1)

    def stop(self) -> None:
        self._stop = True
        if self._scan_task and not self._scan_task.done():
            self._scan_task.cancel()
