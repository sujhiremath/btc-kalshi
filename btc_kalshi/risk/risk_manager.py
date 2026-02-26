"""
Risk manager: state, window, feed health, re-entry rules, sizing, exposure, daily stop, profit protection.
Live uses StateMachine; paper skips state check (always allows entries when feeds healthy).
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable, Optional, Tuple

from btc_kalshi.core.constants import ROC_REENTRY_THRESHOLD
from btc_kalshi.core.logger import get_logger

REENTRY_COOLDOWN_MINUTES = 15
MAX_ENTRIES_PER_CONTRACT_PER_DAY = 2


class RiskManager:
    """
    evaluate_signal(signal, expiry_ts) → (approved, reason, size). Checks: state (live only),
    window, feed health, re-entry (max 2/contract/day, 15-min cooldown, ROC>=0.6% after stop),
    sizing, exposure, daily stop, profit protection. on_trade_exit(), reset_daily().
    """

    def __init__(
        self,
        state_machine: Any,
        trading_window: Any,
        position_sizer: Any,
        streak_manager: Any,
        sqlite_manager: Any,
        event_logger: Optional[Any] = None,
        mode: str = "live",
        get_feed_healthy: Optional[Callable[[], bool]] = None,
    ) -> None:
        self._state_machine = state_machine
        self._trading_window = trading_window
        self._position_sizer = position_sizer
        self._streak_manager = streak_manager
        self._db = sqlite_manager
        self._event_logger = event_logger
        self._mode = mode
        self._get_feed_healthy = get_feed_healthy or (lambda: True)
        self._logger = get_logger("risk-manager")
        # contract_id -> {"ts": datetime, "was_stop_loss": bool}
        self._reentry_last_exit: dict[str, dict[str, Any]] = {}

    def _now(self) -> datetime:
        return datetime.now(timezone.utc)

    async def evaluate_signal(
        self,
        signal: Any,
        expiry_ts: Optional[str] = None,
    ) -> Tuple[bool, str, int]:
        """
        Returns (approved, reason, size). Size is 0 when not approved.
        """
        # Live only: state must allow new entries
        if self._mode == "live" and self._state_machine is not None:
            can_accept = await self._state_machine.can_accept_new_entries()
            if not can_accept:
                return (False, "state_not_active", 0)

        if not self._trading_window.is_entry_allowed():
            return (False, "outside_window", 0)

        if not self._get_feed_healthy():
            return (False, "feed_unhealthy", 0)

        state = await self._db.get_bot_state(mode=self._mode)
        trading_date = state.get("trading_date") or self._now().strftime("%Y-%m-%d")

        # Re-entry: max 2 per contract per day
        count = await self._db.count_contract_entries_today(
            self._mode, signal.contract_id, trading_date
        )
        if count >= MAX_ENTRIES_PER_CONTRACT_PER_DAY:
            return (False, "max_entries_per_contract", 0)

        # Re-entry: 15-min cooldown and ROC after stop-loss
        last = self._reentry_last_exit.get(signal.contract_id)
        if last is not None:
            elapsed = (self._now() - last["ts"]).total_seconds()
            if elapsed < REENTRY_COOLDOWN_MINUTES * 60:
                return (False, "reentry_cooldown", 0)
            if last.get("was_stop_loss"):
                roc = signal.roc_value if hasattr(signal, "roc_value") else None
                if roc is None or roc < ROC_REENTRY_THRESHOLD:
                    return (False, "reentry_roc_after_stop", 0)

        if await self._streak_manager.check_daily_stop():
            return (False, "daily_stop", 0)

        protection = await self._streak_manager.check_profit_protection()
        if protection.get("action") == "stop":
            return (False, "profit_stop", 0)

        if await self._streak_manager.check_drawdown_floors():
            return (False, "drawdown_floor", 0)

        starting = float(state.get("starting_bankroll") or 0)
        daily_net = float(state.get("daily_pnl_net") or 0)
        bankroll = starting + daily_net
        mult = float(state.get("size_multiplier") or 1.0)
        entry = getattr(signal, "entry_price", 0.5)
        stop = getattr(signal, "stop_price", 0.1)

        size = self._position_sizer.calculate_size(bankroll, entry, stop, mult)
        if size <= 0:
            return (False, "sizing_zero", 0)

        if expiry_ts:
            side = getattr(signal, "side", "YES")
            can_open = await self._position_sizer.can_open_position(
                entry=entry,
                stop=stop,
                size=size,
                expiry_ts=expiry_ts,
                mode=self._mode,
                side=side,
            )
            if not can_open:
                return (False, "exposure_or_limit", 0)

        return (True, "", size)

    def on_trade_exit(self, contract_id: str, was_stop_loss: bool = False) -> None:
        """Record exit for re-entry cooldown and ROC-after-stop rule."""
        self._reentry_last_exit[contract_id] = {
            "ts": self._now(),
            "was_stop_loss": was_stop_loss,
        }

    def reset_daily(self) -> None:
        """Clear re-entry tracking (cooldown / last exit)."""
        self._reentry_last_exit.clear()
