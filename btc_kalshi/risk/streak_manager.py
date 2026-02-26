"""
Streak manager: loss/win streaks, daily stop, drawdown floors, profit protection.
Mode-aware: reads/writes bot_state (live) or paper_state (paper). Independent streaks per mode.
"""
from __future__ import annotations

from typing import Any, Dict, Optional

from btc_kalshi.core.constants import (
    DAILY_STOP_LOSS_PCT,
    INTRADAY_DRAWDOWN_FLOOR,
    PROFIT_REDUCE_THRESHOLD,
    PROFIT_STOP_THRESHOLD,
)
from btc_kalshi.core.logger import get_logger


# Loss streak: 1→continue, 2→75%, 3→pause 30m+75%, 4→pause 90m+50%, 5→stop
LOSS_MULTIPLIERS = {1: 1.0, 2: 0.75, 3: 0.75, 4: 0.5, 5: 0.5}
LOSS_PAUSE_MINUTES = {1: 0, 2: 0, 3: 30, 4: 90, 5: 0}
LOSS_TRANSITION_AT_5 = "STOPPED"

# Win streak: 2-3→115%, 4-5→hold (keep current), 7+→80%
WIN_MULTIPLIERS = {2: 1.15, 3: 1.15, 4: None, 5: None, 6: None}  # None = hold
WIN_MULTIPLIER_7PLUS = 0.8


class StreakManager:
    """
    Tracks loss/win streaks and applies size_multiplier / pause / lifecycle transitions.
    record_trade_result(pnl, is_win) returns action dict. check_daily_stop,
    check_drawdown_floors, check_profit_protection for risk gates.
    """

    def __init__(
        self,
        sqlite_manager: Any,
        state_machine: Any,
        event_logger: Optional[Any] = None,
        mode: str = "live",
    ) -> None:
        self._db = sqlite_manager
        self._state_machine = state_machine
        self._event_logger = event_logger
        self._mode = mode
        self._logger = get_logger("streak-manager")

    async def _get_state(self) -> Dict[str, Any]:
        return await self._db.get_bot_state(mode=self._mode)

    async def _update_state(self, **kwargs: Any) -> None:
        await self._db.update_bot_state(mode=self._mode, **kwargs)

    async def record_trade_result(self, pnl: float, is_win: bool) -> Dict[str, Any]:
        """
        Update streak and PnL; apply loss/win rules. Returns action dict with
        size_multiplier, pause_minutes (optional), transition (optional).
        """
        state = await self._get_state()
        starting = float(state.get("starting_bankroll") or 0)
        daily_gross = float(state.get("daily_pnl_gross") or 0)
        daily_net = float(state.get("daily_pnl_net") or 0)
        peak = float(state.get("intraday_peak_equity") or starting)
        streak_type = state.get("current_streak_type")
        streak_count = int(state.get("current_streak_count") or 0)
        mult = float(state.get("size_multiplier") or 1.0)

        # Update PnL and peak
        daily_gross += pnl
        daily_net += pnl
        equity = starting + daily_net
        new_peak = max(peak, equity) if starting else peak

        # Update streak
        if is_win:
            if streak_type == "win":
                streak_count += 1
            else:
                streak_type = "win"
                streak_count = 1
        else:
            if streak_type == "loss":
                streak_count += 1
            else:
                streak_type = "loss"
                streak_count = 1

        # Apply rules
        pause_minutes: int = 0
        transition: Optional[str] = None

        if streak_type == "loss":
            mult = LOSS_MULTIPLIERS.get(streak_count, LOSS_MULTIPLIERS[5])
            pause_minutes = LOSS_PAUSE_MINUTES.get(streak_count, 0)
            if streak_count >= 5:
                transition = LOSS_TRANSITION_AT_5
        else:
            if streak_count >= 7:
                mult = WIN_MULTIPLIER_7PLUS
            elif streak_count in (2, 3):
                mult = WIN_MULTIPLIERS[streak_count]
            # 4, 5, 6: hold (mult unchanged)

        # Persist
        await self._update_state(
            daily_pnl_gross=daily_gross,
            daily_pnl_net=daily_net,
            intraday_peak_equity=new_peak,
            current_streak_type=streak_type,
            current_streak_count=streak_count,
            size_multiplier=mult,
        )

        # Lifecycle transition (live only)
        if self._mode == "live" and transition and self._state_machine is not None:
            await self._state_machine.transition(transition, reason="loss_streak_5")

        action: Dict[str, Any] = {"size_multiplier": mult}
        if pause_minutes:
            action["pause_minutes"] = pause_minutes
        if transition:
            action["transition"] = transition

        if self._event_logger is not None:
            self._event_logger.log_event(
                event_type="streak_update",
                severity="INFO",
                service_name="streak-manager",
                contract_id=None,
                payload={
                    "mode": self._mode,
                    "pnl": pnl,
                    "is_win": is_win,
                    "streak_type": streak_type,
                    "streak_count": streak_count,
                    "size_multiplier": mult,
                    "action": action,
                },
                mode=self._mode,
            )

        return action

    async def check_daily_stop(self) -> bool:
        """True if daily loss >= DAILY_STOP_LOSS_PCT of starting bankroll."""
        state = await self._get_state()
        starting = float(state.get("starting_bankroll") or 0)
        daily_net = float(state.get("daily_pnl_net") or 0)
        if starting <= 0:
            return False
        threshold = -DAILY_STOP_LOSS_PCT * starting
        return daily_net <= threshold

    async def check_drawdown_floors(self) -> bool:
        """True if drawdown from intraday peak >= INTRADAY_DRAWDOWN_FLOOR."""
        state = await self._get_state()
        starting = float(state.get("starting_bankroll") or 0)
        daily_net = float(state.get("daily_pnl_net") or 0)
        peak = float(state.get("intraday_peak_equity") or starting)
        if peak <= 0:
            return False
        equity = starting + daily_net
        drawdown = (peak - equity) / peak
        return drawdown >= INTRADAY_DRAWDOWN_FLOOR

    async def check_profit_protection(self) -> Dict[str, Any]:
        """
        Returns dict with triggered (bool), action (e.g. 'reduce' or 'stop' or None).
        PROFIT_REDUCE_THRESHOLD (12%): reduce size; PROFIT_STOP_THRESHOLD (25%): stop.
        """
        state = await self._get_state()
        starting = float(state.get("starting_bankroll") or 0)
        daily_net = float(state.get("daily_pnl_net") or 0)
        if starting <= 0:
            return {"triggered": False, "action": None}
        pct = daily_net / starting
        if pct >= PROFIT_STOP_THRESHOLD:
            return {"triggered": True, "action": "stop"}
        if pct >= PROFIT_REDUCE_THRESHOLD:
            return {"triggered": True, "action": "reduce"}
        return {"triggered": False, "action": None}
