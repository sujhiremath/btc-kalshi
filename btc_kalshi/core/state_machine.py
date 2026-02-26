"""LIVE lifecycle state machine. Paper runs independently (always active when feeds healthy)."""
from __future__ import annotations

from typing import Any, Optional, Set, Tuple

from btc_kalshi.core.logger import get_logger
from btc_kalshi.db.sqlite_manager import SQLiteStateManager

LIVE_MODE = "live"

STATES: Set[str] = {
    "DISARMED",
    "ARMED",
    "READY",
    "ACTIVE",
    "PAUSED",
    "STOPPED",
    "KILLED",
    "RECOVERY_PENDING",
}

VALID_TRANSITIONS: Set[Tuple[str, str]] = {
    ("DISARMED", "ARMED"),
    ("ARMED", "DISARMED"),
    ("ARMED", "READY"),
    ("READY", "ARMED"),
    ("READY", "ACTIVE"),
    ("ACTIVE", "PAUSED"),
    ("ACTIVE", "STOPPED"),
    ("ACTIVE", "KILLED"),
    ("PAUSED", "ACTIVE"),
    ("PAUSED", "STOPPED"),
    ("PAUSED", "KILLED"),
    ("STOPPED", "RECOVERY_PENDING"),
    ("STOPPED", "KILLED"),
    ("RECOVERY_PENDING", "ACTIVE"),
    ("RECOVERY_PENDING", "STOPPED"),
    ("KILLED", "DISARMED"),
}


class LifecycleStateMachine:
    """
    Manages LIVE lifecycle only. Enforces strict valid transitions.
    Always reads/writes state via SQLite (bot_state for live).
    """

    def __init__(
        self,
        db: SQLiteStateManager,
        event_logger: Optional[Any] = None,
    ) -> None:
        self._db = db
        self._event_logger = event_logger
        self._logger = get_logger("state-machine")

    async def get_state(self) -> str:
        """Current lifecycle state from SQLite (live)."""
        state = await self._db.get_bot_state(mode=LIVE_MODE)
        return (state.get("lifecycle_state") or "DISARMED").upper()

    async def transition(self, to_state: str, reason: str = "") -> bool:
        """
        Transition to to_state if valid. Writes to SQLite and optionally logs event.
        Returns True if transition was applied, False if invalid.
        """
        to_state = to_state.upper()
        if to_state not in STATES:
            self._logger.warning(
                "Invalid state name",
                extra={"to_state": to_state, "reason": reason},
            )
            return False

        current = await self.get_state()
        if (current, to_state) not in VALID_TRANSITIONS:
            self._logger.warning(
                "Invalid lifecycle transition",
                extra={"from_state": current, "to_state": to_state, "reason": reason},
            )
            return False

        await self._db.update_bot_state(
            mode=LIVE_MODE,
            lifecycle_state=to_state,
            armed=1 if to_state in ("ARMED", "READY", "ACTIVE", "PAUSED", "RECOVERY_PENDING") else 0,
        )

        self._logger.info(
            "Lifecycle transition",
            extra={"from_state": current, "to_state": to_state, "reason": reason},
        )

        if self._event_logger is not None:
            self._event_logger.log_event(
                event_type="lifecycle_transition",
                severity="INFO",
                service_name="state-machine",
                contract_id=None,
                payload={
                    "from_state": current,
                    "to_state": to_state,
                    "reason": reason,
                },
                mode=LIVE_MODE,
            )

        return True

    async def can_accept_new_entries(self) -> bool:
        """True only when state is ACTIVE."""
        return await self.get_state() == "ACTIVE"

    async def can_manage_exits(self) -> bool:
        """True when state is ACTIVE, PAUSED, or STOPPED."""
        return await self.get_state() in ("ACTIVE", "PAUSED", "STOPPED")

    async def is_killed(self) -> bool:
        """True when state is KILLED."""
        return await self.get_state() == "KILLED"
