"""
Trading window enforcer: trading days, active hours (ET), entry/exit rules, macro avoid.
All times Eastern Time (ET).
"""
from __future__ import annotations

import json
from datetime import datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")

# Active window: 9:30 AM - 8:00 PM ET
ACTIVE_START = time(9, 30)
ACTIVE_END = time(20, 0)

# Entry cutoff: no new entries at or after 7 PM ET
ENTRY_CUTOFF = time(19, 0)

# Avoid windows (ET): no new entries during these intervals
AVOID_WINDOWS: list[tuple[time, time]] = [
    (time(9, 30), time(9, 45)),   # 9:30-9:45
    (time(12, 0), time(13, 0)),    # 12:00-1:00 lunch
    (time(14, 45), time(15, 0)),    # 2:45-3:00
]

# Macro event window: 15 minutes before/after
MACRO_WINDOW_MINUTES = 15


def _to_et(when: datetime | None) -> datetime:
    """Return current time in ET or convert when to ET (naive assumed ET)."""
    if when is None:
        return datetime.now(ET)
    if when.tzinfo is None:
        return when.replace(tzinfo=ET)
    return when.astimezone(ET)


def _in_time_range(t: time, start: time, end: time) -> bool:
    """True if time t is in [start, end); end is exclusive for same-day ranges."""
    if start <= end:
        return start <= t < end
    return t >= start or t < end


def _in_avoid_window(t: time) -> bool:
    """True if t falls inside any avoid window."""
    for start, end in AVOID_WINDOWS:
        if _in_time_range(t, start, end):
            return True
    return False


class TradingWindowEnforcer:
    """
    Enforces trading day, active window (9:30 AM–8 PM ET), entry cutoff (before 7 PM),
    avoid windows (9:30–9:45, 12:00–1:00, 2:45–3:00), and macro-event proximity.
    """

    def __init__(self, calendar_path: str | Path = "data/macro_calendar.json") -> None:
        self._calendar_path = Path(calendar_path)

    def _load_macro_events(self) -> list[datetime]:
        """Load macro event datetimes (ET) from JSON."""
        if not self._calendar_path.exists():
            return []
        raw = self._calendar_path.read_text(encoding="utf-8")
        data = json.loads(raw)
        events: list[datetime] = []
        for item in data:
            dt_str = item.get("datetime_et")
            if not dt_str:
                continue
            try:
                # Parse as naive ET
                dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
                if dt.tzinfo is not None:
                    dt = dt.astimezone(ET).replace(tzinfo=ET)
                else:
                    dt = dt.replace(tzinfo=ET)
                events.append(dt)
            except (ValueError, TypeError):
                continue
        return events

    def is_trading_day(self, when: datetime | None = None) -> bool:
        """True if the given time (ET) is a weekday (trading day)."""
        et = _to_et(when)
        return et.weekday() < 5  # Monday=0 .. Friday=4

    def is_in_active_window(self, when: datetime | None = None) -> bool:
        """True if within active window 9:30 AM–8:00 PM ET."""
        et = _to_et(when)
        t = et.time()
        return _in_time_range(t, ACTIVE_START, ACTIVE_END)

    def is_near_macro_event(self, when: datetime | None = None) -> bool:
        """True if within 15 minutes before or after any macro event."""
        et = _to_et(when)
        events = self._load_macro_events()
        delta = timedelta(minutes=MACRO_WINDOW_MINUTES)
        for ev in events:
            if ev - delta <= et <= ev + delta:
                return True
        return False

    def is_entry_allowed(self, when: datetime | None = None) -> bool:
        """
        True if: trading day, in active window, before 7 PM ET,
        not in avoid windows (9:30–9:45, 12:00–1:00, 2:45–3:00), and not near macro.
        """
        et = _to_et(when)
        if not self.is_trading_day(et):
            return False
        if not self.is_in_active_window(et):
            return False
        t = et.time()
        if t >= ENTRY_CUTOFF:
            return False
        if _in_avoid_window(t):
            return False
        if self.is_near_macro_event(et):
            return False
        return True

    def is_exit_management_allowed(self, when: datetime | None = None) -> bool:
        """True if trading day and within active window (9:30 AM–8 PM ET)."""
        et = _to_et(when)
        return self.is_trading_day(et) and self.is_in_active_window(et)
