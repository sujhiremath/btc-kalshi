"""
Kill switch: LIVE only (paper has no real money). Within 5s: suspend signals,
cancel orders, market-close live positions, set KILLED. Independent of risk manager. CRITICAL log.
heartbeat_monitor(services): 5s check, 60s escalation.
"""
from __future__ import annotations

import asyncio
from typing import Any, Callable, Dict

from btc_kalshi.core.logger import get_logger

KILL_SWITCH_TIMEOUT_SECONDS = 5
HEARTBEAT_CHECK_SECONDS = 5
HEARTBEAT_ESCALATION_SECONDS = 60


def _exit_side(side: str) -> str:
    return "no" if (side or "yes").lower() == "yes" else "yes"


async def execute_kill_switch(
    exchange: Any,
    sqlite_manager: Any,
    state_machine: Any,
    suspend_signals: Any,
) -> None:
    """
    Within 5s: suspend signals, cancel all open orders, market-close live positions, set KILLED.
    Independent of risk manager. CRITICAL log.
    """
    logger = get_logger("kill-switch")
    logger.critical("KILL SWITCH EXECUTING", extra={"reason": "kill_switch"})

    async def _do() -> None:
        await suspend_signals()
        orders = await exchange.get_open_orders(contract_id=None)
        for o in orders:
            oid = o.get("id") or o.get("order_id")
            if oid:
                try:
                    await exchange.cancel_order(oid)
                except Exception as e:
                    logger.warning("Kill switch: cancel order failed", extra={"order_id": oid, "error": str(e)})
        positions = await exchange.get_positions()
        for p in positions:
            contract_id = p.get("ticker") or p.get("contract_id") or ""
            size = int(p.get("position") or p.get("size") or 0)
            side = (p.get("side") or "yes").lower()
            if contract_id and size > 0:
                try:
                    await exchange.place_order(
                        contract_id=contract_id,
                        side=_exit_side(side),
                        count=size,
                        type="market",
                    )
                except Exception as e:
                    logger.warning(
                        "Kill switch: market close failed",
                        extra={"contract_id": contract_id, "error": str(e)},
                    )
        if state_machine is not None:
            await state_machine.transition("KILLED", "kill_switch")

    try:
        await asyncio.wait_for(_do(), timeout=KILL_SWITCH_TIMEOUT_SECONDS)
    except asyncio.TimeoutError:
        logger.critical("KILL SWITCH TIMEOUT (5s) — partial execution", extra={})
    logger.critical("KILL SWITCH COMPLETE", extra={})


class HeartbeatMonitor:
    """5s check, 60s escalation. services: dict name -> last_heartbeat_ts (float)."""

    def __init__(
        self,
        escalation_callback: Callable[[str], Any],
        check_interval: float = HEARTBEAT_CHECK_SECONDS,
        escalation_after_seconds: float = HEARTBEAT_ESCALATION_SECONDS,
    ) -> None:
        self._services: Dict[str, float] = {}
        self._escalation = escalation_callback
        self._interval = check_interval
        self._escalation_after = escalation_after_seconds
        self._stop = False
        self._logger = get_logger("heartbeat-monitor")

    def heartbeat(self, service_name: str) -> None:
        import time
        self._services[service_name] = time.monotonic()

    def register(self, service_name: str) -> None:
        import time
        self._services[service_name] = time.monotonic()

    async def run(self) -> None:
        import time
        self._stop = False
        while not self._stop:
            now = time.monotonic()
            for name, last in list(self._services.items()):
                if now - last > self._escalation_after:
                    self._logger.critical(
                        "Heartbeat escalation: service stale",
                        extra={"service": name, "last_heartbeat_ago": now - last},
                    )
                    try:
                        self._escalation(name)
                    except Exception as e:
                        self._logger.exception("Escalation callback error: %s", e)
            await asyncio.sleep(self._interval)

    def stop(self) -> None:
        self._stop = True


def heartbeat_monitor(services: Dict[str, float]) -> None:
    """
    Standalone: 5s check, 60s escalation. services: name -> last_heartbeat_ts.
    Use HeartbeatMonitor for async loop; this is a simple predicate/helper.
    """
    import time
    now = time.monotonic()
    for name, last in services.items():
        if now - last > HEARTBEAT_ESCALATION_SECONDS:
            get_logger("heartbeat-monitor").critical(
                "Heartbeat escalation: service stale",
                extra={"service": name, "last_heartbeat_ago": now - last},
            )
