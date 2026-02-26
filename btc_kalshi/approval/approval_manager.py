"""
Approval gates LIVE entries only. Paper never calls this (auto-approves instead).
semi_auto → ntfy + wait 60s; full_auto → True immediately. Timeout → False, no penalty.
"""
from __future__ import annotations

import asyncio
from typing import Any, Dict, Optional

from btc_kalshi.core.logger import get_logger

APPROVAL_TIMEOUT_SECONDS = 60


class ApprovalManager:
    """
    request_approval(signal) → bool. receive_approval(approval_id, approved).
    Only used for LIVE; paper should auto-approve and not call this.
    """

    def __init__(
        self,
        alert_service: Any,
        event_logger: Optional[Any] = None,
        mode: str = "semi_auto",
    ) -> None:
        self._alert_service = alert_service
        self._event_logger = event_logger
        self._mode = (mode or "semi_auto").lower()
        self._logger = get_logger("approval-manager")
        self._pending: Dict[str, asyncio.Future] = {}
        self._timeout_seconds = APPROVAL_TIMEOUT_SECONDS

    def receive_approval(self, approval_id: str, approved: bool) -> None:
        """Record operator decision for the given approval_id."""
        if not approval_id:
            return
        fut = self._pending.get(approval_id)
        if fut is not None and not fut.done():
            fut.set_result(approved)
            self._pending.pop(approval_id, None)

    def _log_outcome(
        self,
        approval_id: str,
        outcome: str,
        contract_id: Optional[str] = None,
    ) -> None:
        if self._event_logger is None:
            return
        payload: Dict[str, Any] = {"approval_id": approval_id, "outcome": outcome}
        if contract_id is not None:
            payload["contract_id"] = contract_id
        try:
            self._event_logger.log_event(
                event_type="approval_outcome",
                severity="INFO",
                service_name="approval-manager",
                contract_id=contract_id,
                payload=payload,
                mode="live",
            )
        except Exception as e:
            self._logger.warning("event_logger.log_event failed", extra={"error": str(e)})

    async def request_approval(self, signal: Any) -> bool:
        """
        full_auto → return True immediately. semi_auto → send ntfy, wait up to 60s
        for receive_approval(approval_id, approved). Timeout → False, no penalty.
        """
        if self._mode == "full_auto":
            return True

        contract_id = getattr(signal, "contract_id", None)
        approval_id = await self._alert_service.send_approval_request(signal)
        fut: asyncio.Future[bool] = asyncio.get_running_loop().create_future()
        self._pending[approval_id] = fut

        try:
            result = await asyncio.wait_for(fut, timeout=self._timeout_seconds)
            outcome = "approved" if result else "rejected"
            self._log_outcome(approval_id, outcome, contract_id=contract_id)
            return result
        except asyncio.TimeoutError:
            self._pending.pop(approval_id, None)
            self._log_outcome(approval_id, "timeout", contract_id=contract_id)
            self._logger.info(
                "Approval timeout",
                extra={"approval_id": approval_id, "contract_id": contract_id},
            )
            return False
        finally:
            self._pending.pop(approval_id, None)
