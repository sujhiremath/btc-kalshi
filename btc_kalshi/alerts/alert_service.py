"""
Alert service: ntfy.sh (httpx POST) + email (aiosmtplib).
CRITICAL → both, WARNING/INFO → ntfy. Fallback: ntfy fails → email.
Both fail → log ERROR, no crash. send_approval_request(signal) → approval_id.
"""
from __future__ import annotations

import uuid
from email.message import EmailMessage
from typing import Any, Optional

import aiosmtplib
import httpx

from btc_kalshi.core.logger import get_logger

NTFY_BASE = "https://ntfy.sh"
SMTP_PORT = 587


class AlertService:
    def __init__(self, settings: Any) -> None:
        self._settings = settings
        self._logger = get_logger("alert-service")

    async def _send_ntfy(self, message: str, title: Optional[str] = None) -> bool:
        topic = getattr(self._settings, "NTFY_TOPIC", None) or ""
        if not topic:
            self._logger.debug("NTFY_TOPIC not set, skipping ntfy")
            return False
        try:
            async with httpx.AsyncClient() as client:
                url = f"{NTFY_BASE}/{topic}"
                headers = {}
                if title:
                    headers["Title"] = title
                r = await client.post(url, content=message.encode("utf-8"), headers=headers or None)
                return 200 <= r.status_code < 300
        except Exception as e:
            self._logger.warning("ntfy send failed", extra={"error": str(e)})
            return False

    async def _send_email(self, message: str, title: Optional[str] = None) -> bool:
        host = getattr(self._settings, "SMTP_HOST", None) or ""
        to_addr = getattr(self._settings, "ALERT_EMAIL_TO", None) or ""
        user = getattr(self._settings, "SMTP_USER", None) or ""
        password = getattr(self._settings, "SMTP_PASS", None) or ""
        if not host or not to_addr:
            self._logger.debug("SMTP or ALERT_EMAIL_TO not set, skipping email")
            return False
        try:
            em = EmailMessage()
            em["From"] = user
            em["To"] = to_addr
            em["Subject"] = title or "BTC-Kalshi Alert"
            em.set_content(message)
            await aiosmtplib.send(
                em,
                hostname=host,
                port=SMTP_PORT,
                username=user or None,
                password=password or None,
                start_tls=True,
            )
            return True
        except Exception as e:
            self._logger.warning("email send failed", extra={"error": str(e)})
            return False

    async def send(
        self,
        level: str,
        message: str,
        title: Optional[str] = None,
    ) -> None:
        """
        CRITICAL → both ntfy and email. WARNING/INFO → ntfy only.
        Fallback: ntfy fails → try email. Both fail → log ERROR, no crash.
        """
        level_upper = (level or "INFO").upper()
        ok_ntfy = await self._send_ntfy(message, title=title or level_upper)

        if level_upper == "CRITICAL":
            ok_email = await self._send_email(message, title=title or level_upper)
        else:
            ok_email = False
            if not ok_ntfy:
                ok_email = await self._send_email(message, title=title or level_upper)

        if not ok_ntfy and not ok_email:
            self._logger.error(
                "Alert delivery failed (ntfy and email)",
                extra={"level": level_upper, "message_preview": message[:100]},
            )

    async def send_approval_request(self, signal: Any) -> str:
        """
        Send an approval request alert for the given signal. Returns approval_id.
        """
        approval_id = uuid.uuid4().hex
        contract_id = getattr(signal, "contract_id", "?")
        side = getattr(signal, "side", "?")
        message = f"Approval request {approval_id}: {contract_id} {side}"
        await self.send(level="INFO", message=message, title="Approval request")
        return approval_id
