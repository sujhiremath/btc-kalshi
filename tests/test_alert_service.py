"""
Tests for AlertService: ntfy + email. CRITICAL→both, WARNING/INFO→ntfy.
Fallback: ntfy fails→email. Both fail→log ERROR, no crash.
"""
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from btc_kalshi.alerts.alert_service import AlertService


@pytest.fixture
def settings():
    s = MagicMock()
    s.NTFY_TOPIC = "test-topic"
    s.SMTP_HOST = "smtp.test"
    s.SMTP_USER = "user@test"
    s.SMTP_PASS = "secret"
    s.ALERT_EMAIL_TO = "alerts@test"
    return s


@pytest.fixture
def alert_service(settings):
    return AlertService(settings=settings)


@pytest.mark.asyncio
async def test_critical_sends_both(alert_service, settings):
    """CRITICAL severity sends to both ntfy and email."""
    with patch("btc_kalshi.alerts.alert_service.httpx") as m_httpx, patch(
        "btc_kalshi.alerts.alert_service.aiosmtplib"
    ) as m_smtp:
        m_httpx.AsyncClient.return_value.__aenter__.return_value.post = AsyncMock(
            return_value=MagicMock(status_code=200)
        )
        m_smtp.send.return_value = AsyncMock()

        await alert_service.send(level="CRITICAL", message="Critical alert")

        m_httpx.AsyncClient.return_value.__aenter__.return_value.post.assert_called_once()
        m_smtp.send.assert_called_once()


@pytest.mark.asyncio
async def test_info_sends_ntfy_only(alert_service):
    """INFO (and WARNING) send to ntfy only, not email."""
    with patch("btc_kalshi.alerts.alert_service.httpx") as m_httpx, patch(
        "btc_kalshi.alerts.alert_service.aiosmtplib"
    ) as m_smtp:
        m_httpx.AsyncClient.return_value.__aenter__.return_value.post = AsyncMock(
            return_value=MagicMock(status_code=200)
        )

        await alert_service.send(level="INFO", message="Info message")

        m_httpx.AsyncClient.return_value.__aenter__.return_value.post.assert_called_once()
        m_smtp.send.assert_not_called()


@pytest.mark.asyncio
async def test_ntfy_failure_falls_back(alert_service):
    """When ntfy fails, fall back to email."""
    with patch("btc_kalshi.alerts.alert_service.httpx") as m_httpx, patch(
        "btc_kalshi.alerts.alert_service.aiosmtplib"
    ) as m_smtp:
        m_httpx.AsyncClient.return_value.__aenter__.return_value.post = AsyncMock(
            side_effect=Exception("ntfy failed")
        )
        m_smtp.send.return_value = AsyncMock()

        await alert_service.send(level="INFO", message="Fallback test")

        m_smtp.send.assert_called_once()


@pytest.mark.asyncio
async def test_both_fail_no_crash(alert_service):
    """When both ntfy and email fail, log ERROR and do not raise."""
    mock_logger = MagicMock()
    with patch("btc_kalshi.alerts.alert_service.httpx") as m_httpx, patch(
        "btc_kalshi.alerts.alert_service.aiosmtplib"
    ) as m_smtp, patch.object(alert_service, "_logger", mock_logger):
        m_httpx.AsyncClient.return_value.__aenter__.return_value.post = AsyncMock(
            side_effect=Exception("ntfy failed")
        )
        m_smtp.send.return_value = AsyncMock(side_effect=Exception("smtp failed"))

        await alert_service.send(level="CRITICAL", message="Both fail")

        mock_logger.error.assert_called()
        # No exception propagates
        assert True
