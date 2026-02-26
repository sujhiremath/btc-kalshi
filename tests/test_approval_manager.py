"""
Tests for ApprovalManager: gates LIVE entries only. full_auto→True immediately,
semi_auto→ntfy+wait 60s. request_approval(signal)→bool, receive_approval(approval_id, approved). Timeout→False.
"""
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from btc_kalshi.approval.approval_manager import ApprovalManager


@pytest.fixture
def alert_service():
    s = MagicMock()
    s.send_approval_request = AsyncMock(return_value="test-approval-id")
    return s


@pytest.fixture
def event_logger():
    return MagicMock()


@pytest.mark.asyncio
async def test_full_auto_approves_immediately(alert_service, event_logger):
    """full_auto mode: request_approval returns True immediately, no ntfy."""
    manager = ApprovalManager(
        alert_service=alert_service,
        event_logger=event_logger,
        mode="full_auto",
    )
    signal = MagicMock(contract_id="BTC-20250115", side="YES")

    result = await manager.request_approval(signal)

    assert result is True
    alert_service.send_approval_request.assert_not_called()


@pytest.mark.asyncio
async def test_semi_auto_approved(alert_service, event_logger):
    """semi_auto: send request, receive_approval(approval_id, True) → request_approval returns True."""
    manager = ApprovalManager(
        alert_service=alert_service,
        event_logger=event_logger,
        mode="semi_auto",
    )
    signal = MagicMock(contract_id="BTC-20250115", side="YES")

    async def approve_soon():
        import asyncio
        await asyncio.sleep(0.02)
        manager.receive_approval("test-approval-id", True)

    import asyncio
    task = asyncio.create_task(manager.request_approval(signal))
    await asyncio.sleep(0.01)
    await approve_soon()
    result = await task

    assert result is True
    alert_service.send_approval_request.assert_called_once_with(signal)


@pytest.mark.asyncio
async def test_semi_auto_rejected(alert_service, event_logger):
    """semi_auto: receive_approval(approval_id, False) → request_approval returns False."""
    manager = ApprovalManager(
        alert_service=alert_service,
        event_logger=event_logger,
        mode="semi_auto",
    )
    signal = MagicMock(contract_id="BTC-20250115", side="YES")

    async def reject_soon():
        import asyncio
        await asyncio.sleep(0.02)
        manager.receive_approval("test-approval-id", False)

    import asyncio
    task = asyncio.create_task(manager.request_approval(signal))
    await asyncio.sleep(0.01)
    await reject_soon()
    result = await task

    assert result is False
    alert_service.send_approval_request.assert_called_once_with(signal)


@pytest.mark.asyncio
async def test_semi_auto_timeout(alert_service, event_logger):
    """semi_auto: no receive_approval within wait → request_approval returns False."""
    manager = ApprovalManager(
        alert_service=alert_service,
        event_logger=event_logger,
        mode="semi_auto",
    )
    signal = MagicMock(contract_id="BTC-20250115", side="YES")

    with patch.object(manager, "_timeout_seconds", 0.05):
        result = await manager.request_approval(signal)

    assert result is False
    alert_service.send_approval_request.assert_called_once_with(signal)


@pytest.mark.asyncio
async def test_approval_logs_outcome(alert_service, event_logger):
    """Approval outcome (approved/rejected/timeout) is logged via event_logger."""
    manager = ApprovalManager(
        alert_service=alert_service,
        event_logger=event_logger,
        mode="semi_auto",
    )
    signal = MagicMock(contract_id="BTC-20250115", side="YES")

    import asyncio
    task = asyncio.create_task(manager.request_approval(signal))
    await asyncio.sleep(0.02)
    manager.receive_approval("test-approval-id", True)
    await task

    event_logger.log_event.assert_called()
    call = event_logger.log_event.call_args
    assert call.kwargs.get("event_type") == "approval_outcome"
    assert call.kwargs.get("payload", {}).get("outcome") == "approved"
