"""Tests for lifecycle state machine (LIVE only, strict transitions)."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from btc_kalshi.core.state_machine import LifecycleStateMachine


@pytest.mark.asyncio
async def test_valid_transition_disarmed_to_armed(state_manager):
    """DISARMED -> ARMED is allowed."""
    sm = LifecycleStateMachine(state_manager)
    # Initial state is DISARMED
    current = await sm.get_state()
    assert current == "DISARMED"

    ok = await sm.transition("ARMED", reason="operator_arm")
    assert ok is True
    assert await sm.get_state() == "ARMED"


@pytest.mark.asyncio
async def test_invalid_transition_disarmed_to_active(state_manager):
    """DISARMED -> ACTIVE is not allowed."""
    sm = LifecycleStateMachine(state_manager)
    ok = await sm.transition("ACTIVE", reason="invalid")
    assert ok is False
    assert await sm.get_state() == "DISARMED"


@pytest.mark.asyncio
async def test_kill_switch_from_any_active_state(state_manager):
    """From ACTIVE, PAUSED, or STOPPED we can transition to KILLED."""
    sm = LifecycleStateMachine(state_manager)
    # DISARMED -> ARMED -> READY -> ACTIVE
    await sm.transition("ARMED", reason="arm")
    await sm.transition("READY", reason="ready")
    await sm.transition("ACTIVE", reason="go")
    assert await sm.get_state() == "ACTIVE"

    ok = await sm.transition("KILLED", reason="kill_switch")
    assert ok is True
    assert await sm.get_state() == "KILLED"
    assert await sm.is_killed() is True

    # From PAUSED -> KILLED
    await state_manager.update_bot_state(mode="live", lifecycle_state="PAUSED")
    ok = await sm.transition("KILLED", reason="kill")
    assert ok is True
    assert await sm.get_state() == "KILLED"

    # From STOPPED -> KILLED
    await state_manager.update_bot_state(mode="live", lifecycle_state="STOPPED")
    ok = await sm.transition("KILLED", reason="kill")
    assert ok is True


@pytest.mark.asyncio
async def test_can_accept_entries_only_active(state_manager):
    """can_accept_new_entries() is True only when state is ACTIVE."""
    sm = LifecycleStateMachine(state_manager)
    assert await sm.can_accept_new_entries() is False  # DISARMED

    await sm.transition("ARMED", reason="arm")
    assert await sm.can_accept_new_entries() is False

    await sm.transition("READY", reason="ready")
    assert await sm.can_accept_new_entries() is False

    await sm.transition("ACTIVE", reason="go")
    assert await sm.can_accept_new_entries() is True

    await sm.transition("PAUSED", reason="pause")
    assert await sm.can_accept_new_entries() is False


@pytest.mark.asyncio
async def test_can_manage_exits(state_manager):
    """can_manage_exits() is True for ACTIVE, PAUSED, STOPPED."""
    sm = LifecycleStateMachine(state_manager)
    assert await sm.can_manage_exits() is False  # DISARMED

    for state in ("ACTIVE", "PAUSED", "STOPPED"):
        await state_manager.update_bot_state(mode="live", lifecycle_state=state)
        assert await sm.can_manage_exits() is True

    await state_manager.update_bot_state(mode="live", lifecycle_state="ARMED")
    assert await sm.can_manage_exits() is False


@pytest.mark.asyncio
async def test_recovery_pending_flow(state_manager):
    """STOPPED -> RECOVERY_PENDING -> ACTIVE or STOPPED."""
    sm = LifecycleStateMachine(state_manager)
    await state_manager.update_bot_state(mode="live", lifecycle_state="STOPPED")

    ok = await sm.transition("RECOVERY_PENDING", reason="start_recovery")
    assert ok is True
    assert await sm.get_state() == "RECOVERY_PENDING"

    ok = await sm.transition("ACTIVE", reason="recovered")
    assert ok is True
    assert await sm.get_state() == "ACTIVE"

    await state_manager.update_bot_state(mode="live", lifecycle_state="RECOVERY_PENDING")
    ok = await sm.transition("STOPPED", reason="recovery_failed")
    assert ok is True


@pytest.mark.asyncio
async def test_transition_logs_event(state_manager):
    """transition() logs an event when event_logger is provided."""
    event_logger = MagicMock()
    event_logger.log_event = MagicMock()  # sync for simplicity

    sm = LifecycleStateMachine(state_manager, event_logger=event_logger)
    await sm.transition("ARMED", reason="test_arm")

    event_logger.log_event.assert_called()
    call = event_logger.log_event.call_args
    assert call.kwargs.get("event_type") == "lifecycle_transition" or "transition" in str(call).lower()
    assert call.kwargs.get("payload", {}).get("to_state") == "ARMED"
    assert call.kwargs.get("payload", {}).get("reason") == "test_arm"
