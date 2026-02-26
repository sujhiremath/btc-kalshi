"""
Tests for FastAPI dashboard: status, positions, signals, arm/disarm/kill, approve/reject.
"""
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi.testclient import TestClient

from btc_kalshi.dashboard.app import create_app


@pytest.fixture
def mock_db():
    db = MagicMock()
    db.get_bot_state = AsyncMock(side_effect=lambda mode=None: {
        "lifecycle_state": "DISARMED" if mode == "live" else "ACTIVE",
        "daily_pnl_net": 10.0 if mode == "live" else 5.0,
        "daily_pnl_gross": 12.0,
        "mode": mode or "live",
    })
    db.get_open_positions = AsyncMock(return_value=[])
    return db


@pytest.fixture
def mock_state_machine():
    sm = MagicMock()
    sm.get_state = AsyncMock(return_value="DISARMED")
    sm.transition = AsyncMock(return_value=True)
    return sm


@pytest.fixture
def mock_approval_manager():
    am = MagicMock()
    am.receive_approval = MagicMock()
    am.get_pending_approval_ids = MagicMock(return_value=[])
    return am


@pytest.fixture
def client(mock_db, mock_state_machine, mock_approval_manager):
    def get_btc_price():
        return 97500.0

    signal_log = [
        {"contract_id": "BTC-20250115", "side": "YES", "timestamp": "2025-01-15T12:00:00Z"},
    ]
    app = create_app(
        db=mock_db,
        state_machine=mock_state_machine,
        approval_manager=mock_approval_manager,
        get_btc_price=get_btc_price,
        signal_log=signal_log,
    )
    return TestClient(app)


def test_status_endpoint_returns_live_and_paper(client, mock_db):
    """GET /api/status returns both modes' stats."""
    mock_db.get_bot_state = AsyncMock(side_effect=[
        {"lifecycle_state": "ARMED", "daily_pnl_net": 1.0, "mode": "live"},
        {"lifecycle_state": "ACTIVE", "daily_pnl_net": 2.0, "mode": "paper"},
    ])
    response = client.get("/api/status")
    assert response.status_code == 200
    data = response.json()
    assert "live" in data
    assert "paper" in data
    assert data["live"]["lifecycle_state"] == "ARMED"
    assert data["paper"]["lifecycle_state"] == "ACTIVE"


def test_positions_mode_filter(client, mock_db):
    """GET /api/positions?mode=paper returns paper only."""
    mock_db.get_open_positions = AsyncMock(return_value=[
        {"position_id": "p1", "contract_id": "BTC-X", "filled_size": 10, "mode": "paper"},
    ])
    response = client.get("/api/positions?mode=paper")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) == 1
    mock_db.get_open_positions.assert_called_once_with(mode="paper")


def test_arm_endpoint(client, mock_state_machine):
    """POST /api/arm transitions to ARMED."""
    response = client.post("/api/arm")
    assert response.status_code in (200, 204)
    mock_state_machine.transition.assert_called_with("ARMED", "dashboard")


def test_kill_endpoint(client, mock_state_machine):
    """POST /api/kill transitions to KILLED."""
    response = client.post("/api/kill")
    assert response.status_code in (200, 204)
    mock_state_machine.transition.assert_called_with("KILLED", "dashboard")


def test_approve_endpoint(client, mock_approval_manager):
    """POST /api/approve/{id} calls receive_approval(id, True)."""
    response = client.post("/api/approve/abc123")
    assert response.status_code in (200, 204)
    mock_approval_manager.receive_approval.assert_called_with("abc123", True)
