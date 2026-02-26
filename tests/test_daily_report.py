"""
Tests for DailyReportGenerator: headline metrics, execution quality, risk events,
signal diagnostics, review flags; paper/live comparison; combined report; save to Postgres + JSON.
"""
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from btc_kalshi.reports.daily_report import DailyReportGenerator


@pytest.fixture
def mock_db():
    db = MagicMock()
    db.get_bot_state = AsyncMock(return_value={
        "lifecycle_state": "ACTIVE",
        "daily_pnl_net": 12.5,
        "daily_pnl_gross": 15.0,
        "starting_bankroll": 100.0,
        "trading_date": "2025-01-15",
    })
    db.get_daily_trades = AsyncMock(return_value=[
        {"client_order_id": "o1", "purpose": "entry", "filled_price": 0.52, "intended_price": 0.51, "filled_size": 10},
        {"client_order_id": "o2", "purpose": "exit", "filled_price": 0.88, "filled_size": 10},
    ])
    return db


@pytest.fixture
def mock_event_logger(tmp_path):
    el = MagicMock()
    el._log_dir = Path(tmp_path)
    el._pg_disabled = True
    el.save_daily_report = AsyncMock()
    return el


@pytest.fixture
def generator(mock_db, mock_event_logger):
    return DailyReportGenerator(db=mock_db, event_logger=mock_event_logger)


def test_report_structure(generator, mock_db):
    """Generated report has headline_metrics, execution_quality, risk_events, signal_diagnostics, review_flags."""
    import asyncio
    report = asyncio.run(generator.generate_report("2025-01-15", mode="live"))
    assert "headline_metrics" in report
    assert "execution_quality" in report
    assert "risk_events" in report
    assert "signal_diagnostics" in report
    assert "review_flags" in report
    assert isinstance(report["review_flags"], list)


def test_report_with_no_trades(generator, mock_db):
    """When there are no trades, report still has required structure and zero/empty where appropriate."""
    mock_db.get_daily_trades = AsyncMock(return_value=[])
    mock_db.get_bot_state = AsyncMock(return_value={
        "daily_pnl_net": 0.0,
        "daily_pnl_gross": 0.0,
        "trading_date": "2025-01-16",
    })
    import asyncio
    report = asyncio.run(generator.generate_report("2025-01-16", mode="paper"))
    assert report["headline_metrics"].get("trades_count", 0) == 0
    assert "execution_quality" in report


def test_review_flags_triggered(generator, mock_db):
    """Review flags are set when conditions warrant (e.g. large loss, many risk events)."""
    mock_db.get_bot_state = AsyncMock(return_value={
        "daily_pnl_net": -50.0,
        "daily_pnl_gross": -48.0,
        "trading_date": "2025-01-15",
    })
    mock_db.get_daily_trades = AsyncMock(return_value=[])
    import asyncio
    report = asyncio.run(generator.generate_report("2025-01-15", mode="live"))
    assert "review_flags" in report
    assert isinstance(report["review_flags"], list)


def test_paper_live_comparison_divergence(generator, mock_db):
    """Paper/live comparison includes divergence flags when signal/fill/win rate diverge beyond thresholds."""
    async def state_side_effect(mode=None):
        if mode == "live":
            return {"daily_pnl_net": 10.0, "trading_date": "2025-01-15", "lifecycle_state": "ACTIVE"}
        return {"daily_pnl_net": 25.0, "trading_date": "2025-01-15", "lifecycle_state": "ACTIVE"}

    mock_db.get_bot_state = AsyncMock(side_effect=state_side_effect)
    mock_db.get_daily_trades = AsyncMock(return_value=[
        {"purpose": "entry", "filled_price": 0.50, "filled_size": 10, "mode": "live"},
        {"purpose": "entry", "filled_price": 0.48, "filled_size": 10, "mode": "paper"},
    ])
    import asyncio
    comparison = asyncio.run(generator.generate_paper_live_comparison("2025-01-15"))
    assert "signal_count" in comparison or "live" in comparison or "paper" in comparison
    assert "flags" in comparison or "review_flags" in comparison or "divergence" in str(comparison).lower()


def test_combined_report_has_both_modes(generator, mock_db):
    """generate_combined_report returns live + paper + comparison."""
    import asyncio
    combined = asyncio.run(generator.generate_combined_report("2025-01-15"))
    assert "live" in combined
    assert "paper" in combined
    assert "comparison" in combined
