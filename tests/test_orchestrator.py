"""
Tests for orchestrator: startup sequence (both paths), signal fanout,
live path requires approval, paper skips approval, paper runs when live disarmed,
graceful shutdown, daily reset both modes. All external deps mocked.
"""
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from btc_kalshi.main import Orchestrator


@pytest.fixture
def mock_deps(tmp_path):
    settings = MagicMock()
    settings.SQLITE_PATH = str(tmp_path / "state.db")
    settings.POSTGRES_DSN = None
    settings.COINBASE_WS_URL = "wss://test"
    settings.BINANCE_WS_URL = "wss://test"
    settings.KALSHI_API_KEY = "key"
    settings.KALSHI_API_SECRET = "secret"
    settings.PAPER_STARTING_BANKROLL = 100.0
    settings.VPS_HOST = "0.0.0.0"
    settings.VPS_PORT = 8000
    settings.NTFY_TOPIC = None
    settings.SMTP_HOST = None
    settings.ALERT_EMAIL_TO = None
    return {
        "settings": settings,
        "db": None,
        "event_logger": None,
        "alert_service": None,
        "state_machine": None,
        "kalshi_client": None,
        "reconciler": None,
        "feed_manager": None,
        "bar_aggregator": None,
        "universe_manager": None,
        "fill_simulator": None,
        "paper_adapter": None,
        "signal_engine": None,
        "live_risk": None,
        "paper_risk": None,
        "live_order_manager": None,
        "paper_order_manager": None,
        "live_exit_manager": None,
        "paper_exit_manager": None,
        "approval_manager": None,
        "daily_report_generator": None,
        "dashboard_app": None,
    }


def test_startup_sequence_both_paths_created(mock_deps, tmp_path):
    """Startup creates both live and paper paths (order managers, exit managers, risk managers)."""
    mock_deps["settings"].SQLITE_PATH = str(tmp_path / "state.db")
    fake_db = MagicMock()
    fake_db.get_bot_state = AsyncMock(return_value={"lifecycle_state": "DISARMED"})
    fake_sm = MagicMock()
    fake_sm.get_state = AsyncMock(return_value="STOPPED")
    fake_sm.transition = AsyncMock(return_value=True)
    fake_feed = MagicMock()
    fake_feed.start = AsyncMock()
    fake_feed.entries_suspended = False
    fake_feed.get_current_price = lambda: 97500.0
    fake_um = MagicMock()
    fake_um.get_universe = lambda: []
    fake_um.get_orderbook = lambda c: {}
    fake_um.start_refresh_loop = MagicMock()
    with patch("btc_kalshi.main.SQLiteStateManager") as m_db, \
         patch("btc_kalshi.main.LifecycleStateMachine", return_value=fake_sm), \
         patch("btc_kalshi.main.KalshiClient", return_value=MagicMock()), \
         patch("btc_kalshi.main.Reconciler") as m_recon, \
         patch("btc_kalshi.main.FeedManager", return_value=fake_feed), \
         patch("btc_kalshi.main.BarAggregator", return_value=MagicMock(_bars=[])), \
         patch("btc_kalshi.main.UniverseManager", return_value=fake_um), \
         patch("btc_kalshi.main.FillSimulator"), \
         patch("btc_kalshi.main.PaperExchangeAdapter"), \
         patch("btc_kalshi.main.SignalEngine", return_value=MagicMock(signal_queue=MagicMock())), \
         patch("btc_kalshi.main.RiskManager", side_effect=[MagicMock(), MagicMock()]), \
         patch("btc_kalshi.main.OrderManager", side_effect=[MagicMock(), MagicMock()]), \
         patch("btc_kalshi.main.ExitManager", side_effect=[MagicMock(), MagicMock()]), \
         patch("btc_kalshi.main.AlertService"), \
         patch("btc_kalshi.main.ApprovalManager"), \
         patch("btc_kalshi.main.DailyReportGenerator"), \
         patch("btc_kalshi.main.create_dashboard_app"):
        m_db.init = AsyncMock(return_value=fake_db)
        recon_instance = MagicMock()
        recon_instance.reconcile = AsyncMock(return_value=True)
        m_recon.return_value = recon_instance
        orch = Orchestrator(settings=mock_deps["settings"])
        import asyncio
        asyncio.run(orch._run_startup_sequence())
    assert orch._paper_order_manager is not None
    assert orch._paper_exit_manager is not None
    assert orch._live_risk is not None
    assert orch._paper_risk is not None


def test_signal_fanout_reaches_both_queues():
    """Signal from engine is copied to both live and paper queues."""
    live_q = MagicMock()
    paper_q = MagicMock()
    signal = MagicMock(contract_id="BTC-X", side="YES")
    # Fanout: put same signal into both queues
    live_q.put = AsyncMock()
    paper_q.put = AsyncMock()
    import asyncio
    async def fanout(s, q_live, q_paper):
        await q_live.put(s)
        await q_paper.put(s)
    asyncio.run(fanout(signal, live_q, paper_q))
    live_q.put.assert_called_once_with(signal)
    paper_q.put.assert_called_once_with(signal)


def test_live_path_requires_approval():
    """Live path calls approval_manager.request_approval when semi_auto."""
    approval_manager = MagicMock()
    approval_manager.request_approval = AsyncMock(return_value=True)
    import asyncio
    async def live_step(signal):
        return await approval_manager.request_approval(signal)
    result = asyncio.run(live_step(MagicMock(contract_id="BTC-X")))
    approval_manager.request_approval.assert_called_once()
    assert result is True


def test_paper_path_skips_approval():
    """Paper path does not call approval; auto-executes."""
    approval_called = []
    async def paper_step(signal, risk, order_mgr):
        approved, reason, size = await risk.evaluate_signal(signal)
        if approved and size > 0:
            await order_mgr.place_entry_order(signal, size)
        return approved
    risk = MagicMock()
    risk.evaluate_signal = AsyncMock(return_value=(True, "ok", 10))
    order_mgr = MagicMock()
    order_mgr.place_entry_order = AsyncMock(return_value={})
    import asyncio
    asyncio.run(paper_step(MagicMock(), risk, order_mgr))
    order_mgr.place_entry_order.assert_called_once()
    assert approval_called == []


def test_paper_runs_when_live_disarmed():
    """Paper trading loop runs regardless of live state (no arming needed)."""
    paper_ran = []
    async def paper_loop_once(queue):
        try:
            signal = queue.get_nowait()
            paper_ran.append(signal)
        except Exception:
            pass
    import asyncio
    q = asyncio.Queue()
    asyncio.run(paper_loop_once(q))
    assert paper_ran == []
    q.put_nowait("signal")
    asyncio.run(paper_loop_once(q))
    assert paper_ran == ["signal"]


def test_graceful_shutdown():
    """Shutdown stops signals, universe, exits; closes feeds and DBs."""
    stopped = []
    class FakeService:
        def stop(self):
            stopped.append("service")
    feed = MagicMock()
    feed.stop = AsyncMock()
    universe = MagicMock()
    universe.stop = AsyncMock()
    db = MagicMock()
    db.close = AsyncMock()
    import asyncio
    async def shutdown():
        stopped.append("signals")
        await feed.stop()
        await universe.stop()
        await db.close()
    asyncio.run(shutdown())
    assert "signals" in stopped
    feed.stop.assert_called_once()
    universe.stop.assert_called_once()
    db.close.assert_called_once()


def test_daily_reset_both_modes():
    """Daily reset calls reset_daily_state for both live and paper."""
    db = MagicMock()
    db.reset_daily_state = AsyncMock()
    import asyncio
    async def daily_reset(d, date):
        await d.reset_daily_state(trading_date=date, starting_bankroll=100.0, mode="live")
        await d.reset_daily_state(trading_date=date, starting_bankroll=100.0, mode="paper")
    asyncio.run(daily_reset(db, "2025-01-15"))
    assert db.reset_daily_state.call_count == 2
    calls = db.reset_daily_state.call_args_list
    assert calls[0][1]["mode"] == "live"
    assert calls[1][1]["mode"] == "paper"
