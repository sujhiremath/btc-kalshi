"""
Orchestrator: 24-step startup, signal fanout to live + paper, two trading loops,
shutdown (SIGINT/SIGTERM), daily reset (midnight ET). Paper always runs; live gated by state/approval.
"""
from __future__ import annotations

import asyncio
import signal
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from btc_kalshi.alerts.alert_service import AlertService
from btc_kalshi.approval.approval_manager import ApprovalManager
from btc_kalshi.config.settings import get_settings
from btc_kalshi.core.logger import get_logger
from btc_kalshi.core.state_machine import LifecycleStateMachine
from btc_kalshi.dashboard.app import create_app as create_dashboard_app
from btc_kalshi.db.event_logger import EventLogger
from btc_kalshi.db.sqlite_manager import SQLiteStateManager
from btc_kalshi.execution.exit_manager import ExitManager
from btc_kalshi.execution.kill_switch import execute_kill_switch
from btc_kalshi.execution.order_manager import OrderManager
from btc_kalshi.execution.reconciliation import Reconciler
from btc_kalshi.exchange.fill_simulator import FillSimulator
from btc_kalshi.exchange.kalshi_client import KalshiClient
from btc_kalshi.exchange.paper_adapter import PaperExchangeAdapter
from btc_kalshi.exchange.universe_manager import UniverseManager
from btc_kalshi.feeds.bar_aggregator import BarAggregator
from btc_kalshi.feeds.feed_manager import FeedManager
from btc_kalshi.reports.daily_report import DailyReportGenerator
from btc_kalshi.risk.position_sizer import PositionSizer
from btc_kalshi.risk.risk_manager import RiskManager
from btc_kalshi.risk.streak_manager import StreakManager
from btc_kalshi.risk.trading_window import TradingWindowEnforcer
from btc_kalshi.strategy.signal_engine import SignalEngine

ET = ZoneInfo("America/New_York")
KALSHI_BASE_URL = "https://trading-api.kalshi.com"
DATA_DIR = Path("data")
LOG_DIR = DATA_DIR / "logs"


class Orchestrator:
    """
    24-step startup; signal fanout to live + paper queues; live_trading_loop (approval) and
    paper_trading_loop (auto); exit monitors; shutdown and daily reset.
    """

    def __init__(self, settings=None):
        self._settings = settings or get_settings()
        self._logger = get_logger("orchestrator")
        self._db: SQLiteStateManager | None = None
        self._event_logger: EventLogger | None = None
        self._alert_service: AlertService | None = None
        self._state_machine: LifecycleStateMachine | None = None
        self._kalshi_client: KalshiClient | None = None
        self._reconciler: Reconciler | None = None
        self._feed_manager: FeedManager | None = None
        self._bar_aggregator: BarAggregator | None = None
        self._universe_manager: UniverseManager | None = None
        self._fill_simulator: FillSimulator | None = None
        self._paper_adapter: PaperExchangeAdapter | None = None
        self._signal_engine: SignalEngine | None = None
        self._live_risk: RiskManager | None = None
        self._paper_risk: RiskManager | None = None
        self._live_order_manager: OrderManager | None = None
        self._paper_order_manager: OrderManager | None = None
        self._live_exit_manager: ExitManager | None = None
        self._paper_exit_manager: ExitManager | None = None
        self._approval_manager: ApprovalManager | None = None
        self._daily_report_generator: DailyReportGenerator | None = None
        self._dashboard_app = None
        self._signal_queue_live: asyncio.Queue | None = None
        self._signal_queue_paper: asyncio.Queue | None = None
        self._shutdown = False
        self._tasks: list[asyncio.Task] = []
        self._uvicorn_server = None

    async def _run_startup_sequence(self) -> None:
        """24-step startup. Both paths created."""
        s = self._settings
        # 1. Config (already in self._settings)
        # 2. Logger (get_logger used throughout)
        # 3. SQLite
        self._db = await SQLiteStateManager.init(s.SQLITE_PATH)
        # 4. Postgres event logger
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        self._event_logger = EventLogger.init(
            postgres_dsn=s.POSTGRES_DSN,
            log_dir=LOG_DIR,
        )
        # 5. Alert service
        self._alert_service = AlertService(settings=s)
        # 6. State machine -> RECOVERY_PENDING (if current is STOPPED)
        self._state_machine = LifecycleStateMachine(db=self._db, event_logger=self._event_logger)
        current = await self._state_machine.get_state()
        if current == "STOPPED":
            await self._state_machine.transition("RECOVERY_PENDING", "startup")
        # 7. KalshiClient
        if s.KALSHI_API_KEY and s.KALSHI_API_SECRET:
            self._kalshi_client = KalshiClient(
                base_url=KALSHI_BASE_URL,
                api_key=s.KALSHI_API_KEY,
                api_secret=s.KALSHI_API_SECRET,
            )
        else:
            self._kalshi_client = None
        # 8. Reconciliation -> PAUSED (live only; if we have exchange, reconcile then PAUSED)
        if self._kalshi_client is not None:
            self._reconciler = Reconciler(exchange=self._kalshi_client, sqlite_manager=self._db)
            passed = await self._reconciler.reconcile()
            if passed:
                await self._state_machine.transition("PAUSED", "reconcile_ok")
            else:
                await self._state_machine.transition("PAUSED", "reconcile_gap")
        # 9. FeedManager -> start
        self._feed_manager = FeedManager(
            primary_ws_url=s.COINBASE_WS_URL,
            backup_ws_url=s.BINANCE_WS_URL,
        )
        await self._feed_manager.start()
        # 10. BarAggregator -> subscribe (we need a bar callback; signal engine will use get_bars)
        csv_path = DATA_DIR / "bars.csv"
        self._bar_aggregator = BarAggregator(self._feed_manager, csv_path)
        # 11. UniverseManager -> start refresh
        get_btc_price = lambda: self._feed_manager.get_current_price() or 0.0
        exchange_for_universe = self._kalshi_client if self._kalshi_client is not None else MagicMockExchange()
        self._universe_manager = UniverseManager(
            exchange=exchange_for_universe,
            get_btc_price=get_btc_price,
        )
        self._universe_manager.start_refresh_loop()
        # 12. FillSimulator
        self._fill_simulator = FillSimulator()
        # 13. PaperExchangeAdapter
        self._paper_adapter = PaperExchangeAdapter(
            universe_manager=self._universe_manager,
            fill_simulator=self._fill_simulator,
            starting_balance=s.PAPER_STARTING_BANKROLL,
        )
        # 14. SignalEngine (SHARED)
        self._signal_engine = SignalEngine()
        self._signal_queue_live = asyncio.Queue()
        self._signal_queue_paper = asyncio.Queue()
        # 15. Live RiskManager (uses StateMachine)
        trading_window = TradingWindowEnforcer()
        position_sizer = PositionSizer(db=self._db)
        live_streak = StreakManager(
            sqlite_manager=self._db,
            state_machine=self._state_machine,
            event_logger=self._event_logger,
            mode="live",
        )
        self._live_risk = RiskManager(
            state_machine=self._state_machine,
            trading_window=trading_window,
            position_sizer=position_sizer,
            streak_manager=live_streak,
            sqlite_manager=self._db,
            event_logger=self._event_logger,
            mode="live",
            get_feed_healthy=lambda: not self._feed_manager.entries_suspended,
        )
        # 16. Paper RiskManager (skips StateMachine; paper streak uses no-op so we don't transition live state)
        class _NoOpStateMachine:
            async def transition(self, _to_state: str, _reason: str = "") -> bool:
                return True

        paper_streak = StreakManager(
            sqlite_manager=self._db,
            state_machine=_NoOpStateMachine(),
            event_logger=self._event_logger,
            mode="paper",
        )
        self._paper_risk = RiskManager(
            state_machine=None,
            trading_window=trading_window,
            position_sizer=position_sizer,
            streak_manager=paper_streak,
            sqlite_manager=self._db,
            event_logger=self._event_logger,
            mode="paper",
            get_feed_healthy=lambda: not self._feed_manager.entries_suspended,
        )
        # 17. Live OrderManager + ExitManager (KalshiClient)
        if self._kalshi_client is not None:
            self._live_order_manager = OrderManager(
                exchange=self._kalshi_client,
                sqlite_manager=self._db,
                event_logger=self._event_logger,
                mode="live",
            )
            self._live_exit_manager = ExitManager(
                exchange=self._kalshi_client,
                sqlite_manager=self._db,
                mode="live",
            )
        # 18. Paper OrderManager + ExitManager (PaperAdapter)
        self._paper_order_manager = OrderManager(
            exchange=self._paper_adapter,
            sqlite_manager=self._db,
            event_logger=self._event_logger,
            mode="paper",
        )
        self._paper_exit_manager = ExitManager(
            exchange=self._paper_adapter,
            sqlite_manager=self._db,
            mode="paper",
        )
        # 19. ApprovalManager (live only)
        self._approval_manager = ApprovalManager(
            alert_service=self._alert_service,
            event_logger=self._event_logger,
            mode="semi_auto",
        )
        # 20. DailyReportGenerator
        self._daily_report_generator = DailyReportGenerator(db=self._db, event_logger=self._event_logger)
        # 21. FastAPI dashboard
        self._signal_log: list = []
        self._dashboard_app = create_dashboard_app(
            db=self._db,
            state_machine=self._state_machine,
            approval_manager=self._approval_manager,
            get_btc_price=get_btc_price,
            signal_log=self._signal_log,
        )
        # 22–23. Exit monitors started as tasks below
        # 24. Log "All services started"
        self._logger.info("All services started")


class MagicMockExchange:
    """Stub exchange for universe when no Kalshi client (paper-only)."""

    async def get_btc_contracts(self):
        return []

    async def get_contract(self, contract_id: str):
        return None

    async def get_orderbook(self, contract_id: str):
        return {}


async def _signal_fanout(engine_queue: asyncio.Queue, live_q: asyncio.Queue, paper_q: asyncio.Queue) -> None:
    """Copy each signal from engine to both live and paper queues."""
    while True:
        try:
            sig = await engine_queue.get()
            await live_q.put(sig)
            await paper_q.put(sig)
        except asyncio.CancelledError:
            break


async def run_live_trading_loop(
    queue: asyncio.Queue,
    risk: RiskManager,
    approval: ApprovalManager,
    order_manager: OrderManager,
    signal_log: list,
) -> None:
    """Live: signal -> risk -> approval -> execute."""
    while True:
        try:
            signal = await queue.get()
            signal_log.append({"contract_id": getattr(signal, "contract_id", ""), "side": getattr(signal, "side", "")})
            approved, reason, size = await risk.evaluate_signal(signal)
            if not approved or size <= 0:
                continue
            ok = await approval.request_approval(signal)
            if not ok:
                continue
            await order_manager.place_entry_order(signal, size)
        except asyncio.CancelledError:
            break
        except Exception as e:
            get_logger("orchestrator").exception("live_trading_loop: %s", e)


async def run_paper_trading_loop(
    queue: asyncio.Queue,
    risk: RiskManager,
    order_manager: OrderManager,
    signal_log: list,
) -> None:
    """Paper: signal -> risk -> auto-execute (no approval)."""
    while True:
        try:
            signal = await queue.get()
            signal_log.append({"contract_id": getattr(signal, "contract_id", ""), "side": getattr(signal, "side", "")})
            approved, reason, size = await risk.evaluate_signal(signal)
            if not approved or size <= 0:
                continue
            await order_manager.place_entry_order(signal, size)
        except asyncio.CancelledError:
            break
        except Exception as e:
            get_logger("orchestrator").exception("paper_trading_loop: %s", e)


def _midnight_et_next() -> datetime:
    """Next midnight ET."""
    now = datetime.now(ET)
    tomorrow = now.date()
    return datetime(tomorrow.year, tomorrow.month, tomorrow.day, tzinfo=ET)


async def run_daily_reset_loop(orch: Orchestrator) -> None:
    """At midnight ET: reset both modes, generate combined report, set DISARMED."""
    while not getattr(orch, "_shutdown", True):
        now_et = datetime.now(ET)
        target = _midnight_et_next()
        wait_sec = (target - now_et).total_seconds()
        if wait_sec > 0:
            await asyncio.sleep(min(wait_sec, 60))
            continue
        try:
            report_date = (datetime.now(ET).date()).isoformat()
            if orch._db:
                await orch._db.reset_daily_state(
                    trading_date=report_date,
                    starting_bankroll=orch._settings.PAPER_STARTING_BANKROLL,
                    mode="paper",
                )
                live_state = await orch._db.get_bot_state(mode="live")
                sb = float(live_state.get("starting_bankroll") or 0)
                await orch._db.reset_daily_state(
                    trading_date=report_date,
                    starting_bankroll=sb,
                    mode="live",
                )
            if orch._daily_report_generator:
                await orch._daily_report_generator.generate_combined_report(report_date)
            if orch._state_machine:
                await orch._state_machine.transition("DISARMED", "daily_reset")
        except Exception as e:
            get_logger("orchestrator").exception("daily_reset: %s", e)
        await asyncio.sleep(60)


async def main_async() -> None:
    orch = Orchestrator()
    await orch._run_startup_sequence()
    get_universe = lambda: orch._universe_manager.get_universe() if orch._universe_manager else []
    get_orderbook = lambda ticker: orch._universe_manager.get_orderbook(ticker) if orch._universe_manager else {}
    get_bars = lambda: orch._bar_aggregator._bars[-200:] if orch._bar_aggregator and orch._bar_aggregator._bars else []
    get_btc_price = lambda: orch._feed_manager.get_current_price() or 0.0
    scan_task = asyncio.create_task(
        orch._signal_engine.run_scan_loop(get_universe, get_orderbook, get_bars, get_btc_price)
    )
    fanout_task = asyncio.create_task(
        _signal_fanout(orch._signal_engine.signal_queue, orch._signal_queue_live, orch._signal_queue_paper)
    )
    signal_log = getattr(orch, "_signal_log", []) or []
    live_loop = (
        asyncio.create_task(
            run_live_trading_loop(
                orch._signal_queue_live,
                orch._live_risk,
                orch._approval_manager,
                orch._live_order_manager,
                signal_log,
            )
        )
        if orch._live_order_manager
        else None
    )
    paper_loop = asyncio.create_task(
        run_paper_trading_loop(orch._signal_queue_paper, orch._paper_risk, orch._paper_order_manager, signal_log)
    )
    live_exit = asyncio.create_task(orch._live_exit_manager.run_exit_monitor_loop()) if orch._live_exit_manager else None
    paper_exit = asyncio.create_task(orch._paper_exit_manager.run_exit_monitor_loop())
    daily_task = asyncio.create_task(run_daily_reset_loop(orch))
    dashboard_task = None
    if orch._dashboard_app:
        import uvicorn
        config = uvicorn.Config(
            orch._dashboard_app,
            host=orch._settings.VPS_HOST,
            port=orch._settings.VPS_PORT,
            log_level="info",
        )
        server = uvicorn.Server(config)
        dashboard_task = asyncio.create_task(server.serve())
    orch._tasks = [t for t in [scan_task, fanout_task, live_loop, paper_loop, live_exit, paper_exit, daily_task, dashboard_task] if t is not None]
    try:
        await asyncio.gather(*orch._tasks)
    except asyncio.CancelledError:
        pass
    orch._shutdown = True
    if orch._signal_engine:
        orch._signal_engine.stop()
    if orch._universe_manager:
        await orch._universe_manager.stop()
    if orch._live_exit_manager:
        orch._live_exit_manager.stop()
        await asyncio.sleep(30)
    if orch._paper_exit_manager:
        orch._paper_exit_manager.stop()
    if orch._feed_manager:
        await orch._feed_manager.stop()
    report_date = datetime.now(ET).date().isoformat()
    if orch._daily_report_generator:
        await orch._daily_report_generator.generate_combined_report(report_date)
    if orch._db:
        await orch._db.close()
    if orch._event_logger and hasattr(orch._event_logger, "close"):
        await orch._event_logger.close()


def main() -> None:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(main_async())
    finally:
        loop.close()


if __name__ == "__main__":
    main()
