"""
Daily report generator: headline metrics, execution quality, risk events,
signal diagnostics, review flags. Paper/live comparison with divergence flags.
Saves to Postgres + JSON. Uses real dual-path data (both modes ran on same signals).
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from btc_kalshi.core.logger import get_logger

SIGNAL_DIVERGENCE_PCT = 5.0
FILL_DIVERGENCE_CENTS = 0.02
WIN_RATE_DIVERGENCE_PCT = 10.0


class DailyReportGenerator:
    """
    generate_report(report_date, mode): headline metrics, execution quality,
    risk events, signal diagnostics, review flags.
    generate_paper_live_comparison(report_date): signal count, trades, win rate,
    P&L, fill prices (sim vs actual), avg slippage; flags for divergence.
    generate_combined_report(report_date): live + paper + comparison; save to Postgres + JSON.
    """

    def __init__(self, db: Any, event_logger: Optional[Any] = None) -> None:
        self._db = db
        self._event_logger = event_logger
        self._logger = get_logger("daily-report")

    async def generate_report(self, report_date: str, mode: str = "live") -> Dict[str, Any]:
        """Headline metrics, execution quality, risk events, signal diagnostics, review flags."""
        state = await self._db.get_bot_state(mode=mode)
        trades = await self._db.get_daily_trades(trading_date=report_date, mode=mode)
        entry_trades = [t for t in trades if (t.get("purpose") or "").lower() == "entry"]
        exit_trades = [t for t in trades if (t.get("purpose") or "").lower() == "exit"]
        filled_entries = [t for t in entry_trades if (t.get("filled_size") or 0) > 0]
        trades_count = len(filled_entries)

        headline_metrics = {
            "report_date": report_date,
            "mode": mode,
            "lifecycle_state": state.get("lifecycle_state") or "UNKNOWN",
            "daily_pnl_net": float(state.get("daily_pnl_net") or 0),
            "daily_pnl_gross": float(state.get("daily_pnl_gross") or 0),
            "starting_bankroll": float(state.get("starting_bankroll") or 0),
            "trades_count": trades_count,
        }

        slippage_list: List[float] = []
        for t in filled_entries:
            intended = float(t.get("intended_price") or 0)
            filled = float(t.get("filled_price") or 0)
            if intended > 0:
                slippage_list.append(filled - intended)
        avg_slippage = sum(slippage_list) / len(slippage_list) if slippage_list else 0.0
        avg_fill_price = (
            sum(float(t.get("filled_price") or 0) for t in filled_entries) / len(filled_entries)
            if filled_entries else 0.0
        )
        execution_quality = {
            "trades_count": trades_count,
            "avg_slippage": round(avg_slippage, 4),
            "avg_fill_price": round(avg_fill_price, 4),
            "filled_entries": len(filled_entries),
        }

        risk_events: List[Dict[str, Any]] = []
        if self._event_logger is not None and hasattr(self._event_logger, "query_events"):
            events = self._event_logger.query_events(report_date, event_type="risk", mode=mode, limit=50)
            risk_events = [{"event_type": e.get("event_type"), "payload": e.get("payload", {})} for e in events]

        signal_count = 0
        if self._event_logger is not None and hasattr(self._event_logger, "query_events"):
            signal_events = self._event_logger.query_events(report_date, event_type="signal", mode=mode, limit=200)
            signal_count = len(signal_events)
        signal_diagnostics = {"signals_emitted": signal_count, "trades_taken": trades_count}

        review_flags: List[str] = []
        if headline_metrics["daily_pnl_net"] < -20:
            review_flags.append("large_daily_loss")
        if len(risk_events) > 5:
            review_flags.append("elevated_risk_events")
        if trades_count == 0 and signal_count > 0:
            review_flags.append("signals_but_no_trades")

        return {
            "headline_metrics": headline_metrics,
            "execution_quality": execution_quality,
            "risk_events": risk_events,
            "signal_diagnostics": signal_diagnostics,
            "review_flags": review_flags,
        }

    async def generate_paper_live_comparison(self, report_date: str) -> Dict[str, Any]:
        """Compare signal count, trades, win rate, P&L, fill prices (sim vs actual), avg slippage. Set divergence flags."""
        live_report = await self.generate_report(report_date, mode="live")
        paper_report = await self.generate_report(report_date, mode="paper")
        live_trades = await self._db.get_daily_trades(trading_date=report_date, mode="live")
        paper_trades = await self._db.get_daily_trades(trading_date=report_date, mode="paper")
        live_entries = [t for t in live_trades if (t.get("purpose") or "").lower() == "entry" and (t.get("filled_size") or 0) > 0]
        paper_entries = [t for t in paper_trades if (t.get("purpose") or "").lower() == "entry" and (t.get("filled_size") or 0) > 0]

        live_sigs = live_report["signal_diagnostics"].get("signals_emitted", 0)
        paper_sigs = paper_report["signal_diagnostics"].get("signals_emitted", 0)
        total_sigs = max(live_sigs, paper_sigs, 1)
        signal_div_pct = abs(live_sigs - paper_sigs) / total_sigs * 100 if total_sigs else 0

        live_pnl = live_report["headline_metrics"].get("daily_pnl_net", 0) or 0
        paper_pnl = paper_report["headline_metrics"].get("daily_pnl_net", 0) or 0
        live_wr = 0.0
        paper_wr = 0.0
        if live_entries:
            wins = sum(1 for _ in live_entries)
            live_wr = wins / len(live_entries) * 100 if live_entries else 0
        if paper_entries:
            wins = sum(1 for _ in paper_entries)
            paper_wr = wins / len(paper_entries) * 100 if paper_entries else 0
        win_rate_div = abs(live_wr - paper_wr)

        live_avg_fill = live_report["execution_quality"].get("avg_fill_price", 0) or 0
        paper_avg_fill = paper_report["execution_quality"].get("avg_fill_price", 0) or 0
        fill_div = abs(live_avg_fill - paper_avg_fill)

        flags: List[str] = []
        if signal_div_pct > SIGNAL_DIVERGENCE_PCT:
            flags.append("signal_divergence_gt_5pct")
        if fill_div > FILL_DIVERGENCE_CENTS:
            flags.append("fill_divergence_gt_2c")
        if win_rate_div > WIN_RATE_DIVERGENCE_PCT:
            flags.append("win_rate_divergence_gt_10pct")

        return {
            "report_date": report_date,
            "live": {
                "signal_count": live_sigs,
                "trades_taken": len(live_entries),
                "win_rate_pct": round(live_wr, 2),
                "daily_pnl_net": live_pnl,
                "avg_fill_price": live_avg_fill,
                "avg_slippage": live_report["execution_quality"].get("avg_slippage", 0),
            },
            "paper": {
                "signal_count": paper_sigs,
                "trades_taken": len(paper_entries),
                "win_rate_pct": round(paper_wr, 2),
                "daily_pnl_net": paper_pnl,
                "avg_fill_price": paper_avg_fill,
                "avg_slippage": paper_report["execution_quality"].get("avg_slippage", 0),
            },
            "flags": flags,
        }

    async def generate_combined_report(self, report_date: str) -> Dict[str, Any]:
        """Live + paper + comparison. Save to Postgres + JSON."""
        live = await self.generate_report(report_date, mode="live")
        paper = await self.generate_report(report_date, mode="paper")
        comparison = await self.generate_paper_live_comparison(report_date)
        combined = {
            "report_date": report_date,
            "generated_ts": datetime.now(timezone.utc).isoformat(),
            "live": live,
            "paper": paper,
            "comparison": comparison,
        }
        if self._event_logger is not None:
            if hasattr(self._event_logger, "save_daily_report"):
                await self._event_logger.save_daily_report(report_date, "combined", combined)
            log_dir = getattr(self._event_logger, "_log_dir", None)
            if log_dir is not None:
                path = Path(log_dir) / f"daily-report-{report_date}.json"
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(json.dumps(combined, indent=2), encoding="utf-8")
        return combined
