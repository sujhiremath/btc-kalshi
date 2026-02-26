"""
FastAPI dashboard: HTML + API for status, positions, signals, arm/disarm/kill/resume, approve/reject.
CORS enabled. Dual-mode (live + paper) display.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from fastapi import Depends, FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse

DASHBOARD_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = DASHBOARD_DIR / "templates"


def create_app(
    db: Any = None,
    state_machine: Any = None,
    approval_manager: Any = None,
    get_btc_price: Optional[Callable[[], float]] = None,
    signal_log: Optional[List[Dict[str, Any]]] = None,
) -> FastAPI:
    app = FastAPI(title="BTC-Kalshi Dashboard")
    app.state.db = db
    app.state.state_machine = state_machine
    app.state.approval_manager = approval_manager
    app.state.get_btc_price = get_btc_price or (lambda: 0.0)
    app.state.signal_log = signal_log if signal_log is not None else []

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    def _db(request: Request) -> Any:
        return getattr(request.app.state, "db", None)

    def _state_machine(request: Request) -> Any:
        return getattr(request.app.state, "state_machine", None)

    def _approval_manager(request: Request) -> Any:
        return getattr(request.app.state, "approval_manager", None)

    def _get_btc_price(request: Request) -> Callable[[], float]:
        return getattr(request.app.state, "get_btc_price", lambda: 0.0)

    def _signal_log(request: Request) -> List[Dict[str, Any]]:
        return getattr(request.app.state, "signal_log", [])

    @app.get("/", response_class=HTMLResponse)
    async def index(request: Request) -> HTMLResponse:
        html_path = TEMPLATES_DIR / "index.html"
        if not html_path.exists():
            return HTMLResponse("<html><body>Dashboard template not found.</body></html>", status_code=404)
        html = html_path.read_text(encoding="utf-8")
        return HTMLResponse(html)

    @app.get("/api/status")
    async def api_status(
        db: Any = Depends(_db),
        get_btc_price: Callable = Depends(_get_btc_price),
        am: Any = Depends(_approval_manager),
    ) -> JSONResponse:
        content: Dict[str, Any] = {"live": {}, "paper": {}, "btc_price": 0.0, "pending_approvals": []}
        try:
            content["btc_price"] = float(get_btc_price())
        except Exception:
            pass
        if am is not None and hasattr(am, "get_pending_approval_ids"):
            content["pending_approvals"] = am.get_pending_approval_ids()
        if db is None:
            return JSONResponse(content=content)
        live = await db.get_bot_state(mode="live")
        paper = await db.get_bot_state(mode="paper")
        content["live"] = live
        content["paper"] = paper
        return JSONResponse(content=content)

    @app.get("/api/positions")
    async def api_positions(
        request: Request,
        mode: str = "all",
        db: Any = Depends(_db),
    ) -> JSONResponse:
        if db is None:
            return JSONResponse(content=[])
        if mode == "live":
            positions = await db.get_open_positions(mode="live")
        elif mode == "paper":
            positions = await db.get_open_positions(mode="paper")
        else:
            live_pos = await db.get_open_positions(mode="live")
            paper_pos = await db.get_open_positions(mode="paper")
            positions = [*live_pos, *paper_pos]
        return JSONResponse(content=positions)

    @app.get("/api/signals")
    async def api_signals(signal_log: List = Depends(_signal_log)) -> JSONResponse:
        last_10 = signal_log[-10:] if signal_log else []
        return JSONResponse(content=last_10)

    @app.post("/api/arm")
    async def api_arm(sm: Any = Depends(_state_machine)) -> JSONResponse:
        if sm is None:
            return JSONResponse(content={"ok": False}, status_code=503)
        ok = await sm.transition("ARMED", "dashboard")
        return JSONResponse(content={"ok": ok})

    @app.post("/api/disarm")
    async def api_disarm(sm: Any = Depends(_state_machine)) -> JSONResponse:
        if sm is None:
            return JSONResponse(content={"ok": False}, status_code=503)
        ok = await sm.transition("DISARMED", "dashboard")
        return JSONResponse(content={"ok": ok})

    @app.post("/api/kill")
    async def api_kill(sm: Any = Depends(_state_machine)) -> JSONResponse:
        if sm is None:
            return JSONResponse(content={"ok": False}, status_code=503)
        ok = await sm.transition("KILLED", "dashboard")
        return JSONResponse(content={"ok": ok})

    @app.post("/api/resume")
    async def api_resume(sm: Any = Depends(_state_machine)) -> JSONResponse:
        if sm is None:
            return JSONResponse(content={"ok": False}, status_code=503)
        ok = await sm.transition("ACTIVE", "dashboard")
        return JSONResponse(content={"ok": ok})

    @app.post("/api/approve/{approval_id}")
    async def api_approve(approval_id: str, am: Any = Depends(_approval_manager)) -> JSONResponse:
        if am is None:
            return JSONResponse(content={"ok": False}, status_code=503)
        am.receive_approval(approval_id, True)
        return JSONResponse(content={"ok": True})

    @app.post("/api/reject/{approval_id}")
    async def api_reject(approval_id: str, am: Any = Depends(_approval_manager)) -> JSONResponse:
        if am is None:
            return JSONResponse(content={"ok": False}, status_code=503)
        am.receive_approval(approval_id, False)
        return JSONResponse(content={"ok": True})

    return app
