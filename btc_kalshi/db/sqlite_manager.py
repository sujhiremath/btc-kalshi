from __future__ import annotations

from typing import Any, Dict, Final

import aiosqlite

from btc_kalshi.core.logger import get_logger

_COLUMNS: Final[tuple[str, ...]] = (
    "trading_date",
    "armed",
    "mode",
    "lifecycle_state",
    "current_streak_type",
    "current_streak_count",
    "daily_pnl_gross",
    "daily_pnl_net",
    "starting_bankroll",
    "intraday_peak_equity",
    "last_reconciliation_ts",
    "weekly_pnl_net",
    "size_multiplier",
)


class SQLiteStateManager:
    """
    Authoritative live and paper trading state store backed by SQLite.
    """

    def __init__(self, db_path: str, conn: aiosqlite.Connection) -> None:
        self._db_path = db_path
        self._conn = conn
        self._logger = get_logger("sqlite-state-manager")

    @classmethod
    async def init(cls, db_path: str) -> "SQLiteStateManager":
        """
        Async factory: open connection, create schema, and ensure singleton rows.
        """
        conn = await aiosqlite.connect(db_path)
        conn.row_factory = aiosqlite.Row
        self = cls(db_path, conn)
        await self._initialize_schema()
        await self._ensure_singleton_rows()
        return self

    async def _initialize_schema(self) -> None:
        await self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS bot_state (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                trading_date TEXT,
                armed INTEGER NOT NULL DEFAULT 0,
                mode TEXT NOT NULL,
                lifecycle_state TEXT NOT NULL,
                current_streak_type TEXT,
                current_streak_count INTEGER NOT NULL DEFAULT 0,
                daily_pnl_gross REAL NOT NULL DEFAULT 0.0,
                daily_pnl_net REAL NOT NULL DEFAULT 0.0,
                starting_bankroll REAL NOT NULL DEFAULT 0.0,
                intraday_peak_equity REAL NOT NULL DEFAULT 0.0,
                last_reconciliation_ts TEXT,
                weekly_pnl_net REAL NOT NULL DEFAULT 0.0,
                size_multiplier REAL NOT NULL DEFAULT 1.0
            );

            CREATE TABLE IF NOT EXISTS paper_state (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                trading_date TEXT,
                armed INTEGER NOT NULL DEFAULT 0,
                mode TEXT NOT NULL,
                lifecycle_state TEXT NOT NULL,
                current_streak_type TEXT,
                current_streak_count INTEGER NOT NULL DEFAULT 0,
                daily_pnl_gross REAL NOT NULL DEFAULT 0.0,
                daily_pnl_net REAL NOT NULL DEFAULT 0.0,
                starting_bankroll REAL NOT NULL DEFAULT 0.0,
                intraday_peak_equity REAL NOT NULL DEFAULT 0.0,
                last_reconciliation_ts TEXT,
                weekly_pnl_net REAL NOT NULL DEFAULT 0.0,
                size_multiplier REAL NOT NULL DEFAULT 1.0
            );
            """
        )
        await self._conn.commit()

    async def _ensure_singleton_rows(self) -> None:
        await self._conn.execute(
            """
            INSERT OR IGNORE INTO bot_state (
                id, trading_date, armed, mode, lifecycle_state,
                current_streak_type, current_streak_count,
                daily_pnl_gross, daily_pnl_net,
                starting_bankroll, intraday_peak_equity,
                last_reconciliation_ts, weekly_pnl_net, size_multiplier
            )
            VALUES (
                1, NULL, 0, 'live', 'DISARMED',
                NULL, 0,
                0.0, 0.0,
                0.0, 0.0,
                NULL, 0.0, 1.0
            );
            """
        )

        await self._conn.execute(
            """
            INSERT OR IGNORE INTO paper_state (
                id, trading_date, armed, mode, lifecycle_state,
                current_streak_type, current_streak_count,
                daily_pnl_gross, daily_pnl_net,
                starting_bankroll, intraday_peak_equity,
                last_reconciliation_ts, weekly_pnl_net, size_multiplier
            )
            VALUES (
                1, NULL, 0, 'paper', 'DISARMED',
                NULL, 0,
                0.0, 0.0,
                0.0, 0.0,
                NULL, 0.0, 1.0
            );
            """
        )

        await self._conn.commit()

    @staticmethod
    def _table_name(mode: str) -> str:
        return "paper_state" if mode == "paper" else "bot_state"

    async def get_bot_state(self, mode: str = "live") -> Dict[str, Any]:
        """
        Read the current state row for the given mode ("live" or "paper").
        """
        table = self._table_name(mode)
        async with self._conn.execute(
            f"SELECT * FROM {table} WHERE id = 1"
        ) as cursor:
            row = await cursor.fetchone()

        if row is None:
            raise RuntimeError(f"State row missing for table {table}")

        return dict(row)

    async def update_bot_state(self, mode: str = "live", **kwargs: Any) -> None:
        """
        Atomically update one or more fields on the state row.
        """
        table = self._table_name(mode)

        updates = {k: v for k, v in kwargs.items() if k in _COLUMNS}
        if not updates:
            return

        assignments = ", ".join(f"{col} = ?" for col in updates.keys())
        values = list(updates.values())

        await self._conn.execute(
            f"UPDATE {table} SET {assignments} WHERE id = 1",
            values,
        )
        await self._conn.commit()

        self._logger.info(
            "Updated trading state row",
            extra={
                "mode": mode,
                "table": table,
                "changes": updates,
            },
        )

    async def reset_daily_state(
        self,
        trading_date: str,
        starting_bankroll: float,
        mode: str = "live",
    ) -> None:
        """
        Reset intraday / daily fields at the start of a new trading day.
        """
        updates = {
            "trading_date": trading_date,
            "starting_bankroll": starting_bankroll,
            "daily_pnl_gross": 0.0,
            "daily_pnl_net": 0.0,
            "intraday_peak_equity": starting_bankroll,
            "current_streak_type": None,
            "current_streak_count": 0,
            "last_reconciliation_ts": None,
            "armed": 0,
            "lifecycle_state": "DISARMED",
        }
        await self.update_bot_state(mode=mode, **updates)

        self._logger.info(
            "Reset daily trading state",
            extra={
                "mode": mode,
                "trading_date": trading_date,
                "starting_bankroll": starting_bankroll,
            },
        )

    async def close(self) -> None:
        await self._conn.close()

