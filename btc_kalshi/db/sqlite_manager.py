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

            CREATE TABLE IF NOT EXISTS open_positions (
                position_id TEXT PRIMARY KEY,
                mode TEXT NOT NULL DEFAULT 'live',
                contract_id TEXT NOT NULL,
                contract_title TEXT NOT NULL,
                expiry_ts TEXT NOT NULL,
                side TEXT NOT NULL,
                entry_order_client_id TEXT,
                entry_price_intended REAL,
                entry_price_filled REAL,
                stop_price REAL,
                take_profit_price REAL,
                intended_size INTEGER NOT NULL,
                filled_size INTEGER NOT NULL DEFAULT 0,
                opened_ts TEXT NOT NULL,
                status TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS orders (
                client_order_id TEXT PRIMARY KEY,
                mode TEXT NOT NULL DEFAULT 'live',
                position_id TEXT,
                contract_id TEXT NOT NULL,
                purpose TEXT NOT NULL,
                side TEXT NOT NULL,
                intended_price REAL,
                intended_size INTEGER NOT NULL,
                filled_price REAL,
                filled_size INTEGER NOT NULL DEFAULT 0,
                current_status TEXT NOT NULL,
                created_ts TEXT NOT NULL,
                last_update_ts TEXT
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

    #
    # Position management
    #

    async def open_position(
        self,
        position_id: str,
        contract_id: str,
        contract_title: str,
        expiry_ts: str,
        side: str,
        entry_order_client_id: str | None,
        entry_price_intended: float | None,
        entry_price_filled: float | None,
        stop_price: float | None,
        take_profit_price: float | None,
        intended_size: int,
        opened_ts: str,
        status: str = "OPEN",
        filled_size: int = 0,
        mode: str = "live",
    ) -> None:
        """
        Insert a new open position row for the given mode.
        """
        await self._conn.execute(
            """
            INSERT INTO open_positions (
                position_id,
                mode,
                contract_id,
                contract_title,
                expiry_ts,
                side,
                entry_order_client_id,
                entry_price_intended,
                entry_price_filled,
                stop_price,
                take_profit_price,
                intended_size,
                filled_size,
                opened_ts,
                status
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                position_id,
                mode,
                contract_id,
                contract_title,
                expiry_ts,
                side,
                entry_order_client_id,
                entry_price_intended,
                entry_price_filled,
                stop_price,
                take_profit_price,
                intended_size,
                filled_size,
                opened_ts,
                status,
            ),
        )
        await self._conn.commit()

        self._logger.info(
            "Opened position",
            extra={
                "mode": mode,
                "position_id": position_id,
                "contract_id": contract_id,
                "side": side,
                "intended_size": intended_size,
            },
        )

    async def get_open_positions(self, mode: str = "live") -> list[Dict[str, Any]]:
        """
        Return all non-closed positions for the given mode.
        """
        async with self._conn.execute(
            """
            SELECT * FROM open_positions
            WHERE mode = ? AND status != 'CLOSED'
            """,
            (mode,),
        ) as cursor:
            rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    async def get_position(
        self,
        position_id: str,
        mode: str = "live",
    ) -> Dict[str, Any] | None:
        async with self._conn.execute(
            """
            SELECT * FROM open_positions
            WHERE position_id = ? AND mode = ?
            """,
            (position_id, mode),
        ) as cursor:
            row = await cursor.fetchone()
        return dict(row) if row is not None else None

    async def update_position(
        self,
        position_id: str,
        mode: str = "live",
        **kwargs: Any,
    ) -> None:
        columns = {
            "contract_id",
            "contract_title",
            "expiry_ts",
            "side",
            "entry_order_client_id",
            "entry_price_intended",
            "entry_price_filled",
            "stop_price",
            "take_profit_price",
            "intended_size",
            "filled_size",
            "opened_ts",
            "status",
        }
        updates = {k: v for k, v in kwargs.items() if k in columns}
        if not updates:
            return

        assignments = ", ".join(f"{col} = ?" for col in updates.keys())
        values = list(updates.values()) + [position_id, mode]

        await self._conn.execute(
            f"""
            UPDATE open_positions
            SET {assignments}
            WHERE position_id = ? AND mode = ?
            """,
            values,
        )
        await self._conn.commit()

        self._logger.info(
            "Updated position",
            extra={"mode": mode, "position_id": position_id, "changes": updates},
        )

    async def close_position(
        self,
        position_id: str,
        mode: str = "live",
    ) -> None:
        """
        Mark a position as closed for the given mode.
        """
        await self.update_position(position_id, mode=mode, status="CLOSED")

    async def count_open_positions(self, mode: str = "live") -> int:
        async with self._conn.execute(
            """
            SELECT COUNT(*) as cnt
            FROM open_positions
            WHERE mode = ? AND status != 'CLOSED'
            """,
            (mode,),
        ) as cursor:
            row = await cursor.fetchone()
        return int(row["cnt"]) if row is not None else 0

    async def has_expiry_conflict(
        self,
        expiry_ts: str,
        side: str,
        mode: str = "live",
    ) -> bool:
        async with self._conn.execute(
            """
            SELECT 1
            FROM open_positions
            WHERE mode = ?
              AND expiry_ts = ?
              AND side = ?
              AND status != 'CLOSED'
            LIMIT 1
            """,
            (mode, expiry_ts, side),
        ) as cursor:
            row = await cursor.fetchone()
        return row is not None

    #
    # Order management
    #

    async def create_order(
        self,
        client_order_id: str,
        position_id: str | None,
        contract_id: str,
        purpose: str,
        side: str,
        intended_price: float | None,
        intended_size: int,
        created_ts: str,
        current_status: str = "NEW",
        filled_price: float | None = None,
        filled_size: int = 0,
        last_update_ts: str | None = None,
        mode: str = "live",
    ) -> None:
        await self._conn.execute(
            """
            INSERT INTO orders (
                client_order_id,
                mode,
                position_id,
                contract_id,
                purpose,
                side,
                intended_price,
                intended_size,
                filled_price,
                filled_size,
                current_status,
                created_ts,
                last_update_ts
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                client_order_id,
                mode,
                position_id,
                contract_id,
                purpose,
                side,
                intended_price,
                intended_size,
                filled_price,
                filled_size,
                current_status,
                created_ts,
                last_update_ts,
            ),
        )
        await self._conn.commit()

        self._logger.info(
            "Created order",
            extra={
                "mode": mode,
                "client_order_id": client_order_id,
                "position_id": position_id,
                "purpose": purpose,
            },
        )

    async def update_order(
        self,
        client_order_id: str,
        mode: str = "live",
        **kwargs: Any,
    ) -> None:
        columns = {
            "position_id",
            "contract_id",
            "purpose",
            "side",
            "intended_price",
            "intended_size",
            "filled_price",
            "filled_size",
            "current_status",
            "created_ts",
            "last_update_ts",
        }
        updates = {k: v for k, v in kwargs.items() if k in columns}
        if not updates:
            return

        assignments = ", ".join(f"{col} = ?" for col in updates.keys())
        values = list(updates.values()) + [client_order_id, mode]

        await self._conn.execute(
            f"""
            UPDATE orders
            SET {assignments}
            WHERE client_order_id = ? AND mode = ?
            """,
            values,
        )
        await self._conn.commit()

        self._logger.info(
            "Updated order",
            extra={
                "mode": mode,
                "client_order_id": client_order_id,
                "changes": updates,
            },
        )

    async def get_order(
        self,
        client_order_id: str,
        mode: str = "live",
    ) -> Dict[str, Any] | None:
        async with self._conn.execute(
            """
            SELECT * FROM orders
            WHERE client_order_id = ? AND mode = ?
            """,
            (client_order_id, mode),
        ) as cursor:
            row = await cursor.fetchone()
        return dict(row) if row is not None else None

    async def get_orders_for_position(
        self,
        position_id: str,
        mode: str = "live",
    ) -> list[Dict[str, Any]]:
        async with self._conn.execute(
            """
            SELECT * FROM orders
            WHERE mode = ? AND position_id = ?
            ORDER BY created_ts
            """,
            (mode, position_id),
        ) as cursor:
            rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    async def get_daily_trades(
        self,
        trading_date: str,
        mode: str = "live",
    ) -> list[Dict[str, Any]]:
        """
        Return all orders for the given trading date and mode.

        Assumes created_ts is an ISO8601 string beginning with YYYY-MM-DD.
        """
        like_pattern = f"{trading_date}%"
        async with self._conn.execute(
            """
            SELECT * FROM orders
            WHERE mode = ? AND created_ts LIKE ?
            ORDER BY created_ts
            """,
            (mode, like_pattern),
        ) as cursor:
            rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    async def close(self) -> None:
        await self._conn.close()

