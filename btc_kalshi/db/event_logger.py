from __future__ import annotations

import asyncio
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import asyncpg

from btc_kalshi.core.logger import get_logger


@dataclass
class _EventRecord:
    ts: str
    event_type: str
    severity: str
    service_name: str
    contract_id: Optional[str]
    mode: str
    payload: Dict[str, Any]


class EventLogger:
    """
    Append-only event logger: flat-file first, Postgres second.
    """

    def __init__(self, postgres_dsn: Optional[str], log_dir: Path) -> None:
        self._postgres_dsn = postgres_dsn
        self._log_dir = Path(log_dir)
        self._log_dir.mkdir(parents=True, exist_ok=True)
        self._logger = get_logger("event-logger")
        self._pg_disabled = postgres_dsn is None
        self._pg_initialized = False
        self._pool: Optional[asyncpg.Pool] = None

    @classmethod
    def init(cls, postgres_dsn: Optional[str], log_dir: Path | str) -> "EventLogger":
        """
        Construct an EventLogger with the given Postgres DSN and log directory.

        Postgres is optional; when no DSN is provided, only flat-file logging
        is performed.
        """
        return cls(postgres_dsn=postgres_dsn, log_dir=Path(log_dir))

    @staticmethod
    def _now() -> datetime:
        return datetime.now(timezone.utc)

    def _log_to_flat_file(self, record: _EventRecord) -> None:
        ts = datetime.fromisoformat(record.ts)
        date_str = ts.date().isoformat()
        file_path = self._log_dir / f"events-{date_str}.log"
        with file_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(asdict(record), separators=(",", ":")))
            f.write("\n")

    async def _ensure_pg(self) -> None:
        if self._pg_disabled or self._pg_initialized:
            return
        try:
            self._pool = await asyncpg.create_pool(self._postgres_dsn)  # type: ignore[arg-type]
            async with self._pool.acquire() as conn:
                await conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS event_log (
                        event_id SERIAL PRIMARY KEY,
                        ts TIMESTAMPTZ NOT NULL,
                        event_type TEXT NOT NULL,
                        severity TEXT NOT NULL,
                        service_name TEXT NOT NULL,
                        contract_id TEXT,
                        mode TEXT NOT NULL DEFAULT 'live',
                        payload JSONB NOT NULL
                    );

                    CREATE TABLE IF NOT EXISTS daily_reports (
                        report_date DATE PRIMARY KEY,
                        mode TEXT NOT NULL DEFAULT 'live',
                        summary JSONB NOT NULL,
                        generated_ts TIMESTAMPTZ NOT NULL
                    );

                    CREATE TABLE IF NOT EXISTS paper_live_comparison (
                        id SERIAL PRIMARY KEY,
                        ts_bucket TIMESTAMPTZ NOT NULL,
                        comparison_type TEXT NOT NULL,
                        live_value REAL NOT NULL,
                        paper_value REAL NOT NULL,
                        delta REAL NOT NULL,
                        details JSONB
                    );
                    """
                )
            self._pg_initialized = True
        except Exception as exc:  # pragma: no cover - defensive; not hit in tests
            self._logger.info(
                "Postgres event logging disabled due to initialization error",
                extra={"error": str(exc)},
            )
            self._pg_disabled = True

    async def _log_to_postgres(self, record: _EventRecord) -> None:
        if self._pg_disabled or self._postgres_dsn is None:
            return

        await self._ensure_pg()
        if self._pg_disabled or self._pool is None:
            return

        try:
            async with self._pool.acquire() as conn:
                await conn.execute(
                    """
                    INSERT INTO event_log (
                        ts,
                        event_type,
                        severity,
                        service_name,
                        contract_id,
                        mode,
                        payload
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                    """,
                    record.ts,
                    record.event_type,
                    record.severity,
                    record.service_name,
                    record.contract_id,
                    record.mode,
                    json.dumps(record.payload),
                )
        except Exception as exc:  # pragma: no cover - defensive; not hit in tests
            self._logger.info(
                "Failed to log event to Postgres",
                extra={"error": str(exc)},
            )

    def log_event(
        self,
        event_type: str,
        severity: str,
        service_name: str,
        contract_id: Optional[str],
        payload: Dict[str, Any],
        mode: str = "live",
    ) -> None:
        """
        Log a single event: flat file first, then Postgres.

        Postgres failures are swallowed and never crash the caller.
        """
        ts = self._now().isoformat()
        record = _EventRecord(
            ts=ts,
            event_type=event_type,
            severity=severity,
            service_name=service_name,
            contract_id=contract_id,
            mode=mode,
            payload=payload,
        )

        # Flat-file first (authoritative append-only log)
        self._log_to_flat_file(record)

        # Then Postgres, best-effort only.
        if self._postgres_dsn is not None and not self._pg_disabled:
            try:
                try:
                    loop = asyncio.get_running_loop()
                except RuntimeError:
                    asyncio.run(self._log_to_postgres(record))
                else:
                    loop.create_task(self._log_to_postgres(record))
            except Exception as exc:  # pragma: no cover - defensive
                self._logger.info(
                    "Failed to schedule Postgres event logging",
                    extra={"error": str(exc)},
                )

    def query_events(
        self,
        date: str,
        event_type: Optional[str] = None,
        mode: Optional[str] = None,
        limit: int = 100,
    ) -> list[Dict[str, Any]]:
        """
        Query events from the flat-file log for a given date.

        This does not hit Postgres; it is intentionally flat-file-first.
        """
        file_path = self._log_dir / f"events-{date}.log"
        if not file_path.exists():
            return []

        results: list[Dict[str, Any]] = []
        with file_path.open("r", encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                event = json.loads(line)
                if event_type is not None and event.get("event_type") != event_type:
                    continue
                if mode is not None and event.get("mode") != mode:
                    continue
                results.append(event)
                if len(results) >= limit:
                    break
        return results

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()

