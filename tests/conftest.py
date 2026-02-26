import os
from datetime import datetime, timedelta, timezone
from typing import Iterator, List

import pytest

from btc_kalshi.config.settings import get_settings
from btc_kalshi.db.sqlite_manager import SQLiteStateManager
from btc_kalshi.feeds.bar_aggregator import Bar


@pytest.fixture(autouse=True)
def _reset_settings_cache() -> Iterator[None]:
    """
    Ensure each test gets a fresh Settings instance by clearing the cache.
    """
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.fixture(autouse=True)
def _isolate_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Start each test with a clean environment for config-related variables.
    """
    keys = [
        "KALSHI_API_KEY",
        "KALSHI_API_SECRET",
        "COINBASE_WS_URL",
        "BINANCE_WS_URL",
        "SQLITE_PATH",
        "POSTGRES_DSN",
        "NTFY_TOPIC",
        "SMTP_HOST",
        "SMTP_USER",
        "SMTP_PASS",
        "ALERT_EMAIL_TO",
        "BOT_MODE",
        "LOG_LEVEL",
        "VPS_HOST",
        "VPS_PORT",
        "PAPER_STARTING_BANKROLL",
    ]
    for key in keys:
        monkeypatch.delenv(key, raising=False)


@pytest.fixture()
def ensure_log_dir(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Point the logger's data/logs directory at a temporary path so tests
    don't write into the real project tree.
    """
    # The logger module will create data/logs relative to CWD.
    monkeypatch.chdir(tmp_path)
    os.makedirs("data/logs", exist_ok=True)


def make_bars(
    prices: List[float],
    start_ts: datetime | None = None,
    bar_seconds: int = 5,
) -> List[Bar]:
    """
    Build a list of Bar from a list of close prices (open=high=low=close).
    start_ts defaults to a fixed UTC time; each bar is bar_seconds apart.
    """
    if start_ts is None:
        start_ts = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    bars: List[Bar] = []
    for i, p in enumerate(prices):
        ts = start_ts + timedelta(seconds=i * bar_seconds)
        bars.append(
            Bar(
                timestamp=ts,
                open=p,
                high=p,
                low=p,
                close=p,
                volume=1.0,
                tick_count=1,
            )
        )
    return bars


# Alias so the fixture can be named make_bars and return the helper
_make_bars_helper = make_bars


@pytest.fixture()
def make_bars():
    """Expose make_bars helper so tests can build bars from price lists."""
    return _make_bars_helper


@pytest.fixture()
async def state_manager() -> SQLiteStateManager:
    """
    Provide an in-memory SQLiteStateManager for tests.
    """
    manager = await SQLiteStateManager.init(":memory:")
    try:
        yield manager
    finally:
        await manager.close()
