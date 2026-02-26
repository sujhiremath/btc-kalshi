import os
from typing import Iterator

import pytest

from btc_kalshi.config.settings import get_settings


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
