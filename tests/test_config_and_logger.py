import json

import pytest
from pydantic import ValidationError

from btc_kalshi.config.settings import Settings, get_settings
from btc_kalshi.core.logger import get_logger


def test_default_config_loads(monkeypatch):
    # Provide required notification / email fields; leave BOT_MODE as default "paper".
    monkeypatch.setenv("NTFY_TOPIC", "test-topic")
    monkeypatch.setenv("SMTP_HOST", "smtp.test")
    monkeypatch.setenv("SMTP_USER", "user@test")
    monkeypatch.setenv("SMTP_PASS", "secret")
    monkeypatch.setenv("ALERT_EMAIL_TO", "alert@test")

    get_settings.cache_clear()
    settings = get_settings()

    # Defaults
    assert settings.COINBASE_WS_URL == "wss://advanced-trade-ws.coinbase.com"
    assert (
        settings.BINANCE_WS_URL
        == "wss://stream.binance.com:9443/ws/btcusdt@trade"
    )
    assert settings.SQLITE_PATH == "data/bot_state.db"
    assert settings.BOT_MODE == "paper"
    assert settings.LOG_LEVEL == "INFO"
    assert settings.VPS_HOST == "0.0.0.0"
    assert settings.VPS_PORT == 8000
    assert settings.PAPER_STARTING_BANKROLL == 100.0

    # Env-provided values
    assert settings.NTFY_TOPIC == "test-topic"
    assert settings.SMTP_HOST == "smtp.test"
    assert settings.SMTP_USER == "user@test"
    assert settings.SMTP_PASS == "secret"
    assert settings.ALERT_EMAIL_TO == "alert@test"


def test_live_mode_requires_keys(monkeypatch):
    monkeypatch.setenv("BOT_MODE", "live")
    # Explicitly ensure keys are missing
    monkeypatch.delenv("KALSHI_API_KEY", raising=False)
    monkeypatch.delenv("KALSHI_API_SECRET", raising=False)

    with pytest.raises(ValidationError):
        Settings()


def _parse_last_log_line(captured) -> dict:
    out = captured.out.strip()
    # In case multiple lines are logged, take the last non-empty one.
    last_line = [line for line in out.splitlines() if line][-1]
    return json.loads(last_line)


def test_logger_json_output(capfd, ensure_log_dir):
    logger = get_logger("test-service")
    logger.info("hello world")

    captured = capfd.readouterr()
    record = _parse_last_log_line(captured)

    assert record["msg"] == "hello world"
    assert record["level"] == "INFO"
    assert record["service"] == "test-service"
    assert "ts" in record
    assert "T" in record["ts"]  # crude ISO8601 shape check


def test_logger_extra_fields(capfd, ensure_log_dir):
    logger = get_logger("test-service")
    logger.info("order event", extra={"order_id": 123, "side": "buy"})

    captured = capfd.readouterr()
    record = _parse_last_log_line(captured)

    assert record["msg"] == "order event"
    assert record["order_id"] == 123
    assert record["side"] == "buy"
