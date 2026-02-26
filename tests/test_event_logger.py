import json
from datetime import datetime, timezone

from btc_kalshi.db.event_logger import EventLogger


def _fixed_dt(date_str: str) -> datetime:
    return datetime.fromisoformat(date_str).replace(tzinfo=timezone.utc)


def _read_lines(log_dir):
    return list(log_dir.glob("events-*.log"))[0].read_text().strip().splitlines()


def test_log_event_to_flat_file(tmp_path):
    logger = EventLogger.init(postgres_dsn=None, log_dir=tmp_path)

    logger.log_event(
        event_type="TRADE_EXECUTED",
        severity="INFO",
        service_name="test-service",
        contract_id="C123",
        payload={"foo": "bar"},
    )

    files = list(tmp_path.glob("events-*.log"))
    assert len(files) == 1

    lines = files[0].read_text().strip().splitlines()
    assert len(lines) == 1

    event = json.loads(lines[0])
    assert event["event_type"] == "TRADE_EXECUTED"
    assert event["severity"] == "INFO"
    assert event["service_name"] == "test-service"
    assert event["contract_id"] == "C123"
    assert event["payload"] == {"foo": "bar"}


def test_log_event_without_postgres(tmp_path):
    logger = EventLogger.init(postgres_dsn=None, log_dir=tmp_path)

    # Should not raise even though no Postgres DSN is provided.
    logger.log_event(
        event_type="HEARTBEAT",
        severity="DEBUG",
        service_name="test-service",
        contract_id=None,
        payload={},
    )

    files = list(tmp_path.glob("events-*.log"))
    assert len(files) == 1


def test_event_payload_structure(tmp_path):
    logger = EventLogger.init(postgres_dsn=None, log_dir=tmp_path)

    logger.log_event(
        event_type="SAMPLE_EVENT",
        severity="WARN",
        service_name="config-service",
        contract_id="C999",
        payload={"k": "v"},
    )

    lines = _read_lines(tmp_path)
    event = json.loads(lines[0])

    assert set(event.keys()) == {
        "ts",
        "event_type",
        "severity",
        "service_name",
        "contract_id",
        "mode",
        "payload",
    }
    # Basic shape checks
    assert event["mode"] == "live"
    assert "T" in event["ts"]


def test_daily_file_rotation(tmp_path, monkeypatch):
    logger = EventLogger.init(postgres_dsn=None, log_dir=tmp_path)

    # First event on day 1
    monkeypatch.setattr(
        "btc_kalshi.db.event_logger.EventLogger._now",
        staticmethod(lambda: _fixed_dt("2025-01-01T12:00:00")),
    )
    logger.log_event(
        event_type="DAY1_EVENT",
        severity="INFO",
        service_name="svc",
        contract_id=None,
        payload={},
    )

    # Second event on day 2
    monkeypatch.setattr(
        "btc_kalshi.db.event_logger.EventLogger._now",
        staticmethod(lambda: _fixed_dt("2025-01-02T09:00:00")),
    )
    logger.log_event(
        event_type="DAY2_EVENT",
        severity="INFO",
        service_name="svc",
        contract_id=None,
        payload={},
    )

    day1_file = tmp_path / "events-2025-01-01.log"
    day2_file = tmp_path / "events-2025-01-02.log"

    assert day1_file.exists()
    assert day2_file.exists()


def test_mode_tag_in_event(tmp_path):
    logger = EventLogger.init(postgres_dsn=None, log_dir=tmp_path)

    logger.log_event(
        event_type="PAPER_EVENT",
        severity="INFO",
        service_name="svc",
        contract_id=None,
        payload={"x": 1},
        mode="paper",
    )

    lines = _read_lines(tmp_path)
    event = json.loads(lines[0])

    assert event["mode"] == "paper"

