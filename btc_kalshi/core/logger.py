from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone
from logging import Logger
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Dict


_STANDARD_LOG_RECORD_KEYS = {
    "name",
    "msg",
    "args",
    "levelname",
    "levelno",
    "pathname",
    "filename",
    "module",
    "exc_info",
    "exc_text",
    "stack_info",
    "lineno",
    "funcName",
    "created",
    "msecs",
    "relativeCreated",
    "thread",
    "threadName",
    "processName",
    "process",
}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:  # type: ignore[override]
        ts = datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat()

        log_obj: Dict[str, Any] = {
            "ts": ts,
            "level": record.levelname,
            "service": getattr(record, "service", record.name),
            "msg": record.getMessage(),
        }

        for key, value in record.__dict__.items():
            if key in _STANDARD_LOG_RECORD_KEYS:
                continue
            if key.startswith("_"):
                continue
            try:
                json.dumps(value)
                log_obj[key] = value
            except TypeError:
                log_obj[key] = str(value)

        return json.dumps(log_obj, separators=(",", ":"))


class _ServiceFilter(logging.Filter):
    def __init__(self, service_name: str) -> None:
        super().__init__()
        self._service_name = service_name

    def filter(self, record: logging.LogRecord) -> bool:  # type: ignore[override]
        if not hasattr(record, "service"):
            record.service = self._service_name
        return True


def _ensure_log_dir(base_dir: Path | None = None) -> Path:
    if base_dir is None:
        base_dir = Path(os.getcwd())
    log_dir = base_dir / "data" / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    return log_dir


def get_logger(service_name: str) -> Logger:
    """
    Create or retrieve a JSON-logging logger for the given service name.
    """
    logger = logging.getLogger(service_name)

    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    logger.propagate = False

    formatter = JsonFormatter()
    service_filter = _ServiceFilter(service_name)

    # Stdout handler
    stream_handler = logging.StreamHandler(stream=sys.stdout)
    stream_handler.setFormatter(formatter)
    stream_handler.addFilter(service_filter)

    # Rotating file handler
    log_dir = _ensure_log_dir()
    file_path = log_dir / "bot.log"
    file_handler = RotatingFileHandler(
        file_path,
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    file_handler.addFilter(service_filter)

    logger.addHandler(stream_handler)
    logger.addHandler(file_handler)

    return logger

