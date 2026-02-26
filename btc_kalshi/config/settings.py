from __future__ import annotations

from functools import lru_cache
from typing import Literal, Optional

from pydantic import ValidationError, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    KALSHI_API_KEY: Optional[str] = None
    KALSHI_API_SECRET: Optional[str] = None

    COINBASE_WS_URL: str = "wss://advanced-trade-ws.coinbase.com"
    BINANCE_WS_URL: str = (
        "wss://stream.binance.com:9443/ws/btcusdt@trade"
    )

    SQLITE_PATH: str = "data/bot_state.db"
    POSTGRES_DSN: Optional[str] = None

    NTFY_TOPIC: Optional[str] = None
    SMTP_HOST: Optional[str] = None
    SMTP_USER: Optional[str] = None
    SMTP_PASS: Optional[str] = None
    ALERT_EMAIL_TO: Optional[str] = None

    BOT_MODE: Literal["paper", "live"] = "paper"
    LOG_LEVEL: str = "INFO"

    VPS_HOST: str = "0.0.0.0"
    VPS_PORT: int = 8000

    PAPER_STARTING_BANKROLL: float = 100.0

    @model_validator(mode="after")
    def _validate_live_mode(self) -> "Settings":
        if self.BOT_MODE == "live":
            missing = []
            if not self.KALSHI_API_KEY:
                missing.append("KALSHI_API_KEY")
            if not self.KALSHI_API_SECRET:
                missing.append("KALSHI_API_SECRET")
            if missing:
                raise ValidationError.from_exception_data(
                    "Settings",
                    [
                        {
                            "type": "value_error.missing",
                            "loc": (name,),
                            "msg": f"{name} must be set when BOT_MODE is 'live'",
                            "input": None,
                        }
                        for name in missing
                    ],
                )
        return self


@lru_cache
def get_settings() -> Settings:
    """
    Cached accessor for application settings.
    """
    return Settings()

