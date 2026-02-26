from __future__ import annotations

from math import floor
from typing import Final

# Position sizing & risk
RISK_PER_TRADE: Final[float] = 0.035
SIZING_BUFFER: Final[float] = 0.02
MAX_OPEN_POSITIONS: Final[int] = 3
MAX_OPEN_EXPOSURE: Final[float] = 90.0
DAILY_STOP_LOSS_PCT: Final[float] = 0.05
WEEKLY_DRAWDOWN_PCT: Final[float] = 0.10
INTRADAY_DRAWDOWN_FLOOR: Final[float] = 0.08
HARD_DRAWDOWN_FLOOR: Final[float] = 0.12
PROFIT_REDUCE_THRESHOLD: Final[float] = 0.12
PROFIT_STOP_THRESHOLD: Final[float] = 0.25

# Market constraints
MAX_CONTRACT_ASK: Final[float] = 0.75
MAX_SPREAD: Final[float] = 0.035
ENTRY_WAIT_SECONDS: Final[int] = 90
REPRICE_AFTER_SECONDS: Final[int] = 45
ENTRY_INITIAL_OFFSET: Final[float] = 0.01
ENTRY_REPRICE_OFFSET: Final[float] = 0.02
MIN_FILL_PCT: Final[float] = 0.60

# Exit & risk management
TAKE_PROFIT_PRICE: Final[float] = 0.90
TAKE_PROFIT_PCT: Final[float] = 0.25
STOP_LOSS_PRICE: Final[float] = 0.08
STOP_LOSS_PCT: Final[float] = 0.20

# Market selection filters
STRIKE_DISTANCE_MIN: Final[float] = 0.006
STRIKE_DISTANCE_MAX: Final[float] = 0.012
MIN_EXPIRY_MINUTES: Final[int] = 45
MAX_EXPIRY_MINUTES: Final[int] = 240
TARGET_EXPIRY_MIN_MINUTES: Final[int] = 60
TARGET_EXPIRY_MAX_MINUTES: Final[int] = 180
MIN_SESSION_VOLUME: Final[int] = 500
MIN_OPEN_INTEREST: Final[int] = 750
MIN_BOOK_DEPTH_3C: Final[int] = 50

# Execution & slippage assumptions
EXECUTION_COST_BUFFER: Final[float] = 0.01
EXIT_SLIPPAGE_BUFFER: Final[float] = 0.02

# Momentum / breakout parameters
ROC_ENTRY_THRESHOLD: Final[float] = 0.004
ROC_REENTRY_THRESHOLD: Final[float] = 0.006
ROC_WINDOW: Final[int] = 10
VOLATILITY_WINDOW: Final[int] = 20
EMA_PERIOD: Final[int] = 50
VOL_HIGH_MULTIPLIER: Final[float] = 2.5
VOL_LOW_MULTIPLIER: Final[float] = 0.3
PRICE_MOVE_THRESHOLD: Final[float] = 0.005
PRICE_MOVE_WINDOW_MIN: Final[int] = 60
PRICE_MOVE_WINDOW_MAX: Final[int] = 180
FALSE_BREAKOUT_BARS: Final[int] = 2

# Internal modeling of Kalshi fee as % of winnings.
KALSHI_FEE_RATE: Final[float] = 0.05


def calculate_fee(
    entry_price: float,
    exit_price: float,
    contracts: int,
    won: bool,
) -> float:
    """
    Model Kalshi fees as a percentage of winnings plus a per-contract
    execution cost buffer.

    This function is the single source of truth for fee modeling across
    live trading, paper trading, and backtesting.
    """
    if contracts <= 0:
        return 0.0

    base_fee = 0.0
    if won:
        winnings_per_contract = max(exit_price - entry_price, 0.0)
        winnings = winnings_per_contract * contracts
        base_fee = winnings * KALSHI_FEE_RATE

    execution_cost = EXECUTION_COST_BUFFER * contracts
    return base_fee + execution_cost


def calculate_position_size(
    bankroll: float,
    entry_price: float,
    stop_price: float,
) -> int:
    """
    Position sizing based on risk-per-trade and distance to stop.

    Formula:
        size = floor( (bankroll * RISK_PER_TRADE) /
                      (entry_price - stop_price + SIZING_BUFFER) )

    Returns 0 if inputs are invalid (e.g. entry <= stop).
    """
    if bankroll <= 0:
        return 0

    if entry_price <= stop_price:
        return 0

    per_contract_risk = entry_price - stop_price + SIZING_BUFFER
    if per_contract_risk <= 0:
        return 0

    size = (bankroll * RISK_PER_TRADE) / per_contract_risk
    contracts = floor(size)
    return max(contracts, 0)

