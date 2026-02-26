import pytest

from btc_kalshi.core.constants import (
    EXECUTION_COST_BUFFER,
    calculate_fee,
    calculate_position_size,
)


def test_fee_calculation_winning_trade():
    entry = 0.40
    exit_price = 0.60
    contracts = 10

    fee = calculate_fee(entry, exit_price, contracts, won=True)

    # winnings = (0.60 - 0.40) * 10 = 2.0
    # fee_on_winnings = 2.0 * 0.05 = 0.1
    # execution buffer = EXECUTION_COST_BUFFER * 10 = 0.1
    # total = 0.2
    assert fee == pytest.approx(0.2, rel=1e-6)


def test_fee_calculation_losing_trade():
    entry = 0.60
    exit_price = 0.40
    contracts = 10

    fee = calculate_fee(entry, exit_price, contracts, won=False)

    # No fee on losses, only execution buffer
    expected = EXECUTION_COST_BUFFER * contracts
    assert fee == pytest.approx(expected, rel=1e-6)


def test_position_sizing_normal():
    bankroll = 100.0
    entry = 0.55
    stop = 0.08

    size = calculate_position_size(bankroll, entry, stop)

    # risk capital = 100 * 0.035 = 3.5
    # per-contract risk = 0.55 - 0.08 + 0.02 = 0.49
    # size = floor(3.5 / 0.49) = 7
    assert size == 7


def test_position_sizing_invalid():
    bankroll = 100.0
    entry = 0.40
    stop = 0.45  # entry <= stop should be treated as invalid

    size = calculate_position_size(bankroll, entry, stop)
    assert size == 0

