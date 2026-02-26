# BTC-on-Kalshi Trading Agent

Real-time trading system for Kalshi Bitcoin prediction markets using a momentum breakout strategy.

## Status
Under development. See `docs/` for specification.

## Setup
```bash
cp .env.example .env  # Edit with your real keys
pip install -e ".[dev]"
pytest
```

## Architecture
Single-process Python asyncio application with dual execution paths:
- **Live path**: Real Kalshi API with operator approval gate
- **Paper path**: Internal fill simulation using real orderbook data

Paper trading runs permanently in parallel for shadow comparison.
