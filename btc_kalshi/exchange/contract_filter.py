"""Validates and filters Kalshi BTC contracts (canonical format and eligibility)."""
from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from btc_kalshi.core.constants import (
    MAX_CONTRACT_ASK,
    MAX_EXPIRY_MINUTES,
    MAX_SPREAD,
    MIN_BOOK_DEPTH_3C,
    MIN_EXPIRY_MINUTES,
    MIN_OPEN_INTEREST,
    MIN_SESSION_VOLUME,
    STRIKE_DISTANCE_MAX,
    STRIKE_DISTANCE_MIN,
)


def _parse_strike_from_title(title: str) -> Optional[float]:
    """Extract strike price from 'BTC above $60000?' style title. Returns None if not matched."""
    if not title:
        return None
    # Match "above $60000" or "above $95,000"
    m = re.search(r"above\s+\$([\d,]+)", title, re.IGNORECASE)
    if not m:
        return None
    try:
        return float(m.group(1).replace(",", ""))
    except ValueError:
        return None


def _parse_close_time(contract: Dict[str, Any]) -> Optional[datetime]:
    """Parse close_time from contract. Return naive UTC or timezone-aware datetime."""
    raw = contract.get("close_time") or contract.get("closeTime")
    if not raw:
        return None
    if isinstance(raw, datetime):
        return raw.astimezone(timezone.utc) if raw.tzinfo else raw.replace(tzinfo=timezone.utc)
    s = str(raw).strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    elif "+" not in s and "-" not in s[-6:]:
        s = s + "+00:00"
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        return None


def is_canonical_format(contract: Dict[str, Any]) -> bool:
    """
    Return True only for contracts in canonical form: BTC above $X (at time T).
    Reject 'below' and malformed titles.
    """
    title = (contract.get("title") or "").strip()
    if not title:
        return False
    title_lower = title.lower()
    if "below" in title_lower:
        return False
    if "btc" not in title_lower or "above" not in title_lower:
        return False
    strike = _parse_strike_from_title(title)
    return strike is not None and strike > 0


def check_eligibility(
    contract: Dict[str, Any],
    btc_price: float,
    orderbook: Dict[str, Any],
    now: datetime,
) -> Tuple[bool, str]:
    """
    Check if contract is eligible: strike distance, expiry, volume, OI, book depth, ask, spread.
    Returns (eligible, reason_string). reason_string is empty when eligible.
    """
    if btc_price <= 0:
        return False, "invalid_btc_price"

    strike = _parse_strike_from_title(contract.get("title") or "")
    if strike is None:
        return False, "no_strike"
    # "Above" contract: strike distance = (strike - btc_price) / btc_price
    distance = (strike - btc_price) / btc_price
    if distance < STRIKE_DISTANCE_MIN:
        return False, "strike_too_close"
    if distance > STRIKE_DISTANCE_MAX:
        return False, "strike_too_far"

    close = _parse_close_time(contract)
    if close is None:
        return False, "no_expiry"
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    if close.tzinfo is None:
        close = close.replace(tzinfo=timezone.utc)
    delta_minutes = (close - now).total_seconds() / 60
    if delta_minutes < MIN_EXPIRY_MINUTES:
        return False, "expiry_too_soon"
    if delta_minutes > MAX_EXPIRY_MINUTES:
        return False, "expiry_too_far"

    volume = contract.get("volume") or contract.get("session_volume") or 0
    if int(volume) < MIN_SESSION_VOLUME:
        return False, "volume_too_low"

    oi = contract.get("open_interest") or contract.get("open_interest_quantity") or 0
    if int(oi) < MIN_OPEN_INTEREST:
        return False, "open_interest_too_low"

    asks = orderbook.get("asks") or []
    bids = orderbook.get("bids") or []
    if not asks:
        return False, "no_ask"
    best_ask_cents = asks[0].get("price") if isinstance(asks[0], dict) else asks[0]
    best_ask = float(best_ask_cents) / 100.0
    if best_ask > MAX_CONTRACT_ASK:
        return False, "ask_too_high"

    best_bid_cents = bids[0].get("price") if (bids and isinstance(bids[0], dict)) else (bids[0] if bids else 0)
    best_bid = float(best_bid_cents) / 100.0
    spread = best_ask - best_bid
    if spread > MAX_SPREAD:
        return False, "spread_too_wide"

    # Depth: total quantity within 3 cents of best ask
    depth_cut = best_ask_cents + 3
    depth = 0
    for level in asks:
        p = level.get("price") if isinstance(level, dict) else level
        if float(p) <= depth_cut:
            q = level.get("quantity") if isinstance(level, dict) else (level[1] if isinstance(level, (list, tuple)) else 0)
            depth += int(q) if q is not None else 0
    if depth < MIN_BOOK_DEPTH_3C:
        return False, "book_depth_too_low"

    return True, ""


def filter_universe(
    contracts: List[Dict[str, Any]],
    btc_price: float,
    orderbooks: Dict[str, Dict[str, Any]],
    now: datetime,
) -> List[Dict[str, Any]]:
    """
    Return only contracts that are canonical and pass check_eligibility.
    orderbooks keyed by contract ticker (or id).
    """
    result: List[Dict[str, Any]] = []
    for c in contracts:
        if not is_canonical_format(c):
            continue
        ticker = c.get("ticker") or c.get("id") or ""
        ob = orderbooks.get(ticker) if ticker else None
        if not ob:
            continue
        ok, _ = check_eligibility(c, btc_price, ob, now)
        if ok:
            result.append(c)
    return result
