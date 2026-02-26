"""Real Kalshi API client implementing ExchangeProtocol."""
from __future__ import annotations

import asyncio
import base64
import random
import time
from typing import Any, Dict, List, Optional

import httpx

from btc_kalshi.core.logger import get_logger
from btc_kalshi.exchange.exchange_protocol import ExchangeProtocol

# Kalshi trade API prefix
TRADE_API_PREFIX = "/trade-api/v2"


class KalshiClient(ExchangeProtocol):
    """
    Live Kalshi exchange client: httpx, auth headers, retry with jitter on 503,
    and api_healthy flag set on 5xx.
    """

    def __init__(
        self,
        base_url: str,
        api_key: str,
        api_secret: str,
        *,
        transport: Optional[httpx.AsyncBaseTransport] = None,
        max_retries: int = 3,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._api_key = api_key
        self._api_secret = api_secret
        self._max_retries = max_retries
        self._logger = get_logger("kalshi-client")
        self._api_healthy = True
        self._client: Optional[httpx.AsyncClient] = None
        self._transport = transport

    @property
    def api_healthy(self) -> bool:
        return self._api_healthy

    def _auth_headers(self, method: str, path: str) -> Dict[str, str]:
        """Build Kalshi-style auth headers (timestamp + key + signature placeholder)."""
        ts_ms = str(int(time.time() * 1000))
        # Message format: timestamp + method + path (no query)
        path_only = path.split("?")[0]
        message = ts_ms + method.upper() + path_only
        # Production should sign with RSA-PSS using api_secret (private key).
        # Placeholder so requests are sent with required headers:
        sig = base64.b64encode(message.encode()).decode()
        return {
            "KALSHI-ACCESS-KEY": self._api_key,
            "KALSHI-ACCESS-TIMESTAMP": ts_ms,
            "KALSHI-ACCESS-SIGNATURE": sig,
        }

    async def _ensure_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                base_url=self._base_url,
                timeout=30.0,
                transport=self._transport,
            )
        return self._client

    async def _request(
        self,
        method: str,
        path: str,
        *,
        json: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> httpx.Response:
        full_path = TRADE_API_PREFIX + path
        headers = self._auth_headers(method, full_path)
        client = await self._ensure_client()
        last_exc: Optional[Exception] = None
        for attempt in range(self._max_retries + 1):
            try:
                r = await client.request(
                    method,
                    full_path,
                    headers=headers,
                    json=json,
                    params=params,
                )
                if r.status_code >= 500:
                    self._api_healthy = False
                    self._logger.critical(
                        "Kalshi API systemic failure",
                        extra={"status": r.status_code, "path": full_path},
                    )
                if r.status_code == 503 and attempt < self._max_retries:
                    jitter = random.uniform(0.5, 1.5)
                    delay = (2**attempt) * jitter
                    await asyncio.sleep(delay)
                    continue
                r.raise_for_status()
                return r
            except httpx.HTTPStatusError as e:
                last_exc = e
                if e.response.status_code == 503 and attempt < self._max_retries:
                    jitter = random.uniform(0.5, 1.5)
                    delay = (2**attempt) * jitter
                    await asyncio.sleep(delay)
                    continue
                raise
            except Exception as e:
                last_exc = e
                raise
        if last_exc:
            raise last_exc
        raise RuntimeError("unreachable")

    async def get_btc_contracts(self) -> List[Dict[str, Any]]:
        """Fetch BTC-related markets and return as list of contract-like dicts."""
        all_contracts: List[Dict[str, Any]] = []
        cursor: Optional[str] = None
        while True:
            params: Dict[str, Any] = {"series_ticker": "BTC", "limit": 200}
            if cursor:
                params["cursor"] = cursor
            r = await self._request("GET", "/markets", params=params)
            data = r.json()
            markets = data.get("markets") or []
            for m in markets:
                all_contracts.append(dict(m))
            cursor = data.get("cursor")
            if not cursor:
                break
        return all_contracts

    async def get_contract(self, contract_id: str) -> Optional[Dict[str, Any]]:
        """Fetch a single market by ticker."""
        try:
            r = await self._request("GET", f"/markets/{contract_id}")
            return r.json()
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return None
            raise

    async def get_orderbook(self, contract_id: str) -> Dict[str, Any]:
        """Return orderbook for the contract."""
        r = await self._request("GET", f"/markets/{contract_id}/orderbook")
        return r.json()

    async def place_order(
        self,
        contract_id: str,
        side: str,
        count: int,
        price_cents: Optional[int] = None,
        type: str = "limit",
        client_order_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Place an order."""
        body: Dict[str, Any] = {
            "ticker": contract_id,
            "action": side.lower(),
            "count": count,
            "type": type,
        }
        if price_cents is not None:
            body["yes_price"] = price_cents
            body["no_price"] = 100 - price_cents
        if client_order_id:
            body["client_order_id"] = client_order_id
        r = await self._request("POST", "/portfolio/orders", json=body)
        return r.json()

    async def cancel_order(self, order_id: str) -> Dict[str, Any]:
        """Cancel order by id."""
        r = await self._request("DELETE", f"/portfolio/orders/{order_id}")
        return r.json()

    async def get_order(self, order_id: str) -> Optional[Dict[str, Any]]:
        """Get order by id."""
        try:
            r = await self._request("GET", f"/portfolio/orders/{order_id}")
            return r.json()
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return None
            raise

    async def get_open_orders(
        self, contract_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """List open orders."""
        params: Dict[str, Any] = {"status": "resting"}
        if contract_id:
            params["ticker"] = contract_id
        r = await self._request("GET", "/portfolio/orders", params=params)
        data = r.json()
        return list(data.get("orders") or [])

    async def get_positions(self) -> List[Dict[str, Any]]:
        """List current positions."""
        r = await self._request("GET", "/portfolio/positions")
        data = r.json()
        return list(data.get("market_positions") or [])

    async def get_balance(self) -> Dict[str, Any]:
        """Return account balance."""
        r = await self._request("GET", "/portfolio/balance")
        return r.json()

    async def close(self) -> None:
        if self._client and not self._client.is_closed:
            await self._client.aclose()
            self._client = None
