"""Shared interface for live (Kalshi API) and paper (simulation) exchange adapters."""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional


class ExchangeProtocol(ABC):
    """
    Abstract protocol implemented by both the real Kalshi client and the
    internal paper-trading simulation adapter.
    """

    @abstractmethod
    async def get_btc_contracts(self) -> List[Dict[str, Any]]:
        """Return list of BTC-related contract/market dicts."""
        ...

    @abstractmethod
    async def get_contract(self, contract_id: str) -> Optional[Dict[str, Any]]:
        """Fetch a single contract by id/ticker."""
        ...

    @abstractmethod
    async def get_orderbook(self, contract_id: str) -> Dict[str, Any]:
        """Return orderbook (bids/asks) for the contract."""
        ...

    @abstractmethod
    async def place_order(
        self,
        contract_id: str,
        side: str,
        count: int,
        price_cents: Optional[int] = None,
        type: str = "limit",
        client_order_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Place an order; returns order info."""
        ...

    @abstractmethod
    async def cancel_order(self, order_id: str) -> Dict[str, Any]:
        """Cancel an order by id."""
        ...

    @abstractmethod
    async def get_order(self, order_id: str) -> Optional[Dict[str, Any]]:
        """Get order by id."""
        ...

    @abstractmethod
    async def get_open_orders(self, contract_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """List open orders, optionally filtered by contract."""
        ...

    @abstractmethod
    async def get_positions(self) -> List[Dict[str, Any]]:
        """List current positions."""
        ...

    @abstractmethod
    async def get_balance(self) -> Dict[str, Any]:
        """Return account balance info."""
        ...
