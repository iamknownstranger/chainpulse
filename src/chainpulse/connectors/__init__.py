"""Connector registry."""

from __future__ import annotations

from collections.abc import Callable

from chainpulse.connectors.base import Connector
from chainpulse.connectors.binance import BinanceConnector
from chainpulse.connectors.blockscout import BlockscoutConnector
from chainpulse.connectors.defillama import DefiLlamaConnector
from chainpulse.connectors.hyperliquid import HyperliquidConnector

REGISTRY: dict[str, Callable[[], Connector]] = {
    BinanceConnector.venue: BinanceConnector,
    HyperliquidConnector.venue: HyperliquidConnector,
    DefiLlamaConnector.venue: DefiLlamaConnector,
}

__all__ = [
    "REGISTRY",
    "BinanceConnector",
    "BlockscoutConnector",
    "Connector",
    "DefiLlamaConnector",
    "HyperliquidConnector",
]
