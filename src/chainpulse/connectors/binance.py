"""Binance USD-M perpetuals (public market-data endpoints, no API key)."""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any

from chainpulse.connectors.base import Connector
from chainpulse.http import HttpClient
from chainpulse.models import FundingEvent, FundingSnapshot

log = logging.getLogger(__name__)

FAPI = "https://fapi.binance.com"


class BinanceConnector(Connector):
    venue = "binance-usdm"

    def __init__(self, client: HttpClient | None = None) -> None:
        super().__init__(client)
        self._perp_bases: dict[str, str] | None = None

    async def _usdt_perp_symbols(self) -> dict[str, str]:
        """symbol -> base asset, for TRADING USDT perpetuals."""
        if self._perp_bases is None:
            info = await self.client.get_json(f"{FAPI}/fapi/v1/exchangeInfo")
            self._perp_bases = {
                s["symbol"]: s["baseAsset"]
                for s in info["symbols"]
                if s.get("contractType") == "PERPETUAL"
                and s.get("quoteAsset") == "USDT"
                and s.get("status") == "TRADING"
            }
        return self._perp_bases

    async def funding_snapshots(self) -> list[FundingSnapshot]:
        perps = await self._usdt_perp_symbols()
        raw: list[dict[str, Any]] = await self.client.get_json(f"{FAPI}/fapi/v1/premiumIndex")
        out: list[FundingSnapshot] = []
        for row in raw:
            symbol = row["symbol"]
            base = perps.get(symbol)
            if base is None:
                continue  # delivery contracts / non-USDT quotes are out of scope
            out.append(
                FundingSnapshot(
                    venue=self.venue,
                    base=base,
                    quote="USDT",
                    rate=Decimal(row["lastFundingRate"]),
                    interval_hours=8,
                    mark_price=Decimal(row["markPrice"]),
                    next_funding_ts_ms=int(row["nextFundingTime"]),
                )
            )
        log.info("binance: %d perpetual snapshots", len(out))
        return out

    async def funding_history(self, symbol: str, limit: int = 1000) -> list[FundingEvent]:
        """Most-recent-first settled funding events for one symbol."""
        perps = await self._usdt_perp_symbols()
        base = perps[symbol]  # KeyError on unknown/delivery symbol is fine: caller bug
        rows: list[dict[str, Any]] = await self.client.get_json(
            f"{FAPI}/fapi/v1/fundingRate", params={"symbol": symbol, "limit": min(limit, 1000)}
        )
        return [
            FundingEvent(
                venue=self.venue,
                base=base,
                quote="USDT",
                funding_time_ms=int(r["fundingTime"]),
                rate=Decimal(r["fundingRate"]),
            )
            for r in rows
        ]
