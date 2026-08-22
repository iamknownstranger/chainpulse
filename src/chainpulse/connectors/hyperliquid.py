"""Hyperliquid perp DEX (public info endpoint, no API key).

Funding on Hyperliquid is an *hourly* rate; Binance is 8-hourly. The
``interval_hours`` field carries that difference so APR normalization
downstream is honest rather than a silent apples-to-oranges join.
"""

from __future__ import annotations

import logging
from decimal import Decimal

from chainpulse.connectors.base import Connector
from chainpulse.models import FundingSnapshot

log = logging.getLogger(__name__)

INFO_URL = "https://api.hyperliquid.xyz/info"


class HyperliquidConnector(Connector):
    venue = "hyperliquid"

    async def funding_snapshots(self) -> list[FundingSnapshot]:
        payload = await self.client.post_json(INFO_URL, {"type": "metaAndAssetCtxs"})
        if isinstance(payload[0], dict):  # newer schema: [{universe,...}, ctxs]
            universe, ctxs = payload[0]["universe"], payload[1]
        else:  # legacy: [universe, ctxs]
            universe, ctxs = payload
        out: list[FundingSnapshot] = []
        for meta, ctx in zip(universe, ctxs, strict=True):
            if meta.get("isDelisted"):
                continue
            out.append(
                FundingSnapshot(
                    venue=self.venue,
                    base=meta["name"].upper(),
                    quote="USD",
                    rate=Decimal(ctx["funding"]),
                    interval_hours=1,
                    mark_price=Decimal(ctx["markPx"]) if ctx.get("markPx") else None,
                    open_interest_base=Decimal(ctx["openInterest"]),
                )
            )
        log.info("hyperliquid: %d perpetual snapshots", len(out))
        return out
