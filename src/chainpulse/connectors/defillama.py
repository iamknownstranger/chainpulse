"""DeFiLlama aggregates: protocol/chain TVL and stablecoin-aware yield pools.

Free public API, no key, fair-use throttled (~300 req/min/IP) - the token
bucket keeps us far under it.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any

from chainpulse.connectors.base import Connector
from chainpulse.http import HttpClient
from chainpulse.models import ChainTvl, ProtocolTvl, YieldPool

log = logging.getLogger(__name__)


class DefiLlamaConnector(Connector):
    venue = "defillama"

    def __init__(self, client: HttpClient | None = None) -> None:
        super().__init__(client)
        self._yields_client = client or HttpClient(base_url="https://yields.llama.fi")

    async def protocol_tvl(self, slug: str) -> ProtocolTvl:
        value = await self.client.get_json(f"https://api.llama.fi/tvl/{slug}")
        return ProtocolTvl(protocol=slug, tvl_usd=Decimal(str(value)))

    async def chain_tvls(self) -> list[ChainTvl]:
        rows: list[dict[str, Any]] = await self.client.get_json("https://api.llama.fi/chains")
        out = [
            ChainTvl(chain=r["name"], tvl_usd=Decimal(str(r.get("tvl", 0))))
            for r in rows
            if r.get("tvl") is not None
        ]
        log.info("defillama: %d chains", len(out))
        return out

    async def yield_pools(self, limit: int | None = None) -> list[YieldPool]:
        payload: dict[str, Any] = await self._yields_client.get_json(
            "https://yields.llama.fi/pools"
        )
        pools = payload.get("data", [])
        if limit is not None:
            pools = sorted(pools, key=lambda p: p.get("tvlUsd") or 0, reverse=True)[:limit]
        out = [
            YieldPool(
                pool_id=p["pool"],
                chain=p.get("chain", ""),
                project=p.get("project", ""),
                symbol=p.get("symbol", ""),
                tvl_usd=Decimal(str(p.get("tvlUsd") or 0)),
                apy_pct=Decimal(str(p["apy"])) if p.get("apy") is not None else None,
                stablecoin=bool(p.get("stablecoin", False)),
            )
            for p in pools
        ]
        log.info("defillama: %d yield pools", len(out))
        return out

    async def aclose(self) -> None:
        await super().aclose()
        if self._owns_client:
            await self._yields_client.close()
