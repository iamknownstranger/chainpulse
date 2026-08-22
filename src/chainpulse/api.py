"""Read API over the collected dataset + a live on-chain wallet endpoint."""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query

from chainpulse.connectors import BlockscoutConnector
from chainpulse.storage import DuckDBStorage, utc_iso

log = logging.getLogger(__name__)


def create_app(storage_path: str | Path = "data/chainpulse.duckdb") -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        app.state.storage = DuckDBStorage(storage_path)
        app.state.blockscout = BlockscoutConnector()
        yield
        await app.state.blockscout.aclose()
        app.state.storage.close()

    app = FastAPI(title="ChainPulse", version="0.1.0", lifespan=lifespan)

    @app.get("/health")
    async def health() -> dict:
        return {"status": "ok", "stats": app.state.storage.stats()}

    @app.get("/funding/latest")
    async def funding_latest(venue: str | None = None, base: str | None = None) -> list[dict]:
        rows = app.state.storage.latest_funding()
        cols = [
            "venue",
            "symbol",
            "base",
            "rate",
            "interval_hours",
            "mark_price",
            "next_funding_ts_ms",
            "collected_at",
        ]
        out = [dict(zip(cols, row, strict=True)) for row in rows]
        if venue:
            out = [r for r in out if r["venue"] == venue]
        if base:
            out = [r for r in out if r["base"] == base.upper()]
        return out

    @app.get("/funding/spread")
    async def funding_spread(top_n: int = Query(20, ge=1, le=100)) -> list[dict]:
        """Assets where funding APR diverges across venues (cash-and-carry candidates)."""
        cols = ["base", "short_venue", "short_apr", "long_venue", "long_apr", "spread_pct"]
        return [
            dict(zip(cols, row, strict=True)) for row in app.state.storage.funding_spread(top_n)
        ]

    @app.get("/tvl/chains")
    async def tvl_chains() -> list[dict]:
        rows = app.state.storage.chain_tvl_latest()
        return [
            {
                "chain": chain,
                "tvl_usd": tvl,
                "prev_tvl_usd": prev,
                "delta_pct": (float(tvl - prev) / float(prev) * 100) if prev else None,
                "collected_at": utc_iso(None),
            }
            for chain, tvl, prev in rows
        ]

    @app.get("/yields")
    async def yields(
        min_tvl_usd: float = Query(1_000_000, ge=0), limit: int = Query(50, ge=1, le=500)
    ) -> list[dict]:
        cols = ["pool_id", "chain", "project", "symbol", "tvl_usd", "apy_pct", "stablecoin"]
        return [
            dict(zip(cols, row, strict=True))
            for row in app.state.storage.yields(min_tvl_usd, limit)
        ]

    @app.get("/alerts")
    async def alerts(
        since_hours: float = Query(48, ge=0.1), limit: int = Query(100, ge=1, le=500)
    ) -> list[dict]:
        return app.state.storage.recent_alerts(since_hours, limit)

    @app.get("/ticks/latest")
    async def ticks_latest(
        symbol: str | None = None, limit: int = Query(200, ge=1, le=2000)
    ) -> list[dict]:
        rows = app.state.storage.con.execute(
            """SELECT venue, symbol, base, mark_price, funding_rate, event_ts_ms
               FROM tick_samples
               WHERE (? IS NULL OR symbol = ?)
               ORDER BY event_ts_ms DESC LIMIT ?""",
            [symbol.upper() if symbol else None, symbol.upper() if symbol else None, limit],
        ).fetchall()
        cols = ["venue", "symbol", "base", "mark_price", "funding_rate", "event_ts_ms"]
        return [dict(zip(cols, row, strict=True)) for row in rows]

    @app.get("/wallet/{chain}/{address}")
    async def wallet(chain: str, address: str) -> dict:
        try:
            wb = await app.state.blockscout.wallet_balance(address, chain)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from None
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=502, detail=f"upstream error: {exc}") from None
        return wb.model_dump(mode="json")

    return app


app = create_app()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)
