"""Async orchestration: collect from all venues, persist what succeeded.

Sources fail independently (rate limits, geo-blocks, upstream outages), so
each collection task is isolated: one venue going down must never stop the
others from landing data.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Any

from chainpulse.alerts import AlertConfig, evaluate_alerts
from chainpulse.connectors import (
    BinanceConnector,
    DefiLlamaConnector,
    HyperliquidConnector,
)
from chainpulse.storage import DuckDBStorage

log = logging.getLogger(__name__)


async def _collect(
    label: str,
    coro: Awaitable[list[Any]],
    storage: DuckDBStorage,
    saver: Callable[[list[Any]], int],
) -> str:
    try:
        records = await coro
        saved = saver(records)
        msg = f"{label}: collected={len(records)} new_rows={saved}"
        log.info(msg)
        return msg
    except Exception as exc:  # noqa: BLE001 - isolation is the point
        log.exception("%s failed, continuing without it", label)
        return f"{label}: FAILED ({type(exc).__name__}: {exc})"


async def run_snapshot(
    storage_path: str = "data/chainpulse.duckdb",
    storage: DuckDBStorage | None = None,
    alert_cfg: AlertConfig | None = None,
) -> dict[str, str]:
    """One full sweep across every venue. Safe to re-run at any cadence.

    Sources run concurrently and fail independently; wall time is the slowest
    source, not the sum of them.
    """
    jobs: list[tuple[str, Awaitable[list[Any]], Callable[[list[Any]], int]]] = []
    own_db = storage is None
    db = storage if storage is not None else DuckDBStorage(storage_path)
    try:
        async with (
            BinanceConnector() as binance,
            HyperliquidConnector() as hl,
            DefiLlamaConnector() as llama,
        ):
            jobs = [
                ("binance.funding", binance.funding_snapshots(), db.save_funding),
                ("hyperliquid.funding", hl.funding_snapshots(), db.save_funding),
                ("defillama.chains", llama.chain_tvls(), db.save_chain_tvl),
                ("defillama.yields", llama.yield_pools(limit=100), db.save_yield_pools),
            ]
            outcomes = await asyncio.gather(
                *(_collect(label, coro, db, saver) for label, coro, saver in jobs)
            )
        results = dict(zip((label for label, _, _ in jobs), outcomes, strict=True))
        db.record_sweep(results)
        try:
            evaluate_alerts(db, alert_cfg)
        except Exception:  # noqa: BLE001 - alerts must never break a sweep
            log.exception("alert evaluation failed")
        return results
    finally:
        if own_db:
            db.close()


async def backfill_funding_history(
    symbol: str = "BTCUSDT",
    limit: int = 1000,
    storage_path: str = "data/chainpulse.duckdb",
    storage: DuckDBStorage | None = None,
) -> dict[str, str]:
    """Resume-style backfill: fetch settled funding events past the watermark."""
    stream = f"binance.history.{symbol}"
    async with BinanceConnector() as binance:
        events = await binance.funding_history(symbol, limit=limit)

    own_db = storage is None
    db = storage if storage is not None else DuckDBStorage(storage_path)
    try:
        saved = 0
        if events:
            oldest = min(e.funding_time_ms for e in events)
            newest = max(e.funding_time_ms for e in events)
            wm = db.get_watermark(stream) or {}
            if newest <= wm.get("last_funding_time_ms", 0):
                summary = "no-new-events"
            else:
                saved = db.save_funding(events)
                db.set_watermark(stream, {"last_funding_time_ms": newest})
                summary = f"backfilled={saved} range=[{oldest},{newest}]"
        else:
            summary = "no-events-returned"
        return {"backfill": summary, "rows_saved": str(saved)}
    finally:
        if own_db:
            db.close()


async def backfill_top_funding(
    symbols: list[str],
    limit: int = 1000,
    concurrency: int = 3,
    storage_path: str = "data/chainpulse.duckdb",
    storage: DuckDBStorage | None = None,
) -> dict[str, str]:
    """Watermark-resumable history backfill for many symbols at once."""
    sem = asyncio.Semaphore(concurrency)

    async def one(sym: str) -> tuple[str, str]:
        async with sem:
            res = await backfill_funding_history(sym, limit, storage_path, storage=storage)
            return sym, res["backfill"]

    pairs = await asyncio.gather(*(one(s) for s in symbols))
    return dict(pairs)
