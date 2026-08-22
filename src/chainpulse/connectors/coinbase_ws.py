"""Live Coinbase spot ticker stream (public, keyless, US-cloud friendly).

Complements the Binance perp stream: where Binance is often geo-blocked from
shared clouds, Coinbase's public feed is not - so at least one live source
works everywhere. Ticks land in the same downsampled ``tick_samples`` table,
so downstream doesn't care which venue a row came from.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import aiohttp

from chainpulse.models import TickSample

log = logging.getLogger(__name__)

WS_URL = "wss://ws-feed.exchange.coinbase.com"
DEFAULT_PRODUCTS = ["BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD", "DOGE-USD"]


def parse_coinbase_ticker(msg: dict[str, Any], venue: str = "coinbase-spot") -> TickSample | None:
    """Pure parser: one ticker frame -> one downsampled tick model."""
    if msg.get("type") != "ticker" or not msg.get("price"):
        return None
    base, _, quote = msg["product_id"].rpartition("-")
    ts_raw = msg.get("time")
    try:
        event_ts_ms = int(
            datetime.fromisoformat(ts_raw.replace("Z", "+00:00")).timestamp() * 1000
        ) if ts_raw else 0
    except ValueError:
        event_ts_ms = 0
    return TickSample(
        venue=venue,
        base=base.upper(),
        quote=quote.upper(),
        mark_price=Decimal(msg["price"]),
        funding_rate=None,
        event_ts_ms=event_ts_ms or int(datetime.now(UTC).timestamp() * 1000),
    )


class CoinbaseTickerStream:
    def __init__(
        self,
        on_tick: Any,
        products: list[str] | None = None,
        max_backoff_s: float = 30.0,
    ) -> None:
        self._on_tick = on_tick
        self._products = products or DEFAULT_PRODUCTS
        self._max_backoff = max_backoff_s
        self._stop = asyncio.Event()

    def stop(self) -> None:
        self._stop.set()

    async def run(self) -> None:
        backoff = 1.0
        async with aiohttp.ClientSession() as session:
            while not self._stop.is_set():
                try:
                    await self._pump(session)
                    backoff = 1.0
                except Exception as exc:  # noqa: BLE001 - reconnect is the contract
                    if self._stop.is_set():
                        break
                    log.warning("coinbase stream dropped (%s), retrying in %.1fs", exc, backoff)
                    await asyncio.sleep(backoff)
                    backoff = min(self._max_backoff, backoff * 2)

    async def _pump(self, session: aiohttp.ClientSession) -> None:
        async with session.ws_connect(WS_URL, heartbeat=20) as ws:
            await ws.send_json(
                {"type": "subscribe", "product_ids": self._products, "channels": ["ticker"]}
            )
            log.info("connected to coinbase tickers: %s", self._products)
            async for msg in ws:
                if msg.type is aiohttp.WSMsgType.TEXT:
                    tick = parse_coinbase_ticker(json.loads(msg.data))
                    if tick is not None:
                        await self._on_tick([tick])
                elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                    raise ConnectionError("websocket closed by server")
                if self._stop.is_set():
                    return


async def sample_coinbase(
    minutes: float,
    storage: Any,
    products: list[str] | None = None,
) -> int:
    """Stream for a bounded window, persisting downsampled ticks."""
    saved = 0

    async def persist(ticks: list[TickSample]) -> None:
        nonlocal saved
        saved += storage.save_ticks(ticks)

    stream = CoinbaseTickerStream(persist, products)
    task = asyncio.create_task(stream.run())
    try:
        await asyncio.sleep(minutes * 60)
    finally:
        stream.stop()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
    return saved
