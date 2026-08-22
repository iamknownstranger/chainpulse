"""Live Binance USD-M mark-price stream (public, keyless).

Subscribes to ``!markPrice@arr`` — one event per second carrying mark price
AND funding rate for every perpetual. Events are downsampled to one row per
symbol per minute bucket before persistence, so a day of streaming is ~1.4k
rows/symbol instead of 86k.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from collections.abc import Awaitable, Callable
from decimal import Decimal
from typing import Any

import aiohttp

from chainpulse.models import TickSample

log = logging.getLogger(__name__)

WS_URL = "wss://fstream.binance.com/ws/!markPrice@arr"


def parse_mark_price_batch(
    payload: list[dict[str, Any]],
    symbol_to_base: dict[str, str],
    venue: str = "binance-usdm",
    quote: str = "USDT",
) -> list[TickSample]:
    """Pure parser: wire frames -> downsampled tick models."""
    out: list[TickSample] = []
    for row in payload:
        base = symbol_to_base.get(row.get("s", ""))
        if base is None:
            continue
        out.append(
            TickSample(
                venue=venue,
                base=base,
                quote=quote,
                mark_price=Decimal(row["p"]),
                funding_rate=Decimal(row["r"]) if row.get("r") else None,
                event_ts_ms=int(row["E"]),
            )
        )
    return out


class BinanceMarkPriceStream:
    def __init__(
        self,
        on_batch: Callable[[list[TickSample]], Awaitable[None]],
        symbol_to_base: dict[str, str],
        max_backoff_s: float = 30.0,
    ) -> None:
        self._on_batch = on_batch
        self._symbols = symbol_to_base
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
                    log.warning("stream dropped (%s), reconnecting in %.1fs", exc, backoff)
                    await asyncio.sleep(backoff)
                    backoff = min(self._max_backoff, backoff * 2)

    async def _pump(self, session: aiohttp.ClientSession) -> None:
        async with session.ws_connect(WS_URL, heartbeat=20) as ws:
            log.info("connected: %s", WS_URL)
            async for msg in ws:
                if msg.type is aiohttp.WSMsgType.TEXT:
                    ticks = parse_mark_price_batch(json.loads(msg.data), self._symbols)
                    if ticks:
                        await self._on_batch(ticks)
                elif msg.type in (
                    aiohttp.WSMsgType.CLOSED,
                    aiohttp.WSMsgType.ERROR,
                ):
                    raise ConnectionError("websocket closed by server")
                if self._stop.is_set():
                    return


async def sample_for(
    minutes: float,
    storage: Any,
    symbol_to_base: dict[str, str] | None = None,
) -> int:
    """Stream for a bounded window, persisting downsampled ticks. Returns rows saved."""
    if symbol_to_base is None:
        from chainpulse.connectors.binance import BinanceConnector  # local: avoids cycle

        async with BinanceConnector() as conn:
            symbol_to_base = await conn._usdt_perp_symbols()
    saved = 0

    async def persist(ticks: list[TickSample]) -> None:
        nonlocal saved
        saved += storage.save_ticks(ticks)

    stream = BinanceMarkPriceStream(persist, symbol_to_base or {})
    task = asyncio.create_task(stream.run())
    try:
        await asyncio.sleep(minutes * 60)
    finally:
        stream.stop()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
    return saved
