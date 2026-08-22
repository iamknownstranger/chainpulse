#!/usr/bin/env python3
"""Long-running collector: sweep + resumable backfills on an interval.

python scripts/scheduler.py --interval-min 30 --backfill-symbols BTCUSDT,ETHUSDT
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from chainpulse.pipeline import backfill_top_funding, run_snapshot  # noqa: E402
from chainpulse.storage import DuckDBStorage  # noqa: E402


async def tick_once(storage: DuckDBStorage, symbols: list[str]) -> None:
    results = await run_snapshot(storage=storage)
    for src, msg in sorted(results.items()):
        logging.info("%-22s %s", src, msg)
    if symbols:
        backfills = await backfill_top_funding(symbols, storage=storage)
        for sym, msg in sorted(backfills.items()):
            logging.info("%-22s %s", f"history.{sym}", msg)


def main() -> None:
    parser = argparse.ArgumentParser(description="ChainPulse scheduler")
    parser.add_argument("--db", default="data/chainpulse.duckdb")
    parser.add_argument("--interval-min", type=float, default=30.0)
    parser.add_argument("--backfill-symbols", default="BTCUSDT,ETHUSDT,SOLUSDT")
    parser.add_argument("--once", action="store_true", help="single sweep then exit")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    symbols = [s.strip().upper() for s in args.backfill_symbols.split(",") if s.strip()]

    async def loop() -> None:
        with DuckDBStorage(args.db) as storage:
            while True:
                started = time.monotonic()
                await tick_once(storage, symbols)
                if args.once:
                    return
                elapsed = time.monotonic() - started
                sleep_for = max(10.0, args.interval_min * 60 - elapsed)
                logging.info("next sweep in %.0f min", sleep_for / 60)
                await asyncio.sleep(sleep_for)

    try:
        asyncio.run(loop())
    except KeyboardInterrupt:
        logging.info("scheduler stopped")


if __name__ == "__main__":
    main()
