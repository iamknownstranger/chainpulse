#!/usr/bin/env python3
"""CLI: run a full collection sweep or backfill settled funding history.

Examples:
    python scripts/snapshot.py snapshot
    python scripts/snapshot.py backfill --symbol ETHUSDT --limit 500
    python scripts/snapshot.py stats
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from chainpulse.pipeline import backfill_funding_history, run_snapshot  # noqa: E402
from chainpulse.storage import DuckDBStorage  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description="ChainPulse collector")
    parser.add_argument("--db", default="data/chainpulse.duckdb")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("snapshot", help="collect current state from all venues")
    backfill = sub.add_parser("backfill", help="resume-style funding history backfill")
    backfill.add_argument("--symbol", default="BTCUSDT")
    backfill.add_argument("--limit", type=int, default=1000)
    sub.add_parser("stats", help="print table counts")

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    if args.command == "snapshot":
        results = asyncio.run(run_snapshot(args.db))
        for k, v in sorted(results.items()):
            print(f"{k:>22}  {v}")
    elif args.command == "backfill":
        results = asyncio.run(backfill_funding_history(args.symbol, args.limit, args.db))
        print(json.dumps(results, indent=2))
    else:
        with DuckDBStorage(args.db) as storage:
            print(json.dumps(storage.stats(), indent=2))


if __name__ == "__main__":
    main()
