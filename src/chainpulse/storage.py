"""DuckDB persistence with natural-key dedup and stream watermarks.

Every insert is idempotent: rows carry a natural key and re-runs use
``ON CONFLICT DO NOTHING`` instead of trusting callers to behave. Streams
that page through history record a watermark so backfills resume where they
left off rather than double-counting.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import duckdb

from chainpulse.models import FundingEvent, FundingSnapshot, WalletBalance

SCHEMA = """
CREATE TABLE IF NOT EXISTS funding_events (
    venue            TEXT NOT NULL,
    symbol           TEXT NOT NULL,
    base             TEXT NOT NULL,
    quote            TEXT NOT NULL,
    rate             DECIMAL(28, 18) NOT NULL,
    interval_hours   INTEGER NOT NULL,
    mark_price       DECIMAL(28, 18),
    open_interest_base DECIMAL(28, 18),
    next_funding_ts_ms BIGINT,
    funding_time_ms  BIGINT NOT NULL,
    collected_at     TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (venue, symbol, funding_time_ms)
);
CREATE TABLE IF NOT EXISTS chain_tvl (
    chain         TEXT NOT NULL,
    tvl_usd       DECIMAL(38, 6) NOT NULL,
    collected_at  TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (chain, collected_at)
);
CREATE TABLE IF NOT EXISTS yield_pools (
    pool_id       TEXT NOT NULL,
    chain         TEXT NOT NULL,
    project       TEXT NOT NULL,
    symbol        TEXT NOT NULL,
    tvl_usd       DECIMAL(38, 6) NOT NULL,
    apy_pct       DOUBLE,
    stablecoin    BOOLEAN NOT NULL,
    collected_at  TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (pool_id, collected_at)
);
CREATE TABLE IF NOT EXISTS wallet_balances (
    address         TEXT NOT NULL,
    chain           TEXT NOT NULL,
    native_symbol   TEXT NOT NULL,
    balance_native  DECIMAL(38, 18) NOT NULL,
    usd_price       DECIMAL(28, 18),
    balance_usd     DECIMAL(38, 18),
    collected_at    TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (address, chain, collected_at)
);
CREATE TABLE IF NOT EXISTS watermarks (
    stream      TEXT PRIMARY KEY,
    value       JSON NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL
);
CREATE TABLE IF NOT EXISTS sweep_log (
    ts        TIMESTAMPTZ NOT NULL,
    source    TEXT NOT NULL,
    status    TEXT NOT NULL,
    message   TEXT
);
"""


class DuckDBStorage:
    def __init__(self, path: str | Path = "data/chainpulse.duckdb") -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.con = duckdb.connect(str(self.path))
        self.con.execute(SCHEMA)

    def close(self) -> None:
        self.con.close()

    def __enter__(self) -> DuckDBStorage:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    # -- inserts ---------------------------------------------------------

    def save_funding(self, events: Sequence[FundingSnapshot | FundingEvent]) -> int:
        if not events:
            return 0

        def dec(v: Decimal | None) -> str | None:  # noqa: UP047
            return str(v) if v is not None else None

        rows = []
        for e in events:
            event_ts = getattr(e, "funding_time_ms", None)
            if event_ts is None:  # snapshots key on the upcoming settlement
                event_ts = getattr(e, "next_funding_ts_ms", None) or 0
            rows.append(
                (
                    e.venue,
                    e.symbol,
                    e.base,
                    e.quote,
                    str(e.rate),
                    e.interval_hours,
                    dec(getattr(e, "mark_price", None)),
                    dec(getattr(e, "open_interest_base", None)),
                    getattr(e, "next_funding_ts_ms", None),
                    event_ts,
                    e.collected_at,
                )
            )
        before = self._count("funding_events")
        self.con.executemany(
            """INSERT INTO funding_events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT DO NOTHING""",
            rows,
        )
        return self._count("funding_events") - before

    def save_chain_tvl(self, records: list) -> int:
        if not records:
            return 0
        self.con.executemany(
            "INSERT INTO chain_tvl VALUES (?, ?, ?) ON CONFLICT DO NOTHING",
            [(r.chain, str(r.tvl_usd), r.collected_at) for r in records],
        )
        return len(records)

    def save_yield_pools(self, pools: list) -> int:
        if not pools:
            return 0
        self.con.executemany(
            "INSERT INTO yield_pools VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
            [
                (
                    p.pool_id,
                    p.chain,
                    p.project,
                    p.symbol,
                    str(p.tvl_usd),
                    float(p.apy_pct) if p.apy_pct is not None else None,
                    p.stablecoin,
                    p.collected_at,
                )
                for p in pools
            ],
        )
        return len(pools)

    def save_wallet_balance(self, wb: WalletBalance) -> None:
        self.con.execute(
            "INSERT INTO wallet_balances VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
            (
                wb.address,
                wb.chain,
                wb.native_symbol,
                str(wb.balance_native),
                str(wb.usd_price) if wb.usd_price is not None else None,
                str(wb.balance_usd) if wb.balance_usd is not None else None,
                wb.collected_at,
            ),
        )

    def record_sweep(self, results: dict[str, str]) -> None:
        self.con.executemany(
            "INSERT INTO sweep_log VALUES (now(), ?, ?, ?)",
            [(src, "failed" if "FAILED" in msg else "ok", msg) for src, msg in results.items()],
        )

    def sweep_health(self) -> list[dict]:
        return [
            dict(zip(("ts", "source", "status", "message"), row, strict=True))
            for row in self.con.execute(
                """
                SELECT max(ts), source, arg_max(status, ts), arg_max(message, ts)
                FROM sweep_log GROUP BY source ORDER BY source
                """
            ).fetchall()
        ]

    # -- watermarks ------------------------------------------------------

    def get_watermark(self, stream: str) -> dict | None:
        row = self.con.execute("SELECT value FROM watermarks WHERE stream = ?", [stream]).fetchone()
        return json.loads(row[0]) if row else None

    def set_watermark(self, stream: str, value: dict) -> None:
        self.con.execute(
            """INSERT INTO watermarks VALUES (?, ?, now())
               ON CONFLICT (stream) DO UPDATE SET value = excluded.value, updated_at = now()""",
            [stream, json.dumps(value)],
        )

    # -- reads for the API ----------------------------------------------

    def latest_funding(self) -> list[tuple]:
        return self.con.execute(
            """
            SELECT venue, symbol, base, rate, interval_hours, mark_price, next_funding_ts_ms,
                   collected_at
            FROM funding_events
            WHERE funding_time_ms IN (
                SELECT max(funding_time_ms) FROM funding_events GROUP BY venue, symbol
            )
            ORDER BY base, venue
            """
        ).fetchall()

    def funding_spread(self, top_n: int = 20) -> list[tuple]:
        """Cross-venue APR spread per asset: long the cheap venue, short the rich one."""
        return self.con.execute(
            """
            WITH aprs AS (
                SELECT base, venue,
                       rate * 8760 / interval_hours * 100 AS apr_pct
                FROM funding_events
                WHERE funding_time_ms IN (
                    SELECT max(funding_time_ms) FROM funding_events GROUP BY venue, symbol
                )
            )
            SELECT base,
                   arg_min(venue, apr_pct) AS short_venue,
                   min(apr_pct) AS short_apr,
                   arg_max(venue, apr_pct) AS long_venue,
                   max(apr_pct) AS long_apr,
                   max(apr_pct) - min(apr_pct) AS spread_pct
            FROM aprs
            GROUP BY base
            HAVING count(DISTINCT venue) > 1
            ORDER BY spread_pct DESC
            LIMIT ?
            """,
            [top_n],
        ).fetchall()

    def chain_tvl_latest(self) -> list[tuple]:
        return self.con.execute(
            """
            WITH ranked AS (
                SELECT chain, tvl_usd, collected_at,
                       row_number() OVER (PARTITION BY chain ORDER BY collected_at DESC) AS rn
                FROM chain_tvl
            )
            SELECT chain,
                   max(CASE WHEN rn = 1 THEN tvl_usd END) AS tvl_usd,
                   max(CASE WHEN rn = 2 THEN tvl_usd END) AS prev_tvl_usd
            FROM ranked
            WHERE rn <= 2
            GROUP BY chain
            ORDER BY tvl_usd DESC
            """
        ).fetchall()

    def yields(self, min_tvl_usd: float = 0.0, limit: int = 50) -> list[tuple]:
        return self.con.execute(
            """
            SELECT pool_id, chain, project, symbol, tvl_usd, apy_pct, stablecoin
            FROM (
                SELECT *, row_number() OVER (PARTITION BY pool_id ORDER BY collected_at DESC) AS rn
                FROM yield_pools
            )
            WHERE rn = 1 AND tvl_usd >= ?
            ORDER BY apy_pct DESC NULLS LAST
            LIMIT ?
            """,
            [min_tvl_usd, limit],
        ).fetchall()

    def _count(self, table: str) -> int:
        row = self.con.execute(f"SELECT count(*) FROM {table}").fetchone()  # noqa: S608
        assert row is not None
        return int(row[0])

    def stats(self) -> dict[str, int | None]:
        tables = ["funding_events", "chain_tvl", "yield_pools", "wallet_balances"]
        counts: dict[str, int | None] = {t: self._count(t) for t in tables}
        wm = self.con.execute("SELECT stream, updated_at FROM watermarks").fetchall()
        counts["watermarked_streams"] = len(wm)
        return counts


def utc_iso(ts: datetime | None) -> str | None:
    return ts.isoformat() if ts else None
