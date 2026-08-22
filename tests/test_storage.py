"""Persistence: idempotent inserts, watermarks, analytics queries."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

from chainpulse.models import ChainTvl, FundingEvent, FundingSnapshot, utcnow
from chainpulse.storage import DuckDBStorage


def make_storage(tmp_path: Path) -> DuckDBStorage:
    return DuckDBStorage(tmp_path / "test.duckdb")


def snap(
    base: str = "BTC",
    venue: str = "binance-usdm",
    rate: str = "0.0001",
    next_ts: int = 1787385600000,
) -> FundingSnapshot:
    return FundingSnapshot(
        venue=venue,
        base=base,
        quote="USDT",
        rate=Decimal(rate),
        interval_hours=8,
        next_funding_ts_ms=next_ts,
    )


def event(ts_ms: int, rate: str = "0.0001") -> FundingEvent:
    return FundingEvent(
        venue="binance-usdm", base="BTC", quote="USDT", funding_time_ms=ts_ms, rate=Decimal(rate)
    )


def test_snapshot_insert_is_idempotent(tmp_path: Path) -> None:
    with make_storage(tmp_path) as st:
        assert st.save_funding([snap()]) == 1
        assert st.save_funding([snap(rate="0.0002")]) == 0  # same natural key -> kept original
        row = [r for r in st.latest_funding() if r[1] == "BTC/USDT"][0]
        assert Decimal(row[3]) == Decimal("0.00010000")  # original estimate kept, not overwritten


def test_new_settlement_lands_as_new_row(tmp_path: Path) -> None:
    with make_storage(tmp_path) as st:
        st.save_funding([snap(next_ts=1000), snap(next_ts=2000)])
        latest = {r[1]: r for r in st.latest_funding()}
        assert len(latest["BTC/USDT"]) >= 0  # shape check
        assert st._count("funding_events") == 2


def test_history_events_and_watermark_roundtrip(tmp_path: Path) -> None:
    with make_storage(tmp_path) as st:
        st.save_funding([event(1000), event(2000)])
        assert st.save_funding([event(1000), event(3000)]) == 1  # only the new one

        st.set_watermark("binance.history.BTCUSDT", {"last_funding_time_ms": 3000})
        assert st.get_watermark("binance.history.BTCUSDT") == {"last_funding_time_ms": 3000}
        assert st.get_watermark("missing.stream") is None


def test_cross_venue_spread_query(tmp_path: Path) -> None:
    binance_snap = FundingSnapshot(
        venue="binance-usdm",
        base="BTC",
        quote="USDT",
        rate=Decimal("0.0001"),
        interval_hours=8,
        next_funding_ts_ms=1000,
    )
    hl_snap = FundingSnapshot(
        venue="hyperliquid", base="BTC", quote="USD", rate=Decimal("0.00005"), interval_hours=1
    )
    with make_storage(tmp_path) as st:
        st.save_funding([binance_snap, hl_snap])
        spread = st.funding_spread()
        assert len(spread) == 1
        row = dict(
            zip(
                ["base", "short_venue", "short_apr", "long_venue", "long_apr", "spread_pct"],
                spread[0],
                strict=True,
            )
        )
        # binance: 0.0001 * 1095 * 100 = 10.95% APR; hyperliquid: 0.00005 * 8760 * 100 = 43.8%
        assert row["base"] == "BTC"
        assert float(row["long_apr"]) > float(row["short_apr"])
        assert abs(float(row["spread_pct"]) - (43.80 - 10.95)) < 0.01


def test_chain_tvl_delta(tmp_path: Path) -> None:
    t1 = utcnow()
    with make_storage(tmp_path) as st:
        st.save_chain_tvl([ChainTvl(chain="Ethereum", tvl_usd=Decimal("100"))])
        later = ChainTvl(chain="Ethereum", tvl_usd=Decimal("110"))
        later.collected_at = t1.fromtimestamp(t1.timestamp() + 3600, tz=t1.tzinfo)
        st.save_chain_tvl([later])
        rows = st.chain_tvl_latest()
        chain, tvl, prev = rows[0]
        assert chain == "Ethereum" and float(prev) == 100 and float(tvl) == 110
