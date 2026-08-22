"""Alert engine + live-stream parser/downsampling tests (no network)."""

from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

from chainpulse.alerts import AlertConfig, evaluate_alerts
from chainpulse.connectors.binance_ws import parse_mark_price_batch
from chainpulse.models import ChainTvl, FundingSnapshot, TickSample, utcnow
from chainpulse.storage import DuckDBStorage


def seed_storage(tmp_path: Path) -> DuckDBStorage:
    st = DuckDBStorage(tmp_path / "a.duckdb")
    now_ms = int(utcnow().timestamp() * 1000)
    st.save_funding(
        [
            # BTC: binance ~11% APR vs hyperliquid ~44% APR -> 33pp spread alert
            FundingSnapshot(
                venue="binance-usdm",
                base="BTC",
                quote="USDT",
                rate=Decimal("0.0001"),
                interval_hours=8,
                next_funding_ts_ms=now_ms,
            ),
            FundingSnapshot(
                venue="hyperliquid",
                base="BTC",
                quote="USD",
                rate=Decimal("0.00005"),
                interval_hours=1,
            ),
        ]
    )
    t = utcnow()
    later = ChainTvl(chain="Ethereum", tvl_usd=Decimal("100"))
    later.collected_at = t
    st.save_chain_tvl([later])
    dropped = ChainTvl(chain="Ethereum", tvl_usd=Decimal("90"))
    dropped.collected_at = t.fromtimestamp(t.timestamp() + 3600, tz=t.tzinfo)
    st.save_chain_tvl([dropped])
    return st


def test_alerts_fire_and_are_idempotent(tmp_path: Path) -> None:
    with seed_storage(tmp_path) as st:
        cfg = AlertConfig(funding_apr_abs_pct=30, tvl_drop_pct=5)
        first = evaluate_alerts(st, cfg)
        kinds = {a.kind for a in first}
        assert "funding_apr" in kinds and "tvl_drop" in kinds
        again = evaluate_alerts(st, cfg)  # same hour bucket -> no duplicates persisted
        assert len(again) == len(first)
        assert len(st.recent_alerts()) == len(first)


def test_quiet_storage_raises_nothing(tmp_path: Path) -> None:
    with DuckDBStorage(tmp_path / "q.duckdb") as st:
        assert evaluate_alerts(st, AlertConfig()) == []


def test_mark_price_parser_maps_symbols_and_downsamples() -> None:
    payload = json.loads(
        json.dumps(
            [
                {"s": "BTCUSDT", "p": "77417.0", "r": "0.0001", "E": 1787385601234},
                {"s": "ETHUSDT", "p": "3100.5", "r": "-0.000075", "E": 1787385601456},
                {"s": "BTCUSDT_260925", "p": "80000", "r": "0.0001", "E": 1787385601999},
                {"s": "BTCUSDT", "p": "77418.0", "r": "0.0001", "E": 1787385634567},
            ]
        )
    )
    ticks = parse_mark_price_batch(payload, {"BTCUSDT": "BTC", "ETHUSDT": "ETH"})
    assert {t.base for t in ticks} == {"BTC", "ETH"}  # delivery contract unmapped -> skipped
    btc = [t for t in ticks if t.base == "BTC"]
    # same minute bucket despite different event seconds
    assert btc[0].bucket_ts_ms == btc[1].bucket_ts_ms
    assert all(isinstance(t.mark_price, Decimal) for t in ticks)


def test_tick_persistence_idempotent(tmp_path: Path) -> None:
    tick = TickSample(
        venue="binance-usdm",
        base="BTC",
        quote="USDT",
        mark_price=Decimal("77417"),
        funding_rate=Decimal("0.0001"),
        event_ts_ms=1787385601234,
    )
    with DuckDBStorage(tmp_path / "t.duckdb") as st:
        assert st.save_ticks([tick]) > 0
        assert st.save_ticks([tick]) == 0  # same bucket -> no dup
        assert st.latest_tick_age_s() is not None


def test_coinbase_ticker_parser() -> None:
    from chainpulse.connectors.coinbase_ws import parse_coinbase_ticker

    tick = parse_coinbase_ticker(
        {
            "type": "ticker",
            "product_id": "BTC-USD",
            "price": "77417.5",
            "time": "2026-08-22T15:04:59.123456Z",
        }
    )
    assert tick is not None
    assert (tick.base, tick.quote) == ("BTC", "USD")
    assert tick.mark_price == Decimal("77417.5")

    assert parse_coinbase_ticker({"type": "subscriptions"}) is None  # control frames skipped
