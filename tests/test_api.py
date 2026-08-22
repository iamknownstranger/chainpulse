"""API tests over a pre-seeded DuckDB."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from chainpulse.api import create_app
from chainpulse.models import FundingSnapshot
from chainpulse.storage import DuckDBStorage


@pytest.fixture()
def client(tmp_path: Path):
    db = tmp_path / "api.duckdb"
    with DuckDBStorage(db) as st:
        st.save_funding(
            [
                FundingSnapshot(
                    venue="binance-usdm",
                    base="BTC",
                    quote="USDT",
                    rate=Decimal("0.0001"),
                    interval_hours=8,
                    next_funding_ts_ms=1000,
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
    app = create_app(db)
    with TestClient(app) as test_client:
        yield test_client


def test_health_reports_stats(client: TestClient) -> None:
    body = client.get("/health").json()
    assert body["status"] == "ok"
    assert body["stats"]["funding_events"] == 2


def test_funding_latest_with_filters(client: TestClient) -> None:
    rows = client.get("/funding/latest").json()
    assert len(rows) == 2
    hl = client.get("/funding/latest", params={"venue": "hyperliquid"}).json()
    assert len(hl) == 1 and hl[0]["interval_hours"] == 1
    btc = client.get("/funding/latest", params={"base": "btc"}).json()  # case-insensitive
    assert len(btc) == 2


def test_spread_endpoint(client: TestClient) -> None:
    spread = client.get("/funding/spread").json()
    assert len(spread) == 1
    assert spread[0]["long_apr"] > spread[0]["short_apr"]


def test_wallet_rejects_unsupported_chain(client: TestClient) -> None:
    resp = client.get("/wallet/solana/0xabc")
    assert resp.status_code == 400
