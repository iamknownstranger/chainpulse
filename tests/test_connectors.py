"""Parser tests: recorded wire fixtures -> normalized models."""

from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from chainpulse.connectors.binance import BinanceConnector
from chainpulse.connectors.blockscout import BlockscoutConnector
from chainpulse.connectors.defillama import DefiLlamaConnector
from chainpulse.connectors.hyperliquid import HyperliquidConnector
from chainpulse.http import HttpClient
from chainpulse.models import WalletBalance

FIXTURES = Path(__file__).parent / "fixtures"


def load(name: str) -> Any:
    return json.loads((FIXTURES / name).read_text())


class StaticClient(HttpClient):
    """Returns canned payloads; lets us test parsing in isolation."""

    def __init__(self, responses: dict[str, Any]) -> None:
        super().__init__(rate_per_sec=10_000)
        self.responses = responses

    async def get_json(self, path: str, params=None) -> Any:  # type: ignore[override]
        for key, payload in self.responses.items():
            if key in path:
                return payload
        raise AssertionError(f"unexpected GET {path}")

    async def post_json(self, path: str, payload=None) -> Any:  # type: ignore[override]
        return next(iter(self.responses.values()))


async def test_binance_filters_delivery_contracts() -> None:
    conn = BinanceConnector(
        StaticClient(
            {
                "exchangeInfo": load("binance_exchange_info.json"),
                "premiumIndex": load("binance_premium_index.json"),
            }
        )
    )
    snaps = await conn.funding_snapshots()
    symbols = {s.symbol for s in snaps}
    assert symbols == {"BTC/USDT", "ETH/USDT"}  # delivery contract dropped
    btc = next(s for s in snaps if s.base == "BTC")
    assert btc.rate == Decimal("0.00010000")
    assert btc.interval_hours == 8
    assert btc.mark_price == Decimal("77335.33441529")


async def test_binance_funding_history() -> None:
    conn = BinanceConnector(
        StaticClient(
            {
                "exchangeInfo": load("binance_exchange_info.json"),
                "fundingRate": load("binance_funding_rate.json"),
            }
        )
    )
    events = await conn.funding_history("BTCUSDT")
    assert [e.rate for e in events] == [Decimal("0.00010000"), Decimal("0.00012500")]
    assert events[0].symbol == "BTC/USDT"


async def test_hyperliquid_skips_delisted_and_uses_hourly_interval() -> None:
    conn = HyperliquidConnector(StaticClient({"info": load("hyperliquid_meta_ctxs.json")}))
    snaps = await conn.funding_snapshots()
    bases = {s.base for s in snaps}
    assert bases == {"BTC", "SOL"}  # MATIC delisted -> excluded
    sol = next(s for s in snaps if s.base == "SOL")
    assert sol.interval_hours == 1  # hourly funding, unlike binance's 8h
    assert sol.open_interest_base == Decimal("100.5")


async def test_defillama_chains_and_pools() -> None:
    llama = DefiLlamaConnector(
        StaticClient(
            {"chains": load("defillama_chains.json"), "pools": load("defillama_pools.json")}
        )
    )
    chains = await llama.chain_tvls()
    assert ("Ethereum" in {c.chain for c in chains}) and (
        "BrokenChain" not in {c.chain for c in chains}
    )

    pools = await llama.yield_pools(limit=10)
    assert pools[1].apy_pct is None  # null APY survives as NULL, not 0
    assert pools[0].tvl_usd == Decimal("24084169094")


async def test_blockscout_wei_conversion() -> None:
    conn = BlockscoutConnector(StaticClient({"addresses": load("blockscout_address.json")}))
    wb = await conn.wallet_balance("0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045")
    expected_native = Decimal("6640474280883335348") / Decimal(10**18)
    assert wb.balance_native == expected_native
    assert wb.balance_usd == (expected_native * Decimal("2439.2"))
    assert wb.native_symbol == "ETH"
    assert isinstance(wb.balance_native, Decimal)


def test_wallet_balance_unsupported_chain() -> None:
    with pytest.raises(ValueError):
        WalletBalance.from_wei("0xabc", "solana", Decimal(1), None)
