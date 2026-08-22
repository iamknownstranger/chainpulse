"""On-chain wallet balances via Blockscout's per-instance REST API (no key)."""

from __future__ import annotations

import logging
from decimal import Decimal

from chainpulse.connectors.base import Connector
from chainpulse.http import HttpClient
from chainpulse.models import WalletBalance

log = logging.getLogger(__name__)

INSTANCES = {
    "ethereum": "https://eth.blockscout.com",
}


class BlockscoutConnector(Connector):
    venue = "blockscout"

    def __init__(self, client: HttpClient | None = None) -> None:
        super().__init__(client)

    async def wallet_balance(self, address: str, chain: str = "ethereum") -> WalletBalance:
        try:
            base_url = INSTANCES[chain]
        except KeyError:
            raise ValueError(f"unsupported chain: {chain}") from None
        data = await self.client.get_json(f"{base_url}/api/v2/addresses/{address}")
        wei_raw = data.get("coin_balance") or "0"
        price_raw = data.get("exchange_rate")
        balance = WalletBalance.from_wei(
            address=address.lower(),
            chain=chain,
            wei=Decimal(wei_raw),
            usd_price=Decimal(str(price_raw)) if price_raw else None,
        )
        log.info("blockscout: %s native balance fetched", balance.native_symbol)
        return balance
