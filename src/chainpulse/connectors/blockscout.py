"""On-chain wallet balances via Blockscout's per-instance REST API (no key)."""

from __future__ import annotations

import logging
from datetime import datetime
from decimal import Decimal

from chainpulse.connectors.base import Connector
from chainpulse.http import HttpClient, HttpError
from chainpulse.models import TokenHolding, TxSummary, WalletBalance

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

    async def wallet_overview(self, address: str, chain: str = "ethereum") -> dict:
        """Full picture for a pasted address: native balance, ENS, tokens, recent txs."""
        base_url = INSTANCES.get(chain)
        if base_url is None:
            raise ValueError(f"unsupported chain: {chain}")
        addr_path = f"{base_url}/api/v2/addresses/{address}"

        info = await self.client.get_json(addr_path)
        wei_raw = info.get("coin_balance") or "0"
        price_raw = info.get("exchange_rate")
        balance = WalletBalance.from_wei(
            address=address.lower(),
            chain=chain,
            wei=Decimal(wei_raw),
            usd_price=Decimal(str(price_raw)) if price_raw else None,
        )
        ens = info.get("ens_domain_name")

        try:
            raw_tokens = await self.client.get_json(f"{addr_path}/token-balances")
        except HttpError:
            raw_tokens = []
        tokens: list[TokenHolding] = []
        for row in raw_tokens[:50]:
            tok = row.get("token") or {}
            decimals = int(tok.get("decimals") or 18)
            raw_value = Decimal(row.get("value") or 0)
            scale = Decimal(10) ** min(decimals, 36)
            price = Decimal(str(tok["exchange_rate"])) if tok.get("exchange_rate") else None
            tokens.append(
                TokenHolding(
                    contract=tok.get("address_hash") or tok.get("address") or "",
                    symbol=tok.get("symbol") or "?",
                    decimals=decimals,
                    balance_native=raw_value / scale,
                    usd_price=price,
                    usd_value=(raw_value / scale * price) if price else None,
                )
            )
        tokens.sort(key=lambda t: t.usd_value or 0, reverse=True)

        try:
            raw_txs = await self.client.get_json(f"{addr_path}/transactions")
        except HttpError:
            raw_txs = []
        items = raw_txs.get("items", []) if isinstance(raw_txs, dict) else raw_txs
        addr_lc = address.lower()

        def _norm_ts(raw: str | None) -> int:
            if not raw:
                return 0
            try:
                v = int(raw)
            except ValueError:
                try:
                    return int(
                        datetime.fromisoformat(raw.replace("Z", "+00:00")).timestamp() * 1000
                    )
                except ValueError:
                    return 0
            return v * 1000 if v < 10**12 else v

        txs = []
        for t in items[:6]:
            to_addr = (t.get("to") or {}).get("hash", "")
            ts_raw = str(t["timestamp"]) if t.get("timestamp") is not None else ""
            txs.append(
                TxSummary(
                    hash=t.get("hash") or "",
                    method=t.get("method") or "transfer",
                    value_native=Decimal(str(t.get("value") or 0)) / Decimal(10**18),
                    direction="in" if to_addr.lower() == addr_lc else "out",
                    timestamp_ms=_norm_ts(ts_raw),
                )
            )
        log.info(
            "blockscout overview: %s tokens=%d txs=%d", ens or address[:10], len(tokens), len(txs)
        )
        return {
            "balance": balance,
            "ens_name": ens,
            "tokens": tokens,
            "recent_txs": txs,
        }
