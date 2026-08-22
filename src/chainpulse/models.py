"""Normalized domain schemas.

Money-precision rule: every price, balance and rate is a ``Decimal`` parsed
from the source string. Floats silently corrupt nanosecond timestamps and
satoshi-scale balances; we keep strings until the boundary, then Decimal all
the way down.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import ClassVar

from pydantic import BaseModel, Field, computed_field, field_validator


def utcnow() -> datetime:
    return datetime.now(UTC)


def canonical(base: str, quote: str) -> str:
    return f"{base.upper()}/{quote.upper()}"


class Record(BaseModel):
    collected_at: datetime = Field(default_factory=utcnow)

    @field_validator("collected_at", mode="after")
    @classmethod
    def _tz_aware(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("collected_at must be timezone-aware")
        return v


class FundingSnapshot(Record):
    """One venue's current funding state for a perpetual market."""

    venue: str
    base: str
    quote: str
    rate: Decimal
    interval_hours: int
    mark_price: Decimal | None = None
    open_interest_base: Decimal | None = None
    next_funding_ts_ms: int | None = None

    symbol: str = ""

    def model_post_init(self, __context: object) -> None:
        if not self.symbol:
            self.symbol = canonical(self.base, self.quote)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def apr_pct(self) -> Decimal:
        """Annualized percentage rate, normalized across funding intervals."""
        periods_per_year = Decimal(24 * 365) / Decimal(self.interval_hours)
        return (self.rate * periods_per_year * Decimal(100)).quantize(Decimal("0.01"))


class FundingEvent(Record):
    """A settled historical funding payment."""

    venue: str
    base: str
    quote: str
    funding_time_ms: int
    rate: Decimal
    interval_hours: int = 8

    symbol: str = ""

    def model_post_init(self, __context: object) -> None:
        if not self.symbol:
            self.symbol = canonical(self.base, self.quote)


class ProtocolTvl(Record):
    protocol: str
    tvl_usd: Decimal


class ChainTvl(Record):
    chain: str
    tvl_usd: Decimal


class YieldPool(Record):
    pool_id: str
    chain: str
    project: str
    symbol: str
    tvl_usd: Decimal
    apy_pct: Decimal | None = None
    stablecoin: bool = False


class WalletBalance(Record):
    CHAIN_DECIMALS: ClassVar[dict[str, int]] = {
        "ethereum": 18,
        "base": 18,
        "zksync": 18,
        "monad": 18,
    }
    NATIVE_SYMBOLS: ClassVar[dict[str, str]] = {"ethereum": "ETH"}

    address: str
    chain: str = "ethereum"
    native_symbol: str = ""
    balance_native: Decimal
    usd_price: Decimal | None = None
    balance_usd: Decimal | None = None

    def model_post_init(self, __context: object) -> None:
        if not self.native_symbol:
            self.native_symbol = self.NATIVE_SYMBOLS.get(self.chain, self.chain.upper())

    @classmethod
    def from_wei(
        cls, address: str, chain: str, wei: Decimal, usd_price: Decimal | None
    ) -> WalletBalance:
        decimals = cls.CHAIN_DECIMALS.get(chain)
        if decimals is None:
            raise ValueError(f"unsupported chain: {chain}")
        scale = Decimal(10) ** decimals
        balance_native = wei / scale
        balance_usd = balance_native * usd_price if usd_price is not None else None
        return cls(
            address=address,
            chain=chain,
            balance_native=balance_native,
            usd_price=usd_price,
            balance_usd=balance_usd,
        )


class TickSample(Record):
    """One downsampled observation from a live market stream."""

    venue: str
    base: str
    quote: str
    mark_price: Decimal
    funding_rate: Decimal | None = None
    event_ts_ms: int
    bucket_seconds: ClassVar[int] = 60

    symbol: str = ""

    def model_post_init(self, __context: object) -> None:
        if not self.symbol:
            self.symbol = canonical(self.base, self.quote)

    @property
    def bucket_ts_ms(self) -> int:
        step = self.bucket_seconds * 1000
        return self.event_ts_ms - (self.event_ts_ms % step)


class Alert(Record):
    """A threshold breach raised after a sweep. Idempotent per hour-bucket."""

    kind: str
    severity: str = "warning"
    subject: str
    detail: str
    bucket_ts_ms: int = 0

    def model_post_init(self, __context: object) -> None:
        if not self.bucket_ts_ms:
            self.bucket_ts_ms = int(self.collected_at.timestamp() * 1000) // 3_600_000 * 3_600_000


class TokenHolding(Record):
    contract: str
    symbol: str
    decimals: int = 18
    balance_native: Decimal
    usd_price: Decimal | None = None
    usd_value: Decimal | None = None


class TxSummary(Record):
    hash: str
    method: str = ""
    value_native: Decimal = Decimal(0)
    direction: str = ""
    timestamp_ms: int = 0
