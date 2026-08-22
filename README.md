# ChainPulse

Normalized crypto market & on-chain intelligence, built entirely on **free,
keyless public APIs** — no signups, no API keys, no cost.

It pulls the same three data planes a professional market-data stack cares
about — **perpetual funding rates across venues**, **DeFi TVL/yields**, and
**on-chain wallet balances** — into one normalized schema, persists them
idempotently to DuckDB, and serves cross-venue analytics over a FastAPI.

```
Binance (USDM perps)  ─┐                          ┌─> DuckDB (dedup + watermarks) ─> FastAPI
Hyperliquid (perp DEX) ─┼─> async connectors ──────┤        │
DeFiLlama (TVL/yields) ─┘   (token bucket, retry,  │        └─> /funding/spread (cash-and-carry view)
Blockscout (ETH balances)    Decimal precision)    ┘            /tvl/chains, /yields, /wallet/{chain}/{addr}
```

## Why this is interesting

| Design choice | Rationale |
|---|---|
| **Cross-venue funding APR spread** | Binance funds every 8h, Hyperliquid hourly; naive rate comparisons lie. Rates are normalized to annualized APR before ranking, so `/funding/spread` surfaces genuine cash-and-carry candidates. |
| **`Decimal` end-to-end** | Floats silently corrupt satoshi-scale balances and sub-pip rates. Wire strings become `Decimal` at the boundary and stay there. |
| **Natural-key idempotent writes** | Every table has a primary key matching the event's identity; re-runs use `ON CONFLICT DO NOTHING`, so the collector can crash/restart on any cadence without double-counting. |
| **Resume-style backfills** | History backfills record per-stream watermarks and only fetch past them — the same pattern production funding-history pipelines need. |
| **Per-source failure isolation** | Venues fail independently (rate limits, geo-blocks). Each collection task is quarantined; one outage never stops the others from landing data. |
| **Keyless by design** | Only public endpoints: Binance market data, Hyperledger info API, DeFiLlama, Blockscout REST. Client-side token buckets keep request volume polite. |

## Quick start

```bash
make install      # uv venv + editable install with dev tools
make snapshot     # one sweep: funding snapshots + chain TVL + top-100 yield pools
python scripts/snapshot.py backfill --symbol ETHUSDT   # settled funding history w/ watermark resume
make api          # http://127.0.0.1:8000/docs
```

## API

| Endpoint | What it shows |
|---|---|
| `GET /health` | row counts per table |
| `GET /funding/latest?venue=&base=` | latest funding state per venue/symbol |
| `GET /funding/spread?top_n=20` | assets ranked by cross-venue annualized APR divergence |
| `GET /tvl/chains` | latest TVL per chain vs previous snapshot |
| `GET /yields?min_tvl_usd=&limit=` | yield pools ranked by APY |
| `GET /wallet/{chain}/{address}` | live native balance via Blockscout (Decimal wei conversion) |

## Layout

```
src/chainpulse/
  models.py       normalized schemas (pydantic v2), canonical BASE/QUOTE symbols
  ratelimit.py    async token bucket
  http.py         shared client: throttling + exponential-backoff retries (Retry-After aware)
  connectors/     binance, hyperliquid, defillama, blockscout (+ registry)
  storage.py      DuckDB schema, idempotent inserts, watermarks, analytics SQL
  pipeline.py     orchestrated sweeps + resumable backfill
  api.py          FastAPI service
tests/            parser fixtures (recorded wire payloads), storage/API/ratelimit/http tests
```

## Data sources (all keyless)

- Binance USD-M public market data (`exchangeInfo`, `premiumIndex`, `fundingRate`)
- Hyperliquid public info endpoint (`metaAndAssetCtxs`)
- DeFiLlama free API (`api.llama.fi`, `yields.llama.fi`)
- Blockscout per-instance REST (`eth.blockscout.com/api/v2`)

## Development

```bash
make lint && make typecheck && make test
```

MIT licensed.
