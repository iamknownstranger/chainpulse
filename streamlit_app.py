"""ChainPulse dashboard — the backend boots inside the app itself.

Works locally and on Streamlit Community Cloud: on first load (or when data
goes stale) it opens the DuckDB store and runs a full collection sweep in-
process, then serves charts off it. Sources fail independently, so geo-blocked
or throttled venues never take the page down.
"""

from __future__ import annotations

import asyncio
import sys
import threading
import time
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

import pandas as pd  # noqa: E402
import plotly.express as px  # noqa: E402
import streamlit as st  # noqa: E402

from chainpulse.connectors.blockscout import BlockscoutConnector  # noqa: E402
from chainpulse.models import utcnow  # noqa: E402
from chainpulse.pipeline import run_snapshot  # noqa: E402
from chainpulse.storage import DuckDBStorage  # noqa: E402

DB_PATH = Path(__file__).resolve().parent / "data" / "chainpulse.duckdb"
STALE_AFTER_MIN = 30
SWEEP_LOCK = threading.Lock()

st.set_page_config(
    page_title="ChainPulse",
    page_icon="🛰️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# --------------------------------------------------------------------------
# styling
# --------------------------------------------------------------------------
HERO_CSS = """
<style>
  @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;800&display=swap');
  html, body, [class*="css"] { font-family: 'Inter', sans-serif; }
  .block-container { padding-top: 1.2rem; max-width: 1400px; }

  div[data-testid="stStatusWidget"] { visibility: hidden; }

  .cp-hero {
    background: linear-gradient(120deg, #0e7490 0%, #312e81 55%, #6d28d9 100%);
    border-radius: 18px; padding: 26px 32px; margin-bottom: 22px;
    color: #f0f6ff;
  }
  .cp-hero h1 { margin: 0; font-size: 2.15rem; font-weight: 800; letter-spacing: -0.02em; }
  .cp-hero p  { margin: 6px 0 0; opacity: 0.85; font-size: 1.0rem; }

  .cp-card {
    background: var(--secondary-background-color);
    border: 1px solid rgba(148, 163, 184, 0.14);
    border-radius: 16px; padding: 18px 22px; height: 100%;
  }
  .cp-card .label { font-size: 0.78rem; letter-spacing: 0.09em; text-transform: uppercase;
                    color: #8aa0bf; font-weight: 600; }
  .cp-card .value { font-size: 1.65rem; font-weight: 800; margin-top: 4px; color: #f4f8ff; }
  .cp-card .sub   { font-size: 0.82rem; color: #93a6c4; margin-top: 2px; }

  button[kind="primary"] { border-radius: 10px !important; font-weight: 600 !important; }
</style>
"""


def money(v: Decimal | float | None) -> str:
    x = float(v or 0)
    for div, suffix in ((1e12, "T"), (1e9, "B"), (1e6, "M"), (1e3, "K")):
        if abs(x) >= div:
            return f"${x / div:,.2f}{suffix}"
    return f"${x:,.2f}"


def style_fig(fig, height: int = 430):
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#dbe7fb", family="Inter"),
        height=height,
        margin=dict(l=10, r=10, t=34, b=10),
        coloraxis_colorbar=dict(thickness=10),
    )
    fig.update_xaxes(gridcolor="#233150", zerolinecolor="#233150")
    fig.update_yaxes(gridcolor="#233150", zerolinecolor="#233150")
    return fig


# --------------------------------------------------------------------------
# backend lifecycle (boots with the app)
# --------------------------------------------------------------------------
@st.cache_resource(show_spinner=False)
def open_storage() -> DuckDBStorage:
    return DuckDBStorage(DB_PATH)


def data_age_min(storage: DuckDBStorage) -> float | None:
    row = storage.con.execute("SELECT max(collected_at) FROM chain_tvl").fetchone()
    if not row or row[0] is None:
        return None
    return (utcnow() - row[0]).total_seconds() / 60


def run_sweep(storage: DuckDBStorage) -> dict[str, str]:
    if not SWEEP_LOCK.acquire(blocking=False):
        return {}
    try:
        return asyncio.run(run_snapshot(str(DB_PATH), storage=storage))
    finally:
        SWEEP_LOCK.release()


st.markdown(HERO_CSS, unsafe_allow_html=True)
storage = open_storage()

age = data_age_min(storage)
if age is None:
    with st.status("🚀 Starting backend — collecting first snapshot…", expanded=True) as status:
        st.write("Opening DuckDB store and sweeping all venues…")
        results = run_sweep(storage)
        ok = sum(1 for m in results.values() if "FAILED" not in m)
        st.write(f"Sweep finished: {ok}/{len(results)} sources reported.")
        status.update(label=f"Backend ready ({ok}/{len(results)} sources)", state="complete")
elif age > STALE_AFTER_MIN:
    run_sweep(storage)
    st.rerun()

# --------------------------------------------------------------------------
# sidebar
# --------------------------------------------------------------------------
with st.sidebar:
    st.markdown("## 🛰️ ChainPulse")
    age_now = data_age_min(storage)
    if age_now is None:
        st.caption("no data yet")
    else:
        st.caption(f"data age: **{age_now:.0f} min** ago")

    if st.button("↻ Refresh now", type="primary", use_container_width=True):
        run_sweep(storage)
        st.rerun()

    st.divider()
    st.markdown("### Source health")
    for row in storage.sweep_health():
        icon = "✅" if row["status"] == "ok" else "⛔"
        when = f"{row['ts']:%H:%M}" if row["ts"] else ""
        st.markdown(f"{icon} `{row['source']}` · {when}")

    st.divider()
    st.caption("Built on keyless public APIs:\n\n"
               "- Binance USD-M public market data\n"
               "- Hyperliquid info endpoint\n"
               "- DeFiLlama aggregates\n"
               "- Blockscout REST")

# --------------------------------------------------------------------------
# hero + headline cards
# --------------------------------------------------------------------------
st.markdown(
    """
    <div class="cp-hero">
      <h1>🛰️ ChainPulse</h1>
      <p>Normalized crypto market &amp; on-chain intelligence — cross-venue funding,
      DeFi TVL &amp; yields, wallet balances. Zero API keys.</p>
    </div>
    """,
    unsafe_allow_html=True,
)

chains = storage.chain_tvl_latest()
total_tvl = sum((c[1] for c in chains), Decimal(0)) if chains else Decimal(0)
pools = storage.yields(min_tvl_usd=0, limit=500)
health = storage.sweep_health()
sources_ok = sum(1 for r in health if r["status"] == "ok")

c1, c2, c3, c4 = st.columns(4)
for col, label, value, sub in (
    (c1, "Total tracked TVL", money(total_tvl), f"across {len(chains):,} chains"),
    (c2, "Yield pools", f"{len(pools):,}", "top pools by TVL"),
    (c3, "Funding venues", f"{len({r[0] for r in storage.latest_funding()})}", "reporting perp markets"),
    (c4, "Sources healthy", f"{sources_ok}/{max(len(health), 1)}", "last sweep"),
):
    col.markdown(
        f'<div class="cp-card"><div class="label">{label}</div>'
        f'<div class="value">{value}</div><div class="sub">{sub}</div></div>',
        unsafe_allow_html=True,
    )

st.write("")
tab_funding, tab_tvl, tab_yields, tab_wallet = st.tabs(
    ["⚡ Funding Radar", "⛓️ Chain TVL", "🌾 Yield Pools", "👛 Wallet Explorer"]
)

# --------------------------------------------------------------------------
# funding radar
# --------------------------------------------------------------------------
with tab_funding:
    spread_rows = storage.funding_spread(top_n=50)
    if not spread_rows:
        st.warning(
            "**No perpetual snapshots yet.** Binance/Hyperliquid are unreachable from this "
            "host right now (geo-restrictions are common on shared clouds). "
            "Run `make snapshot` locally, or hit *Refresh now* later — TVL, yields and the "
            "wallet explorer work regardless."
        )
    else:
        left, right = st.columns((3, 2), gap="large")
        df = pd.DataFrame(
            spread_rows,
            columns=["base", "short_venue", "short_apr", "long_venue", "long_apr", "spread_pct"],
        ).head(15).sort_values("spread_pct")
        fig = px.bar(
            df, x="spread_pct", y="base", orientation="h",
            color="spread_pct", color_continuous_scale="Tealgrn",
            custom_data=["short_venue", "short_apr", "long_venue", "long_apr"],
            labels={"spread_pct": "Annualized APR spread (%)", "base": ""},
            title="Cash-and-carry candidates — cross-venue funding divergence",
        )
        fig.update_traces(
            hovertemplate="<b>%{y}</b><br>spread %{x:.2f}% APR<br>"
            "short %{customdata[0]} (%{customdata[1]:.2f}%)<br>"
            "long %{customdata[2]} (%{customdata[3]:.2f}%)<extra></extra>"
        )
        left.plotly_chart(style_fig(fig), use_container_width=True)

        latest = storage.latest_funding()
        ldf = pd.DataFrame(
            latest,
            columns=[
                "venue", "symbol", "base", "rate", "interval_hours",
                "mark_price", "next_funding_ts_ms", "collected_at",
            ],
        )
        right.markdown("#### Latest per venue")
        right.dataframe(
            ldf[["venue", "symbol", "rate", "interval_hours", "mark_price"]],
            hide_index=True,
            column_config={
                "rate": st.column_config.NumberColumn("rate / interval", format="%.5f%%"),
                "interval_hours": st.column_config.NumberColumn("every (h)"),
                "mark_price": st.column_config.NumberColumn("mark", format="$ %.2f"),
            },
            use_container_width=True,
            height=380,
        )

# --------------------------------------------------------------------------
# chain tvl
# --------------------------------------------------------------------------
with tab_tvl:
    if not chains:
        st.info("No TVL data yet — hit **Refresh now**.")
    else:
        top_n = st.slider("Chains shown", 5, min(len(chains), 40), 20)
        tdf = pd.DataFrame(chains[:top_n], columns=["chain", "tvl_usd", "prev_tvl_usd"])
        tdf["tvl"] = tdf["tvl_usd"].map(float)
        tdf["delta"] = tdf.apply(
            lambda r: (float(r.tvl_usd) - float(r.prev_tvl_usd)) / float(r.prev_tvl_usd) * 100
            if r.prev_tvl_usd
            else None,
            axis=1,
        ).round(2)
        fig = px.bar(
            tdf.sort_values("tvl"), x="tvl", y="chain", orientation="h",
            color="tvl", color_continuous_scale="Sunsetdark",
            labels={"tvl": "TVL (USD)", "chain": ""},
            title="Total value locked per chain",
        )
        st.plotly_chart(style_fig(fig, height=max(360, 24 * top_n)), use_container_width=True)
        st.dataframe(
            tdf[["chain", "tvl_usd", "delta"]],
            hide_index=True,
            column_config={
                "tvl_usd": st.column_config.NumberColumn("TVL", format="$ %.0f"),
                "delta": st.column_config.NumberColumn("Δ vs prev snapshot", format="%+.2f%%"),
            },
            use_container_width=True,
        )

# --------------------------------------------------------------------------
# yields
# --------------------------------------------------------------------------
with tab_yields:
    fcol1, fcol2, fcol3 = st.columns([2, 2, 1])
    min_tvl = fcol1.select_slider(
        "Min pool TVL", options=[0, 100_000, 1_000_000, 10_000_000, 100_000_000],
        format_func=money, value=1_000_000,
    )
    stable_only = fcol2.checkbox("Stablecoin pools only")
    top_k = fcol3.number_input("Rows", 10, 300, 50, step=10)
    rows = storage.yields(min_tvl_usd=float(min_tvl), limit=300)
    ydf = pd.DataFrame(
        rows,
        columns=["pool_id", "chain", "project", "symbol", "tvl_usd", "apy_pct", "stablecoin"],
    )
    if stable_only:
        ydf = ydf[ydf["stablecoin"]]
    ydf = ydf.sort_values("apy_pct", ascending=False, na_position="last").head(int(top_k))
    st.dataframe(
        ydf[["project", "chain", "symbol", "tvl_usd", "apy_pct", "stablecoin"]],
        hide_index=True,
        column_config={
            "project": st.column_config.TextColumn("Protocol"),
            "symbol": st.column_config.TextColumn("Pool"),
            "tvl_usd": st.column_config.NumberColumn("TVL", format="$ %.0f"),
            "apy_pct": st.column_config.ProgressColumn(
                "APY %", min_value=0, max_value=100, format="%.2f%%"
            ),
            "stablecoin": st.column_config.CheckboxColumn("Stable"),
        },
        use_container_width=True,
        height=520,
    )

# --------------------------------------------------------------------------
# wallet explorer (live on-chain lookup)
# --------------------------------------------------------------------------
with tab_wallet:
    wcol1, wcol2 = st.columns([4, 1])
    address = wcol1.text_input(
        "Address", placeholder="0x…", value="0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045",
        label_visibility="collapsed",
    )
    fetch_btn = wcol2.button("Fetch balance", type="primary", use_container_width=True)

    if fetch_btn and address.startswith("0x"):
        t0 = time.time()
        try:
            wb = asyncio.run(BlockscoutConnector().wallet_balance(address.strip()))
            storage.save_wallet_balance(wb)
            k1, k2 = st.columns(2)
            k1.metric(f"Balance ({wb.native_symbol})", f"{wb.balance_native:,.6f}")
            k2.metric(
                "USD value",
                money(wb.balance_usd) if wb.balance_usd is not None else "—",
                f"@ ${wb.usd_price:,.2f}/token" if wb.usd_price is not None else None,
                delta_color="off",
            )
            st.caption(f"fetched live from Blockscout in {time.time() - t0:.1f}s")
        except ValueError as exc:
            st.error(str(exc))
        except Exception as exc:  # noqa: BLE001
            st.error(f"Upstream unavailable: {exc}")

    hist = storage.con.execute(
        "SELECT address, native_symbol, balance_native, balance_usd, collected_at "
        "FROM wallet_balances ORDER BY collected_at DESC LIMIT 10"
    ).fetchall()
    if hist:
        st.markdown("#### Recent lookups")
        st.dataframe(
            pd.DataFrame(hist, columns=["address", "asset", "balance", "usd_value", "at"]),
            hide_index=True,
            column_config={
                "balance": st.column_config.NumberColumn(format="%.6f"),
                "usd_value": st.column_config.NumberColumn("usd value", format="$ %.2f"),
            },
            use_container_width=True,
        )

st.caption("ChainPulse · MIT · data from free keyless public APIs · not investment advice")
