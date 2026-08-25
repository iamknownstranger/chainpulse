"""ChainPulse dashboard — normalized crypto market & on-chain intelligence.

The backend boots with the app: first load (or an explicit refresh) opens the
DuckDB store and sweeps all venues in-process. Sources fail independently,
so geo-blocked venues never take the page down.
"""

from __future__ import annotations

import asyncio
import secrets
import sys
import threading
import time
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

import pandas as pd  # noqa: E402
import plotly.express as px  # noqa: E402
import plotly.graph_objects as go  # noqa: E402
import streamlit as st  # noqa: E402

from chainpulse.connectors.blockscout import BlockscoutConnector  # noqa: E402
from chainpulse.enclave import AttestationError, KmsLite, attest  # noqa: E402
from chainpulse.models import utcnow  # noqa: E402
from chainpulse.pipeline import run_snapshot  # noqa: E402
from chainpulse.storage import DuckDBStorage  # noqa: E402

DB_PATH = Path(__file__).resolve().parent / "data" / "chainpulse.duckdb"
VAULT_ROOT = Path(__file__).resolve().parent / "data" / "vault"
STALE_AFTER_MIN = 30
SWEEP_LOCK = threading.Lock()

# --------------------------------------------------------------------------
# design tokens
# --------------------------------------------------------------------------
ACCENT = "#34d399"
ACCENT_2 = "#a78bfa"
WARN = "#fbbf24"
DANGER = "#f87171"
TEXT = "#e8eef9"
MUTED = "#8fa0bd"
GRID = "rgba(148,163,184,0.08)"

st.set_page_config(page_title="ChainPulse", page_icon="", layout="wide")

CSS = """
<style>
  @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
  html, body, [class*="css"], button, input {
    font-family: 'Inter', -apple-system, sans-serif !important;
  }
  .block-container { padding: 2rem 2.4rem 3rem; max-width: 1360px; }
  header[data-testid="stHeader"] { background: transparent; }
  div[data-testid="stStatusWidget"] { visibility: hidden; }

  /* brand row */
  .cp-brand { display:flex; align-items:center; gap:.65rem; margin-bottom:.15rem; }
  .cp-dot { width:11px; height:11px; border-radius:50%;
            background:#34d399; box-shadow:0 0 14px rgba(52,211,153,.75); }
  .cp-title { font-size:1.5rem; font-weight:700; letter-spacing:-0.02em; color:#f2f6fd; }
  .cp-sub { color:#8fa0bd; font-size:.92rem; margin-top:.15rem; }

  /* stat cards */
  .cp-card {
    background:#10151f;
    border:1px solid rgba(148,163,184,0.10);
    border-radius:12px; padding:16px 20px; height:100%;
  }
  .cp-card .label { font-size:.72rem; letter-spacing:.08em; text-transform:uppercase;
                    color:#8fa0bd; font-weight:600; }
  .cp-card .value { font-size:1.45rem; font-weight:650; margin-top:5px; color:#f2f6fd;
                    font-variant-numeric: tabular-nums; }
  .cp-card .sub   { font-size:.78rem; color:#8fa0bd; margin-top:3px; }

  /* section headings */
  .cp-h { font-size:.95rem; font-weight:600; color:#cdd8ea; margin:0 0 .35rem; }

  div[data-testid="stDataFrame"] { border:1px solid rgba(148,163,184,0.08);
                                   border-radius:10px; overflow:hidden; }
  button[kind="primary"] { background:#34d399 !important; border-color:#34d399 !important;
                           color:#06281c !important; font-weight:600 !important;
                           border-radius:9px !important; }
  button[kind="secondary"] { border-radius:9px !important; }
  hr.cp-rule { border:none; border-top:1px solid rgba(148,163,184,0.12); margin:1.1rem 0; }

  .cp-chip { display:inline-flex; align-items:center; gap:.4rem;
             background:#10151f; border:1px solid rgba(148,163,184,0.12);
             border-radius:999px; padding:.32rem .8rem; font-size:.82rem; color:#cdd8ea;
             margin-right:.45rem; }
  .cp-chip b { color:#f2f6fd; font-variant-numeric:tabular-nums; }
</style>
"""


def money(v: Decimal | float | None) -> str:
    x = float(v or 0)
    sign = "-" if x < 0 else ""
    x = abs(x)
    for div, suffix in ((1e12, "T"), (1e9, "B"), (1e6, "M"), (1e3, "K")):
        if x >= div:
            return f"{sign}${x / div:,.2f}{suffix}"
    return f"{sign}${x:,.2f}"


def style_fig(fig: go.Figure, height: int = 380, show_y_grid: bool = False) -> go.Figure:
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color=MUTED, family="Inter"),
        height=height,
        margin=dict(l=4, r=4, t=28, b=4),
        title=dict(font=dict(size=13, color="#cdd8ea")),
        coloraxis_showscale=False,
    )
    fig.update_xaxes(gridcolor=GRID, zerolinecolor=GRID, tickfont=dict(size=11))
    fig.update_yaxes(
        gridcolor=GRID if show_y_grid else "rgba(0,0,0,0)",
        zerolinecolor=GRID,
        tickfont=dict(size=11),
    )
    return fig


def section(title: str) -> None:
    st.markdown(f'<p class="cp-h">{title}</p>', unsafe_allow_html=True)


# --------------------------------------------------------------------------
# backend lifecycle
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


st.markdown(CSS, unsafe_allow_html=True)
storage = open_storage()

age = data_age_min(storage)
first_boot = age is None
if first_boot:
    with st.status("Starting backend — collecting first snapshot", expanded=True) as status:
        st.write("Opening DuckDB store and sweeping all venues…")
        results = run_sweep(storage)
        ok = sum(1 for m in results.values() if "FAILED" not in m)
        status.update(label=f"Backend ready · {ok}/{len(results)} sources", state="complete")
elif age > STALE_AFTER_MIN:
    with st.spinner("Refreshing market data…"):
        run_sweep(storage)

# --------------------------------------------------------------------------
# header
# --------------------------------------------------------------------------
st.markdown(
    """
    <div class="cp-brand"><span class="cp-dot"></span>
      <span class="cp-title">ChainPulse</span></div>
    <div class="cp-sub">Cross-venue funding · DeFi TVL &amp; yields · on-chain balances —
    all keyless public APIs.</div>
    """,
    unsafe_allow_html=True,
)

# --------------------------------------------------------------------------
# sidebar
# --------------------------------------------------------------------------
with st.sidebar:
    st.markdown(
        '<div class="cp-brand" style="margin-top:.2rem">'
        '<span class="cp-dot"></span><span class="cp-title" style="font-size:1.05rem">'
        "ChainPulse</span></div>",
        unsafe_allow_html=True,
    )
    age_now = data_age_min(storage)
    if age_now is None:
        st.caption("no data collected yet")
    else:
        freshness = "live" if age_now < STALE_AFTER_MIN else "stale"
        st.markdown(
            f'<span class="cp-chip">data age <b>{age_now:.0f}m</b></span>'
            f'<span class="cp-chip">{freshness}</span>',
            unsafe_allow_html=True,
        )

    if st.button("Refresh now", type="primary", use_container_width=True):
        with st.spinner("Sweeping venues…"):
            run_sweep(storage)
        st.rerun()

    st.divider()
    health = storage.sweep_health()
    if health:
        section("Source health")
        icon = {"ok": "&#9679;", "failed": "&#10007;"}
        color = {"ok": ACCENT, "failed": DANGER}
        for row in health:
            when = f"{row['ts']:%H:%M}" if row["ts"] else ""
            st.markdown(
                f'<span style="color:{color[row["status"]]}">{icon[row["status"]]}</span>'
                f'&nbsp;<code style="font-size:.8rem;color:{TEXT}">{row["source"]}</code>'
                f'&nbsp;<span style="color:{MUTED};font-size:.78rem">{when}</span>',
                unsafe_allow_html=True,
            )

    st.divider()
    st.caption(
        "Data: Binance · Hyperliquid · Coinbase\n\nDeFiLlama · Blockscout — free public APIs."
    )

# --------------------------------------------------------------------------
# headline stats
# --------------------------------------------------------------------------
latest_funding_rows = storage.latest_funding()
chains = storage.chain_tvl_latest()
total_tvl = sum((c[1] for c in chains), Decimal(0)) if chains else Decimal(0)
pools_n = len(storage.yields(min_tvl_usd=0, limit=500))
alerts_now = storage.recent_alerts(since_hours=24.0, limit=500)
tick_age = storage.latest_tick_age_s()
health = storage.sweep_health()
sources_ok = sum(1 for r in health if r["status"] == "ok")

c1, c2, c3, c4 = st.columns(4)
for col, label, value, sub in (
    (c1, "Tracked TVL", money(total_tvl), f"{len(chains):,} chains"),
    (c2, "Yield pools", f"{pools_n:,}", "by current snapshot"),
    (
        c3,
        "Perp markets",
        f"{len(latest_funding_rows):,}",
        f"{len({r[0] for r in latest_funding_rows})} venues",
    ),
    (
        c4,
        "Sources healthy",
        f"{sources_ok}/{max(len(health), 1)}",
        "last sweep" + ("" if not alerts_now else f" · {len(alerts_now)} alerts/24h"),
    ),
):
    col.markdown(
        f'<div class="cp-card"><div class="label">{label}</div>'
        f'<div class="value">{value}</div><div class="sub">{sub}</div></div>',
        unsafe_allow_html=True,
    )

live_chip = ""
if tick_age is not None and tick_age < 180:
    live_chip = (
        '<span class="cp-chip"><span class="cp-dot" '
        f'style="width:7px;height:7px"></span> live feed · {tick_age:.0f}s ago</span>'
    )
st.markdown(f"<div style='margin:.85rem 0 .2rem'>{live_chip}</div>", unsafe_allow_html=True)

tab_funding, tab_tvl, tab_yields, tab_alerts, tab_wallet, tab_vault = st.tabs(
    ["Funding", "Chains", "Yields", "Alerts", "Wallet", "Vault"]
)

# ==========================================================================
# funding radar
# ==========================================================================
with tab_funding:
    spread_rows = storage.funding_spread(top_n=50)
    if not spread_rows:
        st.info(
            "**No perpetual snapshots yet.** Binance/Hyperliquid are unreachable from this "
            "host right now (geo-restrictions are common on shared clouds). "
            "TVL, yields, wallet lookup and the vault work regardless — hit **Refresh now** "
            "later to retry funding collection."
        )
    else:
        cols = ["base", "short_venue", "short_apr", "long_venue", "long_apr", "spread_pct"]
        df = pd.DataFrame(spread_rows, columns=cols).sort_values("spread_pct", ascending=False)
        top = df.head(15).iloc[::-1]

        fig = go.Figure(
            go.Bar(
                x=top["spread_pct"].astype(float),
                y=top["base"],
                orientation="h",
                marker_color=ACCENT,
                custom_data=top[["long_venue", "long_apr", "short_venue", "short_apr"]].values,
                hovertemplate=(
                    "<b>%{y}</b><br>spread %{x:.2f}% APR<br>"
                    "<span style='color:#8fa0bd'>long</span> %{customdata[0]} "
                    "(%{customdata[1]:.2f}%)<br>"
                    "<span style='color:#8fa0bd'>short</span> %{customdata[2]} "
                    "(%{customdata[3]:.2f}%)<extra></extra>"
                ),
            )
        )
        fig.update_layout(title="Cash-and-carry candidates — annualized funding spread")
        left, right = st.columns((3, 2), gap="large")
        with left:
            section("Top divergence across venues")
            st.plotly_chart(style_fig(fig, height=420), use_container_width=True)
        with right:
            section("Latest snapshot per venue")
            ldf = pd.DataFrame(
                latest_funding_rows,
                columns=[
                    "venue",
                    "symbol",
                    "base",
                    "rate",
                    "interval_hours",
                    "mark_price",
                    "next_funding_ts_ms",
                    "collected_at",
                ],
            )[["venue", "symbol", "rate", "interval_hours", "mark_price"]]
            st.dataframe(
                ldf,
                hide_index=True,
                column_config={
                    "venue": st.column_config.TextColumn("venue"),
                    "symbol": st.column_config.TextColumn("market"),
                    "rate": st.column_config.NumberColumn("rate/int", format="%.5f"),
                    "interval_hours": st.column_config.NumberColumn("every (h)"),
                    "mark_price": st.column_config.NumberColumn("mark", format="$ %.2f"),
                },
                use_container_width=True,
                height=420,
            )
        st.download_button(
            "Download spread CSV",
            df.to_csv(index=False).encode(),
            "funding_spread.csv",
            mime="text/csv",
        )

# ==========================================================================
# chains
# ==========================================================================
with tab_tvl:
    if not chains:
        st.info("No TVL data yet — hit **Refresh now**.")
    else:
        ctl_l, ctl_r = st.columns([1, 3])
        top_n = ctl_l.slider("Chains shown", 5, min(len(chains), 40), 20)
        tdf = pd.DataFrame(chains[:top_n], columns=["chain", "tvl_usd", "prev_tvl_usd"])
        tdf["tvl"] = tdf["tvl_usd"].map(float)
        tdf["delta_pct"] = tdf.apply(
            lambda r: (
                (float(r.tvl_usd) - float(r.prev_tvl_usd)) / float(r.prev_tvl_usd) * 100
                if r.prev_tvl_usd
                else None
            ),
            axis=1,
        ).round(2)

        fig = px.bar(
            tdf.iloc[::-1],
            x="tvl",
            y="chain",
            orientation="h",
            color_discrete_sequence=[ACCENT],
            labels={"tvl": "", "chain": ""},
        )
        fig.update_traces(
            hovertemplate="<b>%{y}</b><br>" + "%{x:$,.0f}<extra></extra>",
            marker_line_width=0,
        )
        left, right = st.columns((3, 2), gap="large")
        with left:
            section("Total value locked")
            st.plotly_chart(style_fig(fig, height=max(360, 22 * top_n)), use_container_width=True)
        with right:
            section("vs previous snapshot")
            st.dataframe(
                tdf[["chain", "tvl_usd", "delta_pct"]],
                hide_index=True,
                column_config={
                    "chain": st.column_config.TextColumn("chain"),
                    "tvl_usd": st.column_config.NumberColumn("TVL", format="$ %.0f"),
                    "delta_pct": st.column_config.NumberColumn("Δ %", format="%+.2f"),
                },
                use_container_width=True,
                height=420,
            )

# ==========================================================================
# yields
# ==========================================================================
with tab_yields:
    rows = storage.yields(min_tvl_usd=0, limit=300)
    ydf_all = pd.DataFrame(
        rows,
        columns=["pool_id", "chain", "project", "symbol", "tvl_usd", "apy_pct", "stablecoin"],
    )
    if ydf_all.empty:
        st.info("No yield data yet — hit **Refresh now**.")
    else:
        fcol1, fcol2, fcol3, fcol4 = st.columns([2, 1.2, 1, 1])
        chains_avail = sorted(ydf_all["chain"].unique())
        sel_chain = fcol1.selectbox("Chain", ["all", *chains_avail])
        stable_only = fcol2.checkbox("Stablecoins only", value=False)
        min_tvl = fcol3.number_input(
            "Min TVL ($M)", min_value=0.0, value=1.0, step=0.5, format="%.1f"
        )
        top_k = int(fcol4.number_input("Rows", 10, 300, 50, step=10))

        view = ydf_all.copy()
        if sel_chain != "all":
            view = view[view["chain"] == sel_chain]
        if stable_only:
            view = view[view["stablecoin"]]
        view = view[view["tvl_usd"].map(float) >= min_tvl * 1e6]
        view = view.sort_values("apy_pct", ascending=False, na_position="last").head(top_k)
        apy_max = float(view["apy_pct"].dropna().max()) if len(view) else 100
        st.dataframe(
            view[["project", "chain", "symbol", "tvl_usd", "apy_pct", "stablecoin"]],
            hide_index=True,
            column_config={
                "project": st.column_config.TextColumn("protocol"),
                "symbol": st.column_config.TextColumn("pool"),
                "tvl_usd": st.column_config.NumberColumn("TVL", format="$ %.0f"),
                "apy_pct": st.column_config.ProgressColumn(
                    "APY",
                    min_value=0,
                    max_value=max(25.0, min(apy_max, 250)),
                    format="%.2f%%",
                ),
                "stablecoin": st.column_config.CheckboxColumn("stable"),
            },
            use_container_width=True,
            height=520,
        )
        st.download_button(
            "Download CSV",
            view.to_csv(index=False).encode(),
            "yield_pools.csv",
            mime="text/csv",
        )

# ==========================================================================
# alerts
# ==========================================================================
with tab_alerts:
    if not alerts_now:
        st.success("No threshold breaches in the last 24h.")
    else:
        adf = pd.DataFrame(alerts_now)
        kind_counts = adf["kind"].value_counts()
        sev_counts = adf["severity"].value_counts()
        chips = "".join(
            f'<span class="cp-chip">{k.replace("_", " ")} <b>{v}</b></span>'
            for k, v in kind_counts.items()
        )
        crit = sev_counts.get("critical", 0)
        if crit:
            chips += (
                f'<span class="cp-chip" style="border-color:{DANGER};color:{DANGER}">'
                f"critical <b>{crit}</b></span>"
            )
        st.markdown(f"<div style='margin-bottom:.6rem'>{chips}</div>", unsafe_allow_html=True)
        st.dataframe(
            adf,
            hide_index=True,
            column_config={
                "ts": st.column_config.TimeColumn("time"),
                "kind": st.column_config.TextColumn("kind"),
                "severity": st.column_config.TextColumn("severity"),
                "subject": st.column_config.TextColumn("subject"),
                "detail": st.column_config.TextColumn("detail", width="large"),
            },
            use_container_width=True,
            height=430,
        )

# ==========================================================================
# wallet explorer
# ==========================================================================
with tab_wallet:
    wcol1, wcol2 = st.columns([4, 1])
    address = wcol1.text_input(
        "Wallet address",
        placeholder="Paste any 0x… address",
        value="0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045",
        label_visibility="collapsed",
    )
    fetch_btn = wcol2.button("Look up", type="primary", use_container_width=True)

    addr = address.strip().lower()
    if fetch_btn:
        if not (addr.startswith("0x") and len(addr) == 42):
            st.error("Enter a valid 20-byte EVM address (0x + 40 hex chars).")
        else:
            t0 = time.time()
            try:
                ov = asyncio.run(BlockscoutConnector().wallet_overview(addr))
                storage.save_wallet_balance(ov["balance"])
                bal = ov["balance"]
                tokens_val = sum((t.usd_value or Decimal(0)) for t in ov["tokens"])
                k1, k2, k3, k4 = st.columns(4)
                k1.metric(f"Native ({bal.native_symbol})", f"{bal.balance_native:,.4f}")
                k2.metric(
                    "USD value",
                    money(bal.balance_usd) if bal.balance_usd is not None else "—",
                    f"@ ${bal.usd_price:,.2f}" if bal.usd_price is not None else None,
                    delta_color="off",
                )
                k3.metric(
                    "Tokens", money(tokens_val), f"{len(ov['tokens'])} contracts", delta_color="off"
                )
                k4.metric("ENS", ov["ens_name"] or "—", delta_color="off")

                lw, rw = st.columns(2, gap="large")
                with lw:
                    section("Top ERC-20 holdings")
                    if ov["tokens"]:
                        tdf_w = pd.DataFrame(
                            [
                                {
                                    "symbol": t.symbol,
                                    "balance": float(t.balance_native),
                                    "usd": float(t.usd_value) if t.usd_value else None,
                                }
                                for t in ov["tokens"][:12]
                            ]
                        )
                        st.dataframe(
                            tdf_w,
                            hide_index=True,
                            column_config={
                                "balance": st.column_config.NumberColumn(format="%.4f"),
                                "usd": st.column_config.NumberColumn("value", format="$ %.2f"),
                            },
                            use_container_width=True,
                        )
                    else:
                        st.caption("no ERC-20 transfers found")
                with rw:
                    section("Recent transactions")
                    if ov["recent_txs"]:
                        xdf = pd.DataFrame(
                            [
                                {
                                    "when": pd.to_datetime(t.timestamp_ms, unit="ms", utc=True),
                                    "dir": ("in" if t.direction == "in" else "out"),
                                    "method": t.method,
                                    "ETH": float(t.value_native),
                                }
                                for t in ov["recent_txs"]
                            ]
                        )
                        st.dataframe(
                            xdf,
                            hide_index=True,
                            column_config={
                                "when": st.column_config.DatetimeColumn("when"),
                                "dir": st.column_config.TextColumn("dir"),
                                "method": st.column_config.TextColumn("method"),
                                "ETH": st.column_config.NumberColumn(format="%.4f"),
                            },
                            use_container_width=True,
                        )
                    else:
                        st.caption("no recent transactions")
                st.caption(f"fetched live from Blockscout in {time.time() - t0:.1f}s")
            except ValueError as exc:
                st.error(str(exc))
            except Exception as exc:  # noqa: BLE001
                st.error(f"Upstream unavailable: {exc}")

    hist = storage.con.execute(
        "SELECT address, native_symbol, balance_native, balance_usd, collected_at "
        "FROM wallet_balances ORDER BY collected_at DESC LIMIT 8"
    ).fetchall()
    if hist:
        with st.expander("Recent lookups"):
            st.dataframe(
                pd.DataFrame(hist, columns=["address", "asset", "balance", "usd", "at"]),
                hide_index=True,
                column_config={
                    "balance": st.column_config.NumberColumn(format="%.6f"),
                    "usd": st.column_config.NumberColumn(format="$ %.2f"),
                },
                use_container_width=True,
            )

# ==========================================================================
# enclave vault
# ==========================================================================
with tab_vault:
    vault = KmsLite(VAULT_ROOT)
    st.caption(
        "Seal API keys or wallet private keys behind an attestation policy. Blobs are "
        "envelope-encrypted at rest; the key-encryption key is released only against a fresh, "
        "non-replayed attestation whose code measurement (PCR0) matches policy. Signing happens "
        "inside the boundary — plaintext never returns. *Local simulation of the attested-enclave "
        "trust model; the hardware root of trust is modeled by a passphrase.*"
    )
    st.divider()

    sealed_names = sorted(p.stem for p in vault.secrets_dir.glob("*.sealed"))
    srow1, srow2 = st.columns(2, gap="large")

    with srow1:
        section("Seal a new secret")
        new_name = st.text_input("Name", placeholder="my-wallet-key", key="vault_name")
        new_secret = st.text_input("Secret value", type="password", key="vault_secret")
        seal_pass = st.text_input("Root passphrase", type="password", key="vault_pass_seal")
        do_seal = st.button(
            "Seal",
            type="primary",
            disabled=not (new_name.strip() and new_secret and seal_pass),
        )
        if do_seal:
            try:
                vault.seal(new_name.strip(), new_secret, seal_pass)
                st.success(f"Sealed `{new_name.strip()}` → data/vault/secrets/")
                time.sleep(0.6)
                st.rerun()
            except FileExistsError:
                st.warning(f"`{new_name.strip()}` already exists.")

    with srow2:
        section("Attest & sign — sign-without-reveal")
        if not sealed_names:
            st.caption("Nothing sealed yet.")
        else:
            use_name = st.selectbox("Secret", sealed_names, key="vault_use")
            message = st.text_input("Message", value="withdraw 1.0 ETH to 0xabc", key="vault_msg")
            use_pass = st.text_input("Root passphrase", type="password", key="vault_pass_use")
            if st.button("Attest & sign", type="primary", disabled=not use_pass):
                try:
                    doc = attest(nonce=f"ui-{secrets.token_hex(8)}")
                    sig_hex, pub_hex = vault.sign_message(use_name, message, use_pass, doc)
                    verified = KmsLite.verify_signature(message, sig_hex, pub_hex)
                    m1, m2, m3 = st.columns(3)
                    m1.metric("PCR0", doc.pcr0[:10] + "…")
                    m2.metric("Nonce", doc.nonce[:10] + "…")
                    m3.metric("Signature", "verified" if verified else "invalid")
                    with st.expander("Attestation document"):
                        st.code(doc.to_json(), language="json")
                    st.code(sig_hex[:96] + "…", language="text")
                except AttestationError as exc:
                    st.error(f"Attestation failed — {exc}")

    st.caption(
        "PCR0 = SHA-256 over this module's sources · nonce single-use per KMS · freshness window "
        "300s · blobs AAD-bound to their names, so entries cannot be swapped."
    )

st.divider()
st.caption(
    "ChainPulse · MIT · built on keyless public APIs · market data is informational, not advice"
)
