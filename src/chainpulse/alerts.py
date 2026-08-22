"""Threshold alerts evaluated after every sweep.

Rules are deliberately simple and auditable: funding APR spikes, chain TVL
drops and implausibly high yields. Every alert is keyed by (kind, subject,
hour-bucket) so re-running sweeps never duplicates notifications.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from chainpulse.models import Alert
from chainpulse.storage import DuckDBStorage

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class AlertConfig:
    funding_apr_abs_pct: float = 30.0  # |annualized funding| beyond this is unusual
    tvl_drop_pct: float = 5.0  # chain TVL fell this much between sweeps
    apy_flag_pct: float = 100.0  # yields this high are usually a risk flag
    min_tvl_usd_for_apy: float = 1e6  # ignore dust pools when flagging APY


def evaluate_alerts(storage: DuckDBStorage, cfg: AlertConfig | None = None) -> list[Alert]:
    cfg = cfg or AlertConfig()
    alerts: list[Alert] = []

    for row in storage.funding_spread(top_n=1000):
        base, _, short_apr, _, long_apr, _ = row
        for apr in (float(short_apr), float(long_apr)):
            if abs(apr) >= cfg.funding_apr_abs_pct:
                sign = "rich" if apr > 0 else "deeply negative"
                sev = "warning" if abs(apr) < 2 * cfg.funding_apr_abs_pct else "critical"
                detail = f"{apr:.1f}% annualized funding ({sign}) — check venue legs before trading"
                alerts.append(Alert(kind="funding_apr", severity=sev, subject=base, detail=detail))
                break

    for chain, tvl, prev in storage.chain_tvl_latest():
        if prev:
            drop = (float(tvl) - float(prev)) / float(prev) * 100
            if drop <= -cfg.tvl_drop_pct:
                alerts.append(
                    Alert(
                        kind="tvl_drop",
                        severity="warning",
                        subject=chain,
                        detail=f"TVL fell {abs(drop):.2f}% to ${float(tvl):.0f} vs prev snapshot",
                    )
                )

    for _pool_id, chain, project, symbol, tvl_usd, apy_pct, _stable in storage.yields(
        min_tvl_usd=cfg.min_tvl_usd_for_apy, limit=500
    ):
        if apy_pct is not None and float(apy_pct) >= cfg.apy_flag_pct:
            detail = (
                f"{float(apy_pct):,.0f}% APY on ${float(tvl_usd) / 1e6:,.1f}M TVL"
                f" ({chain}) — verify emissions"
            )
            alerts.append(
                Alert(
                    kind="yield_outlier",
                    severity="warning",
                    subject=f"{project}/{symbol}",
                    detail=detail,
                )
            )

    saved = storage.save_alerts(alerts)
    log.info("alerts: evaluated=%d new=%d", len(alerts), saved)
    return alerts
