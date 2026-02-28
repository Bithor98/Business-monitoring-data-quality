from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class Alert:
    date: str
    alert_type: str
    severity: str
    metric: str
    value: float
    baseline: float
    details: str


def _daily_revenue(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    d["date"] = pd.to_datetime(d["date"], errors="coerce")
    daily = (
        d.groupby(d["date"].dt.date, as_index=False)["revenue"]
        .sum()
        .rename(columns={"date": "day"})
    )
    daily["day"] = pd.to_datetime(daily["day"])
    daily = daily.sort_values("day").reset_index(drop=True)
    return daily


def detect_revenue_drop(daily: pd.DataFrame, pct_threshold: float = 35.0) -> List[Alert]:
    alerts: List[Alert] = []
    daily = daily.copy()
    daily["prev_revenue"] = daily["revenue"].shift(1)
    daily["pct_change"] = (daily["revenue"] - daily["prev_revenue"]) / daily["prev_revenue"] * 100

    bad = daily[(daily["prev_revenue"] > 0) & (daily["pct_change"] <= -pct_threshold)]
    for _, r in bad.iterrows():
        alerts.append(
            Alert(
                date=r["day"].date().isoformat(),
                alert_type="revenue_drop",
                severity="high",
                metric="revenue_pct_change",
                value=float(round(r["pct_change"], 2)),
                baseline=float(round(r["prev_revenue"], 2)),
                details=f"Revenue cayó {abs(r['pct_change']):.2f}% vs día anterior.",
            )
        )
    return alerts


def detect_revenue_spike_zscore(daily: pd.DataFrame, z_threshold: float = 3.0) -> List[Alert]:
    alerts: List[Alert] = []
    x = daily["revenue"].astype(float)
    mu = float(x.mean())
    sigma = float(x.std(ddof=0)) if float(x.std(ddof=0)) != 0 else 1.0
    z = (x - mu) / sigma

    for i, zi in enumerate(z):
        if zi >= z_threshold:
            r = daily.iloc[i]
            alerts.append(
                Alert(
                    date=r["day"].date().isoformat(),
                    alert_type="revenue_spike",
                    severity="medium",
                    metric="revenue_zscore",
                    value=float(round(zi, 2)),
                    baseline=float(round(mu, 2)),
                    details=f"Pico de revenue (z={zi:.2f}). Revisar campañas, fraude o errores.",
                )
            )
    return alerts


def detect_cancel_rate(df: pd.DataFrame, rate_threshold: float = 8.0) -> List[Alert]:
    # Si la columna status existe y tiene cancelled/refunded, calculamos tasa diaria
    if "status" not in df.columns:
        return []

    d = df.copy()
    d["date"] = pd.to_datetime(d["date"], errors="coerce")
    d["is_cancel"] = d["status"].isin(["cancelled", "refunded"]).astype(int)

    daily = d.groupby(d["date"].dt.date, as_index=False).agg(
        orders=("order_id", "count"),
        cancels=("is_cancel", "sum"),
    )
    daily["cancel_rate_pct"] = (daily["cancels"] / daily["orders"]) * 100

    alerts: List[Alert] = []
    bad = daily[daily["cancel_rate_pct"] >= rate_threshold]
    for _, r in bad.iterrows():
        alerts.append(
            Alert(
                date=pd.to_datetime(r["date"]).date().isoformat() if "date" in r else str(r.iloc[0]),
                alert_type="high_cancel_rate",
                severity="medium",
                metric="cancel_rate_pct",
                value=float(round(r["cancel_rate_pct"], 2)),
                baseline=float(rate_threshold),
                details=f"Tasa cancel/refund {r['cancel_rate_pct']:.2f}% (>= {rate_threshold}%).",
            )
        )
    return alerts


def save_alerts(alerts: List[Alert], out_dir: Path = Path("reports")) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame([a.__dict__ for a in alerts]).sort_values(["severity", "date"], ascending=[True, True])

    csv_path = out_dir / "business_alerts.csv"
    md_path = out_dir / "business_alerts.md"

    df.to_csv(csv_path, index=False)

    lines = []
    lines.append("# Business Alerts")
    lines.append("")
    if df.empty:
        lines.append("✅ No alerts detected.")
    else:
        lines.append(df.to_markdown(index=False))
    md_path.write_text("\n".join(lines), encoding="utf-8")

    return md_path


def main() -> None:
    in_path = Path("data/processed/orders_clean.csv")
    if not in_path.exists():
        raise FileNotFoundError("No encuentro data/processed/orders_clean.csv. Ejecuta src/clean.py primero.")

    df = pd.read_csv(in_path)

    daily = _daily_revenue(df)

    alerts: List[Alert] = []
    alerts += detect_revenue_drop(daily, pct_threshold=35.0)
    alerts += detect_revenue_spike_zscore(daily, z_threshold=3.0)
    alerts += detect_cancel_rate(df, rate_threshold=8.0)

    md_path = save_alerts(alerts)

    print("🚨 Alerts report generado:")
    print(f"- Total alerts: {len(alerts)}")
    print(f"- Reports: {md_path.parent.resolve()}")


if __name__ == "__main__":
    main()