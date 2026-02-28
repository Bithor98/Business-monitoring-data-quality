from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd


@dataclass(frozen=True)
class ValidationResult:
    table: str
    checks: pd.DataFrame          # detalle por check
    nulls: pd.DataFrame           # null rate por columna
    score: int                    # 0-100
    failed_rows: Dict[str, pd.DataFrame]  # ejemplos de filas que fallan


def null_report(df: pd.DataFrame) -> pd.DataFrame:
    rep = (
        df.isna()
        .mean()
        .mul(100)
        .round(2)
        .rename("null_pct")
        .to_frame()
        .sort_values("null_pct", ascending=False)
    )
    return rep


def _check_duplicates(df: pd.DataFrame, key: str) -> Tuple[bool, int]:
    if key not in df.columns:
        return False, 0
    dup_count = int(df.duplicated(subset=[key]).sum())
    return dup_count == 0, dup_count


def _check_non_negative(df: pd.DataFrame, col: str) -> Tuple[bool, int]:
    if col not in df.columns:
        return False, 0
    bad = int((df[col] < 0).sum())
    return bad == 0, bad


def _check_date_range(df: pd.DataFrame, col: str, min_date: str, max_date: str) -> Tuple[bool, int]:
    if col not in df.columns:
        return False, 0

    d = pd.to_datetime(df[col], errors="coerce")
    bad = int((d < pd.to_datetime(min_date)).sum() + (d > pd.to_datetime(max_date)).sum())
    return bad == 0, bad


def _check_null_threshold(df: pd.DataFrame, col: str, max_null_pct: float) -> Tuple[bool, float]:
    if col not in df.columns:
        return False, 100.0
    pct = float(df[col].isna().mean() * 100)
    return pct <= max_null_pct, round(pct, 2)


def compute_score(checks_df: pd.DataFrame) -> int:
    """
    Score simple 0-100:
    - cada check fallido resta puntos según severidad
    """
    score = 100
    for _, row in checks_df.iterrows():
        if row["passed"] is True:
            continue
        severity = row["severity"]
        if severity == "critical":
            score -= 25
        elif severity == "high":
            score -= 15
        elif severity == "medium":
            score -= 8
        else:
            score -= 3
    return max(0, min(100, score))


def validate_orders(orders: pd.DataFrame) -> ValidationResult:
    checks: List[dict] = []
    failed_rows: Dict[str, pd.DataFrame] = {}

    # Check: duplicates order_id
    passed, bad_count = _check_duplicates(orders, "order_id")
    checks.append({
        "check": "duplicate_order_id",
        "passed": passed,
        "bad_count": bad_count,
        "severity": "critical",
        "hint": "Order IDs must be unique."
    })
    if bad_count:
        failed_rows["duplicate_order_id"] = orders[orders.duplicated(subset=["order_id"], keep=False)].head(20)

    # Check: null threshold customer_id
    passed, pct = _check_null_threshold(orders, "customer_id", max_null_pct=0.1)  # <= 0.1% nulos
    checks.append({
        "check": "null_customer_id",
        "passed": passed,
        "bad_count": pct,
        "severity": "high",
        "hint": "Customer ID nulls break joins and customer analytics."
    })
    if not passed:
        failed_rows["null_customer_id"] = orders[orders["customer_id"].isna()].head(20)

    # Check: revenue non-negative
    passed, bad_count = _check_non_negative(orders, "revenue")
    checks.append({
        "check": "negative_revenue",
        "passed": passed,
        "bad_count": bad_count,
        "severity": "critical",
        "hint": "Revenue must never be negative (unless explicit returns model)."
    })
    if bad_count:
        failed_rows["negative_revenue"] = orders[orders["revenue"] < 0].head(20)

    # Check: date range sanity (ajusta si quieres)
    passed, bad_count = _check_date_range(orders, "date", min_date="2024-01-01", max_date="2026-12-31")
    checks.append({
        "check": "date_out_of_range",
        "passed": passed,
        "bad_count": bad_count,
        "severity": "medium",
        "hint": "Dates outside expected range usually indicate parsing or source errors."
    })
    if bad_count:
        d = pd.to_datetime(orders["date"], errors="coerce")
        failed_rows["date_out_of_range"] = orders[(d < "2024-01-01") | (d > "2026-12-31")].head(20)

    checks_df = pd.DataFrame(checks)
    nrep = null_report(orders)
    score = compute_score(checks_df)

    return ValidationResult(
        table="orders",
        checks=checks_df,
        nulls=nrep,
        score=score,
        failed_rows=failed_rows,
    )


def save_validation_report(result: ValidationResult, out_dir: Path = Path("reports")) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)

    checks_path = out_dir / f"dq_checks_{result.table}.csv"
    nulls_path = out_dir / f"dq_nulls_{result.table}.csv"
    summary_path = out_dir / f"dq_summary_{result.table}.md"

    result.checks.to_csv(checks_path, index=False)
    result.nulls.to_csv(nulls_path)

    lines = []
    lines.append(f"# Data Quality Summary — `{result.table}`")
    lines.append("")
    lines.append(f"**Quality Score:** {result.score}/100")
    lines.append("")
    lines.append("## Checks")
    lines.append(result.checks.to_markdown(index=False))
    lines.append("")
    lines.append("## Nulls (top)")
    lines.append(result.nulls.head(15).to_markdown())
    lines.append("")

    summary_path.write_text("\n".join(lines), encoding="utf-8")
    return summary_path


def main() -> None:
    import sys

    path_arg = sys.argv[1] if len(sys.argv) > 1 else "data/raw/orders.csv"
    orders_path = Path(path_arg)
    if not orders_path.exists():
        raise FileNotFoundError("No encuentro data/raw/orders.csv. Ejecuta primero src/generate_data.py")

    orders = pd.read_csv(orders_path)
    result = validate_orders(orders)
    md_path = save_validation_report(result)

    print("✅ Data quality report generado:")
    print(f"- Score: {result.score}/100")
    print(f"- Reports: {md_path.parent.resolve()}")


if __name__ == "__main__":
    main()