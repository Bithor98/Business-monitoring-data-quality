from pathlib import Path
import pandas as pd


def build_executive_summary(
    alerts_path: Path = Path("reports/business_alerts.csv"),
    out_path: Path = Path("reports/executive_summary.md"),
    top_n: int = 10,
):
    if not alerts_path.exists():
        raise FileNotFoundError("No existe business_alerts.csv")

    df = pd.read_csv(alerts_path)

    if df.empty:
        out_path.write_text("# Executive Summary\n\nNo alerts detected.", encoding="utf-8")
        return

    # priorizamos por severidad y magnitud
    severity_order = {"high": 0, "medium": 1, "low": 2}
    df["severity_rank"] = df["severity"].map(severity_order).fillna(3)

    top = (
        df.sort_values(["severity_rank", "value"])
        .head(top_n)
        .drop(columns=["severity_rank"])
    )

    lines = []
    lines.append("# Executive Summary — Business Monitoring")
    lines.append("")
    lines.append(f"Top {top_n} alertas más relevantes detectadas:")
    lines.append("")
    lines.append(top.to_markdown(index=False))

    out_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"📊 Executive summary generado en {out_path}")


if __name__ == "__main__":
    build_executive_summary()