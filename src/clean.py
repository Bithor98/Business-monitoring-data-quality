from __future__ import annotations

from pathlib import Path

import pandas as pd


def clean_orders(
    in_path: Path = Path("data/raw/orders.csv"),
    out_path: Path = Path("data/processed/orders_clean.csv"),
) -> pd.DataFrame:
    if not in_path.exists():
        raise FileNotFoundError("No se encuentra orders.csv en data/raw")

    orders = pd.read_csv(in_path)
    original_rows = len(orders)

    # 1️⃣ Eliminar duplicados por order_id
    orders = orders.drop_duplicates(subset=["order_id"])
    after_dupes = len(orders)

    # 2️⃣ Eliminar pedidos sin customer_id
    orders = orders.dropna(subset=["customer_id"])
    after_nulls = len(orders)

    # 3️⃣ Corregir revenue negativo → 0
    neg_mask = orders["revenue"] < 0
    neg_count = int(neg_mask.sum())
    orders.loc[neg_mask, "revenue"] = 0

    # 4️⃣ Eliminar fechas futuras
    orders["date"] = pd.to_datetime(orders["date"], errors="coerce")
    orders = orders[orders["date"] <= pd.Timestamp("2026-12-31")]
    after_dates = len(orders)

    # Crear carpeta si no existe
    out_path.parent.mkdir(parents=True, exist_ok=True)
    orders.to_csv(out_path, index=False)

    # Logging simple en consola
    print("🧹 CLEANING REPORT")
    print(f"- Filas iniciales: {original_rows}")
    print(f"- Tras duplicados: {after_dupes}")
    print(f"- Tras customer_id nulo: {after_nulls}")
    print(f"- Revenue negativo corregido: {neg_count}")
    print(f"- Tras fechas inválidas: {after_dates}")
    print(f"✅ Orders limpias guardadas en {out_path}")

    return orders


if __name__ == "__main__":
    clean_orders()