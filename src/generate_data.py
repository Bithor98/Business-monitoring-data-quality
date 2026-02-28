from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class DataGenConfig:
    out_dir: Path = Path("data/raw")
    seed: int = 42
    n_orders: int = 5000
    n_customers: int = 800
    n_products: int = 120
    start_date: str = "2025-01-01"
    days: int = 180


def _random_dates(start: datetime, days: int, n: int) -> pd.Series:
    return pd.to_datetime([start + timedelta(days=random.randint(0, days - 1)) for _ in range(n)])


def generate_raw_data(cfg: DataGenConfig = DataGenConfig()) -> None:
    random.seed(cfg.seed)
    np.random.seed(cfg.seed)
    cfg.out_dir.mkdir(parents=True, exist_ok=True)

    start_dt = datetime.fromisoformat(cfg.start_date)

    # Customers
    customers = pd.DataFrame({
        "customer_id": [f"C{str(i).zfill(5)}" for i in range(1, cfg.n_customers + 1)],
        "signup_date": _random_dates(start_dt - timedelta(days=365), 365, cfg.n_customers),
        "country": np.random.choice(["ES", "FR", "DE", "IT", "PT"], size=cfg.n_customers, p=[0.55, 0.12, 0.12, 0.11, 0.10]),
        "segment": np.random.choice(["consumer", "pro", "vip"], size=cfg.n_customers, p=[0.80, 0.17, 0.03]),
    })

    # Products
    products = pd.DataFrame({
        "product_id": [f"P{str(i).zfill(4)}" for i in range(1, cfg.n_products + 1)],
        "category": np.random.choice(["apparel", "electronics", "home", "beauty", "sports"], size=cfg.n_products),
        "cost": np.round(np.random.uniform(2.0, 60.0, size=cfg.n_products), 2),
    })

    # Orders
    order_dates = _random_dates(start_dt, cfg.days, cfg.n_orders)
    qty = np.random.poisson(lam=2.0, size=cfg.n_orders).clip(1, 10)
    price = np.round(np.random.uniform(5.0, 150.0, size=cfg.n_orders), 2)

    orders = pd.DataFrame({
        "order_id": [f"O{str(i).zfill(7)}" for i in range(1, cfg.n_orders + 1)],
        "date": order_dates,
        "customer_id": np.random.choice(customers["customer_id"], size=cfg.n_orders),
        "product_id": np.random.choice(products["product_id"], size=cfg.n_orders),
        "qty": qty,
        "price": price,
        "status": np.random.choice(["paid", "cancelled", "refunded"], size=cfg.n_orders, p=[0.90, 0.07, 0.03]),
        "channel": np.random.choice(["web", "marketplace", "retail"], size=cfg.n_orders, p=[0.65, 0.25, 0.10]),
    })

    orders["revenue"] = np.round(orders["qty"] * orders["price"], 2)

    # ---------------------------
    # Inject realistic "bad data"
    # ---------------------------

    # 1) Duplicate some order_ids
    dup_idx = np.random.choice(orders.index, size=15, replace=False)
    orders.loc[dup_idx, "order_id"] = orders.loc[dup_idx[0], "order_id"]

    # 2) Nulls in critical fields
    null_idx = np.random.choice(orders.index, size=25, replace=False)
    orders.loc[null_idx, "customer_id"] = None

    # 3) Negative revenue (data error)
    neg_idx = np.random.choice(orders.index, size=10, replace=False)
    orders.loc[neg_idx, "revenue"] = -abs(orders.loc[neg_idx, "revenue"])

    # 4) Future dates (bad)
    future_idx = np.random.choice(orders.index, size=8, replace=False)
    orders.loc[future_idx, "date"] = pd.to_datetime("2027-01-01")

    # 5) Crazy spike day (business anomaly)
    spike_day = start_dt + timedelta(days=int(cfg.days * 0.6))
    spike_mask = orders["date"].dt.date == spike_day.date()
    if spike_mask.sum() > 0:
        orders.loc[spike_mask, "revenue"] *= 8  # spike

    customers.to_csv(cfg.out_dir / "customers.csv", index=False)
    products.to_csv(cfg.out_dir / "products.csv", index=False)
    orders.to_csv(cfg.out_dir / "orders.csv", index=False)

    print(f"✅ Raw data generated in: {cfg.out_dir.resolve()}")
    print("Files: customers.csv, products.csv, orders.csv")


if __name__ == "__main__":
    generate_raw_data()