"""
customer_etl.py — Customer and Orders Ingestion
DHL Data Engineer Portfolio — Project 03

Loads customers.csv → dim_customer
Loads outbound_orders.csv → fact_orders (incremental)

Key behaviours:
  - Incremental load: only inserts order_ids not already in fact_orders
  - Deduplication: logs and removes duplicate order_ids from source before load
  - Referential integrity check: every order must have a valid customer_id
  - Lifetime metrics: first/last order date, total orders, total revenue computed
    and upserted into dim_customer after each orders load
"""

import logging
import time
from datetime import datetime
from pathlib import Path
import duckdb
import pandas as pd
import numpy as np

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BASE_DIR  = Path(__file__).resolve().parent.parent
DB_PATH   = BASE_DIR.parent / "01-sku-segmentation-pipeline" / "outputs" / "dhl_warehouse.duckdb"
DATA_DIR  = BASE_DIR.parent.parent / "shared" / "data" / "dhl-synthetic"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def get_logger(name: str = "customer_etl") -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        h = logging.StreamHandler()
        h.setFormatter(logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        ))
        logger.addHandler(h)
        logger.setLevel(logging.INFO)
    return logger

# ---------------------------------------------------------------------------
# Step 1: Load dim_customer
# ---------------------------------------------------------------------------

def load_customers(conn: duckdb.DuckDBPyConnection, data_dir: Path,
                   logger: logging.Logger) -> dict:
    """
    Load customers.csv into dim_customer.
    Uses INSERT OR REPLACE to upsert on customer_id primary key.
    """
    path = data_dir / "customers.csv"
    logger.info(f"  Reading {path.name}...")
    df = pd.read_csv(path)

    df = df.rename(columns={
        "Customer_ID":    "customer_id",
        "Customer_Type":  "customer_type",
        "Region":         "region",
        "SLA_Hours":      "sla_hours",
        "Annual_Rev_Band":"annual_rev_band",
        "Active_Flag":    "active_flag",
        "Contract_Since": "contract_since",
    })

    df["active_flag"]    = df["active_flag"].astype(bool)
    df["contract_since"] = pd.to_datetime(df["contract_since"], errors="coerce")
    df["etl_loaded_at"]  = datetime.utcnow()

    # Add columns that will be populated after orders load
    df["current_rfm_segment"] = None
    df["first_order_date"]    = None
    df["last_order_date"]     = None
    df["lifetime_orders"]     = 0
    df["lifetime_revenue"]    = 0.0

    # Get existing customers to decide insert vs update
    existing = {r[0] for r in conn.execute("SELECT customer_id FROM dim_customer").fetchall()}
    new_customers = df[~df["customer_id"].isin(existing)]
    existing_customers = df[df["customer_id"].isin(existing)]

    # Insert new customers
    if len(new_customers) > 0:
        cols = ["customer_id", "customer_type", "region", "sla_hours", "annual_rev_band",
                "active_flag", "contract_since", "current_rfm_segment",
                "first_order_date", "last_order_date", "lifetime_orders",
                "lifetime_revenue", "etl_loaded_at"]
        conn.register("_cust_new", new_customers[cols])
        col_list = ", ".join(f'"{c}"' for c in cols)
        conn.execute(f"INSERT INTO dim_customer ({col_list}) SELECT {col_list} FROM _cust_new")
        conn.unregister("_cust_new")

    # Update existing customers' non-metric fields (type, region, sla, etc.)
    if len(existing_customers) > 0:
        for _, row in existing_customers.iterrows():
            conn.execute("""
                UPDATE dim_customer SET
                    customer_type   = ?,
                    region          = ?,
                    sla_hours       = ?,
                    annual_rev_band = ?,
                    active_flag     = ?,
                    contract_since  = ?,
                    etl_loaded_at   = ?
                WHERE customer_id = ?
            """, [row["customer_type"], row["region"], row["sla_hours"],
                  row["annual_rev_band"], bool(row["active_flag"]),
                  row["contract_since"], row["etl_loaded_at"],
                  row["customer_id"]])

    total = conn.execute("SELECT COUNT(*) FROM dim_customer").fetchone()[0]
    logger.info(f"  dim_customer: {len(new_customers)} inserted, {len(existing_customers)} updated → {total:,} total")
    return {"customers_inserted": len(new_customers), "customers_updated": len(existing_customers)}


# ---------------------------------------------------------------------------
# Step 2: Load fact_orders (incremental)
# ---------------------------------------------------------------------------

def load_orders(conn: duckdb.DuckDBPyConnection, data_dir: Path,
                logger: logging.Logger) -> dict:
    """
    Load outbound_orders.csv → fact_orders (incremental).
    - Deduplicates source by order_id (logs count)
    - Checks referential integrity against dim_customer
    - Only inserts order_ids not already in fact_orders
    """
    t_start = time.time()
    path = data_dir / "outbound_orders.csv"
    logger.info(f"  Reading {path.name}...")
    df = pd.read_csv(path, low_memory=False)

    df = df.rename(columns={
        "Order_ID":      "order_id",
        "Order_Date":    "order_date",
        "Ship_Date":     "ship_date",
        "SKU_ID":        "sku_id",
        "Customer_ID":   "customer_id",
        "Warehouse_ID":  "warehouse_id",
        "Channel":       "channel",
        "Ordered_Qty":   "ordered_qty",
        "Shipped_Qty":   "shipped_qty",
        "Revenue":       "revenue",
        "On_Time_Flag":  "on_time_flag",
        "In_Full_Flag":  "in_full_flag",
        "OTIF_Flag":     "otif_flag",
    })

    # Deduplication
    pre_dedup = len(df)
    dups = df[df.duplicated("order_id", keep=False)]
    if len(dups) > 0:
        logger.warning(f"  Found {df['order_id'].duplicated().sum()} duplicate order_ids — keeping first occurrence")
    df = df.drop_duplicates(subset="order_id", keep="first")
    logger.info(f"  Source rows: {pre_dedup:,} → {len(df):,} after dedup")

    # Type casting
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce").dt.date
    df["ship_date"]  = pd.to_datetime(df["ship_date"],  errors="coerce").dt.date
    df["ordered_qty"]  = pd.to_numeric(df["ordered_qty"],  errors="coerce").fillna(0).astype(int)
    df["shipped_qty"]  = pd.to_numeric(df["shipped_qty"],  errors="coerce").fillna(0).astype(int)
    df["revenue"]      = pd.to_numeric(df["revenue"],      errors="coerce").fillna(0.0)
    df["on_time_flag"] = df["on_time_flag"].astype(bool)
    df["in_full_flag"] = df["in_full_flag"].astype(bool)
    df["otif_flag"]    = df["otif_flag"].astype(bool)

    # days_to_ship
    order_dt = pd.to_datetime(df["order_date"])
    ship_dt  = pd.to_datetime(df["ship_date"])
    df["days_to_ship"] = (ship_dt - order_dt).dt.days.clip(lower=0)

    df["etl_loaded_at"] = datetime.utcnow()

    # Referential integrity check
    valid_customers = {r[0] for r in conn.execute("SELECT customer_id FROM dim_customer").fetchall()}
    invalid_mask = ~df["customer_id"].isin(valid_customers)
    if invalid_mask.sum() > 0:
        logger.warning(f"  {invalid_mask.sum()} orders have unknown customer_ids — excluding from load")
        df = df[~invalid_mask]

    # Incremental: skip already-loaded order_ids
    existing_orders = {r[0] for r in conn.execute("SELECT order_id FROM fact_orders").fetchall()}
    new_orders = df[~df["order_id"].isin(existing_orders)].copy()
    logger.info(f"  New orders (not yet loaded): {len(new_orders):,} of {len(df):,}")

    if len(new_orders) == 0:
        logger.info("  fact_orders is already up to date — nothing to insert")
        return {"orders_inserted": 0, "status": "UP_TO_DATE"}

    # Load
    cols = ["order_id", "customer_id", "sku_id", "warehouse_id", "order_date",
            "ship_date", "channel", "ordered_qty", "shipped_qty", "revenue",
            "on_time_flag", "in_full_flag", "otif_flag", "days_to_ship", "etl_loaded_at"]
    new_orders = new_orders[[c for c in cols if c in new_orders.columns]]

    chunk_size = 10_000
    col_list = ", ".join(f'"{c}"' for c in new_orders.columns)
    for i in range(0, len(new_orders), chunk_size):
        chunk = new_orders.iloc[i:i + chunk_size]
        conn.register("_orders_staging", chunk)
        conn.execute(f"INSERT INTO fact_orders ({col_list}) SELECT {col_list} FROM _orders_staging")
        conn.unregister("_orders_staging")

    total = conn.execute("SELECT COUNT(*) FROM fact_orders").fetchone()[0]
    duration = round(time.time() - t_start, 2)
    logger.info(f"  Inserted {len(new_orders):,} orders in {duration}s → {total:,} total in fact_orders")
    return {"orders_inserted": len(new_orders), "status": "OK", "duration_s": duration}


# ---------------------------------------------------------------------------
# Step 3: Update dim_customer lifetime metrics
# ---------------------------------------------------------------------------

def update_lifetime_metrics(conn: duckdb.DuckDBPyConnection, logger: logging.Logger) -> None:
    """
    Recompute and upsert lifetime metrics into dim_customer from fact_orders.
    Updates: first_order_date, last_order_date, lifetime_orders, lifetime_revenue.
    """
    logger.info("  Updating dim_customer lifetime metrics from fact_orders...")
    conn.execute("""
        UPDATE dim_customer AS dc
        SET
            first_order_date = metrics.first_order,
            last_order_date  = metrics.last_order,
            lifetime_orders  = metrics.total_orders,
            lifetime_revenue = metrics.total_revenue
        FROM (
            SELECT
                customer_id,
                MIN(order_date)     AS first_order,
                MAX(order_date)     AS last_order,
                COUNT(*)            AS total_orders,
                SUM(revenue)        AS total_revenue
            FROM fact_orders
            GROUP BY customer_id
        ) AS metrics
        WHERE dc.customer_id = metrics.customer_id
    """)
    logger.info("  dim_customer lifetime metrics updated")


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_customer_etl(db_path: Path = DB_PATH, data_dir: Path = DATA_DIR,
                     logger: logging.Logger = None) -> dict:
    if logger is None:
        logger = get_logger()

    t_start = time.time()
    logger.info("=" * 60)
    logger.info("CUSTOMER ETL — START")
    logger.info(f"DB:   {db_path}")
    logger.info(f"Data: {data_dir}")
    logger.info("=" * 60)

    conn = duckdb.connect(str(db_path))

    cust_stats   = load_customers(conn, data_dir, logger)
    orders_stats = load_orders(conn, data_dir, logger)
    update_lifetime_metrics(conn, logger)

    conn.close()

    duration = round(time.time() - t_start, 2)
    logger.info(f"\nCustomer ETL complete in {duration}s")
    logger.info("CUSTOMER ETL — COMPLETE")

    return {**cust_stats, **orders_stats, "duration_s": duration}


if __name__ == "__main__":
    import sys
    db = Path(sys.argv[1]) if len(sys.argv) > 1 else DB_PATH
    logger = get_logger()
    stats = run_customer_etl(db_path=db, logger=logger)
    print(f"\nCustomers: {stats.get('customers_inserted',0)} inserted, {stats.get('customers_updated',0)} updated")
    print(f"Orders:    {stats.get('orders_inserted',0)} inserted — {stats.get('status','OK')}")
