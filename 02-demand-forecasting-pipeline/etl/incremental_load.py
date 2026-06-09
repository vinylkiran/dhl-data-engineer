"""
incremental_load.py — Incremental Load Pattern
DHL Demand Forecasting Pipeline — Project 02

Simulates production behaviour where new daily demand records arrive each day.
Unlike Project 01's full truncate-and-reload, this module:
  - Checks the max date already loaded in fact_daily_demand
  - Only processes records newer than that watermark date
  - Appends new records without touching existing data
  - Logs how many new records were ingested each run

In this portfolio simulation, we use a configurable cutoff date to simulate
"new data arriving" — defaulting to simulating data through 2023-12-31.
"""

import logging
import time
from datetime import datetime, date
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

def get_logger(name: str = "incremental_load") -> logging.Logger:
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
# Incremental load functions
# ---------------------------------------------------------------------------

def get_watermark(conn: duckdb.DuckDBPyConnection, logger: logging.Logger) -> date:
    """Return the max date currently loaded in fact_daily_demand."""
    result = conn.execute("""
        SELECT MAX(d.full_date)
        FROM fact_daily_demand f
        JOIN dim_date d ON f.date_key = d.date_key
    """).fetchone()[0]

    if result is None:
        watermark = date(2021, 12, 31)  # Load everything
        logger.info(f"  No existing data found — loading all records from scratch")
    else:
        watermark = result if isinstance(result, date) else result.date()
        logger.info(f"  Watermark date (last loaded): {watermark}")
    return watermark


def load_new_demand_records(conn: duckdb.DuckDBPyConnection, watermark: date,
                             sim_cutoff: date, logger: logging.Logger) -> dict:
    """
    Load demand records newer than watermark up to sim_cutoff.
    Returns load statistics.
    """
    t_start = time.time()
    loaded_at = datetime.utcnow()

    # Read source CSV
    demand_path = DATA_DIR / "daily_demand.csv"
    logger.info(f"  Reading {demand_path.name}...")
    df = pd.read_csv(demand_path, low_memory=False)
    df["Date"] = pd.to_datetime(df["Date"]).dt.date

    # Filter to new records only
    new_df = df[(df["Date"] > watermark) & (df["Date"] <= sim_cutoff)].copy()

    if len(new_df) == 0:
        logger.info(f"  No new records found (watermark={watermark}, cutoff={sim_cutoff})")
        return {"new_records": 0, "watermark": watermark, "duration_s": 0, "status": "NO_NEW_DATA"}

    logger.info(f"  New records found: {len(new_df):,} (dates: {new_df['Date'].min()} to {new_df['Date'].max()})")

    # Transform — look up surrogate keys
    # date_key
    date_map = conn.execute("SELECT full_date, date_key FROM dim_date").df()
    date_map["full_date"] = pd.to_datetime(date_map["full_date"]).dt.date
    new_df = new_df.merge(date_map, left_on="Date", right_on="full_date", how="left")

    # sku_key
    sku_map = conn.execute("SELECT sku_id, sku_key FROM dim_sku").df()
    new_df = new_df.merge(sku_map, left_on="SKU_ID", right_on="sku_id", how="left")

    # warehouse_key
    wh_map = conn.execute("SELECT warehouse_id, warehouse_key FROM dim_warehouse").df()
    new_df = new_df.merge(wh_map, left_on="Warehouse_ID", right_on="warehouse_id", how="left")

    # Rename and cast
    new_df = new_df.rename(columns={
        "Quantity_Demanded": "quantity_demanded",
        "Quantity_Fulfilled": "quantity_fulfilled",
        "Stockout_Flag": "stockout_flag",
        "Revenue": "revenue",
        "ABC_Class": "abc_class",
        "XYZ_Class": "xyz_class",
    })

    new_df["quantity_demanded"]   = pd.to_numeric(new_df["quantity_demanded"], errors="coerce").fillna(0).astype(int)
    new_df["quantity_fulfilled"]  = pd.to_numeric(new_df["quantity_fulfilled"], errors="coerce").fillna(0).astype(int)
    new_df["quantity_unfulfilled"] = (new_df["quantity_demanded"] - new_df["quantity_fulfilled"]).clip(lower=0)
    new_df["stockout_flag"]       = new_df["stockout_flag"].astype(bool)
    new_df["revenue"]             = pd.to_numeric(new_df["revenue"], errors="coerce").fillna(0)
    new_df["fill_rate"]           = np.where(
        new_df["quantity_demanded"] > 0,
        new_df["quantity_fulfilled"] / new_df["quantity_demanded"], None
    )

    # Get max existing demand_key for surrogate key continuation
    max_key = conn.execute("SELECT COALESCE(MAX(demand_key), 0) FROM fact_daily_demand").fetchone()[0]
    new_df = new_df.reset_index(drop=True)
    new_df["demand_key"]      = range(int(max_key) + 1, int(max_key) + len(new_df) + 1)
    new_df["etl_loaded_at"]   = loaded_at
    new_df["etl_source_file"] = "daily_demand.csv (incremental)"

    # Select only the columns the table expects
    load_cols = [
        "demand_key", "date_key", "sku_key", "warehouse_key",
        "abc_class", "xyz_class",
        "quantity_demanded", "quantity_fulfilled", "quantity_unfulfilled",
        "stockout_flag", "revenue", "fill_rate",
        "etl_loaded_at", "etl_source_file"
    ]
    new_df = new_df[[c for c in load_cols if c in new_df.columns]]

    # Append to fact_daily_demand
    conn.register("_incr_staging", new_df)
    col_list = ", ".join(f'"{c}"' for c in new_df.columns)
    conn.execute(f"INSERT INTO fact_daily_demand ({col_list}) SELECT {col_list} FROM _incr_staging")
    conn.unregister("_incr_staging")

    duration_s = round(time.time() - t_start, 3)
    new_max    = new_df["Date"].max() if "Date" in new_df.columns else sim_cutoff

    logger.info(f"  Appended {len(new_df):,} new records in {duration_s}s")
    logger.info(f"  New watermark: {sim_cutoff}")

    return {
        "new_records":  len(new_df),
        "watermark":    watermark,
        "new_watermark": sim_cutoff,
        "duration_s":   duration_s,
        "status":       "OK",
    }


def run_incremental_load(db_path: Path = DB_PATH, sim_cutoff: date = date(2023, 12, 31),
                          logger: logging.Logger = None) -> dict:
    """
    Main entry point for incremental load.
    sim_cutoff: simulate data arriving up to this date (for portfolio demonstration).
    """
    if logger is None:
        logger = get_logger()

    logger.info("=" * 60)
    logger.info("INCREMENTAL LOAD — START")
    logger.info(f"Database: {db_path}")
    logger.info(f"Simulation cutoff: {sim_cutoff}")
    logger.info("=" * 60)

    conn = duckdb.connect(str(db_path))

    watermark = get_watermark(conn, logger)

    if watermark >= sim_cutoff:
        logger.info(f"  Data already up to date (watermark={watermark} >= cutoff={sim_cutoff})")
        conn.close()
        return {"new_records": 0, "status": "ALREADY_CURRENT"}

    stats = load_new_demand_records(conn, watermark, sim_cutoff, logger)

    # Verify total row count
    total = conn.execute("SELECT COUNT(*) FROM fact_daily_demand").fetchone()[0]
    logger.info(f"  Total rows in fact_daily_demand after load: {total:,}")

    conn.close()
    logger.info("INCREMENTAL LOAD — COMPLETE")
    return stats


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    db = Path(sys.argv[1]) if len(sys.argv) > 1 else DB_PATH
    logger = get_logger()
    stats = run_incremental_load(db_path=db, logger=logger)
    print(f"\nIncremental load: {stats['new_records']:,} new records — {stats['status']}")
