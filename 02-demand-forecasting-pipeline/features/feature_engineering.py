"""
feature_engineering.py — Feature Engineering Pipeline
DHL Demand Forecasting Pipeline — Project 02

Computes and stores forecasting features for every active SKU and date in the demand history.
Features:
  - Lag features: prior demand at lag 1, 7, 14, 28 days
  - Rolling averages: 7, 14, 28 day windows
  - Rolling std deviation: 7, 14 day windows
  - Calendar: day_of_week (0-6), week_of_year, month, quarter, is_weekend, season
  - Segment: abc_class, xyz_class from dim_sku

Edge case handling:
  - Lag features at start of history (insufficient prior data) → filled with SKU mean
  - Rolling stats with < window days of prior data → filled with available data mean/std

Stores results in fact_feature_store (append-only for dates not already present).
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

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH  = BASE_DIR.parent / "01-sku-segmentation-pipeline" / "outputs" / "dhl_warehouse.duckdb"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def get_logger(name: str = "feature_engineering") -> logging.Logger:
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
# Calendar helpers
# ---------------------------------------------------------------------------

def get_season(month: int) -> str:
    if month in (3, 4, 5):   return "Spring"
    if month in (6, 7, 8):   return "Summer"
    if month in (9, 10, 11): return "Autumn"
    return "Winter"

# ---------------------------------------------------------------------------
# Feature computation
# ---------------------------------------------------------------------------

def compute_features_for_sku_warehouse(df_sku: pd.DataFrame, sku_id: str,
                                        warehouse_id: str, abc_class: str,
                                        xyz_class: str) -> pd.DataFrame:
    """
    Compute all features for a single SKU-warehouse combination.
    df_sku must be sorted by date with columns: feature_date, quantity_demanded.
    """
    df = df_sku.copy().sort_values("feature_date").reset_index(drop=True)
    qty = df["quantity_demanded"].astype(float)
    sku_mean = qty.mean() if len(qty) > 0 else 0.0

    # Lag features — shift by n periods
    for lag in [1, 7, 14, 28]:
        col = qty.shift(lag)
        # Fill NaN (insufficient history) with SKU mean
        df[f"lag_{lag}"] = col.fillna(sku_mean)

    # Rolling averages (min_periods=1 uses available data)
    for w in [7, 14, 28]:
        df[f"rolling_avg_{w}"] = (
            qty.shift(1).rolling(window=w, min_periods=1).mean().fillna(sku_mean)
        )

    # Rolling std (min_periods=2 for std)
    for w in [7, 14]:
        roll_std = qty.shift(1).rolling(window=w, min_periods=2).std()
        df[f"rolling_std_{w}"] = roll_std.fillna(0.0)

    # Calendar features
    dates = pd.to_datetime(df["feature_date"])
    df["day_of_week"]  = dates.dt.dayofweek      # 0=Monday
    df["week_of_year"] = dates.dt.isocalendar().week.astype(int)
    df["month"]        = dates.dt.month
    df["quarter"]      = dates.dt.quarter
    df["is_weekend"]   = df["day_of_week"].isin([5, 6])
    df["season"]       = df["month"].apply(get_season)

    # Segment
    df["abc_class"] = abc_class
    df["xyz_class"] = xyz_class
    df["sku_id"]    = sku_id
    df["warehouse_id"] = warehouse_id

    return df


def run_feature_engineering(db_path: Path = DB_PATH, logger: logging.Logger = None,
                              batch_size: int = 100) -> dict:
    """
    Main feature engineering entry point.
    Computes features for all SKU-warehouse combinations not yet in fact_feature_store.
    Appends results to fact_feature_store.
    """
    if logger is None:
        logger = get_logger()

    t_start = time.time()
    loaded_at = datetime.utcnow()

    logger.info("=" * 60)
    logger.info("FEATURE ENGINEERING — START")
    logger.info(f"Database: {db_path}")
    logger.info("=" * 60)

    conn = duckdb.connect(str(db_path))

    # --- Get existing watermark from feature store ---
    existing_max = conn.execute(
        "SELECT MAX(feature_date) FROM fact_feature_store"
    ).fetchone()[0]
    if existing_max is not None:
        existing_max = pd.to_datetime(existing_max).date()
        logger.info(f"  Existing feature store max date: {existing_max}")
    else:
        existing_max = date(2021, 12, 31)
        logger.info("  Feature store is empty — computing all features")

    # --- Load demand data for all SKUs ---
    logger.info("  Loading demand data from warehouse...")
    demand_df = conn.execute("""
        SELECT
            d.full_date AS feature_date,
            s.sku_id,
            w.warehouse_id,
            s.abc_class,
            s.xyz_class,
            f.quantity_demanded
        FROM fact_daily_demand f
        JOIN dim_date      d ON f.date_key      = d.date_key
        JOIN dim_sku       s ON f.sku_key       = s.sku_key
        JOIN dim_warehouse w ON f.warehouse_key = w.warehouse_key
        WHERE s.active_flag = TRUE
        ORDER BY s.sku_id, w.warehouse_id, d.full_date
    """).df()

    demand_df["feature_date"] = pd.to_datetime(demand_df["feature_date"]).dt.date
    logger.info(f"  Loaded {len(demand_df):,} demand rows for {demand_df['sku_id'].nunique():,} SKUs")

    # --- Get max existing feature_id ---
    max_fid = conn.execute(
        "SELECT COALESCE(MAX(feature_id), 0) FROM fact_feature_store"
    ).fetchone()[0]
    feature_id_counter = int(max_fid)

    # --- Process each SKU-warehouse combination ---
    sku_wh_groups = demand_df.groupby(["sku_id", "warehouse_id"])
    total_groups = len(sku_wh_groups)
    logger.info(f"  Processing {total_groups:,} SKU-warehouse combinations...")

    all_features = []
    processed = 0

    for (sku_id, warehouse_id), group_df in sku_wh_groups:
        abc = group_df["abc_class"].iloc[0]
        xyz = group_df["xyz_class"].iloc[0]

        feat_df = compute_features_for_sku_warehouse(
            group_df[["feature_date", "quantity_demanded"]],
            sku_id, warehouse_id, abc, xyz
        )

        # Only keep dates not yet in the feature store
        feat_df = feat_df[feat_df["feature_date"] > existing_max]

        if len(feat_df) > 0:
            all_features.append(feat_df)

        processed += 1
        if processed % 500 == 0:
            logger.info(f"  Processed {processed:,}/{total_groups:,} SKU-warehouse pairs...")

    if not all_features:
        logger.info("  No new features to compute — feature store is up to date")
        conn.close()
        return {"rows_added": 0, "duration_s": round(time.time() - t_start, 2), "status": "UP_TO_DATE"}

    # --- Combine and assign feature IDs ---
    logger.info("  Combining feature batches...")
    combined = pd.concat(all_features, ignore_index=True)
    combined["feature_id"]      = range(feature_id_counter + 1, feature_id_counter + len(combined) + 1)
    combined["etl_loaded_at"]   = loaded_at
    combined["etl_source_file"] = "fact_daily_demand (computed)"

    # Select final columns matching DDL order
    final_cols = [
        "feature_id", "sku_id", "warehouse_id", "feature_date",
        "lag_1", "lag_7", "lag_14", "lag_28",
        "rolling_avg_7", "rolling_avg_14", "rolling_avg_28",
        "rolling_std_7", "rolling_std_14",
        "day_of_week", "week_of_year", "month", "quarter",
        "is_weekend", "season", "abc_class", "xyz_class",
        "etl_loaded_at", "etl_source_file"
    ]
    combined = combined[[c for c in final_cols if c in combined.columns]]

    # --- Load into DuckDB in batches ---
    logger.info(f"  Loading {len(combined):,} feature rows into fact_feature_store...")
    chunk_size = 50_000
    col_list   = ", ".join(f'"{c}"' for c in combined.columns)

    for i in range(0, len(combined), chunk_size):
        chunk = combined.iloc[i:i + chunk_size]
        conn.register("_feat_staging", chunk)
        conn.execute(f"INSERT INTO fact_feature_store ({col_list}) SELECT {col_list} FROM _feat_staging")
        conn.unregister("_feat_staging")

    # --- Verify ---
    total_features = conn.execute("SELECT COUNT(*) FROM fact_feature_store").fetchone()[0]
    conn.close()

    duration_s = round(time.time() - t_start, 2)
    logger.info(f"  Total rows in fact_feature_store: {total_features:,}")
    logger.info(f"  Feature engineering complete in {duration_s}s")
    logger.info("FEATURE ENGINEERING — COMPLETE")

    return {
        "rows_added":   len(combined),
        "total_rows":   total_features,
        "duration_s":   duration_s,
        "status":       "OK",
    }


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    db = Path(sys.argv[1]) if len(sys.argv) > 1 else DB_PATH
    logger = get_logger()
    stats = run_feature_engineering(db_path=db, logger=logger)
    print(f"\nFeature engineering: {stats['rows_added']:,} rows added — {stats['status']}")
    print(f"Runtime: {stats['duration_s']}s")
