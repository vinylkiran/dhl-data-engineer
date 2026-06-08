"""
transform.py — Transform Layer
DHL SKU Segmentation Pipeline — Project 01

Builds all dimension and fact tables from raw extracted DataFrames.
Responsibilities:
  - Build dim_date programmatically (every date 2022-01-01 to 2023-12-31)
  - Standardise column names to snake_case
  - Cast data types (dates, numeric, boolean)
  - Handle nulls with documented business rules
  - Add audit columns (etl_loaded_at, etl_source_file)
  - Generate surrogate keys for all dimension tables
  - Derive computed measures (fill_rate, quantity_unfulfilled, IRA)
"""

import logging
import re
from datetime import datetime, date
import pandas as pd
import numpy as np

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def get_logger(name: str = "transform") -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        ))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger

# ---------------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------------

def to_snake_case(col: str) -> str:
    """Convert CamelCase / Mixed_Case column names to snake_case."""
    col = re.sub(r'([A-Z]+)([A-Z][a-z])', r'\1_\2', col)
    col = re.sub(r'([a-z\d])([A-Z])', r'\1_\2', col)
    return col.strip().lower().replace(' ', '_').replace('-', '_')


def rename_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Rename all columns to snake_case."""
    return df.rename(columns={c: to_snake_case(c) for c in df.columns})


def add_audit_cols(df: pd.DataFrame, source_file: str, loaded_at: datetime) -> pd.DataFrame:
    """Add etl_loaded_at and etl_source_file audit columns."""
    df = df.copy()
    df["etl_loaded_at"] = loaded_at
    df["etl_source_file"] = source_file
    return df


def log_nulls(df: pd.DataFrame, table_name: str, logger: logging.Logger):
    """Log any null values found in the DataFrame."""
    null_counts = df.isnull().sum()
    nulls = null_counts[null_counts > 0]
    if len(nulls) > 0:
        for col, count in nulls.items():
            pct = count / len(df) * 100
            logger.warning(f"  NULL values in {table_name}.{col}: {count:,} ({pct:.2f}%)")
    else:
        logger.info(f"  No nulls found in {table_name}")


# ---------------------------------------------------------------------------
# Dimension builders
# ---------------------------------------------------------------------------

def build_dim_date(loaded_at: datetime, logger: logging.Logger) -> pd.DataFrame:
    """Build complete date dimension from 2022-01-01 to 2023-12-31."""
    logger.info("Building dim_date...")

    dates = pd.date_range(start="2022-01-01", end="2023-12-31", freq="D")
    df = pd.DataFrame({"full_date": dates})

    df["date_key"]      = df["full_date"].dt.strftime("%Y%m%d").astype(int)
    df["day_of_week"]   = df["full_date"].dt.dayofweek + 1  # 1=Monday
    df["day_name"]      = df["full_date"].dt.day_name()
    df["day_of_month"]  = df["full_date"].dt.day
    df["day_of_year"]   = df["full_date"].dt.dayofyear
    df["week_of_year"]  = df["full_date"].dt.isocalendar().week.astype(int)
    df["month_num"]     = df["full_date"].dt.month
    df["month_name"]    = df["full_date"].dt.month_name()
    df["quarter"]       = df["full_date"].dt.quarter
    df["year"]          = df["full_date"].dt.year
    df["is_weekend"]    = df["day_of_week"].isin([6, 7])
    df["is_month_start"] = df["full_date"].dt.is_month_start
    df["is_month_end"]  = df["full_date"].dt.is_month_end
    df["is_quarter_end"] = df["full_date"].dt.is_quarter_end

    # Season (Northern Hemisphere)
    def get_season(month: int) -> str:
        if month in (3, 4, 5):   return "Spring"
        if month in (6, 7, 8):   return "Summer"
        if month in (9, 10, 11): return "Autumn"
        return "Winter"

    df["season"] = df["month_num"].apply(get_season)
    # Keep full_date as Timestamp — DuckDB will cast to DATE correctly
    # (converting to Python date causes positional type mismatch on load)

    df = add_audit_cols(df, "programmatic", loaded_at)
    logger.info(f"  dim_date: {len(df):,} rows ({df['full_date'].min()} to {df['full_date'].max()})")
    return df


def build_dim_warehouse(loaded_at: datetime, logger: logging.Logger) -> pd.DataFrame:
    """Build warehouse dimension from known warehouse IDs."""
    logger.info("Building dim_warehouse...")

    warehouses = [
        {
            "warehouse_id":   "DHL-WH-IL02",
            "warehouse_name": "DHL Warehouse Illinois 02",
            "city":           "Chicago",
            "state":          "Illinois",
            "region":         "Midwest",
            "country":        "USA",
            "timezone":       "America/Chicago",
            "active_flag":    True,
        },
        {
            "warehouse_id":   "DHL-WH-NJ01",
            "warehouse_name": "DHL Warehouse New Jersey 01",
            "city":           "Newark",
            "state":          "New Jersey",
            "region":         "Northeast",
            "country":        "USA",
            "timezone":       "America/New_York",
            "active_flag":    True,
        },
        {
            "warehouse_id":   "DHL-WH-TX03",
            "warehouse_name": "DHL Warehouse Texas 03",
            "city":           "Dallas",
            "state":          "Texas",
            "region":         "South",
            "country":        "USA",
            "timezone":       "America/Chicago",
            "active_flag":    True,
        },
    ]

    df = pd.DataFrame(warehouses)
    df.insert(0, "warehouse_key", range(1, len(df) + 1))
    df = add_audit_cols(df, "programmatic", loaded_at)
    logger.info(f"  dim_warehouse: {len(df):,} rows")
    return df


def build_dim_supplier(raw: pd.DataFrame, loaded_at: datetime, logger: logging.Logger) -> pd.DataFrame:
    """Build supplier dimension from suppliers.csv."""
    logger.info("Building dim_supplier...")

    df = rename_cols(raw.copy())
    log_nulls(df, "dim_supplier (pre-transform)", logger)

    # Cast types
    df["lead_time_avg_days"] = pd.to_numeric(df["lead_time_avg_days"], errors="coerce")
    df["lead_time_std_days"] = pd.to_numeric(df["lead_time_std_days"], errors="coerce")
    df["otif_rate"]    = pd.to_numeric(df["otif_rate"], errors="coerce")
    df["fill_rate"]    = pd.to_numeric(df["fill_rate"], errors="coerce")
    df["defect_rate"]  = pd.to_numeric(df["defect_rate"], errors="coerce")
    df["active_flag"]  = df["active_flag"].astype(bool)

    # Surrogate key
    df = df.sort_values("supplier_id").reset_index(drop=True)
    df.insert(0, "supplier_key", range(1, len(df) + 1))

    df = add_audit_cols(df, "suppliers.csv", loaded_at)
    log_nulls(df[df.columns.difference(["etl_loaded_at","etl_source_file"])], "dim_supplier", logger)
    logger.info(f"  dim_supplier: {len(df):,} rows")
    return df


def build_dim_sku(raw: pd.DataFrame, supplier_dim: pd.DataFrame,
                  loaded_at: datetime, logger: logging.Logger) -> pd.DataFrame:
    """Build SKU dimension from sku_master.csv."""
    logger.info("Building dim_sku...")

    df = rename_cols(raw.copy())
    log_nulls(df, "dim_sku (pre-transform)", logger)

    # Cast types
    df["unit_cost"]    = pd.to_numeric(df["unit_cost"], errors="coerce")
    df["unit_price"]   = pd.to_numeric(df["unit_price"], errors="coerce")
    df["weight_kg"]    = pd.to_numeric(df["weight_kg"], errors="coerce")
    df["volume_cbm"]   = pd.to_numeric(df["volume_cbm"], errors="coerce")
    df["lead_time_days"]      = pd.to_numeric(df["lead_time_days"], errors="coerce").astype("Int64")
    df["min_order_qty"]       = pd.to_numeric(df["min_order_qty"], errors="coerce").astype("Int64")
    df["safety_stock_qty"]    = pd.to_numeric(df["safety_stock_qty"], errors="coerce").astype("Int64")
    df["reorder_point_qty"]   = pd.to_numeric(df["reorder_point_qty"], errors="coerce").astype("Int64")
    df["active_flag"] = df["active_flag"].astype(bool)

    # XYZ class may not be in sku_master — add if missing
    if "xyz_class" not in df.columns:
        df["xyz_class"] = None

    # Surrogate key
    df = df.sort_values("sku_id").reset_index(drop=True)
    df.insert(0, "sku_key", range(1, len(df) + 1))

    df = add_audit_cols(df, "sku_master.csv", loaded_at)
    logger.info(f"  dim_sku: {len(df):,} rows")
    return df


# ---------------------------------------------------------------------------
# Fact builders
# ---------------------------------------------------------------------------

def build_fact_daily_demand(raw: pd.DataFrame, dim_date: pd.DataFrame,
                             dim_sku: pd.DataFrame, dim_warehouse: pd.DataFrame,
                             loaded_at: datetime, logger: logging.Logger) -> pd.DataFrame:
    """Build fact_daily_demand from daily_demand.csv."""
    logger.info("Building fact_daily_demand...")

    df = rename_cols(raw.copy())
    log_nulls(df, "fact_daily_demand (pre-transform)", logger)

    # Cast types
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["quantity_demanded"]  = pd.to_numeric(df["quantity_demanded"], errors="coerce").fillna(0).astype(int)
    df["quantity_fulfilled"] = pd.to_numeric(df["quantity_fulfilled"], errors="coerce").fillna(0).astype(int)
    df["stockout_flag"]      = df["stockout_flag"].astype(bool)
    df["revenue"]            = pd.to_numeric(df["revenue"], errors="coerce")

    # Business rule: null revenue on stockout records → 0
    null_rev_count = df["revenue"].isnull().sum()
    if null_rev_count > 0:
        logger.warning(f"  Null revenue in {null_rev_count:,} rows — setting to 0 (business rule: stockout records)")
    df["revenue"] = df["revenue"].fillna(0)

    # Derived measures
    df["quantity_unfulfilled"] = (df["quantity_demanded"] - df["quantity_fulfilled"]).clip(lower=0)
    df["fill_rate"] = np.where(
        df["quantity_demanded"] > 0,
        df["quantity_fulfilled"] / df["quantity_demanded"],
        None
    )

    # --- Join surrogate keys ---
    # date_key
    date_lookup = dim_date[["full_date", "date_key"]].copy()
    date_lookup["full_date"] = pd.to_datetime(date_lookup["full_date"]).dt.date
    df = df.merge(date_lookup, left_on="date", right_on="full_date", how="left")

    # sku_key
    sku_lookup = dim_sku[["sku_id", "sku_key"]].copy()
    df = df.merge(sku_lookup, on="sku_id", how="left")

    # warehouse_key
    wh_lookup = dim_warehouse[["warehouse_id", "warehouse_key"]].copy()
    df = df.merge(wh_lookup, on="warehouse_id", how="left")

    # Check for unmatched foreign keys
    for fk, col in [("date_key", "date"), ("sku_key", "sku_id"), ("warehouse_key", "warehouse_id")]:
        unmatched = df[fk].isnull().sum()
        if unmatched > 0:
            logger.warning(f"  {unmatched:,} rows with unmatched {fk} (source: {col})")

    # Surrogate key
    df = df.reset_index(drop=True)
    df.insert(0, "demand_key", range(1, len(df) + 1))

    # Select final columns
    final_cols = [
        "demand_key", "date_key", "sku_key", "warehouse_key",
        "abc_class", "xyz_class",
        "quantity_demanded", "quantity_fulfilled", "quantity_unfulfilled",
        "stockout_flag", "revenue", "fill_rate",
        "etl_loaded_at", "etl_source_file"
    ]
    df = add_audit_cols(df, "daily_demand.csv", loaded_at)
    df = df[[c for c in final_cols if c in df.columns]]

    log_nulls(df[["demand_key","date_key","sku_key","warehouse_key"]], "fact_daily_demand FKs", logger)
    logger.info(f"  fact_daily_demand: {len(df):,} rows")
    return df


def build_fact_inventory_snapshot(raw: pd.DataFrame, dim_date: pd.DataFrame,
                                   dim_sku: pd.DataFrame, dim_warehouse: pd.DataFrame,
                                   loaded_at: datetime, logger: logging.Logger) -> pd.DataFrame:
    """Build fact_inventory_snapshot from inventory_snapshot.csv."""
    logger.info("Building fact_inventory_snapshot...")

    df = rename_cols(raw.copy())
    log_nulls(df, "fact_inventory_snapshot (pre-transform)", logger)

    # Cast types
    df["snapshot_date"]   = pd.to_datetime(df["snapshot_date"]).dt.date
    df["on_hand_qty"]     = pd.to_numeric(df["on_hand_qty"], errors="coerce").fillna(0).astype(int)
    df["in_transit_qty"]  = pd.to_numeric(df["in_transit_qty"], errors="coerce").fillna(0).astype(int)
    df["committed_qty"]   = pd.to_numeric(df["committed_qty"], errors="coerce").fillna(0).astype(int)
    df["available_qty"]   = pd.to_numeric(df["available_qty"], errors="coerce").fillna(0).astype(int)
    df["inventory_value"] = pd.to_numeric(df["inventory_value"], errors="coerce").fillna(0)

    # Derived: IRA = available / on_hand (0 if on_hand = 0)
    df["inventory_record_accuracy"] = np.where(
        df["on_hand_qty"] > 0,
        df["available_qty"] / df["on_hand_qty"],
        None
    )

    # --- Join surrogate keys ---
    date_lookup = dim_date[["full_date", "date_key"]].copy()
    date_lookup["full_date"] = pd.to_datetime(date_lookup["full_date"]).dt.date
    df = df.merge(date_lookup, left_on="snapshot_date", right_on="full_date", how="left")

    sku_lookup = dim_warehouse_lookup = None
    sku_lookup   = dim_sku[["sku_id", "sku_key"]].copy()
    wh_lookup    = dim_warehouse[["warehouse_id", "warehouse_key"]].copy()
    df = df.merge(sku_lookup, on="sku_id", how="left")
    df = df.merge(wh_lookup, on="warehouse_id", how="left")

    for fk, col in [("date_key", "snapshot_date"), ("sku_key", "sku_id"), ("warehouse_key", "warehouse_id")]:
        unmatched = df[fk].isnull().sum()
        if unmatched > 0:
            logger.warning(f"  {unmatched:,} rows with unmatched {fk}")

    # Surrogate key
    df = df.reset_index(drop=True)
    df.insert(0, "snapshot_key", range(1, len(df) + 1))

    final_cols = [
        "snapshot_key", "date_key", "sku_key", "warehouse_key",
        "on_hand_qty", "in_transit_qty", "committed_qty", "available_qty",
        "inventory_value", "inventory_record_accuracy",
        "etl_loaded_at", "etl_source_file"
    ]
    df = add_audit_cols(df, "inventory_snapshot.csv", loaded_at)
    df = df[[c for c in final_cols if c in df.columns]]

    logger.info(f"  fact_inventory_snapshot: {len(df):,} rows")
    return df


# ---------------------------------------------------------------------------
# Main transform entry point
# ---------------------------------------------------------------------------

def transform_all(extracted: dict, logger: logging.Logger = None) -> dict:
    """
    Transform all extracted DataFrames into dimension and fact tables.
    Returns dict of {table_name: DataFrame}.
    """
    if logger is None:
        logger = get_logger("transform")

    loaded_at = datetime.utcnow()

    logger.info("=" * 60)
    logger.info("TRANSFORM STAGE — START")
    logger.info(f"ETL timestamp: {loaded_at.isoformat()}")
    logger.info("=" * 60)

    transformed = {}

    # Dimensions (order matters — facts depend on dims)
    transformed["dim_date"]      = build_dim_date(loaded_at, logger)
    transformed["dim_warehouse"] = build_dim_warehouse(loaded_at, logger)
    transformed["dim_supplier"]  = build_dim_supplier(extracted["suppliers"], loaded_at, logger)
    transformed["dim_sku"]       = build_dim_sku(extracted["sku_master"], transformed["dim_supplier"], loaded_at, logger)

    # Facts
    transformed["fact_daily_demand"] = build_fact_daily_demand(
        extracted["daily_demand"],
        transformed["dim_date"],
        transformed["dim_sku"],
        transformed["dim_warehouse"],
        loaded_at, logger
    )
    transformed["fact_inventory_snapshot"] = build_fact_inventory_snapshot(
        extracted["inventory_snapshot"],
        transformed["dim_date"],
        transformed["dim_sku"],
        transformed["dim_warehouse"],
        loaded_at, logger
    )

    logger.info("TRANSFORM STAGE — COMPLETE")
    logger.info("Table summary:")
    for name, df in transformed.items():
        logger.info(f"  {name}: {len(df):,} rows × {len(df.columns)} cols")

    return transformed


# ---------------------------------------------------------------------------
# Standalone run
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    from extract import extract_all
    logger = get_logger("transform")
    extracted = extract_all(logger=logger)
    transformed = transform_all(extracted, logger=logger)
    print("\nTransformed tables:")
    for name, df in transformed.items():
        print(f"  {name}: {len(df):,} rows")
