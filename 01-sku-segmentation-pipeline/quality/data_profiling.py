"""
data_profiling.py — Data Profiling
DHL SKU Segmentation Pipeline — Project 01

Profiles every table in the DuckDB warehouse.
For each column: data type, null count/%, distinct count, min, max, mean.
Flags columns with >5% nulls or >50% duplicate values.
Exports profile to outputs/data_profile.csv.

Usage:
    python data_profiling.py
    python data_profiling.py --db-path /custom/path/dhl_warehouse.duckdb
"""

import argparse
import logging
from datetime import datetime
from pathlib import Path
import duckdb
import pandas as pd

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BASE_DIR   = Path(__file__).resolve().parent.parent
DB_PATH    = BASE_DIR / "outputs" / "dhl_warehouse.duckdb"
OUTPUT_DIR = BASE_DIR / "outputs"

TABLES = [
    "dim_date",
    "dim_warehouse",
    "dim_supplier",
    "dim_sku",
    "fact_daily_demand",
    "fact_inventory_snapshot",
]

NULL_FLAG_THRESHOLD      = 0.05   # Flag if >5% nulls
DUPLICATE_FLAG_THRESHOLD = 0.50   # Flag if >50% duplicates (low cardinality)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def get_logger(name: str = "profiling") -> logging.Logger:
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
# Profiling functions
# ---------------------------------------------------------------------------

def profile_column(conn: duckdb.DuckDBPyConnection, table: str,
                   col: str, dtype: str, row_count: int) -> dict:
    """Profile a single column: nulls, distinct, min, max, mean."""

    # Null count
    null_count = conn.execute(
        f"SELECT COUNT(*) FROM {table} WHERE \"{col}\" IS NULL"
    ).fetchone()[0]
    null_pct = round(null_count / row_count * 100, 4) if row_count > 0 else 0

    # Distinct count
    distinct_count = conn.execute(
        f"SELECT COUNT(DISTINCT \"{col}\") FROM {table}"
    ).fetchone()[0]
    duplicate_pct = round((1 - distinct_count / row_count) * 100, 4) if row_count > 0 else 0

    # Min / max / mean (only for non-string, non-bool types)
    col_min = col_max = col_mean = None
    numeric_types = ("integer", "bigint", "double", "float", "decimal", "numeric", "int")
    date_types    = ("date", "timestamp")

    dtype_lower = dtype.lower()

    if any(t in dtype_lower for t in numeric_types):
        try:
            row = conn.execute(
                f"SELECT MIN(\"{col}\"), MAX(\"{col}\"), AVG(\"{col}\") FROM {table}"
            ).fetchone()
            col_min, col_max, col_mean = row
            if col_mean is not None:
                col_mean = round(float(col_mean), 4)
        except Exception:
            pass
    elif any(t in dtype_lower for t in date_types):
        try:
            row = conn.execute(
                f"SELECT MIN(\"{col}\"), MAX(\"{col}\") FROM {table}"
            ).fetchone()
            col_min, col_max = str(row[0]), str(row[1])
        except Exception:
            pass
    else:
        # String: show example values
        try:
            sample = conn.execute(
                f"SELECT \"{col}\" FROM {table} WHERE \"{col}\" IS NOT NULL LIMIT 1"
            ).fetchone()
            col_min = sample[0] if sample else None
        except Exception:
            pass

    # Flags
    flag_high_nulls = null_pct > (NULL_FLAG_THRESHOLD * 100)
    flag_low_cardinality = (distinct_count > 0 and
                            duplicate_pct > (DUPLICATE_FLAG_THRESHOLD * 100) and
                            "key" not in col.lower() and
                            "flag" not in col.lower())

    return {
        "table":            table,
        "column":           col,
        "data_type":        dtype,
        "row_count":        row_count,
        "null_count":       null_count,
        "null_pct":         null_pct,
        "distinct_count":   distinct_count,
        "duplicate_pct":    duplicate_pct,
        "min":              col_min,
        "max":              col_max,
        "mean":             col_mean,
        "flag_high_nulls":  flag_high_nulls,
        "flag_low_cardinality": flag_low_cardinality,
        "profiled_at":      datetime.utcnow().isoformat(),
    }


def profile_table(conn: duckdb.DuckDBPyConnection, table: str,
                  logger: logging.Logger) -> list:
    """Profile all columns in a table."""
    logger.info(f"  Profiling {table}...")

    row_count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

    # Get column metadata
    col_info = conn.execute(f"DESCRIBE {table}").fetchdf()

    profiles = []
    for _, row in col_info.iterrows():
        col_name = row["column_name"]
        col_type = row["column_type"]
        profile  = profile_column(conn, table, col_name, col_type, row_count)
        profiles.append(profile)

    flagged = sum(1 for p in profiles if p["flag_high_nulls"] or p["flag_low_cardinality"])
    logger.info(f"    {row_count:,} rows × {len(profiles)} cols — {flagged} columns flagged")

    return profiles


# ---------------------------------------------------------------------------
# Main profiling entry point
# ---------------------------------------------------------------------------

def run_profiling(db_path: Path = DB_PATH, output_dir: Path = OUTPUT_DIR,
                  logger: logging.Logger = None) -> pd.DataFrame:
    """Profile all tables in the warehouse. Returns a DataFrame of profiles."""
    if logger is None:
        logger = get_logger("profiling")

    logger.info("=" * 60)
    logger.info("DATA PROFILING — START")
    logger.info(f"Database: {db_path}")
    logger.info("=" * 60)

    conn = duckdb.connect(str(db_path), read_only=True)

    all_profiles = []
    for table in TABLES:
        try:
            profiles = profile_table(conn, table, logger)
            all_profiles.extend(profiles)
        except Exception as e:
            logger.error(f"  Error profiling {table}: {e}")

    conn.close()

    profile_df = pd.DataFrame(all_profiles)

    # Summary
    flagged_nulls = profile_df["flag_high_nulls"].sum()
    flagged_card  = profile_df["flag_low_cardinality"].sum()
    logger.info(f"\nProfile summary:")
    logger.info(f"  Tables profiled:          {len(TABLES)}")
    logger.info(f"  Total columns profiled:   {len(profile_df)}")
    logger.info(f"  Columns with >5% nulls:   {flagged_nulls}")
    logger.info(f"  Low-cardinality columns:  {flagged_card}")

    if flagged_nulls > 0:
        logger.warning("High-null columns (>5%):")
        for _, row in profile_df[profile_df["flag_high_nulls"]].iterrows():
            logger.warning(f"  {row['table']}.{row['column']}: {row['null_pct']:.1f}% nulls")

    # Export
    output_dir.mkdir(parents=True, exist_ok=True)
    profile_path = output_dir / "data_profile.csv"
    profile_df.to_csv(profile_path, index=False)
    logger.info(f"\nData profile saved: {profile_path}")
    logger.info("DATA PROFILING — COMPLETE")

    return profile_df


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DHL Warehouse Data Profiling")
    parser.add_argument("--db-path", type=Path, default=DB_PATH)
    args = parser.parse_args()

    logger = get_logger("profiling")
    profile_df = run_profiling(db_path=args.db_path, logger=logger)
    print(f"\nProfiling complete: {len(profile_df)} columns profiled across {len(TABLES)} tables")
