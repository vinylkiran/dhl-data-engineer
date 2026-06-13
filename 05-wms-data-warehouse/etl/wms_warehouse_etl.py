"""
wms_warehouse_etl.py — WMS Data Warehouse ETL
DHL Data Engineer Portfolio — Project 05

Populates:
  fact_error_log          — one row per error event (accuracy_flag=0 in fact_wms_tasks)
  fact_inventory_accuracy — monthly accuracy snapshots from inventory_snapshot.csv

Uses incremental load via meta_pipeline_runs:
  - Records every run in meta_pipeline_runs (start + end + status)
  - Only processes records newer than the last successful run watermark
  - On first run: processes all data (no watermark)
"""

import logging
import time
import sys
from datetime import datetime
from pathlib import Path
import duckdb
import pandas as pd

BASE_DIR     = Path(__file__).resolve().parent.parent
DB_PATH      = Path("/tmp/dhl_p5.duckdb")
SHARED_DATA  = BASE_DIR.parent / "shared" / "data" / "dhl-synthetic"
OUTPUT_DIR   = BASE_DIR / "outputs"

INVENTORY_FILE = SHARED_DATA / "inventory_snapshot.csv"
SKU_MASTER     = SHARED_DATA / "sku_master.csv"


def get_logger(name="wms_warehouse_etl"):
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
# Pipeline run audit helpers
# ---------------------------------------------------------------------------

def start_run(conn, pipeline_name: str) -> int:
    """Insert a 'running' record and return the run_id."""
    run_start = datetime.utcnow()
    next_id = conn.execute(
        "SELECT COALESCE(MAX(run_id), 0) + 1 FROM meta_pipeline_runs"
    ).fetchone()[0]
    conn.execute("""
        INSERT INTO meta_pipeline_runs
            (run_id, pipeline_name, run_start, status, rows_processed, rows_inserted, rows_updated)
        VALUES (?, ?, ?, 'running', 0, 0, 0)
    """, [next_id, pipeline_name, run_start])
    return next_id


def finish_run(conn, run_id: int, run_start: datetime,
               rows_processed: int, rows_inserted: int, rows_updated: int,
               status: str = "success", error_message: str = None):
    """Update the run record with final stats."""
    run_end = datetime.utcnow()
    duration = (run_end - run_start).total_seconds()
    conn.execute("""
        UPDATE meta_pipeline_runs
        SET run_end          = ?,
            duration_seconds = ?,
            status           = ?,
            rows_processed   = ?,
            rows_inserted    = ?,
            rows_updated     = ?,
            error_message    = ?
        WHERE run_id = ?
    """, [run_end, duration, status, rows_processed, rows_inserted, rows_updated,
          error_message, run_id])


def get_last_successful_watermark(conn, pipeline_name: str):
    """Return the run_end timestamp of the last successful run, or None."""
    row = conn.execute("""
        SELECT MAX(run_end) FROM meta_pipeline_runs
        WHERE pipeline_name = ? AND status = 'success'
    """, [pipeline_name]).fetchone()
    return row[0] if row else None


# ---------------------------------------------------------------------------
# ETL step 1: fact_error_log
# ---------------------------------------------------------------------------

def load_error_log(conn, logger, watermark=None) -> dict:
    """
    Extract error events from fact_wms_tasks (accuracy_flag = FALSE).
    Enrich with SKU category from sku_id prefix and zone from dim_location.
    Incremental: skip task_ids already in fact_error_log.
    """
    logger.info("  Loading fact_error_log...")

    # Get already-loaded task_ids to avoid duplicates
    existing_task_ids = {r[0] for r in conn.execute(
        "SELECT task_id FROM fact_error_log"
    ).fetchall()}
    logger.info(f"    Existing error records: {len(existing_task_ids):,}")

    # Pull error tasks from fact_wms_tasks
    errors_df = conn.execute("""
        SELECT
            t.task_id,
            t.sku_id,
            t.warehouse_id,
            t.operator_surrogate_id  AS operator_id,
            t.task_date,
            t.shift,
            t.task_type,
            t.error_code,
            t.etl_loaded_at
        FROM fact_wms_tasks t
        WHERE t.accuracy_flag = FALSE
          AND t.error_code IS NOT NULL
          AND t.error_code != ''
          AND t.error_code != 'None'
    """).df()

    if len(errors_df) == 0:
        logger.info("    No error tasks found in source")
        return {"rows_processed": 0, "rows_inserted": 0}

    # Incremental filter
    new_errors = errors_df[~errors_df["task_id"].isin(existing_task_ids)].copy()
    logger.info(f"    Source errors: {len(errors_df):,} | New (not yet loaded): {len(new_errors):,}")

    if len(new_errors) == 0:
        logger.info("    No new error records to load")
        return {"rows_processed": len(errors_df), "rows_inserted": 0}

    # Derive category from SKU prefix (e.g. "PHM-001234" → "PHM")
    new_errors["category"] = new_errors["sku_id"].str.split("-").str[0]

    # Try to get zone from dim_location (best-effort — may be NULL if no match)
    # fact_wms_tasks doesn't have location_id, so zone is NULL by default
    new_errors["zone"] = None

    # Build error_context
    def build_context(row):
        return (f"{row['task_type']} error in {row['warehouse_id']} | "
                f"SKU: {row['sku_id']} | Code: {row['error_code']} | "
                f"Shift: {row['shift']}")
    new_errors["error_context"] = new_errors.apply(build_context, axis=1)

    # Assign error_id (sequential from current max)
    current_max = conn.execute(
        "SELECT COALESCE(MAX(error_id), 0) FROM fact_error_log"
    ).fetchone()[0]
    new_errors = new_errors.reset_index(drop=True)
    new_errors["error_id"] = current_max + new_errors.index + 1
    new_errors["etl_loaded_at"] = datetime.utcnow()

    insert_cols = ["error_id", "task_id", "sku_id", "warehouse_id", "operator_id",
                   "task_date", "shift", "task_type", "error_code", "zone",
                   "category", "error_context", "etl_loaded_at"]
    col_list = ", ".join(f'"{c}"' for c in insert_cols)
    conn.register("_error_staging", new_errors[insert_cols])
    conn.execute(f"INSERT INTO fact_error_log ({col_list}) SELECT {col_list} FROM _error_staging")
    conn.unregister("_error_staging")

    inserted = conn.execute("SELECT COUNT(*) FROM fact_error_log").fetchone()[0]
    logger.info(f"    fact_error_log: {inserted:,} total rows ({len(new_errors):,} newly inserted)")
    return {"rows_processed": len(errors_df), "rows_inserted": len(new_errors)}


# ---------------------------------------------------------------------------
# ETL step 2: fact_inventory_accuracy
# ---------------------------------------------------------------------------

def load_inventory_accuracy(conn, logger, watermark=None) -> dict:
    """
    Build monthly inventory accuracy from inventory_snapshot.csv.
    Uses fact_inventory_snapshot if it exists in DB; falls back to CSV.
    Groups by snapshot_date month, warehouse_id, SKU category.
    """
    logger.info("  Loading fact_inventory_accuracy...")

    # Check if fact_inventory_snapshot was loaded by DE Project 1
    has_snapshot_table = conn.execute("""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_name = 'fact_inventory_snapshot'
    """).fetchone()[0] > 0

    if has_snapshot_table:
        logger.info("    Source: fact_inventory_snapshot (DB table, joining dim tables)")
        snap_df = conn.execute("""
            SELECT
                dd.full_date                   AS snapshot_date,
                dw.warehouse_id                AS warehouse_id,
                ds.sku_id                      AS sku_id,
                f.on_hand_qty,
                f.inventory_value              AS unit_cost,
                f.inventory_record_accuracy
            FROM fact_inventory_snapshot f
            JOIN dim_date      dd ON f.date_key      = dd.date_key
            JOIN dim_warehouse dw ON f.warehouse_key = dw.warehouse_key
            JOIN dim_sku       ds ON f.sku_key        = ds.sku_key
        """).df()
    elif INVENTORY_FILE.exists():
        logger.info(f"    Source: {INVENTORY_FILE.name} (CSV)")
        snap_df = pd.read_csv(INVENTORY_FILE, parse_dates=["snapshot_date"])
    else:
        logger.warning("    No inventory source found — skipping fact_inventory_accuracy")
        return {"rows_processed": 0, "rows_inserted": 0}

    logger.info(f"    {len(snap_df):,} inventory snapshot rows loaded")

    # Derive category from SKU prefix
    snap_df["category"] = snap_df["sku_id"].str.split("-").str[0]

    # Create snapshot_month for grouping
    snap_df["snapshot_date"] = pd.to_datetime(snap_df["snapshot_date"])
    snap_df["snapshot_month"] = snap_df["snapshot_date"].dt.to_period("M").dt.to_timestamp()

    # Determine "accurate": use inventory_record_accuracy if available, else on_hand_qty > 0
    if "inventory_record_accuracy" in snap_df.columns:
        snap_df["is_accurate"] = snap_df["inventory_record_accuracy"] >= 1.0
    else:
        snap_df["is_accurate"] = snap_df["on_hand_qty"] > 0
    snap_df["on_hand_value"] = snap_df.get("inventory_value",
                               snap_df["on_hand_qty"] * 1.0).fillna(0)

    # Aggregate to month / warehouse / category
    agg = snap_df.groupby(["snapshot_month", "warehouse_id", "category"]).agg(
        total_skus_counted=("sku_id", "count"),
        accurate_count=("is_accurate", "sum"),
        total_on_hand_value=("on_hand_value", "sum")
    ).reset_index()
    agg["discrepancy_count"] = agg["total_skus_counted"] - agg["accurate_count"]
    agg["accuracy_pct"] = (agg["accurate_count"] / agg["total_skus_counted"] * 100).round(3)
    agg["discrepancy_value"] = (
        agg["total_on_hand_value"] * (1 - agg["accurate_count"] / agg["total_skus_counted"])
    ).round(2)

    # Incremental: skip months already loaded
    existing = set()
    existing_rows = conn.execute("""
        SELECT snapshot_date, warehouse_id, category FROM fact_inventory_accuracy
    """).fetchall()
    for row in existing_rows:
        existing.add((str(row[0])[:7], row[1], row[2]))  # YYYY-MM key

    new_rows = agg[~agg.apply(
        lambda r: (str(r["snapshot_month"])[:7], r["warehouse_id"], r["category"]) in existing,
        axis=1
    )].copy()
    logger.info(f"    Aggregated rows: {len(agg):,} | New: {len(new_rows):,}")

    if len(new_rows) == 0:
        logger.info("    No new inventory accuracy records to load")
        return {"rows_processed": len(snap_df), "rows_inserted": 0}

    current_max = conn.execute(
        "SELECT COALESCE(MAX(accuracy_id), 0) FROM fact_inventory_accuracy"
    ).fetchone()[0]
    new_rows = new_rows.reset_index(drop=True)
    new_rows["accuracy_id"] = current_max + new_rows.index + 1
    new_rows["snapshot_date"] = new_rows["snapshot_month"]
    new_rows["etl_loaded_at"] = datetime.utcnow()

    insert_cols = ["accuracy_id", "snapshot_date", "warehouse_id", "category",
                   "total_skus_counted", "accurate_count", "discrepancy_count",
                   "accuracy_pct", "total_on_hand_value", "discrepancy_value", "etl_loaded_at"]
    col_list = ", ".join(f'"{c}"' for c in insert_cols)
    conn.register("_inv_staging", new_rows[insert_cols])
    conn.execute(f"INSERT INTO fact_inventory_accuracy ({col_list}) SELECT {col_list} FROM _inv_staging")
    conn.unregister("_inv_staging")

    inserted = conn.execute("SELECT COUNT(*) FROM fact_inventory_accuracy").fetchone()[0]
    logger.info(f"    fact_inventory_accuracy: {inserted:,} total rows ({len(new_rows):,} newly inserted)")
    return {"rows_processed": len(snap_df), "rows_inserted": len(new_rows)}


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------

def run_etl(db_path: Path = DB_PATH, logger: logging.Logger = None) -> dict:
    if logger is None:
        logger = get_logger()

    logger.info("=" * 60)
    logger.info("WMS WAREHOUSE ETL — START")
    logger.info("=" * 60)

    t0 = datetime.utcnow()
    conn = duckdb.connect(str(db_path))
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Check watermark for incremental load
    watermark = get_last_successful_watermark(conn, "wms_warehouse_etl")
    if watermark:
        logger.info(f"  Incremental load — watermark: {watermark}")
    else:
        logger.info("  Full load (no prior successful run found)")

    run_id = start_run(conn, "wms_warehouse_etl")

    total_processed = 0
    total_inserted  = 0

    try:
        r1 = load_error_log(conn, logger, watermark=watermark)
        total_processed += r1["rows_processed"]
        total_inserted  += r1["rows_inserted"]

        r2 = load_inventory_accuracy(conn, logger, watermark=watermark)
        total_processed += r2["rows_processed"]
        total_inserted  += r2["rows_inserted"]

        finish_run(conn, run_id, t0,
                   rows_processed=total_processed,
                   rows_inserted=total_inserted,
                   rows_updated=0,
                   status="success")
        logger.info(f"\nETL complete: {total_processed:,} rows processed, {total_inserted:,} inserted")

    except Exception as e:
        finish_run(conn, run_id, t0, 0, 0, 0, status="failed", error_message=str(e))
        conn.close()
        raise

    conn.close()
    logger.info("WMS WAREHOUSE ETL — COMPLETE")
    return {"rows_processed": total_processed, "rows_inserted": total_inserted}


if __name__ == "__main__":
    db = Path(sys.argv[1]) if len(sys.argv) > 1 else DB_PATH
    logger = get_logger()
    stats = run_etl(db_path=db, logger=logger)
    print(f"\nRows processed : {stats['rows_processed']:,}")
    print(f"Rows inserted  : {stats['rows_inserted']:,}")
