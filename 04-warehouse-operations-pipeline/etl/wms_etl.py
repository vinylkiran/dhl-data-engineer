"""
wms_etl.py — WMS Task Ingestion
DHL Data Engineer Portfolio — Project 04

Loads:
  warehouse_locations.csv → dim_location  (SCD Type 2)
  wms_tasks.csv           → fact_wms_tasks (incremental)

Key behaviours:
  - dim_location SCD2: if a location attribute changes, close old record
    (valid_to = now, is_current = False) and insert new row
  - dim_operator: anonymise raw OP-XXXX IDs as surrogate integers;
    assign hire_date_cohort from first task date seen per operator
  - fact_wms_tasks incremental: skip task_ids already loaded
"""

import hashlib
import logging
import time
from datetime import datetime
from pathlib import Path
import duckdb
import pandas as pd
import numpy as np

# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH  = BASE_DIR.parent / "01-sku-segmentation-pipeline" / "outputs" / "dhl_warehouse.duckdb"
DATA_DIR = BASE_DIR.parent.parent / "shared" / "data" / "dhl-synthetic"

def get_logger(name="wms_etl"):
    logger = logging.getLogger(name)
    if not logger.handlers:
        h = logging.StreamHandler()
        h.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                                          datefmt="%Y-%m-%d %H:%M:%S"))
        logger.addHandler(h); logger.setLevel(logging.INFO)
    return logger

# ---------------------------------------------------------------------------
# Step 1: Load dim_location with SCD Type 2
# ---------------------------------------------------------------------------

def load_locations(conn: duckdb.DuckDBPyConnection, data_dir: Path,
                   logger: logging.Logger) -> None:
    path = data_dir / "warehouse_locations.csv"
    logger.info(f"  Reading {path.name}...")
    df = pd.read_csv(path)
    df.columns = [c.lower() for c in df.columns]
    df = df.rename(columns={"active_flag": "active_flag_src"})
    df["active_flag_src"] = df["active_flag_src"].astype(bool)

    now = datetime.utcnow()

    # Get current dim_location state
    existing = conn.execute("""
        SELECT location_id, warehouse_id, zone, aisle, bay, level,
               capacity_units, storage_type, active_flag, location_surrogate_id
        FROM dim_location WHERE is_current = TRUE
    """).df()

    max_surr = conn.execute(
        "SELECT COALESCE(MAX(location_surrogate_id), 0) FROM dim_location"
    ).fetchone()[0]
    surr_counter = int(max_surr)

    inserted = expired = 0

    for _, row in df.iterrows():
        loc_id = row["location_id"]
        match = existing[existing["location_id"] == loc_id]

        attrs = {
            "warehouse_id":  row.get("warehouse_id"),
            "zone":          row.get("zone"),
            "aisle":         row.get("aisle"),
            "bay":           int(row["bay"]) if pd.notna(row.get("bay")) else None,
            "level":         int(row["level"]) if pd.notna(row.get("level")) else None,
            "capacity_units":int(row["capacity_units"]) if pd.notna(row.get("capacity_units")) else None,
            "storage_type":  row.get("storage_type"),
            "active_flag":   bool(row["active_flag_src"]),
        }

        if len(match) == 0:
            # New location — insert
            surr_counter += 1
            conn.execute("""
                INSERT INTO dim_location
                (location_surrogate_id, location_id, warehouse_id, zone, aisle, bay, level,
                 capacity_units, storage_type, active_flag, valid_from, valid_to, is_current, etl_loaded_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,NULL,TRUE,?)
            """, [surr_counter, loc_id, attrs["warehouse_id"], attrs["zone"],
                  attrs["aisle"], attrs["bay"], attrs["level"],
                  attrs["capacity_units"], attrs["storage_type"],
                  attrs["active_flag"], now, now])
            inserted += 1
        else:
            # Check for attribute changes (SCD2 trigger)
            ex = match.iloc[0]
            changed = (
                str(ex["zone"])          != str(attrs["zone"]) or
                str(ex["storage_type"])  != str(attrs["storage_type"]) or
                ex["active_flag"]        != attrs["active_flag"]
            )
            if changed:
                # Expire old record
                conn.execute("""
                    UPDATE dim_location SET valid_to = ?, is_current = FALSE
                    WHERE location_surrogate_id = ?
                """, [now, int(ex["location_surrogate_id"])])
                # Insert new
                surr_counter += 1
                conn.execute("""
                    INSERT INTO dim_location
                    (location_surrogate_id, location_id, warehouse_id, zone, aisle, bay, level,
                     capacity_units, storage_type, active_flag, valid_from, valid_to, is_current, etl_loaded_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,NULL,TRUE,?)
                """, [surr_counter, loc_id, attrs["warehouse_id"], attrs["zone"],
                      attrs["aisle"], attrs["bay"], attrs["level"],
                      attrs["capacity_units"], attrs["storage_type"],
                      attrs["active_flag"], now, now])
                expired += 1; inserted += 1

    total = conn.execute("SELECT COUNT(*) FROM dim_location").fetchone()[0]
    current = conn.execute("SELECT COUNT(*) FROM dim_location WHERE is_current=TRUE").fetchone()[0]
    logger.info(f"  dim_location: {inserted} inserted, {expired} SCD2 expires → {total} total ({current} current)")


# ---------------------------------------------------------------------------
# Step 2: Load dim_operator (anonymise)
# ---------------------------------------------------------------------------

def load_operators(conn: duckdb.DuckDBPyConnection, tasks_df: pd.DataFrame,
                   logger: logging.Logger) -> dict:
    """
    Build dim_operator from unique operator IDs seen in tasks.
    Anonymises raw OP-XXXX with a hash-based surrogate.
    Returns mapping: raw_op_id → operator_surrogate_id
    """
    now = datetime.utcnow()
    existing = {r[0]: r[1] for r in conn.execute(
        "SELECT operator_id, operator_surrogate_id FROM dim_operator"
    ).fetchall()}
    max_surr = conn.execute(
        "SELECT COALESCE(MAX(operator_surrogate_id), 0) FROM dim_operator"
    ).fetchone()[0]
    surr_counter = int(max_surr)

    # Earliest task date per operator for hire_date_cohort
    op_first = tasks_df.groupby("Operator_ID")["Task_Date"].min().to_dict()
    op_warehouse = tasks_df.groupby("Operator_ID")["Warehouse_ID"].first().to_dict()

    new_ops = 0
    op_map = {}

    for raw_id, first_date_str in op_first.items():
        # Anonymise: hash raw_id → stable anonymous identifier
        anon_id = "OP-" + hashlib.sha256(raw_id.encode()).hexdigest()[:8].upper()

        if anon_id not in existing:
            surr_counter += 1
            first_date = pd.to_datetime(first_date_str)
            cohort = f"{first_date.year}-Q{(first_date.month - 1)//3 + 1}"
            conn.execute("""
                INSERT INTO dim_operator
                (operator_surrogate_id, operator_id, warehouse_id, hire_date_cohort, active_flag, etl_loaded_at)
                VALUES (?,?,?,?,TRUE,?)
            """, [surr_counter, anon_id, op_warehouse.get(raw_id), cohort, now])
            existing[anon_id] = surr_counter
            new_ops += 1

        op_map[raw_id] = existing[anon_id]

    total = conn.execute("SELECT COUNT(*) FROM dim_operator").fetchone()[0]
    logger.info(f"  dim_operator: {new_ops} new operators → {total} total")
    return op_map


# ---------------------------------------------------------------------------
# Step 3: Load fact_wms_tasks (incremental)
# ---------------------------------------------------------------------------

def load_wms_tasks(conn: duckdb.DuckDBPyConnection, data_dir: Path,
                   op_map: dict, logger: logging.Logger) -> dict:
    path = data_dir / "wms_tasks.csv"
    logger.info(f"  Reading {path.name}...")
    df = pd.read_csv(path, low_memory=False)

    # Type conversions
    df["Task_Date"]      = pd.to_datetime(df["Task_Date"], errors="coerce").dt.date
    df["Duration_Min"]   = pd.to_numeric(df["Duration_Min"], errors="coerce")
    df["Quantity"]       = pd.to_numeric(df["Quantity"], errors="coerce").fillna(0).astype(int)
    df["Accuracy_Flag"]  = df["Accuracy_Flag"].astype(bool)
    df["Error_Code"]     = df["Error_Code"].astype(str).replace("nan", None)

    # Incremental: skip already-loaded task_ids
    existing_tasks = {r[0] for r in conn.execute("SELECT task_id FROM fact_wms_tasks").fetchall()}
    new_df = df[~df["Task_ID"].isin(existing_tasks)].copy()
    logger.info(f"  New tasks to load: {len(new_df):,} of {len(df):,}")

    if len(new_df) == 0:
        return {"tasks_inserted": 0, "status": "UP_TO_DATE"}

    # Map operators to surrogate IDs
    new_df["operator_surrogate_id"] = new_df["Operator_ID"].map(op_map).fillna(0).astype(int)

    # Build load DataFrame
    load_df = pd.DataFrame({
        "task_id":              new_df["Task_ID"],
        "sku_id":               new_df["SKU_ID"],
        "location_id":          None,            # not in source — populated via location join post-load
        "warehouse_id":         new_df["Warehouse_ID"],
        "operator_surrogate_id":new_df["operator_surrogate_id"],
        "task_date":            new_df["Task_Date"],
        "task_type":            new_df["Task_Type"],
        "shift":                new_df["Shift"],
        "duration_min":         new_df["Duration_Min"],
        "quantity":             new_df["Quantity"],
        "accuracy_flag":        new_df["Accuracy_Flag"],
        "error_code":           new_df["Error_Code"],
        "etl_loaded_at":        datetime.utcnow(),
    })

    cols = list(load_df.columns)
    col_list = ", ".join(f'"{c}"' for c in cols)
    chunk_size = 20_000
    for i in range(0, len(load_df), chunk_size):
        chunk = load_df.iloc[i:i + chunk_size]
        conn.register("_wms_staging", chunk)
        conn.execute(f"INSERT INTO fact_wms_tasks ({col_list}) SELECT {col_list} FROM _wms_staging")
        conn.unregister("_wms_staging")

    total = conn.execute("SELECT COUNT(*) FROM fact_wms_tasks").fetchone()[0]
    logger.info(f"  Inserted {len(load_df):,} tasks → {total:,} total in fact_wms_tasks")
    return {"tasks_inserted": len(load_df), "status": "OK"}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run_wms_etl(db_path: Path = DB_PATH, data_dir: Path = DATA_DIR,
                logger: logging.Logger = None) -> dict:
    if logger is None:
        logger = get_logger()
    t0 = time.time()
    logger.info("=" * 60)
    logger.info("WMS ETL — START")
    logger.info("=" * 60)

    conn = duckdb.connect(str(db_path))
    load_locations(conn, data_dir, logger)

    # Load raw tasks CSV for operator extraction
    tasks_df = pd.read_csv(data_dir / "wms_tasks.csv", low_memory=False,
                           usecols=["Task_ID", "Task_Date", "Operator_ID", "Warehouse_ID"])
    op_map = load_operators(conn, tasks_df, logger)
    stats = load_wms_tasks(conn, data_dir, op_map, logger)

    conn.close()
    stats["duration_s"] = round(time.time() - t0, 2)
    logger.info(f"WMS ETL complete in {stats['duration_s']}s")
    logger.info("WMS ETL — COMPLETE")
    return stats


if __name__ == "__main__":
    import sys
    db = Path(sys.argv[1]) if len(sys.argv) > 1 else DB_PATH
    stats = run_wms_etl(db_path=db)
    print(f"Tasks inserted: {stats.get('tasks_inserted', 0)} — {stats.get('status')}")
