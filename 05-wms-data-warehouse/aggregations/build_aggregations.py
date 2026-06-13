"""
build_aggregations.py — Pre-aggregated WMS Summary Tables
DHL Data Engineer Portfolio — Project 05

Builds (full rebuild each run — derived from validated source data):
  fact_wms_daily_kpis     — tasks grouped by date / warehouse / shift
  fact_wms_monthly_kpis   — rolled up to year-month / warehouse
  fact_operator_daily     — operator scorecards by date / warehouse / shift

Performance flags:
  pick_accuracy_pct >= 99.8% → high_performer
  pick_accuracy_pct <  98.5% → needs_coaching
  otherwise                  → standard
"""

import logging
import sys
import time
from datetime import datetime
from pathlib import Path
import duckdb
import pandas as pd

BASE_DIR   = Path(__file__).resolve().parent.parent
DB_PATH    = Path("/tmp/dhl_p5.duckdb")
OUTPUT_DIR = BASE_DIR / "outputs"

HIGH_PERFORMER_THRESHOLD  = 99.8
NEEDS_COACHING_THRESHOLD  = 98.5


def get_logger(name="build_aggregations"):
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
# Aggregation 1: fact_wms_daily_kpis
# ---------------------------------------------------------------------------

def build_daily_kpis(conn, logger) -> int:
    t0 = time.time()
    logger.info("  Building fact_wms_daily_kpis...")

    conn.execute("DELETE FROM fact_wms_daily_kpis")

    conn.execute("""
        INSERT INTO fact_wms_daily_kpis (
            kpi_id, kpi_date, warehouse_id, shift,
            total_tasks, total_picks, total_putaways,
            total_replenishments, total_cycle_counts,
            pick_accuracy_pct, putaway_accuracy_pct,
            cycle_count_accuracy_pct, overall_accuracy_pct,
            avg_pick_duration_min, avg_putaway_duration_min,
            picks_per_labour_hour, total_errors, etl_loaded_at
        )
        SELECT
            ROW_NUMBER() OVER (ORDER BY task_date, warehouse_id, shift) AS kpi_id,
            task_date,
            warehouse_id,
            shift,
            COUNT(*)                                                           AS total_tasks,
            SUM(CASE WHEN task_type = 'Pick'         THEN 1 ELSE 0 END)       AS total_picks,
            SUM(CASE WHEN task_type = 'Putaway'      THEN 1 ELSE 0 END)       AS total_putaways,
            SUM(CASE WHEN task_type = 'Replenishment' THEN 1 ELSE 0 END)      AS total_replenishments,
            SUM(CASE WHEN task_type = 'Cycle Count'  THEN 1 ELSE 0 END)       AS total_cycle_counts,
            ROUND(
                100.0 * SUM(CASE WHEN task_type = 'Pick' AND accuracy_flag THEN 1 ELSE 0 END)
                      / NULLIF(SUM(CASE WHEN task_type = 'Pick' THEN 1 ELSE 0 END), 0), 3
            )                                                                  AS pick_accuracy_pct,
            ROUND(
                100.0 * SUM(CASE WHEN task_type = 'Putaway' AND accuracy_flag THEN 1 ELSE 0 END)
                      / NULLIF(SUM(CASE WHEN task_type = 'Putaway' THEN 1 ELSE 0 END), 0), 3
            )                                                                  AS putaway_accuracy_pct,
            ROUND(
                100.0 * SUM(CASE WHEN task_type = 'Cycle Count' AND accuracy_flag THEN 1 ELSE 0 END)
                      / NULLIF(SUM(CASE WHEN task_type = 'Cycle Count' THEN 1 ELSE 0 END), 0), 3
            )                                                                  AS cycle_count_accuracy_pct,
            ROUND(
                100.0 * SUM(CASE WHEN accuracy_flag THEN 1 ELSE 0 END)
                      / NULLIF(COUNT(*), 0), 3
            )                                                                  AS overall_accuracy_pct,
            ROUND(
                AVG(CASE WHEN task_type = 'Pick' THEN duration_min END), 3
            )                                                                  AS avg_pick_duration_min,
            ROUND(
                AVG(CASE WHEN task_type = 'Putaway' THEN duration_min END), 3
            )                                                                  AS avg_putaway_duration_min,
            ROUND(
                SUM(CASE WHEN task_type = 'Pick' THEN 1 ELSE 0 END)
                / NULLIF(
                    SUM(CASE WHEN task_type = 'Pick' THEN COALESCE(duration_min, 0) END) / 60.0,
                    0
                ), 3
            )                                                                  AS picks_per_labour_hour,
            SUM(CASE WHEN NOT accuracy_flag THEN 1 ELSE 0 END)               AS total_errors,
            CURRENT_TIMESTAMP
        FROM fact_wms_tasks
        GROUP BY task_date, warehouse_id, shift
        ORDER BY task_date, warehouse_id, shift
    """)

    count = conn.execute("SELECT COUNT(*) FROM fact_wms_daily_kpis").fetchone()[0]
    logger.info(f"    → {count:,} rows inserted in {round(time.time()-t0, 2)}s")
    return count


# ---------------------------------------------------------------------------
# Aggregation 2: fact_wms_monthly_kpis
# ---------------------------------------------------------------------------

def build_monthly_kpis(conn, logger) -> int:
    t0 = time.time()
    logger.info("  Building fact_wms_monthly_kpis...")

    conn.execute("DELETE FROM fact_wms_monthly_kpis")

    conn.execute("""
        INSERT INTO fact_wms_monthly_kpis (
            monthly_kpi_id, kpi_year, kpi_month, warehouse_id,
            total_tasks, total_picks, total_putaways,
            total_replenishments, total_cycle_counts,
            pick_accuracy_pct, putaway_accuracy_pct,
            cycle_count_accuracy_pct, overall_accuracy_pct,
            avg_pick_duration_min, avg_putaway_duration_min,
            picks_per_labour_hour, total_errors, working_days, etl_loaded_at
        )
        SELECT
            ROW_NUMBER() OVER (ORDER BY kpi_year, kpi_month, warehouse_id) AS monthly_kpi_id,
            kpi_year,
            kpi_month,
            warehouse_id,
            SUM(total_tasks)            AS total_tasks,
            SUM(total_picks)            AS total_picks,
            SUM(total_putaways)         AS total_putaways,
            SUM(total_replenishments)   AS total_replenishments,
            SUM(total_cycle_counts)     AS total_cycle_counts,
            ROUND(
                SUM(total_picks * pick_accuracy_pct / 100.0)
                / NULLIF(SUM(total_picks), 0) * 100, 3
            )                           AS pick_accuracy_pct,
            ROUND(
                SUM(total_putaways * COALESCE(putaway_accuracy_pct, 0) / 100.0)
                / NULLIF(SUM(total_putaways), 0) * 100, 3
            )                           AS putaway_accuracy_pct,
            ROUND(
                SUM(total_cycle_counts * COALESCE(cycle_count_accuracy_pct, 0) / 100.0)
                / NULLIF(SUM(total_cycle_counts), 0) * 100, 3
            )                           AS cycle_count_accuracy_pct,
            ROUND(
                SUM(total_tasks * COALESCE(overall_accuracy_pct, 0) / 100.0)
                / NULLIF(SUM(total_tasks), 0) * 100, 3
            )                           AS overall_accuracy_pct,
            ROUND(AVG(avg_pick_duration_min), 3)    AS avg_pick_duration_min,
            ROUND(AVG(avg_putaway_duration_min), 3) AS avg_putaway_duration_min,
            ROUND(
                SUM(total_picks)
                / NULLIF(SUM(total_picks * COALESCE(avg_pick_duration_min, 0)) / 60.0, 0)
            , 3)                        AS picks_per_labour_hour,
            SUM(total_errors)           AS total_errors,
            COUNT(DISTINCT kpi_date)    AS working_days,
            CURRENT_TIMESTAMP
        FROM (
            SELECT *,
                   EXTRACT(YEAR  FROM kpi_date)::INTEGER AS kpi_year,
                   EXTRACT(MONTH FROM kpi_date)::INTEGER AS kpi_month
            FROM fact_wms_daily_kpis
        ) sub
        GROUP BY kpi_year, kpi_month, warehouse_id
        ORDER BY kpi_year, kpi_month, warehouse_id
    """)

    count = conn.execute("SELECT COUNT(*) FROM fact_wms_monthly_kpis").fetchone()[0]
    logger.info(f"    → {count:,} rows inserted in {round(time.time()-t0, 2)}s")
    return count


# ---------------------------------------------------------------------------
# Aggregation 3: fact_operator_daily
# ---------------------------------------------------------------------------

def build_operator_daily(conn, logger) -> int:
    t0 = time.time()
    logger.info("  Building fact_operator_daily...")

    conn.execute("DELETE FROM fact_operator_daily")

    # Build base aggregation in Python for performance_flag logic
    op_df = conn.execute("""
        SELECT
            operator_surrogate_id  AS operator_id,
            warehouse_id,
            task_date,
            shift,
            COUNT(*)                                                        AS tasks_completed,
            SUM(CASE WHEN task_type = 'Pick' THEN 1 ELSE 0 END)            AS picks_completed,
            ROUND(
                100.0 * SUM(CASE WHEN task_type = 'Pick' AND accuracy_flag THEN 1 ELSE 0 END)
                      / NULLIF(SUM(CASE WHEN task_type = 'Pick' THEN 1 ELSE 0 END), 0), 3
            )                                                               AS pick_accuracy_pct,
            ROUND(AVG(duration_min), 3)                                     AS avg_duration_min,
            SUM(CASE WHEN NOT accuracy_flag THEN 1 ELSE 0 END)             AS error_count
        FROM fact_wms_tasks
        WHERE operator_surrogate_id IS NOT NULL AND operator_surrogate_id > 0
        GROUP BY operator_surrogate_id, warehouse_id, task_date, shift
        ORDER BY operator_surrogate_id, warehouse_id, task_date, shift
    """).df()

    # Top error code per operator-day-shift
    error_mode = conn.execute("""
        SELECT
            operator_surrogate_id  AS operator_id,
            warehouse_id,
            task_date,
            shift,
            error_code,
            COUNT(*) AS cnt,
            ROW_NUMBER() OVER (
                PARTITION BY operator_surrogate_id, warehouse_id, task_date, shift
                ORDER BY COUNT(*) DESC
            ) AS rn
        FROM fact_wms_tasks
        WHERE NOT accuracy_flag
          AND error_code IS NOT NULL
          AND error_code != ''
          AND error_code != 'None'
          AND operator_surrogate_id IS NOT NULL
          AND operator_surrogate_id > 0
        GROUP BY operator_surrogate_id, warehouse_id, task_date, shift, error_code
    """).df()
    top_errors = (error_mode[error_mode["rn"] == 1]
                  [["operator_id", "warehouse_id", "task_date", "shift", "error_code"]]
                  .rename(columns={"error_code": "top_error_code"}))

    op_df = op_df.merge(top_errors,
                        on=["operator_id", "warehouse_id", "task_date", "shift"],
                        how="left")

    # Performance flag
    def perf_flag(acc):
        if acc is None or pd.isna(acc):
            return "standard"
        if acc >= HIGH_PERFORMER_THRESHOLD:
            return "high_performer"
        if acc < NEEDS_COACHING_THRESHOLD:
            return "needs_coaching"
        return "standard"

    op_df["performance_flag"] = op_df["pick_accuracy_pct"].apply(perf_flag)

    # Assign surrogate IDs
    op_df = op_df.reset_index(drop=True)
    op_df["operator_daily_id"] = op_df.index + 1
    op_df["etl_loaded_at"] = datetime.utcnow()

    insert_cols = ["operator_daily_id", "operator_id", "warehouse_id", "task_date",
                   "shift", "tasks_completed", "picks_completed", "pick_accuracy_pct",
                   "avg_duration_min", "error_count", "top_error_code", "performance_flag",
                   "etl_loaded_at"]
    col_list = ", ".join(f'"{c}"' for c in insert_cols)
    conn.register("_op_staging", op_df[insert_cols])
    conn.execute(f"INSERT INTO fact_operator_daily ({col_list}) SELECT {col_list} FROM _op_staging")
    conn.unregister("_op_staging")

    count = conn.execute("SELECT COUNT(*) FROM fact_operator_daily").fetchone()[0]
    logger.info(f"    → {count:,} rows inserted in {round(time.time()-t0, 2)}s")

    # Summary of performance flags
    flags = op_df["performance_flag"].value_counts().to_dict()
    logger.info(f"    Performance flags: {flags}")
    return count


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def build_all_aggregations(db_path: Path = DB_PATH, logger: logging.Logger = None) -> dict:
    if logger is None:
        logger = get_logger()

    logger.info("=" * 60)
    logger.info("BUILD AGGREGATIONS — START")
    logger.info("=" * 60)

    t0 = time.time()
    conn = duckdb.connect(str(db_path))

    daily_count   = build_daily_kpis(conn, logger)
    monthly_count = build_monthly_kpis(conn, logger)
    operator_count = build_operator_daily(conn, logger)

    conn.close()

    elapsed = round(time.time() - t0, 2)
    logger.info(f"\nAggregations complete in {elapsed}s")
    logger.info(f"  fact_wms_daily_kpis   : {daily_count:,} rows")
    logger.info(f"  fact_wms_monthly_kpis : {monthly_count:,} rows")
    logger.info(f"  fact_operator_daily   : {operator_count:,} rows")
    logger.info("BUILD AGGREGATIONS — COMPLETE")

    return {
        "fact_wms_daily_kpis":   daily_count,
        "fact_wms_monthly_kpis": monthly_count,
        "fact_operator_daily":   operator_count,
    }


if __name__ == "__main__":
    db = Path(sys.argv[1]) if len(sys.argv) > 1 else DB_PATH
    logger = get_logger()
    counts = build_all_aggregations(db_path=db, logger=logger)
    for k, v in counts.items():
        print(f"  {k}: {v:,} rows")
