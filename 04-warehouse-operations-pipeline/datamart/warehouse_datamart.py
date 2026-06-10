"""
warehouse_datamart.py — Warehouse Manager Self-Service Data Mart
DHL Data Engineer Portfolio — Project 04

Creates 4 DuckDB views and exports to CSV:
  v_daily_kpis            — pick/putaway accuracy, tasks, avg duration (last 30 days)
  v_operator_scorecard    — per-operator accuracy, tasks/shift, error breakdown (current month)
  v_slotting_queue        — pending slotting recommendations ordered by savings
  v_cooccurrence_adjacency — top 50 co-occurrence pairs per warehouse
"""

import logging
from datetime import datetime
from pathlib import Path
import duckdb
import pandas as pd

BASE_DIR   = Path(__file__).resolve().parent.parent
DB_PATH    = BASE_DIR.parent / "01-sku-segmentation-pipeline" / "outputs" / "dhl_warehouse.duckdb"
OUTPUT_DIR = BASE_DIR / "outputs"

def get_logger(name="warehouse_datamart"):
    logger = logging.getLogger(name)
    if not logger.handlers:
        h = logging.StreamHandler()
        h.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                                          datefmt="%Y-%m-%d %H:%M:%S"))
        logger.addHandler(h); logger.setLevel(logging.INFO)
    return logger

VIEWS = {
    "v_daily_kpis": """
        CREATE OR REPLACE VIEW v_daily_kpis AS
        WITH ref AS (SELECT MAX(task_date) AS max_date FROM fact_wms_tasks)
        SELECT
            t.warehouse_id,
            t.task_date,
            t.task_type,
            COUNT(*)                                                    AS tasks_completed,
            ROUND(100.0 * SUM(CASE WHEN t.accuracy_flag THEN 1 ELSE 0 END)
                        / NULLIF(COUNT(*), 0), 2)                       AS accuracy_rate_pct,
            ROUND(AVG(t.duration_min), 2)                              AS avg_duration_min,
            SUM(t.quantity)                                            AS total_quantity
        FROM fact_wms_tasks t, ref
        WHERE t.task_date >= (ref.max_date - INTERVAL 30 DAYS)
        GROUP BY t.warehouse_id, t.task_date, t.task_type
        ORDER BY t.warehouse_id, t.task_date DESC, t.task_type
    """,

    "v_operator_scorecard": """
        CREATE OR REPLACE VIEW v_operator_scorecard AS
        WITH ref AS (SELECT MAX(task_date) AS max_date FROM fact_wms_tasks),
             monthly AS (
                 SELECT t.operator_surrogate_id, t.warehouse_id,
                        t.task_date, t.shift, t.accuracy_flag, t.error_code, t.task_type
                 FROM fact_wms_tasks t, ref
                 WHERE EXTRACT(YEAR  FROM t.task_date) = EXTRACT(YEAR  FROM ref.max_date)
                   AND EXTRACT(MONTH FROM t.task_date) = EXTRACT(MONTH FROM ref.max_date)
             )
        SELECT
            m.operator_surrogate_id,
            o.hire_date_cohort,
            m.warehouse_id,
            COUNT(*)                                                         AS total_tasks,
            COUNT(DISTINCT m.task_date || '-' || m.shift)                   AS shifts_worked,
            ROUND(1.0 * COUNT(*) / NULLIF(COUNT(DISTINCT m.task_date || '-' || m.shift), 0), 1)
                                                                             AS tasks_per_shift,
            ROUND(100.0 * SUM(CASE WHEN m.accuracy_flag THEN 1 ELSE 0 END)
                        / NULLIF(COUNT(*), 0), 2)                           AS accuracy_rate_pct,
            SUM(CASE WHEN NOT m.accuracy_flag THEN 1 ELSE 0 END)           AS error_count,
            COUNT(DISTINCT m.task_type)                                     AS task_types_performed
        FROM monthly m
        LEFT JOIN dim_operator o ON m.operator_surrogate_id = o.operator_surrogate_id
        WHERE m.operator_surrogate_id IS NOT NULL AND m.operator_surrogate_id > 0
        GROUP BY m.operator_surrogate_id, o.hire_date_cohort, m.warehouse_id
        ORDER BY accuracy_rate_pct DESC
    """,

    "v_slotting_queue": """
        CREATE OR REPLACE VIEW v_slotting_queue AS
        SELECT
            slotting_id,
            sku_id,
            warehouse_id,
            recommendation_date,
            prior_zone,
            recommended_zone,
            pick_frequency_at_recommendation,
            ROUND(estimated_daily_minutes_saved, 2)          AS est_daily_minutes_saved,
            ROUND(estimated_daily_minutes_saved * 260, 1)    AS est_annual_minutes_saved,
            implementation_status,
            etl_loaded_at
        FROM fact_slotting_history
        WHERE implementation_status = 'pending'
        ORDER BY estimated_daily_minutes_saved DESC
    """,

    "v_cooccurrence_adjacency": """
        CREATE OR REPLACE VIEW v_cooccurrence_adjacency AS
        WITH ranked AS (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY warehouse_id ORDER BY lift_score DESC) AS rn
            FROM fact_cooccurrence
            WHERE lift_score > 1.0
        )
        SELECT
            warehouse_id,
            sku_id_1,
            sku_id_2,
            co_occurrence_count,
            co_occurrence_window,
            ROUND(lift_score, 3)    AS lift_score,
            'Store in adjacent bays to reduce pick path distance' AS recommendation,
            last_calculated_at
        FROM ranked
        WHERE rn <= 50
        ORDER BY warehouse_id, lift_score DESC
    """,
}


def build_warehouse_datamart(db_path: Path = DB_PATH, output_dir: Path = OUTPUT_DIR,
                              logger: logging.Logger = None) -> dict:
    if logger is None:
        logger = get_logger()
    logger.info("=" * 60)
    logger.info("WAREHOUSE DATA MART — START")
    logger.info("=" * 60)

    conn = duckdb.connect(str(db_path))
    output_dir.mkdir(parents=True, exist_ok=True)
    exports = {}

    for view_name, view_sql in VIEWS.items():
        logger.info(f"  Creating view: {view_name}")
        conn.execute(view_sql)
        df = conn.execute(f"SELECT * FROM {view_name}").df()
        csv_path = output_dir / f"{view_name}.csv"
        df.to_csv(csv_path, index=False)
        exports[view_name] = len(df)
        logger.info(f"    → {len(df):,} rows exported to {csv_path.name}")

    conn.close()
    logger.info("WAREHOUSE DATA MART — COMPLETE")
    return exports


if __name__ == "__main__":
    import sys
    db = Path(sys.argv[1]) if len(sys.argv) > 1 else DB_PATH
    logger = get_logger()
    exports = build_warehouse_datamart(db_path=db, logger=logger)
    for name, rows in exports.items():
        print(f"  {name}: {rows:,} rows")
