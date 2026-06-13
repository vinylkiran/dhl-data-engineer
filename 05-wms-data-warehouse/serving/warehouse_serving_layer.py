"""
warehouse_serving_layer.py — Optimised Serving Layer for BA/DA Dashboard
DHL Data Engineer Portfolio — Project 05

Creates 7 DuckDB views + exports each to CSV:
  v_network_kpis_current_month  — network-level KPIs for current calendar month
  v_warehouse_comparison        — side-by-side KPIs across all warehouses, last 30 days
  v_kpi_trends_12m              — monthly KPI trends, last 12 months
  v_operator_leaderboard        — current month operator ranking within each warehouse
  v_error_patterns              — error code frequency, trend, top affected SKUs and zones
  v_coaching_list               — operators with needs_coaching flag in last 14 days
  v_high_performer_list         — high performers for recognition
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


def get_logger(name="warehouse_serving_layer"):
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


VIEWS = {
    "v_network_kpis_current_month": """
        CREATE OR REPLACE VIEW v_network_kpis_current_month AS
        WITH ref AS (SELECT MAX(kpi_date) AS max_date FROM fact_wms_daily_kpis)
        SELECT
            EXTRACT(YEAR  FROM m.kpi_date)::INTEGER  AS kpi_year,
            EXTRACT(MONTH FROM m.kpi_date)::INTEGER  AS kpi_month,
            SUM(m.total_tasks)                        AS network_total_tasks,
            SUM(m.total_picks)                        AS network_total_picks,
            SUM(m.total_putaways)                     AS network_total_putaways,
            SUM(m.total_errors)                       AS network_total_errors,
            ROUND(
                SUM(m.total_picks * COALESCE(m.pick_accuracy_pct,0)/100.0)
                / NULLIF(SUM(m.total_picks),0) * 100, 3
            )                                         AS network_pick_accuracy_pct,
            ROUND(
                SUM(m.total_tasks * COALESCE(m.overall_accuracy_pct,0)/100.0)
                / NULLIF(SUM(m.total_tasks),0) * 100, 3
            )                                         AS network_overall_accuracy_pct,
            ROUND(AVG(m.avg_pick_duration_min), 3)    AS avg_pick_duration_min,
            ROUND(AVG(m.picks_per_labour_hour), 3)    AS avg_picks_per_labour_hour,
            COUNT(DISTINCT m.warehouse_id)            AS active_warehouses
        FROM fact_wms_daily_kpis m, ref
        WHERE EXTRACT(YEAR  FROM m.kpi_date) = EXTRACT(YEAR  FROM ref.max_date)
          AND EXTRACT(MONTH FROM m.kpi_date) = EXTRACT(MONTH FROM ref.max_date)
        GROUP BY kpi_year, kpi_month
    """,

    "v_warehouse_comparison": """
        CREATE OR REPLACE VIEW v_warehouse_comparison AS
        WITH ref AS (SELECT MAX(kpi_date) AS max_date FROM fact_wms_daily_kpis)
        SELECT
            m.warehouse_id,
            SUM(m.total_tasks)                        AS total_tasks,
            SUM(m.total_picks)                        AS total_picks,
            SUM(m.total_errors)                       AS total_errors,
            ROUND(
                SUM(m.total_picks * COALESCE(m.pick_accuracy_pct,0)/100.0)
                / NULLIF(SUM(m.total_picks),0) * 100, 3
            )                                         AS pick_accuracy_pct,
            ROUND(
                SUM(m.total_tasks * COALESCE(m.overall_accuracy_pct,0)/100.0)
                / NULLIF(SUM(m.total_tasks),0) * 100, 3
            )                                         AS overall_accuracy_pct,
            ROUND(AVG(m.avg_pick_duration_min), 3)    AS avg_pick_duration_min,
            ROUND(AVG(m.avg_putaway_duration_min), 3) AS avg_putaway_duration_min,
            ROUND(AVG(m.picks_per_labour_hour), 3)    AS picks_per_labour_hour,
            COUNT(DISTINCT m.kpi_date)                AS working_days
        FROM fact_wms_daily_kpis m, ref
        WHERE m.kpi_date >= (ref.max_date - INTERVAL 30 DAYS)
        GROUP BY m.warehouse_id
        ORDER BY overall_accuracy_pct DESC
    """,

    "v_kpi_trends_12m": """
        CREATE OR REPLACE VIEW v_kpi_trends_12m AS
        WITH ref AS (SELECT MAX(kpi_date) AS max_date FROM fact_wms_daily_kpis),
             cutoff AS (
                 SELECT DATE_TRUNC('month', max_date - INTERVAL 11 MONTHS) AS start_month
                 FROM ref
             )
        SELECT
            m.kpi_year,
            m.kpi_month,
            m.warehouse_id,
            m.total_tasks,
            m.total_picks,
            m.total_errors,
            m.pick_accuracy_pct,
            m.overall_accuracy_pct,
            m.avg_pick_duration_min,
            m.picks_per_labour_hour,
            m.working_days,
            -- Month-over-month pick accuracy change
            ROUND(
                m.pick_accuracy_pct - LAG(m.pick_accuracy_pct) OVER (
                    PARTITION BY m.warehouse_id ORDER BY m.kpi_year, m.kpi_month
                ), 3
            ) AS pick_accuracy_mom_delta
        FROM fact_wms_monthly_kpis m, cutoff
        WHERE MAKE_DATE(m.kpi_year, m.kpi_month, 1) >= cutoff.start_month
        ORDER BY m.warehouse_id, m.kpi_year, m.kpi_month
    """,

    "v_operator_leaderboard": """
        CREATE OR REPLACE VIEW v_operator_leaderboard AS
        WITH ref AS (SELECT MAX(task_date) AS max_date FROM fact_operator_daily),
             monthly AS (
                 SELECT
                     od.operator_id,
                     od.warehouse_id,
                     SUM(od.tasks_completed)  AS total_tasks,
                     SUM(od.picks_completed)  AS total_picks,
                     SUM(od.error_count)      AS total_errors,
                     ROUND(
                         SUM(od.picks_completed * COALESCE(od.pick_accuracy_pct,0)/100.0)
                         / NULLIF(SUM(od.picks_completed),0) * 100, 3
                     )                        AS pick_accuracy_pct,
                     COUNT(DISTINCT od.task_date || '-' || od.shift) AS shifts_worked
                 FROM fact_operator_daily od
                 CROSS JOIN ref
                 WHERE EXTRACT(YEAR  FROM od.task_date) = EXTRACT(YEAR  FROM ref.max_date)
                   AND EXTRACT(MONTH FROM od.task_date) = EXTRACT(MONTH FROM ref.max_date)
                 GROUP BY od.operator_id, od.warehouse_id
             )
        SELECT
            m.warehouse_id,
            m.operator_id,
            o.hire_date_cohort,
            m.total_tasks,
            m.total_picks,
            m.total_errors,
            m.pick_accuracy_pct,
            m.shifts_worked,
            RANK() OVER (PARTITION BY m.warehouse_id ORDER BY m.pick_accuracy_pct DESC) AS rank_in_warehouse
        FROM monthly m
        LEFT JOIN dim_operator o ON m.operator_id = o.operator_surrogate_id
        WHERE m.total_picks > 0
        ORDER BY m.warehouse_id, rank_in_warehouse
    """,

    "v_error_patterns": """
        CREATE OR REPLACE VIEW v_error_patterns AS
        WITH ref AS (SELECT MAX(task_date) AS max_date FROM fact_error_log),
             recent AS (
                 SELECT warehouse_id, error_code, category,
                        COUNT(*) AS error_count_30d
                 FROM fact_error_log, ref
                 WHERE task_date >= (ref.max_date - INTERVAL 30 DAYS)
                 GROUP BY warehouse_id, error_code, category
             ),
             prior AS (
                 SELECT warehouse_id, error_code,
                        COUNT(*) AS error_count_prior_30d
                 FROM fact_error_log, ref
                 WHERE task_date >= (ref.max_date - INTERVAL 60 DAYS)
                   AND task_date <  (ref.max_date - INTERVAL 30 DAYS)
                 GROUP BY warehouse_id, error_code
             ),
             top_skus AS (
                 SELECT warehouse_id, error_code,
                        STRING_AGG(sku_id, ', ' ORDER BY cnt DESC) AS top_skus
                 FROM (
                     SELECT warehouse_id, error_code, sku_id, COUNT(*) AS cnt,
                            ROW_NUMBER() OVER (PARTITION BY warehouse_id, error_code ORDER BY COUNT(*) DESC) AS rn
                     FROM fact_error_log
                     WHERE sku_id IS NOT NULL
                     GROUP BY warehouse_id, error_code, sku_id
                 ) t WHERE rn <= 3
                 GROUP BY warehouse_id, error_code
             )
        SELECT
            r.warehouse_id,
            r.error_code,
            r.category,
            r.error_count_30d,
            COALESCE(p.error_count_prior_30d, 0)    AS error_count_prior_30d,
            ROUND(
                r.error_count_30d * 1.0
                / NULLIF(COALESCE(p.error_count_prior_30d, 0), 0), 2
            )                                        AS trend_ratio,
            COALESCE(ts.top_skus, 'N/A')             AS top_affected_skus
        FROM recent r
        LEFT JOIN prior p  USING (warehouse_id, error_code)
        LEFT JOIN top_skus ts USING (warehouse_id, error_code)
        ORDER BY r.warehouse_id, r.error_count_30d DESC
    """,

    "v_coaching_list": """
        CREATE OR REPLACE VIEW v_coaching_list AS
        WITH ref AS (SELECT MAX(task_date) AS max_date FROM fact_operator_daily),
             coaching AS (
                 SELECT
                     od.operator_id,
                     od.warehouse_id,
                     COUNT(DISTINCT od.task_date) AS days_flagged,
                     ROUND(AVG(od.pick_accuracy_pct), 3) AS avg_pick_accuracy_pct,
                     SUM(od.error_count) AS total_errors,
                     STRING_AGG(DISTINCT od.top_error_code, ', ') AS error_codes
                 FROM fact_operator_daily od
                 CROSS JOIN ref
                 WHERE od.performance_flag = 'needs_coaching'
                   AND od.task_date >= (ref.max_date - INTERVAL 14 DAYS)
                 GROUP BY od.operator_id, od.warehouse_id
             )
        SELECT
            c.warehouse_id,
            c.operator_id,
            o.hire_date_cohort,
            c.days_flagged,
            c.avg_pick_accuracy_pct,
            c.total_errors,
            c.error_codes,
            'Schedule coaching session — pick accuracy below 98.5% threshold' AS recommended_action
        FROM coaching c
        LEFT JOIN dim_operator o ON c.operator_id = o.operator_surrogate_id
        ORDER BY c.warehouse_id, c.avg_pick_accuracy_pct ASC
    """,

    "v_high_performer_list": """
        CREATE OR REPLACE VIEW v_high_performer_list AS
        WITH ref AS (SELECT MAX(task_date) AS max_date FROM fact_operator_daily),
             perf AS (
                 SELECT
                     od.operator_id,
                     od.warehouse_id,
                     COUNT(DISTINCT od.task_date) AS days_as_high_performer,
                     ROUND(AVG(od.pick_accuracy_pct), 3) AS avg_pick_accuracy_pct,
                     SUM(od.tasks_completed) AS total_tasks
                 FROM fact_operator_daily od
                 CROSS JOIN ref
                 WHERE od.performance_flag = 'high_performer'
                   AND od.task_date >= (ref.max_date - INTERVAL 30 DAYS)
                 GROUP BY od.operator_id, od.warehouse_id
             )
        SELECT
            p.warehouse_id,
            p.operator_id,
            o.hire_date_cohort,
            p.days_as_high_performer,
            p.avg_pick_accuracy_pct,
            p.total_tasks,
            'Eligible for recognition — sustained accuracy above 99.8%' AS recognition_note
        FROM perf p
        LEFT JOIN dim_operator o ON p.operator_id = o.operator_surrogate_id
        WHERE p.days_as_high_performer >= 5
        ORDER BY p.warehouse_id, p.avg_pick_accuracy_pct DESC
    """,
}


def build_serving_layer(db_path: Path = DB_PATH, output_dir: Path = OUTPUT_DIR,
                         logger: logging.Logger = None) -> dict:
    if logger is None:
        logger = get_logger()

    logger.info("=" * 60)
    logger.info("WAREHOUSE SERVING LAYER — START")
    logger.info("=" * 60)

    conn = duckdb.connect(str(db_path))
    output_dir.mkdir(parents=True, exist_ok=True)
    exports = {}

    for view_name, view_sql in VIEWS.items():
        t0 = time.time()
        logger.info(f"  Creating view: {view_name}")
        conn.execute(view_sql)
        df = conn.execute(f"SELECT * FROM {view_name}").df()
        csv_path = output_dir / f"{view_name}.csv"
        df.to_csv(csv_path, index=False)
        exports[view_name] = len(df)
        logger.info(f"    → {len(df):,} rows | {round(time.time()-t0,3)}s | {csv_path.name}")

    conn.close()
    logger.info("WAREHOUSE SERVING LAYER — COMPLETE")
    return exports


if __name__ == "__main__":
    db = Path(sys.argv[1]) if len(sys.argv) > 1 else DB_PATH
    logger = get_logger()
    exports = build_serving_layer(db_path=db, logger=logger)
    for name, rows in exports.items():
        print(f"  {name}: {rows:,} rows")
