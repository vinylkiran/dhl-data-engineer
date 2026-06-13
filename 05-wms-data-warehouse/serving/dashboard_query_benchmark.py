"""
dashboard_query_benchmark.py — Serving Layer Performance Benchmark
DHL Data Engineer Portfolio — Project 05

Benchmarks each of the 7 serving views two ways:
  1. Pre-aggregated: query the DuckDB warehouse (fact_wms_daily_kpis,
     fact_wms_monthly_kpis, fact_operator_daily) which has pre-computed results
  2. Raw: equivalent query against flat fact_wms_tasks (original approach)

Calculates speedup factor for each view.
Exports: outputs/query_benchmark.csv
"""

import time
import sys
import logging
from pathlib import Path
from datetime import datetime
import duckdb
import pandas as pd

BASE_DIR   = Path(__file__).resolve().parent.parent
DB_PATH    = Path("/tmp/dhl_p5.duckdb")
OUTPUT_DIR = BASE_DIR / "outputs"
N_RUNS     = 3   # average over 3 runs per query


def get_logger(name="dashboard_query_benchmark"):
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


def _time_query(conn, sql: str, n: int = N_RUNS) -> float:
    """Return average query time in seconds over n runs."""
    times = []
    for _ in range(n):
        t0 = time.perf_counter()
        conn.execute(sql).df()
        times.append(time.perf_counter() - t0)
    return sum(times) / len(times)


# ---------------------------------------------------------------------------
# Benchmark pair definitions
# ---------------------------------------------------------------------------

BENCHMARKS = [
    {
        "view": "v_network_kpis_current_month",
        "description": "Network-level KPIs for current month",
        "pre_agg_sql": "SELECT * FROM v_network_kpis_current_month",
        "raw_sql": """
            WITH ref AS (SELECT MAX(task_date) AS max_date FROM fact_wms_tasks)
            SELECT
                EXTRACT(YEAR  FROM t.task_date)::INTEGER AS kpi_year,
                EXTRACT(MONTH FROM t.task_date)::INTEGER AS kpi_month,
                COUNT(*)                                 AS network_total_tasks,
                SUM(CASE WHEN task_type='Pick' THEN 1 ELSE 0 END) AS network_total_picks,
                SUM(CASE WHEN task_type='Putaway' THEN 1 ELSE 0 END) AS network_total_putaways,
                SUM(CASE WHEN NOT accuracy_flag THEN 1 ELSE 0 END) AS network_total_errors,
                ROUND(100.0*SUM(CASE WHEN task_type='Pick' AND accuracy_flag THEN 1 ELSE 0 END)
                      /NULLIF(SUM(CASE WHEN task_type='Pick' THEN 1 ELSE 0 END),0),3) AS network_pick_accuracy_pct,
                ROUND(100.0*SUM(CASE WHEN accuracy_flag THEN 1 ELSE 0 END)/NULLIF(COUNT(*),0),3) AS overall_accuracy_pct,
                ROUND(AVG(CASE WHEN task_type='Pick' THEN duration_min END),3) AS avg_pick_duration_min,
                COUNT(DISTINCT warehouse_id) AS active_warehouses
            FROM fact_wms_tasks t, ref
            WHERE EXTRACT(YEAR  FROM t.task_date) = EXTRACT(YEAR  FROM ref.max_date)
              AND EXTRACT(MONTH FROM t.task_date) = EXTRACT(MONTH FROM ref.max_date)
            GROUP BY kpi_year, kpi_month
        """,
    },
    {
        "view": "v_warehouse_comparison",
        "description": "KPI comparison across all warehouses, last 30 days",
        "pre_agg_sql": "SELECT * FROM v_warehouse_comparison",
        "raw_sql": """
            WITH ref AS (SELECT MAX(task_date) AS max_date FROM fact_wms_tasks)
            SELECT
                t.warehouse_id,
                COUNT(*) AS total_tasks,
                SUM(CASE WHEN task_type='Pick' THEN 1 ELSE 0 END) AS total_picks,
                SUM(CASE WHEN NOT accuracy_flag THEN 1 ELSE 0 END) AS total_errors,
                ROUND(100.0*SUM(CASE WHEN task_type='Pick' AND accuracy_flag THEN 1 ELSE 0 END)
                      /NULLIF(SUM(CASE WHEN task_type='Pick' THEN 1 ELSE 0 END),0),3) AS pick_accuracy_pct,
                ROUND(100.0*SUM(CASE WHEN accuracy_flag THEN 1 ELSE 0 END)/NULLIF(COUNT(*),0),3) AS overall_accuracy_pct,
                ROUND(AVG(CASE WHEN task_type='Pick' THEN duration_min END),3) AS avg_pick_duration_min
            FROM fact_wms_tasks t, ref
            WHERE t.task_date >= (ref.max_date - INTERVAL 30 DAYS)
            GROUP BY t.warehouse_id ORDER BY overall_accuracy_pct DESC
        """,
    },
    {
        "view": "v_kpi_trends_12m",
        "description": "Monthly KPI trends for last 12 months",
        "pre_agg_sql": "SELECT * FROM v_kpi_trends_12m",
        "raw_sql": """
            WITH ref AS (SELECT MAX(task_date) AS max_date FROM fact_wms_tasks)
            SELECT
                EXTRACT(YEAR  FROM t.task_date)::INTEGER AS kpi_year,
                EXTRACT(MONTH FROM t.task_date)::INTEGER AS kpi_month,
                t.warehouse_id,
                COUNT(*) AS total_tasks,
                SUM(CASE WHEN task_type='Pick' THEN 1 ELSE 0 END) AS total_picks,
                SUM(CASE WHEN NOT accuracy_flag THEN 1 ELSE 0 END) AS total_errors,
                ROUND(100.0*SUM(CASE WHEN task_type='Pick' AND accuracy_flag THEN 1 ELSE 0 END)
                      /NULLIF(SUM(CASE WHEN task_type='Pick' THEN 1 ELSE 0 END),0),3) AS pick_accuracy_pct
            FROM fact_wms_tasks t, ref
            WHERE t.task_date >= DATE_TRUNC('month', ref.max_date - INTERVAL 11 MONTHS)
            GROUP BY kpi_year, kpi_month, t.warehouse_id
            ORDER BY t.warehouse_id, kpi_year, kpi_month
        """,
    },
    {
        "view": "v_operator_leaderboard",
        "description": "Operator ranking by accuracy, current month",
        "pre_agg_sql": "SELECT * FROM v_operator_leaderboard",
        "raw_sql": """
            WITH ref AS (SELECT MAX(task_date) AS max_date FROM fact_wms_tasks)
            SELECT
                t.warehouse_id,
                t.operator_surrogate_id AS operator_id,
                COUNT(*) AS total_tasks,
                SUM(CASE WHEN task_type='Pick' THEN 1 ELSE 0 END) AS total_picks,
                SUM(CASE WHEN NOT accuracy_flag THEN 1 ELSE 0 END) AS total_errors,
                ROUND(100.0*SUM(CASE WHEN task_type='Pick' AND accuracy_flag THEN 1 ELSE 0 END)
                      /NULLIF(SUM(CASE WHEN task_type='Pick' THEN 1 ELSE 0 END),0),3) AS pick_accuracy_pct,
                RANK() OVER (PARTITION BY t.warehouse_id ORDER BY
                    ROUND(100.0*SUM(CASE WHEN task_type='Pick' AND accuracy_flag THEN 1 ELSE 0 END)
                          /NULLIF(SUM(CASE WHEN task_type='Pick' THEN 1 ELSE 0 END),0),3) DESC
                ) AS rank_in_warehouse
            FROM fact_wms_tasks t, ref
            WHERE EXTRACT(YEAR  FROM t.task_date) = EXTRACT(YEAR  FROM ref.max_date)
              AND EXTRACT(MONTH FROM t.task_date) = EXTRACT(MONTH FROM ref.max_date)
              AND t.operator_surrogate_id IS NOT NULL
            GROUP BY t.warehouse_id, t.operator_surrogate_id
            ORDER BY t.warehouse_id, rank_in_warehouse
        """,
    },
    {
        "view": "v_error_patterns",
        "description": "Error code frequency and trends",
        "pre_agg_sql": "SELECT * FROM v_error_patterns",
        "raw_sql": """
            WITH ref AS (SELECT MAX(task_date) AS max_date FROM fact_wms_tasks),
            recent AS (
                SELECT warehouse_id, error_code, COUNT(*) AS cnt_30d
                FROM fact_wms_tasks, ref
                WHERE NOT accuracy_flag
                  AND error_code IS NOT NULL AND error_code != '' AND error_code != 'None'
                  AND task_date >= (ref.max_date - INTERVAL 30 DAYS)
                GROUP BY warehouse_id, error_code
            )
            SELECT warehouse_id, error_code, cnt_30d
            FROM recent
            ORDER BY warehouse_id, cnt_30d DESC
        """,
    },
    {
        "view": "v_coaching_list",
        "description": "Operators needing coaching (last 14 days)",
        "pre_agg_sql": "SELECT * FROM v_coaching_list",
        "raw_sql": """
            WITH ref AS (SELECT MAX(task_date) AS max_date FROM fact_wms_tasks),
            op_acc AS (
                SELECT warehouse_id, operator_surrogate_id,
                       ROUND(100.0*SUM(CASE WHEN task_type='Pick' AND accuracy_flag THEN 1 ELSE 0 END)
                             /NULLIF(SUM(CASE WHEN task_type='Pick' THEN 1 ELSE 0 END),0),3) AS pick_acc
                FROM fact_wms_tasks, ref
                WHERE task_date >= (ref.max_date - INTERVAL 14 DAYS)
                  AND operator_surrogate_id IS NOT NULL
                GROUP BY warehouse_id, operator_surrogate_id
            )
            SELECT warehouse_id, operator_surrogate_id, pick_acc
            FROM op_acc
            WHERE pick_acc < 98.5
            ORDER BY warehouse_id, pick_acc ASC
        """,
    },
    {
        "view": "v_high_performer_list",
        "description": "High performers for recognition (last 30 days)",
        "pre_agg_sql": "SELECT * FROM v_high_performer_list",
        "raw_sql": """
            WITH ref AS (SELECT MAX(task_date) AS max_date FROM fact_wms_tasks),
            op_acc AS (
                SELECT warehouse_id, operator_surrogate_id,
                       ROUND(100.0*SUM(CASE WHEN task_type='Pick' AND accuracy_flag THEN 1 ELSE 0 END)
                             /NULLIF(SUM(CASE WHEN task_type='Pick' THEN 1 ELSE 0 END),0),3) AS pick_acc,
                       COUNT(DISTINCT task_date) AS active_days
                FROM fact_wms_tasks, ref
                WHERE task_date >= (ref.max_date - INTERVAL 30 DAYS)
                  AND operator_surrogate_id IS NOT NULL
                GROUP BY warehouse_id, operator_surrogate_id
            )
            SELECT warehouse_id, operator_surrogate_id, pick_acc, active_days
            FROM op_acc
            WHERE pick_acc >= 99.8 AND active_days >= 5
            ORDER BY warehouse_id, pick_acc DESC
        """,
    },
]


def run_benchmark(db_path: Path = DB_PATH, output_dir: Path = OUTPUT_DIR,
                  logger: logging.Logger = None) -> pd.DataFrame:
    if logger is None:
        logger = get_logger()

    logger.info("=" * 60)
    logger.info(f"QUERY BENCHMARK — START (averaging over {N_RUNS} runs each)")
    logger.info("=" * 60)

    conn = duckdb.connect(str(db_path))
    output_dir.mkdir(parents=True, exist_ok=True)

    results = []
    for b in BENCHMARKS:
        view     = b["view"]
        desc     = b["description"]
        pre_sql  = b["pre_agg_sql"]
        raw_sql  = b["raw_sql"]

        logger.info(f"\n  Benchmarking: {view}")
        logger.info(f"  ({desc})")

        # Pre-aggregated timing
        t_pre = _time_query(conn, pre_sql)
        logger.info(f"    Pre-aggregated : {t_pre*1000:.2f} ms")

        # Raw timing
        t_raw = _time_query(conn, raw_sql)
        logger.info(f"    Raw (flat)     : {t_raw*1000:.2f} ms")

        speedup = round(t_raw / t_pre, 2) if t_pre > 0 else float("inf")
        logger.info(f"    Speedup        : {speedup}x")

        results.append({
            "view":             view,
            "description":      desc,
            "pre_agg_ms":       round(t_pre * 1000, 2),
            "raw_ms":           round(t_raw * 1000, 2),
            "speedup_factor":   speedup,
            "benchmarked_at":   datetime.utcnow().isoformat(),
        })

    conn.close()

    report = pd.DataFrame(results)
    out_path = output_dir / "query_benchmark.csv"
    report.to_csv(out_path, index=False)

    avg_speedup = report["speedup_factor"].mean()
    logger.info(f"\n{'='*60}")
    logger.info(f"BENCHMARK SUMMARY")
    logger.info(f"{'='*60}")
    logger.info(f"  Average speedup across all 7 views: {avg_speedup:.1f}x")
    logger.info(f"  Fastest improvement: {report.loc[report['speedup_factor'].idxmax(), 'view']} "
                f"({report['speedup_factor'].max():.1f}x)")
    logger.info(f"  Benchmark report saved: {out_path}")
    logger.info("QUERY BENCHMARK — COMPLETE")

    return report


if __name__ == "__main__":
    db = Path(sys.argv[1]) if len(sys.argv) > 1 else DB_PATH
    logger = get_logger()
    report = run_benchmark(db_path=db, logger=logger)
    print("\n" + "=" * 60)
    print("BENCHMARK RESULTS")
    print("=" * 60)
    for _, row in report.iterrows():
        print(f"  {row['view'][:40]:<40} "
              f"pre={row['pre_agg_ms']:6.1f}ms  "
              f"raw={row['raw_ms']:6.1f}ms  "
              f"speedup={row['speedup_factor']:5.1f}x")
    print(f"\n  Overall average speedup: {report['speedup_factor'].mean():.1f}x")
