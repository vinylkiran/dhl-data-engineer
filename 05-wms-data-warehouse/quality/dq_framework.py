"""
dq_framework.py — Comprehensive WMS Data Quality Framework
DHL Data Engineer Portfolio — Project 05

Checks organised by severity:

  CRITICAL (pipeline should stop if these fail):
    - No nulls in fact table primary keys
    - No negative quantities
    - No future dates in task_date

  HIGH (log and alert):
    - Accuracy rates between 0 and 100%
    - Error codes only populated when accuracy_flag=False
    - Warehouse IDs exist in dim_warehouse

  MEDIUM (log for review):
    - Task durations between 1 and 120 minutes
    - Picks per labour hour between 1 and 80

  LOW (informational):
    - Operators with zero tasks on a working day
    - SKUs with no picks in 30 days

Exports: outputs/dq_report.csv
Columns: check_name, severity, status, rows_checked, rows_failed,
         failure_pct, sample_failures, checked_at
"""

import logging
import sys
from datetime import datetime
from pathlib import Path
import duckdb
import pandas as pd

BASE_DIR   = Path(__file__).resolve().parent.parent
DB_PATH    = Path("/tmp/dhl_p5.duckdb")
OUTPUT_DIR = BASE_DIR / "outputs"

DURATION_MIN_MIN = 1
DURATION_MAX_MIN = 120
PICKS_PER_HOUR_MIN = 1
PICKS_PER_HOUR_MAX = 80
NO_PICK_DAYS = 30


def get_logger(name="dq_framework"):
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


def _result(check_name, severity, status, rows_checked, rows_failed, sample_failures=""):
    failure_pct = round(rows_failed / rows_checked * 100, 3) if rows_checked > 0 else 0.0
    return {
        "check_name": check_name,
        "severity": severity,
        "status": status,
        "rows_checked": rows_checked,
        "rows_failed": rows_failed,
        "failure_pct": failure_pct,
        "sample_failures": str(sample_failures)[:300] if sample_failures else "",
        "checked_at": datetime.utcnow().isoformat(),
    }


def _log(logger, severity, status, check_name, detail):
    icon = {"PASS": "✓", "FAIL": "✗", "WARN": "~", "INFO": "ℹ"}.get(status, "?")
    logger.info(f"  [{severity[:4]}] {icon} {check_name}: {status} — {detail}")


# ===========================================================================
# CRITICAL CHECKS
# ===========================================================================

def check_no_null_pks(conn, logger):
    """Fact table primary columns must never be NULL."""
    checks = [
        ("fact_wms_tasks",        "task_id"),
        ("fact_wms_daily_kpis",   "kpi_id"),
        ("fact_operator_daily",   "operator_daily_id"),
        ("fact_error_log",        "error_id"),
    ]
    total_checked = 0
    total_failed  = 0
    samples = []

    for table, col in checks:
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        nulls = conn.execute(f"SELECT COUNT(*) FROM {table} WHERE {col} IS NULL").fetchone()[0]
        total_checked += count
        total_failed  += nulls
        if nulls > 0:
            samples.append(f"{table}.{col}: {nulls} NULLs")

    status = "PASS" if total_failed == 0 else "FAIL"
    detail = f"{total_checked:,} rows checked across {len(checks)} tables, {total_failed} NULL PKs"
    _log(logger, "CRITICAL", status, "no_null_primary_keys", detail)
    return _result("no_null_primary_keys", "CRITICAL", status, total_checked, total_failed,
                   "; ".join(samples))


def check_no_negative_quantities(conn, logger):
    """Quantities in task tables must be >= 0."""
    total = conn.execute("SELECT COUNT(*) FROM fact_wms_tasks WHERE quantity IS NOT NULL").fetchone()[0]
    bad   = conn.execute("SELECT COUNT(*) FROM fact_wms_tasks WHERE quantity < 0").fetchone()[0]
    samples = []
    if bad > 0:
        rows = conn.execute(
            "SELECT task_id, sku_id, quantity FROM fact_wms_tasks WHERE quantity < 0 LIMIT 3"
        ).fetchall()
        samples = [f"{r[0]}|qty={r[2]}" for r in rows]

    status = "PASS" if bad == 0 else "FAIL"
    detail = f"Negative quantities: {bad:,} of {total:,}"
    _log(logger, "CRITICAL", status, "no_negative_quantities", detail)
    return _result("no_negative_quantities", "CRITICAL", status, total, bad, "; ".join(samples))


def check_no_future_dates(conn, logger):
    """task_date must not be in the future."""
    total = conn.execute("SELECT COUNT(*) FROM fact_wms_tasks").fetchone()[0]
    bad   = conn.execute(
        "SELECT COUNT(*) FROM fact_wms_tasks WHERE task_date > CURRENT_DATE"
    ).fetchone()[0]
    samples = []
    if bad > 0:
        rows = conn.execute(
            "SELECT task_id, task_date FROM fact_wms_tasks WHERE task_date > CURRENT_DATE LIMIT 3"
        ).fetchall()
        samples = [f"{r[0]}|{r[1]}" for r in rows]

    status = "PASS" if bad == 0 else "FAIL"
    detail = f"Future-dated tasks: {bad:,} of {total:,}"
    _log(logger, "CRITICAL", status, "no_future_dates", detail)
    return _result("no_future_dates", "CRITICAL", status, total, bad, "; ".join(samples))


# ===========================================================================
# HIGH CHECKS
# ===========================================================================

def check_accuracy_rates_valid(conn, logger):
    """pick_accuracy_pct must be between 0 and 100 in daily KPIs."""
    total = conn.execute(
        "SELECT COUNT(*) FROM fact_wms_daily_kpis WHERE pick_accuracy_pct IS NOT NULL"
    ).fetchone()[0]
    bad   = conn.execute("""
        SELECT COUNT(*) FROM fact_wms_daily_kpis
        WHERE pick_accuracy_pct IS NOT NULL
          AND (pick_accuracy_pct < 0 OR pick_accuracy_pct > 100)
    """).fetchone()[0]
    samples = []
    if bad > 0:
        rows = conn.execute("""
            SELECT kpi_date, warehouse_id, pick_accuracy_pct
            FROM fact_wms_daily_kpis
            WHERE pick_accuracy_pct < 0 OR pick_accuracy_pct > 100
            LIMIT 3
        """).fetchall()
        samples = [f"{r[0]}|{r[1]}|{r[2]}" for r in rows]

    status = "PASS" if bad == 0 else "FAIL"
    detail = f"Accuracy out of 0-100 range: {bad:,} of {total:,} daily KPI rows"
    _log(logger, "HIGH", status, "accuracy_rates_valid", detail)
    return _result("accuracy_rates_valid", "HIGH", status, total, bad, "; ".join(samples))


def check_error_codes_with_accurate_tasks(conn, logger):
    """Error codes should only appear when accuracy_flag=False."""
    total = conn.execute("""
        SELECT COUNT(*) FROM fact_wms_tasks
        WHERE accuracy_flag = TRUE
    """).fetchone()[0]
    bad   = conn.execute("""
        SELECT COUNT(*) FROM fact_wms_tasks
        WHERE accuracy_flag = TRUE
          AND error_code IS NOT NULL
          AND error_code != ''
          AND error_code != 'None'
    """).fetchone()[0]
    samples = []
    if bad > 0:
        rows = conn.execute("""
            SELECT task_id, error_code FROM fact_wms_tasks
            WHERE accuracy_flag = TRUE
              AND error_code IS NOT NULL AND error_code != '' AND error_code != 'None'
            LIMIT 3
        """).fetchall()
        samples = [f"{r[0]}|{r[1]}" for r in rows]

    status = "PASS" if bad == 0 else "FAIL"
    detail = f"Accurate tasks with error_code: {bad:,} of {total:,}"
    _log(logger, "HIGH", status, "error_codes_only_on_errors", detail)
    return _result("error_codes_only_on_errors", "HIGH", status, total, bad, "; ".join(samples))


def check_warehouse_referential_integrity(conn, logger):
    """All warehouse_ids in fact tables must exist in dim_warehouse."""
    total = conn.execute("SELECT COUNT(*) FROM fact_wms_tasks").fetchone()[0]
    orphans = conn.execute("""
        SELECT COUNT(*) FROM fact_wms_tasks t
        WHERE NOT EXISTS (
            SELECT 1 FROM dim_warehouse dw WHERE dw.warehouse_id = t.warehouse_id
        )
    """).fetchone()[0]
    samples = []
    if orphans > 0:
        rows = conn.execute("""
            SELECT DISTINCT t.warehouse_id FROM fact_wms_tasks t
            WHERE NOT EXISTS (SELECT 1 FROM dim_warehouse dw WHERE dw.warehouse_id = t.warehouse_id)
            LIMIT 3
        """).fetchall()
        samples = [r[0] for r in rows]

    status = "PASS" if orphans == 0 else "FAIL"
    detail = f"Tasks with unknown warehouse_id: {orphans:,} of {total:,}"
    _log(logger, "HIGH", status, "warehouse_referential_integrity", detail)
    return _result("warehouse_referential_integrity", "HIGH", status, total, orphans,
                   "; ".join(str(s) for s in samples))


# ===========================================================================
# MEDIUM CHECKS
# ===========================================================================

def check_task_duration_bounds(conn, logger):
    """Task durations should be between 1 and 120 minutes."""
    total = conn.execute(
        "SELECT COUNT(*) FROM fact_wms_tasks WHERE duration_min IS NOT NULL"
    ).fetchone()[0]
    bad   = conn.execute(f"""
        SELECT COUNT(*) FROM fact_wms_tasks
        WHERE duration_min IS NOT NULL
          AND (duration_min < {DURATION_MIN_MIN} OR duration_min > {DURATION_MAX_MIN})
    """).fetchone()[0]
    samples = []
    if bad > 0:
        rows = conn.execute(f"""
            SELECT task_id, task_type, duration_min FROM fact_wms_tasks
            WHERE duration_min < {DURATION_MIN_MIN} OR duration_min > {DURATION_MAX_MIN}
            LIMIT 3
        """).fetchall()
        samples = [f"{r[0]}|{r[1]}|{r[2]:.1f}min" for r in rows]

    status = "PASS" if bad == 0 else "WARN"
    detail = (f"Durations outside {DURATION_MIN_MIN}-{DURATION_MAX_MIN}min: "
              f"{bad:,} of {total:,}")
    _log(logger, "MEDIUM", status, "task_duration_bounds", detail)
    return _result("task_duration_bounds", "MEDIUM", status, total, bad, "; ".join(samples))


def check_picks_per_labour_hour(conn, logger):
    """Picks per labour hour in daily KPIs should be between 1 and 80."""
    total = conn.execute(
        "SELECT COUNT(*) FROM fact_wms_daily_kpis WHERE picks_per_labour_hour IS NOT NULL"
    ).fetchone()[0]
    bad   = conn.execute(f"""
        SELECT COUNT(*) FROM fact_wms_daily_kpis
        WHERE picks_per_labour_hour IS NOT NULL
          AND (picks_per_labour_hour < {PICKS_PER_HOUR_MIN}
               OR picks_per_labour_hour > {PICKS_PER_HOUR_MAX})
    """).fetchone()[0]
    samples = []
    if bad > 0:
        rows = conn.execute(f"""
            SELECT kpi_date, warehouse_id, shift, picks_per_labour_hour
            FROM fact_wms_daily_kpis
            WHERE picks_per_labour_hour < {PICKS_PER_HOUR_MIN}
               OR picks_per_labour_hour > {PICKS_PER_HOUR_MAX}
            LIMIT 3
        """).fetchall()
        samples = [f"{r[0]}|{r[1]}|{r[2]}|{r[3]:.1f}" for r in rows]

    status = "PASS" if bad == 0 else "WARN"
    detail = (f"Picks/labour-hour outside {PICKS_PER_HOUR_MIN}-{PICKS_PER_HOUR_MAX}: "
              f"{bad:,} of {total:,}")
    _log(logger, "MEDIUM", status, "picks_per_labour_hour_bounds", detail)
    return _result("picks_per_labour_hour_bounds", "MEDIUM", status, total, bad,
                   "; ".join(samples))


# ===========================================================================
# LOW CHECKS
# ===========================================================================

def check_operators_with_zero_tasks(conn, logger):
    """Operators in dim_operator with zero tasks in the last 30 days (informational)."""
    all_ops = conn.execute(
        "SELECT COUNT(*) FROM dim_operator"
    ).fetchone()[0]
    # Operators active in last 30 days
    active = conn.execute("""
        SELECT COUNT(DISTINCT operator_surrogate_id) FROM fact_wms_tasks
        WHERE task_date >= (SELECT MAX(task_date) - INTERVAL 30 DAYS FROM fact_wms_tasks)
          AND operator_surrogate_id IS NOT NULL
          AND operator_surrogate_id > 0
    """).fetchone()[0]
    inactive = all_ops - active

    status = "INFO"
    detail = (f"dim_operator: {all_ops} operators | Active in last 30 days: {active} | "
              f"Potentially inactive: {inactive}")
    _log(logger, "LOW", status, "operators_with_zero_tasks", detail)
    return _result("operators_with_zero_tasks", "LOW", status, all_ops, inactive, detail)


def check_skus_with_no_picks(conn, logger):
    """SKUs with no pick tasks in the last 30 days (informational — may be slow movers)."""
    all_skus = conn.execute(
        "SELECT COUNT(*) FROM dim_sku WHERE active_flag = TRUE"
    ).fetchone()[0]
    skus_with_picks = conn.execute(f"""
        SELECT COUNT(DISTINCT sku_id) FROM fact_wms_tasks
        WHERE task_type = 'Pick'
          AND task_date >= (SELECT MAX(task_date) - INTERVAL {NO_PICK_DAYS} DAYS FROM fact_wms_tasks)
    """).fetchone()[0]
    no_picks = all_skus - skus_with_picks

    status = "INFO"
    detail = (f"Active SKUs: {all_skus:,} | With picks in last {NO_PICK_DAYS} days: "
              f"{skus_with_picks:,} | No recent picks: {no_picks:,}")
    _log(logger, "LOW", status, "skus_with_no_picks", detail)
    return _result("skus_with_no_picks", "LOW", status, all_skus, no_picks, detail)


# ===========================================================================
# Main
# ===========================================================================

def run_dq_framework(db_path: Path = DB_PATH, output_dir: Path = OUTPUT_DIR,
                     logger: logging.Logger = None) -> pd.DataFrame:
    if logger is None:
        logger = get_logger()

    logger.info("=" * 60)
    logger.info("DQ FRAMEWORK — START")
    logger.info("=" * 60)

    conn = duckdb.connect(str(db_path), read_only=True)

    logger.info("\n--- CRITICAL ---")
    critical = [
        check_no_null_pks(conn, logger),
        check_no_negative_quantities(conn, logger),
        check_no_future_dates(conn, logger),
    ]

    logger.info("\n--- HIGH ---")
    high = [
        check_accuracy_rates_valid(conn, logger),
        check_error_codes_with_accurate_tasks(conn, logger),
        check_warehouse_referential_integrity(conn, logger),
    ]

    logger.info("\n--- MEDIUM ---")
    medium = [
        check_task_duration_bounds(conn, logger),
        check_picks_per_labour_hour(conn, logger),
    ]

    logger.info("\n--- LOW ---")
    low = [
        check_operators_with_zero_tasks(conn, logger),
        check_skus_with_no_picks(conn, logger),
    ]

    conn.close()

    all_results = critical + high + medium + low
    report = pd.DataFrame(all_results)

    by_status = report["status"].value_counts().to_dict()
    logger.info(f"\nDQ Summary: {by_status}")

    critical_fails = report[(report["severity"] == "CRITICAL") & (report["status"] == "FAIL")]
    if len(critical_fails) > 0:
        logger.error(f"  ⚠ {len(critical_fails)} CRITICAL check(s) FAILED — pipeline should halt!")
        for _, row in critical_fails.iterrows():
            logger.error(f"    CRITICAL FAIL: {row['check_name']} — {row['sample_failures']}")

    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / "dq_report.csv"
    report.to_csv(out_path, index=False)
    logger.info(f"\nDQ report saved: {out_path}")
    logger.info("DQ FRAMEWORK — COMPLETE")
    return report


if __name__ == "__main__":
    db = Path(sys.argv[1]) if len(sys.argv) > 1 else DB_PATH
    report = run_dq_framework(db_path=db)
    passed = (report["status"] == "PASS").sum()
    failed = (report["status"] == "FAIL").sum()
    warned = (report["status"] == "WARN").sum()
    infoed = (report["status"] == "INFO").sum()
    print(f"\n{passed} PASS | {warned} WARN | {failed} FAIL | {infoed} INFO "
          f"(out of {len(report)} checks)")
