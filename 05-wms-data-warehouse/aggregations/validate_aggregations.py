"""
validate_aggregations.py — Validate Aggregated Tables Against Source Data
DHL Data Engineer Portfolio — Project 05

Checks:
  1. Total task count in fact_wms_daily_kpis equals total tasks in fact_wms_tasks
  2. Pick accuracy in fact_wms_daily_kpis matches recalculation from raw tasks
     for a random sample of 10 dates
  3. No dates are missing from the daily KPI table within the data range
  4. Operator count in fact_operator_daily matches distinct operators in source
  5. Monthly KPI totals match daily KPI totals rolled up

Exports: outputs/aggregation_validation.csv
"""

import logging
import sys
import random
from datetime import datetime
from pathlib import Path
import duckdb
import pandas as pd

BASE_DIR   = Path(__file__).resolve().parent.parent
DB_PATH    = Path("/tmp/dhl_p5.duckdb")
OUTPUT_DIR = BASE_DIR / "outputs"

TOLERANCE  = 0.01   # 0.01% tolerance for floating-point accuracy comparisons


def get_logger(name="validate_aggregations"):
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


def _result(check, status, rows_checked, rows_failed, detail):
    return {
        "check": check,
        "status": status,
        "rows_checked": rows_checked,
        "rows_failed": rows_failed,
        "detail": detail,
        "checked_at": datetime.utcnow().isoformat(),
    }


# ---------------------------------------------------------------------------
# Check 1: Total task count
# ---------------------------------------------------------------------------

def check_total_task_count(conn, logger):
    source_count = conn.execute(
        "SELECT COUNT(*) FROM fact_wms_tasks"
    ).fetchone()[0]
    agg_count = conn.execute(
        "SELECT SUM(total_tasks) FROM fact_wms_daily_kpis"
    ).fetchone()[0] or 0

    match = source_count == agg_count
    status = "PASS" if match else "FAIL"
    detail = (f"fact_wms_tasks={source_count:,} | "
              f"fact_wms_daily_kpis.SUM(total_tasks)={int(agg_count):,}")
    if not match:
        detail += f" | DELTA={source_count - int(agg_count):,}"
    logger.info(f"  {'✓' if match else '✗'} total_task_count: {status} — {detail}")
    return _result("total_task_count", status, source_count, abs(source_count - int(agg_count)), detail)


# ---------------------------------------------------------------------------
# Check 2: Pick accuracy spot check (10 random dates)
# ---------------------------------------------------------------------------

def check_pick_accuracy_sample(conn, logger):
    # Get all distinct dates from daily KPIs
    dates = [r[0] for r in conn.execute(
        "SELECT DISTINCT kpi_date FROM fact_wms_daily_kpis ORDER BY kpi_date"
    ).fetchall()]

    if len(dates) < 2:
        return _result("pick_accuracy_sample", "WARN", 0, 0, "Too few dates to sample")

    sample_dates = random.sample(dates, min(10, len(dates)))
    mismatches = 0
    details = []

    for d in sample_dates:
        # Get stored accuracy for this date (averaged across shifts, weighted)
        stored = conn.execute("""
            SELECT
                ROUND(
                    SUM(total_picks * COALESCE(pick_accuracy_pct, 0) / 100.0)
                    / NULLIF(SUM(total_picks), 0) * 100, 3
                ) AS weighted_accuracy
            FROM fact_wms_daily_kpis
            WHERE kpi_date = ?
        """, [d]).fetchone()[0]

        # Recalculate directly from fact_wms_tasks
        recalc = conn.execute("""
            SELECT
                ROUND(
                    100.0 * SUM(CASE WHEN accuracy_flag THEN 1 ELSE 0 END)
                          / NULLIF(COUNT(*), 0), 3
                )
            FROM fact_wms_tasks
            WHERE task_type = 'Pick' AND task_date = ?
        """, [d]).fetchone()[0]

        if stored is None or recalc is None:
            continue
        diff = abs(float(stored) - float(recalc))
        if diff > TOLERANCE:
            mismatches += 1
            details.append(f"{d}: stored={stored} recalc={recalc} diff={diff:.4f}")

    status = "PASS" if mismatches == 0 else "FAIL"
    detail = (f"Sampled {len(sample_dates)} dates, {mismatches} accuracy mismatches"
              + (f": {'; '.join(details)}" if details else ""))
    logger.info(f"  {'✓' if status=='PASS' else '✗'} pick_accuracy_sample: {status} — {detail}")
    return _result("pick_accuracy_sample", status, len(sample_dates), mismatches, detail)


# ---------------------------------------------------------------------------
# Check 3: No missing dates in daily KPI range
# ---------------------------------------------------------------------------

def check_no_missing_dates(conn, logger):
    row = conn.execute("""
        SELECT MIN(task_date), MAX(task_date) FROM fact_wms_tasks
    """).fetchone()
    min_date, max_date = row[0], row[1]

    if min_date is None:
        return _result("no_missing_dates", "WARN", 0, 0, "No task data found")

    # All distinct dates in source
    source_dates = {r[0] for r in conn.execute(
        "SELECT DISTINCT task_date FROM fact_wms_tasks"
    ).fetchall()}

    # All distinct dates in daily KPIs
    kpi_dates = {r[0] for r in conn.execute(
        "SELECT DISTINCT kpi_date FROM fact_wms_daily_kpis"
    ).fetchall()}

    missing = source_dates - kpi_dates
    extra   = kpi_dates - source_dates

    status = "PASS" if len(missing) == 0 else "FAIL"
    detail = (f"Source dates: {len(source_dates)} | KPI dates: {len(kpi_dates)} | "
              f"Missing from KPI: {len(missing)} | Extra in KPI: {len(extra)}")
    if missing:
        detail += f" | Sample missing: {sorted(missing)[:3]}"
    logger.info(f"  {'✓' if status=='PASS' else '✗'} no_missing_dates: {status} — {detail}")
    return _result("no_missing_dates", status, len(source_dates), len(missing), detail)


# ---------------------------------------------------------------------------
# Check 4: Operator count matches
# ---------------------------------------------------------------------------

def check_operator_count(conn, logger):
    source_ops = conn.execute("""
        SELECT COUNT(DISTINCT operator_surrogate_id) FROM fact_wms_tasks
        WHERE operator_surrogate_id IS NOT NULL AND operator_surrogate_id > 0
    """).fetchone()[0]

    kpi_ops = conn.execute("""
        SELECT COUNT(DISTINCT operator_id) FROM fact_operator_daily
        WHERE operator_id IS NOT NULL AND operator_id > 0
    """).fetchone()[0]

    match = source_ops == kpi_ops
    status = "PASS" if match else "FAIL"
    detail = f"Source distinct operators: {source_ops} | fact_operator_daily: {kpi_ops}"
    logger.info(f"  {'✓' if match else '✗'} operator_count: {status} — {detail}")
    return _result("operator_count", status, source_ops, abs(source_ops - kpi_ops), detail)


# ---------------------------------------------------------------------------
# Check 5: Monthly vs daily totals reconciliation
# ---------------------------------------------------------------------------

def check_monthly_vs_daily_totals(conn, logger):
    daily_total = conn.execute(
        "SELECT SUM(total_tasks) FROM fact_wms_daily_kpis"
    ).fetchone()[0] or 0

    monthly_total = conn.execute(
        "SELECT SUM(total_tasks) FROM fact_wms_monthly_kpis"
    ).fetchone()[0] or 0

    match = int(daily_total) == int(monthly_total)
    status = "PASS" if match else "FAIL"
    detail = (f"Daily total: {int(daily_total):,} | Monthly total: {int(monthly_total):,}"
              + (f" | DELTA={int(daily_total)-int(monthly_total):,}" if not match else ""))
    logger.info(f"  {'✓' if match else '✗'} monthly_vs_daily_totals: {status} — {detail}")
    return _result("monthly_vs_daily_totals", status, int(daily_total),
                   abs(int(daily_total) - int(monthly_total)), detail)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run_validation(db_path: Path = DB_PATH, output_dir: Path = OUTPUT_DIR,
                   logger: logging.Logger = None) -> pd.DataFrame:
    if logger is None:
        logger = get_logger()

    logger.info("=" * 60)
    logger.info("AGGREGATION VALIDATION — START")
    logger.info("=" * 60)

    conn = duckdb.connect(str(db_path))
    results = [
        check_total_task_count(conn, logger),
        check_pick_accuracy_sample(conn, logger),
        check_no_missing_dates(conn, logger),
        check_operator_count(conn, logger),
        check_monthly_vs_daily_totals(conn, logger),
    ]
    conn.close()

    report = pd.DataFrame(results)
    passed = (report["status"] == "PASS").sum()
    warned = (report["status"] == "WARN").sum()
    failed = (report["status"] == "FAIL").sum()
    logger.info(f"\nValidation summary: {passed} PASS | {warned} WARN | {failed} FAIL")

    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / "aggregation_validation.csv"
    report.to_csv(out_path, index=False)
    logger.info(f"Validation report saved: {out_path}")
    logger.info("AGGREGATION VALIDATION — COMPLETE")
    return report


if __name__ == "__main__":
    db = Path(sys.argv[1]) if len(sys.argv) > 1 else DB_PATH
    report = run_validation(db_path=db)
    print(f"\n{(report['status']=='PASS').sum()}/{len(report)} checks passed")
