"""
data_quality.py — WMS Data Quality Checks
DHL Data Engineer Portfolio — Project 04

Checks:
  1. Task durations positive and < 120 min (flag outliers)
  2. Accuracy flags are boolean (0 or 1 only)
  3. Error codes only populated when accuracy_flag = False
  4. No duplicate task IDs
  5. All warehouse IDs exist in dim_warehouse
  6. Pick counts per day per warehouse within expected range (50–500)
  7. Operator productivity outliers (>3 std dev from mean tasks/shift)

Exports: outputs/wms_dq_report.csv
"""

import logging
from datetime import datetime
from pathlib import Path
import duckdb
import pandas as pd

BASE_DIR   = Path(__file__).resolve().parent.parent
DB_PATH    = BASE_DIR.parent / "01-sku-segmentation-pipeline" / "outputs" / "dhl_warehouse.duckdb"
OUTPUT_DIR = BASE_DIR / "outputs"

DURATION_MAX_MIN  = 120
PICK_MIN_PER_DAY  = 50
PICK_MAX_PER_DAY  = 500
OUTLIER_STD_DEV   = 3.0

def get_logger(name="wms_data_quality"):
    logger = logging.getLogger(name)
    if not logger.handlers:
        h = logging.StreamHandler()
        h.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                                          datefmt="%Y-%m-%d %H:%M:%S"))
        logger.addHandler(h); logger.setLevel(logging.INFO)
    return logger

def _result(check, status, rows_checked, rows_failed, detail):
    return {"check": check, "status": status, "rows_checked": rows_checked,
            "rows_failed": rows_failed, "detail": detail,
            "timestamp": datetime.utcnow().isoformat()}

def check_duration_bounds(conn, logger):
    total = conn.execute("SELECT COUNT(*) FROM fact_wms_tasks").fetchone()[0]
    bad = conn.execute(f"""
        SELECT COUNT(*) FROM fact_wms_tasks
        WHERE duration_min IS NOT NULL
          AND (duration_min <= 0 OR duration_min > {DURATION_MAX_MIN})
    """).fetchone()[0]
    status = "PASS" if bad == 0 else "WARN"
    detail = f"tasks with duration ≤0 or >{DURATION_MAX_MIN}min: {bad:,} of {total:,}"
    logger.info(f"  {'✓' if status=='PASS' else '~'} duration_bounds: {status} — {detail}")
    return _result("duration_bounds", status, total, bad, detail)

def check_accuracy_flag_values(conn, logger):
    total = conn.execute("SELECT COUNT(*) FROM fact_wms_tasks").fetchone()[0]
    # accuracy_flag is stored as BOOLEAN — check for nulls only
    bad = conn.execute("SELECT COUNT(*) FROM fact_wms_tasks WHERE accuracy_flag IS NULL").fetchone()[0]
    status = "PASS" if bad == 0 else "FAIL"
    detail = f"tasks with null accuracy_flag: {bad:,}"
    logger.info(f"  {'✓' if status=='PASS' else '✗'} accuracy_flag_values: {status} — {detail}")
    return _result("accuracy_flag_values", status, total, bad, detail)

def check_error_code_consistency(conn, logger):
    total = conn.execute("SELECT COUNT(*) FROM fact_wms_tasks").fetchone()[0]
    # error_code should only be populated when accuracy_flag = False
    bad = conn.execute("""
        SELECT COUNT(*) FROM fact_wms_tasks
        WHERE accuracy_flag = TRUE
          AND error_code IS NOT NULL
          AND error_code != ''
          AND error_code != 'None'
    """).fetchone()[0]
    status = "PASS" if bad == 0 else "WARN"
    detail = f"tasks with error_code but accuracy_flag=True: {bad:,}"
    logger.info(f"  {'✓' if status=='PASS' else '~'} error_code_consistency: {status} — {detail}")
    return _result("error_code_consistency", status, total, bad, detail)

def check_no_duplicate_task_ids(conn, logger):
    total = conn.execute("SELECT COUNT(*) FROM fact_wms_tasks").fetchone()[0]
    distinct = conn.execute("SELECT COUNT(DISTINCT task_id) FROM fact_wms_tasks").fetchone()[0]
    dups = total - distinct
    status = "PASS" if dups == 0 else "FAIL"
    detail = f"total={total:,}, distinct={distinct:,}, duplicates={dups:,}"
    logger.info(f"  {'✓' if status=='PASS' else '✗'} no_duplicate_task_ids: {status} — {detail}")
    return _result("no_duplicate_task_ids", status, total, dups, detail)

def check_warehouse_referential_integrity(conn, logger):
    total = conn.execute("SELECT COUNT(*) FROM fact_wms_tasks").fetchone()[0]
    orphans = conn.execute("""
        SELECT COUNT(*) FROM fact_wms_tasks t
        WHERE NOT EXISTS (
            SELECT 1 FROM dim_warehouse dw WHERE dw.warehouse_id = t.warehouse_id
        )
    """).fetchone()[0]
    status = "PASS" if orphans == 0 else "FAIL"
    detail = f"tasks with unknown warehouse_id: {orphans:,} of {total:,}"
    logger.info(f"  {'✓' if status=='PASS' else '✗'} warehouse_ref_integrity: {status} — {detail}")
    return _result("warehouse_ref_integrity", status, total, orphans, detail)

def check_daily_pick_volume(conn, logger):
    daily = conn.execute("""
        SELECT warehouse_id, task_date, COUNT(*) AS pick_count
        FROM fact_wms_tasks
        WHERE task_type = 'Pick'
        GROUP BY warehouse_id, task_date
    """).df()
    if len(daily) == 0:
        return _result("daily_pick_volume", "WARN", 0, 0, "No pick tasks found")
    anomalies = daily[(daily["pick_count"] < PICK_MIN_PER_DAY) | (daily["pick_count"] > PICK_MAX_PER_DAY)]
    status = "PASS" if len(anomalies) == 0 else "WARN"
    detail = (f"{len(anomalies):,} warehouse-day combinations outside expected range "
              f"({PICK_MIN_PER_DAY}–{PICK_MAX_PER_DAY} picks/day) "
              f"of {len(daily):,} total warehouse-days")
    logger.info(f"  {'✓' if status=='PASS' else '~'} daily_pick_volume: {status} — {detail}")
    return _result("daily_pick_volume", status, len(daily), len(anomalies), detail)

def check_operator_productivity(conn, logger):
    op_tasks = conn.execute("""
        SELECT operator_surrogate_id, task_date, shift, COUNT(*) AS tasks
        FROM fact_wms_tasks
        WHERE operator_surrogate_id IS NOT NULL AND operator_surrogate_id > 0
        GROUP BY operator_surrogate_id, task_date, shift
    """).df()
    if len(op_tasks) == 0:
        return _result("operator_productivity", "WARN", 0, 0, "No operator task data found")
    mean = op_tasks["tasks"].mean()
    std  = op_tasks["tasks"].std()
    outliers = op_tasks[abs(op_tasks["tasks"] - mean) > OUTLIER_STD_DEV * std]
    status = "PASS" if len(outliers) == 0 else "WARN"
    detail = (f"operator-shift records >±{OUTLIER_STD_DEV}σ from mean ({mean:.1f}±{std:.1f}): "
              f"{len(outliers):,} of {len(op_tasks):,}")
    logger.info(f"  {'✓' if status=='PASS' else '~'} operator_productivity: {status} — {detail}")
    return _result("operator_productivity", status, len(op_tasks), len(outliers), detail)


def run_data_quality(db_path: Path = DB_PATH, output_dir: Path = OUTPUT_DIR,
                     logger: logging.Logger = None) -> pd.DataFrame:
    if logger is None:
        logger = get_logger()
    logger.info("=" * 60)
    logger.info("WMS DATA QUALITY — START")
    logger.info("=" * 60)

    conn = duckdb.connect(str(db_path), read_only=True)
    results = [
        check_duration_bounds(conn, logger),
        check_accuracy_flag_values(conn, logger),
        check_error_code_consistency(conn, logger),
        check_no_duplicate_task_ids(conn, logger),
        check_warehouse_referential_integrity(conn, logger),
        check_daily_pick_volume(conn, logger),
        check_operator_productivity(conn, logger),
    ]
    conn.close()

    report = pd.DataFrame(results)
    passed = (report["status"] == "PASS").sum()
    warned = (report["status"] == "WARN").sum()
    failed = (report["status"] == "FAIL").sum()
    logger.info(f"\nDQ summary: {passed} PASS | {warned} WARN | {failed} FAIL")

    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / "wms_dq_report.csv"
    report.to_csv(out_path, index=False)
    logger.info(f"DQ report saved: {out_path}")
    logger.info("WMS DATA QUALITY — COMPLETE")
    return report


if __name__ == "__main__":
    import sys
    db = Path(sys.argv[1]) if len(sys.argv) > 1 else DB_PATH
    report = run_data_quality(db_path=db)
    print(f"\n{(report['status']=='PASS').sum()}/{len(report)} checks passed")
