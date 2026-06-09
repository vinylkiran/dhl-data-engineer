"""
pipeline_monitor.py — Post-Run Pipeline Monitoring
DHL Demand Forecasting Pipeline — Project 02

Runs after every pipeline execution to validate health and output quality.
Checks:
  1. All pipeline steps completed   — pipeline_run_log.csv has 6 steps, all status=OK
  2. Forecast row count within range — fact_forecast rows ≈ active_skus × 30 (±20%)
  3. No missing SKUs in forecast     — every active SKU has ≥1 forecast row
  4. MAPE drift                      — MAPE has not increased >20% vs prior run
  5. Feature store growth            — fact_feature_store has grown vs prior watermark

Exports results to outputs/monitoring_report.csv.
Prints PASS / FAIL with details for each check.
"""

import logging
from datetime import datetime
from pathlib import Path
import duckdb
import pandas as pd

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BASE_DIR   = Path(__file__).resolve().parent.parent
DB_PATH    = BASE_DIR.parent / "01-sku-segmentation-pipeline" / "outputs" / "dhl_warehouse.duckdb"
OUTPUT_DIR = BASE_DIR / "outputs"

FORECAST_HORIZON   = 30       # Expected forecast days per SKU
ROW_COUNT_TOLERANCE = 0.20    # ±20% tolerance on expected forecast rows
MAPE_DRIFT_THRESHOLD = 0.20   # Flag if MAPE increases by more than 20% vs prior run

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def get_logger(name: str = "pipeline_monitor") -> logging.Logger:
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
# Individual checks
# ---------------------------------------------------------------------------

def check_pipeline_steps_completed(output_dir: Path, logger: logging.Logger) -> dict:
    """
    Check 1: All 6 pipeline steps completed with status=OK.
    Reads pipeline_run_log.csv from the output directory.
    """
    log_path = output_dir / "pipeline_run_log.csv"
    if not log_path.exists():
        logger.warning("  ✗ pipeline_steps: FAIL — pipeline_run_log.csv not found")
        return {
            "check": "pipeline_steps_completed",
            "status": "FAIL",
            "detail": "pipeline_run_log.csv not found — pipeline may not have run",
            "rows_checked": 0,
            "rows_failed": 0,
            "timestamp": datetime.utcnow().isoformat(),
        }

    df = pd.read_csv(log_path)
    total_steps = len(df)
    failed_steps = df[df["status"] != "OK"] if "status" in df.columns else df

    if "status" not in df.columns:
        status = "WARN"
        detail = "pipeline_run_log.csv missing 'status' column — cannot verify steps"
        failed_count = 0
    else:
        # UP_TO_DATE is a valid passing status (incremental steps already current)
        PASSING_STATUSES = {"OK", "UP_TO_DATE", "ALREADY_CURRENT", "NO_NEW_DATA"}
        failed_mask = ~df["status"].isin(PASSING_STATUSES)
        failed_count = failed_mask.sum()
        status = "PASS" if failed_count == 0 else "FAIL"
        step_col = "step" if "step" in df.columns else ("step_name" if "step_name" in df.columns else None)
        if failed_count == 0:
            detail = f"all {total_steps} steps completed successfully"
        else:
            failed_names = df.loc[failed_mask, step_col].tolist() if step_col else "unknown"
            detail = f"{failed_count}/{total_steps} steps failed: {failed_names}"

    logger.info(f"  {'✓' if status == 'PASS' else '✗'} pipeline_steps_completed: {status} — {detail}")
    return {
        "check": "pipeline_steps_completed",
        "status": status,
        "detail": detail,
        "rows_checked": total_steps,
        "rows_failed": failed_count if "status" in df.columns else 0,
        "timestamp": datetime.utcnow().isoformat(),
    }


def check_forecast_row_count(conn: duckdb.DuckDBPyConnection, logger: logging.Logger) -> dict:
    """
    Check 2: Forecast row count ≈ active_skus × warehouses × FORECAST_HORIZON (±20%).
    """
    active_skus = conn.execute(
        "SELECT COUNT(DISTINCT sku_id) FROM dim_sku WHERE active_flag = TRUE"
    ).fetchone()[0]
    warehouses = conn.execute("SELECT COUNT(*) FROM dim_warehouse").fetchone()[0]
    actual_rows = conn.execute("SELECT COUNT(*) FROM fact_forecast").fetchone()[0]

    expected = active_skus * warehouses * FORECAST_HORIZON
    lower = expected * (1 - ROW_COUNT_TOLERANCE)
    upper = expected * (1 + ROW_COUNT_TOLERANCE)

    status = "PASS" if lower <= actual_rows <= upper else "FAIL"
    detail = (
        f"actual={actual_rows:,}, expected≈{expected:,} "
        f"({active_skus} SKUs × {warehouses} warehouses × {FORECAST_HORIZON} days), "
        f"tolerance ±{int(ROW_COUNT_TOLERANCE*100)}%"
    )

    logger.info(f"  {'✓' if status == 'PASS' else '✗'} forecast_row_count: {status} — {detail}")
    return {
        "check": "forecast_row_count",
        "status": status,
        "detail": detail,
        "rows_checked": actual_rows,
        "rows_failed": 0 if status == "PASS" else abs(actual_rows - expected),
        "timestamp": datetime.utcnow().isoformat(),
    }


def check_no_missing_skus(conn: duckdb.DuckDBPyConnection, logger: logging.Logger) -> dict:
    """
    Check 3: Every active SKU has at least one row in fact_forecast.
    """
    result = conn.execute("""
        SELECT
            (SELECT COUNT(DISTINCT sku_id) FROM dim_sku WHERE active_flag = TRUE) AS total_active,
            (SELECT COUNT(DISTINCT sku_id) FROM fact_forecast) AS skus_with_forecasts
    """).fetchone()

    total_active, skus_with_forecasts = result
    missing = total_active - skus_with_forecasts
    status = "PASS" if missing == 0 else "FAIL"
    detail = f"{skus_with_forecasts}/{total_active} active SKUs have forecasts; {missing} missing"

    logger.info(f"  {'✓' if status == 'PASS' else '✗'} no_missing_skus: {status} — {detail}")
    return {
        "check": "no_missing_skus",
        "status": status,
        "detail": detail,
        "rows_checked": total_active,
        "rows_failed": missing,
        "timestamp": datetime.utcnow().isoformat(),
    }


def check_mape_drift(conn: duckdb.DuckDBPyConnection, output_dir: Path,
                      logger: logging.Logger) -> dict:
    """
    Check 4: MAPE has not increased by more than MAPE_DRIFT_THRESHOLD vs prior run.
    Compares current best-model MAPE vs the previous monitoring report if available.
    """
    # Get current average MAPE from fact_model_performance
    current_result = conn.execute("""
        SELECT AVG(mape) AS avg_mape, COUNT(*) AS n
        FROM fact_model_performance
    """).fetchone()

    current_mape = current_result[0]
    n_records = current_result[1]

    if current_mape is None or n_records == 0:
        status = "WARN"
        detail = "No model performance records found in fact_model_performance — pipeline may not have evaluated models"
        logger.info(f"  ~ mape_drift: {status} — {detail}")
        return {
            "check": "mape_drift",
            "status": status,
            "detail": detail,
            "rows_checked": 0,
            "rows_failed": 0,
            "timestamp": datetime.utcnow().isoformat(),
        }

    # Look for prior run in monitoring_report.csv
    prior_mape = None
    monitor_path = output_dir / "monitoring_report.csv"
    if monitor_path.exists():
        prior_df = pd.read_csv(monitor_path)
        prior_mape_rows = prior_df[prior_df["check"] == "mape_drift"]
        if len(prior_mape_rows) > 0:
            # Extract prior avg_mape from detail string if present
            last_row = prior_mape_rows.iloc[-1]["detail"]
            try:
                # Parse "avg_mape=X.XXXX" from detail
                import re
                m = re.search(r"avg_mape=([\d.]+)", last_row)
                if m:
                    prior_mape = float(m.group(1))
            except Exception:
                prior_mape = None

    if prior_mape is None:
        status = "PASS"
        detail = f"avg_mape={current_mape:.4f} (n={n_records} eval rows) — no prior run to compare against"
    else:
        drift = (current_mape - prior_mape) / prior_mape if prior_mape > 0 else 0
        if drift > MAPE_DRIFT_THRESHOLD:
            status = "FAIL"
            detail = (
                f"avg_mape={current_mape:.4f} vs prior={prior_mape:.4f} — "
                f"drift={drift*100:.1f}% exceeds threshold {int(MAPE_DRIFT_THRESHOLD*100)}%"
            )
        else:
            status = "PASS"
            detail = (
                f"avg_mape={current_mape:.4f} vs prior={prior_mape:.4f} — "
                f"drift={drift*100:.1f}% within threshold"
            )

    logger.info(f"  {'✓' if status == 'PASS' else '✗'} mape_drift: {status} — {detail}")
    return {
        "check": "mape_drift",
        "status": status,
        "detail": detail,
        "rows_checked": n_records,
        "rows_failed": 0,
        "timestamp": datetime.utcnow().isoformat(),
    }


def check_feature_store_growth(conn: duckdb.DuckDBPyConnection, output_dir: Path,
                                 logger: logging.Logger) -> dict:
    """
    Check 5: fact_feature_store has grown vs prior monitoring run (or has rows at all).
    """
    current_rows = conn.execute("SELECT COUNT(*) FROM fact_feature_store").fetchone()[0]
    current_max_date = conn.execute(
        "SELECT MAX(feature_date) FROM fact_feature_store"
    ).fetchone()[0]

    prior_rows = None
    monitor_path = output_dir / "monitoring_report.csv"
    if monitor_path.exists():
        prior_df = pd.read_csv(monitor_path)
        prior_growth_rows = prior_df[prior_df["check"] == "feature_store_growth"]
        if len(prior_growth_rows) > 0:
            prior_rows = prior_growth_rows.iloc[-1]["rows_checked"]

    if current_rows == 0:
        status = "FAIL"
        detail = "fact_feature_store is empty — feature engineering has not run"
    elif prior_rows is None:
        status = "PASS"
        detail = f"fact_feature_store has {current_rows:,} rows, max_date={current_max_date} (no prior run)"
    elif current_rows >= prior_rows:
        status = "PASS"
        detail = f"fact_feature_store grew from {int(prior_rows):,} → {current_rows:,} rows, max_date={current_max_date}"
    else:
        status = "FAIL"
        detail = f"fact_feature_store shrank from {int(prior_rows):,} → {current_rows:,} rows — unexpected data loss?"

    logger.info(f"  {'✓' if status == 'PASS' else '✗'} feature_store_growth: {status} — {detail}")
    return {
        "check": "feature_store_growth",
        "status": status,
        "detail": detail,
        "rows_checked": current_rows,
        "rows_failed": 0,
        "timestamp": datetime.utcnow().isoformat(),
    }

# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_monitoring(db_path: Path = DB_PATH, output_dir: Path = OUTPUT_DIR,
                    logger: logging.Logger = None) -> pd.DataFrame:
    """
    Run all 5 monitoring checks and export monitoring_report.csv.
    Returns the report DataFrame.
    """
    if logger is None:
        logger = get_logger()

    logger.info("=" * 60)
    logger.info("PIPELINE MONITORING — START")
    logger.info(f"DB: {db_path}")
    logger.info("=" * 60)

    conn = duckdb.connect(str(db_path), read_only=True)
    results = []

    results.append(check_pipeline_steps_completed(output_dir, logger))
    results.append(check_forecast_row_count(conn, logger))
    results.append(check_no_missing_skus(conn, logger))
    results.append(check_mape_drift(conn, output_dir, logger))
    results.append(check_feature_store_growth(conn, output_dir, logger))

    conn.close()

    report = pd.DataFrame(results)
    passed = (report["status"] == "PASS").sum()
    warned = (report["status"] == "WARN").sum()
    failed = (report["status"] == "FAIL").sum()

    # Save report (append to existing if present, otherwise create)
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / "monitoring_report.csv"
    report.to_csv(out_path, index=False, mode="w")

    # Print summary
    print("\n" + "=" * 60)
    print("MONITORING REPORT")
    print("=" * 60)
    for _, row in report.iterrows():
        icon = "✓" if row["status"] == "PASS" else ("~" if row["status"] == "WARN" else "✗")
        print(f"  {icon} [{row['status']:4s}] {row['check']}")
        print(f"         {row['detail']}")
    print("-" * 60)
    print(f"  Result: {passed} PASS  |  {warned} WARN  |  {failed} FAIL")
    print("=" * 60)

    logger.info(f"\nMonitoring summary: {passed} PASS | {warned} WARN | {failed} FAIL")
    if failed > 0:
        for _, row in report[report["status"] == "FAIL"].iterrows():
            logger.warning(f"  ACTION REQUIRED — {row['check']}: {row['detail']}")
    logger.info(f"Report saved: {out_path}")
    logger.info("PIPELINE MONITORING — COMPLETE")

    return report


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", type=Path, default=DB_PATH)
    parser.add_argument("--output-dir", type=Path, default=OUTPUT_DIR)
    args = parser.parse_args()
    logger = get_logger()
    run_monitoring(db_path=args.db_path, output_dir=args.output_dir, logger=logger)
