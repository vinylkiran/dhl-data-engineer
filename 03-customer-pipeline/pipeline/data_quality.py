"""
data_quality.py — Customer Pipeline Data Quality Checks
DHL Data Engineer Portfolio — Project 03

Validates:
  1. No duplicate customer_ids in dim_customer
  2. Every customer in fact_rfm_scores exists in dim_customer
  3. All RFM scores are between 1 and 5
  4. No customer appears in both test and control in the same A/B test
  5. Conversion dates are after assignment dates
  6. Segment distribution — Champions must not exceed 15% of customers

Exports results to outputs/customer_dq_report.csv.
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

CHAMPION_MAX_PCT = 15.0  # Champions should not exceed this % of total customers

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def get_logger(name: str = "data_quality") -> logging.Logger:
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

def check_no_duplicate_customers(conn, logger) -> dict:
    total = conn.execute("SELECT COUNT(*) FROM dim_customer").fetchone()[0]
    distinct = conn.execute("SELECT COUNT(DISTINCT customer_id) FROM dim_customer").fetchone()[0]
    dups = total - distinct
    status = "PASS" if dups == 0 else "FAIL"
    detail = f"total={total:,}, distinct={distinct:,}, duplicates={dups}"
    logger.info(f"  {'✓' if status=='PASS' else '✗'} no_duplicate_customers: {status} — {detail}")
    return {"check": "no_duplicate_customers", "status": status,
            "rows_checked": total, "rows_failed": dups,
            "detail": detail, "timestamp": datetime.utcnow().isoformat()}


def check_rfm_referential_integrity(conn, logger) -> dict:
    orphans = conn.execute("""
        SELECT COUNT(*) FROM fact_rfm_scores rf
        WHERE NOT EXISTS (
            SELECT 1 FROM dim_customer dc WHERE dc.customer_id = rf.customer_id
        )
    """).fetchone()[0]
    total = conn.execute("SELECT COUNT(*) FROM fact_rfm_scores").fetchone()[0]
    status = "PASS" if orphans == 0 else "FAIL"
    detail = f"rfm rows with no matching customer: {orphans} of {total:,}"
    logger.info(f"  {'✓' if status=='PASS' else '✗'} rfm_referential_integrity: {status} — {detail}")
    return {"check": "rfm_referential_integrity", "status": status,
            "rows_checked": total, "rows_failed": orphans,
            "detail": detail, "timestamp": datetime.utcnow().isoformat()}


def check_rfm_score_bounds(conn, logger) -> dict:
    total = conn.execute("SELECT COUNT(*) FROM fact_rfm_scores").fetchone()[0]
    out_of_bounds = conn.execute("""
        SELECT COUNT(*) FROM fact_rfm_scores
        WHERE recency_score   NOT BETWEEN 1 AND 5
           OR frequency_score NOT BETWEEN 1 AND 5
           OR monetary_score  NOT BETWEEN 1 AND 5
    """).fetchone()[0]
    status = "PASS" if out_of_bounds == 0 else "FAIL"
    detail = f"rows with score outside 1-5: {out_of_bounds} of {total:,}"
    logger.info(f"  {'✓' if status=='PASS' else '✗'} rfm_score_bounds: {status} — {detail}")
    return {"check": "rfm_score_bounds", "status": status,
            "rows_checked": total, "rows_failed": out_of_bounds,
            "detail": detail, "timestamp": datetime.utcnow().isoformat()}


def check_no_ab_test_contamination(conn, logger) -> dict:
    total_assignments = conn.execute("SELECT COUNT(*) FROM fact_ab_assignments").fetchone()[0]
    contaminated = conn.execute("""
        SELECT COUNT(*) FROM (
            SELECT customer_id, test_name, COUNT(DISTINCT test_group) AS n_groups
            FROM fact_ab_assignments
            GROUP BY customer_id, test_name
            HAVING n_groups > 1
        )
    """).fetchone()[0]
    status = "PASS" if contaminated == 0 else "FAIL"
    detail = f"customers in both test and control for same test: {contaminated}"
    logger.info(f"  {'✓' if status=='PASS' else '✗'} no_ab_test_contamination: {status} — {detail}")
    return {"check": "no_ab_test_contamination", "status": status,
            "rows_checked": total_assignments, "rows_failed": contaminated,
            "detail": detail, "timestamp": datetime.utcnow().isoformat()}


def check_conversion_date_order(conn, logger) -> dict:
    total = conn.execute(
        "SELECT COUNT(*) FROM fact_ab_assignments WHERE converted_flag = TRUE AND conversion_date IS NOT NULL"
    ).fetchone()[0]
    bad = conn.execute("""
        SELECT COUNT(*) FROM fact_ab_assignments
        WHERE converted_flag = TRUE
          AND conversion_date IS NOT NULL
          AND CAST(assigned_at AS DATE) >= conversion_date
    """).fetchone()[0]
    status = "PASS" if bad == 0 else "FAIL"
    detail = f"conversions before assignment date: {bad} of {total:,} converted customers"
    logger.info(f"  {'✓' if status=='PASS' else '✗'} conversion_date_order: {status} — {detail}")
    return {"check": "conversion_date_order", "status": status,
            "rows_checked": total, "rows_failed": bad,
            "detail": detail, "timestamp": datetime.utcnow().isoformat()}


def check_segment_distribution(conn, logger) -> dict:
    total = conn.execute(
        "SELECT COUNT(*) FROM dim_customer WHERE active_flag = TRUE AND current_rfm_segment IS NOT NULL"
    ).fetchone()[0]
    if total == 0:
        return {"check": "segment_distribution", "status": "WARN",
                "rows_checked": 0, "rows_failed": 0,
                "detail": "No customers scored yet",
                "timestamp": datetime.utcnow().isoformat()}

    champion_count = conn.execute(
        "SELECT COUNT(*) FROM dim_customer WHERE active_flag = TRUE AND current_rfm_segment = 'Champions'"
    ).fetchone()[0]
    champion_pct = champion_count / total * 100
    status = "PASS" if champion_pct <= CHAMPION_MAX_PCT else "FAIL"
    detail = (
        f"Champions={champion_count:,} ({champion_pct:.1f}% of {total:,}) "
        f"— threshold ≤{CHAMPION_MAX_PCT:.0f}%"
    )
    logger.info(f"  {'✓' if status=='PASS' else '✗'} segment_distribution: {status} — {detail}")
    return {"check": "segment_distribution", "status": status,
            "rows_checked": total, "rows_failed": 0 if status == "PASS" else champion_count,
            "detail": detail, "timestamp": datetime.utcnow().isoformat()}


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_data_quality(db_path: Path = DB_PATH, output_dir: Path = OUTPUT_DIR,
                     logger: logging.Logger = None) -> pd.DataFrame:
    if logger is None:
        logger = get_logger()

    logger.info("=" * 60)
    logger.info("CUSTOMER DATA QUALITY — START")
    logger.info(f"DB: {db_path}")
    logger.info("=" * 60)

    conn = duckdb.connect(str(db_path), read_only=True)
    results = []

    results.append(check_no_duplicate_customers(conn, logger))
    results.append(check_rfm_referential_integrity(conn, logger))
    results.append(check_rfm_score_bounds(conn, logger))
    results.append(check_no_ab_test_contamination(conn, logger))
    results.append(check_conversion_date_order(conn, logger))
    results.append(check_segment_distribution(conn, logger))

    conn.close()

    report = pd.DataFrame(results)
    passed = (report["status"] == "PASS").sum()
    failed = (report["status"] == "FAIL").sum()
    warned = (report["status"] == "WARN").sum()

    logger.info(f"\nDQ summary: {passed}/{len(report)} checks passed | {warned} WARN | {failed} FAIL")
    if failed > 0:
        for _, row in report[report["status"] == "FAIL"].iterrows():
            logger.warning(f"  ACTION REQUIRED — {row['check']}: {row['detail']}")

    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / "customer_dq_report.csv"
    report.to_csv(out_path, index=False)
    logger.info(f"DQ report saved: {out_path}")
    logger.info("CUSTOMER DATA QUALITY — COMPLETE")

    return report


if __name__ == "__main__":
    import sys
    db = Path(sys.argv[1]) if len(sys.argv) > 1 else DB_PATH
    logger = get_logger()
    report = run_data_quality(db_path=db, logger=logger)
    print(f"\n{(report['status']=='PASS').sum()}/{len(report)} checks passed")
