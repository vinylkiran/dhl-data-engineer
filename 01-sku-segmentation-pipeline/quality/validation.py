"""
validation.py — Data Quality Validation Framework
DHL SKU Segmentation Pipeline — Project 01

Runs 8 validation checks against the loaded DuckDB warehouse.
Exports results to outputs/validation_report.csv.

Checks:
  1. Null check            — no nulls in PK columns
  2. Referential integrity — every FK exists in its dimension
  3. Duplicate PK check    — no duplicate PKs in dimension tables
  4. Date range check      — all dates in fact tables within 2022-2023
  5. Revenue sanity        — no negative revenue
  6. Quantity sanity       — no negative quantities
  7. Stockout consistency  — when stockout=1, fulfilled < demanded
  8. Completeness          — fact row count matches source CSV

Usage:
    python validation.py
    python validation.py --db-path /custom/path/dhl_warehouse.duckdb
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path
import duckdb
import pandas as pd

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BASE_DIR    = Path(__file__).resolve().parent.parent
DB_PATH     = BASE_DIR / "outputs" / "dhl_warehouse.duckdb"
OUTPUT_DIR  = BASE_DIR / "outputs"
DATA_DIR    = BASE_DIR.parent.parent / "shared" / "data" / "dhl-synthetic"

DATE_MIN = "2022-01-01"
DATE_MAX = "2023-12-31"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def get_logger(name: str = "validation") -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        ))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger

# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------

def make_result(check_name: str, status: str, rows_checked: int,
                rows_failed: int, detail: str = "") -> dict:
    failure_pct = round(rows_failed / rows_checked * 100, 4) if rows_checked > 0 else 0.0
    return {
        "check_name":   check_name,
        "status":       status,      # PASS / FAIL / ERROR
        "rows_checked": rows_checked,
        "rows_failed":  rows_failed,
        "failure_pct":  failure_pct,
        "detail":       detail,
        "timestamp":    datetime.utcnow().isoformat(),
    }


def safe_check(check_name: str, fn, logger: logging.Logger) -> dict:
    """Run a check function, catching any exceptions as ERRORs."""
    try:
        result = fn()
        icon = "✓" if result["status"] == "PASS" else "✗"
        logger.info(
            f"  {icon} {check_name}: {result['status']} "
            f"({result['rows_failed']:,}/{result['rows_checked']:,} failed)"
            + (f" — {result['detail']}" if result['detail'] else "")
        )
        return result
    except Exception as e:
        logger.error(f"  ✗ {check_name}: ERROR — {e}")
        return make_result(check_name, "ERROR", 0, 0, str(e))

# ---------------------------------------------------------------------------
# Check functions
# ---------------------------------------------------------------------------

def check_null_pks(conn: duckdb.DuckDBPyConnection) -> list:
    """Check 1: No nulls in primary key columns."""
    pk_cols = {
        "dim_date":               "date_key",
        "dim_warehouse":          "warehouse_key",
        "dim_supplier":           "supplier_key",
        "dim_sku":                "sku_key",
        "fact_daily_demand":      "demand_key",
        "fact_inventory_snapshot":"snapshot_key",
    }
    results = []
    for table, pk in pk_cols.items():
        def _check(t=table, p=pk):
            total = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            nulls = conn.execute(f"SELECT COUNT(*) FROM {t} WHERE {p} IS NULL").fetchone()[0]
            status = "PASS" if nulls == 0 else "FAIL"
            return make_result(f"null_pk_{t}", status, total, nulls,
                               f"table={t}, pk={p}")
        results.append(safe_check(f"null_pk_{table}", _check, logging.getLogger("validation")))
    return results


def check_referential_integrity(conn: duckdb.DuckDBPyConnection) -> list:
    """Check 2: Every FK in fact tables exists in its dimension."""
    fk_checks = [
        ("fact_daily_demand",      "date_key",      "dim_date",      "date_key"),
        ("fact_daily_demand",      "sku_key",       "dim_sku",       "sku_key"),
        ("fact_daily_demand",      "warehouse_key", "dim_warehouse", "warehouse_key"),
        ("fact_inventory_snapshot","date_key",      "dim_date",      "date_key"),
        ("fact_inventory_snapshot","sku_key",       "dim_sku",       "sku_key"),
        ("fact_inventory_snapshot","warehouse_key", "dim_warehouse", "warehouse_key"),
    ]
    results = []
    for fact_t, fk_col, dim_t, pk_col in fk_checks:
        def _check(ft=fact_t, fk=fk_col, dt=dim_t, pk=pk_col):
            total = conn.execute(f"SELECT COUNT(*) FROM {ft}").fetchone()[0]
            orphans = conn.execute(f"""
                SELECT COUNT(*) FROM {ft} f
                LEFT JOIN {dt} d ON f.{fk} = d.{pk}
                WHERE d.{pk} IS NULL AND f.{fk} IS NOT NULL
            """).fetchone()[0]
            status = "PASS" if orphans == 0 else "FAIL"
            return make_result(f"ri_{ft}_{fk}", status, total, orphans,
                               f"{ft}.{fk} → {dt}.{pk}")
        results.append(safe_check(f"ri_{fact_t}_{fk_col}", _check, logging.getLogger("validation")))
    return results


def check_duplicate_pks(conn: duckdb.DuckDBPyConnection) -> list:
    """Check 3: No duplicate PKs in dimension tables."""
    dim_pks = {
        "dim_date":      "date_key",
        "dim_warehouse": "warehouse_key",
        "dim_supplier":  "supplier_key",
        "dim_sku":       "sku_key",
    }
    results = []
    for table, pk in dim_pks.items():
        def _check(t=table, p=pk):
            total = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            dups = conn.execute(f"""
                SELECT COUNT(*) FROM (
                    SELECT {p}, COUNT(*) c FROM {t}
                    GROUP BY {p} HAVING c > 1
                )
            """).fetchone()[0]
            status = "PASS" if dups == 0 else "FAIL"
            return make_result(f"dup_pk_{t}", status, total, dups,
                               f"table={t}, pk={p}")
        results.append(safe_check(f"dup_pk_{table}", _check, logging.getLogger("validation")))
    return results


def check_date_range(conn: duckdb.DuckDBPyConnection) -> list:
    """Check 4: All dates in fact tables within 2022-01-01 to 2023-12-31."""
    results = []
    for table in ["fact_daily_demand", "fact_inventory_snapshot"]:
        def _check(t=table):
            total = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            out_of_range = conn.execute(f"""
                SELECT COUNT(*) FROM {t} f
                JOIN dim_date d ON f.date_key = d.date_key
                WHERE d.full_date < '{DATE_MIN}' OR d.full_date > '{DATE_MAX}'
            """).fetchone()[0]
            status = "PASS" if out_of_range == 0 else "FAIL"
            return make_result(f"date_range_{t}", status, total, out_of_range,
                               f"expected {DATE_MIN} to {DATE_MAX}")
        results.append(safe_check(f"date_range_{table}", _check, logging.getLogger("validation")))
    return results


def check_revenue_sanity(conn: duckdb.DuckDBPyConnection) -> dict:
    """Check 5: No negative revenue values."""
    def _check():
        total = conn.execute("SELECT COUNT(*) FROM fact_daily_demand").fetchone()[0]
        neg   = conn.execute("SELECT COUNT(*) FROM fact_daily_demand WHERE revenue < 0").fetchone()[0]
        status = "PASS" if neg == 0 else "FAIL"
        return make_result("revenue_non_negative", status, total, neg)
    return safe_check("revenue_non_negative", _check, logging.getLogger("validation"))


def check_quantity_sanity(conn: duckdb.DuckDBPyConnection) -> list:
    """Check 6: No negative quantities in fact tables."""
    qty_checks = [
        ("fact_daily_demand",      "quantity_demanded"),
        ("fact_daily_demand",      "quantity_fulfilled"),
        ("fact_inventory_snapshot","on_hand_qty"),
        ("fact_inventory_snapshot","available_qty"),
    ]
    results = []
    for table, col in qty_checks:
        def _check(t=table, c=col):
            total = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            neg   = conn.execute(f"SELECT COUNT(*) FROM {t} WHERE {c} < 0").fetchone()[0]
            status = "PASS" if neg == 0 else "FAIL"
            return make_result(f"qty_non_negative_{t}_{c}", status, total, neg,
                               f"table={t}, col={c}")
        results.append(safe_check(f"qty_non_negative_{table}_{col}", _check, logging.getLogger("validation")))
    return results


def check_stockout_consistency(conn: duckdb.DuckDBPyConnection) -> dict:
    """Check 7: When stockout_flag=true, quantity_fulfilled <= quantity_demanded."""
    def _check():
        total = conn.execute(
            "SELECT COUNT(*) FROM fact_daily_demand WHERE stockout_flag = TRUE"
        ).fetchone()[0]
        violations = conn.execute("""
            SELECT COUNT(*) FROM fact_daily_demand
            WHERE stockout_flag = TRUE
              AND quantity_fulfilled > quantity_demanded
        """).fetchone()[0]
        status = "PASS" if violations == 0 else "FAIL"
        return make_result("stockout_consistency", status, total, violations,
                           "stockout=TRUE but fulfilled > demanded")
    return safe_check("stockout_consistency", _check, logging.getLogger("validation"))


def check_completeness(conn: duckdb.DuckDBPyConnection,
                       data_dir: Path = DATA_DIR) -> list:
    """Check 8: Row count in fact_daily_demand matches source CSV."""
    results = []

    checks = [
        ("fact_daily_demand",       data_dir / "daily_demand.csv"),
        ("fact_inventory_snapshot", data_dir / "inventory_snapshot.csv"),
    ]

    for table, csv_path in checks:
        def _check(t=table, p=csv_path):
            source_rows = sum(1 for _ in open(p)) - 1  # subtract header
            loaded_rows = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            delta = abs(loaded_rows - source_rows)
            status = "PASS" if delta == 0 else "FAIL"
            return make_result(f"completeness_{t}", status, source_rows, delta,
                               f"source={source_rows:,}, loaded={loaded_rows:,}")
        results.append(safe_check(f"completeness_{table}", _check, logging.getLogger("validation")))
    return results


# ---------------------------------------------------------------------------
# Main validation entry point
# ---------------------------------------------------------------------------

def run_validation(db_path: Path = DB_PATH, output_dir: Path = OUTPUT_DIR,
                   logger: logging.Logger = None) -> pd.DataFrame:
    """Run all validation checks. Returns a DataFrame of results."""
    if logger is None:
        logger = get_logger("validation")

    logger.info("=" * 60)
    logger.info("DATA QUALITY VALIDATION — START")
    logger.info(f"Database: {db_path}")
    logger.info("=" * 60)

    conn = duckdb.connect(str(db_path), read_only=True)
    all_results = []

    logger.info("\nCheck 1: Null primary keys")
    all_results.extend(check_null_pks(conn))

    logger.info("\nCheck 2: Referential integrity")
    all_results.extend(check_referential_integrity(conn))

    logger.info("\nCheck 3: Duplicate primary keys")
    all_results.extend(check_duplicate_pks(conn))

    logger.info("\nCheck 4: Date range")
    all_results.extend(check_date_range(conn))

    logger.info("\nCheck 5: Revenue sanity")
    all_results.append(check_revenue_sanity(conn))

    logger.info("\nCheck 6: Quantity sanity")
    all_results.extend(check_quantity_sanity(conn))

    logger.info("\nCheck 7: Stockout consistency")
    all_results.append(check_stockout_consistency(conn))

    logger.info("\nCheck 8: Completeness")
    all_results.extend(check_completeness(conn, DATA_DIR))

    conn.close()

    # Build report DataFrame
    report = pd.DataFrame(all_results)

    # Summary
    passed = (report["status"] == "PASS").sum()
    failed = (report["status"] == "FAIL").sum()
    errors = (report["status"] == "ERROR").sum()
    total  = len(report)

    logger.info("\n" + "=" * 60)
    logger.info("VALIDATION SUMMARY")
    logger.info(f"  PASS:  {passed}/{total}")
    logger.info(f"  FAIL:  {failed}/{total}")
    logger.info(f"  ERROR: {errors}/{total}")

    if failed > 0:
        logger.warning("Failed checks:")
        for _, row in report[report["status"] == "FAIL"].iterrows():
            logger.warning(f"  ✗ {row['check_name']}: {row['rows_failed']:,} failures — {row['detail']}")

    # Export report
    output_dir.mkdir(parents=True, exist_ok=True)
    report_path = output_dir / "validation_report.csv"
    report.to_csv(report_path, index=False)
    logger.info(f"\nValidation report saved: {report_path}")
    logger.info("DATA QUALITY VALIDATION — COMPLETE")

    return report


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DHL Warehouse Data Quality Validation")
    parser.add_argument("--db-path", type=Path, default=DB_PATH)
    args = parser.parse_args()

    logger = get_logger("validation")
    report = run_validation(db_path=args.db_path, logger=logger)
    print(f"\nValidation complete: {(report['status']=='PASS').sum()}/{len(report)} checks passed")
