"""
setup_schema.py — Run wms_warehouse_schema.sql against the DHL warehouse
DHL Data Engineer Portfolio — Project 05
"""
import sys
from pathlib import Path
import duckdb

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH  = Path("/tmp/dhl_p5.duckdb")
SQL_FILE = Path(__file__).resolve().parent / "wms_warehouse_schema.sql"

EXPECTED_TABLES = [
    "fact_wms_daily_kpis",
    "fact_wms_monthly_kpis",
    "fact_operator_daily",
    "fact_error_log",
    "fact_inventory_accuracy",
    "meta_pipeline_runs",
]

def main():
    print(f"DB path : {DB_PATH}")
    print(f"SQL file: {SQL_FILE}")

    conn = duckdb.connect(str(DB_PATH))

    sql = SQL_FILE.read_text()
    # Strip single-line comments, then split on semicolon
    import re
    sql_no_comments = re.sub(r"--[^\n]*", "", sql)
    statements = [s.strip() for s in sql_no_comments.split(";") if s.strip()]
    print(f"\nRunning {len(statements)} DDL statements...")
    for stmt in statements:
        conn.execute(stmt)
        first_line = stmt.splitlines()[0][:80]
        print(f"  OK: {first_line}...")

    # Verify tables exist
    existing = {r[0] for r in conn.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
    ).fetchall()}

    print("\nTable verification:")
    all_ok = True
    for t in EXPECTED_TABLES:
        ok = t in existing
        print(f"  {'✓' if ok else '✗'} {t}")
        if not ok:
            all_ok = False

    conn.close()
    if not all_ok:
        print("\nERROR: Some tables were not created.")
        sys.exit(1)
    print(f"\nAll {len(EXPECTED_TABLES)} tables created successfully.")

if __name__ == "__main__":
    main()
