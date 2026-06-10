"""
setup_schema.py — Customer Pipeline Schema Setup
DHL Data Engineer Portfolio — Project 03

Extends dhl_warehouse.duckdb with customer pipeline tables.
Executes each DDL statement individually to avoid DuckDB multi-statement issues.
"""

import sys
from pathlib import Path
import duckdb

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH  = BASE_DIR.parent / "01-sku-segmentation-pipeline" / "outputs" / "dhl_warehouse.duckdb"

DDL_STATEMENTS = [
    # dim_customer
    """CREATE TABLE IF NOT EXISTS dim_customer (
        customer_id         VARCHAR        NOT NULL PRIMARY KEY,
        customer_type       VARCHAR,
        region              VARCHAR,
        sla_hours           INTEGER,
        annual_rev_band     VARCHAR,
        active_flag         BOOLEAN        DEFAULT TRUE,
        contract_since      DATE,
        current_rfm_segment VARCHAR,
        first_order_date    DATE,
        last_order_date     DATE,
        lifetime_orders     INTEGER        DEFAULT 0,
        lifetime_revenue    DOUBLE         DEFAULT 0.0,
        etl_loaded_at       TIMESTAMP
    )""",

    # fact_orders
    """CREATE TABLE IF NOT EXISTS fact_orders (
        order_id            VARCHAR        NOT NULL PRIMARY KEY,
        customer_id         VARCHAR        NOT NULL,
        sku_id              VARCHAR,
        warehouse_id        VARCHAR,
        order_date          DATE,
        ship_date           DATE,
        channel             VARCHAR,
        ordered_qty         INTEGER,
        shipped_qty         INTEGER,
        revenue             DOUBLE,
        on_time_flag        BOOLEAN,
        in_full_flag        BOOLEAN,
        otif_flag           BOOLEAN,
        days_to_ship        INTEGER,
        etl_loaded_at       TIMESTAMP
    )""",

    # fact_rfm_scores (SCD Type 2)
    """CREATE TABLE IF NOT EXISTS fact_rfm_scores (
        score_id            INTEGER        NOT NULL PRIMARY KEY,
        customer_id         VARCHAR        NOT NULL,
        scoring_date        DATE           NOT NULL,
        recency_days        INTEGER,
        frequency_count     INTEGER,
        monetary_value      DOUBLE,
        recency_score       INTEGER,
        frequency_score     INTEGER,
        monetary_score      INTEGER,
        rfm_segment         VARCHAR,
        is_current_flag     BOOLEAN        DEFAULT TRUE,
        valid_from          TIMESTAMP,
        valid_to            TIMESTAMP,
        etl_loaded_at       TIMESTAMP
    )""",

    # dim_ab_test_registry
    """CREATE TABLE IF NOT EXISTS dim_ab_test_registry (
        test_id             INTEGER        NOT NULL PRIMARY KEY,
        test_name           VARCHAR        NOT NULL UNIQUE,
        hypothesis          VARCHAR,
        target_segment      VARCHAR,
        primary_metric      VARCHAR,
        split_ratio         DOUBLE         DEFAULT 0.5,
        test_start_date     DATE,
        test_end_date       DATE,
        status              VARCHAR        DEFAULT 'planned',
        created_at          TIMESTAMP
    )""",

    # fact_ab_assignments
    """CREATE TABLE IF NOT EXISTS fact_ab_assignments (
        assignment_id           INTEGER        NOT NULL PRIMARY KEY,
        customer_id             VARCHAR        NOT NULL,
        test_name               VARCHAR        NOT NULL,
        test_group              VARCHAR        NOT NULL,
        assigned_at             TIMESTAMP,
        test_start_date         DATE,
        test_end_date           DATE,
        primary_metric_value    DOUBLE,
        converted_flag          BOOLEAN        DEFAULT FALSE,
        conversion_date         DATE,
        revenue_post_assignment DOUBLE         DEFAULT 0.0
    )""",

    # Indexes
    "CREATE INDEX IF NOT EXISTS idx_fact_orders_customer ON fact_orders (customer_id)",
    "CREATE INDEX IF NOT EXISTS idx_fact_orders_date     ON fact_orders (order_date)",
    "CREATE INDEX IF NOT EXISTS idx_fact_rfm_customer    ON fact_rfm_scores (customer_id)",
    "CREATE INDEX IF NOT EXISTS idx_fact_rfm_current     ON fact_rfm_scores (is_current_flag)",
    "CREATE INDEX IF NOT EXISTS idx_fact_ab_customer     ON fact_ab_assignments (customer_id)",
    "CREATE INDEX IF NOT EXISTS idx_fact_ab_test         ON fact_ab_assignments (test_name)",
]

def setup_schema(db_path: Path = DB_PATH) -> None:
    print(f"Connecting to: {db_path}")
    conn = duckdb.connect(str(db_path))
    tables_before = {r[0] for r in conn.execute("SHOW TABLES").fetchall()}

    for stmt in DDL_STATEMENTS:
        name = stmt.strip().split()[0:5]
        try:
            conn.execute(stmt)
        except Exception as e:
            print(f"  ERROR executing: {' '.join(name)}... → {e}")
            conn.close()
            sys.exit(1)

    tables_after = {r[0] for r in conn.execute("SHOW TABLES").fetchall()}
    new_tables = tables_after - tables_before

    conn.close()
    print(f"\nSchema setup complete.")
    print(f"New tables created: {sorted(new_tables) if new_tables else '(all already existed)'}")
    print(f"Total tables in warehouse: {len(tables_after)}")

    # Verify all expected tables exist
    expected = {"dim_customer", "fact_orders", "fact_rfm_scores",
                "dim_ab_test_registry", "fact_ab_assignments"}
    missing = expected - tables_after
    if missing:
        print(f"WARNING: Missing tables: {missing}")
        sys.exit(1)
    print("All 5 customer pipeline tables confirmed present.")


if __name__ == "__main__":
    db = Path(sys.argv[1]) if len(sys.argv) > 1 else DB_PATH
    setup_schema(db)
