"""
setup_schema.py — Warehouse Operations Schema Setup
DHL Data Engineer Portfolio — Project 04
"""
import sys
from pathlib import Path
import duckdb

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH  = BASE_DIR.parent / "01-sku-segmentation-pipeline" / "outputs" / "dhl_warehouse.duckdb"

DDL_STATEMENTS = [
    """CREATE TABLE IF NOT EXISTS dim_location (
        location_surrogate_id   INTEGER     NOT NULL PRIMARY KEY,
        location_id             VARCHAR     NOT NULL,
        warehouse_id            VARCHAR,
        zone                    VARCHAR,
        aisle                   VARCHAR,
        bay                     INTEGER,
        level                   INTEGER,
        capacity_units          INTEGER,
        storage_type            VARCHAR,
        active_flag             BOOLEAN     DEFAULT TRUE,
        valid_from              TIMESTAMP   NOT NULL,
        valid_to                TIMESTAMP,
        is_current              BOOLEAN     DEFAULT TRUE,
        etl_loaded_at           TIMESTAMP
    )""",
    """CREATE TABLE IF NOT EXISTS dim_operator (
        operator_surrogate_id   INTEGER     NOT NULL PRIMARY KEY,
        operator_id             VARCHAR     NOT NULL UNIQUE,
        warehouse_id            VARCHAR,
        hire_date_cohort        VARCHAR,
        active_flag             BOOLEAN     DEFAULT TRUE,
        etl_loaded_at           TIMESTAMP
    )""",
    """CREATE TABLE IF NOT EXISTS fact_wms_tasks (
        task_id                 VARCHAR     NOT NULL PRIMARY KEY,
        sku_id                  VARCHAR,
        location_id             VARCHAR,
        warehouse_id            VARCHAR,
        operator_surrogate_id   INTEGER,
        task_date               DATE,
        task_type               VARCHAR,
        shift                   VARCHAR,
        duration_min            DOUBLE,
        quantity                INTEGER,
        accuracy_flag           BOOLEAN,
        error_code              VARCHAR,
        etl_loaded_at           TIMESTAMP
    )""",
    """CREATE TABLE IF NOT EXISTS fact_slotting_history (
        slotting_id                         INTEGER     NOT NULL PRIMARY KEY,
        sku_id                              VARCHAR     NOT NULL,
        warehouse_id                        VARCHAR     NOT NULL,
        recommendation_date                 DATE,
        prior_zone                          VARCHAR,
        recommended_zone                    VARCHAR,
        pick_frequency_at_recommendation    INTEGER,
        estimated_daily_minutes_saved       DOUBLE,
        implementation_status               VARCHAR     DEFAULT 'pending',
        implementation_date                 DATE,
        actual_minutes_saved_post           DOUBLE,
        etl_loaded_at                       TIMESTAMP
    )""",
    """CREATE TABLE IF NOT EXISTS fact_cooccurrence (
        pair_id                 INTEGER     NOT NULL PRIMARY KEY,
        sku_id_1                VARCHAR     NOT NULL,
        sku_id_2                VARCHAR     NOT NULL,
        warehouse_id            VARCHAR     NOT NULL,
        co_occurrence_count     INTEGER,
        co_occurrence_window    VARCHAR,
        lift_score              DOUBLE,
        last_calculated_at      TIMESTAMP
    )""",
    "CREATE INDEX IF NOT EXISTS idx_fact_wms_task_date   ON fact_wms_tasks (task_date)",
    "CREATE INDEX IF NOT EXISTS idx_fact_wms_warehouse   ON fact_wms_tasks (warehouse_id)",
    "CREATE INDEX IF NOT EXISTS idx_fact_wms_sku         ON fact_wms_tasks (sku_id)",
    "CREATE INDEX IF NOT EXISTS idx_dim_location_current ON dim_location (is_current)",
    "CREATE INDEX IF NOT EXISTS idx_dim_location_id      ON dim_location (location_id)",
    "CREATE INDEX IF NOT EXISTS idx_slotting_status      ON fact_slotting_history (implementation_status)",
    "CREATE INDEX IF NOT EXISTS idx_cooccurrence_wh      ON fact_cooccurrence (warehouse_id)",
]

def setup_schema(db_path: Path = DB_PATH) -> None:
    print(f"Connecting to: {db_path}")
    conn = duckdb.connect(str(db_path))
    tables_before = {r[0] for r in conn.execute("SHOW TABLES").fetchall()}
    for stmt in DDL_STATEMENTS:
        try:
            conn.execute(stmt)
        except Exception as e:
            print(f"  ERROR: {str(e)[:100]}")
            conn.close(); sys.exit(1)
    tables_after = {r[0] for r in conn.execute("SHOW TABLES").fetchall()}
    new_tables = sorted(tables_after - tables_before)
    conn.close()
    print(f"Schema setup complete. New tables: {new_tables}")
    expected = {"dim_location","dim_operator","fact_wms_tasks","fact_slotting_history","fact_cooccurrence"}
    missing = expected - tables_after
    if missing:
        print(f"WARNING: Missing tables: {missing}"); sys.exit(1)
    print(f"All 5 warehouse pipeline tables confirmed. Total tables: {len(tables_after)}")

if __name__ == "__main__":
    db = Path(sys.argv[1]) if len(sys.argv) > 1 else DB_PATH
    setup_schema(db)
