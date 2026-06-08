"""
load.py — Load Layer
DHL SKU Segmentation Pipeline — Project 01

Connects to the persistent DuckDB database.
Creates schema if not already present.
Truncates and reloads all dimension and fact tables.
Validates row counts after load match source.
Logs all load statistics.
"""

import logging
import time
from datetime import datetime
from pathlib import Path
import duckdb
import pandas as pd

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH  = BASE_DIR / "outputs" / "dhl_warehouse.duckdb"
SCHEMA_PATH = BASE_DIR / "schema" / "create_schema.sql"

# Load order: dimensions first, then facts (FK dependency)
LOAD_ORDER = [
    "dim_date",
    "dim_warehouse",
    "dim_supplier",
    "dim_sku",
    "fact_daily_demand",
    "fact_inventory_snapshot",
]

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def get_logger(name: str = "load") -> logging.Logger:
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
# Database helpers
# ---------------------------------------------------------------------------

def get_connection(db_path: Path = DB_PATH) -> duckdb.DuckDBPyConnection:
    """Open a persistent DuckDB connection."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(db_path))


def create_schema(conn: duckdb.DuckDBPyConnection, schema_path: Path,
                  logger: logging.Logger):
    """Execute the schema DDL — create tables and indexes directly in Python."""
    logger.info(f"Creating schema (using inline DDL)...")

    ddl_statements = [
        # ---- DIMENSION TABLES ----
        """CREATE TABLE IF NOT EXISTS dim_sku (
            sku_key         INTEGER PRIMARY KEY,
            sku_id          VARCHAR(20) NOT NULL UNIQUE,
            category        VARCHAR(50) NOT NULL,
            abc_class       VARCHAR(1)  NOT NULL,
            xyz_class       VARCHAR(1),
            unit_cost       DOUBLE NOT NULL,
            unit_price      DOUBLE NOT NULL,
            weight_kg       DOUBLE,
            volume_cbm      DOUBLE,
            storage_type    VARCHAR(20) NOT NULL,
            supplier_id     VARCHAR(20),
            lead_time_days  INTEGER,
            min_order_qty   INTEGER,
            safety_stock_qty    INTEGER,
            reorder_point_qty   INTEGER,
            primary_warehouse   VARCHAR(20),
            active_flag     BOOLEAN NOT NULL DEFAULT TRUE,
            etl_loaded_at   TIMESTAMP NOT NULL,
            etl_source_file VARCHAR(200) NOT NULL
        )""",
        """CREATE TABLE IF NOT EXISTS dim_date (
            date_key        INTEGER PRIMARY KEY,
            full_date       DATE NOT NULL UNIQUE,
            day_of_week     INTEGER NOT NULL,
            day_name        VARCHAR(10) NOT NULL,
            day_of_month    INTEGER NOT NULL,
            day_of_year     INTEGER NOT NULL,
            week_of_year    INTEGER NOT NULL,
            month_num       INTEGER NOT NULL,
            month_name      VARCHAR(10) NOT NULL,
            quarter         INTEGER NOT NULL,
            year            INTEGER NOT NULL,
            is_weekend      BOOLEAN NOT NULL,
            is_month_start  BOOLEAN NOT NULL,
            is_month_end    BOOLEAN NOT NULL,
            is_quarter_end  BOOLEAN NOT NULL,
            season          VARCHAR(10) NOT NULL,
            etl_loaded_at   TIMESTAMP NOT NULL,
            etl_source_file VARCHAR(200) NOT NULL
        )""",
        """CREATE TABLE IF NOT EXISTS dim_warehouse (
            warehouse_key   INTEGER PRIMARY KEY,
            warehouse_id    VARCHAR(20) NOT NULL UNIQUE,
            warehouse_name  VARCHAR(100) NOT NULL,
            city            VARCHAR(50) NOT NULL,
            state           VARCHAR(50) NOT NULL,
            region          VARCHAR(20) NOT NULL,
            country         VARCHAR(50) NOT NULL DEFAULT 'USA',
            timezone        VARCHAR(50) NOT NULL,
            active_flag     BOOLEAN NOT NULL DEFAULT TRUE,
            etl_loaded_at   TIMESTAMP NOT NULL,
            etl_source_file VARCHAR(200) NOT NULL
        )""",
        """CREATE TABLE IF NOT EXISTS dim_supplier (
            supplier_key        INTEGER PRIMARY KEY,
            supplier_id         VARCHAR(20) NOT NULL UNIQUE,
            supplier_name       VARCHAR(100) NOT NULL,
            country             VARCHAR(50) NOT NULL,
            category_focus      VARCHAR(50),
            lead_time_avg_days  DOUBLE,
            lead_time_std_days  DOUBLE,
            otif_rate           DOUBLE,
            fill_rate           DOUBLE,
            defect_rate         DOUBLE,
            active_flag         BOOLEAN NOT NULL DEFAULT TRUE,
            etl_loaded_at       TIMESTAMP NOT NULL,
            etl_source_file     VARCHAR(200) NOT NULL
        )""",
        # ---- FACT TABLES ----
        """CREATE TABLE IF NOT EXISTS fact_daily_demand (
            demand_key          BIGINT PRIMARY KEY,
            date_key            INTEGER,
            sku_key             INTEGER,
            warehouse_key       INTEGER,
            abc_class           VARCHAR(1),
            xyz_class           VARCHAR(1),
            quantity_demanded   INTEGER NOT NULL DEFAULT 0,
            quantity_fulfilled  INTEGER NOT NULL DEFAULT 0,
            quantity_unfulfilled INTEGER NOT NULL DEFAULT 0,
            stockout_flag       BOOLEAN NOT NULL DEFAULT FALSE,
            revenue             DOUBLE NOT NULL DEFAULT 0,
            fill_rate           DOUBLE,
            etl_loaded_at       TIMESTAMP NOT NULL,
            etl_source_file     VARCHAR(200) NOT NULL
        )""",
        """CREATE TABLE IF NOT EXISTS fact_inventory_snapshot (
            snapshot_key        BIGINT PRIMARY KEY,
            date_key            INTEGER,
            sku_key             INTEGER,
            warehouse_key       INTEGER,
            on_hand_qty         INTEGER NOT NULL DEFAULT 0,
            in_transit_qty      INTEGER NOT NULL DEFAULT 0,
            committed_qty       INTEGER NOT NULL DEFAULT 0,
            available_qty       INTEGER NOT NULL DEFAULT 0,
            inventory_value     DOUBLE NOT NULL DEFAULT 0,
            inventory_record_accuracy DOUBLE,
            etl_loaded_at       TIMESTAMP NOT NULL,
            etl_source_file     VARCHAR(200) NOT NULL
        )""",
        # ---- INDEXES ----
        "CREATE INDEX IF NOT EXISTS idx_dim_sku_id       ON dim_sku(sku_id)",
        "CREATE INDEX IF NOT EXISTS idx_dim_sku_category ON dim_sku(category)",
        "CREATE INDEX IF NOT EXISTS idx_dim_sku_abc      ON dim_sku(abc_class)",
        "CREATE INDEX IF NOT EXISTS idx_dim_date_full    ON dim_date(full_date)",
        "CREATE INDEX IF NOT EXISTS idx_dim_date_month   ON dim_date(year, month_num)",
        "CREATE INDEX IF NOT EXISTS idx_dim_wh_id        ON dim_warehouse(warehouse_id)",
        "CREATE INDEX IF NOT EXISTS idx_dim_sup_id       ON dim_supplier(supplier_id)",
        "CREATE INDEX IF NOT EXISTS idx_fdd_date         ON fact_daily_demand(date_key)",
        "CREATE INDEX IF NOT EXISTS idx_fdd_sku          ON fact_daily_demand(sku_key)",
        "CREATE INDEX IF NOT EXISTS idx_fdd_wh           ON fact_daily_demand(warehouse_key)",
        "CREATE INDEX IF NOT EXISTS idx_fdd_stockout     ON fact_daily_demand(stockout_flag)",
        "CREATE INDEX IF NOT EXISTS idx_fis_date         ON fact_inventory_snapshot(date_key)",
        "CREATE INDEX IF NOT EXISTS idx_fis_sku          ON fact_inventory_snapshot(sku_key)",
        "CREATE INDEX IF NOT EXISTS idx_fis_wh           ON fact_inventory_snapshot(warehouse_key)",
    ]

    for stmt in ddl_statements:
        try:
            conn.execute(stmt)
        except Exception as e:
            if "already exists" in str(e).lower():
                pass
            else:
                logger.warning(f"  DDL warning: {e}")
    logger.info("  Schema created/verified.")


def truncate_table(conn: duckdb.DuckDBPyConnection, table_name: str,
                   logger: logging.Logger):
    """Truncate a table (DELETE all rows)."""
    try:
        conn.execute(f"DELETE FROM {table_name}")
        logger.info(f"  Truncated {table_name}")
    except Exception as e:
        logger.warning(f"  Could not truncate {table_name}: {e}")


def load_table(conn: duckdb.DuckDBPyConnection, table_name: str,
               df: pd.DataFrame, logger: logging.Logger) -> dict:
    """
    Load a DataFrame into a DuckDB table.
    Returns load statistics dict.
    """
    t_start = time.time()
    source_rows = len(df)

    try:
        # Get the column order from the database table
        table_cols = [row[0] for row in conn.execute(f"DESCRIBE {table_name}").fetchall()]
        # Only keep columns that exist in both df and the table, in table order
        load_cols = [c for c in table_cols if c in df.columns]
        df_load = df[load_cols]

        # Register the DataFrame as a temporary view, then insert
        conn.register("_staging", df_load)
        col_list = ", ".join(f'"{c}"' for c in load_cols)
        conn.execute(f"INSERT INTO {table_name} ({col_list}) SELECT {col_list} FROM _staging")
        conn.unregister("_staging")

        # Verify row count
        loaded_rows = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        duration_s = round(time.time() - t_start, 3)

        status = "OK" if loaded_rows == source_rows else "ROW_COUNT_MISMATCH"
        if status == "ROW_COUNT_MISMATCH":
            logger.warning(
                f"  {table_name}: source={source_rows:,}, loaded={loaded_rows:,} — MISMATCH"
            )
        else:
            logger.info(
                f"  {table_name}: {loaded_rows:,} rows loaded in {duration_s}s"
            )

        return {
            "table":        table_name,
            "source_rows":  source_rows,
            "loaded_rows":  loaded_rows,
            "duration_s":   duration_s,
            "status":       status,
            "error":        None,
        }

    except Exception as e:
        duration_s = round(time.time() - t_start, 3)
        logger.error(f"  LOAD ERROR for {table_name}: {e}")
        return {
            "table":        table_name,
            "source_rows":  source_rows,
            "loaded_rows":  0,
            "duration_s":   duration_s,
            "status":       "ERROR",
            "error":        str(e),
        }


# ---------------------------------------------------------------------------
# Main load entry point
# ---------------------------------------------------------------------------

def load_all(transformed: dict, db_path: Path = DB_PATH,
             logger: logging.Logger = None) -> list:
    """
    Load all transformed tables into DuckDB.
    Returns list of load statistics dicts.
    """
    if logger is None:
        logger = get_logger("load")

    logger.info("=" * 60)
    logger.info("LOAD STAGE — START")
    logger.info(f"Target database: {db_path}")
    logger.info("=" * 60)

    conn = get_connection(db_path)

    # Create schema
    create_schema(conn, SCHEMA_PATH, logger)

    # Truncate all tables in reverse load order (facts first, then dims)
    logger.info("Truncating existing data...")
    for table in reversed(LOAD_ORDER):
        if table in transformed:
            truncate_table(conn, table, logger)

    # Load in dependency order
    logger.info("Loading tables...")
    stats = []
    for table in LOAD_ORDER:
        if table not in transformed:
            logger.warning(f"  Skipping {table} — not found in transformed data")
            continue
        stat = load_table(conn, table, transformed[table], logger)
        stats.append(stat)

    conn.close()

    # Summary
    ok     = sum(1 for s in stats if s["status"] == "OK")
    errors = sum(1 for s in stats if s["status"] == "ERROR")
    mismatches = sum(1 for s in stats if s["status"] == "ROW_COUNT_MISMATCH")
    total_rows = sum(s["loaded_rows"] for s in stats)

    logger.info("-" * 60)
    logger.info(f"Load summary: {ok} OK | {mismatches} mismatches | {errors} errors")
    logger.info(f"Total rows in warehouse: {total_rows:,}")
    logger.info("LOAD STAGE — COMPLETE")

    if errors > 0:
        raise RuntimeError(f"Load stage completed with {errors} error(s). Check logs.")

    return stats


# ---------------------------------------------------------------------------
# Standalone run
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    sys.path.insert(0, str(Path(__file__).parent))
    from extract import extract_all
    from transform import transform_all

    logger = get_logger("load")
    extracted   = extract_all(logger=logger)
    transformed = transform_all(extracted, logger=logger)
    stats       = load_all(transformed, logger=logger)

    print("\nLoad statistics:")
    for s in stats:
        print(f"  {s['table']}: {s['loaded_rows']:,} rows — {s['status']} ({s['duration_s']}s)")
