"""
setup_schema.py — Create forecasting tables in the DuckDB warehouse.
Run once before the pipeline: python schema/setup_schema.py
"""
import sys
from pathlib import Path
from datetime import datetime
import duckdb

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH  = BASE_DIR.parent / "01-sku-segmentation-pipeline" / "outputs" / "dhl_warehouse.duckdb"

DDL = [
    """CREATE TABLE IF NOT EXISTS dim_model (
        model_id        INTEGER PRIMARY KEY,
        model_name      VARCHAR(100) NOT NULL UNIQUE,
        model_type      VARCHAR(20)  NOT NULL,
        description     VARCHAR(500),
        hyperparameters VARCHAR(1000),
        created_at      TIMESTAMP NOT NULL,
        etl_loaded_at   TIMESTAMP NOT NULL,
        etl_source_file VARCHAR(200) NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS fact_forecast (
        forecast_id           BIGINT PRIMARY KEY,
        sku_key               INTEGER,
        warehouse_key         INTEGER,
        model_id              INTEGER,
        sku_id                VARCHAR(20) NOT NULL,
        warehouse_id          VARCHAR(20) NOT NULL,
        forecast_date         DATE NOT NULL,
        forecast_horizon_days INTEGER NOT NULL,
        forecasted_qty        DOUBLE NOT NULL,
        lower_bound           DOUBLE,
        upper_bound           DOUBLE,
        confidence_level      DOUBLE,
        generated_at          TIMESTAMP NOT NULL,
        etl_loaded_at         TIMESTAMP NOT NULL,
        etl_source_file       VARCHAR(200) NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS fact_model_performance (
        performance_id  BIGINT PRIMARY KEY,
        model_id        INTEGER NOT NULL,
        sku_id          VARCHAR(20) NOT NULL,
        abc_class       VARCHAR(1),
        evaluation_date DATE NOT NULL,
        train_start     DATE NOT NULL,
        train_end       DATE NOT NULL,
        test_start      DATE NOT NULL,
        test_end        DATE NOT NULL,
        mape            DOUBLE,
        rmse            DOUBLE,
        mae             DOUBLE,
        bias            DOUBLE,
        record_created_at TIMESTAMP NOT NULL,
        etl_loaded_at   TIMESTAMP NOT NULL,
        etl_source_file VARCHAR(200) NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS fact_feature_store (
        feature_id      BIGINT PRIMARY KEY,
        sku_id          VARCHAR(20) NOT NULL,
        warehouse_id    VARCHAR(20) NOT NULL,
        feature_date    DATE NOT NULL,
        lag_1           DOUBLE,
        lag_7           DOUBLE,
        lag_14          DOUBLE,
        lag_28          DOUBLE,
        rolling_avg_7   DOUBLE,
        rolling_avg_14  DOUBLE,
        rolling_avg_28  DOUBLE,
        rolling_std_7   DOUBLE,
        rolling_std_14  DOUBLE,
        day_of_week     INTEGER NOT NULL,
        week_of_year    INTEGER NOT NULL,
        month           INTEGER NOT NULL,
        quarter         INTEGER NOT NULL,
        is_weekend      BOOLEAN NOT NULL,
        season          VARCHAR(10) NOT NULL,
        abc_class       VARCHAR(1),
        xyz_class       VARCHAR(1),
        etl_loaded_at   TIMESTAMP NOT NULL,
        etl_source_file VARCHAR(200) NOT NULL
    )""",
    "CREATE INDEX IF NOT EXISTS idx_fact_forecast_sku   ON fact_forecast(sku_id)",
    "CREATE INDEX IF NOT EXISTS idx_fact_forecast_date  ON fact_forecast(forecast_date)",
    "CREATE INDEX IF NOT EXISTS idx_fact_forecast_model ON fact_forecast(model_id)",
    "CREATE INDEX IF NOT EXISTS idx_fmp_model           ON fact_model_performance(model_id)",
    "CREATE INDEX IF NOT EXISTS idx_fmp_sku             ON fact_model_performance(sku_id)",
    "CREATE INDEX IF NOT EXISTS idx_fmp_abc             ON fact_model_performance(abc_class)",
    "CREATE INDEX IF NOT EXISTS idx_ffs_sku             ON fact_feature_store(sku_id)",
    "CREATE INDEX IF NOT EXISTS idx_ffs_date            ON fact_feature_store(feature_date)",
    "CREATE INDEX IF NOT EXISTS idx_ffs_sku_date        ON fact_feature_store(sku_id, feature_date)",
]

if __name__ == "__main__":
    # Allow custom DB path
    db_path = Path(sys.argv[1]) if len(sys.argv) > 1 else DB_PATH
    print(f"Setting up forecasting schema in: {db_path}")
    conn = duckdb.connect(str(db_path))
    for stmt in DDL:
        try:
            conn.execute(stmt)
        except Exception as e:
            if "already exists" in str(e).lower():
                pass
            else:
                print(f"  WARNING: {e}")
    conn.close()

    # Verify
    conn = duckdb.connect(str(db_path), read_only=True)
    for tbl in ["dim_model", "fact_forecast", "fact_model_performance", "fact_feature_store"]:
        cnt = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
        print(f"  ✓ {tbl}: {cnt} rows")
    conn.close()
    print("Schema setup complete.")
