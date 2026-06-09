-- =============================================================================
-- DHL Demand Forecasting — Schema Extension DDL
-- Extends: outputs/dhl_warehouse.duckdb (from Project 01)
-- Project: 02 — Demand Forecasting Pipeline
-- Author: Vinyl Kiran Anipe (Data Engineer)
-- Version: 1.0
-- =============================================================================
-- NOTE: This file is for documentation. The Python setup script (schema/setup_schema.py)
-- executes these statements programmatically to avoid DuckDB multi-statement parse issues.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- dim_model: Forecasting model registry
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_model (
    model_id            INTEGER PRIMARY KEY,
    model_name          VARCHAR(100) NOT NULL UNIQUE,
    model_type          VARCHAR(20)  NOT NULL,   -- baseline / statistical / ml
    description         VARCHAR(500),
    hyperparameters     VARCHAR(1000),            -- JSON string
    created_at          TIMESTAMP NOT NULL,
    etl_loaded_at       TIMESTAMP NOT NULL,
    etl_source_file     VARCHAR(200) NOT NULL
);

-- -----------------------------------------------------------------------------
-- fact_forecast: All forecast outputs
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_forecast (
    forecast_id             BIGINT PRIMARY KEY,
    sku_key                 INTEGER,              -- FK to dim_sku
    warehouse_key           INTEGER,              -- FK to dim_warehouse
    model_id                INTEGER,              -- FK to dim_model
    sku_id                  VARCHAR(20) NOT NULL,
    warehouse_id            VARCHAR(20) NOT NULL,
    forecast_date           DATE NOT NULL,        -- The date being forecast
    forecast_horizon_days   INTEGER NOT NULL,     -- Days ahead (1-30)
    forecasted_qty          DOUBLE NOT NULL,
    lower_bound             DOUBLE,
    upper_bound             DOUBLE,
    confidence_level        DOUBLE,               -- e.g. 0.80 for 80% CI
    generated_at            TIMESTAMP NOT NULL,
    etl_loaded_at           TIMESTAMP NOT NULL,
    etl_source_file         VARCHAR(200) NOT NULL
);

-- -----------------------------------------------------------------------------
-- fact_model_performance: Model evaluation results
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_model_performance (
    performance_id      BIGINT PRIMARY KEY,
    model_id            INTEGER NOT NULL,         -- FK to dim_model
    sku_id              VARCHAR(20) NOT NULL,
    abc_class           VARCHAR(1),
    evaluation_date     DATE NOT NULL,
    train_start         DATE NOT NULL,
    train_end           DATE NOT NULL,
    test_start          DATE NOT NULL,
    test_end            DATE NOT NULL,
    mape                DOUBLE,                   -- Mean Absolute Percentage Error
    rmse                DOUBLE,                   -- Root Mean Square Error
    mae                 DOUBLE,                   -- Mean Absolute Error
    bias                DOUBLE,                   -- Mean forecast bias (+ = over-forecast)
    record_created_at   TIMESTAMP NOT NULL,
    etl_loaded_at       TIMESTAMP NOT NULL,
    etl_source_file     VARCHAR(200) NOT NULL
);

-- -----------------------------------------------------------------------------
-- fact_feature_store: Pre-computed ML/forecasting features
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_feature_store (
    feature_id          BIGINT PRIMARY KEY,
    sku_id              VARCHAR(20) NOT NULL,
    warehouse_id        VARCHAR(20) NOT NULL,
    feature_date        DATE NOT NULL,
    -- Lag features
    lag_1               DOUBLE,
    lag_7               DOUBLE,
    lag_14              DOUBLE,
    lag_28              DOUBLE,
    -- Rolling averages
    rolling_avg_7       DOUBLE,
    rolling_avg_14      DOUBLE,
    rolling_avg_28      DOUBLE,
    -- Rolling standard deviations
    rolling_std_7       DOUBLE,
    rolling_std_14      DOUBLE,
    -- Calendar features
    day_of_week         INTEGER NOT NULL,         -- 0=Monday, 6=Sunday
    week_of_year        INTEGER NOT NULL,
    month               INTEGER NOT NULL,
    quarter             INTEGER NOT NULL,
    is_weekend          BOOLEAN NOT NULL,
    season              VARCHAR(10) NOT NULL,
    -- SKU segment
    abc_class           VARCHAR(1),
    xyz_class           VARCHAR(1),
    -- Audit
    etl_loaded_at       TIMESTAMP NOT NULL,
    etl_source_file     VARCHAR(200) NOT NULL
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_fact_forecast_sku      ON fact_forecast(sku_id);
CREATE INDEX IF NOT EXISTS idx_fact_forecast_date     ON fact_forecast(forecast_date);
CREATE INDEX IF NOT EXISTS idx_fact_forecast_model    ON fact_forecast(model_id);
CREATE INDEX IF NOT EXISTS idx_fmp_model              ON fact_model_performance(model_id);
CREATE INDEX IF NOT EXISTS idx_fmp_sku                ON fact_model_performance(sku_id);
CREATE INDEX IF NOT EXISTS idx_fmp_abc                ON fact_model_performance(abc_class);
CREATE INDEX IF NOT EXISTS idx_ffs_sku                ON fact_feature_store(sku_id);
CREATE INDEX IF NOT EXISTS idx_ffs_date               ON fact_feature_store(feature_date);
CREATE INDEX IF NOT EXISTS idx_ffs_sku_date           ON fact_feature_store(sku_id, feature_date);
