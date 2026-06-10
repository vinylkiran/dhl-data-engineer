-- warehouse_schema.sql — Warehouse Operations Pipeline Schema
-- DHL Data Engineer Portfolio — Project 04
-- Extends dhl_warehouse.duckdb with 5 new tables.
-- Execute via schema/setup_schema.py (statement by statement).

-- ============================================================
-- dim_location  (SCD Type 2)
-- ============================================================
CREATE TABLE IF NOT EXISTS dim_location (
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
    valid_to                TIMESTAMP,          -- NULL = currently active
    is_current              BOOLEAN     DEFAULT TRUE,
    etl_loaded_at           TIMESTAMP
);

-- ============================================================
-- dim_operator  (anonymised)
-- ============================================================
CREATE TABLE IF NOT EXISTS dim_operator (
    operator_surrogate_id   INTEGER     NOT NULL PRIMARY KEY,
    operator_id             VARCHAR     NOT NULL UNIQUE,   -- hashed/anonymised
    warehouse_id            VARCHAR,
    hire_date_cohort        VARCHAR,    -- e.g. "2022-Q1" (quarter of first task)
    active_flag             BOOLEAN     DEFAULT TRUE,
    etl_loaded_at           TIMESTAMP
);

-- ============================================================
-- fact_wms_tasks
-- ============================================================
CREATE TABLE IF NOT EXISTS fact_wms_tasks (
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
);

-- ============================================================
-- fact_slotting_history
-- ============================================================
CREATE TABLE IF NOT EXISTS fact_slotting_history (
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
);

-- ============================================================
-- fact_cooccurrence
-- ============================================================
CREATE TABLE IF NOT EXISTS fact_cooccurrence (
    pair_id                 INTEGER     NOT NULL PRIMARY KEY,
    sku_id_1                VARCHAR     NOT NULL,
    sku_id_2                VARCHAR     NOT NULL,
    warehouse_id            VARCHAR     NOT NULL,
    co_occurrence_count     INTEGER,
    co_occurrence_window    VARCHAR,    -- 'shift' or 'day'
    lift_score              DOUBLE,
    last_calculated_at      TIMESTAMP
);

-- ============================================================
-- Indexes
-- ============================================================
CREATE INDEX IF NOT EXISTS idx_fact_wms_task_date     ON fact_wms_tasks (task_date);
CREATE INDEX IF NOT EXISTS idx_fact_wms_warehouse     ON fact_wms_tasks (warehouse_id);
CREATE INDEX IF NOT EXISTS idx_fact_wms_sku           ON fact_wms_tasks (sku_id);
CREATE INDEX IF NOT EXISTS idx_dim_location_current   ON dim_location (is_current);
CREATE INDEX IF NOT EXISTS idx_dim_location_id        ON dim_location (location_id);
CREATE INDEX IF NOT EXISTS idx_slotting_status        ON fact_slotting_history (implementation_status);
CREATE INDEX IF NOT EXISTS idx_cooccurrence_wh        ON fact_cooccurrence (warehouse_id);
