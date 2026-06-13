-- =============================================================
-- WMS Data Warehouse Schema
-- DHL Data Engineer Portfolio — Project 05
-- =============================================================
-- Extends the existing DHL warehouse with 6 new tables:
--   fact_wms_daily_kpis        Pre-aggregated daily KPIs
--   fact_wms_monthly_kpis      Monthly KPIs for trend analysis
--   fact_operator_daily        Daily operator scorecards
--   fact_error_log             Detailed error event log
--   fact_inventory_accuracy    Monthly inventory accuracy snapshots
--   meta_pipeline_runs         Pipeline execution audit log
-- =============================================================

-- Pre-aggregated daily KPI table
-- Replaces ad-hoc GROUP BY queries on 219k-row fact_wms_tasks
CREATE TABLE IF NOT EXISTS fact_wms_daily_kpis (
    kpi_id                    INTEGER PRIMARY KEY,
    kpi_date                  DATE        NOT NULL,
    warehouse_id              VARCHAR(20) NOT NULL,
    shift                     VARCHAR(20) NOT NULL,
    total_tasks               INTEGER     NOT NULL DEFAULT 0,
    total_picks               INTEGER     NOT NULL DEFAULT 0,
    total_putaways            INTEGER     NOT NULL DEFAULT 0,
    total_replenishments      INTEGER     NOT NULL DEFAULT 0,
    total_cycle_counts        INTEGER     NOT NULL DEFAULT 0,
    pick_accuracy_pct         DECIMAL(6,3),
    putaway_accuracy_pct      DECIMAL(6,3),
    cycle_count_accuracy_pct  DECIMAL(6,3),
    overall_accuracy_pct      DECIMAL(6,3),
    avg_pick_duration_min     DECIMAL(8,3),
    avg_putaway_duration_min  DECIMAL(8,3),
    picks_per_labour_hour     DECIMAL(8,3),
    total_errors              INTEGER     NOT NULL DEFAULT 0,
    etl_loaded_at             TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_daily_kpis_natural
    ON fact_wms_daily_kpis (kpi_date, warehouse_id, shift);

CREATE INDEX IF NOT EXISTS idx_daily_kpis_date
    ON fact_wms_daily_kpis (kpi_date);

CREATE INDEX IF NOT EXISTS idx_daily_kpis_warehouse
    ON fact_wms_daily_kpis (warehouse_id);

-- Monthly KPI table for management reporting
CREATE TABLE IF NOT EXISTS fact_wms_monthly_kpis (
    monthly_kpi_id            INTEGER PRIMARY KEY,
    kpi_year                  INTEGER     NOT NULL,
    kpi_month                 INTEGER     NOT NULL,
    warehouse_id              VARCHAR(20) NOT NULL,
    total_tasks               INTEGER     NOT NULL DEFAULT 0,
    total_picks               INTEGER     NOT NULL DEFAULT 0,
    total_putaways            INTEGER     NOT NULL DEFAULT 0,
    total_replenishments      INTEGER     NOT NULL DEFAULT 0,
    total_cycle_counts        INTEGER     NOT NULL DEFAULT 0,
    pick_accuracy_pct         DECIMAL(6,3),
    putaway_accuracy_pct      DECIMAL(6,3),
    cycle_count_accuracy_pct  DECIMAL(6,3),
    overall_accuracy_pct      DECIMAL(6,3),
    avg_pick_duration_min     DECIMAL(8,3),
    avg_putaway_duration_min  DECIMAL(8,3),
    picks_per_labour_hour     DECIMAL(8,3),
    total_errors              INTEGER     NOT NULL DEFAULT 0,
    working_days              INTEGER,
    etl_loaded_at             TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_monthly_kpis_natural
    ON fact_wms_monthly_kpis (kpi_year, kpi_month, warehouse_id);

CREATE INDEX IF NOT EXISTS idx_monthly_kpis_warehouse
    ON fact_wms_monthly_kpis (warehouse_id);

-- Daily operator scorecard
CREATE TABLE IF NOT EXISTS fact_operator_daily (
    operator_daily_id         INTEGER PRIMARY KEY,
    operator_id               INTEGER     NOT NULL,   -- surrogate key from dim_operator
    warehouse_id              VARCHAR(20) NOT NULL,
    task_date                 DATE        NOT NULL,
    shift                     VARCHAR(20) NOT NULL,
    tasks_completed           INTEGER     NOT NULL DEFAULT 0,
    picks_completed           INTEGER     NOT NULL DEFAULT 0,
    pick_accuracy_pct         DECIMAL(6,3),
    avg_duration_min          DECIMAL(8,3),
    error_count               INTEGER     NOT NULL DEFAULT 0,
    top_error_code            VARCHAR(50),
    performance_flag          VARCHAR(20),            -- high_performer / standard / needs_coaching
    etl_loaded_at             TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_operator_daily_natural
    ON fact_operator_daily (operator_id, warehouse_id, task_date, shift);

CREATE INDEX IF NOT EXISTS idx_operator_daily_date
    ON fact_operator_daily (task_date);

CREATE INDEX IF NOT EXISTS idx_operator_daily_flag
    ON fact_operator_daily (performance_flag);

-- Detailed error event log (one row per error event)
CREATE TABLE IF NOT EXISTS fact_error_log (
    error_id                  INTEGER PRIMARY KEY,
    task_id                   VARCHAR(30) NOT NULL,
    sku_id                    VARCHAR(20),
    warehouse_id              VARCHAR(20) NOT NULL,
    operator_id               INTEGER,               -- surrogate key
    task_date                 DATE        NOT NULL,
    shift                     VARCHAR(20),
    task_type                 VARCHAR(30),
    error_code                VARCHAR(50),
    zone                      VARCHAR(30),
    category                  VARCHAR(20),           -- derived from sku_id prefix (e.g. PHM, AUT)
    error_context             VARCHAR(200),          -- human-readable description
    etl_loaded_at             TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_error_log_date
    ON fact_error_log (task_date);

CREATE INDEX IF NOT EXISTS idx_error_log_warehouse
    ON fact_error_log (warehouse_id);

CREATE INDEX IF NOT EXISTS idx_error_log_error_code
    ON fact_error_log (error_code);

CREATE INDEX IF NOT EXISTS idx_error_log_operator
    ON fact_error_log (operator_id);

-- Monthly inventory accuracy snapshots
CREATE TABLE IF NOT EXISTS fact_inventory_accuracy (
    accuracy_id               INTEGER PRIMARY KEY,
    snapshot_date             DATE        NOT NULL,
    warehouse_id              VARCHAR(20) NOT NULL,
    category                  VARCHAR(20) NOT NULL,  -- SKU category (PHM, AUT, etc.)
    total_skus_counted        INTEGER     NOT NULL DEFAULT 0,
    accurate_count            INTEGER     NOT NULL DEFAULT 0,
    discrepancy_count         INTEGER     NOT NULL DEFAULT 0,
    accuracy_pct              DECIMAL(6,3),
    total_on_hand_value       DECIMAL(14,2),
    discrepancy_value         DECIMAL(14,2),
    etl_loaded_at             TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_inv_accuracy_natural
    ON fact_inventory_accuracy (snapshot_date, warehouse_id, category);

CREATE INDEX IF NOT EXISTS idx_inv_accuracy_warehouse
    ON fact_inventory_accuracy (warehouse_id);

-- Pipeline execution audit log
CREATE TABLE IF NOT EXISTS meta_pipeline_runs (
    run_id                    INTEGER PRIMARY KEY,
    pipeline_name             VARCHAR(100) NOT NULL,
    run_start                 TIMESTAMP    NOT NULL,
    run_end                   TIMESTAMP,
    duration_seconds          DECIMAL(10,3),
    status                    VARCHAR(20)  NOT NULL DEFAULT 'running',  -- running/success/failed
    rows_processed            INTEGER      DEFAULT 0,
    rows_inserted             INTEGER      DEFAULT 0,
    rows_updated              INTEGER      DEFAULT 0,
    error_message             VARCHAR(1000),
    run_by                    VARCHAR(50)  DEFAULT 'pipeline'
);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_name
    ON meta_pipeline_runs (pipeline_name);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status
    ON meta_pipeline_runs (status);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_start
    ON meta_pipeline_runs (run_start);
