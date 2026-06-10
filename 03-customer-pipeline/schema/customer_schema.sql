-- customer_schema.sql — Customer Pipeline Schema
-- DHL Data Engineer Portfolio — Project 03
-- Extends dhl_warehouse.duckdb with 4 new tables.
-- Execute via schema/setup_schema.py (statement by statement).

-- ============================================================
-- dim_customer
-- ============================================================
CREATE TABLE IF NOT EXISTS dim_customer (
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
);

-- ============================================================
-- fact_orders
-- ============================================================
CREATE TABLE IF NOT EXISTS fact_orders (
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
);

-- ============================================================
-- fact_rfm_scores  (SCD Type 2)
-- ============================================================
CREATE TABLE IF NOT EXISTS fact_rfm_scores (
    score_id            INTEGER        NOT NULL PRIMARY KEY,
    customer_id         VARCHAR        NOT NULL,
    scoring_date        DATE           NOT NULL,
    recency_days        INTEGER,
    frequency_count     INTEGER,
    monetary_value      DOUBLE,
    recency_score       INTEGER        CHECK (recency_score BETWEEN 1 AND 5),
    frequency_score     INTEGER        CHECK (frequency_score BETWEEN 1 AND 5),
    monetary_score      INTEGER        CHECK (monetary_score BETWEEN 1 AND 5),
    rfm_segment         VARCHAR,
    is_current_flag     BOOLEAN        DEFAULT TRUE,
    valid_from          TIMESTAMP,
    valid_to            TIMESTAMP,     -- NULL means currently active
    etl_loaded_at       TIMESTAMP
);

-- ============================================================
-- dim_ab_test_registry  (test catalogue)
-- ============================================================
CREATE TABLE IF NOT EXISTS dim_ab_test_registry (
    test_id             INTEGER        NOT NULL PRIMARY KEY,
    test_name           VARCHAR        NOT NULL UNIQUE,
    hypothesis          VARCHAR,
    target_segment      VARCHAR,
    primary_metric      VARCHAR,
    split_ratio         DOUBLE         DEFAULT 0.5,
    test_start_date     DATE,
    test_end_date       DATE,
    status              VARCHAR        DEFAULT 'planned',  -- planned/active/completed
    created_at          TIMESTAMP
);

-- ============================================================
-- fact_ab_assignments
-- ============================================================
CREATE TABLE IF NOT EXISTS fact_ab_assignments (
    assignment_id           INTEGER        NOT NULL PRIMARY KEY,
    customer_id             VARCHAR        NOT NULL,
    test_name               VARCHAR        NOT NULL,
    test_group              VARCHAR        NOT NULL,  -- test / control
    assigned_at             TIMESTAMP,
    test_start_date         DATE,
    test_end_date           DATE,
    primary_metric_value    DOUBLE,
    converted_flag          BOOLEAN        DEFAULT FALSE,
    conversion_date         DATE,
    revenue_post_assignment DOUBLE         DEFAULT 0.0
);

-- ============================================================
-- Indexes
-- ============================================================
CREATE INDEX IF NOT EXISTS idx_fact_orders_customer    ON fact_orders (customer_id);
CREATE INDEX IF NOT EXISTS idx_fact_orders_date        ON fact_orders (order_date);
CREATE INDEX IF NOT EXISTS idx_fact_rfm_customer       ON fact_rfm_scores (customer_id);
CREATE INDEX IF NOT EXISTS idx_fact_rfm_current        ON fact_rfm_scores (is_current_flag);
CREATE INDEX IF NOT EXISTS idx_fact_ab_customer        ON fact_ab_assignments (customer_id);
CREATE INDEX IF NOT EXISTS idx_fact_ab_test            ON fact_ab_assignments (test_name);
