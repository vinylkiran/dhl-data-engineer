-- =============================================================================
-- DHL Supply Chain Warehouse — Star Schema DDL
-- Database: outputs/dhl_warehouse.duckdb
-- Project: 01 — SKU Segmentation Pipeline
-- Author: Vinyl Kiran Anipe (Data Engineer)
-- Version: 1.0
-- =============================================================================

-- -----------------------------------------------------------------------------
-- DIMENSION TABLES
-- -----------------------------------------------------------------------------

-- dim_sku: SKU master dimension
CREATE TABLE IF NOT EXISTS dim_sku (
    sku_key         INTEGER PRIMARY KEY,          -- Surrogate key
    sku_id          VARCHAR(20) NOT NULL UNIQUE,  -- Natural key: e.g. HLT-000001
    category        VARCHAR(50) NOT NULL,
    abc_class       VARCHAR(1)  NOT NULL CHECK (abc_class IN ('A','B','C')),
    xyz_class       VARCHAR(1)  CHECK (xyz_class IN ('X','Y','Z')),
    unit_cost       DECIMAL(12,4) NOT NULL,
    unit_price      DECIMAL(12,4) NOT NULL,
    weight_kg       DECIMAL(10,4),
    volume_cbm      DECIMAL(10,4),
    storage_type    VARCHAR(20) NOT NULL CHECK (storage_type IN ('Ambient','Bulk','Controlled','Hazmat')),
    supplier_id     VARCHAR(20),
    lead_time_days  INTEGER,
    min_order_qty   INTEGER,
    safety_stock_qty    INTEGER,
    reorder_point_qty   INTEGER,
    primary_warehouse   VARCHAR(20),
    active_flag     BOOLEAN NOT NULL DEFAULT TRUE,
    etl_loaded_at   TIMESTAMP NOT NULL,
    etl_source_file VARCHAR(200) NOT NULL
);

-- dim_date: Date dimension (2022-01-01 to 2023-12-31)
CREATE TABLE IF NOT EXISTS dim_date (
    date_key        INTEGER PRIMARY KEY,          -- Surrogate key: YYYYMMDD integer
    full_date       DATE NOT NULL UNIQUE,
    day_of_week     INTEGER NOT NULL,             -- 1=Monday ... 7=Sunday
    day_name        VARCHAR(10) NOT NULL,
    day_of_month    INTEGER NOT NULL,
    day_of_year     INTEGER NOT NULL,
    week_of_year    INTEGER NOT NULL,
    month_num       INTEGER NOT NULL,
    month_name      VARCHAR(10) NOT NULL,
    quarter         INTEGER NOT NULL CHECK (quarter IN (1,2,3,4)),
    year            INTEGER NOT NULL,
    is_weekend      BOOLEAN NOT NULL,
    is_month_start  BOOLEAN NOT NULL,
    is_month_end    BOOLEAN NOT NULL,
    is_quarter_end  BOOLEAN NOT NULL,
    season          VARCHAR(10) NOT NULL CHECK (season IN ('Spring','Summer','Autumn','Winter')),
    etl_loaded_at   TIMESTAMP NOT NULL,
    etl_source_file VARCHAR(200) NOT NULL
);

-- dim_warehouse: Warehouse dimension
CREATE TABLE IF NOT EXISTS dim_warehouse (
    warehouse_key   INTEGER PRIMARY KEY,          -- Surrogate key
    warehouse_id    VARCHAR(20) NOT NULL UNIQUE,  -- Natural key: e.g. DHL-WH-IL02
    warehouse_name  VARCHAR(100) NOT NULL,
    city            VARCHAR(50) NOT NULL,
    state           VARCHAR(50) NOT NULL,
    region          VARCHAR(20) NOT NULL,
    country         VARCHAR(50) NOT NULL DEFAULT 'USA',
    timezone        VARCHAR(50) NOT NULL,
    active_flag     BOOLEAN NOT NULL DEFAULT TRUE,
    etl_loaded_at   TIMESTAMP NOT NULL,
    etl_source_file VARCHAR(200) NOT NULL
);

-- dim_supplier: Supplier dimension
CREATE TABLE IF NOT EXISTS dim_supplier (
    supplier_key        INTEGER PRIMARY KEY,       -- Surrogate key
    supplier_id         VARCHAR(20) NOT NULL UNIQUE, -- Natural key: e.g. SUP-0001
    supplier_name       VARCHAR(100) NOT NULL,
    country             VARCHAR(50) NOT NULL,
    category_focus      VARCHAR(50),
    lead_time_avg_days  DECIMAL(6,2),
    lead_time_std_days  DECIMAL(6,2),
    otif_rate           DECIMAL(6,4),
    fill_rate           DECIMAL(6,4),
    defect_rate         DECIMAL(6,4),
    active_flag         BOOLEAN NOT NULL DEFAULT TRUE,
    etl_loaded_at       TIMESTAMP NOT NULL,
    etl_source_file     VARCHAR(200) NOT NULL
);

-- -----------------------------------------------------------------------------
-- FACT TABLES
-- -----------------------------------------------------------------------------

-- fact_daily_demand: Daily demand and fulfilment fact
CREATE TABLE IF NOT EXISTS fact_daily_demand (
    demand_key          BIGINT PRIMARY KEY,        -- Surrogate key
    date_key            INTEGER NOT NULL REFERENCES dim_date(date_key),
    sku_key             INTEGER NOT NULL REFERENCES dim_sku(sku_key),
    warehouse_key       INTEGER NOT NULL REFERENCES dim_warehouse(warehouse_key),
    -- Degenerate dimensions (denormalised for query convenience)
    abc_class           VARCHAR(1),
    xyz_class           VARCHAR(1),
    -- Measures
    quantity_demanded   INTEGER NOT NULL DEFAULT 0,
    quantity_fulfilled  INTEGER NOT NULL DEFAULT 0,
    quantity_unfulfilled INTEGER NOT NULL DEFAULT 0,  -- Derived: demanded - fulfilled
    stockout_flag       BOOLEAN NOT NULL DEFAULT FALSE,
    revenue             DECIMAL(14,4) NOT NULL DEFAULT 0,
    fill_rate           DECIMAL(6,4),                  -- Derived: fulfilled / demanded
    -- Audit
    etl_loaded_at       TIMESTAMP NOT NULL,
    etl_source_file     VARCHAR(200) NOT NULL
);

-- fact_inventory_snapshot: Monthly inventory position fact
CREATE TABLE IF NOT EXISTS fact_inventory_snapshot (
    snapshot_key        BIGINT PRIMARY KEY,        -- Surrogate key
    date_key            INTEGER NOT NULL REFERENCES dim_date(date_key),
    sku_key             INTEGER NOT NULL REFERENCES dim_sku(sku_key),
    warehouse_key       INTEGER NOT NULL REFERENCES dim_warehouse(warehouse_key),
    -- Measures
    on_hand_qty         INTEGER NOT NULL DEFAULT 0,
    in_transit_qty      INTEGER NOT NULL DEFAULT 0,
    committed_qty       INTEGER NOT NULL DEFAULT 0,
    available_qty       INTEGER NOT NULL DEFAULT 0,
    inventory_value     DECIMAL(14,4) NOT NULL DEFAULT 0,
    inventory_record_accuracy DECIMAL(6,4),            -- Derived: available / on_hand
    -- Audit
    etl_loaded_at       TIMESTAMP NOT NULL,
    etl_source_file     VARCHAR(200) NOT NULL
);

-- -----------------------------------------------------------------------------
-- INDEXES (DuckDB uses ART indexes)
-- -----------------------------------------------------------------------------

-- dim_sku lookups by natural key and category
CREATE INDEX IF NOT EXISTS idx_dim_sku_id       ON dim_sku(sku_id);
CREATE INDEX IF NOT EXISTS idx_dim_sku_category ON dim_sku(category);
CREATE INDEX IF NOT EXISTS idx_dim_sku_abc      ON dim_sku(abc_class);

-- dim_date lookups by date and period
CREATE INDEX IF NOT EXISTS idx_dim_date_full    ON dim_date(full_date);
CREATE INDEX IF NOT EXISTS idx_dim_date_month   ON dim_date(year, month_num);
CREATE INDEX IF NOT EXISTS idx_dim_date_quarter ON dim_date(year, quarter);

-- dim_warehouse natural key
CREATE INDEX IF NOT EXISTS idx_dim_wh_id        ON dim_warehouse(warehouse_id);

-- dim_supplier natural key
CREATE INDEX IF NOT EXISTS idx_dim_sup_id       ON dim_supplier(supplier_id);

-- fact_daily_demand — composite index for common query patterns
CREATE INDEX IF NOT EXISTS idx_fdd_date         ON fact_daily_demand(date_key);
CREATE INDEX IF NOT EXISTS idx_fdd_sku          ON fact_daily_demand(sku_key);
CREATE INDEX IF NOT EXISTS idx_fdd_wh           ON fact_daily_demand(warehouse_key);
CREATE INDEX IF NOT EXISTS idx_fdd_stockout     ON fact_daily_demand(stockout_flag);

-- fact_inventory_snapshot
CREATE INDEX IF NOT EXISTS idx_fis_date         ON fact_inventory_snapshot(date_key);
CREATE INDEX IF NOT EXISTS idx_fis_sku          ON fact_inventory_snapshot(sku_key);
CREATE INDEX IF NOT EXISTS idx_fis_wh           ON fact_inventory_snapshot(warehouse_key);
