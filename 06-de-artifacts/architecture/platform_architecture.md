# DHL Data Engineering Platform — Master Architecture Document
## DE Portfolio — Project 06 Reference Artifact

---

## 1. Platform Overview

The DHL Data Engineering platform is a five-pipeline analytical data warehouse built on DuckDB, covering the full data lifecycle from raw CSV ingestion through dimensional modelling, aggregation, quality assurance, and serving. It was designed to mirror a real logistics company's analytical infrastructure at a scale appropriate for local development and portfolio demonstration.

The platform processes data across five operational domains: inventory segmentation, demand forecasting, customer analytics, warehouse operations, and WMS reporting. All five pipelines share a single DuckDB warehouse file, a common set of conventions, and a consistent quality framework.

---

## 2. Overall Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│  SOURCE LAYER (shared/data/dhl-synthetic/)                          │
│                                                                     │
│  sku_master.csv        wms_tasks.csv          customers.csv         │
│  demand_history.csv    warehouse_locations.csv orders.csv           │
│  inventory_snapshot.csv  suppliers.csv        operator_data.csv     │
└─────────────────────────┬───────────────────────────────────────────┘
                          │  CSV → Python ETL
                          ▼
┌─────────────────────────────────────────────────────────────────────┐
│  EXTRACT & TRANSFORM LAYER (Python ETL scripts per project)         │
│                                                                     │
│  01: etl/extract.py, transform.py, load.py, pipeline.py            │
│  02: etl/incremental_load.py + features/feature_engineering.py      │
│  03: etl/customer_etl.py                                            │
│  04: etl/wms_etl.py                                                 │
│  05: etl/wms_warehouse_etl.py                                       │
└─────────────────────────┬───────────────────────────────────────────┘
                          │  INSERT / UPDATE / UPSERT
                          ▼
┌─────────────────────────────────────────────────────────────────────┐
│  DHL WAREHOUSE (dhl_warehouse.duckdb)                               │
│                                                                     │
│  DIMENSIONS          FACTS                     META                 │
│  ────────────        ─────────────────────     ────────             │
│  dim_date            fact_daily_demand          meta_pipeline_runs   │
│  dim_sku             fact_feature_store                              │
│  dim_warehouse       fact_forecast                                   │
│  dim_supplier        fact_model_performance                          │
│  dim_customer        fact_inventory_snapshot                         │
│  dim_location        fact_orders                                     │
│  dim_operator        fact_rfm_scores                                 │
│  dim_model           fact_ab_assignments                             │
│  dim_ab_test_registry fact_cooccurrence                              │
│                      fact_slotting_history                           │
│                      fact_wms_tasks                                  │
│                      fact_error_log                                  │
│                      fact_inventory_accuracy                         │
│                      fact_wms_daily_kpis       (pre-aggregated)      │
│                      fact_wms_monthly_kpis     (pre-aggregated)      │
│                      fact_operator_daily       (pre-aggregated)      │
└─────────────────────────┬───────────────────────────────────────────┘
                          │
          ┌───────────────┴────────────────┐
          │  AGGREGATION LAYER             │  QUALITY LAYER
          │  (Project 05)                  │  (every project)
          │                                │
          │  build_aggregations.py         │  dq_framework.py
          │  → fact_wms_daily_kpis         │  → dq_report.csv
          │  → fact_wms_monthly_kpis       │  anomaly_detection.py
          │  → fact_operator_daily         │  → anomaly_flags.csv
          └───────────────┬────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────────┐
│  SERVING LAYER (DuckDB views + CSV exports)                         │
│                                                                     │
│  Project 03:  v_customer_segments, v_champion_customers,            │
│               v_at_risk_customers, v_segment_performance            │
│  Project 04:  v_daily_kpis, v_operator_scorecard,                   │
│               v_slotting_queue, v_cooccurrence_adjacency            │
│  Project 05:  v_network_kpis_current_month, v_warehouse_comparison, │
│               v_kpi_trends_12m, v_operator_leaderboard,             │
│               v_error_patterns, v_coaching_list,                    │
│               v_high_performer_list                                 │
└─────────────────────────┬───────────────────────────────────────────┘
                          │  CSV exports / direct DuckDB query
                          ▼
┌─────────────────────────────────────────────────────────────────────┐
│  CONSUMING LAYER                                                    │
│                                                                     │
│  BA/DA Portfolio dashboards (Plotly HTML)                           │
│  Commercial team (RFM segment CSVs)                                 │
│  Warehouse operations (slotting recommendations CSV)                │
│  Supply chain planners (forecast_output CSV)                        │
│  WMS management (operator scorecard, coaching list CSVs)            │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 3. Warehouse Structure by Subject Area

### 3.1 Inventory Subject Area (Project 01)

Covers SKU master data, supplier relationships, inventory stock levels, and the core demand signal.

| Object | Type | Description |
|---|---|---|
| dim_sku | Dimension | 2,000 SKUs with ABC class, category, active flag, reorder point |
| dim_supplier | Dimension | 80 suppliers with lead time, contract tier, country |
| dim_warehouse | Dimension | 3 DHL warehouses (NJ01, IL02, TX03) with region, capacity |
| dim_date | Dimension | 730-day calendar with fiscal period, holiday flags |
| fact_daily_demand | Fact | 574,509 rows — daily demand per SKU per warehouse, 2022–2023 |
| fact_inventory_snapshot | Fact | 19,200 rows — monthly stock levels with inventory_record_accuracy |

### 3.2 Forecasting Subject Area (Project 02)

Covers the ML feature store, forecasting model registry, model evaluation, and forward-looking demand forecasts.

| Object | Type | Description |
|---|---|---|
| dim_model | Dimension | 3 models: moving_average_14d, moving_average_28d, seasonal_naive |
| fact_feature_store | Fact | 574,509 rows — engineered features per SKU per day |
| fact_model_performance | Fact | 4,992 rows — MAPE/RMSE/MAE evaluation per model per ABC class |
| fact_forecast | Fact | 149,760 rows — 90-day forward forecasts with 80% confidence intervals |

### 3.3 Customer Subject Area (Project 03)

Covers customer master data, order history, RFM scoring, and A/B test infrastructure.

| Object | Type | Description |
|---|---|---|
| dim_customer | Dimension | 500 customers with lifetime revenue, order dates, RFM segment |
| dim_ab_test_registry | Dimension | A/B test catalogue with test metadata and hypothesis |
| fact_orders | Fact | 68,941 rows — customer order history with incremental load |
| fact_rfm_scores | Fact (SCD2) | 398 rows — RFM score history with valid_from/valid_to tracking |
| fact_ab_assignments | Fact | 60 rows — customer-to-variant assignments with outcome tracking |

### 3.4 Warehouse Operations Subject Area (Project 04)

Covers WMS task execution, operator anonymisation, location SCD2 history, slotting, and co-occurrence.

| Object | Type | Description |
|---|---|---|
| dim_location | Dimension (SCD2) | 2,640 rows — warehouse locations with zone/storage_type history |
| dim_operator | Dimension | 60 anonymised operators with hire cohort |
| fact_wms_tasks | Fact | 219,000 rows — every WMS task: Pick, Putaway, Replenishment, etc. |
| fact_slotting_history | Fact | 882 rows — slotting recommendations with estimated minutes saved |
| fact_cooccurrence | Fact | 600 rows — top 200 lift-scored SKU pairs per warehouse |

### 3.5 WMS Reporting Subject Area (Project 05)

Covers pre-aggregated KPI tables, error logging, inventory accuracy, and the full serving layer.

| Object | Type | Description |
|---|---|---|
| fact_wms_daily_kpis | Aggregate Fact | 6,570 rows — KPIs pre-computed by date/warehouse/shift |
| fact_wms_monthly_kpis | Aggregate Fact | 72 rows — monthly KPI roll-up for management reporting |
| fact_operator_daily | Aggregate Fact | 162,317 rows — per-operator scorecards with performance flag |
| fact_error_log | Fact | 1,417 rows — detailed WMS error events |
| fact_inventory_accuracy | Fact | 576 rows — monthly inventory accuracy by category |
| meta_pipeline_runs | Meta | Pipeline execution audit log |

---

## 4. Design Principles Applied Consistently

### 4.1 Incremental Loading

Projects 02, 03, 04, and 05 all implement watermark-based incremental loading. The pattern:

1. Determine the watermark (either from `meta_pipeline_runs.run_end` or from MAX of an existing key column)
2. Only process records with IDs or timestamps newer than the watermark
3. On first run (no watermark): process all data

The specific watermark mechanism varies by pipeline:
- **Project 02**: `last_processed_date` tracked in a side file; SQL `WHERE date > ?`
- **Project 03**: `fact_orders` uses set-difference on `order_id` to find new orders
- **Project 04**: `fact_wms_tasks` uses set-difference on `task_id`; `dim_location` uses row-by-row SCD2 comparison
- **Project 05**: `meta_pipeline_runs` watermark; error log uses task_id set-difference

### 4.2 SCD Type 2 for Slowly Changing Dimensions

Two dimensions use SCD Type 2 to preserve history:

**dim_location** (Project 04): Tracks zone, storage_type, and active_flag history. Trigger: any change in these three fields creates a new SCD2 row. Pattern: expire old row (valid_to = NOW, is_current = FALSE), insert new row (valid_from = NOW, valid_to = NULL, is_current = TRUE).

**fact_rfm_scores** (Project 03): Treated as a slowly changing fact. On each full scoring run, all is_current=TRUE rows are expired and new scores are inserted. This preserves the complete RFM history.

### 4.3 Pre-Aggregation for Query Performance

Project 05 introduces the pre-aggregation layer to separate raw data from reporting data:
- `fact_wms_daily_kpis` collapses 219,000 task rows into 6,570 date/warehouse/shift summaries
- `fact_wms_monthly_kpis` further collapses to 72 month/warehouse summaries
- Trend queries (12-month window) show 3.3x speedup against pre-aggregated tables vs raw GROUP BY

### 4.4 Audit Logging via meta_pipeline_runs

Every pipeline run in Project 05 is recorded in `meta_pipeline_runs` with: run_id, pipeline_name, run_start, run_end, duration_seconds, status, rows_processed, rows_inserted, rows_updated, error_message. This enables watermark-based incremental loading and provides a full audit trail for compliance.

### 4.5 Layered Data Quality by Severity

Project 05's DQ framework establishes four severity tiers applied across all tables:
- **CRITICAL**: Pipeline halts — null PKs, negative quantities, future dates
- **HIGH**: Alert sent — accuracy rates out of range, error code consistency, referential integrity
- **MEDIUM**: Log for review — duration bounds, productivity bounds
- **LOW**: Informational — operator inactivity, slow-moving SKUs

All five projects run their own DQ checks and export `*_dq_report.csv` to their `outputs/` folder.

---

## 5. Adding a New Pipeline

To add a sixth pipeline following established patterns:

**Step 1 — Schema.** Create `06-new-pipeline/schema/` with a `.sql` DDL file and a `setup_schema.py` runner. Follow naming conventions: `dim_` prefix for dimensions, `fact_` prefix for facts, `snake_case` columns, `_id` suffix for surrogate keys. Every table must include `etl_loaded_at TIMESTAMP NOT NULL`.

**Step 2 — ETL.** Create `etl/new_etl.py`. Structure: `get_logger()`, `start_run(conn, pipeline_name)` / `finish_run(conn, run_id, ...)` wrappers calling `meta_pipeline_runs`, at least one load function per target table implementing watermark-based incremental load.

**Step 3 — Quality.** Create `quality/dq_checks.py`. Implement at minimum: one CRITICAL check (null PKs), one HIGH check (referential integrity to dim_warehouse or dim_sku), one MEDIUM check (value bounds). Export results to `outputs/dq_report.csv`.

**Step 4 — Serving.** Create `serving/` with a `CREATE OR REPLACE VIEW` for each dashboard consumer. Export each view to CSV for downstream use.

**Step 5 — Documentation.** Write three docs: `schema_design.md` (table definitions, relationships, design rationale), `pipeline_runbook.md` (how to run, how to interpret outputs, troubleshooting), `data_dictionary.md` (every column with type, definition, source mapping, example).

**Step 6 — Push.** Commit with the pattern `DE Project N: <Subject> — <what was built> complete`.
