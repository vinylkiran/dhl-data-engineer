# Table Inventory — DHL Data Engineering Platform
## DE Portfolio — Project 06 Reference Artifact

Complete inventory of every table and view created across all five DE projects. Use this as the starting point for any data governance, impact analysis, or onboarding query.

**Last updated:** 2026-06-13 | **Total objects:** 41 (26 tables + 15 views)

---

## Dimensions

| table_name | table_type | project | subject_area | row_count_approx | refresh_pattern | primary_consumer |
|---|---|---|---|---|---|---|
| dim_date | Dimension | 01 — SKU Segmentation | Shared | 730 | Full reload (one-time, calendar spine) | All fact tables (FK join) |
| dim_sku | Dimension | 01 — SKU Segmentation | Inventory | 2,000 | Full reload (upsert on sku_id) | fact_daily_demand, fact_wms_tasks, fact_inventory_snapshot |
| dim_warehouse | Dimension | 01 — SKU Segmentation | Shared | 3 | Full reload (static; 3 DHL locations) | All fact tables (FK join) |
| dim_supplier | Dimension | 01 — SKU Segmentation | Inventory | 80 | Full reload | dim_sku.supplier_id (reference) |
| dim_customer | Dimension | 03 — Customer Pipeline | Customer | 500 | Incremental upsert (customer_id key) | fact_orders, fact_rfm_scores |
| dim_ab_test_registry | Dimension | 03 — Customer Pipeline | Customer | varies | Full reload (test catalogue) | fact_ab_assignments |
| dim_location | Dimension (SCD2) | 04 — Warehouse Ops | Warehouse Operations | 2,640 | SCD2 (zone/storage_type/active_flag triggers) | fact_wms_tasks (zone lookup) |
| dim_operator | Dimension | 04 — Warehouse Ops | Warehouse Operations | 60 | Full reload (anonymised) | fact_wms_tasks, fact_operator_daily |
| dim_model | Dimension | 02 — Demand Forecasting | Forecasting | 3 | Full reload (model registry) | fact_model_performance, fact_forecast |

---

## Facts — Raw

| table_name | table_type | project | subject_area | row_count_approx | refresh_pattern | primary_consumer |
|---|---|---|---|---|---|---|
| fact_daily_demand | Fact | 01 — SKU Segmentation | Inventory | 574,509 | Incremental (date watermark) | fact_feature_store (feature engineering), ABC class analysis |
| fact_inventory_snapshot | Fact | 01 — SKU Segmentation | Inventory | 19,200 | Full reload (monthly snapshots) | fact_inventory_accuracy (Project 05 ETL) |
| fact_feature_store | Fact | 02 — Demand Forecasting | Forecasting | 574,509 | Incremental (date watermark, mirrors fact_daily_demand) | fact_model_performance (training), fact_forecast (inference) |
| fact_model_performance | Fact | 02 — Demand Forecasting | Forecasting | 4,992 | Full reload per model evaluation run | DS portfolio model comparison analysis |
| fact_forecast | Fact | 02 — Demand Forecasting | Forecasting | 149,760 | Full reload (90-day rolling horizon) | forecast_output.csv, BA/DA dashboard |
| fact_orders | Fact | 03 — Customer Pipeline | Customer | 68,941 | Incremental (order_id set-difference) | fact_rfm_scores (RFM computation), dim_customer (lifetime metrics update) |
| fact_rfm_scores | Fact (SCD2) | 03 — Customer Pipeline | Customer | 398 (is_current=TRUE) | Expire-and-replace (full SCD2 on each scoring run) | v_customer_segments, commercial team CSVs |
| fact_ab_assignments | Fact | 03 — Customer Pipeline | Customer | 60 | Full reload | A/B test outcome analysis |
| fact_cooccurrence | Fact | 04 — Warehouse Ops | Warehouse Operations | 600 | Full rebuild (lift score recomputation) | v_cooccurrence_adjacency, slotting decisions |
| fact_wms_tasks | Fact | 04 — Warehouse Ops | Warehouse Operations | 219,000 | Incremental (task_id set-difference) | All WMS KPI aggregations (Project 05 source) |
| fact_slotting_history | Fact | 04 — Warehouse Ops | Warehouse Operations | 882 | Incremental (skip existing sku/warehouse pending) | v_slotting_queue, warehouse manager CSV |
| fact_error_log | Fact | 05 — WMS Data Warehouse | WMS Reporting | 1,417 | Incremental (task_id set-difference from fact_wms_tasks where accuracy_flag=FALSE) | v_error_patterns, anomaly_detection |
| fact_inventory_accuracy | Fact | 05 — WMS Data Warehouse | WMS Reporting | 576 | Incremental (snapshot_month/warehouse/category key) | Inventory accuracy trending, DQ framework |

---

## Facts — Pre-Aggregated

| table_name | table_type | project | subject_area | row_count_approx | refresh_pattern | primary_consumer |
|---|---|---|---|---|---|---|
| fact_wms_daily_kpis | Aggregate Fact | 05 — WMS Data Warehouse | WMS Reporting | 6,570 | Full rebuild (DELETE + INSERT each run) | v_warehouse_comparison, v_kpi_trends_12m, BA/DA dashboard |
| fact_wms_monthly_kpis | Aggregate Fact | 05 — WMS Data Warehouse | WMS Reporting | 72 | Derived from fact_wms_daily_kpis (full rebuild) | v_kpi_trends_12m, management reporting |
| fact_operator_daily | Aggregate Fact | 05 — WMS Data Warehouse | WMS Reporting | 162,317 | Full rebuild (DELETE + INSERT each run) | v_operator_leaderboard, v_coaching_list, v_high_performer_list |

---

## Meta

| table_name | table_type | project | subject_area | row_count_approx | refresh_pattern | primary_consumer |
|---|---|---|---|---|---|---|
| meta_pipeline_runs | Meta / Audit | 05 — WMS Data Warehouse | Platform | 2+ | Append-only (one row per pipeline run) | Watermark for incremental load, operations audit trail |

---

## Views — Customer Subject Area (Project 03)

| table_name | table_type | project | subject_area | row_count_approx | refresh_pattern | primary_consumer |
|---|---|---|---|---|---|---|
| v_customer_segments | View | 03 — Customer Pipeline | Customer | ~500 | Derived (reads fact_rfm_scores is_current=TRUE) | BA/DA RFM dashboard |
| v_champion_customers | View | 03 — Customer Pipeline | Customer | ~50–75 | Derived | Commercial team outreach campaigns |
| v_at_risk_customers | View | 03 — Customer Pipeline | Customer | ~75–100 | Derived | Commercial team retention campaigns |
| v_segment_performance | View | 03 — Customer Pipeline | Customer | 6–8 | Derived | Executive RFM segment summary |

---

## Views — Warehouse Operations Subject Area (Project 04)

| table_name | table_type | project | subject_area | row_count_approx | refresh_pattern | primary_consumer |
|---|---|---|---|---|---|---|
| v_daily_kpis | View | 04 — Warehouse Ops | Warehouse Operations | varies | Derived (last 30 days from fact_wms_tasks) | Operations daily review |
| v_operator_scorecard | View | 04 — Warehouse Ops | Warehouse Operations | varies | Derived | Warehouse manager shift review |
| v_slotting_queue | View | 04 — Warehouse Ops | Warehouse Operations | ~882 | Derived (pending slotting_history rows) | Warehouse manager, slot move scheduling |
| v_cooccurrence_adjacency | View | 04 — Warehouse Ops | Warehouse Operations | ~600 | Derived (top lift pairs) | Slotting decisions, co-location planning |

---

## Views — WMS Reporting Subject Area (Project 05)

| table_name | table_type | project | subject_area | row_count_approx | refresh_pattern | primary_consumer |
|---|---|---|---|---|---|---|
| v_network_kpis_current_month | View | 05 — WMS Data Warehouse | WMS Reporting | 1 | Derived (current calendar month from fact_wms_daily_kpis) | Network operations director, executive dashboard |
| v_warehouse_comparison | View | 05 — WMS Data Warehouse | WMS Reporting | 3 | Derived (last 30 days by warehouse) | Operations manager, regional network review |
| v_kpi_trends_12m | View | 05 — WMS Data Warehouse | WMS Reporting | ~36 | Derived (last 12 months from fact_wms_monthly_kpis) | Trend reporting, executive deck |
| v_operator_leaderboard | View | 05 — WMS Data Warehouse | WMS Reporting | ~60 | Derived (current month from fact_operator_daily) | Warehouse managers, recognition programs |
| v_error_patterns | View | 05 — WMS Data Warehouse | WMS Reporting | ~15–30 | Derived (last 30 days from fact_error_log) | Quality team, process improvement |
| v_coaching_list | View | 05 — WMS Data Warehouse | WMS Reporting | varies | Derived (last 14 days, needs_coaching flag) | HR/Training, coaching session scheduling |
| v_high_performer_list | View | 05 — WMS Data Warehouse | WMS Reporting | varies | Derived (last 30 days, high_performer flag) | HR, recognition and retention |

---

## Summary Statistics

| Metric | Value |
|---|---|
| Total objects | 41 |
| Dimension tables | 9 |
| Raw fact tables | 13 |
| Pre-aggregated fact tables | 3 |
| Meta tables | 1 |
| Views | 15 |
| Total fact row count (approx) | 1,783,803 |
| Largest single table | fact_daily_demand / fact_feature_store (574,509 rows each) |
| Smallest fact table | fact_ab_assignments (60 rows) |
| Tables with SCD2 | 2 (dim_location, fact_rfm_scores) |
| Tables with incremental load | 7 (fact_daily_demand, fact_feature_store, fact_orders, fact_rfm_scores, fact_wms_tasks, fact_error_log, fact_inventory_accuracy) |
| Tables with full reload | 9 |
| Tables with pre-aggregation rebuild | 3 (fact_wms_daily_kpis, fact_wms_monthly_kpis, fact_operator_daily) |

---

## Cross-Reference: Which Projects Share Which Tables

| Table | P01 | P02 | P03 | P04 | P05 |
|---|---|---|---|---|---|
| dim_date | Writes | Reads | Reads | Reads | Reads |
| dim_sku | Writes | Reads | — | Reads | Reads |
| dim_warehouse | Writes | Reads | — | Reads | Reads |
| dim_supplier | Writes | — | — | — | — |
| dim_customer | — | — | Writes | — | — |
| dim_location | — | — | — | Writes | Reads |
| dim_operator | — | — | — | Writes | Reads |
| dim_model | — | Writes | — | — | — |
| dim_ab_test_registry | — | — | Writes | — | — |
| fact_daily_demand | Writes | Reads | — | — | — |
| fact_inventory_snapshot | Writes | — | — | — | Reads |
| fact_feature_store | — | Writes | — | — | — |
| fact_model_performance | — | Writes | — | — | — |
| fact_forecast | — | Writes | — | — | — |
| fact_orders | — | — | Writes | — | — |
| fact_rfm_scores | — | — | Writes | — | — |
| fact_ab_assignments | — | — | Writes | — | — |
| fact_wms_tasks | — | — | — | Writes | Reads |
| fact_slotting_history | — | — | — | Writes | — |
| fact_cooccurrence | — | — | — | Writes | — |
| fact_error_log | — | — | — | — | Writes |
| fact_inventory_accuracy | — | — | — | — | Writes |
| fact_wms_daily_kpis | — | — | — | — | Writes |
| fact_wms_monthly_kpis | — | — | — | — | Writes |
| fact_operator_daily | — | — | — | — | Writes |
| meta_pipeline_runs | — | — | — | — | Writes |
