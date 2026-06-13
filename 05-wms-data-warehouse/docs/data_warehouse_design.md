# WMS Data Warehouse Design
## DHL Data Engineer Portfolio — Project 05

---

## Overview

Project 05 builds the reporting and analytics layer on top of the WMS operational data loaded in Project 04. While Project 04 was concerned with operational data ingestion (ETL of raw WMS tasks, slotting, co-occurrence), Project 05 is concerned with making that data queryable at scale for dashboards, management reporting, and operational monitoring.

The core architectural decision is to separate the **operational layer** (raw fact_wms_tasks with 219,000 rows) from the **reporting layer** (pre-aggregated summary tables and optimised serving views). This is the standard data warehouse pattern and is motivated in the Performance Rationale section below.

---

## Full Star Schema: WMS Reporting Layer

### Dimension Tables (inherited from Projects 01–04)

| Table | Rows | Description |
|---|---|---|
| dim_date | 1,096 | Calendar dimension, Jan 2021–Dec 2023 |
| dim_sku | 2,000 | SKU master with ABC class, category, active flag |
| dim_warehouse | 3 | NJ01, IL02, TX03 with region and capacity |
| dim_supplier | 50 | Supplier master with lead time and contract tier |
| dim_location | 2,640 | Warehouse locations with SCD Type 2 history |
| dim_operator | 60 | Anonymised operator dimension with cohort |
| dim_customer | ~1,200 | Customer master with lifetime metrics |

### Fact Tables — Operational (Projects 01–04)

| Table | Rows | Grain |
|---|---|---|
| fact_daily_demand | ~180,000 | One row per SKU per day |
| fact_wms_tasks | 219,000 | One row per WMS task execution |
| fact_orders | ~5,000 | One row per customer order |
| fact_rfm_scores | ~1,200 | SCD2 RFM score history per customer |
| fact_slotting_history | 882 | Slotting recommendation per SKU-warehouse pair |
| fact_cooccurrence | 600 | Top lift pairs per warehouse |

### Fact Tables — WMS Data Warehouse (Project 05)

| Table | Rows | Grain |
|---|---|---|
| fact_wms_daily_kpis | ~1,080 | One row per date / warehouse / shift |
| fact_wms_monthly_kpis | ~36 | One row per year-month / warehouse |
| fact_operator_daily | ~5,400 | One row per operator / warehouse / date / shift |
| fact_error_log | ~21,900 | One row per error event |
| fact_inventory_accuracy | varies | Monthly per warehouse per SKU category |
| meta_pipeline_runs | grows | One row per pipeline execution |

### Key Relationships

```
dim_warehouse (1) ──── (N) fact_wms_daily_kpis
dim_warehouse (1) ──── (N) fact_wms_monthly_kpis
dim_warehouse (1) ──── (N) fact_operator_daily
dim_warehouse (1) ──── (N) fact_error_log
dim_operator  (1) ──── (N) fact_operator_daily    [via operator_surrogate_id]
dim_operator  (1) ──── (N) fact_error_log          [via operator_id]
dim_sku       (1) ──── (N) fact_error_log          [via sku_id]
```

Cardinalities: All warehouse-level fact tables have many-to-one relationships to dim_warehouse. fact_operator_daily has a compound natural key of (operator_id, warehouse_id, task_date, shift) enforced by UNIQUE INDEX.

---

## Why Pre-Aggregated Summary Tables?

The fundamental problem with querying `fact_wms_tasks` directly for dashboard use is query latency vs data freshness:

| Approach | Latency | Freshness | When it fails |
|---|---|---|---|
| Direct GROUP BY on fact_wms_tasks | High (scans 219k rows) | Real-time | Dashboard with 10 concurrent users |
| Pre-aggregated daily/monthly tables | Low (scans ~1,080 rows) | Refreshed once daily | If someone needs sub-daily freshness |
| DuckDB views on pre-aggregated tables | Lowest (pre-computed joins) | Same as aggregates | Never for daily reporting use cases |

In the benchmark results (`outputs/query_benchmark.csv`), pre-aggregated queries consistently outperform raw GROUP BY queries. The speedup is most pronounced for multi-month trend queries (v_kpi_trends_12m) because the monthly aggregation collapses 219,000 rows into 36 before any filtering or calculation occurs.

### Trade-offs accepted

Pre-aggregation has one real cost: the aggregation job must run after each ETL load. If ETL runs at 03:00 and aggregations run at 03:30, the dashboard is 30 minutes stale relative to raw data. For daily operational reporting this is acceptable. For real-time monitoring dashboards, the architecture would need OLAP streaming ingestion (e.g., Apache Flink → Delta Lake) — outside the scope of this portfolio project.

---

## SCD Type 2 for Dimension Tables

Inherited from Project 04. `dim_location` tracks warehouse location attribute history using the valid_from / valid_to / is_current pattern. Two queries are important:

**Current state:**
```sql
SELECT * FROM dim_location WHERE is_current = TRUE;
```

**Point-in-time reconstruction (for historical accuracy analysis):**
```sql
SELECT * FROM dim_location
WHERE valid_from <= '2023-06-01'
  AND (valid_to IS NULL OR valid_to > '2023-06-01');
```

SCD2 was not applied to `dim_operator` because operator anonymisation is applied at ETL time — operator identities do not change retroactively. If a raw operator ID were reassigned (a hypothetical), a new anonymised hash would be generated and treated as a distinct operator.

---

## meta_pipeline_runs: Pipeline Observability

Every pipeline execution that touches the WMS warehouse writes a record to `meta_pipeline_runs`. The table captures:

- **run_id**: auto-increment primary key; never reused
- **pipeline_name**: allows filtering by pipeline for watermark queries
- **run_start / run_end / duration_seconds**: timing data for SLA monitoring
- **status**: `running` while in flight, then updated to `success` or `failed`
- **rows_processed / rows_inserted / rows_updated**: volume metrics for anomaly detection on the pipeline itself ("why did ETL insert 10x normal rows today?")
- **error_message**: stored for debugging; NULL on success

### Why pipeline observability matters

Without `meta_pipeline_runs`, incremental load is impossible to implement correctly. The watermark pattern relies on: "what was the last time this pipeline succeeded?" If we store the watermark in application code or environment variables, it is lost on restart and the pipeline performs a full reload.

Storing run history in the warehouse also creates an audit trail for compliance — if a downstream report is questioned, we can trace exactly which pipeline run produced the data and when.

---

## Serving Layer Design

The serving layer consists of 7 DuckDB views and their corresponding CSV exports. The design principle is **consumer-specific views**: each view is optimised for one dashboard panel or one downstream consumer, rather than a generic view that every consumer queries and filters differently.

| View | Consumer | Time Scope |
|---|---|---|
| v_network_kpis_current_month | Executive dashboard | Current calendar month |
| v_warehouse_comparison | Ops manager dashboard | Rolling 30 days |
| v_kpi_trends_12m | Trend analysis / BA | Rolling 12 months |
| v_operator_leaderboard | Warehouse manager | Current month |
| v_error_patterns | Quality analyst | Rolling 30 days + prior 30 |
| v_coaching_list | HR / Training manager | Rolling 14 days |
| v_high_performer_list | HR / Recognition | Rolling 30 days |

### Decoupling the dashboard from raw data

The serving layer creates a stable API contract between the engineering team (who own the data warehouse) and the analytics team (who build dashboards on top of it). If the underlying table structure changes — for example, if `fact_wms_daily_kpis` gains new columns — the views can be updated without any change to the dashboard SQL.

This is particularly valuable when multiple BI tools (Tableau, Power BI, Looker) consume the same views. Without the decoupling layer, each tool would need to be updated separately when the underlying schema changes.
