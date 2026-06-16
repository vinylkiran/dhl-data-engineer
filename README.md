# DHL Supply Chain — Data Engineering Portfolio

A six-project end-to-end analytical data engineering platform built on real DHL supply chain domain knowledge. Covers the full data lifecycle: raw CSV ingestion, dimensional modelling, ML feature engineering, incremental ETL, pre-aggregation, a four-tier data quality framework, statistical anomaly detection, and a serving layer consumed by downstream dashboards.

**Stack:** Python · DuckDB · Pandas · Kimball Star Schema · SQL · Git

---

## Platform Overview

The platform ingests nine synthetic source files representing a three-warehouse DHL operation (2,000 SKUs, 60 operators, 500 customers, two years of daily demand and WMS task data). All five analytical pipelines write to a single shared DuckDB warehouse — 41 objects, 26 tables, 15 views, approximately 1.78 million fact rows.

```
Source CSVs → Python ETL → DuckDB Warehouse → Aggregation + Quality → Serving Layer → Consumers
```

Every pipeline implements: incremental loading via watermark, audit logging via `meta_pipeline_runs`, data quality checks with severity tiers, and CSV exports for downstream consumption.

---

## Projects

| # | Project | Description | Key Output |
|---|---|---|---|
| 01 | [SKU Segmentation](./01-sku-segmentation-pipeline/) | ABC/velocity classification across 2,000 SKUs; builds dim_sku, dim_warehouse, fact_daily_demand, fact_inventory_snapshot | `sku_segmentation_output.csv` — ABC class, velocity class, reorder recommendation per SKU |
| 02 | [Demand Forecasting](./02-demand-forecasting-pipeline/) | Three-model comparison (MA14d, MA28d, seasonal naive); feature engineering; 90-day forward forecasts with 80% CI; MAPE/RMSE holdout evaluation | `forecast_output.csv` — 149,760 forecast rows with confidence intervals |
| 03 | [Customer Pipeline](./03-customer-pipeline/) | Full-history order ingestion; RFM scoring with SCD2 expire-and-replace; A/B test infrastructure; commercial segment views | `rfm_scores.csv` — Champions, Loyal, At Risk, Lost segment assignments |
| 04 | [Warehouse Operations](./04-warehouse-operations-pipeline/) | WMS task ETL with SHA-256 operator anonymisation; SCD2 location dimension; slotting recommendations with estimated time savings; co-occurrence lift scoring | `slotting_recommendations.csv` — 882 SKU moves with est_annual_minutes_saved |
| 05 | [WMS Data Warehouse](./05-wms-data-warehouse/) | Pre-aggregated KPI layer (daily/monthly/operator); 4-tier DQ framework; 90-day rolling statistical anomaly detection; 7-view serving layer; query benchmark | `v_warehouse_comparison.csv`, `v_coaching_list.csv`, `anomaly_flags.csv` |
| 06 | [DE Artifacts](./06-de-artifacts/) | Platform architecture, tech decision records, master data lineage, complete table inventory, coding standards, onboarding guide | Reference documentation for the full platform |

---

## Tech Stack

| Layer | Technology | Rationale |
|---|---|---|
| Database | DuckDB (embedded columnar) | Zero infrastructure; vectorised GROUP BY on 219K rows in <100ms; single portable `.duckdb` file |
| ETL | Python + Pandas | Full control over incremental logic, SCD2, watermarking, operator anonymisation |
| Schema | Kimball Star Schema | Conformed dimensions across all five subject areas; SCD2 where history matters |
| Quality | Custom DQ framework | Four severity tiers (CRITICAL/HIGH/MEDIUM/LOW); exports `dq_report.csv` per run |
| Anomaly detection | NumPy rolling statistics | 90-day rolling σ for accuracy/volume; 30-day personal baseline for operator drops |
| Serving | DuckDB views + CSV | 15 views; 7 in Project 05 alone; benchmarked against raw queries |
| Version control | Git + GitHub | One commit per project; credential-safe push via `/tmp` credential store |

---

## Synthetic Dataset

The source data in `shared/data/dhl-synthetic/` was generated to mirror real DHL operational characteristics:

- **3 warehouses** — NJ01 (New Jersey), IL02 (Illinois), TX03 (Texas)
- **2,000 SKUs** across 5 product categories (PHM, ELC, FDG, CLN, SPT)
- **60 warehouse operators** — anonymised at ingest; never stored in identifiable form
- **500 customers** with 2-year order history (68,941 orders)
- **219,000 WMS task records** — Picks, Putaways, Replenishments, Cycle Counts across Jan 2022–Dec 2023
- **574,509 daily demand records** — one row per SKU per warehouse per day

All data is synthetic. No real DHL customer, employee, or operational data is included.

---

## Methodology

**Why not dbt or Airflow?** The custom Python approach deliberately exposes the underlying engineering logic — watermark management, SCD2 implementation, incremental load patterns, quality framework design — that these tools abstract away. For a portfolio demonstrating data engineering depth rather than tool configuration, this is the correct trade-off. `06-de-artifacts/architecture/tech_stack_decisions.md` documents every major technology decision and what would change at production scale.

**Why DuckDB instead of Postgres or Snowflake?** DuckDB runs embedded with zero infrastructure. The entire 26-table warehouse ships as a single 193MB file. A production DHL platform would use Snowflake or Databricks for multi-user concurrency and compute/storage separation — TDR-01 in the tech decisions document explains the full comparison.

**Honest benchmark results:** Project 05's query benchmark shows an average 1.2× speedup from pre-aggregation. One view (`v_coaching_list`) is actually 0.1× — slower than the raw query — because `fact_operator_daily` at 162K rows is larger than the 14-day filtered raw query for that specific use case. This is documented as-is rather than hidden.

---

## Platform Stats

| Metric | Value |
|---|---|
| Total warehouse objects | 41 (26 tables + 15 views) |
| Total fact rows | ~1,783,803 |
| Dimension tables | 9 |
| Raw fact tables | 13 |
| Pre-aggregated fact tables | 3 |
| Serving views | 15 |
| Pipelines | 5 |
| DQ checks (Project 05) | 10 across 4 severity tiers |
| Anomaly flags detected | 817 (704 operator drops, 109 accuracy drops, 4 error spikes) |
| Largest table | fact_daily_demand / fact_feature_store (574,509 rows each) |

---

## Reference Documents

All platform documentation is in `06-de-artifacts/`:

- `architecture/platform_architecture.md` — End-to-end architecture diagram, subject area breakdown, design principles, guide to adding a new pipeline
- `architecture/tech_stack_decisions.md` — Five technology decision records (TDRs) covering DuckDB, Python ETL, star schema, operator anonymisation, and what changes at production scale
- `lineage/master_data_lineage.md` — Complete source-to-consumer lineage for five major output artifacts
- `lineage/table_inventory.md` — Every table and view: type, project, row count, refresh pattern, primary consumer
- `standards/de_coding_standards.md` — Naming conventions, SQL patterns, Python ETL standards, DQ requirements, Git commit standards
- `standards/onboarding_guide.md` — Environment setup, working copy rule, how to run each pipeline, how to interpret DQ reports, troubleshooting
