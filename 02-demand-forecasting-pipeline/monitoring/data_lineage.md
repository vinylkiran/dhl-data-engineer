# Data Lineage — DHL Demand Forecasting Pipeline
## Project 02 | DE Portfolio

---

## Overview

This document traces the full data lineage from source CSV files through to the final forecast output consumed by planners. Every transformation, join, and persistence step is documented, making it possible to trace any forecast value back to its source demand record.

---

## Lineage Diagram

```
SOURCE LAYER
──────────────────────────────────────────────────────────────────
shared/data/dhl-synthetic/
  ├── daily_demand.csv           (1,095 days × SKUs × warehouses)
  ├── sku_master.csv             (200 SKUs)
  ├── warehouse_master.csv       (5 warehouses)
  ├── supplier_catalogue.csv     (suppliers)
  └── date_spine.csv             (calendar 2022–2024)

       │
       │  [ETL: incremental_load.py]
       │  Watermark check on fact_daily_demand.max(date_key)
       │  Filter: Date > watermark AND Date ≤ sim_cutoff
       │  Surrogate key assignment: COALESCE(MAX(demand_key),0) + offset
       │
       ▼

WAREHOUSE LAYER (DuckDB: dhl_warehouse.duckdb)
──────────────────────────────────────────────────────────────────
  dim_date          ← date_spine.csv         (730 rows)
  dim_sku           ← sku_master.csv         (200 rows)
  dim_warehouse     ← warehouse_master.csv   (5 rows)
  dim_supplier      ← supplier_catalogue.csv
  fact_daily_demand ← daily_demand.csv       (incremental append)

       │
       │  [feature_engineering.py]
       │  Group by (sku_id, warehouse_id)
       │  Compute: lag_1/7/14/28, rolling_avg_7/14/28, rolling_std_7/14
       │  Compute: day_of_week, week_of_year, month, quarter, is_weekend, season
       │  Edge fill: NaN → SKU mean (insufficient history)
       │  Filter: feature_date > existing watermark (incremental)
       │  Batch INSERT in 50k-row chunks
       │
       ▼

FEATURE LAYER
──────────────────────────────────────────────────────────────────
  fact_feature_store  (feature_id, sku_id, warehouse_id, feature_date,
                       lag_*, rolling_avg_*, rolling_std_*, calendar_*,
                       abc_class, xyz_class)

       │
       │  [forecast_pipeline.py — Step 1: Incremental Load]
       │  Delegates to incremental_load.run_incremental_load()
       │
       │  [forecast_pipeline.py — Step 2: Feature Engineering]
       │  Delegates to feature_engineering.run_feature_engineering()
       │
       │  [forecast_pipeline.py — Step 3: Model Registration]
       │  Upserts 3 model records into dim_model
       │    moving_average_14d  — 14-day rolling mean of lag-shifted demand
       │    moving_average_28d  — 28-day rolling mean of lag-shifted demand
       │    seasonal_naive      — demand from same day 28 days prior (lag_28)
       │
       │  [forecast_pipeline.py — Step 4: Model Evaluation]
       │  Holdout period: Oct 1 – Dec 31, 2023
       │  Training period: Jan 1, 2022 – Sep 30, 2023
       │  Reads: fact_feature_store columns (rolling_avg_14, rolling_avg_28, lag_28)
       │  Computes: MAPE, RMSE, MAE, Bias per (model, sku_id, warehouse_id)
       │  Writes: fact_model_performance
       │
       │  [forecast_pipeline.py — Step 5: Forward Forecast Generation]
       │  Horizon: Jan 1 – Jan 30, 2024 (30 days)
       │  Basis: Nov–Dec 2023 demand (last 28 days as rolling window seed)
       │  Best model selected per ABC class by lowest avg MAPE
       │  Confidence intervals: 80% CI using z=1.282, std from eval period
       │  Writes: fact_forecast
       │  Exports: outputs/forecast_output.csv (planner-facing CSV)
       │
       │  [forecast_pipeline.py — Step 6: Pipeline Run Log]
       │  Writes: outputs/pipeline_run_log.csv
       │
       ▼

MODEL LAYER
──────────────────────────────────────────────────────────────────
  dim_model             (model_id, model_name, model_type, parameters, description)
  fact_model_performance (perf_id, model_id, sku_id, warehouse_id,
                          holdout_start, holdout_end, mape, rmse, mae, bias,
                          n_eval_points, evaluated_at)
  fact_forecast          (forecast_id, model_id, sku_id, warehouse_id,
                          forecast_date, forecast_generated_at,
                          predicted_demand, lower_bound_80, upper_bound_80,
                          mape_on_holdout, model_name, abc_class)

       │
       │  [pipeline_monitor.py]
       │  Check 1: pipeline_run_log.csv — all steps OK
       │  Check 2: fact_forecast row count ≈ skus × warehouses × 30
       │  Check 3: no active SKUs missing from fact_forecast
       │  Check 4: MAPE drift vs prior run ≤ 20%
       │  Check 5: fact_feature_store has grown vs prior run
       │  Exports: outputs/monitoring_report.csv
       │
       │  [feature_validation.py]
       │  5 checks on fact_feature_store integrity
       │  Exports: outputs/feature_validation.csv
       │
       │  [benchmarking.py]
       │  3 pipeline runs, compare vs 2-3 day manual baseline
       │  Exports: outputs/pipeline_benchmark.csv
       │
       ▼

CONSUMER LAYER
──────────────────────────────────────────────────────────────────
  outputs/forecast_output.csv   ← Demand planners, S&OP process
  outputs/pipeline_benchmark.csv ← Management / BA impact reporting
  outputs/monitoring_report.csv  ← Data engineering / ops team
  outputs/feature_validation.csv ← Data quality review
```

---

## Table-Level Lineage

### `fact_daily_demand`

| Column | Source | Transformation |
|---|---|---|
| demand_key | Computed | `COALESCE(MAX(demand_key),0) + row_offset` |
| date_key | dim_date.date_key | Left join on `Date = full_date` |
| sku_key | dim_sku.sku_key | Left join on `SKU_ID = sku_id` |
| warehouse_key | dim_warehouse.warehouse_key | Left join on `Warehouse_ID = warehouse_id` |
| quantity_demanded | daily_demand.Quantity_Demanded | Cast to int, NaN→0 |
| quantity_fulfilled | daily_demand.Quantity_Fulfilled | Cast to int, NaN→0 |
| quantity_unfulfilled | Computed | `max(0, quantity_demanded - quantity_fulfilled)` |
| stockout_flag | daily_demand.Stockout_Flag | Cast to bool |
| revenue | daily_demand.Revenue | Cast to float, NaN→0 |
| fill_rate | Computed | `quantity_fulfilled / quantity_demanded` (NULL if denominator=0) |

### `fact_feature_store`

| Column | Source | Transformation |
|---|---|---|
| feature_id | Computed | Sequential from `MAX(feature_id)+1` |
| lag_1..28 | fact_daily_demand.quantity_demanded | `qty.shift(n)`, NaN→SKU mean |
| rolling_avg_7/14/28 | fact_daily_demand.quantity_demanded | `qty.shift(1).rolling(w, min_periods=1).mean()`, NaN→SKU mean |
| rolling_std_7/14 | fact_daily_demand.quantity_demanded | `qty.shift(1).rolling(w, min_periods=2).std()`, NaN→0 |
| day_of_week | feature_date | `pd.Timestamp.dayofweek` (0=Monday) |
| week_of_year | feature_date | `pd.Timestamp.isocalendar().week` |
| month | feature_date | `pd.Timestamp.month` |
| quarter | feature_date | `pd.Timestamp.quarter` |
| is_weekend | day_of_week | `day_of_week in [5,6]` |
| season | month | `{3-5:Spring, 6-8:Summer, 9-11:Autumn, else:Winter}` |
| abc_class / xyz_class | dim_sku | Passed through from demand join |

### `fact_forecast`

| Column | Source | Transformation |
|---|---|---|
| forecast_id | Computed | Sequential |
| model_id | dim_model.model_id | Best model per ABC class (lowest MAPE) |
| predicted_demand | fact_feature_store | `rolling_avg_14`, `rolling_avg_28`, or `lag_28` depending on model |
| lower_bound_80 | Computed | `max(0, predicted_demand - 1.282 × std_dev)` |
| upper_bound_80 | Computed | `predicted_demand + 1.282 × std_dev` |
| mape_on_holdout | fact_model_performance | Copied from evaluation results |

### `fact_model_performance`

| Column | Source | Transformation |
|---|---|---|
| mape | Computed | `mean(|actual - predicted| / actual)` × 100, over holdout period |
| rmse | Computed | `sqrt(mean((actual - predicted)²))` |
| mae | Computed | `mean(|actual - predicted|)` |
| bias | Computed | `mean(predicted - actual)` (positive = over-forecast) |

---

## Incremental Load Watermarking

The watermark pattern prevents double-loading and supports idempotent re-runs:

1. `get_watermark()` reads `MAX(d.full_date)` from `fact_daily_demand JOIN dim_date`
2. If no rows exist, watermark defaults to `2021-12-31` (load everything)
3. Source CSV filtered to `Date > watermark AND Date ≤ sim_cutoff`
4. After load, new watermark = `sim_cutoff`

The same pattern is replicated in `feature_engineering.py` using `MAX(feature_date)` from `fact_feature_store`.

---

## Data Quality Gates

Before any data reaches the consumer layer, it passes through:

1. **Extract validation** (Project 01 `validation.py`): 26 checks on raw data completeness and referential integrity
2. **Feature validation** (`feature_validation.py`): 5 checks on feature store correctness
3. **Pipeline monitoring** (`pipeline_monitor.py`): 5 checks on pipeline execution health and output volume

---

## Known Lineage Gaps

| Gap | Impact | Mitigation |
|---|---|---|
| `dim_sku.xyz_class` is NULL for all rows — XYZ class is not in sku_master.csv, only in daily_demand.csv | Forecast segmentation uses ABC class only | Feature store carries xyz_class from demand join; dim_sku not updated |
| Confidence intervals use global std over all SKUs, not per-SKU | Wide CIs for low-volume SKUs, narrow for high-volume | Per-SKU CI is a planned enhancement |
| No audit trail for which demand records contributed to a given forecast | Cannot trace forecast_id → demand_key | feature_id linkage through fact_feature_store provides indirect trace |
