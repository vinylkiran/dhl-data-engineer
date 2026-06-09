# Pipeline Architecture — DHL Demand Forecasting Pipeline
## Project 02 | DE Portfolio

---

## Architecture Summary

The Project 02 demand forecasting pipeline extends the Project 01 star schema warehouse with four new tables and a fully automated Python orchestration layer. It replaces a manual 2-3 day analyst process with a sub-minute pipeline that runs incremental ETL, computes a feature store, evaluates three forecast models on a holdout period, generates 30-day forward forecasts, and writes planner-ready CSV output.

---

## Why Incremental Load (vs Full Reload)

Project 01 used a full truncate-and-reload pattern — appropriate for the initial historical backfill where all data is replaced on each run. Project 02 switches to an incremental watermark-based append for `fact_daily_demand` and `fact_feature_store` for three reasons:

**Volume growth**: In production, daily demand tables accumulate millions of rows per year. A full reload of 2+ years of history on every weekly run is wasteful and error-prone (DROP TABLE during a partial load leaves no data).

**Auditability**: An append-only fact table preserves the full load history. The `etl_loaded_at` and `etl_source_file` columns on every row record exactly when and from where each record was loaded.

**Recovery**: If a run fails mid-load, the watermark has not advanced — the next run re-processes the same date range. There are no partial states to clean up.

The watermark is read via `MAX(d.full_date)` from `fact_daily_demand JOIN dim_date`, making it a query over committed data rather than a metadata table that could get out of sync.

---

## Why DuckDB

DuckDB was chosen for this portfolio project over alternatives (PostgreSQL, SQLite, Spark) for the following reasons:

**Single-file portability**: The entire warehouse lives in one `.duckdb` file. No server process, no connection strings, no infrastructure to provision — a recruiter or reviewer can clone the repo and run the pipeline immediately.

**Columnar analytics performance**: DuckDB is a columnar OLAP engine. Aggregation queries (rolling averages, group-by SKU/warehouse, holdout evaluation) run 5-20× faster than SQLite on the same hardware with no configuration.

**pandas integration**: `conn.execute(...).df()` and `conn.register("_staging", df)` allow zero-copy DataFrame interchange. Feature engineering can use pandas for row-wise operations, then push results back to DuckDB in bulk — no ORM or manual type mapping required.

**Production analogy**: DuckDB's SQL dialect is ANSI-compatible and its patterns (watermarks, bulk INSERT, window functions) translate directly to Redshift, Snowflake, or BigQuery. It demonstrates the same engineering decisions a production pipeline would make.

---

## Why These Three Models

The three baseline models were chosen deliberately as a benchmark tier, not a production final answer:

| Model | Logic | Why Included |
|---|---|---|
| `moving_average_14d` | 14-day rolling mean of prior demand | Short-window mean — captures recent trend, sensitive to spikes |
| `moving_average_28d` | 28-day rolling mean of prior demand | Long-window mean — smoother, less reactive to short-term noise |
| `seasonal_naive` | Demand from 28 days ago (lag_28) | Captures weekly periodicity; strong for regular-cycle SKUs |

These are interpretable, parameter-free, and compute in milliseconds. They establish a minimum performance bar. Any ML model added to the registry must beat them on holdout MAPE to justify the added complexity.

The models use pre-computed feature store columns (`rolling_avg_14`, `rolling_avg_28`, `lag_28`) rather than re-reading raw demand — demonstrating that the feature store pays for its storage cost in query simplicity.

---

## Feature Store Design

The `fact_feature_store` table is a wide denormalised table storing all engineered features for every (sku_id, warehouse_id, feature_date) triple. Design decisions:

**Pre-computation over on-the-fly calculation**: Feature values are computed once and stored. The forecast pipeline reads pre-computed features rather than re-computing rolling windows at query time. This is the correct pattern for production systems where multiple models may consume the same features.

**Append-only with watermark**: Like `fact_daily_demand`, the feature store only stores dates newer than its existing max date. This means feature engineering is idempotent — running it twice does not duplicate rows.

**SKU mean fill for edge dates**: Lag and rolling features at the start of a SKU's history (insufficient prior data) are filled with the SKU's historical mean. This is a deliberate choice over NULL/0: NULL would cause model output to be NULL, and 0 would bias forecasts downward for new SKUs. The mean is a neutral imputation that keeps the pipeline running and is explicitly flagged in `feature_validation.py` (lag1 correctness check allows ≤5% mismatch).

**Seasonal encoding**: Season is stored as a string category (Winter/Spring/Summer/Autumn) rather than one-hot encoded, to keep the schema model-agnostic. ML models that need numeric encoding would apply it at training time.

---

## Holdout Evaluation Design

The model evaluation uses a time-series holdout split, not random cross-validation:

- **Train**: Jan 2022 – Sep 2023 (all demand and feature history)
- **Holdout**: Oct–Dec 2023 (last quarter, never used in feature computation)

This respects temporal ordering — the model is tested on data it could not have seen during feature engineering, mirroring production conditions where forecasts are always made for future dates.

MAPE is the primary metric because it is scale-invariant (percentage, not absolute units), making it comparable across SKUs with vastly different demand volumes. RMSE, MAE, and bias are computed as supplementary diagnostics.

Best model selection is done per ABC class (A/B/C), acknowledging that high-value A-class SKUs may have different demand patterns than low-value C-class SKUs.

---

## Confidence Interval Approach

80% confidence intervals are computed using the normal approximation:

```
lower_80 = max(0, predicted - 1.282 × std_dev)
upper_80 = predicted + 1.282 × std_dev
```

`std_dev` is the standard deviation of forecast errors on the holdout period. This is a simplification — it assumes errors are normally distributed and stationary. In production, per-SKU or quantile regression CIs would be more accurate. The 80% level was chosen (over 95%) because demand planning typically operates at service level targets of 80-90%; extremely wide CIs reduce their operational usefulness.

---

## Late-Arriving Data

The incremental watermark pattern handles late-arriving data conservatively: it does not automatically reprocess historical dates. If a corrected demand record arrives for a date already loaded (e.g., a December shipment quantity is revised in January), the pipeline will not re-ingest it unless the database is reset and re-run from scratch.

In production, a late-arrival correction table would handle this. For the portfolio scope, it is documented as a known limitation (see `docs/pipeline_runbook.md`).

---

## Adding a New Model

To add a fourth forecasting model:

1. Add a new record to `dim_model` in `forecast_pipeline.py` Step 3 (or insert directly via SQL)
2. Add a new function `predict_<model_name>(feature_df)` in `forecast_pipeline.py` that returns a Series of predictions from feature store columns
3. Add the model name to the `MODELS` dict in Step 4 so evaluation runs automatically
4. The best-model selection in Step 5 will automatically consider the new model

No schema changes are required — `fact_forecast` and `fact_model_performance` are model-agnostic via `model_id`.

---

## Performance Characteristics

On the synthetic dataset (200 SKUs × 5 warehouses × 730 days = ~730,000 demand rows):

| Stage | Typical Runtime |
|---|---|
| Incremental load (first run) | 1-3 seconds |
| Incremental load (already current) | < 0.1 seconds |
| Feature engineering (full rebuild) | 10-30 seconds |
| Feature engineering (incremental) | 1-5 seconds |
| Model evaluation (3 models) | 2-5 seconds |
| Forecast generation (30 days) | 1-3 seconds |
| Full pipeline (first run) | 15-40 seconds |
| Full pipeline (incremental) | 3-10 seconds |

Benchmarked against a manual process baseline of 2-3 working days (960-1440 minutes). See `outputs/pipeline_benchmark.csv` for actual measured runtimes.

---

## Known Limitations

| Limitation | Impact | Planned Fix |
|---|---|---|
| `dim_sku.xyz_class` is NULL (not in sku_master.csv) | Forecast segmentation uses ABC only | Populate from daily_demand.csv on next ETL run |
| Confidence intervals use global std across all SKUs | CIs are too wide for high-volume, too narrow for low-volume SKUs | Per-SKU CI computation |
| No per-SKU cross-validation | Single holdout may not represent all SKU patterns | Rolling origin cross-validation |
| No external feature inputs | No promotions, holidays, or external demand signals | Extend feature store with dim_calendar events |
| Watermark does not reprocess late arrivals | Corrected historical data not reflected in forecasts | Add late-arrival correction table |
