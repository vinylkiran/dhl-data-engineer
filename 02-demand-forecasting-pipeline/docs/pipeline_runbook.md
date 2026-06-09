# Pipeline Runbook — DHL Demand Forecasting Pipeline
## Project 02 | DE Portfolio

---

## Prerequisites

Before running any pipeline component, confirm:

1. **DuckDB warehouse exists** with Project 01 tables populated:
   ```
   01-sku-segmentation-pipeline/outputs/dhl_warehouse.duckdb
   ```
   If it does not exist, run Project 01 first:
   ```bash
   cd 01-sku-segmentation-pipeline
   python etl/pipeline.py
   ```

2. **Forecasting schema exists** in the warehouse (4 new tables):
   ```bash
   cd 02-demand-forecasting-pipeline
   python schema/setup_schema.py
   ```
   Expected output: `Schema setup complete — 4 forecasting tables created.`

3. **Python dependencies installed**:
   ```bash
   pip install duckdb pandas numpy
   ```

4. **Source data present** at:
   ```
   shared/data/dhl-synthetic/daily_demand.csv
   ```

---

## Standard Run (Full Pipeline)

Run the full pipeline in one command:

```bash
cd 02-demand-forecasting-pipeline
python pipeline/forecast_pipeline.py
```

This executes all 6 steps:
1. Incremental load of new demand records
2. Feature engineering for new dates
3. Model registration (upsert 3 models into dim_model)
4. Model evaluation on Oct–Dec 2023 holdout
5. Forward forecast generation for Jan 2024
6. Pipeline run log export

Expected output files in `outputs/`:
- `forecast_output.csv` — planner-facing forecast CSV
- `pipeline_run_log.csv` — step-by-step execution log

**Custom DB path:**
```bash
python pipeline/forecast_pipeline.py --db-path /path/to/custom.duckdb
```

---

## Run Individual Components

**Incremental load only:**
```bash
python etl/incremental_load.py
# Or with custom cutoff:
python etl/incremental_load.py --sim-cutoff 2023-12-31
```

**Feature engineering only:**
```bash
python features/feature_engineering.py
```

**Feature validation:**
```bash
python features/feature_validation.py
# Outputs: outputs/feature_validation.csv
```

**Benchmarking (3 pipeline runs):**
```bash
python pipeline/benchmarking.py
# Outputs: outputs/pipeline_benchmark.csv
```

**Post-run monitoring:**
```bash
python monitoring/pipeline_monitor.py
# Outputs: outputs/monitoring_report.csv
```

---

## Interpreting the Monitoring Report

Run `python monitoring/pipeline_monitor.py` after every pipeline execution. The report prints 5 checks:

| Check | PASS | FAIL |
|---|---|---|
| `pipeline_steps_completed` | All 6 steps in pipeline_run_log.csv have status=OK | One or more steps failed or log file missing |
| `forecast_row_count` | Row count within ±20% of active_skus × warehouses × 30 | Row count too low (SKUs missing) or too high (duplicate rows) |
| `no_missing_skus` | Every active SKU has ≥1 forecast row | Some SKUs have no forecast — planner has blind spots |
| `mape_drift` | MAPE has not increased by more than 20% vs prior run | Model accuracy degraded — data distribution may have shifted |
| `feature_store_growth` | fact_feature_store has grown vs prior run | No new features computed — check incremental load |

**WARN** status on `mape_drift` means no prior run exists to compare against — this is expected on first run.

---

## Failure Handling

### `pipeline_steps_completed` FAIL

Open `outputs/pipeline_run_log.csv` and find the step with `status != OK`. Check the `error` column for the exception message, then re-run that component standalone to see the full traceback:

```bash
python etl/incremental_load.py          # if step 1 failed
python features/feature_engineering.py  # if step 2 failed
```

### `forecast_row_count` FAIL (too low)

Likely cause: some SKU-warehouse combinations have no demand in the forecast basis window (Nov–Dec 2023). Check:

```sql
SELECT COUNT(DISTINCT sku_id) FROM dim_sku WHERE active_flag = TRUE;
SELECT COUNT(DISTINCT sku_id) FROM fact_forecast;
```

If the counts differ, the missing SKUs had no demand in the last 28 days used as the rolling window seed. The pipeline fills these with 0 predicted demand — verify in `forecast_output.csv`.

### `no_missing_skus` FAIL

Run:
```sql
SELECT s.sku_id, s.abc_class
FROM dim_sku s
LEFT JOIN fact_forecast ff ON s.sku_id = ff.sku_id
WHERE s.active_flag = TRUE AND ff.forecast_id IS NULL;
```

These SKUs have no demand history or were excluded from the feature store. Check `fact_feature_store` for those SKU IDs.

### `mape_drift` FAIL (MAPE increased >20%)

This indicates model accuracy has degraded. Actions:

1. Check whether the underlying demand data distribution has changed (new SKUs, warehouse changes)
2. Review `fact_model_performance` for which models/SKUs drove the MAPE increase:
   ```sql
   SELECT model_id, sku_id, mape FROM fact_model_performance ORDER BY mape DESC LIMIT 20;
   ```
3. If drift is systemic, consider retraining with an extended holdout window or adding a new model

### `feature_store_growth` FAIL

Means the feature store did not grow after the latest pipeline run. Likely causes:

- Incremental load watermark already equals or exceeds `sim_cutoff` — no new demand records, so no new features
- Feature engineering found `existing_max` at current max date — nothing to compute

Verify watermark:
```sql
SELECT MAX(d.full_date) FROM fact_daily_demand f JOIN dim_date d ON f.date_key = d.date_key;
SELECT MAX(feature_date) FROM fact_feature_store;
```

If both dates match your data, the pipeline is correctly reporting that it is up to date.

---

## Reprocessing Historical Dates

The incremental load watermark prevents re-ingestion of already-loaded dates. To force a full reprocess:

**Option A — Reset specific tables (DuckDB SQL):**
```python
import duckdb
conn = duckdb.connect("outputs/dhl_warehouse.duckdb")
conn.execute("DELETE FROM fact_feature_store WHERE feature_date >= '2023-01-01'")
conn.execute("DELETE FROM fact_forecast")
conn.execute("DELETE FROM fact_model_performance")
conn.close()
```
Then re-run `python pipeline/forecast_pipeline.py`.

**Option B — Full reset:**
```bash
# Delete the duckdb file and re-run Project 01 then Project 02
rm outputs/dhl_warehouse.duckdb
cd ../01-sku-segmentation-pipeline && python etl/pipeline.py
cd ../02-demand-forecasting-pipeline && python schema/setup_schema.py && python pipeline/forecast_pipeline.py
```

---

## Onboarding a New Warehouse

To add a sixth warehouse:

1. Add a row to `shared/data/dhl-synthetic/warehouse_master.csv`
2. Add demand records for that warehouse to `daily_demand.csv`
3. Re-run Project 01 ETL to load the new warehouse into `dim_warehouse`
4. The incremental load will pick up new demand records for the warehouse on next run
5. Feature engineering will compute features for all (existing_sku, new_warehouse) pairs not yet in the feature store
6. Forecast pipeline will generate forecasts for the new warehouse automatically

No code changes required.

---

## Output Files Reference

| File | Updated By | Content |
|---|---|---|
| `outputs/forecast_output.csv` | forecast_pipeline.py step 5 | Best-model 30-day forward forecasts with CI, planner-ready |
| `outputs/pipeline_run_log.csv` | forecast_pipeline.py step 6 | Step name, status, rows processed, duration per step |
| `outputs/pipeline_benchmark.csv` | benchmarking.py | 3-run timing comparison vs manual baseline |
| `outputs/monitoring_report.csv` | pipeline_monitor.py | 5 health checks with PASS/FAIL/WARN |
| `outputs/feature_validation.csv` | feature_validation.py | 5 feature store integrity checks |
