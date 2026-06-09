# Model Registry — DHL Demand Forecasting Pipeline
## Project 02 | DE Portfolio

---

## Overview

The model registry (`dim_model`) stores metadata for every forecasting model that has been evaluated against the holdout period. This document describes each model currently registered, its mathematical definition, strengths and weaknesses, and guidance for the data science team on when to replace it with a more sophisticated approach.

---

## Registered Models

### Model 1 — `moving_average_14d`

| Field | Value |
|---|---|
| `model_name` | moving_average_14d |
| `model_type` | statistical |
| `parameters` | `{"window": 14, "feature_column": "rolling_avg_14"}` |
| `description` | 14-day rolling mean of prior demand |

**What it does**: Predicts tomorrow's demand as the arithmetic mean of the 14 most recent demand observations. Uses the pre-computed `rolling_avg_14` column from `fact_feature_store`, which is a `shift(1).rolling(14).mean()` — meaning the window ends the day before the forecast date (no data leakage).

**Mathematical definition**:
```
prediction(t) = (1/14) × Σ demand(t-1), demand(t-2), ..., demand(t-14)
```

**When to use**: Good baseline for SKUs with stable, trending demand where recent history is representative. Responds faster to demand changes than the 28-day window.

**Strengths**:
- Simple, fully interpretable
- Responds to recent demand shifts within 2 weeks
- No training required — computes from pre-computed feature column

**Weaknesses**:
- Sensitive to short-term spikes (a one-week promotion inflates the forecast for 14 days after it ends)
- Cannot capture weekly seasonality
- Tends to lag actual demand in strongly trending series

**Best performing ABC class**: Typically performs well for Class B SKUs with moderate demand volume and moderate variability.

**DS team replacement criteria**: Replace with a model that achieves at least 10% lower MAPE on holdout for the same ABC class. Consider SARIMA(p,d,q)(P,D,Q,7) for weekly seasonality, or LightGBM with lag features for non-linear patterns.

---

### Model 2 — `moving_average_28d`

| Field | Value |
|---|---|
| `model_name` | moving_average_28d |
| `model_type` | statistical |
| `parameters` | `{"window": 28, "feature_column": "rolling_avg_28"}` |
| `description` | 28-day rolling mean of prior demand |

**What it does**: Same as `moving_average_14d` but averages over a 28-day window. Uses `rolling_avg_28` from the feature store.

**Mathematical definition**:
```
prediction(t) = (1/28) × Σ demand(t-1), demand(t-2), ..., demand(t-28)
```

**When to use**: Best for stable, high-volume SKUs where demand does not change rapidly week-to-week. Provides a smoother forecast that is less affected by short-term promotional noise.

**Strengths**:
- Smooths out short-term noise more aggressively than the 14-day window
- More stable forecast — planners can commit to procurement based on it
- Covers a full 4-week cycle, capturing monthly rhythm

**Weaknesses**:
- Slow to respond to genuine demand shifts — takes up to 4 weeks for a demand change to fully propagate
- Over-smooths SKUs with genuine weekly seasonality
- Accumulates error in trending series

**Best performing ABC class**: Typically best for Class A SKUs — high-value, high-volume items where stability is prioritised over responsiveness.

**DS team replacement criteria**: Replace when a model achieves ≥10% lower MAPE with ≤50% wider confidence intervals. For A-class SKUs specifically, lower bias matters as much as lower MAPE — over-forecasting drives excess inventory for expensive items.

---

### Model 3 — `seasonal_naive`

| Field | Value |
|---|---|
| `model_name` | seasonal_naive |
| `model_type` | statistical |
| `parameters` | `{"lag": 28, "feature_column": "lag_28"}` |
| `description` | Demand from 28 days ago (4-week seasonal lag) |

**What it does**: Predicts tomorrow's demand as the actual demand observed exactly 28 days ago. Uses the pre-computed `lag_28` column from `fact_feature_store`. Captures 4-week seasonal cycles.

**Mathematical definition**:
```
prediction(t) = demand(t - 28)
```

**When to use**: Best for SKUs with strong, stable weekly patterns — where demand on a given day of week is consistent 4 weeks apart. Naturally handles promotions that repeat on the same cadence.

**Strengths**:
- Captures 4-week (monthly) seasonality by design
- Zero parameter tuning required
- Outperforms moving averages on strongly seasonal SKUs
- Interpretable: "we expect demand to match what we saw 4 weeks ago"

**Weaknesses**:
- Single data point — no averaging, so outliers in demand 28 days ago propagate directly to the forecast
- Cannot adapt to trend (a growing or declining SKU will persistently under- or over-forecast)
- Assumes 4-week periodicity; performs poorly for SKUs with 7-day or irregular cycles

**Best performing ABC class**: Often strongest for Class C SKUs — low-value, low-volume items with irregular but somewhat cyclical demand, where simpler forecasts are more appropriate.

**DS team replacement criteria**: Replace with a proper seasonal decomposition model (STL + ARIMA, or Prophet) when the seasonal period is not exactly 28 days or when the SKU has a strong trend component.

---

## Model Selection Logic

The pipeline selects the best model per ABC class based on lowest average MAPE across the Oct–Dec 2023 holdout period:

```python
# From forecast_pipeline.py Step 5
best_per_class = (
    perf_df
    .groupby(["abc_class", "model_name"])["mape"]
    .mean()
    .reset_index()
    .sort_values("mape")
    .groupby("abc_class")
    .first()
    .reset_index()
)
```

This means:
- Class A SKUs will use whichever of the 3 models had the lowest avg MAPE across all A-class SKUs
- Same for Class B and C
- All SKUs within an ABC class use the same model (not per-SKU best model)

Per-SKU model selection is a planned enhancement — it would reduce average MAPE but makes the forecast output harder to explain to planners ("why does this SKU use a different model than the one next to it?").

---

## Adding a New Model

1. Insert a record into `dim_model`:
   ```sql
   INSERT INTO dim_model (model_id, model_name, model_type, parameters, description, created_at)
   VALUES (4, 'exponential_smoothing', 'statistical',
           '{"alpha": 0.3, "feature_columns": ["lag_1", "rolling_avg_7"]}',
           'Simple exponential smoothing with alpha=0.3',
           CURRENT_TIMESTAMP);
   ```

2. Add a predict function in `forecast_pipeline.py`:
   ```python
   def predict_exponential_smoothing(feature_df):
       alpha = 0.3
       # alpha × lag_1 + (1-alpha) × rolling_avg_7
       return alpha * feature_df["lag_1"] + (1 - alpha) * feature_df["rolling_avg_7"]
   ```

3. Register it in the `MODELS` dict in `run_pipeline()`:
   ```python
   MODELS = {
       "moving_average_14d": predict_ma14,
       "moving_average_28d": predict_ma28,
       "seasonal_naive": predict_seasonal_naive,
       "exponential_smoothing": predict_exponential_smoothing,  # new
   }
   ```

4. The evaluation loop, best-model selection, and forecast generation all handle the new model automatically — no further changes required.

---

## Performance Benchmarks (Holdout: Oct–Dec 2023)

See `outputs/pipeline_benchmark.csv` and `fact_model_performance` in the DuckDB warehouse for actual measured values after running the pipeline. Summary statistics are written to `outputs/pipeline_run_log.csv` at step 4.

Typical MAPE ranges on this synthetic dataset:
- Class A (high-value, regular demand): 15–25% MAPE
- Class B (moderate volume): 20–35% MAPE
- Class C (low-volume, irregular): 30–60% MAPE

These ranges reflect the inherent difficulty of forecasting each class, not model failure. Class C SKUs have sparse, noisy demand that is structurally harder to predict.

---

## Glossary

| Metric | Definition |
|---|---|
| **MAPE** | Mean Absolute Percentage Error: `mean(|actual-predicted|/actual) × 100`. Scale-invariant. Cannot be computed when actual=0. |
| **RMSE** | Root Mean Square Error: `sqrt(mean((actual-predicted)²))`. Penalises large errors more than MAE. In original units (units of demand). |
| **MAE** | Mean Absolute Error: `mean(|actual-predicted|)`. Robust to outliers. In original units. |
| **Bias** | `mean(predicted-actual)`. Positive = systematic over-forecast. Negative = systematic under-forecast. |
