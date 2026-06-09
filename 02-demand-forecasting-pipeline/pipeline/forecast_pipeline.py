"""
forecast_pipeline.py — Automated Demand Forecasting Pipeline
DHL Demand Forecasting Pipeline — Project 02

Six-step automated pipeline:
  Step 1 — Ingest:        Incremental load of new demand data
  Step 2 — Features:      Compute features for new dates
  Step 3 — Models:        Run 3 baseline models, generate 30-day forecasts
  Step 4 — Evaluation:    Calculate MAPE/RMSE/MAE/bias on Oct-Dec 2023 holdout
  Step 5 — Export:        Write planner-ready forecast CSV
  Step 6 — Log:           Write pipeline run log

Usage:
    python pipeline/forecast_pipeline.py
    python pipeline/forecast_pipeline.py --db-path /path/to/dhl_warehouse.duckdb
"""

import sys
import argparse
import logging
import time
import csv
from datetime import datetime, date
from pathlib import Path
import duckdb
import pandas as pd
import numpy as np

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BASE_DIR   = Path(__file__).resolve().parent.parent
DB_PATH    = BASE_DIR.parent / "01-sku-segmentation-pipeline" / "outputs" / "dhl_warehouse.duckdb"
OUTPUT_DIR = BASE_DIR / "outputs"

HOLDOUT_START = date(2023, 10, 1)
HOLDOUT_END   = date(2023, 12, 31)
TRAIN_START   = date(2022, 1, 1)
TRAIN_END     = date(2023, 9, 30)
FORECAST_HORIZON = 30
CONFIDENCE_LEVEL = 0.80

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def get_logger(name: str = "forecast_pipeline") -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        h = logging.StreamHandler()
        h.setFormatter(logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        ))
        logger.addHandler(h)
        logger.setLevel(logging.INFO)
    return logger

# ---------------------------------------------------------------------------
# Step 1: Ingest
# ---------------------------------------------------------------------------

def step_ingest(conn: duckdb.DuckDBPyConnection, logger: logging.Logger) -> dict:
    """Check watermark and log current state — data already loaded by Project 1."""
    total = conn.execute("SELECT COUNT(*) FROM fact_daily_demand").fetchone()[0]
    max_date = conn.execute("""
        SELECT MAX(d.full_date) FROM fact_daily_demand f
        JOIN dim_date d ON f.date_key = d.date_key
    """).fetchone()[0]
    logger.info(f"  fact_daily_demand: {total:,} rows, max date: {max_date}")
    return {"rows_in_warehouse": total, "max_date": str(max_date), "new_records": 0}

# ---------------------------------------------------------------------------
# Step 2: Features
# ---------------------------------------------------------------------------

def step_features(conn: duckdb.DuckDBPyConnection, db_path: Path, logger: logging.Logger) -> dict:
    """Run feature engineering for any dates not yet in feature store."""
    sys.path.insert(0, str(BASE_DIR / "features"))
    from feature_engineering import run_feature_engineering
    stats = run_feature_engineering(db_path=db_path, logger=logger)
    return stats

# ---------------------------------------------------------------------------
# Step 3: Models
# ---------------------------------------------------------------------------

def register_models(conn: duckdb.DuckDBPyConnection, loaded_at: datetime,
                    logger: logging.Logger) -> dict:
    """Register the three baseline models in dim_model. Return {name: id}."""
    models = [
        {
            "model_id":       1,
            "model_name":     "moving_average_14d",
            "model_type":     "baseline",
            "description":    "14-day moving average of historical demand",
            "hyperparameters": '{"window": 14}',
            "created_at":     loaded_at,
        },
        {
            "model_id":       2,
            "model_name":     "moving_average_28d",
            "model_type":     "baseline",
            "description":    "28-day moving average of historical demand",
            "hyperparameters": '{"window": 28}',
            "created_at":     loaded_at,
        },
        {
            "model_id":       3,
            "model_name":     "seasonal_naive",
            "model_type":     "baseline",
            "description":    "Seasonal naive: same weekday 4 weeks ago (28-day lag)",
            "hyperparameters": '{"lag_days": 28}',
            "created_at":     loaded_at,
        },
    ]

    # Truncate and reload dim_model
    conn.execute("DELETE FROM dim_model")
    df = pd.DataFrame(models)
    df["etl_loaded_at"]   = loaded_at
    df["etl_source_file"] = "programmatic"
    conn.register("_model_staging", df)
    conn.execute("INSERT INTO dim_model SELECT * FROM _model_staging")
    conn.unregister("_model_staging")
    logger.info(f"  Registered {len(models)} models in dim_model")

    return {m["model_name"]: m["model_id"] for m in models}


def generate_forecasts(conn: duckdb.DuckDBPyConnection, model_ids: dict,
                        loaded_at: datetime, logger: logging.Logger) -> int:
    """
    Generate 30-day forward forecasts for all active SKUs using feature store.
    Returns total forecast rows generated.
    """
    logger.info("  Generating forecasts from feature store...")

    # Get the last 28 days of demand for each active SKU-warehouse combo
    # Use this as the basis for rolling average models
    logger.info("  Loading recent demand for forecast generation...")
    recent = conn.execute("""
        SELECT
            s.sku_id, s.sku_key, w.warehouse_id, w.warehouse_key,
            s.abc_class, s.xyz_class,
            d.full_date,
            f.quantity_demanded
        FROM fact_daily_demand f
        JOIN dim_sku       s ON f.sku_key       = s.sku_key
        JOIN dim_warehouse w ON f.warehouse_key = w.warehouse_key
        JOIN dim_date      d ON f.date_key      = d.date_key
        WHERE s.active_flag = TRUE
          AND d.full_date >= DATE '2023-11-01'
          AND d.full_date <= DATE '2023-12-31'
        ORDER BY s.sku_id, w.warehouse_id, d.full_date
    """).df()

    recent["full_date"] = pd.to_datetime(recent["full_date"]).dt.date

    # Get all distinct active SKU-warehouse combos
    combos = recent[["sku_id", "sku_key", "warehouse_id", "warehouse_key",
                      "abc_class", "xyz_class"]].drop_duplicates()

    # Last known date = 2023-12-31; forecast starts 2024-01-01
    base_date = date(2023, 12, 31)
    forecast_start = date(2024, 1, 1)

    all_forecasts = []
    forecast_id = 1

    for _, row in combos.iterrows():
        sku_id       = row["sku_id"]
        sku_key      = row["sku_key"]
        wh_id        = row["warehouse_id"]
        wh_key       = row["warehouse_key"]

        hist = recent[
            (recent["sku_id"] == sku_id) & (recent["warehouse_id"] == wh_id)
        ].sort_values("full_date")

        qty = hist["quantity_demanded"].values.astype(float)
        if len(qty) == 0:
            qty = np.array([0.0])

        # Model 1: 14-day MA
        ma14 = float(np.mean(qty[-14:])) if len(qty) >= 1 else 0.0
        # Model 2: 28-day MA
        ma28 = float(np.mean(qty[-28:])) if len(qty) >= 1 else 0.0
        # Model 3: Seasonal naive — value 28 days ago
        sn   = float(qty[-28]) if len(qty) >= 28 else ma14

        # Compute std for confidence intervals
        std_val = float(np.std(qty[-14:])) if len(qty) >= 2 else max(ma14 * 0.2, 1.0)

        forecasts_for_combo = [
            (1, "moving_average_14d",  ma14),
            (2, "moving_average_28d",  ma28),
            (3, "seasonal_naive",      sn),
        ]

        for h in range(1, FORECAST_HORIZON + 1):
            fcast_date = pd.Timestamp(forecast_start) + pd.Timedelta(days=h - 1)
            for model_id, _, fcasted_qty in forecasts_for_combo:
                z = 1.282  # 80% CI
                lower = max(0.0, round(fcasted_qty - z * std_val, 4))
                upper = round(fcasted_qty + z * std_val, 4)

                all_forecasts.append({
                    "forecast_id":           forecast_id,
                    "sku_key":               int(sku_key),
                    "warehouse_key":         int(wh_key),
                    "model_id":              model_id,
                    "sku_id":                sku_id,
                    "warehouse_id":          wh_id,
                    "forecast_date":         fcast_date.date(),
                    "forecast_horizon_days": h,
                    "forecasted_qty":        round(fcasted_qty, 4),
                    "lower_bound":           lower,
                    "upper_bound":           upper,
                    "confidence_level":      CONFIDENCE_LEVEL,
                    "generated_at":          loaded_at,
                    "etl_loaded_at":         loaded_at,
                    "etl_source_file":       "forecast_pipeline.py",
                })
                forecast_id += 1

    if not all_forecasts:
        logger.warning("  No forecasts generated")
        return 0

    # Load into fact_forecast
    conn.execute("DELETE FROM fact_forecast")
    fcast_df = pd.DataFrame(all_forecasts)
    chunk_size = 50_000
    col_list = ", ".join(f'"{c}"' for c in fcast_df.columns)
    for i in range(0, len(fcast_df), chunk_size):
        chunk = fcast_df.iloc[i:i + chunk_size]
        conn.register("_fcast_staging", chunk)
        conn.execute(f"INSERT INTO fact_forecast ({col_list}) SELECT {col_list} FROM _fcast_staging")
        conn.unregister("_fcast_staging")

    logger.info(f"  Generated {len(fcast_df):,} forecast rows for {len(combos):,} SKU-warehouse combos")
    return len(fcast_df)

# ---------------------------------------------------------------------------
# Step 4: Evaluation
# ---------------------------------------------------------------------------

def step_evaluation(conn: duckdb.DuckDBPyConnection, loaded_at: datetime,
                     logger: logging.Logger) -> pd.DataFrame:
    """
    Evaluate all three models on the Oct-Dec 2023 holdout.
    Returns performance DataFrame.
    """
    logger.info("  Loading holdout demand (Oct-Dec 2023)...")

    # Get actual demand in holdout period
    actual = conn.execute(f"""
        SELECT
            s.sku_id, w.warehouse_id, s.abc_class,
            d.full_date AS demand_date,
            f.quantity_demanded AS actual_qty
        FROM fact_daily_demand f
        JOIN dim_sku       s ON f.sku_key       = s.sku_key
        JOIN dim_warehouse w ON f.warehouse_key = w.warehouse_key
        JOIN dim_date      d ON f.date_key      = d.date_key
        WHERE s.active_flag = TRUE
          AND d.full_date >= '{HOLDOUT_START}'
          AND d.full_date <= '{HOLDOUT_END}'
    """).df()
    actual["demand_date"] = pd.to_datetime(actual["demand_date"]).dt.date

    # Get feature store predictions for holdout period
    # For evaluation, use rolling_avg_14 (model 1), rolling_avg_28 (model 2), lag_28 (model 3)
    holdout_features = conn.execute(f"""
        SELECT
            sku_id, warehouse_id, feature_date,
            rolling_avg_14,
            rolling_avg_28,
            lag_28
        FROM fact_feature_store
        WHERE feature_date >= '{HOLDOUT_START}'
          AND feature_date <= '{HOLDOUT_END}'
    """).df()
    holdout_features["feature_date"] = pd.to_datetime(holdout_features["feature_date"]).dt.date

    if len(actual) == 0 or len(holdout_features) == 0:
        logger.warning("  No holdout data found for evaluation")
        return pd.DataFrame()

    # Merge actual with predictions
    merged = actual.merge(
        holdout_features,
        left_on=["sku_id", "warehouse_id", "demand_date"],
        right_on=["sku_id", "warehouse_id", "feature_date"],
        how="inner"
    )

    model_cols = {
        1: "rolling_avg_14",
        2: "rolling_avg_28",
        3: "lag_28",
    }

    perf_records = []
    perf_id = 1

    for model_id, pred_col in model_cols.items():
        eval_df = merged[["sku_id", "abc_class", "actual_qty", pred_col]].copy()
        eval_df = eval_df.dropna(subset=[pred_col])
        eval_df["actual"] = eval_df["actual_qty"].astype(float)
        eval_df["pred"]   = eval_df[pred_col].astype(float)
        eval_df["error"]  = eval_df["pred"] - eval_df["actual"]
        eval_df["abs_err"]   = eval_df["error"].abs()
        eval_df["pct_err"]   = np.where(eval_df["actual"] > 0,
                                         eval_df["abs_err"] / eval_df["actual"], np.nan)

        # Per SKU
        for sku_id, sku_df in eval_df.groupby("sku_id"):
            abc = sku_df["abc_class"].iloc[0]
            mape = float(sku_df["pct_err"].mean() * 100) if sku_df["pct_err"].notna().any() else None
            rmse = float(np.sqrt((sku_df["error"] ** 2).mean()))
            mae  = float(sku_df["abs_err"].mean())
            bias = float(sku_df["error"].mean())

            perf_records.append({
                "performance_id":   perf_id,
                "model_id":         model_id,
                "sku_id":           sku_id,
                "abc_class":        abc,
                "evaluation_date":  loaded_at.date(),
                "train_start":      TRAIN_START,
                "train_end":        TRAIN_END,
                "test_start":       HOLDOUT_START,
                "test_end":         HOLDOUT_END,
                "mape":             mape,
                "rmse":             rmse,
                "mae":              mae,
                "bias":             bias,
                "record_created_at": loaded_at,
                "etl_loaded_at":    loaded_at,
                "etl_source_file":  "forecast_pipeline.py",
            })
            perf_id += 1

    perf_df = pd.DataFrame(perf_records)
    if len(perf_df) == 0:
        return perf_df

    # Load into fact_model_performance
    conn.execute("DELETE FROM fact_model_performance")
    col_list = ", ".join(f'"{c}"' for c in perf_df.columns)
    chunk_size = 50_000
    for i in range(0, len(perf_df), chunk_size):
        chunk = perf_df.iloc[i:i + chunk_size]
        conn.register("_perf_staging", chunk)
        conn.execute(f"INSERT INTO fact_model_performance ({col_list}) SELECT {col_list} FROM _perf_staging")
        conn.unregister("_perf_staging")

    # Summary by model and ABC class
    summary = perf_df.groupby(["model_id", "abc_class"])["mape"].mean().round(2)
    logger.info(f"  Stored {len(perf_df):,} performance records")
    logger.info("  Mean MAPE by model and ABC class:")
    for (mid, abc), mape in summary.items():
        logger.info(f"    model_id={mid}, ABC={abc}: MAPE={mape:.1f}%")

    return perf_df

# ---------------------------------------------------------------------------
# Step 5: Export planner output
# ---------------------------------------------------------------------------

def step_export(conn: duckdb.DuckDBPyConnection, output_dir: Path,
                logger: logging.Logger) -> str:
    """Export best-performing model forecasts to planner CSV."""
    logger.info("  Determining best model per ABC class...")

    # Best model per ABC class by mean MAPE
    best = conn.execute("""
        SELECT abc_class, model_id, AVG(mape) AS avg_mape
        FROM fact_model_performance
        WHERE mape IS NOT NULL
        GROUP BY abc_class, model_id
        QUALIFY ROW_NUMBER() OVER (PARTITION BY abc_class ORDER BY AVG(mape)) = 1
        ORDER BY abc_class
    """).df()

    if len(best) == 0:
        logger.warning("  No performance data — exporting all model 1 forecasts")
        best = pd.DataFrame({"abc_class": ["A","B","C"], "model_id": [1,1,1]})

    # Build model_id → abc_class mapping
    abc_model_map = dict(zip(best["abc_class"], best["model_id"]))

    # Export forecast for best model per ABC class
    forecast_rows = []
    for abc, model_id in abc_model_map.items():
        rows = conn.execute(f"""
            SELECT
                f.sku_id                AS SKU_ID,
                s.category              AS Category,
                s.abc_class             AS ABC_Class,
                f.warehouse_id          AS Warehouse_ID,
                s.primary_warehouse     AS Primary_Warehouse,
                f.forecast_date         AS Forecast_Date,
                f.forecasted_qty        AS Forecasted_Qty,
                f.lower_bound           AS Lower_Bound,
                f.upper_bound           AS Upper_Bound,
                m.model_name            AS Model_Used,
                f.generated_at          AS Generated_At
            FROM fact_forecast f
            JOIN dim_sku   s ON f.sku_key  = s.sku_key
            JOIN dim_model m ON f.model_id = m.model_id
            WHERE s.abc_class = '{abc}'
              AND f.model_id  = {model_id}
            ORDER BY f.sku_id, f.forecast_date
        """).df()
        forecast_rows.append(rows)

    if forecast_rows:
        planner_df = pd.concat(forecast_rows, ignore_index=True)
    else:
        planner_df = pd.DataFrame()

    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / "forecast_output.csv"
    planner_df.to_csv(out_path, index=False)
    logger.info(f"  Exported {len(planner_df):,} forecast rows to {out_path.name}")
    logger.info(f"  Best model per ABC class: {abc_model_map}")
    return str(out_path)

# ---------------------------------------------------------------------------
# Step 6: Pipeline run log
# ---------------------------------------------------------------------------

def step_log(step_stats: dict, output_dir: Path, logger: logging.Logger):
    """Write pipeline run log CSV."""
    output_dir.mkdir(parents=True, exist_ok=True)
    log_path = output_dir / "pipeline_run_log.csv"

    rows = []
    for step_name, stats in step_stats.items():
        rows.append({
            "step":         step_name,
            "status":       stats.get("status", "OK"),
            "rows_processed": stats.get("rows_processed", 0),
            "duration_s":   stats.get("duration_s", 0),
            "notes":        stats.get("notes", ""),
            "run_at":       datetime.utcnow().isoformat(),
        })

    pd.DataFrame(rows).to_csv(log_path, index=False)
    logger.info(f"  Pipeline run log saved: {log_path.name}")

# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def run_pipeline(db_path: Path = DB_PATH, output_dir: Path = OUTPUT_DIR,
                 logger: logging.Logger = None) -> dict:
    if logger is None:
        logger = get_logger()

    t_pipeline = time.time()
    loaded_at  = datetime.utcnow()
    step_stats = {}

    logger.info("=" * 70)
    logger.info("DEMAND FORECASTING PIPELINE — START")
    logger.info(f"Start: {loaded_at.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    logger.info(f"DB:    {db_path}")
    logger.info("=" * 70)

    conn = duckdb.connect(str(db_path))

    # Step 1 — Ingest
    logger.info("\n>>> STEP 1: INGEST")
    t = time.time()
    ingest_stats = step_ingest(conn, logger)
    step_stats["ingest"] = {**ingest_stats, "duration_s": round(time.time()-t,2),
                             "rows_processed": ingest_stats["rows_in_warehouse"], "status": "OK"}
    logger.info(f"  Step 1 complete in {step_stats['ingest']['duration_s']}s")

    # Step 2 — Features
    logger.info("\n>>> STEP 2: FEATURE ENGINEERING")
    t = time.time()
    try:
        feat_stats = step_features(conn, db_path, logger)
        step_stats["features"] = {**feat_stats, "rows_processed": feat_stats.get("rows_added", 0)}
    except Exception as e:
        logger.error(f"  Feature engineering failed: {e}")
        step_stats["features"] = {"status": "ERROR", "notes": str(e), "rows_processed": 0, "duration_s": 0}
    logger.info(f"  Step 2 complete in {step_stats['features'].get('duration_s',0)}s")

    # Step 3 — Models
    logger.info("\n>>> STEP 3: MODEL EXECUTION")
    t = time.time()
    try:
        model_ids = register_models(conn, loaded_at, logger)
        n_forecasts = generate_forecasts(conn, model_ids, loaded_at, logger)
        step_stats["models"] = {"status": "OK", "rows_processed": n_forecasts,
                                 "duration_s": round(time.time()-t,2)}
    except Exception as e:
        logger.error(f"  Model execution failed: {e}")
        step_stats["models"] = {"status": "ERROR", "notes": str(e), "rows_processed": 0,
                                 "duration_s": round(time.time()-t,2)}
    logger.info(f"  Step 3 complete in {step_stats['models']['duration_s']}s")

    # Step 4 — Evaluation
    logger.info("\n>>> STEP 4: PERFORMANCE EVALUATION")
    t = time.time()
    try:
        perf_df = step_evaluation(conn, loaded_at, logger)
        n_perf  = len(perf_df) if perf_df is not None else 0
        step_stats["evaluation"] = {"status": "OK", "rows_processed": n_perf,
                                     "duration_s": round(time.time()-t,2)}
    except Exception as e:
        logger.error(f"  Evaluation failed: {e}")
        step_stats["evaluation"] = {"status": "ERROR", "notes": str(e), "rows_processed": 0,
                                     "duration_s": round(time.time()-t,2)}
    logger.info(f"  Step 4 complete in {step_stats['evaluation']['duration_s']}s")

    # Step 5 — Export
    logger.info("\n>>> STEP 5: EXPORT PLANNER OUTPUT")
    t = time.time()
    try:
        export_path = step_export(conn, output_dir, logger)
        step_stats["export"] = {"status": "OK", "rows_processed": 0,
                                 "notes": export_path, "duration_s": round(time.time()-t,2)}
    except Exception as e:
        logger.error(f"  Export failed: {e}")
        step_stats["export"] = {"status": "ERROR", "notes": str(e), "rows_processed": 0,
                                 "duration_s": round(time.time()-t,2)}
    logger.info(f"  Step 5 complete in {step_stats['export']['duration_s']}s")

    conn.close()

    # Step 6 — Log
    logger.info("\n>>> STEP 6: PIPELINE LOG")
    t = time.time()
    step_log(step_stats, output_dir, logger)
    step_stats["log"] = {"status": "OK", "rows_processed": 0, "duration_s": round(time.time()-t,2)}

    # Summary
    total_dur = round(time.time() - t_pipeline, 2)
    errors = sum(1 for s in step_stats.values() if s.get("status") == "ERROR")

    logger.info("\n" + "=" * 70)
    logger.info("PIPELINE SUMMARY")
    logger.info(f"Total duration: {total_dur}s")
    logger.info(f"Steps completed: {len(step_stats)} | Errors: {errors}")
    for step, s in step_stats.items():
        icon = "✓" if s.get("status") == "OK" else "✗"
        logger.info(f"  {icon} {step:<20} {s.get('rows_processed',0):>10,} rows  {s.get('duration_s',0)}s")
    logger.info("=" * 70)
    logger.info("DEMAND FORECASTING PIPELINE — COMPLETE")

    return {"status": "SUCCESS" if errors == 0 else "PARTIAL",
            "total_duration_s": total_dur, "step_stats": step_stats}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", type=Path, default=DB_PATH)
    args = parser.parse_args()
    logger = get_logger()
    result = run_pipeline(db_path=args.db_path, logger=logger)
    sys.exit(0 if result["status"] == "SUCCESS" else 1)
