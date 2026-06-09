"""
benchmarking.py — Pipeline Performance Benchmarking
DHL Demand Forecasting Pipeline — Project 02

Quantifies the business impact the BA/DA promised:
  - Runs the forecast pipeline 3 times and records runtime
  - Compares automated runtime vs manual process baseline (2-3 days)
  - Calculates time saved, % reduction, annual hours saved (weekly cadence)
  - Exports to outputs/pipeline_benchmark.csv
  - Prints clear summary
"""

import sys
import time
import logging
from datetime import datetime
from pathlib import Path
import pandas as pd

BASE_DIR   = Path(__file__).resolve().parent.parent
DB_PATH    = BASE_DIR.parent / "01-sku-segmentation-pipeline" / "outputs" / "dhl_warehouse.duckdb"
OUTPUT_DIR = BASE_DIR / "outputs"

# Manual process baseline (minutes)
MANUAL_BASELINE_MIN_DAYS = 2   # 2 days minimum
MANUAL_BASELINE_MAX_DAYS = 3   # 3 days maximum
MINUTES_PER_DAY          = 8 * 60  # 8-hour working day = 480 minutes
FORECAST_CADENCE_PER_YEAR = 52  # Weekly forecasting cycles

def get_logger() -> logging.Logger:
    logger = logging.getLogger("benchmarking")
    if not logger.handlers:
        h = logging.StreamHandler()
        h.setFormatter(logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        ))
        logger.addHandler(h)
        logger.setLevel(logging.INFO)
    return logger


def run_pipeline_once(db_path: Path, logger: logging.Logger) -> float:
    """Run the full pipeline once and return duration in seconds."""
    sys.path.insert(0, str(BASE_DIR / "pipeline"))
    from forecast_pipeline import run_pipeline

    # Use a fresh logger to avoid duplicate handlers
    pl_logger = logging.getLogger(f"pipeline_bench_{int(time.time())}")
    pl_logger.setLevel(logging.WARNING)  # Suppress verbose output during benchmarking

    t_start = time.time()
    run_pipeline(db_path=db_path, output_dir=OUTPUT_DIR, logger=pl_logger)
    return round(time.time() - t_start, 3)


def run_benchmarks(db_path: Path = DB_PATH, n_runs: int = 3,
                   logger: logging.Logger = None) -> pd.DataFrame:
    if logger is None:
        logger = get_logger()

    logger.info("=" * 60)
    logger.info("PIPELINE BENCHMARKING — START")
    logger.info(f"Runs: {n_runs} | DB: {db_path}")
    logger.info("=" * 60)

    run_times = []
    for i in range(1, n_runs + 1):
        logger.info(f"  Run {i}/{n_runs}...")
        duration = run_pipeline_once(db_path, logger)
        run_times.append(duration)
        logger.info(f"  Run {i} completed in {duration}s")

    avg_s    = round(sum(run_times) / len(run_times), 3)
    min_s    = min(run_times)
    max_s    = max(run_times)
    avg_min  = round(avg_s / 60, 4)

    # Manual process baseline
    manual_min_min = MANUAL_BASELINE_MIN_DAYS * MINUTES_PER_DAY
    manual_max_min = MANUAL_BASELINE_MAX_DAYS * MINUTES_PER_DAY
    manual_mid_min = (manual_min_min + manual_max_min) / 2

    # Time saved per cycle
    saved_min_per_cycle = round(manual_mid_min - avg_min, 2)
    pct_reduction       = round((1 - avg_min / manual_mid_min) * 100, 2)

    # Annual savings (weekly cadence)
    annual_hours_saved  = round(saved_min_per_cycle * FORECAST_CADENCE_PER_YEAR / 60, 1)
    annual_days_saved   = round(annual_hours_saved / 8, 1)

    # Build results DataFrame
    records = []
    for i, t in enumerate(run_times, 1):
        records.append({
            "run_number":           i,
            "run_timestamp":        datetime.utcnow().isoformat(),
            "runtime_seconds":      t,
            "runtime_minutes":      round(t / 60, 4),
            "manual_baseline_min_minutes":  manual_min_min,
            "manual_baseline_max_minutes":  manual_max_min,
            "manual_baseline_mid_minutes":  manual_mid_min,
            "time_saved_minutes":   round(manual_mid_min - t / 60, 2),
            "pct_time_reduction":   round((1 - (t / 60) / manual_mid_min) * 100, 2),
            "forecast_cadence_per_year": FORECAST_CADENCE_PER_YEAR,
            "annual_hours_saved":   round((manual_mid_min - t / 60) * FORECAST_CADENCE_PER_YEAR / 60, 1),
        })

    # Add summary row
    records.append({
        "run_number":           "SUMMARY",
        "run_timestamp":        datetime.utcnow().isoformat(),
        "runtime_seconds":      avg_s,
        "runtime_minutes":      avg_min,
        "manual_baseline_min_minutes":  manual_min_min,
        "manual_baseline_max_minutes":  manual_max_min,
        "manual_baseline_mid_minutes":  manual_mid_min,
        "time_saved_minutes":   saved_min_per_cycle,
        "pct_time_reduction":   pct_reduction,
        "forecast_cadence_per_year": FORECAST_CADENCE_PER_YEAR,
        "annual_hours_saved":   annual_hours_saved,
    })

    df = pd.DataFrame(records)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "pipeline_benchmark.csv"
    df.to_csv(out_path, index=False)

    # Print summary
    logger.info("\n" + "=" * 60)
    logger.info("BENCHMARK RESULTS")
    logger.info("=" * 60)
    logger.info(f"  Automated pipeline (avg {n_runs} runs): {avg_s}s ({avg_min} min)")
    logger.info(f"  Manual process baseline:               {manual_min_min}–{manual_max_min} min ({MANUAL_BASELINE_MIN_DAYS}–{MANUAL_BASELINE_MAX_DAYS} days)")
    logger.info(f"  Time saved per cycle:                  {saved_min_per_cycle:,.0f} min ({saved_min_per_cycle/60:.1f} hours)")
    logger.info(f"  Cycle time reduction:                  {pct_reduction:.1f}%")
    logger.info(f"  Annual hours saved (weekly cadence):   {annual_hours_saved:,.0f} hours")
    logger.info(f"  Annual working days saved:             {annual_days_saved:,.0f} days")
    logger.info(f"  Benchmark saved to: {out_path.name}")
    logger.info("PIPELINE BENCHMARKING — COMPLETE")

    # Also print to stdout for visibility
    print("\n" + "=" * 60)
    print("BENCHMARK SUMMARY")
    print("=" * 60)
    print(f"  Automated runtime (avg):     {avg_s}s  ({round(avg_s/60,2)} min)")
    print(f"  Run times:                   {run_times}")
    print(f"  Manual baseline:             {manual_min_min}–{manual_max_min} min")
    print(f"  Time saved per cycle:        {saved_min_per_cycle:,.0f} min")
    print(f"  Cycle time reduction:        {pct_reduction:.1f}%")
    print(f"  Annual hours saved:          {annual_hours_saved:,.0f} hours")
    print(f"  Annual days saved:           {annual_days_saved:.0f} working days")
    print("=" * 60)

    return df


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", type=Path, default=DB_PATH)
    parser.add_argument("--runs", type=int, default=3)
    args = parser.parse_args()
    logger = get_logger()
    run_benchmarks(db_path=args.db_path, n_runs=args.runs, logger=logger)
