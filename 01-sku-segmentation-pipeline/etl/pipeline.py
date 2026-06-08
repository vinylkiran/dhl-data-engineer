"""
pipeline.py — Orchestration Script
DHL SKU Segmentation Pipeline — Project 01

Single entry point to run the full ETL pipeline:
  Extract → Transform → Load

Logs start/end time, total duration, and final row counts.
Handles errors gracefully — logs and exits cleanly without partial loads.

Usage:
    python pipeline.py
    python pipeline.py --db-path /custom/path/dhl_warehouse.duckdb
"""

import argparse
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

# Add etl directory to path
sys.path.insert(0, str(Path(__file__).parent))

from extract   import extract_all, DATA_DIR
from transform import transform_all
from load      import load_all, DB_PATH

# ---------------------------------------------------------------------------
# Logging — pipeline gets its own handler that writes to file + console
# ---------------------------------------------------------------------------

def setup_logging(log_dir: Path) -> logging.Logger:
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"pipeline_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.log"

    logger = logging.getLogger("pipeline")
    logger.setLevel(logging.INFO)

    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Console handler
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    # File handler
    fh = logging.FileHandler(log_file)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    # Propagate to child loggers
    for child in ["extract", "transform", "load"]:
        child_logger = logging.getLogger(child)
        child_logger.setLevel(logging.INFO)
        if not child_logger.handlers:
            child_logger.addHandler(ch)
            child_logger.addHandler(fh)

    logger.info(f"Pipeline log: {log_file}")
    return logger


# ---------------------------------------------------------------------------
# Pipeline runner
# ---------------------------------------------------------------------------

def run_pipeline(db_path: Path = DB_PATH, logger: logging.Logger = None) -> dict:
    """
    Run the full ETL pipeline. Returns a summary dict.
    Raises SystemExit(1) on any stage failure.
    """
    if logger is None:
        base = Path(__file__).resolve().parent.parent
        logger = setup_logging(base / "outputs" / "logs")

    pipeline_start = time.time()
    start_ts = datetime.utcnow()

    logger.info("=" * 70)
    logger.info("DHL SKU SEGMENTATION PIPELINE — START")
    logger.info(f"Start time: {start_ts.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    logger.info(f"Database:   {db_path}")
    logger.info("=" * 70)

    # ----------------------------------------------------------------
    # STAGE 1: EXTRACT
    # ----------------------------------------------------------------
    stage_start = time.time()
    logger.info("\n>>> STAGE 1: EXTRACT")
    try:
        extracted = extract_all(logger=logging.getLogger("extract"))
        extract_duration = round(time.time() - stage_start, 2)
        logger.info(f"Extract completed in {extract_duration}s")
    except Exception as e:
        logger.error(f"EXTRACT STAGE FAILED: {e}")
        logger.error("Pipeline aborted — no data loaded.")
        sys.exit(1)

    # ----------------------------------------------------------------
    # STAGE 2: TRANSFORM
    # ----------------------------------------------------------------
    stage_start = time.time()
    logger.info("\n>>> STAGE 2: TRANSFORM")
    try:
        transformed = transform_all(extracted, logger=logging.getLogger("transform"))
        transform_duration = round(time.time() - stage_start, 2)
        logger.info(f"Transform completed in {transform_duration}s")
    except Exception as e:
        logger.error(f"TRANSFORM STAGE FAILED: {e}")
        logger.error("Pipeline aborted — no data loaded.")
        sys.exit(1)

    # ----------------------------------------------------------------
    # STAGE 3: LOAD
    # ----------------------------------------------------------------
    stage_start = time.time()
    logger.info("\n>>> STAGE 3: LOAD")
    try:
        load_stats = load_all(transformed, db_path=db_path, logger=logging.getLogger("load"))
        load_duration = round(time.time() - stage_start, 2)
        logger.info(f"Load completed in {load_duration}s")
    except Exception as e:
        logger.error(f"LOAD STAGE FAILED: {e}")
        logger.error("Pipeline aborted — database may be in inconsistent state. Check logs.")
        sys.exit(1)

    # ----------------------------------------------------------------
    # PIPELINE SUMMARY
    # ----------------------------------------------------------------
    total_duration = round(time.time() - pipeline_start, 2)
    end_ts = datetime.utcnow()

    logger.info("\n" + "=" * 70)
    logger.info("PIPELINE SUMMARY")
    logger.info("=" * 70)
    logger.info(f"Start:    {start_ts.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    logger.info(f"End:      {end_ts.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    logger.info(f"Duration: {total_duration}s  "
                f"(extract={extract_duration}s | "
                f"transform={transform_duration}s | "
                f"load={load_duration}s)")
    logger.info("")
    logger.info("Final row counts:")
    total_rows = 0
    for s in load_stats:
        status_icon = "✓" if s["status"] == "OK" else "✗"
        logger.info(f"  {status_icon} {s['table']:<35} {s['loaded_rows']:>10,} rows")
        total_rows += s["loaded_rows"]
    logger.info(f"  {'TOTAL':<35} {total_rows:>10,} rows")
    logger.info("=" * 70)
    logger.info("DHL SKU SEGMENTATION PIPELINE — COMPLETE")

    return {
        "status":           "SUCCESS",
        "start_ts":         start_ts.isoformat(),
        "end_ts":           end_ts.isoformat(),
        "total_duration_s": total_duration,
        "extract_duration_s":   extract_duration,
        "transform_duration_s": transform_duration,
        "load_duration_s":      load_duration,
        "load_stats":       load_stats,
        "total_rows":       total_rows,
    }


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DHL SKU Segmentation ETL Pipeline")
    parser.add_argument(
        "--db-path",
        type=Path,
        default=DB_PATH,
        help=f"Path to DuckDB database file (default: {DB_PATH})"
    )
    args = parser.parse_args()

    base_dir = Path(__file__).resolve().parent.parent
    logger = setup_logging(base_dir / "outputs" / "logs")

    result = run_pipeline(db_path=args.db_path, logger=logger)
    sys.exit(0)
