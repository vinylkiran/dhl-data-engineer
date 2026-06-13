"""
anomaly_detection.py — Statistical Anomaly Detection on WMS Operations
DHL Data Engineer Portfolio — Project 05

Uses 90-day rolling statistics to flag:
  1. Daily pick accuracy drops > 2 std devs below warehouse mean (sudden accuracy drop)
  2. Daily task volume drops > 3 std devs below mean (system outage / missing data)
  3. Operator accuracy drops > 5 percentage points vs their own 30-day rolling average
  4. Error rate spikes: any error code exceeding 2x its 30-day average frequency

All flags exported to outputs/anomaly_flags.csv
"""

import logging
import sys
from datetime import datetime
from pathlib import Path
import duckdb
import pandas as pd
import numpy as np

BASE_DIR   = Path(__file__).resolve().parent.parent
DB_PATH    = Path("/tmp/dhl_p5.duckdb")
OUTPUT_DIR = BASE_DIR / "outputs"

ACCURACY_DROP_STD    = 2.0    # standard deviations for accuracy drop flag
VOLUME_DROP_STD      = 3.0    # standard deviations for volume drop flag
OPERATOR_DROP_PCT    = 5.0    # percentage point drop vs 30-day operator average
ERROR_SPIKE_MULTIPLE = 2.0    # error code exceeds X times its 30-day average
ROLLING_DAYS         = 90
OPERATOR_ROLLING     = 30


def get_logger(name="anomaly_detection"):
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


def _flag(anomaly_type, warehouse_id, detection_date, observed_value,
          baseline_value, deviation, severity, description):
    return {
        "anomaly_type":    anomaly_type,
        "warehouse_id":    warehouse_id,
        "detection_date":  str(detection_date),
        "observed_value":  round(float(observed_value), 4) if observed_value is not None else None,
        "baseline_value":  round(float(baseline_value), 4) if baseline_value is not None else None,
        "deviation":       round(float(deviation), 4) if deviation is not None else None,
        "severity":        severity,
        "description":     description,
        "flagged_at":      datetime.utcnow().isoformat(),
    }


# ---------------------------------------------------------------------------
# Anomaly 1: Daily pick accuracy drop
# ---------------------------------------------------------------------------

def detect_accuracy_drops(conn, logger) -> list:
    """Flag dates where pick accuracy falls > 2 std devs below the 90-day warehouse mean."""
    logger.info("  Detecting accuracy drops...")

    df = conn.execute("""
        SELECT kpi_date, warehouse_id,
               SUM(total_picks * COALESCE(pick_accuracy_pct, 0) / 100.0)
                   / NULLIF(SUM(total_picks), 0) * 100  AS daily_pick_accuracy,
               SUM(total_picks) AS total_picks
        FROM fact_wms_daily_kpis
        WHERE total_picks > 0
        GROUP BY kpi_date, warehouse_id
        ORDER BY warehouse_id, kpi_date
    """).df()

    if len(df) == 0:
        logger.info("    No daily KPI data — skipping")
        return []

    df["kpi_date"] = pd.to_datetime(df["kpi_date"])
    flags = []

    for wh_id, wh_df in df.groupby("warehouse_id"):
        wh_df = wh_df.sort_values("kpi_date").reset_index(drop=True)

        # Rolling 90-day mean and std for each date
        wh_df["rolling_mean"] = wh_df["daily_pick_accuracy"].rolling(
            window=ROLLING_DAYS, min_periods=7
        ).mean()
        wh_df["rolling_std"] = wh_df["daily_pick_accuracy"].rolling(
            window=ROLLING_DAYS, min_periods=7
        ).std()

        # Flag dates where accuracy < mean - N*std
        wh_df["lower_bound"] = wh_df["rolling_mean"] - ACCURACY_DROP_STD * wh_df["rolling_std"]
        anomalies = wh_df[
            wh_df["daily_pick_accuracy"] < wh_df["lower_bound"]
        ]

        for _, row in anomalies.iterrows():
            if pd.isna(row["rolling_mean"]):
                continue
            deviation = (row["rolling_mean"] - row["daily_pick_accuracy"]) / max(row["rolling_std"], 0.001)
            severity = "CRITICAL" if deviation > 3 * ACCURACY_DROP_STD else "HIGH"
            flags.append(_flag(
                anomaly_type="accuracy_drop",
                warehouse_id=wh_id,
                detection_date=row["kpi_date"].date(),
                observed_value=row["daily_pick_accuracy"],
                baseline_value=row["rolling_mean"],
                deviation=deviation,
                severity=severity,
                description=(
                    f"Pick accuracy {row['daily_pick_accuracy']:.2f}% is "
                    f"{deviation:.1f}σ below 90-day mean "
                    f"({row['rolling_mean']:.2f}%)"
                )
            ))

    logger.info(f"    Accuracy drops flagged: {len(flags)}")
    return flags


# ---------------------------------------------------------------------------
# Anomaly 2: Daily task volume drops
# ---------------------------------------------------------------------------

def detect_volume_drops(conn, logger) -> list:
    """Flag dates where daily task volume drops > 3 std devs below 90-day mean."""
    logger.info("  Detecting volume drops (potential outages)...")

    df = conn.execute("""
        SELECT kpi_date, warehouse_id, SUM(total_tasks) AS daily_tasks
        FROM fact_wms_daily_kpis
        GROUP BY kpi_date, warehouse_id
        ORDER BY warehouse_id, kpi_date
    """).df()

    if len(df) == 0:
        return []

    df["kpi_date"] = pd.to_datetime(df["kpi_date"])
    flags = []

    for wh_id, wh_df in df.groupby("warehouse_id"):
        wh_df = wh_df.sort_values("kpi_date").reset_index(drop=True)

        wh_df["rolling_mean"] = wh_df["daily_tasks"].rolling(
            window=ROLLING_DAYS, min_periods=7
        ).mean()
        wh_df["rolling_std"] = wh_df["daily_tasks"].rolling(
            window=ROLLING_DAYS, min_periods=7
        ).std()

        wh_df["lower_bound"] = wh_df["rolling_mean"] - VOLUME_DROP_STD * wh_df["rolling_std"]
        anomalies = wh_df[
            (wh_df["daily_tasks"] < wh_df["lower_bound"]) &
            wh_df["lower_bound"].notna()
        ]

        for _, row in anomalies.iterrows():
            if pd.isna(row["rolling_mean"]):
                continue
            deviation = (row["rolling_mean"] - row["daily_tasks"]) / max(row["rolling_std"], 1)
            flags.append(_flag(
                anomaly_type="volume_drop",
                warehouse_id=wh_id,
                detection_date=row["kpi_date"].date(),
                observed_value=row["daily_tasks"],
                baseline_value=row["rolling_mean"],
                deviation=deviation,
                severity="HIGH",
                description=(
                    f"Task volume {int(row['daily_tasks'])} is {deviation:.1f}σ below "
                    f"90-day mean ({row['rolling_mean']:.0f}). "
                    f"Possible data gap or outage."
                )
            ))

    logger.info(f"    Volume drops flagged: {len(flags)}")
    return flags


# ---------------------------------------------------------------------------
# Anomaly 3: Operator accuracy drops
# ---------------------------------------------------------------------------

def detect_operator_accuracy_drops(conn, logger) -> list:
    """Flag operators whose accuracy drops > 5pp vs their 30-day rolling average."""
    logger.info("  Detecting operator accuracy drops...")

    df = conn.execute("""
        SELECT operator_id, warehouse_id, task_date, pick_accuracy_pct
        FROM fact_operator_daily
        WHERE pick_accuracy_pct IS NOT NULL AND picks_completed > 0
        ORDER BY operator_id, warehouse_id, task_date
    """).df()

    if len(df) == 0:
        return []

    df["task_date"] = pd.to_datetime(df["task_date"])
    flags = []

    for (op_id, wh_id), grp in df.groupby(["operator_id", "warehouse_id"]):
        grp = grp.sort_values("task_date").reset_index(drop=True)

        grp["rolling_avg"] = grp["pick_accuracy_pct"].rolling(
            window=OPERATOR_ROLLING, min_periods=5
        ).mean().shift(1)   # shift so we compare today vs past N days

        # Flag rows where today's accuracy is more than OPERATOR_DROP_PCT below rolling avg
        anomalies = grp[
            (grp["pick_accuracy_pct"] < grp["rolling_avg"] - OPERATOR_DROP_PCT) &
            grp["rolling_avg"].notna()
        ]

        for _, row in anomalies.iterrows():
            drop = row["rolling_avg"] - row["pick_accuracy_pct"]
            flags.append(_flag(
                anomaly_type="operator_accuracy_drop",
                warehouse_id=wh_id,
                detection_date=row["task_date"].date(),
                observed_value=row["pick_accuracy_pct"],
                baseline_value=row["rolling_avg"],
                deviation=drop,
                severity="HIGH" if drop > 10 else "MEDIUM",
                description=(
                    f"Operator {op_id} accuracy {row['pick_accuracy_pct']:.2f}% is "
                    f"{drop:.1f}pp below their {OPERATOR_ROLLING}-day average "
                    f"({row['rolling_avg']:.2f}%)"
                )
            ))

    logger.info(f"    Operator accuracy drops flagged: {len(flags)}")
    return flags


# ---------------------------------------------------------------------------
# Anomaly 4: Error rate spikes
# ---------------------------------------------------------------------------

def detect_error_rate_spikes(conn, logger) -> list:
    """Flag error codes whose frequency exceeds 2x their 30-day average."""
    logger.info("  Detecting error rate spikes...")

    df = conn.execute("""
        SELECT warehouse_id, task_date, error_code, COUNT(*) AS error_count
        FROM fact_error_log
        WHERE error_code IS NOT NULL AND error_code != ''
        GROUP BY warehouse_id, task_date, error_code
        ORDER BY warehouse_id, error_code, task_date
    """).df()

    if len(df) == 0:
        logger.info("    No error log data — skipping")
        return []

    df["task_date"] = pd.to_datetime(df["task_date"])
    flags = []

    for (wh_id, err_code), grp in df.groupby(["warehouse_id", "error_code"]):
        grp = grp.sort_values("task_date").reset_index(drop=True)

        grp["rolling_avg"] = grp["error_count"].rolling(
            window=30, min_periods=5
        ).mean().shift(1)

        anomalies = grp[
            (grp["error_count"] > grp["rolling_avg"] * ERROR_SPIKE_MULTIPLE) &
            grp["rolling_avg"].notna() &
            (grp["rolling_avg"] > 0)
        ]

        for _, row in anomalies.iterrows():
            multiple = row["error_count"] / row["rolling_avg"]
            flags.append(_flag(
                anomaly_type="error_rate_spike",
                warehouse_id=wh_id,
                detection_date=row["task_date"].date(),
                observed_value=row["error_count"],
                baseline_value=row["rolling_avg"],
                deviation=multiple,
                severity="HIGH" if multiple > 5 else "MEDIUM",
                description=(
                    f"Error code '{err_code}' occurred {int(row['error_count'])} times "
                    f"({multiple:.1f}x the 30-day average of {row['rolling_avg']:.1f})"
                )
            ))

    logger.info(f"    Error rate spikes flagged: {len(flags)}")
    return flags


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run_anomaly_detection(db_path: Path = DB_PATH, output_dir: Path = OUTPUT_DIR,
                           logger: logging.Logger = None) -> pd.DataFrame:
    if logger is None:
        logger = get_logger()

    logger.info("=" * 60)
    logger.info("ANOMALY DETECTION — START")
    logger.info("=" * 60)

    conn = duckdb.connect(str(db_path), read_only=True)

    all_flags = []
    all_flags.extend(detect_accuracy_drops(conn, logger))
    all_flags.extend(detect_volume_drops(conn, logger))
    all_flags.extend(detect_operator_accuracy_drops(conn, logger))
    all_flags.extend(detect_error_rate_spikes(conn, logger))

    conn.close()

    if all_flags:
        report = pd.DataFrame(all_flags)
    else:
        report = pd.DataFrame(columns=[
            "anomaly_type", "warehouse_id", "detection_date", "observed_value",
            "baseline_value", "deviation", "severity", "description", "flagged_at"
        ])

    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / "anomaly_flags.csv"
    report.to_csv(out_path, index=False)

    severity_counts = report["severity"].value_counts().to_dict() if len(report) > 0 else {}
    type_counts     = report["anomaly_type"].value_counts().to_dict() if len(report) > 0 else {}

    logger.info(f"\nTotal anomaly flags: {len(report)}")
    logger.info(f"  By severity: {severity_counts}")
    logger.info(f"  By type:     {type_counts}")
    logger.info(f"Anomaly report saved: {out_path}")
    logger.info("ANOMALY DETECTION — COMPLETE")
    return report


if __name__ == "__main__":
    db = Path(sys.argv[1]) if len(sys.argv) > 1 else DB_PATH
    report = run_anomaly_detection(db_path=db)
    print(f"\nTotal anomaly flags: {len(report)}")
    if len(report) > 0:
        print(report[["anomaly_type", "warehouse_id", "detection_date", "severity",
                       "description"]].to_string(index=False))
