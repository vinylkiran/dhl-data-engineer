"""
rfm_scoring_pipeline.py — Automated RFM Scoring Pipeline
DHL Data Engineer Portfolio — Project 03

Reference date: MAX(order_date) in fact_orders.
Per customer:
  - Recency:   days since last order
  - Frequency: total order count
  - Monetary:  total revenue
Scores each dimension 1-5 using quintile boundaries on current data.
Assigns segment labels using BA/DA project logic.
SCD Type 2: sets is_current_flag=False on prior scores, inserts new row.
Updates dim_customer.current_rfm_segment.
"""

import logging
import time
from datetime import datetime
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

# RFM segment labels (matches BA/DA project)
def assign_segment(row: pd.Series) -> str:
    r, f, m = row["recency_score"], row["frequency_score"], row["monetary_score"]
    rfm_score = r * 100 + f * 10 + m
    if r >= 4 and f >= 4 and m >= 4:
        return "Champions"
    if r >= 3 and f >= 3 and m >= 3:
        return "Loyal Customers"
    if r >= 4 and f <= 2:
        return "New Customers"
    if r >= 3 and f >= 1 and m >= 2:
        return "Potential Loyalists"
    if r <= 2 and f >= 3 and m >= 3:
        return "At Risk"
    if r <= 2 and f >= 4 and m >= 4:
        return "Can't Lose Them"
    if r == 1 and f == 1:
        return "Lost"
    if r <= 2 and f <= 2 and m <= 2:
        return "Hibernating"
    if r >= 2 and m >= 3:
        return "Promising"
    return "Need Attention"


def score_quintile(series: pd.Series, reverse: bool = False) -> pd.Series:
    """
    Assign 1-5 quintile score. Higher is better (5 = top quintile).
    For Recency, lower days = better, so reverse=True.
    """
    # Use pd.qcut with duplicates='drop', fall back to pd.cut if insufficient distinct values
    try:
        labels = [5, 4, 3, 2, 1] if reverse else [1, 2, 3, 4, 5]
        scores = pd.qcut(series, q=5, labels=labels, duplicates="drop")
        # If qcut produced fewer than 5 bins, fill NaN with median score
        if scores.isna().any():
            median_score = 3
            scores = scores.fillna(median_score)
        return scores.astype(int)
    except Exception:
        # Fallback: percentile-based manual binning
        pcts = series.rank(pct=True)
        if reverse:
            pcts = 1 - pcts
        return pd.cut(pcts, bins=[-0.001, 0.2, 0.4, 0.6, 0.8, 1.0],
                      labels=[1, 2, 3, 4, 5]).astype(int)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def get_logger(name: str = "rfm_scoring") -> logging.Logger:
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
# Main scoring function
# ---------------------------------------------------------------------------

def run_rfm_scoring(db_path: Path = DB_PATH, output_dir: Path = OUTPUT_DIR,
                    logger: logging.Logger = None) -> pd.DataFrame:
    if logger is None:
        logger = get_logger()

    t_start = time.time()
    logger.info("=" * 60)
    logger.info("RFM SCORING PIPELINE — START")
    logger.info(f"DB: {db_path}")
    logger.info("=" * 60)

    conn = duckdb.connect(str(db_path))

    # Reference date = last order date in fact_orders
    ref_date = conn.execute("SELECT MAX(order_date) FROM fact_orders").fetchone()[0]
    if ref_date is None:
        logger.error("  fact_orders is empty — run customer ETL first")
        conn.close()
        return pd.DataFrame()

    scoring_date = ref_date
    logger.info(f"  Reference date: {scoring_date}")
    scored_at = datetime.utcnow()

    # Compute RFM raw values per customer
    logger.info("  Computing R, F, M per customer...")
    rfm_df = conn.execute(f"""
        SELECT
            customer_id,
            CAST('{scoring_date}' AS DATE) - MAX(order_date)   AS recency_days,
            COUNT(*)                                            AS frequency_count,
            SUM(revenue)                                        AS monetary_value
        FROM fact_orders
        GROUP BY customer_id
        HAVING frequency_count > 0
    """).df()

    logger.info(f"  Customers with orders: {len(rfm_df):,}")

    # Score each dimension
    rfm_df["recency_score"]   = score_quintile(rfm_df["recency_days"],    reverse=True)
    rfm_df["frequency_score"] = score_quintile(rfm_df["frequency_count"], reverse=False)
    rfm_df["monetary_score"]  = score_quintile(rfm_df["monetary_value"],  reverse=False)

    # Assign segments
    rfm_df["rfm_segment"] = rfm_df.apply(assign_segment, axis=1)

    # Assign score_ids
    max_score_id = conn.execute(
        "SELECT COALESCE(MAX(score_id), 0) FROM fact_rfm_scores"
    ).fetchone()[0]

    rfm_df = rfm_df.reset_index(drop=True)
    rfm_df["score_id"]       = range(int(max_score_id) + 1,
                                      int(max_score_id) + len(rfm_df) + 1)
    rfm_df["scoring_date"]   = scoring_date
    rfm_df["is_current_flag"]= True
    rfm_df["valid_from"]     = scored_at
    rfm_df["valid_to"]       = None
    rfm_df["etl_loaded_at"]  = scored_at

    # SCD Type 2: expire prior scores
    logger.info("  Expiring prior scores (SCD Type 2)...")
    conn.execute("""
        UPDATE fact_rfm_scores
        SET is_current_flag = FALSE,
            valid_to        = CURRENT_TIMESTAMP
        WHERE is_current_flag = TRUE
    """)

    # Insert new scores
    logger.info(f"  Inserting {len(rfm_df):,} new score records...")
    cols = ["score_id", "customer_id", "scoring_date", "recency_days",
            "frequency_count", "monetary_value", "recency_score",
            "frequency_score", "monetary_score", "rfm_segment",
            "is_current_flag", "valid_from", "valid_to", "etl_loaded_at"]
    rfm_load = rfm_df[[c for c in cols if c in rfm_df.columns]]
    col_list = ", ".join(f'"{c}"' for c in rfm_load.columns)
    conn.register("_rfm_staging", rfm_load)
    conn.execute(f"INSERT INTO fact_rfm_scores ({col_list}) SELECT {col_list} FROM _rfm_staging")
    conn.unregister("_rfm_staging")

    # Update dim_customer.current_rfm_segment
    logger.info("  Updating dim_customer.current_rfm_segment...")
    conn.execute("""
        UPDATE dim_customer dc
        SET current_rfm_segment = rfm.rfm_segment
        FROM (
            SELECT customer_id, rfm_segment
            FROM fact_rfm_scores
            WHERE is_current_flag = TRUE
        ) AS rfm
        WHERE dc.customer_id = rfm.customer_id
    """)

    # Segment distribution
    seg_dist = rfm_df.groupby("rfm_segment")["customer_id"].count().sort_values(ascending=False)
    total_customers = len(rfm_df)

    duration = round(time.time() - t_start, 2)

    logger.info(f"\n  Scoring complete: {total_customers:,} customers scored in {duration}s")
    logger.info("  Segment distribution:")
    for seg, count in seg_dist.items():
        pct = count / total_customers * 100
        logger.info(f"    {seg:<22} {count:>5,}  ({pct:.1f}%)")

    # Export scoring run summary to output
    output_dir.mkdir(parents=True, exist_ok=True)
    summary_path = output_dir / "rfm_scoring_summary.csv"
    seg_df = seg_dist.reset_index()
    seg_df.columns = ["segment", "customer_count"]
    seg_df["pct_of_total"] = (seg_df["customer_count"] / total_customers * 100).round(2)
    seg_df["scoring_date"] = scoring_date
    seg_df.to_csv(summary_path, index=False)
    logger.info(f"  Segment summary saved: {summary_path.name}")

    conn.close()
    logger.info("RFM SCORING PIPELINE — COMPLETE")

    return rfm_df


if __name__ == "__main__":
    import sys
    db = Path(sys.argv[1]) if len(sys.argv) > 1 else DB_PATH
    logger = get_logger()
    df = run_rfm_scoring(db_path=db, logger=logger)
    if len(df) > 0:
        print(f"\n{len(df):,} customers scored")
        print(df["rfm_segment"].value_counts().to_string())
