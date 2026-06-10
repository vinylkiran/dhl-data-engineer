"""
commercial_datamart.py — Commercial Team Data Mart
DHL Data Engineer Portfolio — Project 03

Creates 4 DuckDB views for the commercial team's self-service reporting:
  v_customer_segments   — Current RFM segment for every active customer
  v_at_risk_customers   — At Risk customers: intervention list
  v_champion_customers  — Champions: upsell targeting list
  v_segment_performance — Segment-level KPIs

Exports each view to CSV in outputs/ for Excel/Tableau consumption.
"""

import logging
from datetime import datetime
from pathlib import Path
import duckdb
import pandas as pd

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BASE_DIR   = Path(__file__).resolve().parent.parent
DB_PATH    = BASE_DIR.parent / "01-sku-segmentation-pipeline" / "outputs" / "dhl_warehouse.duckdb"
OUTPUT_DIR = BASE_DIR / "outputs"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def get_logger(name: str = "commercial_datamart") -> logging.Logger:
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
# View definitions
# ---------------------------------------------------------------------------

VIEWS = {
    "v_customer_segments": """
        CREATE OR REPLACE VIEW v_customer_segments AS
        SELECT
            dc.customer_id,
            dc.customer_type,
            dc.region,
            dc.annual_rev_band,
            dc.sla_hours,
            dc.active_flag,
            dc.current_rfm_segment       AS rfm_segment,
            rf.recency_days,
            rf.frequency_count,
            ROUND(rf.monetary_value, 2)  AS monetary_value,
            rf.recency_score,
            rf.frequency_score,
            rf.monetary_score,
            rf.scoring_date,
            dc.last_order_date,
            ROUND(dc.lifetime_revenue, 2) AS lifetime_revenue,
            dc.lifetime_orders
        FROM dim_customer dc
        LEFT JOIN fact_rfm_scores rf
            ON dc.customer_id = rf.customer_id
           AND rf.is_current_flag = TRUE
        WHERE dc.active_flag = TRUE
    """,

    "v_at_risk_customers": """
        CREATE OR REPLACE VIEW v_at_risk_customers AS
        SELECT
            dc.customer_id,
            dc.customer_type,
            dc.region,
            dc.annual_rev_band,
            dc.sla_hours,
            rf.recency_days,
            rf.frequency_count,
            ROUND(rf.monetary_value, 2)   AS monetary_value,
            rf.recency_score,
            rf.frequency_score,
            rf.monetary_score,
            dc.last_order_date,
            ROUND(dc.lifetime_revenue, 2) AS lifetime_revenue,
            dc.lifetime_orders,
            rf.scoring_date,
            'Re-engagement campaign — offer discount or account review' AS recommended_action
        FROM dim_customer dc
        JOIN fact_rfm_scores rf
            ON dc.customer_id = rf.customer_id
           AND rf.is_current_flag = TRUE
        WHERE dc.active_flag = TRUE
          AND dc.current_rfm_segment = 'At Risk'
        ORDER BY rf.monetary_value DESC
    """,

    "v_champion_customers": """
        CREATE OR REPLACE VIEW v_champion_customers AS
        SELECT
            dc.customer_id,
            dc.customer_type,
            dc.region,
            dc.annual_rev_band,
            dc.sla_hours,
            rf.recency_days,
            rf.frequency_count,
            ROUND(rf.monetary_value, 2)   AS monetary_value,
            rf.recency_score,
            rf.frequency_score,
            rf.monetary_score,
            dc.last_order_date,
            ROUND(dc.lifetime_revenue, 2) AS lifetime_revenue,
            dc.lifetime_orders,
            rf.scoring_date,
            'Upsell / loyalty programme — premium tier invitation' AS recommended_action
        FROM dim_customer dc
        JOIN fact_rfm_scores rf
            ON dc.customer_id = rf.customer_id
           AND rf.is_current_flag = TRUE
        WHERE dc.active_flag = TRUE
          AND dc.current_rfm_segment = 'Champions'
        ORDER BY rf.monetary_value DESC
    """,

    "v_segment_performance": """
        CREATE OR REPLACE VIEW v_segment_performance AS
        SELECT
            dc.current_rfm_segment                   AS segment,
            COUNT(DISTINCT dc.customer_id)           AS customer_count,
            ROUND(AVG(dc.lifetime_revenue), 2)       AS avg_lifetime_revenue,
            ROUND(SUM(dc.lifetime_revenue), 2)       AS total_segment_revenue,
            ROUND(AVG(dc.lifetime_orders), 1)        AS avg_orders_per_customer,
            ROUND(
                100.0 * SUM(CASE WHEN fo.otif_flag THEN 1 ELSE 0 END)
                      / NULLIF(COUNT(fo.order_id), 0),
                2
            )                                        AS otif_rate_pct,
            ROUND(AVG(rf.recency_days), 0)           AS avg_recency_days,
            ROUND(AVG(rf.frequency_count), 1)        AS avg_frequency,
            ROUND(AVG(rf.monetary_value), 2)         AS avg_monetary
        FROM dim_customer dc
        LEFT JOIN fact_rfm_scores rf
            ON dc.customer_id = rf.customer_id
           AND rf.is_current_flag = TRUE
        LEFT JOIN fact_orders fo
            ON dc.customer_id = fo.customer_id
        WHERE dc.active_flag = TRUE
          AND dc.current_rfm_segment IS NOT NULL
        GROUP BY dc.current_rfm_segment
        ORDER BY total_segment_revenue DESC
    """,
}

# ---------------------------------------------------------------------------
# Build views and export
# ---------------------------------------------------------------------------

def build_commercial_datamart(db_path: Path = DB_PATH, output_dir: Path = OUTPUT_DIR,
                               logger: logging.Logger = None) -> dict:
    if logger is None:
        logger = get_logger()

    logger.info("=" * 60)
    logger.info("COMMERCIAL DATA MART — START")
    logger.info(f"DB: {db_path}")
    logger.info("=" * 60)

    conn = duckdb.connect(str(db_path))
    output_dir.mkdir(parents=True, exist_ok=True)

    exports = {}
    for view_name, view_sql in VIEWS.items():
        logger.info(f"  Creating view: {view_name}")
        conn.execute(view_sql)
        df = conn.execute(f"SELECT * FROM {view_name}").df()
        csv_path = output_dir / f"{view_name}.csv"
        df.to_csv(csv_path, index=False)
        exports[view_name] = len(df)
        logger.info(f"    → {len(df):,} rows exported to {csv_path.name}")

    # Print segment performance inline
    seg_perf = conn.execute("SELECT * FROM v_segment_performance").df()
    logger.info("\n  SEGMENT PERFORMANCE SUMMARY:")
    logger.info(f"  {'Segment':<22} {'Customers':>9} {'Avg Rev':>12} {'OTIF%':>8} {'Avg Recency':>12}")
    logger.info("  " + "-" * 70)
    for _, row in seg_perf.iterrows():
        logger.info(
            f"  {str(row['segment']):<22} {int(row['customer_count']):>9,} "
            f"  £{float(row['avg_lifetime_revenue']):>10,.2f} "
            f"  {float(row['otif_rate_pct'] or 0):>7.1f}% "
            f"  {float(row['avg_recency_days'] or 0):>10.0f}d"
        )

    conn.close()
    logger.info("\nCOMMERCIAL DATA MART — COMPLETE")
    return exports


if __name__ == "__main__":
    import sys
    db = Path(sys.argv[1]) if len(sys.argv) > 1 else DB_PATH
    logger = get_logger()
    exports = build_commercial_datamart(db_path=db, logger=logger)
    print(f"\nExported views:")
    for name, rows in exports.items():
        print(f"  {name}: {rows:,} rows")
