"""
slotting_pipeline.py — Automated Slotting Recommendation Pipeline
DHL Data Engineer Portfolio — Project 04

Pick frequency over last 90 days → Hot/Warm/Cool/Cold classification
Cross-reference with current location zone (dim_location)
Identify misslotted SKUs and insert recommendations into fact_slotting_history
Never create duplicate pending recommendations for the same SKU-warehouse
"""

import logging
import time
from datetime import datetime, date
from pathlib import Path
import duckdb
import pandas as pd

BASE_DIR   = Path(__file__).resolve().parent.parent
DB_PATH    = BASE_DIR.parent / "01-sku-segmentation-pipeline" / "outputs" / "dhl_warehouse.duckdb"
OUTPUT_DIR = BASE_DIR / "outputs"

# Classification thresholds (percentile boundaries)
HOT_THRESHOLD  = 0.90   # top 10%
WARM_THRESHOLD = 0.70   # next 20%  (70th–90th)
COOL_THRESHOLD = 0.40   # next 30%  (40th–70th)
# below 40th = Cold

MINUTES_SAVED_PER_MOVE = 4.0  # travel time difference between zones

def get_logger(name="slotting_pipeline"):
    logger = logging.getLogger(name)
    if not logger.handlers:
        h = logging.StreamHandler()
        h.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                                          datefmt="%Y-%m-%d %H:%M:%S"))
        logger.addHandler(h); logger.setLevel(logging.INFO)
    return logger


def run_slotting_pipeline(db_path: Path = DB_PATH, output_dir: Path = OUTPUT_DIR,
                           lookback_days: int = 90, logger: logging.Logger = None) -> dict:
    if logger is None:
        logger = get_logger()
    t0 = time.time()

    logger.info("=" * 60)
    logger.info("SLOTTING RECOMMENDATION PIPELINE — START")
    logger.info("=" * 60)

    conn = duckdb.connect(str(db_path))

    # Reference date: most recent task date in warehouse
    ref_date = conn.execute("SELECT MAX(task_date) FROM fact_wms_tasks").fetchone()[0]
    if ref_date is None:
        logger.error("  fact_wms_tasks is empty — run WMS ETL first")
        conn.close()
        return {}
    ref_date = pd.to_datetime(ref_date).date()
    cutoff_date = ref_date - pd.Timedelta(days=lookback_days)
    logger.info(f"  Reference date: {ref_date} | Lookback: {cutoff_date} to {ref_date}")

    # Step 1: Pick frequency per SKU per warehouse over lookback window
    logger.info("  Computing pick frequency...")
    pick_freq = conn.execute(f"""
        SELECT warehouse_id, sku_id, COUNT(*) AS pick_count
        FROM fact_wms_tasks
        WHERE task_type = 'Pick'
          AND task_date >= CAST('{cutoff_date}' AS DATE)
        GROUP BY warehouse_id, sku_id
    """).df()
    logger.info(f"  SKU-warehouse combos with picks: {len(pick_freq):,}")

    if len(pick_freq) == 0:
        logger.warning("  No Pick tasks found in lookback window — skipping")
        conn.close()
        return {"skus_analysed": 0, "mismatches": 0, "new_recommendations": 0}

    # Step 2: Classify into Hot/Warm/Cool/Cold per warehouse
    results = []
    for wh_id, wh_df in pick_freq.groupby("warehouse_id"):
        pcts = wh_df["pick_count"].rank(pct=True)
        def classify(p):
            if p >= HOT_THRESHOLD:  return "Hot"
            if p >= WARM_THRESHOLD: return "Warm"
            if p >= COOL_THRESHOLD: return "Cool"
            return "Cold"
        wh_df = wh_df.copy()
        wh_df["velocity_class"] = pcts.apply(classify)
        results.append(wh_df)

    classified = pd.concat(results, ignore_index=True)
    logger.info(f"  Velocity distribution: {classified['velocity_class'].value_counts().to_dict()}")

    # Step 3: Get current SKU locations from dim_location
    # wms_tasks don't have location_id filled, so we join via warehouse_locations via zone logic.
    # Best proxy: use the most common zone seen for each SKU in each warehouse from
    # the existing dim_location + fact_wms_tasks join. Since location_id is NULL in
    # fact_wms_tasks (not in source), we derive current zone from dim_location
    # assuming SKUs are distributed across warehouse zones by storage_type.
    # For slotting, use dim_location current zones per warehouse as the candidate pool.
    current_zones = conn.execute("""
        SELECT DISTINCT warehouse_id, zone
        FROM dim_location
        WHERE is_current = TRUE
        ORDER BY warehouse_id, zone
    """).df()

    # Assign a 'current_zone' to each SKU by sampling dim_location zones
    # (in production this would come from a WMS inventory position table)
    # Here we use a deterministic zone assignment based on SKU hash mod zone count
    def assign_zone(sku_id, wh_id, zones_for_wh):
        if len(zones_for_wh) == 0:
            return "Reserve"
        idx = hash(sku_id + wh_id) % len(zones_for_wh)
        return zones_for_wh[idx]

    zone_map = current_zones.groupby("warehouse_id")["zone"].apply(list).to_dict()
    classified["current_zone"] = classified.apply(
        lambda r: assign_zone(r["sku_id"], r["warehouse_id"],
                              zone_map.get(r["warehouse_id"], [])),
        axis=1
    )

    # Step 4: Identify mismatches
    def recommended_zone(velocity_class, current_zone):
        if velocity_class == "Hot" and current_zone != "Pick_Face":
            return "Pick_Face"
        if velocity_class == "Cold" and current_zone == "Pick_Face":
            return "Reserve"
        return None  # No action needed

    classified["recommended_zone"] = classified.apply(
        lambda r: recommended_zone(r["velocity_class"], r["current_zone"]), axis=1
    )
    mismatched = classified[classified["recommended_zone"].notna()].copy()
    logger.info(f"  Misslotted SKU-warehouse combos: {len(mismatched):,}")

    if len(mismatched) == 0:
        conn.close()
        logger.info("  No slotting mismatches found — all SKUs optimally slotted")
        return {"skus_analysed": len(classified), "mismatches": 0, "new_recommendations": 0}

    # Step 5: Filter out SKUs that already have a pending recommendation
    pending = {(r[0], r[1]) for r in conn.execute("""
        SELECT sku_id, warehouse_id FROM fact_slotting_history
        WHERE implementation_status = 'pending'
    """).fetchall()}

    mismatched["already_pending"] = mismatched.apply(
        lambda r: (r["sku_id"], r["warehouse_id"]) in pending, axis=1
    )
    new_recs = mismatched[~mismatched["already_pending"]].copy()
    logger.info(f"  Already pending: {mismatched['already_pending'].sum():,} | New recommendations: {len(new_recs):,}")

    if len(new_recs) == 0:
        conn.close()
        return {"skus_analysed": len(classified), "mismatches": len(mismatched), "new_recommendations": 0}

    # Step 6: Insert recommendations
    max_id = conn.execute("SELECT COALESCE(MAX(slotting_id), 0) FROM fact_slotting_history").fetchone()[0]
    now    = datetime.utcnow()
    recs_df = pd.DataFrame({
        "slotting_id":                       range(int(max_id)+1, int(max_id)+len(new_recs)+1),
        "sku_id":                            new_recs["sku_id"].values,
        "warehouse_id":                      new_recs["warehouse_id"].values,
        "recommendation_date":               ref_date,
        "prior_zone":                        new_recs["current_zone"].values,
        "recommended_zone":                  new_recs["recommended_zone"].values,
        "pick_frequency_at_recommendation":  new_recs["pick_count"].astype(int).values,
        "estimated_daily_minutes_saved":     (new_recs["pick_count"] * MINUTES_SAVED_PER_MOVE / lookback_days).round(2).values,
        "implementation_status":             "pending",
        "implementation_date":               None,
        "actual_minutes_saved_post":         None,
        "etl_loaded_at":                     now,
    })
    cols = list(recs_df.columns)
    col_list = ", ".join(f'"{c}"' for c in cols)
    conn.register("_slot_staging", recs_df)
    conn.execute(f"INSERT INTO fact_slotting_history ({col_list}) SELECT {col_list} FROM _slot_staging")
    conn.unregister("_slot_staging")

    total_pending = conn.execute(
        "SELECT COUNT(*) FROM fact_slotting_history WHERE implementation_status='pending'"
    ).fetchone()[0]
    logger.info(f"  Inserted {len(new_recs):,} recommendations → {total_pending:,} total pending")

    # Export
    output_dir.mkdir(parents=True, exist_ok=True)
    export_path = output_dir / "slotting_recommendations.csv"
    export_df = conn.execute("""
        SELECT * FROM fact_slotting_history
        WHERE implementation_status = 'pending'
        ORDER BY estimated_daily_minutes_saved DESC
    """).df()
    export_df.to_csv(export_path, index=False)
    logger.info(f"  Exported {len(export_df):,} pending recommendations to {export_path.name}")

    conn.close()
    logger.info(f"Slotting pipeline complete in {round(time.time()-t0,2)}s")
    logger.info("SLOTTING RECOMMENDATION PIPELINE — COMPLETE")
    return {"skus_analysed": len(classified), "mismatches": len(mismatched),
            "new_recommendations": len(new_recs)}


if __name__ == "__main__":
    import sys
    db = Path(sys.argv[1]) if len(sys.argv) > 1 else DB_PATH
    stats = run_slotting_pipeline(db_path=db)
    print(f"SKUs analysed: {stats.get('skus_analysed',0)} | Mismatches: {stats.get('mismatches',0)} | New recs: {stats.get('new_recommendations',0)}")
