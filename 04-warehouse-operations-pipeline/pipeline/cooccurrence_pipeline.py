"""
cooccurrence_pipeline.py — SKU Co-occurrence Engine
DHL Data Engineer Portfolio — Project 04

Groups pick tasks by (warehouse_id, task_date, shift) → "pick sessions"
Within each session, finds all SKU pairs picked together
Counts co-occurrence frequency; calculates lift score
Stores top 200 pairs per warehouse (lift > 1.0) in fact_cooccurrence
Exports adjacency_recommendations.csv
"""

import logging
import time
from datetime import datetime
from pathlib import Path
from itertools import combinations
from collections import defaultdict
import duckdb
import pandas as pd

BASE_DIR   = Path(__file__).resolve().parent.parent
DB_PATH    = BASE_DIR.parent / "01-sku-segmentation-pipeline" / "outputs" / "dhl_warehouse.duckdb"
OUTPUT_DIR = BASE_DIR / "outputs"
TOP_N_PAIRS_PER_WH = 200

def get_logger(name="cooccurrence_pipeline"):
    logger = logging.getLogger(name)
    if not logger.handlers:
        h = logging.StreamHandler()
        h.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                                          datefmt="%Y-%m-%d %H:%M:%S"))
        logger.addHandler(h); logger.setLevel(logging.INFO)
    return logger


def run_cooccurrence_pipeline(db_path: Path = DB_PATH, output_dir: Path = OUTPUT_DIR,
                               logger: logging.Logger = None) -> dict:
    if logger is None:
        logger = get_logger()
    t0 = time.time()

    logger.info("=" * 60)
    logger.info("CO-OCCURRENCE PIPELINE — START")
    logger.info("=" * 60)

    conn = duckdb.connect(str(db_path))

    # Load all Pick tasks
    logger.info("  Loading Pick tasks from fact_wms_tasks...")
    picks = conn.execute("""
        SELECT warehouse_id, task_date, shift, sku_id
        FROM fact_wms_tasks
        WHERE task_type = 'Pick'
        ORDER BY warehouse_id, task_date, shift
    """).df()

    if len(picks) == 0:
        logger.warning("  No Pick tasks found — skipping")
        conn.close()
        return {"pairs_stored": 0}

    logger.info(f"  Loaded {len(picks):,} pick tasks across {picks['warehouse_id'].nunique()} warehouses")

    now = datetime.utcnow()
    all_pairs = []

    for wh_id, wh_picks in picks.groupby("warehouse_id"):
        logger.info(f"  Processing warehouse: {wh_id}...")

        # Group into sessions: (task_date, shift)
        sessions = wh_picks.groupby(["task_date", "shift"])["sku_id"].apply(list)

        # Count co-occurrences
        cooc_counts = defaultdict(int)
        sku_session_counts = defaultdict(int)
        total_sessions = 0

        for _, skus in sessions.items():
            unique_skus = list(set(skus))
            if len(unique_skus) < 2:
                continue
            total_sessions += 1
            for sku in unique_skus:
                sku_session_counts[sku] += 1
            for a, b in combinations(sorted(unique_skus), 2):
                cooc_counts[(a, b)] += 1

        if total_sessions == 0 or len(cooc_counts) == 0:
            logger.info(f"    No co-occurring SKU pairs in {wh_id}")
            continue

        logger.info(f"    {total_sessions:,} sessions | {len(cooc_counts):,} unique pairs found")

        # Calculate lift: lift(A,B) = P(A∩B) / (P(A) × P(B))
        # P(X) = sessions containing X / total sessions
        pair_results = []
        for (a, b), count in cooc_counts.items():
            p_a   = sku_session_counts[a] / total_sessions
            p_b   = sku_session_counts[b] / total_sessions
            p_ab  = count / total_sessions
            lift  = p_ab / (p_a * p_b) if (p_a * p_b) > 0 else 0.0
            if lift > 1.0:
                pair_results.append({
                    "sku_id_1": a, "sku_id_2": b,
                    "warehouse_id": wh_id,
                    "co_occurrence_count": count,
                    "co_occurrence_window": "shift",
                    "lift_score": round(lift, 4),
                })

        if not pair_results:
            logger.info(f"    No pairs with lift > 1.0 in {wh_id}")
            continue

        # Take top N by lift
        wh_df = pd.DataFrame(pair_results).nlargest(TOP_N_PAIRS_PER_WH, "lift_score")
        logger.info(f"    Keeping top {len(wh_df):,} pairs (lift > 1.0)")
        all_pairs.append(wh_df)

    if not all_pairs:
        logger.info("  No co-occurrence pairs to store")
        conn.close()
        return {"pairs_stored": 0}

    combined = pd.concat(all_pairs, ignore_index=True)

    # Clear existing and replace (full refresh — lift scores change with each load)
    conn.execute("DELETE FROM fact_cooccurrence")

    combined["pair_id"] = range(1, len(combined) + 1)
    combined["last_calculated_at"] = now

    cols = ["pair_id","sku_id_1","sku_id_2","warehouse_id",
            "co_occurrence_count","co_occurrence_window","lift_score","last_calculated_at"]
    col_list = ", ".join(f'"{c}"' for c in cols)
    conn.register("_cooc_staging", combined[cols])
    conn.execute(f"INSERT INTO fact_cooccurrence ({col_list}) SELECT {col_list} FROM _cooc_staging")
    conn.unregister("_cooc_staging")

    total_stored = conn.execute("SELECT COUNT(*) FROM fact_cooccurrence").fetchone()[0]
    logger.info(f"  Stored {total_stored:,} co-occurrence pairs across all warehouses")

    # Export
    output_dir.mkdir(parents=True, exist_ok=True)
    export_path = output_dir / "adjacency_recommendations.csv"
    export_df = conn.execute("""
        SELECT sku_id_1, sku_id_2, warehouse_id, co_occurrence_count, lift_score,
               'Consider adjacent slotting — high co-pick frequency' AS recommendation
        FROM fact_cooccurrence
        ORDER BY warehouse_id, lift_score DESC
    """).df()
    export_df.to_csv(export_path, index=False)
    logger.info(f"  Exported {len(export_df):,} adjacency recommendations to {export_path.name}")

    conn.close()
    logger.info(f"Co-occurrence pipeline complete in {round(time.time()-t0,2)}s")
    logger.info("CO-OCCURRENCE PIPELINE — COMPLETE")
    return {"pairs_stored": int(total_stored)}


if __name__ == "__main__":
    import sys
    db = Path(sys.argv[1]) if len(sys.argv) > 1 else DB_PATH
    stats = run_cooccurrence_pipeline(db_path=db)
    print(f"Pairs stored: {stats.get('pairs_stored', 0)}")
