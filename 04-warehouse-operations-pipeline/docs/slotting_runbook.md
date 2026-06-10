# Slotting Pipeline Runbook
## DHL Data Engineer Portfolio — Project 04

---

## When to Run the Slotting Pipeline

Run the slotting pipeline after every WMS ETL load that brings in at least one week of new pick data. In a production cadence:

- **Weekly (recommended)**: After the Monday morning ETL run. Ensures recommendations reflect the most recent 90-day pick window.
- **After a major season change**: Re-run after peak periods (e.g., Q4 e-commerce peak) end, as pick velocities will shift significantly.
- **After adding a new SKU range**: New SKUs accumulate pick history before appearing in the Hot tier — running after 30+ days of data gives meaningful results.
- **Do not run more than once per week on the same dataset**: The pipeline is idempotent (duplicate-safe), but running it daily on unchanged data wastes compute and does not produce new recommendations.

**Command:**
```bash
cd 04-warehouse-operations-pipeline
python pipeline/slotting_pipeline.py
# Or with a custom DB path:
python pipeline/slotting_pipeline.py --db-path /path/to/dhl_warehouse.duckdb
```

---

## How to Interpret Recommendations

Open `outputs/slotting_recommendations.csv` (or query `v_slotting_queue`). The most important columns:

| Column | What It Tells You |
|---|---|
| `sku_id` | Which SKU needs to move |
| `warehouse_id` | Which warehouse it's in |
| `prior_zone` | Where it is currently slotted |
| `recommended_zone` | Where it should be moved |
| `pick_frequency_at_recommendation` | Picks in the last 90 days (high = Hot, low = Cold) |
| `est_daily_minutes_saved` | Expected time saving per day if moved (in minutes) |
| `est_annual_minutes_saved` | Extrapolated to 260 working days |

**Prioritisation:** Sort by `est_daily_minutes_saved` descending. Focus first on recommendations where the annualised savings exceed one working day (480 minutes). These are the slot changes with the highest ROI for the disruption of moving stock.

**Types of recommendation:**
- `prior_zone=Reserve → recommended_zone=Pick_Face`: Hot SKU buried in storage — move to face to cut pick travel
- `prior_zone=Pick_Face → recommended_zone=Reserve`: Cold SKU wasting face space — move to back, free up prime slot for faster movers

---

## How to Mark a Recommendation as Implemented

Once the warehouse team has physically moved a SKU to its new location, update the status in DuckDB:

```python
import duckdb
conn = duckdb.connect("01-sku-segmentation-pipeline/outputs/dhl_warehouse.duckdb")
conn.execute("""
    UPDATE fact_slotting_history
    SET implementation_status = 'implemented',
        implementation_date   = CURRENT_DATE
    WHERE slotting_id = <id>
""")
conn.close()
```

Or to bulk-update all moves completed on a given date:
```python
conn.execute("""
    UPDATE fact_slotting_history
    SET implementation_status = 'implemented',
        implementation_date   = '2024-01-15'
    WHERE sku_id = 'SKU-XXXXX'
      AND warehouse_id = 'DHL-WH-NJ01'
      AND implementation_status = 'pending'
""")
```

To mark a recommendation as rejected (e.g., the move was reviewed but the warehouse manager decided against it):
```python
conn.execute("""
    UPDATE fact_slotting_history
    SET implementation_status = 'rejected'
    WHERE slotting_id = <id>
""")
```

Rejected recommendations will be re-evaluated on the next slotting run — if the SKU is still misslotted (and no pending record exists), a new recommendation will be inserted.

---

## How to Measure Actual Improvement Post-Implementation

After implementing a slotting change, wait at least 30 days to collect post-move pick data, then compare actual pick durations for the moved SKU before vs after the move.

**Step 1: Record post-implementation metrics**
```python
import duckdb
conn = duckdb.connect("...")

# Calculate average pick duration for the SKU after the implementation date
post_avg = conn.execute("""
    SELECT AVG(duration_min) AS avg_min
    FROM fact_wms_tasks
    WHERE sku_id = 'SKU-XXXXX'
      AND warehouse_id = 'DHL-WH-NJ01'
      AND task_type = 'Pick'
      AND task_date > (
          SELECT implementation_date FROM fact_slotting_history WHERE slotting_id = <id>
      )
""").fetchone()[0]

# Calculate pre-implementation baseline (90 days before recommendation)
pre_avg = conn.execute("""
    SELECT AVG(duration_min) AS avg_min
    FROM fact_wms_tasks
    WHERE sku_id = 'SKU-XXXXX'
      AND warehouse_id = 'DHL-WH-NJ01'
      AND task_type = 'Pick'
      AND task_date < (
          SELECT recommendation_date FROM fact_slotting_history WHERE slotting_id = <id>
      )
""").fetchone()[0]

minutes_saved_per_pick = (pre_avg or 0) - (post_avg or 0)
print(f"Actual saving per pick: {minutes_saved_per_pick:.1f} min")

# Update fact_slotting_history
conn.execute("""
    UPDATE fact_slotting_history
    SET actual_minutes_saved_post = ?
    WHERE slotting_id = ?
""", [minutes_saved_per_pick, <id>])
conn.close()
```

**Step 2: Compare estimated vs actual**
```sql
SELECT slotting_id, sku_id, warehouse_id,
       estimated_daily_minutes_saved,
       actual_minutes_saved_post,
       ROUND(actual_minutes_saved_post / NULLIF(estimated_daily_minutes_saved, 0) * 100, 1)
         AS realisation_pct
FROM fact_slotting_history
WHERE implementation_status = 'implemented'
  AND actual_minutes_saved_post IS NOT NULL
ORDER BY realisation_pct DESC;
```

A realisation above 80% means the model's estimate was accurate. Consistently low realisation across all recommendations suggests the `MINUTES_SAVED_PER_MOVE = 4.0` constant in `slotting_pipeline.py` should be recalibrated using observed data.

---

## Adding a New Warehouse

No code changes are required. The slotting pipeline processes all warehouses present in `fact_wms_tasks`. When a new warehouse is added to `dim_warehouse` and tasks start flowing in, it will automatically appear in the next slotting run.

The only prerequisite is that `warehouse_locations.csv` includes location records for the new warehouse, so that zone assignments in `dim_location` are available for cross-referencing.

---

## Troubleshooting

**No recommendations generated despite mismatched slots:**
- Check that `fact_wms_tasks` has at least 14 days of Pick tasks (the pipeline needs sufficient history to compute meaningful velocity percentiles)
- Verify `dim_location` has current records for the warehouses: `SELECT COUNT(*) FROM dim_location WHERE is_current = TRUE`
- Check that the lookback window (default 90 days) overlaps with the task date range in `fact_wms_tasks`

**All SKUs classified as "Cold":**
- Likely indicates the warehouse has very few Pick tasks relative to Putaway or other task types
- Run: `SELECT task_type, COUNT(*) FROM fact_wms_tasks GROUP BY task_type` to confirm Pick data is present

**Duplicate pending recommendations appearing:**
- This should not happen with the deduplication logic, but if it does: `SELECT sku_id, warehouse_id, COUNT(*) FROM fact_slotting_history WHERE implementation_status='pending' GROUP BY sku_id, warehouse_id HAVING COUNT(*) > 1`
- Clean up with: `DELETE FROM fact_slotting_history WHERE slotting_id NOT IN (SELECT MIN(slotting_id) FROM fact_slotting_history WHERE implementation_status='pending' GROUP BY sku_id, warehouse_id)`
