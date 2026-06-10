# Warehouse Operations Data Model
## DHL Data Engineer Portfolio — Project 04

---

## Entity Relationship Overview

The Project 04 data model extends the existing DHL warehouse star schema with five new tables focused on warehouse operations. The model captures WMS task execution, operator performance, location lifecycle, slotting recommendations, and SKU co-occurrence patterns.

```
dim_operator (1) ─────────────────────────────── (N) fact_wms_tasks
dim_location  (1) ─────────────────────────────── (N) fact_wms_tasks  [via location_id]
dim_warehouse (1) ─────────────────────────────── (N) fact_wms_tasks
dim_sku       (1) ─────────────────────────────── (N) fact_wms_tasks
dim_warehouse (1) ─────────────────────────────── (N) fact_slotting_history
dim_sku       (1) ─────────────────────────────── (N) fact_slotting_history
dim_warehouse (1) ─────────────────────────────── (N) fact_cooccurrence
dim_location  (N) ─── (SCD2 history chain) ─────── (N) dim_location
```

**Cardinalities:**
- `fact_wms_tasks` is the central fact table. Each row represents one WMS task execution (Pick, Putaway, Receiving, Replenishment, Cycle Count, Transfer). One operator performs many tasks; one warehouse contains many tasks; one SKU appears in many tasks.
- `fact_slotting_history` is a slowly changing fact — each row represents one slotting recommendation for a SKU-warehouse combination. One SKU-warehouse pair can have multiple historical recommendations (different dates, different statuses).
- `fact_cooccurrence` is a derived summary table — one row per unique SKU pair per warehouse, representing how often those SKUs are picked in the same session.
- `dim_location` uses SCD Type 2 — one location may have multiple rows representing different historical attribute states.
- `dim_operator` is a conformed dimension with anonymised operator IDs. One operator can appear in many WMS tasks across multiple days.

---

## SCD Type 2 for dim_location

### Why Location History Matters

A warehouse location (e.g., "NJ01-PF-A01-01") is not static. Over time:
- Zones change when slots are reslotted (Pick_Face → Reserve as demand decreases)
- Storage type changes when a cold-chain zone is decommissioned
- Capacity changes when racking is added or removed

If we simply overwrite the current row, we lose the ability to answer questions like:
- "What zone was SKU X in when it was generating 40 picks/day six months ago?"
- "When did this location change from Reserve to Pick_Face, and did accuracy improve after?"
- "How many slotting changes happened at warehouse TX03 in Q3 2023?"

SCD Type 2 answers all of these by keeping a full audit trail.

### How the Pattern Works

Each `dim_location` row has three tracking columns:

| Column | Meaning |
|---|---|
| `valid_from` | Timestamp when this version of the record became active |
| `valid_to` | Timestamp when this version was superseded (NULL = still current) |
| `is_current` | Boolean shortcut: TRUE = this is the active record |

When `wms_etl.py` detects a change in zone, storage_type, or active_flag for a location:
1. The old record's `valid_to` is set to `CURRENT_TIMESTAMP` and `is_current` is set to FALSE
2. A new record is inserted with `valid_from = CURRENT_TIMESTAMP`, `valid_to = NULL`, `is_current = TRUE`

To get the current state of all locations:
```sql
SELECT * FROM dim_location WHERE is_current = TRUE;
```

To reconstruct the warehouse layout at a specific past date:
```sql
SELECT * FROM dim_location
WHERE valid_from <= '2023-06-01'
  AND (valid_to IS NULL OR valid_to > '2023-06-01');
```

### SCD2 Trigger Fields

Not all attribute changes trigger a new SCD2 row — only operationally significant ones. The trigger fields in `wms_etl.py` are: `zone`, `storage_type`, and `active_flag`. Changes to `capacity_units` or `aisle`/`bay` coordinates alone do not trigger SCD2 (they represent corrections rather than meaningful operational changes).

---

## Slotting Pipeline Design

### Hot/Warm/Cool/Cold Classification

Pick frequency is calculated per SKU per warehouse over the prior 90 days. SKUs are classified using percentile boundaries applied to the current warehouse's distribution:

| Class | Percentile | Optimal Zone |
|---|---|---|
| Hot | Top 10% | Pick_Face |
| Warm | 70th–90th | Pick_Face (tolerated) |
| Cool | 40th–70th | Reserve |
| Cold | Bottom 40% | Reserve/Bulk |

Percentiles are computed per warehouse, not globally — a Hot SKU in a small warehouse may have fewer picks than a Cool SKU in the main hub.

### Mismatch Rules

Only two mismatch types generate a recommendation:
- **Hot SKU not in Pick_Face**: High-frequency SKU is buried in Reserve or Bulk, causing excessive travel per pick
- **Cold SKU in Pick_Face**: Low-frequency SKU occupying premium face space that should be used for faster-moving stock

Warm and Cool SKUs outside their ideal zones are not flagged — the margin of improvement is small enough that the disruption of moving them is not justified.

### Duplicate Prevention

Before inserting any new recommendation, the pipeline queries `fact_slotting_history` for all rows where `implementation_status = 'pending'` for the same (sku_id, warehouse_id) pair. If a pending recommendation already exists, no new row is inserted. This prevents the slotting queue from accumulating duplicate entries across weekly pipeline runs.

Once a recommendation is implemented (status updated to `implemented`) or rejected, the pipeline will generate a new recommendation on its next run if the SKU-warehouse combination is still misslotted.

---

## Co-occurrence Calculation Methodology

### What "Co-occurrence" Means

Two SKUs co-occur when they are both picked in the same **pick session** — defined as all pick tasks from the same warehouse, same date, and same shift (Morning/Afternoon/Night). If an operator picks SKU-A and SKU-B in the same session, that is one co-occurrence event.

Co-occurrence count is the total number of sessions in which both SKUs appeared.

### What Lift Score Means

Lift measures whether two SKUs are picked together **more often than you would expect by chance**:

```
lift(A, B) = P(A and B) / (P(A) × P(B))
```

Where P(X) = the fraction of all sessions that contained SKU X.

- **Lift = 1.0**: SKUs A and B are picked together exactly as often as you'd expect if their picks were independent — no special relationship
- **Lift > 1.0**: SKUs are picked together more often than chance — they have affinity
- **Lift < 1.0**: SKUs are picked together less often than chance — they rarely appear in the same order

In plain English: **a lift score of 3.0 means these two SKUs are picked together three times more often than you would expect if their picks were unrelated.** This is a strong signal that they frequently appear on the same customer orders.

### Why This Matters for Operations

If two SKUs have high lift, storing them near each other (adjacent bays, same aisle) reduces the total distance travelled per pick session. The warehouse manager can use the `v_cooccurrence_adjacency` view to see the top 50 high-lift pairs per warehouse and decide whether a slot swap is worthwhile.

Only pairs with lift > 1.0 are stored. The pipeline keeps the top 200 pairs per warehouse by lift score.
