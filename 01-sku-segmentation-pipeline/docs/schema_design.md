# Schema Design — DHL SKU Segmentation Warehouse
**Project:** 01 — SKU Segmentation Pipeline  
**Author:** Vinyl Kiran Anipe (Data Engineer)  
**Database:** `outputs/dhl_warehouse.duckdb`  
**Version:** 1.0 · 2024

---

## 1. Design Philosophy

The warehouse uses a **Kimball-style star schema**. This was chosen over a 3NF relational schema or a flat denormalised table for three reasons:

First, the primary consumers are BA/DA analysts running aggregation queries — group-by revenue by category, stockout rates by warehouse and month, inventory trends by SKU segment. Star schemas are optimised for this query pattern: a single join from the fact table to the relevant dimension gives the analyst everything they need without navigating a normalised graph.

Second, DuckDB's columnar engine handles star schema joins efficiently at the scale of this dataset (574k demand rows, 19k snapshot rows). The overhead of normalisation is not warranted at this scale — the gain in storage efficiency is negligible compared to the gain in query simplicity.

Third, the BA/DA portfolio that consumes this warehouse was designed with a specific set of KPIs and analytical questions. The schema was designed to answer those questions directly, not to be a generic operational store.

---

## 2. Entity Relationship Overview

```
                    ┌──────────────┐
                    │   dim_date   │
                    │  (date_key)  │
                    └──────┬───────┘
                           │
          ┌────────────────┼────────────────┐
          │                │                │
  ┌───────┴──────┐  ┌──────┴───────┐  ┌────┴──────────────┐
  │  dim_sku     │  │fact_daily    │  │fact_inventory_    │
  │  (sku_key)   │◄─│demand        │──│snapshot           │
  └───────┬──────┘  │              │  │                   │
          │         │  date_key FK │  │  date_key FK      │
  ┌───────┴──────┐  │  sku_key  FK │  │  sku_key  FK      │
  │dim_supplier  │  │  wh_key   FK │  │  wh_key   FK      │
  │(supplier_key)│  └──────┬───────┘  └────┬──────────────┘
  └──────────────┘         │               │
                    ┌──────┴───────┐        │
                    │dim_warehouse │◄───────┘
                    │(warehouse_key│
                    └──────────────┘
```

**Two fact tables** (grain below):
- `fact_daily_demand` — one row per SKU per warehouse per day
- `fact_inventory_snapshot` — one row per SKU per warehouse per month-end snapshot

**Four dimension tables:**
- `dim_date` — full calendar for 2022–2023
- `dim_sku` — SKU master attributes
- `dim_warehouse` — three DHL warehouse sites
- `dim_supplier` — 80 suppliers

---

## 3. Table Descriptions

### dim_date
**Grain:** One row per calendar day (2022-01-01 to 2023-12-31 = 730 rows).  
**Purpose:** Enables time-based aggregation at day, week, month, quarter, and year granularity without requiring date manipulation in analytical queries.  
**Key design decisions:**  
- `date_key` is an integer in YYYYMMDD format (e.g. 20220115). This is faster to join on than a DATE type and is human-readable in query results.  
- `season` uses Northern Hemisphere convention (Spring: Mar–May, Summer: Jun–Aug, Autumn: Sep–Nov, Winter: Dec–Feb).  
- `is_weekend`, `is_month_end`, `is_quarter_end` are boolean flags stored explicitly to avoid repeated CASE expressions in downstream queries.

### dim_sku
**Grain:** One row per SKU (2,000 rows including inactive SKUs).  
**Purpose:** Central product dimension. Contains all attributes needed to segment, filter, and group SKUs in analytical queries.  
**Key design decisions:**  
- Includes both ABC class (revenue-based) and XYZ class (variability-based) — together they define the 9-cell segmentation matrix (AX, AY, AZ, ... CZ) that drives replenishment policy.  
- `supplier_id` is a natural key reference — not a FK enforced at DB level — to allow SKUs with no supplier to load cleanly.  
- `active_flag` allows filtering to active SKUs without deleting historical records.

### dim_warehouse
**Grain:** One row per warehouse site (3 rows: IL02, NJ01, TX03).  
**Purpose:** Enables geographic and regional aggregation. Small dimension — values are hardcoded from known business data.  
**Key design decisions:**  
- Region (Midwest, Northeast, South) enables regional rollup without requiring the analyst to know which warehouse maps to which region.  
- `timezone` is stored as an IANA timezone string to support any future time-of-day analysis.

### dim_supplier
**Grain:** One row per supplier (80 rows).  
**Purpose:** Enables supplier performance analysis — linking SKU procurement risk to supplier OTIF rates and defect rates.  
**Key design decisions:**  
- `otif_rate`, `fill_rate`, `defect_rate` are supplier-level averages from the source data. These are historical averages, not real-time metrics.

### fact_daily_demand
**Grain:** One row per SKU per warehouse per day (574,509 rows).  
**Purpose:** Primary analytical fact table. Supports demand analysis, stockout analysis, revenue reporting, and fill rate trending.  
**Key design decisions:**  
- `quantity_unfulfilled` and `fill_rate` are derived at load time and stored in the fact table. This is a deliberate denormalisation — these metrics are queried so frequently that computing them at query time would add unnecessary overhead.  
- `abc_class` and `xyz_class` are stored as degenerate dimensions (not FK to a separate dimension) because they are attributes of the event, not of the SKU at load time — and they make single-table queries possible for the most common segmentation queries.  
- `revenue` is set to 0 for stockout records where the source is null. This is documented as a business rule, not an imputation.

### fact_inventory_snapshot
**Grain:** One row per SKU per warehouse per month-end snapshot (19,200 rows).  
**Purpose:** Tracks inventory position over time. Supports IRA trending, working capital analysis, and safety stock adequacy analysis.  
**Key design decisions:**  
- `inventory_record_accuracy` (available / on_hand) is computed at load and stored. Null when on_hand = 0 to avoid division errors.  
- Snapshot dates are month-end dates — this is inherent in the source data.

---

## 4. Indexing Strategy

DuckDB uses Adaptive Radix Tree (ART) indexes. Indexes are created on:

- All natural keys (sku_id, warehouse_id, supplier_id, full_date) — supports lookup joins in ETL and ad-hoc queries
- Category and ABC class on dim_sku — supports the most common filter predicates
- date_key, sku_key, warehouse_key on both fact tables — supports the most common join predicates
- stockout_flag on fact_daily_demand — supports filtered aggregation

DuckDB also performs automatic join reordering and column pruning, so explicit indexes matter less than in row-store databases. The indexes above are added for robustness and explicit documentation of expected query patterns.

---

## 5. Known Limitations

- **No slowly changing dimensions (SCD).** SKU attributes (ABC class, safety stock) are point-in-time snapshots from the source CSV. If an SKU's ABC class changes over the 24-month period, this is not captured — the dimension always reflects the current classification.
- **dim_warehouse is hardcoded.** The three warehouse records are built programmatically from known IDs, not from a source system. If a fourth warehouse is added to the source data, a code change is required.
- **No Zone dimension.** The source WMS data does not contain a Zone column. Product Category is used as a proxy in operational analysis (see lessons_learned.md in the BA/DA portfolio for context).
- **Snapshot granularity is monthly.** Daily inventory positions are not available in the source data — only month-end snapshots.
- **Supplier dimension is not linked to fact tables.** Supplier attributes are available via `dim_sku.supplier_id → dim_supplier.supplier_id`, but there is no direct FK in the fact tables. Supplier-level analysis requires a two-hop join.

---

## 6. Example Queries

### Query 1: Monthly revenue by ABC class and warehouse

```sql
SELECT
    d.year,
    d.month_name,
    s.abc_class,
    w.warehouse_name,
    SUM(f.revenue)             AS total_revenue,
    SUM(f.quantity_demanded)   AS total_demanded,
    SUM(f.quantity_fulfilled)  AS total_fulfilled,
    AVG(f.fill_rate)           AS avg_fill_rate,
    SUM(f.stockout_flag::INT)  AS stockout_days
FROM fact_daily_demand f
JOIN dim_date      d ON f.date_key      = d.date_key
JOIN dim_sku       s ON f.sku_key       = s.sku_key
JOIN dim_warehouse w ON f.warehouse_key = w.warehouse_key
WHERE d.year = 2023
GROUP BY d.year, d.month_num, d.month_name, s.abc_class, w.warehouse_name
ORDER BY d.month_num, s.abc_class, w.warehouse_name;
```

### Query 2: Top 20 SKUs by stockout rate (A-class only)

```sql
SELECT
    s.sku_id,
    s.category,
    s.abc_class,
    s.xyz_class,
    COUNT(*)                                        AS total_days,
    SUM(f.stockout_flag::INT)                       AS stockout_days,
    ROUND(SUM(f.stockout_flag::INT) * 100.0 / COUNT(*), 2) AS stockout_rate_pct,
    SUM(f.quantity_unfulfilled)                     AS total_unfulfilled_qty,
    SUM(f.revenue)                                  AS total_revenue
FROM fact_daily_demand f
JOIN dim_sku s ON f.sku_key = s.sku_key
WHERE s.abc_class = 'A'
GROUP BY s.sku_id, s.category, s.abc_class, s.xyz_class
HAVING SUM(f.stockout_flag::INT) > 0
ORDER BY stockout_rate_pct DESC
LIMIT 20;
```

### Query 3: Monthly inventory record accuracy by warehouse

```sql
SELECT
    d.year,
    d.month_num,
    d.month_name,
    w.warehouse_id,
    w.region,
    COUNT(*)                            AS snapshot_records,
    ROUND(AVG(i.on_hand_qty), 0)        AS avg_on_hand,
    ROUND(AVG(i.available_qty), 0)      AS avg_available,
    ROUND(AVG(i.inventory_record_accuracy) * 100, 2) AS avg_ira_pct,
    SUM(i.inventory_value)              AS total_inventory_value
FROM fact_inventory_snapshot i
JOIN dim_date      d ON i.date_key      = d.date_key
JOIN dim_warehouse w ON i.warehouse_key = w.warehouse_key
GROUP BY d.year, d.month_num, d.month_name, w.warehouse_id, w.region
ORDER BY d.year, d.month_num, w.warehouse_id;
```

---

*Schema Design v1.0 · Vinyl Kiran Anipe · DHL Data Engineer Portfolio · Project 01 · 2024*
