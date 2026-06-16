# Onboarding Guide — DHL Data Engineering Platform
## DE Portfolio — Project 06 Reference Artifact

This guide is written for a data engineer joining the project for the first time. By the end of it, you will be able to run any pipeline, interpret any output, and add a new pipeline without breaking existing ones.

**Time to get through this guide:** ~45 minutes. Do it once, and keep it open in a tab for the first week.

---

## 1. What You're Looking At

The DHL platform is a five-pipeline analytical data warehouse built in Python + DuckDB. It covers the full logistics analytics stack: inventory segmentation, demand forecasting, customer RFM scoring, warehouse operations, and WMS reporting.

All five pipelines share a single database file (`dhl_warehouse.duckdb`) and a common set of source CSVs in `shared/data/dhl-synthetic/`. The synthetic dataset simulates a real DHL operation: 3 warehouses, 2,000 SKUs, 60 operators, 500 customers, two years of daily demand and WMS task data.

**The most important file on day one:** `06-de-artifacts/lineage/table_inventory.md`. It tells you what every table is, which project created it, how many rows it has, and who reads it.

---

## 2. Repository Structure

```
dhl-data-engineer/
├── shared/
│   └── data/dhl-synthetic/          ← Source CSV files (read-only; never modify)
│       ├── sku_master.csv
│       ├── demand_history.csv
│       ├── customers.csv
│       ├── orders.csv
│       ├── wms_tasks.csv
│       ├── warehouse_locations.csv
│       ├── inventory_snapshot.csv
│       ├── suppliers.csv
│       └── operator_data.csv
├── 01-sku-segmentation-pipeline/    ← Project 01: ABC/velocity segmentation
├── 02-demand-forecasting-pipeline/  ← Project 02: 90-day demand forecasts
├── 03-customer-pipeline/            ← Project 03: RFM scoring, A/B testing
├── 04-warehouse-operations-pipeline/← Project 04: WMS ETL, slotting, co-occurrence
├── 05-wms-data-warehouse/           ← Project 05: Pre-aggregated KPI warehouse
├── 06-de-artifacts/                 ← Project 06: This reference documentation
└── dhl_warehouse.duckdb             ← The warehouse (do not connect to this directly)
```

Each project folder follows the same internal layout:

```
NN-<project>/
  schema/      ← DDL (.sql) and schema runner (setup_schema.py)
  etl/         ← Extraction, transformation, and loading scripts
  pipeline/    ← Domain pipeline orchestration (forecasting, slotting, etc.)
  quality/     ← DQ framework and anomaly detection
  serving/     ← View creation and CSV export
  outputs/     ← Generated CSVs, reports, benchmarks
  docs/        ← Schema design, data dictionary, runbook
```

---

## 3. Environment Setup

### Prerequisites

- Python 3.9+
- pip

### Install Dependencies

All five projects use the same dependencies. From the repo root:

```bash
pip install duckdb pandas numpy --break-system-packages
```

That's it. No database server, no cloud credentials, no Docker.

### Verify the Warehouse

```bash
python3 -c "
import duckdb
conn = duckdb.connect('/tmp/dhl_check.duckdb')
conn.execute(\"ATTACH 'dhl_warehouse.duckdb' AS wh (READ_ONLY)\")
result = conn.execute(\"SELECT COUNT(*) FROM wh.information_schema.tables WHERE table_schema = 'main'\").fetchone()
print(f'Tables in warehouse: {result[0]}')
conn.close()
"
```

You should see `Tables in warehouse: 26`. If you see an error, check that `dhl_warehouse.duckdb` exists in the repo root.

---

## 4. The Working Copy Rule

**Never connect your pipeline directly to `dhl_warehouse.duckdb`.**

The database file is on a mounted filesystem. DuckDB creates a WAL (write-ahead log) when any connection opens for writing. If the WAL exists and you reconnect, DuckDB tries to replay it — and if the tables already exist, it throws `Catalog Error: Table with name "X" already exists`.

**The correct pattern for every pipeline:**

```bash
# Before pipeline work
cp dhl_warehouse.duckdb /tmp/dhl_pN.duckdb   # N = project number

# Run your pipeline
python3 05-wms-data-warehouse/etl/wms_warehouse_etl.py /tmp/dhl_p5.duckdb

# After pipeline work (copy back)
cp /tmp/dhl_p5.duckdb dhl_warehouse.duckdb
```

In Python scripts, `DB_PATH = Path("/tmp/dhl_pN.duckdb")` is already set as the default. The copy step is done manually before and after running scripts.

---

## 5. Warehouse Structure

The warehouse uses a Kimball star schema. If you're not familiar with star schemas: dimensions contain the "who/what/where/when" descriptors; facts contain the measurable events. You JOIN them to answer business questions.

### Five Subject Areas

| # | Subject Area | Core Dimension | Core Fact | Key Metric |
|---|---|---|---|---|
| 1 | Inventory | dim_sku | fact_daily_demand | daily demand per SKU |
| 2 | Forecasting | dim_model | fact_forecast | 90-day demand forecast with CI |
| 3 | Customer | dim_customer | fact_orders | RFM scores and segments |
| 4 | Warehouse Ops | dim_operator | fact_wms_tasks | pick accuracy, task throughput |
| 5 | WMS Reporting | (derived) | fact_wms_daily_kpis | pre-aggregated network KPIs |

### Key Conventions

- All dimension surrogate keys: `<entity>_key` (INTEGER)
- All natural/business keys: `<entity>_id` (VARCHAR)
- All accuracy metrics: stored as percentages (0–100), not fractions
- All operators: anonymised with SHA-256 prefix `OP-XXXXXXXX`
- All tables have `etl_loaded_at TIMESTAMP` as the last column

### Finding Any Table

Use `table_inventory.md` in `06-de-artifacts/lineage/`. It has every table sorted by subject area with row counts and refresh patterns.

To query in DuckDB:

```python
import duckdb
conn = duckdb.connect("/tmp/dhl_p5.duckdb")

# What tables exist?
conn.execute("SHOW TABLES").df()

# How many rows in a table?
conn.execute("SELECT COUNT(*) FROM fact_wms_tasks").fetchone()

# What columns does a table have?
conn.execute("DESCRIBE fact_wms_daily_kpis").df()
```

---

## 6. Running Pipelines

### Run Order (important — later projects depend on earlier ones)

```
Project 01 → Project 02 → Project 03 → Project 04 → Project 05
```

Project 05 reads `fact_wms_tasks` (built by Project 04) and `fact_inventory_snapshot` (built by Project 01). If you only need to re-run Project 05, you can skip 01–04 as long as the warehouse already has data.

### Running Project 05 (the most commonly re-run pipeline)

```bash
# 1. Copy warehouse to /tmp
cp dhl_warehouse.duckdb /tmp/dhl_p5.duckdb

# 2. Run ETL (populates fact_error_log + fact_inventory_accuracy)
python3 05-wms-data-warehouse/etl/wms_warehouse_etl.py

# 3. Build aggregations (populates the 3 pre-agg tables)
python3 05-wms-data-warehouse/aggregations/build_aggregations.py

# 4. Validate aggregations (compares pre-agg vs raw counts)
python3 05-wms-data-warehouse/aggregations/validate_aggregations.py

# 5. Run DQ framework (exports outputs/dq_report.csv)
python3 05-wms-data-warehouse/quality/dq_framework.py

# 6. Run anomaly detection (exports outputs/anomaly_flags.csv)
python3 05-wms-data-warehouse/quality/anomaly_detection.py

# 7. Build serving layer (creates 7 views + exports CSVs)
python3 05-wms-data-warehouse/serving/warehouse_serving_layer.py

# 8. Copy back to workspace
cp /tmp/dhl_p5.duckdb dhl_warehouse.duckdb
```

You can run each script with `python3 script.py /path/to/custom.duckdb` to point at a different DB file.

### Checking a Run Succeeded

```python
conn = duckdb.connect("/tmp/dhl_p5.duckdb")
conn.execute("""
    SELECT pipeline_name, run_start, run_end, status, rows_processed, rows_inserted
    FROM meta_pipeline_runs
    ORDER BY run_start DESC
    LIMIT 5
""").df()
```

A successful run shows `status = 'success'` and non-zero `rows_processed`.

---

## 7. Interpreting DQ Reports

After any pipeline run, check `outputs/dq_report.csv`. The columns are:

| Column | Meaning |
|---|---|
| `check_name` | What was checked |
| `severity` | CRITICAL / HIGH / MEDIUM / LOW |
| `status` | PASS / FAIL / WARN / INFO |
| `metric_value` | The actual measured value |
| `threshold` | The value it needed to be at or better than |
| `message` | Human-readable result |
| `checked_at` | When the check ran |

### What to Do with Each Status

**CRITICAL + FAIL:** Stop. The pipeline has a data integrity problem. Do not use the outputs. Check `meta_pipeline_runs.error_message` for the exception. Most common causes: source CSV has NULLs in a key column, or an incremental load produced duplicate primary keys. Fix the source or the ETL logic, then re-run.

**HIGH + FAIL:** Investigate before using outputs for decisions. HIGH failures mean something unexpected happened (accuracy out of range, referential integrity broken). Often caused by source data quality issues rather than pipeline bugs.

**MEDIUM + WARN / FAIL:** Log for review. Task duration outside expected bounds (1–120 min), productivity outside expected range (1–80 picks/hour). These are informational — check for data entry errors in source but don't block pipeline use.

**LOW + INFO:** Informational only. Zero-task operators, SKUs with no picks. Expected for inactive operators or slow-moving SKUs.

### Interpreting Anomaly Flags (`anomaly_flags.csv`)

The anomaly detection script produces flags in four categories:

| flag_type | What it means | Typical count |
|---|---|---|
| `accuracy_drop` | A warehouse's pick accuracy fell >2σ below its 90-day mean | ~109 |
| `volume_drop` | Daily task volume fell >3σ below 90-day mean | ~0 (rare in synthetic data) |
| `operator_accuracy_drop` | An operator's accuracy fell >5pp vs their 30-day personal average | ~704 |
| `error_rate_spike` | An error code occurred at >2× its 30-day baseline | ~4 |

High `operator_accuracy_drop` counts are normal — they reflect natural variation in individual performance. They become actionable when an operator has repeated flags (5+ days in a row) — those operators appear in `v_coaching_list`.

---

## 8. Common Troubleshooting

### "Catalog Error: Table with name X already exists"

**Cause:** You connected directly to `dhl_warehouse.duckdb` and a stale WAL exists.

**Fix:** Use the `/tmp` working copy pattern. Copy the warehouse to `/tmp/dhl_pN.duckdb` and connect there.

### "NOT NULL constraint failed: meta_pipeline_runs.run_id"

**Cause:** The `start_run()` function tried to let DuckDB auto-generate the PK, which it doesn't support.

**Fix:** Ensure `start_run()` computes `next_id = conn.execute("SELECT COALESCE(MAX(run_id), 0) + 1 FROM meta_pipeline_runs").fetchone()[0]` and passes it explicitly in the INSERT.

### "NotImplementedException: Non-inner join on correlated columns not supported"

**Cause:** A view or query uses `, ref LEFT JOIN dim` syntax where `ref` is a correlated CTE.

**Fix:** Replace comma-join with `CROSS JOIN ref` in the inner CTE, then do the `LEFT JOIN dim` in the outer SELECT. See `de_coding_standards.md` Section 2.1 for the correct pattern.

### "KeyError" or "Column not found" in ETL

**Cause:** A source CSV column was renamed or the query returned different columns than expected.

**Fix:** Add `print(df.columns.tolist())` immediately after the read to inspect what arrived. Compare to what the ETL expects.

### Pipeline ran but `meta_pipeline_runs` shows `status = 'failed'`

**Fix:** `conn.execute("SELECT error_message FROM meta_pipeline_runs ORDER BY run_start DESC LIMIT 1").fetchone()` — this shows the Python exception that caused the failure.

### GitHub push fails with "Authentication failed"

**Cause:** GitHub PATs expire. The token in `git config` may be stale.

**Fix:** Generate a new PAT at github.com → Settings → Developer settings → Personal access tokens → Fine-grained tokens. Then:

```bash
echo "https://x-access-token:YOUR_NEW_TOKEN@github.com" > /tmp/gh_credentials
git config credential.helper "store --file /tmp/gh_credentials"
git push origin main
```

Note: Store credentials in `/tmp/` only — never in the repo directory.

---

## 9. Quick Reference

### Platform Stats (as of last full pipeline run)

| Metric | Value |
|---|---|
| Total warehouse objects | 41 (26 tables + 15 views) |
| Total fact rows | ~1,783,803 |
| Largest fact table | fact_daily_demand (574,509 rows) |
| Warehouses | 3 (NJ01, IL02, TX03) |
| SKUs | 2,000 |
| Operators (anonymised) | 60 |
| Customers | 500 |
| Date range | Jan 2022 – Dec 2023 (2 years) |

### Most Useful Queries for Orientation

```sql
-- What's in the warehouse?
SHOW TABLES;

-- Total rows in every fact table
SELECT 'fact_daily_demand' AS tbl, COUNT(*) AS rows FROM fact_daily_demand
UNION ALL SELECT 'fact_wms_tasks',        COUNT(*) FROM fact_wms_tasks
UNION ALL SELECT 'fact_wms_daily_kpis',   COUNT(*) FROM fact_wms_daily_kpis
UNION ALL SELECT 'fact_operator_daily',   COUNT(*) FROM fact_operator_daily
UNION ALL SELECT 'fact_orders',           COUNT(*) FROM fact_orders;

-- Recent pipeline runs
SELECT pipeline_name, status, rows_inserted, duration_seconds
FROM meta_pipeline_runs ORDER BY run_start DESC LIMIT 5;

-- Current month network KPIs
SELECT * FROM v_network_kpis_current_month;

-- Who needs coaching?
SELECT * FROM v_coaching_list ORDER BY avg_pick_accuracy_pct ASC LIMIT 10;
```

### Key Documents

| Document | Location | Purpose |
|---|---|---|
| Table inventory | `06-de-artifacts/lineage/table_inventory.md` | Every table: type, rows, refresh pattern |
| Data lineage | `06-de-artifacts/lineage/master_data_lineage.md` | Source-to-consumer lineage for 5 outputs |
| Platform architecture | `06-de-artifacts/architecture/platform_architecture.md` | Architecture diagram, design principles |
| Tech decisions | `06-de-artifacts/architecture/tech_stack_decisions.md` | Why DuckDB, why Python ETL, etc. |
| Coding standards | `06-de-artifacts/standards/de_coding_standards.md` | Naming, SQL patterns, Python standards |
| This guide | `06-de-artifacts/standards/onboarding_guide.md` | Setup, running pipelines, troubleshooting |
