# Pipeline Runbook — DHL SKU Segmentation Pipeline
**Project:** 01 — SKU Segmentation Pipeline  
**Author:** Vinyl Kiran Anipe (Data Engineer)  
**Version:** 1.0 · 2024

---

## 1. Prerequisites and Environment Setup

### Python Version
Python 3.9 or higher.

### Required Packages
```bash
pip install duckdb pandas numpy
```

### Directory Structure
The pipeline assumes the following structure relative to the project root:
```
01-sku-segmentation-pipeline/
├── etl/
│   ├── extract.py
│   ├── transform.py
│   ├── load.py
│   └── pipeline.py
├── quality/
│   ├── validation.py
│   └── data_profiling.py
├── schema/
│   └── create_schema.sql
├── outputs/           # Created automatically by the pipeline
│   ├── dhl_warehouse.duckdb
│   ├── validation_report.csv
│   ├── data_profile.csv
│   └── logs/
└── docs/
```

### Source Data
Source CSVs must exist at:
```
../../shared/data/dhl-synthetic/
```
(Two levels up from the project root — in the shared data directory.)

Required files:
- `sku_master.csv`
- `daily_demand.csv`
- `inventory_snapshot.csv`
- `suppliers.csv`
- `warehouse_locations.csv`

---

## 2. Running the Full Pipeline

### Single command (recommended)
```bash
cd 01-sku-segmentation-pipeline/etl/
python pipeline.py
```

This runs extract → transform → load in sequence. Logs are written to `outputs/logs/pipeline_YYYYMMDD_HHMMSS.log`.

### With a custom database path
```bash
python pipeline.py --db-path /path/to/custom/dhl_warehouse.duckdb
```

### Expected output
```
2024-01-15 10:00:00 [INFO] pipeline: DHL SKU SEGMENTATION PIPELINE — START
2024-01-15 10:00:00 [INFO] pipeline: >>> STAGE 1: EXTRACT
2024-01-15 10:00:02 [INFO] pipeline: Extract completed in 2.1s
2024-01-15 10:00:02 [INFO] pipeline: >>> STAGE 2: TRANSFORM
2024-01-15 10:00:05 [INFO] pipeline: Transform completed in 3.4s
2024-01-15 10:00:05 [INFO] pipeline: >>> STAGE 3: LOAD
2024-01-15 10:00:12 [INFO] pipeline: Load completed in 7.2s
...
2024-01-15 10:00:12 [INFO] pipeline: ✓ dim_date                              730 rows
2024-01-15 10:00:12 [INFO] pipeline: ✓ dim_warehouse                           3 rows
2024-01-15 10:00:12 [INFO] pipeline: ✓ dim_supplier                           80 rows
2024-01-15 10:00:12 [INFO] pipeline: ✓ dim_sku                             2,000 rows
2024-01-15 10:00:12 [INFO] pipeline: ✓ fact_daily_demand               574,509 rows
2024-01-15 10:00:12 [INFO] pipeline: ✓ fact_inventory_snapshot          19,200 rows
```

---

## 3. Running Individual Stages

Each stage script can be run independently for testing or debugging:

```bash
# Extract only
cd etl/
python extract.py

# Transform only (requires extract to have run)
python transform.py

# Load only (requires transform to have run)
python load.py
```

For quality checks (run after pipeline):
```bash
cd quality/
python validation.py
python data_profiling.py
```

---

## 4. Interpreting the Validation Report

The validation report is saved to `outputs/validation_report.csv` with these columns:

| Column | Description |
|---|---|
| `check_name` | Unique identifier for the check |
| `status` | `PASS`, `FAIL`, or `ERROR` |
| `rows_checked` | Total rows evaluated |
| `rows_failed` | Rows that failed the check |
| `failure_pct` | `rows_failed / rows_checked × 100` |
| `detail` | Human-readable description of what was checked |
| `timestamp` | UTC timestamp when the check ran |

A `PASS` result with `rows_failed = 0` means the check found no issues. A `FAIL` result means data quality issues were found — see the `rows_failed` and `detail` columns to understand the magnitude and nature of the problem.

**Expected results after a clean load:**

| Check Group | Expected Status |
|---|---|
| Null PK checks (6 checks) | All PASS |
| Referential integrity (6 checks) | All PASS |
| Duplicate PK checks (4 checks) | All PASS |
| Date range checks (2 checks) | All PASS |
| Revenue sanity (1 check) | PASS |
| Quantity sanity (4 checks) | All PASS |
| Stockout consistency (1 check) | PASS |
| Completeness (2 checks) | All PASS |

---

## 5. What to Do If a Validation Check Fails

### Null PK failures
**Symptom:** `null_pk_<table>` check FAIL.  
**Cause:** A surrogate key was not generated for some rows. Usually caused by a transform bug or a source data issue.  
**Action:** Run `python transform.py` in isolation and check the surrogate key generation logic in `transform.py`. Look for rows with NaN in the key column after the reset_index step.

### Referential integrity failures
**Symptom:** `ri_<fact>_<fk>` check FAIL with `rows_failed > 0`.  
**Cause:** A fact row has a FK value that does not exist in its dimension. Common causes: (a) a new warehouse ID appeared in the source data that is not in `dim_warehouse`, (b) a new date outside 2022–2023 appeared in the source data.  
**Action:** Query the fact table for the orphaned FKs:
```sql
SELECT DISTINCT f.warehouse_id
FROM fact_daily_demand f
LEFT JOIN dim_warehouse d ON f.warehouse_key = d.warehouse_key
WHERE d.warehouse_key IS NULL;
```
Then update `transform.py` to add the missing dimension member.

### Duplicate PK failures
**Symptom:** `dup_pk_<table>` check FAIL.  
**Cause:** Surrogate key collision — likely caused by running the pipeline twice without truncating, or a bug in the key generation.  
**Action:** Re-run `python pipeline.py` — the truncate step should resolve this. If it persists, inspect the key generation code in `transform.py`.

### Date range failures
**Symptom:** `date_range_<table>` check FAIL.  
**Cause:** Source data contains dates outside 2022-01-01 to 2023-12-31. These dates will not have a matching `date_key` in `dim_date`.  
**Action:** Extend `dim_date` in `transform.py` to cover the new date range, then re-run the pipeline.

### Completeness failures
**Symptom:** `completeness_<table>` check FAIL with non-zero delta.  
**Cause:** Source CSV row count does not match loaded row count. Could indicate (a) duplicate rows in the source CSV, (b) rows filtered out during transform, or (c) a load error.  
**Action:** Check the load log for any warnings about unmatched foreign keys — these rows may have been excluded. If the delta equals the number of FK warnings, this is expected and documented.

---

## 6. How to Add a New Source Table

1. Add the new CSV filename and its expected columns to `EXPECTED_FILES` in `extract.py`.
2. Add the extract call in `extract_all()` in `extract.py`.
3. Add a `build_dim_<name>()` or `build_fact_<name>()` function in `transform.py`.
4. Add the table to `LOAD_ORDER` in `load.py`.
5. Add the corresponding `CREATE TABLE` statement to `schema/create_schema.sql`.
6. Add the table to `TABLES` in `quality/data_profiling.py`.
7. Add any new validation checks to `quality/validation.py`.
8. Document the new table in `docs/data_dictionary.md`.

---

## 7. How to Handle a Source File with Schema Changes

If a source CSV column is renamed or a new column is added:

1. Update the `EXPECTED_FILES` column list in `extract.py` to reflect the new schema.
2. Update the column rename mapping in `transform.py` (the `rename_cols` function handles snake_case conversion automatically, but explicit column references in the transform functions will need updating).
3. If a required column was removed and has no replacement, decide whether to: (a) drop the column from the schema, (b) set it to NULL, or (c) derive it from other columns. Document the decision in `docs/data_dictionary.md`.
4. Update the `CREATE TABLE` DDL in `schema/create_schema.sql` if the schema change affects the warehouse tables.
5. Re-run `python pipeline.py` and then `python quality/validation.py` to confirm clean load.

---

*Pipeline Runbook v1.0 · Vinyl Kiran Anipe · DHL Data Engineer Portfolio · Project 01 · 2024*
