# Data Engineering Coding Standards
## DHL Data Engineering Platform — Project 06 Reference Artifact

These standards govern all code written for the DHL data engineering platform. A new pipeline that follows these standards will integrate cleanly with existing infrastructure, pass code review without discussion, and be maintainable by any team member who has read this document.

---

## 1. Naming Conventions

### 1.1 Database Objects

**Tables**

| Prefix | Meaning | Examples |
|---|---|---|
| `dim_` | Dimension table | `dim_sku`, `dim_warehouse`, `dim_operator` |
| `fact_` | Fact table (raw or pre-aggregated) | `fact_wms_tasks`, `fact_wms_daily_kpis` |
| `meta_` | Platform metadata / audit | `meta_pipeline_runs` |
| `v_` | View (serving layer) | `v_warehouse_comparison`, `v_coaching_list` |

All table names are `snake_case`. No camelCase, no UPPER_CASE. Maximum 50 characters.

**Columns**

- Surrogate primary keys: `<table_short_name>_key` (e.g. `sku_key`, `warehouse_key`, `date_key`)
- Natural / business keys: `<entity>_id` (e.g. `sku_id`, `warehouse_id`, `operator_id`)
- Timestamps: `_at` suffix (e.g. `created_at`, `etl_loaded_at`, `run_start`, `run_end`)
- Dates (DATE type): `_date` suffix (e.g. `task_date`, `snapshot_date`, `kpi_date`)
- Boolean flags: `is_` prefix (e.g. `is_current`, `is_active`, `is_accurate`)
- Percentages: `_pct` suffix; stored as 0–100 (not 0–1) (e.g. `pick_accuracy_pct = 99.82`)
- Counts: `_count` suffix (e.g. `error_count`, `total_tasks`)

**Required audit columns (every table, no exceptions)**

```sql
etl_loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
```

For SCD2 tables, additionally:

```sql
valid_from   TIMESTAMP NOT NULL,
valid_to     TIMESTAMP,           -- NULL = current row
is_current   BOOLEAN NOT NULL DEFAULT TRUE
```

### 1.2 Python

- Modules: `snake_case.py` (e.g. `wms_warehouse_etl.py`, `build_aggregations.py`)
- Functions: `snake_case` (e.g. `load_error_log`, `build_daily_kpis`)
- Classes: `PascalCase` (rarely used; prefer module-level functions)
- Constants: `UPPER_SNAKE_CASE` (e.g. `DB_PATH`, `SHARED_DATA`, `N_RUNS`)
- Logger variable: always named `logger`; always obtained from `get_logger(name)` at top of function chain

### 1.3 Files and Directories

```
NN-<project-slug>/
  schema/
    <subject>_schema.sql          ← DDL
    setup_schema.py               ← Schema runner
  etl/
    <subject>_etl.py              ← Main ETL script
  pipeline/
    <subject>_pipeline.py         ← Domain-specific pipeline steps
  aggregations/                   ← (Project 05 pattern)
    build_aggregations.py
    validate_aggregations.py
  quality/
    dq_framework.py               ← DQ checks
    anomaly_detection.py          ← Statistical anomaly detection
  serving/
    <subject>_serving_layer.py    ← View creation + CSV export
  outputs/                        ← CSV exports, reports (gitignored if large)
  docs/
    schema_design.md
    data_dictionary.md
    pipeline_runbook.md
```

---

## 2. SQL Standards

### 2.1 DuckDB-Specific Patterns

**Integer primary keys:** DuckDB does not auto-increment INTEGER PKs. Always compute the next ID explicitly:

```sql
SELECT COALESCE(MAX(run_id), 0) + 1 FROM meta_pipeline_runs
```

Never rely on `SERIAL` or `AUTOINCREMENT` — they are not supported in the version used by this platform.

**Inline comments in DDL:** Strip `--` inline comments before parsing SQL in Python, because the semicolon-split pattern treats them as statement boundaries:

```python
import re
sql_clean = re.sub(r"--[^\n]*", "", sql_raw)
statements = [s.strip() for s in sql_clean.split(";") if s.strip()]
```

**CROSS JOIN for correlated filters:** When a CTE uses a correlated reference table (e.g. `ref AS (SELECT MAX(date) ...)`), use `CROSS JOIN` in the inner CTE rather than comma-join followed by `LEFT JOIN`:

```sql
-- WRONG: DuckDB throws "Non-inner join on correlated columns not supported"
FROM fact_operator_daily od, ref LEFT JOIN dim_operator o ON ...

-- CORRECT: explicit CROSS JOIN, then LEFT JOIN in outer SELECT
SELECT ... FROM (
    SELECT od.* FROM fact_operator_daily od CROSS JOIN ref WHERE ...
) monthly LEFT JOIN dim_operator o ON ...
```

**Volume-weighted accuracy rollup:** Never average percentages directly across groups of different sizes. Always weight by volume:

```sql
ROUND(
    SUM(total_picks * pick_accuracy_pct / 100.0)
    / NULLIF(SUM(total_picks), 0) * 100, 3
) AS pick_accuracy_pct
```

### 2.2 General SQL

- Use `CREATE OR REPLACE VIEW` for all serving layer views
- Use `COALESCE(expr, 0)` before division; use `NULLIF(denominator, 0)` to avoid division-by-zero
- All `INSERT` statements must name columns explicitly — never `INSERT INTO table VALUES (...)` without column list
- Use `ROUND(expr, 3)` consistently for percentages; `ROUND(expr, 2)` for currency values
- Use `COUNT(DISTINCT col)` rather than subqueries for cardinality checks

---

## 3. Python ETL Standards

### 3.1 Logger Pattern

Every ETL module must define and use a named logger:

```python
import logging

def get_logger(name: str = "module_name") -> logging.Logger:
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
```

Log levels:
- `logger.info(...)` — normal pipeline progress (start/end of each step, row counts)
- `logger.warning(...)` — unexpected but recoverable situations (no data found, skipping step)
- `logger.error(...)` — errors being caught and handled
- Do not use `print()` for pipeline output — always use the logger

### 3.2 Path Constants

Every ETL script must define its paths at module level:

```python
from pathlib import Path

BASE_DIR    = Path(__file__).resolve().parent.parent   # project root
DB_PATH     = Path("/tmp/dhl_pN.duckdb")               # working copy
SHARED_DATA = BASE_DIR.parent / "shared" / "data" / "dhl-synthetic"
OUTPUT_DIR  = BASE_DIR / "outputs"
```

**Database working copy rule:** Never connect directly to `dhl_warehouse.duckdb` on the mounted workspace. Always `cp` to `/tmp/dhl_pN.duckdb` before pipeline work, and `cp` back on completion. This avoids WAL lock conflicts on the mount:

```bash
cp /sessions/.../dhl_warehouse.duckdb /tmp/dhl_pN.duckdb
python pipeline.py
cp /tmp/dhl_pN.duckdb /sessions/.../dhl_warehouse.duckdb
```

### 3.3 Incremental Load Pattern

Every fact table ETL must implement an incremental load. The preferred pattern is set-difference on the natural key:

```python
# Get keys already in the warehouse
existing_ids = {r[0] for r in conn.execute(
    "SELECT task_id FROM fact_wms_tasks"
).fetchall()}

# Filter source data to new rows only
new_rows = source_df[~source_df["task_id"].isin(existing_ids)].copy()
```

For time-partitioned tables, use a watermark from `meta_pipeline_runs`:

```python
watermark = conn.execute("""
    SELECT MAX(run_end) FROM meta_pipeline_runs
    WHERE pipeline_name = ? AND status = 'success'
""", [pipeline_name]).fetchone()[0]

if watermark:
    source_df = source_df[source_df["created_at"] > watermark]
```

### 3.4 meta_pipeline_runs Pattern

Every pipeline that writes to the warehouse must record its run in `meta_pipeline_runs`:

```python
def start_run(conn, pipeline_name: str) -> int:
    run_start = datetime.utcnow()
    next_id = conn.execute(
        "SELECT COALESCE(MAX(run_id), 0) + 1 FROM meta_pipeline_runs"
    ).fetchone()[0]
    conn.execute("""
        INSERT INTO meta_pipeline_runs
            (run_id, pipeline_name, run_start, status, rows_processed, rows_inserted, rows_updated)
        VALUES (?, ?, ?, 'running', 0, 0, 0)
    """, [next_id, pipeline_name, run_start])
    return next_id


def finish_run(conn, run_id, t0, rows_processed, rows_inserted, rows_updated,
               status="success", error_message=None):
    run_end = datetime.utcnow()
    conn.execute("""
        UPDATE meta_pipeline_runs
        SET run_end=?, duration_seconds=?, status=?,
            rows_processed=?, rows_inserted=?, rows_updated=?, error_message=?
        WHERE run_id=?
    """, [run_end, (run_end - t0).total_seconds(), status,
          rows_processed, rows_inserted, rows_updated, error_message, run_id])
```

The `run_id` from `start_run` must be passed to `finish_run` in both success and failure paths.

### 3.5 Error Handling

Every ETL main function must wrap all pipeline steps in a try/except that records failure to `meta_pipeline_runs` before re-raising:

```python
run_id = start_run(conn, "pipeline_name")
try:
    # ... pipeline steps ...
    finish_run(conn, run_id, t0, processed, inserted, 0, "success")
except Exception as e:
    finish_run(conn, run_id, t0, 0, 0, 0, "failed", str(e))
    conn.close()
    raise
```

### 3.6 Chunked Inserts

For fact tables with more than 50,000 rows, insert in chunks to avoid memory issues:

```python
CHUNK_SIZE = 20_000

for start in range(0, len(df), CHUNK_SIZE):
    chunk = df.iloc[start : start + CHUNK_SIZE]
    conn.register("_staging", chunk)
    conn.execute(f"INSERT INTO fact_table ({col_list}) SELECT {col_list} FROM _staging")
    conn.unregister("_staging")
    logger.info(f"    Inserted rows {start:,}–{min(start+CHUNK_SIZE, len(df)):,}")
```

### 3.7 Operator Anonymisation

All pipelines that handle operator IDs must anonymise before storage:

```python
import hashlib

def anonymise_operator(raw_id: str) -> str:
    """SHA-256 truncated to 8 hex chars, prefixed with 'OP-'. Deterministic, non-reversible."""
    return "OP-" + hashlib.sha256(raw_id.encode()).hexdigest()[:8].upper()
```

Never store raw operator IDs. Never store the mapping table between raw and anonymised IDs.

---

## 4. Data Quality Requirements

### 4.1 Mandatory Checks per Pipeline

Every pipeline that writes a new fact table must implement at minimum:

| Tier | Check | Implementation |
|---|---|---|
| CRITICAL | No null primary keys | `COUNT(*) WHERE pk IS NULL = 0` |
| CRITICAL | No negative quantity columns | `MIN(quantity_col) >= 0` |
| HIGH | Referential integrity to dim_warehouse | All `warehouse_id` values in `dim_warehouse` |
| HIGH | Accuracy rates in valid range | All accuracy pct values between 0 and 100 |
| MEDIUM | Duration or value bounds | Column-specific; document expected range |

### 4.2 DQ Report Format

All DQ checks must export results to `outputs/dq_report.csv` with columns:

```
check_name, severity, status, metric_value, threshold, message, checked_at
```

Status values: `PASS`, `FAIL`, `WARN`, `INFO`

### 4.3 CRITICAL Failure Behaviour

If any CRITICAL check fails, the pipeline must raise an exception after writing the failure to `meta_pipeline_runs`. The pipeline must not proceed to subsequent steps after a CRITICAL failure.

---

## 5. Documentation Requirements

Every project folder must contain, under `docs/`:

**schema_design.md** — Entity-relationship explanation, table grain definitions, design decisions (why this table exists, what it replaces, what questions it answers). Not just a list of columns.

**data_dictionary.md** — Every column in every table: name, data type, nullable, source field, business definition, example value. If the column is derived, the derivation formula. Format: markdown table per table.

**pipeline_runbook.md** — How to run the pipeline end-to-end; prerequisites; what each script does; what to look for in the logs; known issues; troubleshooting section with error → cause → fix triples.

---

## 6. Git Commit Standards

### Commit Message Format

```
DE Project N: <Subject Area> — <one-line summary of what was built>
```

Examples:
```
DE Project 4: Warehouse Operations — WMS ETL, slotting pipeline, co-occurrence analysis complete.
DE Project 5: WMS Data Warehouse — 6 tables, ETL, aggregations, DQ framework, serving layer complete.
DE Project 6: Data Engineering Artifacts — platform architecture, data lineage, coding standards, onboarding guide, README complete. DE portfolio finished.
```

### What Belongs in a Commit

- Each project is one commit
- Do not commit intermediate or broken states
- Do not commit credentials, tokens, or database files
- Do not commit `.pyc`, `__pycache__`, or OS artifacts (`.DS_Store`)

### Security

Never commit credentials. The `.gitignore` at repo root excludes `*.duckdb` and `*.env`. When a GitHub PAT is used for push authentication, pass it via `git config credential.helper "store --file /tmp/gh_credentials"` and point the credentials file to `/tmp/` — never to the repo directory.
