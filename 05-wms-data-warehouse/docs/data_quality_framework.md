# Data Quality Framework
## DHL Data Engineer Portfolio — Project 05

---

## Overview

The DQ framework (`quality/dq_framework.py`) provides a structured approach to validating the WMS data warehouse. Rather than running ad-hoc quality checks, the framework organises all checks into four severity tiers, each with a defined operational response. This allows on-call engineers to immediately understand the urgency of a quality failure without reading the full check details.

---

## Severity Tiers

### CRITICAL — Pipeline should stop if any of these fail

CRITICAL checks catch data corruption or fundamental integrity violations that would make downstream reporting meaningless. A CRITICAL failure means the pipeline should not proceed to the aggregation or serving layer, and on-call should be paged immediately.

**Operational response:** Halt all downstream jobs. Investigate source data before re-running. Do not acknowledge the alert as resolved until the root cause is identified and fixed.

### HIGH — Log and alert

HIGH checks catch significant data quality issues that affect accuracy of reporting but do not necessarily corrupt the entire dataset. For example, an error code appearing on accurate tasks is a data consistency violation — it means some tasks were misclassified — but it doesn't invalidate all KPIs.

**Operational response:** Send an alert to the data engineering Slack channel. Do not halt the pipeline (HIGH failures are logged and noted in the quality report), but investigate within one business day.

### MEDIUM — Log for review

MEDIUM checks catch patterns that fall outside expected operational norms but may have valid explanations. Duration outliers, for instance, may be genuine long-running tasks or they may indicate a clock error in the WMS system.

**Operational response:** Log to the DQ report and include in the weekly data quality review. No immediate action required unless the failure count increases significantly week-over-week.

### LOW — Informational

LOW checks are diagnostic signals, not failures. An operator showing zero tasks on a given day may simply be on leave. SKUs with no picks in 30 days may be seasonal items. LOW results should be read by the analytics team as context for their reports, not by engineering as a problem to fix.

**Operational response:** No action required. Review monthly to identify trends.

---

## Implemented Checks

### CRITICAL Checks

**no_null_primary_keys**
Every fact table has a primary key column that must never be NULL. A NULL primary key means the row cannot be uniquely identified, which breaks any downstream JOIN or deduplication logic.
- Tables checked: fact_wms_tasks (task_id), fact_wms_daily_kpis (kpi_id), fact_operator_daily (operator_daily_id), fact_error_log (error_id)
- Business rationale: Without a valid PK, the row is invisible to any system that uses PKs for update/delete operations. It also prevents reliable idempotency in incremental loads.

**no_negative_quantities**
Pick and putaway task quantities must be zero or positive. A negative quantity in the WMS system would indicate a data entry error or a reversed transaction that was not properly handled.
- Table checked: fact_wms_tasks (quantity column)
- Business rationale: Negative quantities would corrupt any inventory-level calculation derived from WMS task data.

**no_future_dates**
task_date must not be in the future. Future dates in operational WMS data indicate clock synchronisation errors in the scanner devices or ETL timestamp handling bugs.
- Table checked: fact_wms_tasks (task_date)
- Business rationale: A future-dated task would appear in "today's" KPI calculations for multiple days until the date is reached, causing inflated task counts.

---

### HIGH Checks

**accuracy_rates_valid**
pick_accuracy_pct in fact_wms_daily_kpis must be between 0 and 100. Values outside this range indicate a calculation error in the aggregation pipeline.
- Table checked: fact_wms_daily_kpis
- Business rationale: An accuracy rate of 105% or -3% is a calculation artifact that would be displayed to warehouse managers and undermine trust in the dashboard.

**error_codes_only_on_errors**
error_code should only be populated when accuracy_flag = FALSE. If a task is marked accurate but also has an error code, either the accuracy flag is wrong or the error code is spurious — both indicate a data quality issue upstream in the WMS system.
- Table checked: fact_wms_tasks
- Business rationale: The error analysis views (v_error_patterns, v_coaching_list) are based on tasks where accuracy_flag = FALSE and error_code is populated. Polluted error_code data would inflate error counts and misattribute errors to operators.

**warehouse_referential_integrity**
All warehouse_ids in fact_wms_tasks must exist in dim_warehouse. Orphaned warehouse IDs would indicate a warehouse was added to the operational system but not yet registered in the master dimension, or that a warehouse_id was renamed without updating all references.
- Tables checked: fact_wms_tasks against dim_warehouse
- Business rationale: Any KPI grouped by warehouse_id would silently drop tasks with unrecognised IDs, producing understated task counts.

---

### MEDIUM Checks

**task_duration_bounds**
Task durations should be between 1 and 120 minutes. Durations below 1 minute (including zero) suggest scanner errors where a task was marked complete immediately. Durations above 120 minutes are statistically implausible for WMS operations and indicate a system event (shift end, lunch break logged within a task) rather than a real task.
- Table checked: fact_wms_tasks (duration_min)
- Thresholds: 1 min (lower), 120 min (upper)
- Business rationale: picks_per_labour_hour calculations depend on accurate duration data. Outlier durations distort the metric but do not make it meaningless — hence MEDIUM rather than HIGH.

**picks_per_labour_hour_bounds**
Picks per labour hour in fact_wms_daily_kpis should be between 1 and 80. Values below 1 suggest very slow operations or duration data issues. Values above 80 are physically implausible for manual pick operations and suggest duration values close to zero.
- Table checked: fact_wms_daily_kpis (picks_per_labour_hour)
- Business rationale: This metric is used in operations benchmarking. Values outside the plausible range should be flagged for review before being presented in management reports.

---

### LOW Checks

**operators_with_zero_tasks**
Reports the count of operators in dim_operator who have no tasks in the last 30 days. This is informational — operators may be on leave, on a different shift pattern, or recently onboarded.
- Business use: Useful for the HR team to identify potential scheduling gaps.

**skus_with_no_picks**
Reports the count of active SKUs (dim_sku.active_flag = TRUE) with no pick tasks in the last 30 days. These may be genuinely slow-moving items, recently added SKUs not yet in demand, or items with demand fulfilled through a different warehouse not captured in this dataset.
- Business use: The procurement team can use this list to review whether slow-moving active SKUs should be reclassified.

---

## How to Add New Checks

Each check follows the same interface:

```python
def check_my_new_check(conn, logger) -> dict:
    total = conn.execute("SELECT COUNT(*) FROM my_table").fetchone()[0]
    bad   = conn.execute("SELECT COUNT(*) FROM my_table WHERE <bad_condition>").fetchone()[0]
    samples = []
    if bad > 0:
        rows = conn.execute("SELECT id, col FROM my_table WHERE <bad_condition> LIMIT 3").fetchall()
        samples = [f"{r[0]}|{r[1]}" for r in rows]
    status = "PASS" if bad == 0 else "WARN"   # or "FAIL" for HIGH/CRITICAL
    detail = f"Bad rows: {bad:,} of {total:,}"
    _log(logger, "MEDIUM", status, "my_new_check", detail)
    return _result("my_new_check", "MEDIUM", status, total, bad, "; ".join(samples))
```

Then add the function call to the appropriate severity block in `run_dq_framework()`. The report DataFrame will automatically include the new check, and the CSV export will contain its results.

Choose the severity based on: if a failure would cause incorrect reporting that stakeholders would trust without questioning, it is at minimum HIGH. If a failure would cause all downstream calculations to be invalid, it is CRITICAL.
