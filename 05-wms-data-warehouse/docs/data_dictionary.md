# Data Dictionary — WMS Data Warehouse
## DHL Data Engineer Portfolio — Project 05

---

## fact_wms_daily_kpis

Pre-aggregated daily KPI table. One row per calendar date / warehouse / shift combination. Built by `aggregations/build_aggregations.py` from `fact_wms_tasks` as a full rebuild on each run.

| Column | Data Type | Business Definition | Source Mapping | Transformation | Example | Constraints |
|---|---|---|---|---|---|---|
| kpi_id | INTEGER | Surrogate primary key | Generated | ROW_NUMBER() OVER (ORDER BY date, warehouse, shift) | 1 | PK, NOT NULL |
| kpi_date | DATE | Calendar date of the KPI | fact_wms_tasks.task_date | GROUP BY | 2023-10-15 | NOT NULL |
| warehouse_id | VARCHAR(20) | DHL warehouse identifier | fact_wms_tasks.warehouse_id | GROUP BY | DHL-WH-NJ01 | NOT NULL, FK → dim_warehouse |
| shift | VARCHAR(20) | Work shift (Morning / Afternoon / Night) | fact_wms_tasks.shift | GROUP BY | Morning | NOT NULL |
| total_tasks | INTEGER | Total WMS tasks completed this date/warehouse/shift | fact_wms_tasks | COUNT(*) | 250 | NOT NULL DEFAULT 0 |
| total_picks | INTEGER | Pick tasks only | fact_wms_tasks where task_type='Pick' | COUNT(*) | 120 | NOT NULL DEFAULT 0 |
| total_putaways | INTEGER | Putaway tasks only | fact_wms_tasks where task_type='Putaway' | COUNT(*) | 80 | NOT NULL DEFAULT 0 |
| total_replenishments | INTEGER | Replenishment tasks only | fact_wms_tasks where task_type='Replenishment' | COUNT(*) | 30 | NOT NULL DEFAULT 0 |
| total_cycle_counts | INTEGER | Cycle count tasks only | fact_wms_tasks where task_type='Cycle Count' | COUNT(*) | 20 | NOT NULL DEFAULT 0 |
| pick_accuracy_pct | DECIMAL(6,3) | Percentage of pick tasks completed accurately | fact_wms_tasks.accuracy_flag where task_type='Pick' | 100.0 * accurate_picks / total_picks | 99.150 | NULL if no picks |
| putaway_accuracy_pct | DECIMAL(6,3) | Percentage of putaway tasks completed accurately | fact_wms_tasks.accuracy_flag where task_type='Putaway' | 100.0 * accurate_putaways / total_putaways | 98.750 | NULL if no putaways |
| cycle_count_accuracy_pct | DECIMAL(6,3) | Percentage of cycle counts completed accurately | fact_wms_tasks.accuracy_flag where task_type='Cycle Count' | 100.0 * accurate_counts / total_cycle_counts | 97.500 | NULL if no cycle counts |
| overall_accuracy_pct | DECIMAL(6,3) | Percentage of all tasks completed accurately | fact_wms_tasks.accuracy_flag | 100.0 * accurate_tasks / total_tasks | 99.020 | NULL if no tasks |
| avg_pick_duration_min | DECIMAL(8,3) | Average pick task duration in minutes | fact_wms_tasks.duration_min where task_type='Pick' | AVG(duration_min) | 3.450 | NULL if no picks |
| avg_putaway_duration_min | DECIMAL(8,3) | Average putaway task duration in minutes | fact_wms_tasks.duration_min where task_type='Putaway' | AVG(duration_min) | 4.120 | NULL if no putaways |
| picks_per_labour_hour | DECIMAL(8,3) | Productivity: picks completed per hour of pick labour | fact_wms_tasks | total_picks / (total_pick_minutes / 60) | 18.500 | NULL if zero pick minutes |
| total_errors | INTEGER | Total tasks with accuracy_flag = FALSE | fact_wms_tasks.accuracy_flag | COUNT(*) where NOT accuracy_flag | 12 | NOT NULL DEFAULT 0 |
| etl_loaded_at | TIMESTAMP | Timestamp when this row was written by the aggregation pipeline | Generated | CURRENT_TIMESTAMP | 2024-01-15 03:30:00 | NOT NULL |

---

## fact_wms_monthly_kpis

Monthly roll-up of daily KPIs. Built from `fact_wms_daily_kpis` (not directly from `fact_wms_tasks`) to ensure month totals are consistent with daily totals.

| Column | Data Type | Business Definition | Source Mapping | Transformation | Example | Constraints |
|---|---|---|---|---|---|---|
| monthly_kpi_id | INTEGER | Surrogate primary key | Generated | ROW_NUMBER() | 1 | PK, NOT NULL |
| kpi_year | INTEGER | Calendar year | fact_wms_daily_kpis.kpi_date | EXTRACT(YEAR) | 2023 | NOT NULL |
| kpi_month | INTEGER | Calendar month (1–12) | fact_wms_daily_kpis.kpi_date | EXTRACT(MONTH) | 10 | NOT NULL |
| warehouse_id | VARCHAR(20) | DHL warehouse identifier | fact_wms_daily_kpis.warehouse_id | GROUP BY | DHL-WH-NJ01 | NOT NULL |
| total_tasks | INTEGER | Total tasks for the month | fact_wms_daily_kpis.total_tasks | SUM | 7,500 | NOT NULL DEFAULT 0 |
| total_picks | INTEGER | Total picks for the month | fact_wms_daily_kpis.total_picks | SUM | 3,600 | NOT NULL DEFAULT 0 |
| total_putaways | INTEGER | Total putaways for the month | fact_wms_daily_kpis.total_putaways | SUM | 2,400 | NOT NULL DEFAULT 0 |
| total_replenishments | INTEGER | Total replenishments for the month | fact_wms_daily_kpis.total_replenishments | SUM | 900 | NOT NULL DEFAULT 0 |
| total_cycle_counts | INTEGER | Total cycle counts for the month | fact_wms_daily_kpis.total_cycle_counts | SUM | 600 | NOT NULL DEFAULT 0 |
| pick_accuracy_pct | DECIMAL(6,3) | Pick-volume-weighted accuracy for the month | fact_wms_daily_kpis | SUM(picks * accuracy) / SUM(picks) | 99.230 | NULL if no picks |
| putaway_accuracy_pct | DECIMAL(6,3) | Putaway-volume-weighted accuracy for the month | fact_wms_daily_kpis | SUM(putaways * accuracy) / SUM(putaways) | 98.800 | NULL if no putaways |
| cycle_count_accuracy_pct | DECIMAL(6,3) | Cycle-count-volume-weighted accuracy for the month | fact_wms_daily_kpis | SUM(counts * accuracy) / SUM(counts) | 97.600 | NULL if no counts |
| overall_accuracy_pct | DECIMAL(6,3) | Task-volume-weighted overall accuracy for the month | fact_wms_daily_kpis | SUM(tasks * accuracy) / SUM(tasks) | 99.100 | NULL if no tasks |
| avg_pick_duration_min | DECIMAL(8,3) | Average daily pick duration averaged across days | fact_wms_daily_kpis.avg_pick_duration_min | AVG(avg_pick_duration_min) | 3.420 | NULL if no picks |
| avg_putaway_duration_min | DECIMAL(8,3) | Average daily putaway duration averaged across days | fact_wms_daily_kpis.avg_putaway_duration_min | AVG(avg_putaway_duration_min) | 4.080 | NULL if no putaways |
| picks_per_labour_hour | DECIMAL(8,3) | Monthly productivity rate | fact_wms_daily_kpis | SUM(picks) / (SUM(pick_minutes)/60) | 18.200 | NULL if no pick minutes |
| total_errors | INTEGER | Total error events for the month | fact_wms_daily_kpis.total_errors | SUM | 350 | NOT NULL DEFAULT 0 |
| working_days | INTEGER | Number of distinct calendar dates with activity | fact_wms_daily_kpis.kpi_date | COUNT(DISTINCT kpi_date) | 22 | NULL |
| etl_loaded_at | TIMESTAMP | Aggregation pipeline write timestamp | Generated | CURRENT_TIMESTAMP | 2024-01-15 03:32:00 | NOT NULL |

---

## fact_operator_daily

Daily operator scorecard. One row per operator / warehouse / date / shift. Built by `aggregations/build_aggregations.py`.

| Column | Data Type | Business Definition | Source Mapping | Transformation | Example | Constraints |
|---|---|---|---|---|---|---|
| operator_daily_id | INTEGER | Surrogate primary key | Generated | Sequential index | 1 | PK, NOT NULL |
| operator_id | INTEGER | Operator surrogate key (references dim_operator.operator_surrogate_id) | fact_wms_tasks.operator_surrogate_id | GROUP BY | 5 | NOT NULL |
| warehouse_id | VARCHAR(20) | Warehouse where the operator worked | fact_wms_tasks.warehouse_id | GROUP BY | DHL-WH-NJ01 | NOT NULL |
| task_date | DATE | Work date | fact_wms_tasks.task_date | GROUP BY | 2023-10-15 | NOT NULL |
| shift | VARCHAR(20) | Work shift | fact_wms_tasks.shift | GROUP BY | Morning | NOT NULL |
| tasks_completed | INTEGER | All task types completed in this record | fact_wms_tasks | COUNT(*) | 45 | NOT NULL DEFAULT 0 |
| picks_completed | INTEGER | Pick tasks only | fact_wms_tasks where task_type='Pick' | COUNT(*) | 28 | NOT NULL DEFAULT 0 |
| pick_accuracy_pct | DECIMAL(6,3) | Pick accuracy for this operator on this shift | fact_wms_tasks | 100 * accurate_picks / total_picks | 99.800 | NULL if no picks |
| avg_duration_min | DECIMAL(8,3) | Average task duration across all task types | fact_wms_tasks.duration_min | AVG(duration_min) | 3.750 | NULL |
| error_count | INTEGER | Number of inaccurate tasks (accuracy_flag = FALSE) | fact_wms_tasks | COUNT(*) where NOT accuracy_flag | 0 | NOT NULL DEFAULT 0 |
| top_error_code | VARCHAR(50) | Most frequent error code on inaccurate tasks for this record | fact_wms_tasks.error_code | MODE() / ROW_NUMBER by count | WRONG_LOCATION | NULL if no errors |
| performance_flag | VARCHAR(20) | Coaching classification | Derived | IF accuracy>=99.8→high_performer; IF<98.5→needs_coaching; ELSE standard | high_performer | NULL |
| etl_loaded_at | TIMESTAMP | Aggregation pipeline write timestamp | Generated | CURRENT_TIMESTAMP | 2024-01-15 03:31:00 | NOT NULL |

---

## fact_error_log

Detailed error event log. One row per WMS task where accuracy_flag = FALSE. Populated by `etl/wms_warehouse_etl.py`.

| Column | Data Type | Business Definition | Source Mapping | Transformation | Example | Constraints |
|---|---|---|---|---|---|---|
| error_id | INTEGER | Surrogate primary key | Generated | Sequential, max+1 at load time | 1 | PK, NOT NULL |
| task_id | VARCHAR(30) | Source WMS task identifier | fact_wms_tasks.task_id | Direct copy | TASK-0012345 | NOT NULL |
| sku_id | VARCHAR(20) | SKU involved in the error | fact_wms_tasks.sku_id | Direct copy | PHM-000124 | NULL (if task has no SKU) |
| warehouse_id | VARCHAR(20) | Warehouse where error occurred | fact_wms_tasks.warehouse_id | Direct copy | DHL-WH-NJ01 | NOT NULL |
| operator_id | INTEGER | Operator surrogate key | fact_wms_tasks.operator_surrogate_id | Direct copy | 5 | NULL if not assigned |
| task_date | DATE | Date of the error | fact_wms_tasks.task_date | Direct copy | 2023-10-15 | NOT NULL |
| shift | VARCHAR(20) | Shift when error occurred | fact_wms_tasks.shift | Direct copy | Afternoon | NULL |
| task_type | VARCHAR(30) | Type of task that had the error | fact_wms_tasks.task_type | Direct copy | Pick | NULL |
| error_code | VARCHAR(50) | Error classification code from WMS | fact_wms_tasks.error_code | Direct copy | WRONG_LOCATION | NULL |
| zone | VARCHAR(30) | Warehouse zone (not available in source — NULL until location data is enriched) | dim_location (not joined in v1) | Future enrichment | Pick_Face | NULL |
| category | VARCHAR(20) | SKU category derived from sku_id prefix | sku_id | sku_id.split('-')[0] | PHM | NULL if sku_id is NULL |
| error_context | VARCHAR(200) | Human-readable description of the error event | Generated | f-string combining task_type, warehouse, sku, error_code, shift | Pick error in DHL-WH-NJ01 SKU: PHM-000124 | NULL |
| etl_loaded_at | TIMESTAMP | ETL load timestamp | Generated | CURRENT_TIMESTAMP | 2024-01-15 03:30:05 | NOT NULL |

---

## fact_inventory_accuracy

Monthly inventory accuracy snapshots. One row per snapshot month / warehouse / SKU category. Populated by `etl/wms_warehouse_etl.py` from `fact_inventory_snapshot`.

| Column | Data Type | Business Definition | Source Mapping | Transformation | Example | Constraints |
|---|---|---|---|---|---|---|
| accuracy_id | INTEGER | Surrogate primary key | Generated | Sequential | 1 | PK, NOT NULL |
| snapshot_date | DATE | First day of the snapshot month | fact_inventory_snapshot.snapshot_date | DATE_TRUNC('month') | 2023-10-01 | NOT NULL |
| warehouse_id | VARCHAR(20) | Warehouse identifier | fact_inventory_snapshot.warehouse_id | GROUP BY | DHL-WH-NJ01 | NOT NULL |
| category | VARCHAR(20) | SKU category (e.g., PHM, AUT, FMC) | Derived from sku_id | sku_id.split('-')[0] | PHM | NOT NULL |
| total_skus_counted | INTEGER | Total SKU lines counted in this snapshot month for this warehouse/category | fact_inventory_snapshot | COUNT(sku_id) | 125 | NOT NULL DEFAULT 0 |
| accurate_count | INTEGER | SKUs with on_hand_qty > 0 (heuristic for accurate records on synthetic data) | fact_inventory_snapshot | SUM(on_hand_qty > 0) | 123 | NOT NULL DEFAULT 0 |
| discrepancy_count | INTEGER | SKUs with discrepancies (total_skus_counted - accurate_count) | Derived | total_skus_counted - accurate_count | 2 | NOT NULL DEFAULT 0 |
| accuracy_pct | DECIMAL(6,3) | Percentage of SKU lines recorded accurately | Derived | accurate_count / total_skus_counted * 100 | 98.400 | NULL |
| total_on_hand_value | DECIMAL(14,2) | Total estimated on-hand inventory value (quantity * unit_cost) | fact_inventory_snapshot | SUM(qty * cost) | 45,230.50 | NULL |
| discrepancy_value | DECIMAL(14,2) | Estimated value at risk from discrepancies | Derived | total_on_hand_value * (1 - accuracy_pct/100) | 724.00 | NULL |
| etl_loaded_at | TIMESTAMP | ETL load timestamp | Generated | CURRENT_TIMESTAMP | 2024-01-15 03:30:10 | NOT NULL |

---

## meta_pipeline_runs

Audit log of every pipeline execution. One row inserted at run start (status='running'), updated to 'success' or 'failed' at completion.

| Column | Data Type | Business Definition | Source Mapping | Transformation | Example | Constraints |
|---|---|---|---|---|---|---|
| run_id | INTEGER | Surrogate primary key | Generated | Auto-increment (MAX+1 via sequence) | 42 | PK, NOT NULL |
| pipeline_name | VARCHAR(100) | Human-readable pipeline identifier | Hardcoded in each pipeline | Direct | wms_warehouse_etl | NOT NULL |
| run_start | TIMESTAMP | UTC timestamp when the pipeline started | datetime.utcnow() | Direct | 2024-01-15 03:30:00.123 | NOT NULL |
| run_end | TIMESTAMP | UTC timestamp when the pipeline finished | datetime.utcnow() | Updated on completion | 2024-01-15 03:32:45.456 | NULL while running |
| duration_seconds | DECIMAL(10,3) | Total execution time in seconds | Computed | (run_end - run_start).total_seconds() | 165.333 | NULL while running |
| status | VARCHAR(20) | Execution outcome | Hardcoded string | running → success or failed | success | NOT NULL DEFAULT 'running' |
| rows_processed | INTEGER | Total rows read from source by this run | Pipeline code | Accumulates across steps | 219000 | DEFAULT 0 |
| rows_inserted | INTEGER | New rows written to target tables by this run | Pipeline code | Accumulates across steps | 21943 | DEFAULT 0 |
| rows_updated | INTEGER | Existing rows modified by this run | Pipeline code | Accumulates across steps | 0 | DEFAULT 0 |
| error_message | VARCHAR(1000) | Exception message if status='failed', otherwise NULL | Python exception handler | str(exception)[:1000] | NULL | NULL |
| run_by | VARCHAR(50) | Identifies who or what triggered the run | Default value | 'pipeline' (would be username in production) | pipeline | DEFAULT 'pipeline' |
