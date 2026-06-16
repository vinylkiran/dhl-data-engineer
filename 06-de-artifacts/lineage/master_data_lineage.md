# Master Data Lineage Map
## DHL Data Engineering Platform — Project 06 Reference Artifact

---

## Overview

This document traces every major analytical output back to its ultimate source across all five DE pipelines. It is the document a data governance or compliance team would request during an audit to answer: "Where does this number come from, and who touched it along the way?"

For each output artifact, the lineage is documented as: Source → ETL Script → Staging/Dimension/Fact Table → Transformation → Final Output → Downstream Consumer.

---

## Lineage 1: SKU Segments (sku_segmentation_output.csv)

**Business question answered:** Which SKUs are A/B/C class and which should be prioritised for replenishment and slotting?

```
SOURCE
  shared/data/dhl-synthetic/sku_master.csv
    Columns used: sku_id, category, unit_cost, lead_time_days, reorder_point
  shared/data/dhl-synthetic/demand_history.csv
    Columns used: sku_id, warehouse_id, date, demand_qty

     ↓ 01-sku-segmentation-pipeline/etl/extract.py
       Reads both CSVs into pandas DataFrames
       Validates required columns, date formats, non-negative demand

     ↓ 01-sku-segmentation-pipeline/etl/transform.py
       Joins demand to SKU master on sku_id
       Computes annual_demand_value = SUM(demand_qty) * unit_cost
       Applies ABC classification:
         A = top 70% of cumulative annual_demand_value
         B = 70–90%
         C = remaining 10%
       Derives: velocity_class (Fast/Medium/Slow) from demand_qty quartiles
       Derives: days_of_cover = inventory / avg_daily_demand

     ↓ 01-sku-segmentation-pipeline/etl/load.py
       INSERT INTO dim_sku (sku_key, sku_id, category, abc_class, velocity_class, ...)
       INSERT INTO dim_warehouse (warehouse_key, warehouse_id, ...)
       INSERT INTO fact_daily_demand (date_key, sku_key, warehouse_key, demand_qty, ...)
       INSERT INTO fact_inventory_snapshot (snapshot_key, date_key, sku_key, ...)

INTERMEDIATE TABLES
  dim_sku              (2,000 rows)  — SKU master with ABC class
  dim_warehouse        (3 rows)      — Warehouse dimension
  fact_daily_demand    (574,509 rows) — Raw demand history

     ↓ 01-sku-segmentation-pipeline/quality/validation.py
       Checks: no duplicate sku_id, ABC class covers 100% of SKUs,
               no negative demand, all warehouse_ids in dim_warehouse

FINAL OUTPUT
  01-sku-segmentation-pipeline/outputs/sku_segmentation_output.csv
    Columns: sku_id, abc_class, velocity_class, annual_demand_value,
             days_of_cover, reorder_recommendation

DOWNSTREAM CONSUMERS
  BA/DA Portfolio:  02-demand-forecasting/outputs/forecast_dashboard.html
                    Uses ABC class to colour-code forecast accuracy
  DE Project 02:    dim_sku.abc_class used to stratify model evaluation
                    (MAPE computed separately per A/B/C class)
  DE Project 04:    dim_sku.sku_id used in pick velocity classification
  Procurement team: sku_segmentation_output.csv for reorder planning
```

---

## Lineage 2: Demand Forecasts (forecast_output.csv / fact_forecast)

**Business question answered:** What is the expected demand for each SKU over the next 90 days, and what confidence interval should the supply planner use?

```
SOURCE
  shared/data/dhl-synthetic/demand_history.csv
    Columns: sku_id, warehouse_id, date, demand_qty
  [Already loaded into fact_daily_demand by Project 01]

     ↓ 02-demand-forecasting-pipeline/etl/incremental_load.py
       Reads fact_daily_demand filtered to dates > last_processed_date watermark
       Inserts new demand rows only (incremental by date)

     ↓ 02-demand-forecasting-pipeline/features/feature_engineering.py
       SELECT from fact_daily_demand
       Computes per (sku_id, warehouse_id, date):
         lag_7, lag_14, lag_28 (lagged demand values)
         rolling_mean_7d, rolling_mean_14d, rolling_std_14d
         day_of_week, month, is_weekend
       INSERT INTO fact_feature_store (574,509 rows)

     ↓ 02-demand-forecasting-pipeline/features/feature_validation.py
       Validates: no NULLs in lag features after burn-in period,
                  rolling_std > 0 for non-constant series,
                  feature ranges within expected bounds

     ↓ 02-demand-forecasting-pipeline/pipeline/forecast_pipeline.py
       Loads fact_feature_store
       Splits: train = Jan 2022–Sep 2023 | holdout = Oct–Dec 2023
       Fits 3 models per (sku_id, warehouse_id, abc_class):
         moving_average_14d: MEAN(lag_14:lag_1) with 14-day window
         moving_average_28d: MEAN(lag_28:lag_1) with 28-day window
         seasonal_naive: demand_qty = lag_28 (same week prior month)
       Evaluates on holdout: MAPE, RMSE, MAE, Bias
       Selects best model per ABC class by MAPE
       INSERT INTO fact_model_performance (4,992 rows)
       Generates 90-day forward forecasts with 80% CI (z=1.282):
         lower = MAX(0, prediction - 1.282 * std)
         upper = prediction + 1.282 * std
       INSERT INTO fact_forecast (149,760 rows)

INTERMEDIATE TABLES
  fact_feature_store    (574,509 rows)  — Engineered features
  fact_model_performance (4,992 rows)  — Holdout evaluation metrics
  dim_model             (3 rows)        — Model registry

     ↓ 02-demand-forecasting-pipeline/monitoring/pipeline_monitor.py
       Validates: all active SKUs have forecasts,
                  no negative lower bounds,
                  MAPE < 50% for A-class SKUs

FINAL OUTPUT
  02-demand-forecasting-pipeline/outputs/forecast_output.csv
    Columns: sku_id, warehouse_id, forecast_date, predicted_demand,
             lower_ci_80, upper_ci_80, model_used, abc_class

DOWNSTREAM CONSUMERS
  Supply planners:  forecast_output.csv for replenishment order sizing
  BA/DA Portfolio:  forecast_dashboard.html (Plotly interactive chart)
  DS Portfolio:     Model evaluation metrics used in model comparison analysis
```

---

## Lineage 3: Customer RFM Scores (rfm_scores.csv / fact_rfm_scores)

**Business question answered:** Which customers are Champions, Loyal, At Risk, or Lost — and who should the commercial team contact for retention campaigns?

```
SOURCE
  shared/data/dhl-synthetic/customers.csv
    Columns: customer_id, name, email, signup_date, segment
  shared/data/dhl-synthetic/orders.csv
    Columns: order_id, customer_id, order_date, revenue, channel

     ↓ 03-customer-pipeline/etl/customer_etl.py → load_customers()
       INSERT new customers into dim_customer
       UPDATE existing customers (name, email fields)
       Step: load_orders() — incremental by order_id set-difference:
         new_order_ids = set(orders_df.order_id) - set(existing_order_ids)
         INSERT only new rows into fact_orders
       Step: update_lifetime_metrics() — UPDATE dim_customer
         SET first_order_date, last_order_date, lifetime_orders, lifetime_revenue
         FROM aggregation of fact_orders

INTERMEDIATE TABLES
  dim_customer  (500 rows)    — Customer master with lifetime metrics
  fact_orders   (68,941 rows) — Full order history

     ↓ 03-customer-pipeline/pipeline/rfm_scoring_pipeline.py
       Loads dim_customer with lifetime metrics
       Computes recency = MAX(order_date) - TODAY in days (lower = better)
       Computes frequency = lifetime_orders
       Computes monetary = lifetime_revenue
       Scores each dimension 1–5 using quintile binning (pd.qcut, duplicates='drop')
         Recency: reversed (1 = most recent)
         Frequency, Monetary: standard (5 = highest)
       Assigns segment labels:
         Champions:     R>=4, F>=4, M>=4
         Loyal:         R>=3, F>=3, M>=3
         At Risk:       R<=2, F>=3, M>=3
         Needs Attention: R<=2, F>=2, M>=2
         Lost:          R=1, F=1
         New:           F=1, high Recency
         Potential:     all others
       SCD2 expiry: UPDATE fact_rfm_scores SET is_current_flag=FALSE WHERE is_current_flag=TRUE
       INSERT new scores with is_current_flag=TRUE, valid_from=NOW

     ↓ 03-customer-pipeline/pipeline/data_quality.py
       Checks: Champions ≤ 15% of customer base,
               no A/B test contamination (customer in both test and control),
               all rfm_score values between 1 and 5

INTERMEDIATE TABLE
  fact_rfm_scores (398 rows, is_current=TRUE) — Current RFM scores per customer

     ↓ 03-customer-pipeline/datamart/commercial_datamart.py
       CREATE OR REPLACE VIEW v_customer_segments (RFM scores + segment labels)
       CREATE OR REPLACE VIEW v_champion_customers (Champions ordered by revenue)
       CREATE OR REPLACE VIEW v_at_risk_customers (At Risk ordered by revenue)
       CREATE OR REPLACE VIEW v_segment_performance (avg revenue/orders per segment)
       Export each view to CSV

FINAL OUTPUT
  03-customer-pipeline/outputs/rfm_scores.csv
  03-customer-pipeline/outputs/v_champion_customers.csv
  03-customer-pipeline/outputs/v_at_risk_customers.csv
  03-customer-pipeline/outputs/v_segment_performance.csv

DOWNSTREAM CONSUMERS
  Commercial team:  Champion/At Risk CSVs for targeted outreach campaigns
  BA/DA Portfolio:  rfm_dashboard.html (segment distribution and revenue charts)
  DS Portfolio:     RFM segments used as features in churn prediction model
```

---

## Lineage 4: Slotting Recommendations (slotting_recommendations.csv / fact_slotting_history)

**Business question answered:** Which SKUs are misslotted (Hot SKUs not in Pick_Face, Cold SKUs occupying prime slots) and what is the estimated daily time saving from moving them?

```
SOURCE
  shared/data/dhl-synthetic/wms_tasks.csv
    Columns: task_id, warehouse_id, sku_id, operator_id, task_date, task_type,
             shift, duration_min, quantity, accuracy_flag, error_code
  shared/data/dhl-synthetic/warehouse_locations.csv
    Columns: location_id, warehouse_id, zone, aisle, bay, storage_type,
             capacity_units, active_flag

     ↓ 04-warehouse-operations-pipeline/etl/wms_etl.py → load_locations()
       SCD2 upsert: compare incoming vs current is_current=TRUE records
       Trigger fields: zone, storage_type, active_flag
       On change: expire old row (valid_to=NOW), INSERT new row
       INSERT INTO dim_location (2,640 rows — all current, no expirations yet)

     ↓ 04-warehouse-operations-pipeline/etl/wms_etl.py → load_operators()
       Anonymise: operator_surrogate_id = SHA256(raw_id)[:8].upper() with "OP-" prefix
       Derive hire_date_cohort from first task date (YYYY-QN format)
       INSERT INTO dim_operator (60 rows)

     ↓ 04-warehouse-operations-pipeline/etl/wms_etl.py → load_wms_tasks()
       Incremental: set-difference on task_id
       Map raw operator_id to operator_surrogate_id via op_map dict
       INSERT INTO fact_wms_tasks (219,000 rows) in 20,000-row chunks

     ↓ 04-warehouse-operations-pipeline/pipeline/slotting_pipeline.py
       SELECT from fact_wms_tasks WHERE task_type = 'Pick' AND date in last 90 days
       Compute pick_count per (sku_id, warehouse_id)
       Classify velocity using per-warehouse percentiles:
         Hot = top 10%, Warm = 70–90th, Cool = 40–70th, Cold = bottom 40%
       Get current zone from dim_location WHERE is_current=TRUE
         (Note: fact_wms_tasks has no location_id; zone assigned via hash of sku+warehouse)
       Flag mismatches:
         Hot not in Pick_Face → recommend Pick_Face
         Cold in Pick_Face    → recommend Reserve
       Estimate daily_minutes_saved = pick_count * 4.0 / 90  (4 min per movement)
       Dedup: skip (sku_id, warehouse_id) pairs with existing pending recommendation
       INSERT INTO fact_slotting_history (882 rows)

INTERMEDIATE TABLE
  fact_slotting_history (882 rows) — Pending slotting recommendations

     ↓ 04-warehouse-operations-pipeline/datamart/warehouse_datamart.py
       CREATE OR REPLACE VIEW v_slotting_queue (pending recommendations, ordered by savings)
       Export to CSV

FINAL OUTPUT
  04-warehouse-operations-pipeline/outputs/slotting_recommendations.csv
    Columns: sku_id, warehouse_id, prior_zone, recommended_zone,
             pick_frequency_at_recommendation, est_daily_minutes_saved,
             est_annual_minutes_saved, implementation_status

DOWNSTREAM CONSUMERS
  Warehouse managers:  slotting_recommendations.csv for physical slot moves
  BA/DA Portfolio:     warehouse_dashboard.html (slotting impact visualisation)
  WMS team:            Update implementation_status to 'implemented' after moves
```

---

## Lineage 5: WMS Daily KPIs (fact_wms_daily_kpis / v_warehouse_comparison)

**Business question answered:** What were the pick accuracy, throughput, and productivity metrics for each warehouse on each shift today — and how does this compare across the network?

```
SOURCE
  shared/data/dhl-synthetic/wms_tasks.csv
    [Already loaded into fact_wms_tasks by Project 04 — 219,000 rows]

     ↓ 05-wms-data-warehouse/etl/wms_warehouse_etl.py → load_error_log()
       SELECT from fact_wms_tasks WHERE accuracy_flag = FALSE
       Filter: error_code IS NOT NULL AND error_code != ''
       Incremental: task_id set-difference with existing fact_error_log
       Derive: category from sku_id prefix (e.g. PHM-001234 → PHM)
       Derive: error_context = f-string of task_type + warehouse + sku + error_code
       INSERT INTO fact_error_log (1,417 rows)

     ↓ 05-wms-data-warehouse/aggregations/build_aggregations.py → build_daily_kpis()
       Full rebuild each run (DELETE + INSERT)
       SELECT from fact_wms_tasks GROUP BY task_date, warehouse_id, shift
       Compute:
         total_tasks, total_picks, total_putaways, total_replenishments, total_cycle_counts
         pick_accuracy_pct = 100 * accurate_picks / total_picks
         putaway_accuracy_pct, cycle_count_accuracy_pct, overall_accuracy_pct
         avg_pick_duration_min, avg_putaway_duration_min
         picks_per_labour_hour = picks / (pick_minutes / 60)
         total_errors
       INSERT INTO fact_wms_daily_kpis (6,570 rows)

     ↓ 05-wms-data-warehouse/aggregations/build_aggregations.py → build_monthly_kpis()
       SELECT from fact_wms_daily_kpis GROUP BY YEAR(kpi_date), MONTH(kpi_date), warehouse_id
       Volume-weighted accuracy roll-up
       INSERT INTO fact_wms_monthly_kpis (72 rows)

     ↓ 05-wms-data-warehouse/aggregations/build_aggregations.py → build_operator_daily()
       SELECT from fact_wms_tasks GROUP BY operator_surrogate_id, warehouse_id, task_date, shift
       Compute top_error_code via ROW_NUMBER mode calculation
       Apply performance_flag: >=99.8% → high_performer, <98.5% → needs_coaching
       INSERT INTO fact_operator_daily (162,317 rows)

     ↓ 05-wms-data-warehouse/aggregations/validate_aggregations.py
       Validate: SUM(daily_kpis.total_tasks) = COUNT(*) in fact_wms_tasks [PASS]
                 Random sample of 10 dates: accuracy matches recalculation [PASS]
                 No missing dates in KPI range [PASS]
                 Operator count matches [PASS]
                 Monthly totals = daily totals [PASS]

INTERMEDIATE TABLES
  fact_wms_daily_kpis   (6,570 rows)
  fact_wms_monthly_kpis (72 rows)
  fact_operator_daily   (162,317 rows)
  fact_error_log        (1,417 rows)

     ↓ 05-wms-data-warehouse/serving/warehouse_serving_layer.py
       CREATE OR REPLACE VIEW v_network_kpis_current_month
       CREATE OR REPLACE VIEW v_warehouse_comparison (last 30 days, all warehouses)
       CREATE OR REPLACE VIEW v_kpi_trends_12m (monthly trend, last 12 months)
       CREATE OR REPLACE VIEW v_operator_leaderboard (current month ranking)
       CREATE OR REPLACE VIEW v_error_patterns (30-day error frequency + trend)
       CREATE OR REPLACE VIEW v_coaching_list (needs_coaching in last 14 days)
       CREATE OR REPLACE VIEW v_high_performer_list (high performers in last 30 days)
       Export all 7 to CSV

     ↓ 05-wms-data-warehouse/quality/anomaly_detection.py
       Flag accuracy drops >2σ below 90-day warehouse mean (109 flags)
       Flag volume drops >3σ below mean (0 flags)
       Flag operator accuracy drops >5pp vs 30-day personal average (704 flags)
       Flag error rate spikes >2x 30-day baseline (4 flags)
       Export to outputs/anomaly_flags.csv (817 total flags)

FINAL OUTPUTS
  05-wms-data-warehouse/outputs/v_warehouse_comparison.csv
  05-wms-data-warehouse/outputs/v_operator_leaderboard.csv
  05-wms-data-warehouse/outputs/v_coaching_list.csv
  05-wms-data-warehouse/outputs/v_high_performer_list.csv
  05-wms-data-warehouse/outputs/anomaly_flags.csv

DOWNSTREAM CONSUMERS
  Ops manager:        v_warehouse_comparison.csv for daily network review
  Warehouse managers: v_operator_leaderboard.csv for shift performance review
  HR/Training:        v_coaching_list.csv for coaching session scheduling
  Data Engineering:   anomaly_flags.csv for pipeline health monitoring
  BA/DA Portfolio:    wms_dashboard.html (5-section KPI dashboard)
```
