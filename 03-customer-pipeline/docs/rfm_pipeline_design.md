# RFM Pipeline Technical Design
## DHL Data Engineer Portfolio — Project 03

---

## Overview

This document describes the technical design of the customer segmentation and A/B testing pipeline in Project 03. It covers four areas: the SCD Type 2 pattern for historical RFM score preservation, the A/B test assignment and contamination prevention logic, the commercial data mart design, and the incremental load strategy for orders.

---

## SCD Type 2 for RFM Scores

### Why Not Overwrite?

RFM scores change every time the scoring pipeline runs. Simply updating `dim_customer.current_rfm_segment` captures the latest state but destroys the history. This matters because:

- **Trend analysis**: The commercial team needs to know whether a customer is improving (Hibernating → Promising → Loyal) or declining (Champion → At Risk → Lost). A single current score cannot answer this.
- **Campaign attribution**: If a re-engagement campaign ran in Q3, the business needs to compare the customer's segment before and after the campaign to measure impact.
- **SLA compliance and audit**: Some DHL contracts specify service levels based on customer tier. Historical scores provide an audit trail.

### How It Works

`fact_rfm_scores` uses a Slowly Changing Dimension Type 2 pattern:

| Column | Role |
|---|---|
| `score_id` | Surrogate key — unique per scoring run per customer |
| `is_current_flag` | `TRUE` for the active score, `FALSE` for history |
| `valid_from` | Timestamp when this score became active |
| `valid_to` | Timestamp when this score was superseded (`NULL` = still current) |

On each scoring run:
1. All rows where `is_current_flag = TRUE` are expired: `is_current_flag → FALSE`, `valid_to → CURRENT_TIMESTAMP`
2. New score rows are inserted with `is_current_flag = TRUE`, `valid_from = NOW()`, `valid_to = NULL`

To reconstruct a customer's segment at any past date `D`:
```sql
SELECT rfm_segment
FROM fact_rfm_scores
WHERE customer_id = 'CUST-00001'
  AND valid_from <= '2023-06-30'
  AND (valid_to IS NULL OR valid_to > '2023-06-30')
```

### Scoring Frequency

The pipeline is designed to run on-demand or on a weekly schedule. Running more frequently than weekly on the same demand data will produce the same scores (reference date is `MAX(order_date)` in `fact_orders`), which is harmless — the SCD2 insert will still create a new historical record, allowing score stability tracking.

### Quintile Scoring

Each R, F, M dimension is scored 1–5 using `pd.qcut` (quintile boundaries computed from the current population). This means:

- Scores are **relative** to the current customer base, not absolute thresholds
- The score boundaries shift as the customer base changes over time
- A customer scoring 3 in Q1 may score 4 in Q2 because other customers dropped off, even if their own behaviour did not change

This is intentional for segmentation purposes — a score of 5 always means "top 20% of current customers." For absolute threshold comparisons (e.g., "customer hasn't ordered in 90 days"), use the raw `recency_days` column directly.

---

## A/B Test Assignment Logic and Contamination Prevention

### Test Registry

Every A/B test is registered in `dim_ab_test_registry` before assignment begins. The registry stores:
- Test name (unique), hypothesis, target segment, primary metric
- Split ratio (default 50/50), start and end dates, status

This provides a single source of truth for all tests and allows the `assign_customers` function to look up overlapping tests.

### Assignment Process

1. **Eligibility filter**: Customers must be in the target RFM segment and `active_flag = TRUE`
2. **Contamination check**: Customers already assigned to any other **active** test whose date range overlaps the new test are excluded
3. **Random assignment**: Eligible customers are shuffled with a configurable seed (default: 42 for reproducibility), then split at `floor(n × split_ratio)` into test and control
4. **Idempotency**: The function checks `fact_ab_assignments` before inserting — re-running will not double-assign already-assigned customers

### Contamination Prevention

The key guard is the overlap check:
```sql
SELECT DISTINCT customer_id FROM fact_ab_assignments a
JOIN dim_ab_test_registry r ON a.test_name = r.test_name
WHERE r.status = 'active'
  AND a.test_name != <new_test_name>
  AND r.test_start_date <= <new_test_end>
  AND r.test_end_date   >= <new_test_start>
```

A customer in an active retention test cannot simultaneously be in a new upsell test if the date windows overlap. This prevents the confounding effect of a customer receiving multiple interventions simultaneously, which would make it impossible to attribute any observed change to a single test.

### Statistical Analysis

The `analyse_test` function runs a two-proportion z-test:
- **Null hypothesis**: conversion rate of test group = conversion rate of control group
- **Test statistic**: pooled proportion z-test: `z = (p_t - p_c) / sqrt(p_pool × (1-p_pool) × (1/n_t + 1/n_c))`
- **P-value**: two-tailed, using normal approximation
- **Confidence interval**: 95% CI on the difference in proportions
- **Effect size**: Cohen's h

The function recommends deployment if p < 0.05 and the lift direction is positive.

**Important caveat**: On synthetic data with uniform random order assignment, test and control groups will have near-identical conversion rates. A real test would introduce a treatment (e.g., targeted email) that is absent for the control group, producing a genuine difference. The infrastructure is production-ready; the synthetic data simply cannot simulate a real intervention effect.

---

## Commercial Data Mart Design

Four DuckDB views provide the commercial team's self-service layer:

| View | Purpose | Primary Consumer |
|---|---|---|
| `v_customer_segments` | Full customer list with current RFM scores and contact metadata | Account management, CRM import |
| `v_at_risk_customers` | Filtered to At Risk segment, ordered by monetary value | Re-engagement campaign team |
| `v_champion_customers` | Filtered to Champions, ordered by monetary value | Upsell / premium programme team |
| `v_segment_performance` | Aggregate KPIs per segment | Commercial director, S&OP reporting |

Views are **live queries against the warehouse** — they always reflect the latest data without any additional ETL step. The `commercial_datamart.py` script materialises these views to CSV for downstream Excel/Tableau consumption, but the views themselves remain available for direct SQL access.

The design separates concerns: the commercial team consumes pre-filtered, business-friendly views without needing to understand the underlying star schema joins or SCD2 logic.

---

## Incremental Load Strategy for Orders

`fact_orders` uses the same watermark-based incremental pattern established in Project 02:

1. At load time, fetch the set of `order_id` values already in `fact_orders`
2. Filter the source `outbound_orders.csv` to exclude any already-present `order_id`
3. Insert only the new records

This approach is chosen over a date-watermark for orders because `order_id` is a natural key — order records can arrive out of date sequence (e.g., a December order confirmed and exported in January). A pure date watermark would miss late-arriving orders for dates already loaded. The `order_id` set-difference approach correctly handles this at the cost of a slightly larger watermark query.

For very large tables in production, this set would be replaced with a hash or a database-managed identity check, but for the synthetic dataset (≈69,000 orders) the set-difference approach completes in under a second.

### Deduplication

The source CSV is deduplicated on `order_id` before any load logic runs. The count of duplicates is logged and the first occurrence is kept. In production, duplicate order IDs would be routed to a quarantine table for manual review.
