"""
ab_test_pipeline.py — A/B Test Infrastructure Pipeline
DHL Data Engineer Portfolio — Project 03

Functions:
  1. create_test        — Register a new test in dim_ab_test_registry
  2. assign_customers   — Randomly assign eligible customers to test/control
  3. record_outcomes    — Capture post-assignment orders and flag conversions
  4. analyse_test       — Two-proportion z-test, p-value, CI, recommendation
  5. run_at_risk_test   — Convenience wrapper: full At Risk retention test
"""

import logging
import time
import math
import random
from datetime import datetime, date
from pathlib import Path
import duckdb
import pandas as pd
import numpy as np

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BASE_DIR   = Path(__file__).resolve().parent.parent
DB_PATH    = BASE_DIR.parent / "01-sku-segmentation-pipeline" / "outputs" / "dhl_warehouse.duckdb"
OUTPUT_DIR = BASE_DIR / "outputs"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def get_logger(name: str = "ab_test_pipeline") -> logging.Logger:
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

# ---------------------------------------------------------------------------
# 1. create_test
# ---------------------------------------------------------------------------

def create_test(conn: duckdb.DuckDBPyConnection,
                test_name: str,
                hypothesis: str,
                target_segment: str,
                primary_metric: str,
                test_start_date: date,
                test_end_date: date,
                split_ratio: float = 0.5,
                logger: logging.Logger = None) -> int:
    """
    Register a new A/B test. Returns test_id.
    Raises ValueError if a test with the same name already exists.
    """
    if logger is None:
        logger = get_logger()

    existing = conn.execute(
        "SELECT test_id FROM dim_ab_test_registry WHERE test_name = ?", [test_name]
    ).fetchone()
    if existing:
        logger.info(f"  Test '{test_name}' already registered (id={existing[0]}) — skipping")
        return existing[0]

    max_id = conn.execute(
        "SELECT COALESCE(MAX(test_id), 0) FROM dim_ab_test_registry"
    ).fetchone()[0]
    test_id = int(max_id) + 1

    conn.execute("""
        INSERT INTO dim_ab_test_registry
            (test_id, test_name, hypothesis, target_segment, primary_metric,
             split_ratio, test_start_date, test_end_date, status, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'active', ?)
    """, [test_id, test_name, hypothesis, target_segment, primary_metric,
          split_ratio, test_start_date, test_end_date, datetime.utcnow()])

    logger.info(f"  Test registered: '{test_name}' (id={test_id})")
    logger.info(f"    Target segment: {target_segment} | Metric: {primary_metric}")
    logger.info(f"    Period: {test_start_date} to {test_end_date} | Split: {split_ratio:.0%} test")
    return test_id


# ---------------------------------------------------------------------------
# 2. assign_customers
# ---------------------------------------------------------------------------

def assign_customers(conn: duckdb.DuckDBPyConnection,
                     test_name: str,
                     target_segment: str,
                     test_start_date: date,
                     test_end_date: date,
                     split_ratio: float = 0.5,
                     random_seed: int = 42,
                     logger: logging.Logger = None) -> dict:
    """
    Assign eligible customers to test / control.
    Eligibility: in target_segment AND not already in any active test.
    No customer appears in two simultaneously active tests.
    """
    if logger is None:
        logger = get_logger()

    # Find customers already assigned to an overlapping active test
    already_in_test = {r[0] for r in conn.execute("""
        SELECT DISTINCT customer_id FROM fact_ab_assignments a
        JOIN dim_ab_test_registry r ON a.test_name = r.test_name
        WHERE r.status = 'active'
          AND a.test_name != ?
          AND r.test_start_date <= ?
          AND r.test_end_date   >= ?
    """, [test_name, test_end_date, test_start_date]).fetchall()}

    # Get eligible customers from target segment
    eligible = conn.execute("""
        SELECT customer_id FROM dim_customer
        WHERE current_rfm_segment = ?
          AND active_flag = TRUE
    """, [target_segment]).df()

    eligible = eligible[~eligible["customer_id"].isin(already_in_test)]

    # Check for already-assigned customers in THIS test (idempotent)
    already_assigned = {r[0] for r in conn.execute(
        "SELECT customer_id FROM fact_ab_assignments WHERE test_name = ?",
        [test_name]
    ).fetchall()}
    eligible = eligible[~eligible["customer_id"].isin(already_assigned)]

    if len(eligible) == 0:
        logger.info(f"  No eligible customers for '{test_name}' — already assigned or none in segment")
        return {"test_group": 0, "control_group": 0, "excluded": len(already_in_test)}

    # Random assignment
    rng = random.Random(random_seed)
    customers = eligible["customer_id"].tolist()
    rng.shuffle(customers)
    split_at = int(len(customers) * split_ratio)
    test_group    = customers[:split_at]
    control_group = customers[split_at:]

    logger.info(f"  Assigning {len(customers):,} customers to '{test_name}'")
    logger.info(f"    Test: {len(test_group):,}  |  Control: {len(control_group):,}")
    logger.info(f"    Excluded (in other tests): {len(already_in_test):,}")

    # Build assignment records
    max_id = conn.execute(
        "SELECT COALESCE(MAX(assignment_id), 0) FROM fact_ab_assignments"
    ).fetchone()[0]
    assigned_at = datetime.utcnow()

    records = []
    for i, (group, members) in enumerate([("test", test_group), ("control", control_group)]):
        for cid in members:
            max_id += 1
            records.append({
                "assignment_id":           max_id,
                "customer_id":             cid,
                "test_name":               test_name,
                "test_group":              group,
                "assigned_at":             assigned_at,
                "test_start_date":         test_start_date,
                "test_end_date":           test_end_date,
                "primary_metric_value":    None,
                "converted_flag":          False,
                "conversion_date":         None,
                "revenue_post_assignment": 0.0,
            })

    df = pd.DataFrame(records)
    cols = list(df.columns)
    col_list = ", ".join(f'"{c}"' for c in cols)
    conn.register("_ab_staging", df)
    conn.execute(f"INSERT INTO fact_ab_assignments ({col_list}) SELECT {col_list} FROM _ab_staging")
    conn.unregister("_ab_staging")

    return {"test_group": len(test_group), "control_group": len(control_group),
            "excluded": len(already_in_test)}


# ---------------------------------------------------------------------------
# 3. record_outcomes
# ---------------------------------------------------------------------------

def record_outcomes(conn: duckdb.DuckDBPyConnection,
                    test_name: str,
                    logger: logging.Logger = None) -> dict:
    """
    For each assigned customer, look up orders placed after assignment date
    and within the test window. Update:
      - converted_flag = True if any order placed
      - conversion_date = first order date after assignment
      - revenue_post_assignment = total revenue from post-assignment orders
      - primary_metric_value = revenue_post_assignment (proxy for engagement)
    """
    if logger is None:
        logger = get_logger()

    # Get all assignments for this test
    assignments = conn.execute("""
        SELECT assignment_id, customer_id, test_group,
               CAST(assigned_at AS DATE) AS assigned_date,
               test_end_date
        FROM fact_ab_assignments
        WHERE test_name = ?
    """, [test_name]).df()

    if len(assignments) == 0:
        logger.info(f"  No assignments found for test '{test_name}'")
        return {"updated": 0}

    # Get post-assignment orders for these customers
    customer_ids = tuple(assignments["customer_id"].tolist())
    if len(customer_ids) == 1:
        placeholder = f"('{customer_ids[0]}')"
    else:
        placeholder = str(customer_ids)

    orders_df = conn.execute(f"""
        SELECT customer_id,
               MIN(order_date) AS first_order_after,
               SUM(revenue)    AS total_revenue
        FROM fact_orders
        WHERE customer_id IN {placeholder}
        GROUP BY customer_id
    """).df()

    assignments = assignments.merge(orders_df, on="customer_id", how="left")

    # Conversion: placed an order after assignment date
    assignments["assigned_date"]  = pd.to_datetime(assignments["assigned_date"]).dt.date
    assignments["first_order_after"] = pd.to_datetime(
        assignments["first_order_after"], errors="coerce"
    ).dt.date
    assignments["test_end_date"]  = pd.to_datetime(assignments["test_end_date"]).dt.date

    assignments["converted_flag"] = (
        assignments["first_order_after"].notna() &
        (assignments["first_order_after"] > assignments["assigned_date"]) &
        (assignments["first_order_after"] <= assignments["test_end_date"])
    )
    assignments["revenue_post_assignment"] = assignments["total_revenue"].fillna(0.0)
    assignments["primary_metric_value"]    = assignments["revenue_post_assignment"]
    assignments["conversion_date"] = assignments.apply(
        lambda r: r["first_order_after"] if r["converted_flag"] else None, axis=1
    )

    # Write updates back
    updated = 0
    for _, row in assignments.iterrows():
        conn.execute("""
            UPDATE fact_ab_assignments
            SET converted_flag          = ?,
                conversion_date         = ?,
                revenue_post_assignment = ?,
                primary_metric_value    = ?
            WHERE assignment_id = ?
        """, [bool(row["converted_flag"]),
              row["conversion_date"],
              float(row["revenue_post_assignment"]),
              float(row["primary_metric_value"]),
              int(row["assignment_id"])])
        updated += 1

    conv_rate = assignments["converted_flag"].mean() * 100
    logger.info(f"  Outcomes recorded for '{test_name}': {updated:,} assignments updated")
    logger.info(f"  Overall conversion rate: {conv_rate:.1f}%")
    return {"updated": updated}


# ---------------------------------------------------------------------------
# 4. analyse_test
# ---------------------------------------------------------------------------

def analyse_test(conn: duckdb.DuckDBPyConnection,
                 test_name: str,
                 output_dir: Path = OUTPUT_DIR,
                 logger: logging.Logger = None) -> dict:
    """
    Two-proportion z-test on conversion rates.
    Prints p-value, confidence interval, effect size, recommendation.
    """
    if logger is None:
        logger = get_logger()

    results = conn.execute("""
        SELECT test_group,
               COUNT(*) AS n,
               SUM(CASE WHEN converted_flag THEN 1 ELSE 0 END) AS conversions,
               AVG(revenue_post_assignment) AS avg_revenue
        FROM fact_ab_assignments
        WHERE test_name = ?
        GROUP BY test_group
        ORDER BY test_group
    """, [test_name]).df()

    if len(results) < 2:
        logger.warning(f"  Cannot analyse '{test_name}' — need both test and control groups")
        return {}

    test_row    = results[results["test_group"] == "test"].iloc[0]
    control_row = results[results["test_group"] == "control"].iloc[0]

    n_t = int(test_row["n"]);    c_t = int(test_row["conversions"])
    n_c = int(control_row["n"]); c_c = int(control_row["conversions"])

    p_t = c_t / n_t if n_t > 0 else 0
    p_c = c_c / n_c if n_c > 0 else 0

    # Pooled proportion z-test
    p_pool = (c_t + c_c) / (n_t + n_c) if (n_t + n_c) > 0 else 0
    se = math.sqrt(p_pool * (1 - p_pool) * (1/n_t + 1/n_c)) if p_pool > 0 else 0
    z_stat = (p_t - p_c) / se if se > 0 else 0

    # Two-tailed p-value (normal approximation)
    # Using standard normal CDF approximation
    def norm_cdf(z):
        return 0.5 * (1 + math.erf(z / math.sqrt(2)))

    p_value = 2 * (1 - norm_cdf(abs(z_stat)))

    # 95% CI on difference in proportions
    se_diff = math.sqrt(p_t*(1-p_t)/n_t + p_c*(1-p_c)/n_c) if (n_t > 0 and n_c > 0) else 0
    ci_lower = (p_t - p_c) - 1.96 * se_diff
    ci_upper = (p_t - p_c) + 1.96 * se_diff

    # Effect size (Cohen's h)
    cohen_h = abs(2 * math.asin(math.sqrt(p_t)) - 2 * math.asin(math.sqrt(p_c)))

    significant = p_value < 0.05
    lift = ((p_t - p_c) / p_c * 100) if p_c > 0 else 0
    recommendation = (
        f"SIGNIFICANT — test group shows {lift:+.1f}% lift in conversion rate. "
        f"Recommend deploying to full {'At Risk' if 'risk' in test_name.lower() else ''} segment."
        if significant else
        f"NOT SIGNIFICANT (p={p_value:.3f}) — insufficient evidence to prefer test over control. "
        "Consider running longer or with larger sample."
    )

    logger.info(f"\n{'='*60}")
    logger.info(f"A/B TEST ANALYSIS: {test_name}")
    logger.info(f"{'='*60}")
    logger.info(f"  Test group:    n={n_t:,}  conversions={c_t:,}  rate={p_t:.1%}  avg_rev=£{test_row['avg_revenue']:,.2f}")
    logger.info(f"  Control group: n={n_c:,}  conversions={c_c:,}  rate={p_c:.1%}  avg_rev=£{control_row['avg_revenue']:,.2f}")
    logger.info(f"  Lift:          {lift:+.1f}%")
    logger.info(f"  Z-statistic:   {z_stat:.3f}")
    logger.info(f"  P-value:       {p_value:.4f}  ({'< 0.05 ✓ SIGNIFICANT' if significant else '≥ 0.05 not significant'})")
    logger.info(f"  95% CI on diff: [{ci_lower:.4f}, {ci_upper:.4f}]")
    logger.info(f"  Cohen's h:     {cohen_h:.3f}")
    logger.info(f"  Recommendation: {recommendation}")

    # Save analysis to CSV
    output_dir.mkdir(parents=True, exist_ok=True)
    analysis_path = output_dir / f"ab_test_{test_name.lower().replace(' ','_')}_analysis.csv"
    analysis_df = pd.DataFrame([{
        "test_name": test_name, "n_test": n_t, "n_control": n_c,
        "conversions_test": c_t, "conversions_control": c_c,
        "conversion_rate_test": round(p_t, 4), "conversion_rate_control": round(p_c, 4),
        "lift_pct": round(lift, 2), "z_statistic": round(z_stat, 4),
        "p_value": round(p_value, 4), "significant": significant,
        "ci_lower_95": round(ci_lower, 4), "ci_upper_95": round(ci_upper, 4),
        "cohens_h": round(cohen_h, 4), "recommendation": recommendation,
        "analysed_at": datetime.utcnow().isoformat(),
    }])
    analysis_df.to_csv(analysis_path, index=False)
    logger.info(f"  Analysis saved: {analysis_path.name}")

    return {
        "test_name": test_name, "n_test": n_t, "n_control": n_c,
        "p_t": round(p_t, 4), "p_c": round(p_c, 4),
        "lift_pct": round(lift, 2), "p_value": round(p_value, 4),
        "significant": significant, "recommendation": recommendation,
    }


# ---------------------------------------------------------------------------
# 5. run_at_risk_test — full At Risk retention test from BA/DA project
# ---------------------------------------------------------------------------

def run_at_risk_test(db_path: Path = DB_PATH, output_dir: Path = OUTPUT_DIR,
                     logger: logging.Logger = None) -> dict:
    if logger is None:
        logger = get_logger()

    logger.info("=" * 60)
    logger.info("AT RISK RETENTION A/B TEST — START")
    logger.info("=" * 60)

    conn = duckdb.connect(str(db_path))

    # Determine date range from fact_orders
    date_range = conn.execute(
        "SELECT MIN(order_date), MAX(order_date) FROM fact_orders"
    ).fetchone()
    min_date, max_date = date_range

    # Simulate: test ran over final 6 months of order data
    from datetime import timedelta
    if max_date:
        max_date_d = pd.to_datetime(max_date).date()
        min_date_d = pd.to_datetime(min_date).date()
        midpoint    = max_date_d - timedelta(days=180)
        test_start  = max(midpoint, min_date_d)
        test_end    = max_date_d
    else:
        test_start = date(2023, 1, 1)
        test_end   = date(2023, 12, 31)

    test_name = "at_risk_retention_campaign"

    # Step 1: Register
    test_id = create_test(
        conn,
        test_name       = test_name,
        hypothesis      = "Targeted re-engagement emails will increase conversion rate of At Risk customers vs no intervention (control).",
        target_segment  = "At Risk",
        primary_metric  = "conversion_rate",
        test_start_date = test_start,
        test_end_date   = test_end,
        split_ratio     = 0.5,
        logger          = logger,
    )

    # Step 2: Assign
    assign_stats = assign_customers(
        conn, test_name, "At Risk", test_start, test_end,
        split_ratio=0.5, random_seed=42, logger=logger
    )

    # Step 3: Record outcomes
    outcome_stats = record_outcomes(conn, test_name, logger=logger)

    # Step 4: Analyse
    analysis = analyse_test(conn, test_name, output_dir=output_dir, logger=logger)

    conn.close()
    logger.info("AT RISK RETENTION A/B TEST — COMPLETE")
    return analysis


if __name__ == "__main__":
    import sys
    db = Path(sys.argv[1]) if len(sys.argv) > 1 else DB_PATH
    logger = get_logger()
    result = run_at_risk_test(db_path=db, logger=logger)
    if result:
        print(f"\nTest: {result['test_name']}")
        print(f"Significant: {result['significant']} (p={result['p_value']})")
        print(f"Lift: {result['lift_pct']:+.1f}%")
