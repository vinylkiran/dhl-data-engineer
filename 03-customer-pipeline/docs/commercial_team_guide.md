# Commercial Team Guide — Customer Data Exports
## DHL Data Engineer Portfolio — Project 03

*This guide is for the commercial and account management teams. No technical background is required.*

---

## What Is RFM?

RFM stands for **Recency, Frequency, Monetary**. It is a method of scoring customers based on three things:

- **Recency**: How recently did the customer place an order? (fewer days = better)
- **Frequency**: How many total orders have they placed? (more = better)
- **Monetary**: How much have they spent in total? (more = better)

Each dimension is scored 1 to 5, where 5 is the best. These three scores are combined to assign every customer to a segment. Customers are updated in the system regularly — see the "When Are Scores Refreshed?" section below.

---

## Your Data Files

Four CSV files are exported to the `outputs/` folder after each pipeline run. Here is what each one contains and how to use it.

---

### 1. `v_customer_segments.csv` — Full Customer List

**What it contains**: Every active customer, their contact metadata, and their current RFM segment and scores.

**Key columns**:
| Column | Meaning |
|---|---|
| `customer_id` | Unique customer identifier (matches your CRM) |
| `customer_type` | E-Commerce / 3PL / Retail / etc. |
| `region` | Geographic region |
| `annual_rev_band` | Revenue band: `<50K`, `50K-200K`, `200K-1M`, `>1M` |
| `rfm_segment` | Segment name (see segment descriptions below) |
| `recency_days` | Days since last order as of scoring date |
| `frequency_count` | Total number of orders |
| `monetary_value` | Total revenue to date |
| `last_order_date` | Date of their most recent order |
| `lifetime_revenue` | Cumulative revenue from this customer |

**How to use it**: Import into your CRM or Tableau. Filter by `rfm_segment` to focus on specific groups. Sort by `monetary_value` to prioritise high-value accounts.

---

### 2. `v_at_risk_customers.csv` — Intervention List

**What it contains**: Customers currently in the **At Risk** segment — they used to order regularly but have gone quiet. This is your re-engagement priority list.

**Why they matter**: At Risk customers have a history with DHL and meaningful revenue value. Winning them back is far cheaper than acquiring a new customer. Left uncontacted, At Risk customers typically become Hibernating and then Lost.

**Recommended actions** (from the `recommended_action` column):
- Offer a discount on their next order
- Schedule a proactive account review call
- Ask about any service issues that may have caused the drop-off

**How to use it**: The list is sorted by `monetary_value` descending — start with the highest-value accounts. Assign re-engagement tasks in your CRM weekly. Aim to contact within 14 days of a customer appearing on this list.

---

### 3. `v_champion_customers.csv` — Upsell Targeting List

**What it contains**: Customers in the **Champions** segment — they order frequently, recently, and spend the most. These are your most engaged accounts.

**Why they matter**: Champions are your ambassadors. They respond well to early access offers, loyalty programmes, and premium tier invitations. They are also the most at risk of significant revenue loss if a competitor wins them over — proactive relationship management is key.

**Recommended actions**:
- Invite to a premium loyalty tier or partner programme
- Offer co-marketing opportunities or testimonials
- Flag for executive relationship review at major accounts

**How to use it**: Sorted by `monetary_value` descending. Coordinate with account management to ensure Champions have a named contact and regular check-ins scheduled.

---

### 4. `v_segment_performance.csv` — Segment KPIs

**What it contains**: One row per RFM segment with aggregate performance metrics.

**Key columns**:
| Column | Meaning |
|---|---|
| `segment` | RFM segment name |
| `customer_count` | Number of active customers in this segment |
| `avg_lifetime_revenue` | Average total revenue per customer in this segment |
| `total_segment_revenue` | Combined revenue contribution of the segment |
| `avg_orders_per_customer` | Average number of orders placed |
| `otif_rate_pct` | On-Time In-Full fulfilment rate (%) for this segment's orders |
| `avg_recency_days` | Average days since last order |

**How to use it**: Use in the monthly commercial review to track segment movement. If `customer_count` for At Risk is rising month-on-month, escalate to the commercial director. A healthy portfolio has Champions and Loyal Customers making up the largest share of `total_segment_revenue`.

---

## Segment Descriptions

| Segment | What It Means | Priority |
|---|---|---|
| **Champions** | Best customers — recent, frequent, high-spend | Retain and grow |
| **Loyal Customers** | Regular, committed buyers | Maintain relationship |
| **Potential Loyalists** | Recent buyers who could become Loyal | Nurture with offers |
| **New Customers** | Recent first purchase | Onboard and engage |
| **Promising** | Recent but lower frequency or spend | Encourage repeat orders |
| **Need Attention** | Middle of the road — need a push | Light re-engagement |
| **At Risk** | Formerly regular, now going quiet | Re-engage urgently |
| **Can't Lose Them** | Very high value, but haven't ordered recently | Immediate outreach |
| **Hibernating** | Low recency, frequency, and spend | Low-cost re-activation |
| **Lost** | Lowest scores across all three dimensions | Review whether worth re-activating |

---

## When Are RFM Scores Refreshed?

Scores are refreshed each time the data engineering team runs the pipeline, using the date of the most recent order in the system as the reference point. In a production environment this would run weekly (every Monday morning), so your exports would reflect the previous week's order activity.

If you need a fresh score outside the normal schedule, contact the data engineering team and they can run a manual refresh in minutes.

When scores are refreshed, **historical scores are preserved** — the system keeps a full record of each customer's past segments. This allows the team to measure whether re-engagement campaigns have successfully moved customers from At Risk back to Loyal.

---

## How to Request a New A/B Test

A/B tests allow the commercial team to scientifically measure whether a campaign (e.g., a discount email to At Risk customers) actually changes behaviour, rather than assuming it does.

To request a new A/B test:

1. **Define the hypothesis**: "We believe that [action X] will improve [metric Y] for customers in segment [Z]."
2. **Choose a target segment**: Which RFM segment should be targeted? (e.g., At Risk, Hibernating)
3. **Choose the primary metric**: What does success look like? (conversion rate, revenue, order frequency)
4. **Define the test period**: How long will the test run? (minimum 4 weeks recommended for statistical validity)
5. **Describe the treatment**: What will the test group receive that the control group will not?

Bring this information to the data engineering team. They will register the test, randomly assign customers to test and control groups (ensuring no customer is in two tests simultaneously), track orders during the test window, and produce a statistical analysis report showing whether the result is significant.

The system prevents a customer from receiving two interventions at once, which ensures clean results that can be confidently attributed to a single campaign.

---

## Data Freshness and Limitations

- **Data source**: All customer and order data comes from DHL's operational system (outbound_orders.csv and customers.csv). The pipeline does not connect to external CRM or marketing systems.
- **Lifetime revenue**: Calculated from orders in the DHL warehouse only — does not include any revenue from other channels not captured in the source data.
- **Segment stability**: RFM segments can shift between scoring runs if a customer places a large order or goes quiet. Always work from the most recently exported file.
- **OTIF rates**: Calculated from the orders loaded in the pipeline. Any orders not present in the source extract will not be reflected in OTIF metrics.
