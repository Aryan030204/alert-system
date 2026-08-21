"""
queries.py — All SQL for COD vs Prepaid monitoring.

Payment classification:
  • 'Partial'  → payment_gateway_names LIKE '%Gokwik PPCOD%'
  • 'COD'      → NULL / empty / Cash on Delivery (COD) / cash_on_delivery
  • 'Prepaid'  → everything else (Gokwik Cards, UPI, Razorpay, etc.)
"""

# ---------------------------------------------------------------------------
# Shared payment-type CASE expression
# ---------------------------------------------------------------------------
_PAYMENT_CASE = """
    CASE
        WHEN payment_gateway_names LIKE '%Gokwik PPCOD%' THEN 'Partial'
        WHEN payment_gateway_names IS NULL
          OR payment_gateway_names = ''
          OR payment_gateway_names LIKE '%Cash on Delivery (COD)%'
          OR payment_gateway_names LIKE '%cash_on_delivery%' THEN 'COD'
        ELSE 'Prepaid'
    END
""".strip()


# ---------------------------------------------------------------------------
# 1. Overall store-level baseline check (overall_summary table)
#    Compares today's split with the rolling 7-day average (excluding today)
# ---------------------------------------------------------------------------
OVERALL_DOD_QUERY = """
WITH daily AS (
    SELECT
        date,
        total_orders,
        cod_orders,
        prepaid_orders,
        partially_paid_orders,
        ROUND(cod_orders * 100.0 / NULLIF(total_orders, 0), 2)          AS cod_pct,
        ROUND(prepaid_orders * 100.0 / NULLIF(total_orders, 0), 2)      AS prepaid_pct,
        ROUND(partially_paid_orders * 100.0 / NULLIF(total_orders, 0), 2) AS partial_pct
    FROM overall_summary
    WHERE date BETWEEN DATE_SUB(CURDATE(), INTERVAL 7 DAY) AND CURDATE()
),
today AS (
    SELECT *
    FROM daily
    WHERE date = CURDATE()
),
baseline AS (
    SELECT
        COUNT(*)                          AS baseline_days,
        ROUND(AVG(cod_pct), 2)           AS baseline_cod_pct,
        ROUND(AVG(prepaid_pct), 2)       AS baseline_prepaid_pct,
        ROUND(AVG(partial_pct), 2)       AS baseline_partial_pct
    FROM daily
    WHERE date BETWEEN DATE_SUB(CURDATE(), INTERVAL 7 DAY) AND DATE_SUB(CURDATE(), INTERVAL 1 DAY)
)

SELECT
    t.date                                            AS today_date,
    t.total_orders                                    AS today_total_orders,
    t.cod_orders                                      AS today_cod_orders,
    t.prepaid_orders                                  AS today_prepaid_orders,
    t.partially_paid_orders                           AS today_partial_orders,
    t.cod_pct                                         AS today_cod_pct,
    t.prepaid_pct                                     AS today_prepaid_pct,
    t.partial_pct                                     AS today_partial_pct,
    b.baseline_days,
    b.baseline_cod_pct,
    b.baseline_prepaid_pct,
    b.baseline_partial_pct,
    ROUND((t.cod_pct - b.baseline_cod_pct) * 100.0 / NULLIF(b.baseline_cod_pct, 0), 2)         AS delta_cod_pct,
    ROUND((t.prepaid_pct - b.baseline_prepaid_pct) * 100.0 / NULLIF(b.baseline_prepaid_pct, 0), 2) AS delta_prepaid_pct,
    ROUND((t.partial_pct - b.baseline_partial_pct) * 100.0 / NULLIF(b.baseline_partial_pct, 0), 2) AS delta_partial_pct
FROM today t
CROSS JOIN baseline b
"""


# ---------------------------------------------------------------------------
# 2. Product-level baseline check (shopify_orders table)
#
#    Params (positional %s in order):
#      1..N  → product_id whitelist         (WHERE product_id IN (...))
#      N+1   → min_orders                   (HAVING total_orders >= ?)
#      N+2   → min_baseline_days            (baseline history quality filter)
#
#    Returns one row per product that exists on today and has enough baseline
#    days in the last 7 days, sorted by |relative COD delta| descending.
#    Includes product name resolved from line_item via product_id.
# ---------------------------------------------------------------------------
def product_baseline_query(product_ids: list[str]) -> str:
    """
    Build the product-level baseline SQL for a given product_id whitelist.
    product_ids must be non-empty (enforced by monitor.py before calling).
    """
    if not product_ids:
        raise ValueError("product_ids must be a non-empty list")

    placeholders = ", ".join(["%s"] * len(product_ids))

    return f"""
WITH classified AS (
    -- Classify each (date, order, product) exactly once.
    SELECT
        created_date,
        order_id,
        product_id,
        MIN(line_item)  AS product_name,
        {_PAYMENT_CASE} AS payment_type
    FROM shopify_orders
    WHERE created_date BETWEEN DATE_SUB(CURDATE(), INTERVAL 7 DAY) AND CURDATE()
      AND product_id IN ({placeholders})      -- param 1..N: whitelist
    GROUP BY created_date, order_id, product_id, payment_gateway_names
),

daily_product AS (
    SELECT
        created_date,
        product_id,
        MAX(product_name)                                                    AS product_name,
        COUNT(DISTINCT order_id)                                             AS total_orders,
        COUNT(DISTINCT CASE WHEN payment_type = 'COD'     THEN order_id END) AS cod_orders,
        COUNT(DISTINCT CASE WHEN payment_type = 'Prepaid' THEN order_id END) AS prepaid_orders,
        COUNT(DISTINCT CASE WHEN payment_type = 'Partial' THEN order_id END) AS partial_orders,
        ROUND(
            COUNT(DISTINCT CASE WHEN payment_type = 'COD' THEN order_id END)
            * 100.0
            / NULLIF(COUNT(DISTINCT order_id), 0),
        2) AS cod_pct,
        ROUND(
            COUNT(DISTINCT CASE WHEN payment_type = 'Prepaid' THEN order_id END)
            * 100.0
            / NULLIF(COUNT(DISTINCT order_id), 0),
        2) AS prepaid_pct,
        ROUND(
            COUNT(DISTINCT CASE WHEN payment_type = 'Partial' THEN order_id END)
            * 100.0
            / NULLIF(COUNT(DISTINCT order_id), 0),
        2) AS partial_pct
    FROM classified
    GROUP BY created_date, product_id
    HAVING COUNT(DISTINCT order_id) >= %s    -- param N+1: min_orders noise filter
),

today_p AS (
    SELECT *
    FROM daily_product
    WHERE created_date = CURDATE()
),

baseline_p AS (
    SELECT
        product_id,
        ROUND(AVG(cod_pct), 2)     AS baseline_cod_pct,
        ROUND(AVG(prepaid_pct), 2) AS baseline_prepaid_pct,
        ROUND(AVG(partial_pct), 2) AS baseline_partial_pct,
        COUNT(*)                   AS baseline_days
    FROM daily_product
    WHERE created_date BETWEEN DATE_SUB(CURDATE(), INTERVAL 7 DAY) AND DATE_SUB(CURDATE(), INTERVAL 1 DAY)
    GROUP BY product_id
    HAVING COUNT(*) >= %s          -- param N+2: min_baseline_days
)

SELECT
    t.product_id,
    t.product_name,
    t.total_orders                              AS today_total_orders,
    t.cod_orders                                AS today_cod_orders,
    t.prepaid_orders                            AS today_prepaid_orders,
    t.partial_orders                            AS today_partial_orders,
    t.cod_pct                                   AS today_cod_pct,
    t.prepaid_pct                               AS today_prepaid_pct,
    t.partial_pct                               AS today_partial_pct,
    b.baseline_days,
    b.baseline_cod_pct,
    b.baseline_prepaid_pct,
    b.baseline_partial_pct,
    ROUND((t.cod_pct - b.baseline_cod_pct) * 100.0 / NULLIF(b.baseline_cod_pct, 0), 2)         AS delta_cod_pct,
    ROUND((t.prepaid_pct - b.baseline_prepaid_pct) * 100.0 / NULLIF(b.baseline_prepaid_pct, 0), 2) AS delta_prepaid_pct,
    ROUND((t.partial_pct - b.baseline_partial_pct) * 100.0 / NULLIF(b.baseline_partial_pct, 0), 2) AS delta_partial_pct
FROM today_p t
INNER JOIN baseline_p b USING (product_id)
ORDER BY ABS((t.cod_pct - b.baseline_cod_pct) * 100.0 / NULLIF(b.baseline_cod_pct, 0)) DESC
"""
