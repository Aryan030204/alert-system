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
# 1. Overall store-level DoD  (overall_summary table)
#    No params — always compares CURDATE() vs CURDATE()-1
# ---------------------------------------------------------------------------
OVERALL_DOD_QUERY = """
WITH daily AS (
    SELECT
        date,
        total_orders,
        cod_orders,
        prepaid_orders,
        partially_paid_orders,
        ROUND(cod_orders * 100.0 / NULLIF(total_orders, 0), 2) AS cod_pct
    FROM overall_summary
    WHERE date IN (CURDATE(), DATE_SUB(CURDATE(), INTERVAL 1 DAY))
),
today     AS (SELECT * FROM daily WHERE date = CURDATE()),
yesterday AS (SELECT * FROM daily WHERE date = DATE_SUB(CURDATE(), INTERVAL 1 DAY))

SELECT
    t.date                                          AS today_date,
    t.total_orders                                  AS today_total_orders,
    t.cod_orders                                    AS today_cod_orders,
    t.cod_pct                                       AS today_cod_pct,
    y.date                                          AS yesterday_date,
    y.total_orders                                  AS yesterday_total_orders,
    y.cod_orders                                    AS yesterday_cod_orders,
    y.cod_pct                                       AS yesterday_cod_pct,
    ROUND(t.cod_pct - y.cod_pct, 2)                AS delta_cod_pct
FROM today t
CROSS JOIN yesterday y
"""


# ---------------------------------------------------------------------------
# 2. Product-level DoD  (shopify_orders table)
#
#    Params (positional %s in order):
#      1,2   → today_date, yesterday_date   (WHERE created_date IN (...))
#      3..N  → product_id whitelist         (WHERE product_id IN (...))
#      N+1   → min_orders                   (HAVING total_orders >= ?)
#      N+2   → today_date                   (today_p subquery filter)
#      N+3   → yesterday_date               (yesterday_p subquery filter)
#
#    Returns one row per product that exists on BOTH today and yesterday,
#    sorted by |delta_cod_pct| descending.
#    Includes product name resolved from line_item via product_id.
# ---------------------------------------------------------------------------
def product_dod_query(product_ids: list[str]) -> str:
    """
    Build the product-level DoD SQL for a given product_id whitelist.
    product_ids must be non-empty (enforced by monitor.py before calling).
    """
    if not product_ids:
        raise ValueError("product_ids must be a non-empty list")

    placeholders = ", ".join(["%s"] * len(product_ids))

    return f"""
WITH classified AS (
    -- Classify each (date, order, product) exactly once.
    -- GROUP BY includes payment_gateway_names so multi-gateway rows
    -- don't duplicate; the CASE picks the right type per row.
    SELECT
        created_date,
        order_id,
        product_id,
        -- Use MIN(line_item) as the canonical product name
        -- (consistent across orders for the same product_id)
        MIN(line_item)  AS product_name,
        {_PAYMENT_CASE} AS payment_type
    FROM shopify_orders
    WHERE created_date IN (%s, %s)          -- param 1,2: today, yesterday
      AND product_id IN ({placeholders})     -- param 3..N: whitelist
    GROUP BY created_date, order_id, product_id, payment_gateway_names
),

daily_product AS (
    -- Aggregate to (date, product) level on DISTINCT order basis
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
        2) AS cod_pct
    FROM classified
    GROUP BY created_date, product_id
    HAVING COUNT(DISTINCT order_id) >= %s   -- param N+1: min_orders noise filter
),

today_p     AS (SELECT * FROM daily_product WHERE created_date = %s),   -- param N+2
yesterday_p AS (SELECT * FROM daily_product WHERE created_date = %s)    -- param N+3

SELECT
    t.product_id,
    t.product_name,
    t.total_orders                              AS today_total_orders,
    t.cod_orders                                AS today_cod_orders,
    t.prepaid_orders                            AS today_prepaid_orders,
    t.partial_orders                            AS today_partial_orders,
    t.cod_pct                                   AS today_cod_pct,
    y.total_orders                              AS yesterday_total_orders,
    y.cod_orders                                AS yesterday_cod_orders,
    y.cod_pct                                   AS yesterday_cod_pct,
    ROUND(t.cod_pct - y.cod_pct, 2)            AS delta_cod_pct
FROM today_p t
INNER JOIN yesterday_p y USING (product_id)
ORDER BY ABS(t.cod_pct - y.cod_pct) DESC
"""