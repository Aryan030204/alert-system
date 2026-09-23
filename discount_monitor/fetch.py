# =============================================================
# fetch.py - SQL queries: "day so far" totals + same-time-cutoff
#            baseline (last 7 days), plus generic grouped breakdowns
#            (by code, by utm_source, by utm_campaign).
#
# Comparison basis: midnight-to-now TODAY vs midnight-to-[same clock
# time] on each of the prior N days, averaged. This is deliberately
# the same cumulative your Shopify "today" dashboard shows -- so the
# numbers here should track it directly, and a real anomaly stays
# visible in the day-to-date rate rather than getting diluted by a
# fixed rolling window.
#
# `as_of` param: every function defaults to None, which means "use the
# real NOW()/CURDATE()" -- run.py never passes it, so live behavior is
# unchanged. Passing a 'YYYY-MM-DD HH:MM:SS' string instead makes the
# same functions replay as-of that timestamp, which is what
# test_day.py uses to validate a past day through this exact pipeline.
#
# Data-shape notes this file relies on (see Readme "Data quirks"):
#   - discount_amount, total_price, utm_source, utm_campaign are
#     order-level fields populated on exactly ONE line item per
#     order -- so a plain SUM(...) across all rows already gives
#     the correct per-order total, no dedup needed.
#   - line_item_total_discount is NOT populated (confirmed 0 across
#     brands) -- there is no reliable per-product discount split,
#     so this file never attempts one.
# =============================================================

import logging

from config import THRESHOLDS

log = logging.getLogger(__name__)


def _as_of_sql(as_of):
    """Return (date_literal_sql, datetime_literal_sql) for use inline in
    a query. None -> the real CURDATE()/NOW(). A 'YYYY-MM-DD HH:MM:SS'
    string -> that fixed point in time, quoted as a literal."""
    if as_of is None:
        return "CURDATE()", "NOW()"
    return f"'{as_of[:10]}'", f"'{as_of}'"


def _as_of_time_sql(as_of):
    """Return the TIME(...) comparison literal: real TIME(NOW()) or the
    fixed as_of's time-of-day."""
    if as_of is None:
        return "TIME(NOW())"
    return f"'{as_of[11:19]}'"


# -- Brand-level totals -----------------------------------------------

def fetch_current_totals(conn, as_of: str = None) -> dict:
    """Today so far: midnight to now (or midnight to as_of, if given)."""
    date_lit, dt_lit = _as_of_sql(as_of)
    cursor = conn.cursor(dictionary=True)
    cursor.execute(
        f"""
        SELECT
            COUNT(DISTINCT order_id) AS total_orders,
            COUNT(DISTINCT CASE
                WHEN discount_codes IS NOT NULL AND discount_codes != ''
                THEN order_id END) AS discounted_orders,
            COALESCE(SUM(discount_amount), 0) AS discount_amount,
            COALESCE(SUM(total_price), 0) AS gross_sales,
            COUNT(DISTINCT CASE
                WHEN discount_codes IS NOT NULL AND discount_codes != ''
                THEN discount_codes END) AS unique_codes
        FROM shopify_orders
        WHERE created_at >= {date_lit} AND created_at <= {dt_lit}
        """
    )
    row = cursor.fetchone()
    cursor.close()
    return {
        "total_orders": row["total_orders"] or 0,
        "discounted_orders": row["discounted_orders"] or 0,
        "discount_amount": float(row["discount_amount"] or 0),
        "gross_sales": float(row["gross_sales"] or 0),
        "unique_codes": row["unique_codes"] or 0,
    }


def fetch_baseline_totals(conn, as_of: str = None) -> dict:
    """Same midnight-to-[cutoff] window, averaged across the prior N days."""
    days = THRESHOLDS["baseline_days"]
    date_lit, _ = _as_of_sql(as_of)
    time_lit = _as_of_time_sql(as_of)
    cursor = conn.cursor(dictionary=True)
    cursor.execute(
        f"""
        SELECT
            DATE(created_at) AS d,
            COUNT(DISTINCT order_id) AS total_orders,
            COUNT(DISTINCT CASE
                WHEN discount_codes IS NOT NULL AND discount_codes != ''
                THEN order_id END) AS discounted_orders,
            COALESCE(SUM(discount_amount), 0) AS discount_amount,
            COALESCE(SUM(total_price), 0) AS gross_sales
        FROM shopify_orders
        WHERE DATE(created_at) >= DATE_SUB({date_lit}, INTERVAL {days} DAY)
          AND DATE(created_at) < {date_lit}
          AND TIME(created_at) <= {time_lit}
        GROUP BY DATE(created_at)
        """
    )
    rows = cursor.fetchall()
    cursor.close()

    n = len(rows) or 1
    return {
        "days_seen": len(rows),
        "total_orders": sum(r["total_orders"] or 0 for r in rows) / n,
        "discounted_orders": sum(r["discounted_orders"] or 0 for r in rows) / n,
        "discount_amount": sum(float(r["discount_amount"] or 0) for r in rows) / n,
        "gross_sales": sum(float(r["gross_sales"] or 0) for r in rows) / n,
    }


# -- Generic grouped breakdown (by code / by utm_source / by campaign) -

def fetch_current_grouped(conn, group_expr: str, extra_where: str = "", params=None, as_of: str = None) -> dict:
    """
    Today-so-far breakdown by an arbitrary grouping expression.
    Returns {group_value: {"orders": int, "amount": float}}.
    Only counts discounted orders (a discount_codes value is required to
    be in the picture at all).
    """
    date_lit, dt_lit = _as_of_sql(as_of)
    cursor = conn.cursor(dictionary=True)
    cursor.execute(
        f"""
        SELECT
            {group_expr} AS grp,
            COUNT(DISTINCT order_id) AS orders,
            COALESCE(SUM(discount_amount), 0) AS amount
        FROM shopify_orders
        WHERE created_at >= {date_lit} AND created_at <= {dt_lit}
          AND discount_codes IS NOT NULL AND discount_codes != ''
          {extra_where}
        GROUP BY grp
        """,
        params or (),
    )
    rows = cursor.fetchall()
    cursor.close()
    return {
        r["grp"]: {"orders": r["orders"] or 0, "amount": float(r["amount"] or 0)}
        for r in rows
    }


def fetch_baseline_grouped_share(conn, group_expr: str, extra_where: str = "", params=None, as_of: str = None) -> dict:
    """
    Same midnight-to-[cutoff] window on each of the prior N days, returned
    as each group's AVERAGE SHARE (%) of that day's total discount amount
    within scope, averaged across the baseline days.
    {group_value: avg_share_pct}
    """
    days = THRESHOLDS["baseline_days"]
    date_lit, _ = _as_of_sql(as_of)
    time_lit = _as_of_time_sql(as_of)
    cursor = conn.cursor(dictionary=True)
    cursor.execute(
        f"""
        SELECT
            DATE(created_at) AS d,
            {group_expr} AS grp,
            COALESCE(SUM(discount_amount), 0) AS amount
        FROM shopify_orders
        WHERE DATE(created_at) >= DATE_SUB({date_lit}, INTERVAL {days} DAY)
          AND DATE(created_at) < {date_lit}
          AND TIME(created_at) <= {time_lit}
          AND discount_codes IS NOT NULL AND discount_codes != ''
          {extra_where}
        GROUP BY DATE(created_at), grp
        """,
        params or (),
    )
    rows = cursor.fetchall()
    cursor.close()

    if not rows:
        return {}

    day_totals = {}
    for r in rows:
        day_totals[r["d"]] = day_totals.get(r["d"], 0) + float(r["amount"] or 0)

    days_seen = set(r["d"] for r in rows)
    n_days = len(days_seen) or 1

    group_share_sum = {}
    for r in rows:
        day_total = day_totals.get(r["d"], 0)
        if day_total <= 0:
            continue
        share = float(r["amount"] or 0) / day_total * 100
        group_share_sum[r["grp"]] = group_share_sum.get(r["grp"], 0) + share

    return {grp: total / n_days for grp, total in group_share_sum.items()}