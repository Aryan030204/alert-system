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
# real IST now" -- run.py never passes it, so live behavior is
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


# The DB server clock is UTC, but order timestamps (created_at) and
# hour_wise_sales dates/hours are IST. "Now" must therefore be IST (UTC + 5:30),
# not NOW()/CURDATE(): with NOW() the "today so far" window was cut off ~5.5h
# behind real time, and between 00:00 and 05:30 IST CURDATE() was still yesterday.
IST_NOW_SQL = "DATE_ADD(UTC_TIMESTAMP(), INTERVAL 330 MINUTE)"


def _as_of_sql(as_of):
    """Return (date_literal_sql, datetime_literal_sql) for use inline in
    a query. None -> the real IST date/now. A 'YYYY-MM-DD HH:MM:SS'
    string -> that fixed point in time, quoted as a literal."""
    if as_of is None:
        return f"DATE({IST_NOW_SQL})", IST_NOW_SQL
    return f"'{as_of[:10]}'", f"'{as_of}'"


def _as_of_time_sql(as_of):
    """Return the TIME(...) comparison literal: the real IST time-of-day or
    the fixed as_of's time-of-day."""
    if as_of is None:
        return f"TIME({IST_NOW_SQL})"
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


# -- Day-to-date KPI cards (for the alert email) ------------------------

def fetch_kpis(conn, as_of: str = None):
    """Day-to-date KPIs from hour_wise_sales for the alert's date: total
    sales, orders, sessions, ATC sessions, plus derived CVR and AOV.
    Returns None on any failure so a KPI problem never blocks the alert
    itself (the email simply omits the cards)."""
    # The DB server clock is UTC but hour_wise_sales dates/hours are IST, so the
    # live path derives the IST clock explicitly (UTC + 5:30) instead of using
    # CURDATE()/NOW().
    if as_of is None:
        ist = IST_NOW_SQL
        date_lit = f"DATE({ist})"
        hour_lit = f"HOUR({ist})"
        frac_lit = f"(MINUTE({ist}) / 60)"
    else:
        date_lit = f"'{as_of[:10]}'"
        hour_lit = str(int(as_of[11:13]))
        frac_lit = str(int(as_of[14:16]) / 60)
    days = THRESHOLDS["baseline_days"]

    def _sum_cols(weight_sql: str) -> str:
        cols = [("total_sales", "total_sales"), ("number_of_orders", "orders"),
                ("number_of_sessions", "sessions"), ("number_of_atc_sessions", "atc_sessions")]
        return ",\n".join(
            f"COALESCE(SUM(CASE {weight_sql.format(col=col)} END), 0) AS {alias}" for col, alias in cols
        )

    # Today: every hour up to and including the current one (actual values).
    today_weight = f"WHEN hour <= {hour_lit} THEN {{col}} ELSE 0"
    # Baseline: full hours before the current one, plus the current hour
    # prorated by minutes elapsed, so a partial hour today isn't compared
    # against a full hour on prior days.
    base_weight = f"WHEN hour < {hour_lit} THEN {{col}} WHEN hour = {hour_lit} THEN {{col}} * {frac_lit} ELSE 0"
    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute(
            f"SELECT {_sum_cols(today_weight)} FROM hour_wise_sales WHERE date = {date_lit}"
        )
        row = cursor.fetchone()
        cursor.execute(
            f"""
            SELECT date, {_sum_cols(base_weight)}
            FROM hour_wise_sales
            WHERE date >= DATE_SUB({date_lit}, INTERVAL {days} DAY) AND date < {date_lit}
            GROUP BY date
            """
        )
        base_rows = cursor.fetchall()
        cursor.close()
    except Exception as exc:
        log.warning("KPI fetch failed: %s", exc)
        return None

    def _build(total_sales, orders, sessions, atc_sessions):
        return {
            "total_sales": total_sales,
            "orders": orders,
            "sessions": sessions,
            "atc_sessions": atc_sessions,
            "atc_rate": (atc_sessions / sessions * 100) if sessions else None,
            "cvr": (orders / sessions * 100) if sessions else None,
            "aov": (total_sales / orders) if orders else None,
        }

    current = _build(
        float(row["total_sales"] or 0), int(row["orders"] or 0),
        int(row["sessions"] or 0), int(row["atc_sessions"] or 0),
    )

    n = len(base_rows)
    baseline = None
    deltas = {k: None for k in ("total_sales", "sessions", "atc_rate", "cvr", "aov")}
    if n:
        baseline = _build(
            sum(float(r["total_sales"] or 0) for r in base_rows) / n,
            sum(float(r["orders"] or 0) for r in base_rows) / n,
            sum(float(r["sessions"] or 0) for r in base_rows) / n,
            sum(float(r["atc_sessions"] or 0) for r in base_rows) / n,
        )
        for key in deltas:
            cur_v, base_v = current[key], baseline[key]
            deltas[key] = ((cur_v - base_v) / base_v * 100) if (cur_v is not None and base_v) else None

    current["baseline"] = baseline
    current["deltas"] = deltas
    return current


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