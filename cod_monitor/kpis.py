"""
kpis.py - Day-to-date KPI cards (total sales, sessions, ATC sessions, CVR, AOV)
from hour_wise_sales, with change vs the 7-day average at the same time of day.

Same logic as the discount monitor's fetch_kpis. The DB server clock is UTC but
hour_wise_sales dates/hours are IST, so "now" is derived as UTC + 5:30.
"""

from __future__ import annotations

import logging
from typing import Any

from db import run_query

logger = logging.getLogger(__name__)

IST_NOW_SQL = "DATE_ADD(UTC_TIMESTAMP(), INTERVAL 330 MINUTE)"
BASELINE_DAYS = 7

_COLS = [
    ("total_sales", "total_sales"),
    ("number_of_orders", "orders"),
    ("number_of_sessions", "sessions"),
    ("number_of_atc_sessions", "atc_sessions"),
]


def _sum_cols(weight_sql: str) -> str:
    return ",\n".join(
        f"COALESCE(SUM(CASE {weight_sql.format(col=col)} END), 0) AS {alias}"
        for col, alias in _COLS
    )


def _build(total_sales: float, orders: float, sessions: float, atc_sessions: float) -> dict[str, Any]:
    return {
        "total_sales": total_sales,
        "orders": orders,
        "sessions": sessions,
        "atc_sessions": atc_sessions,
        "atc_rate": (atc_sessions / sessions * 100) if sessions else None,
        "cvr": (orders / sessions * 100) if sessions else None,
        "aov": (total_sales / orders) if orders else None,
    }


def fetch_kpis(conn) -> dict[str, Any] | None:
    """Returns None on any failure so a KPI problem never blocks the alert."""
    date_lit = f"DATE({IST_NOW_SQL})"
    hour_lit = f"HOUR({IST_NOW_SQL})"
    frac_lit = f"(MINUTE({IST_NOW_SQL}) / 60)"

    # Today: every hour up to and including the current one (actual values).
    today_weight = f"WHEN hour <= {hour_lit} THEN {{col}} ELSE 0"
    # Baseline: full hours before the current one plus the current hour prorated
    # by minutes elapsed, so a partial hour isn't compared against a full one.
    base_weight = (
        f"WHEN hour < {hour_lit} THEN {{col}} "
        f"WHEN hour = {hour_lit} THEN {{col}} * {frac_lit} ELSE 0"
    )
    try:
        today_rows = run_query(
            conn, f"SELECT {_sum_cols(today_weight)} FROM hour_wise_sales WHERE date = {date_lit}"
        )
        base_rows = run_query(
            conn,
            f"""
            SELECT date, {_sum_cols(base_weight)}
            FROM hour_wise_sales
            WHERE date >= DATE_SUB({date_lit}, INTERVAL {BASELINE_DAYS} DAY) AND date < {date_lit}
            GROUP BY date
            """,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("KPI fetch failed: %s", exc)
        return None

    row = today_rows[0]
    current = _build(
        float(row["total_sales"] or 0), int(row["orders"] or 0),
        int(row["sessions"] or 0), int(row["atc_sessions"] or 0),
    )

    n = len(base_rows)
    baseline = None
    deltas: dict[str, Any] = {k: None for k in ("total_sales", "sessions", "atc_rate", "cvr", "aov")}
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
