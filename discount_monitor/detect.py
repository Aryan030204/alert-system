# =============================================================
# detect.py - Brand-level alert rules, UTM/campaign/code drill-down,
#             cooldown check, alert storage.
#
# All deltas use: (current - baseline) / baseline * 100
# (no percentage-point deltas anywhere in this file)
# =============================================================

import logging

from config import ENABLED_ALERT_TYPES, THRESHOLDS
from fetch import (
    fetch_current_totals,
    fetch_baseline_totals,
    fetch_current_grouped,
    fetch_baseline_grouped_share,
)

log = logging.getLogger(__name__)

CODE_EXPR = "discount_codes"
SOURCE_EXPR = "COALESCE(NULLIF(TRIM(utm_source), ''), 'Direct/Unknown')"
CAMPAIGN_EXPR = "COALESCE(NULLIF(TRIM(utm_campaign), ''), 'Unknown')"


def is_enabled(alert_type: str) -> bool:
    return ENABLED_ALERT_TYPES.get(alert_type, False)


def pct_delta(current: float, baseline: float):
    """(current - baseline) / baseline * 100. None if baseline is 0 (no
    meaningful baseline to compare against -- caller should show 'new')."""
    if not baseline:
        return None
    return (current - baseline) / baseline * 100


# -- Cooldown / storage --------------------------------------------------

def is_on_cooldown(conn, brand: str, alert_type: str) -> bool:
    # Compared inside the DB: alert_time is written with the DB's NOW(), so the
    # comparison must use that same clock, not the app machine's timezone.
    cursor = conn.cursor(dictionary=True)
    cursor.execute(
        """
        SELECT MAX(alert_time) IS NOT NULL
               AND NOW() < DATE_ADD(MAX(alert_time), INTERVAL %s HOUR) AS on_cooldown
        FROM discount_alerts
        WHERE brand = %s AND alert_type = %s
        """,
        (THRESHOLDS["alert_cooldown_hrs"], brand, alert_type),
    )
    row = cursor.fetchone()
    cursor.close()
    return bool(row and row["on_cooldown"])


def store_alert(conn, brand, alert_type, current_value, baseline_value, delta_pct, message):
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO discount_alerts
            (alert_time, brand, alert_type, current_value, baseline_value, delta_pct, message)
        VALUES (NOW(), %s, %s, %s, %s, %s, %s)
        """,
        (
            brand,
            alert_type,
            round(current_value, 2),
            round(baseline_value, 2) if baseline_value is not None else None,
            round(delta_pct, 2) if delta_pct is not None else None,
            message,
        ),
    )
    conn.commit()
    cursor.close()


# -- Brand-level snapshot + alert decision -------------------------------

def build_snapshot(conn, brand_name: str, as_of: str = None):
    """Compute totals, rates and growth deltas for one brand's current
    window vs its 7-day baseline. Returns None if the window has too few
    orders to be meaningful. `as_of` (optional 'YYYY-MM-DD HH:MM:SS')
    replays this as of a past timestamp instead of the real now -- used
    by test_day.py; run.py leaves it unset."""
    current = fetch_current_totals(conn, as_of=as_of)
    if current["total_orders"] < THRESHOLDS["min_total_orders"]:
        log.info(
            "[%s] Skipping - only %s orders in window (threshold: %s)",
            brand_name, current["total_orders"], THRESHOLDS["min_total_orders"],
        )
        return None

    baseline = fetch_baseline_totals(conn, as_of=as_of)

    discount_rate_current = (
        current["discount_amount"] / current["gross_sales"] * 100
        if current["gross_sales"] else 0.0
    )
    discount_rate_baseline = (
        baseline["discount_amount"] / baseline["gross_sales"] * 100
        if baseline["gross_sales"] else 0.0
    )

    # discount USAGE rate: % of orders that used any discount at all
    # (order-count based) -- distinct from discount RATE above, which is
    # amount-based (% of sales value given away). Keeping both: a brand
    # can have more orders using small discounts (usage up, rate flat)
    # or fewer orders using much bigger discounts (usage flat, rate up)
    # -- different signals.
    usage_rate_current = (
        current["discounted_orders"] / current["total_orders"] * 100
        if current["total_orders"] else 0.0
    )
    usage_rate_baseline = (
        baseline["discounted_orders"] / baseline["total_orders"] * 100
        if baseline["total_orders"] else 0.0
    )

    snapshot = {
        "brand": brand_name,
        "current": current,
        "baseline": baseline,
        "discount_rate_current": discount_rate_current,
        "discount_rate_baseline": discount_rate_baseline,
        "discount_rate_delta": pct_delta(discount_rate_current, discount_rate_baseline),
        "usage_rate_current": usage_rate_current,
        "usage_rate_baseline": usage_rate_baseline,
        "usage_rate_delta": pct_delta(usage_rate_current, usage_rate_baseline),
        "discount_amount_delta": pct_delta(current["discount_amount"], baseline["discount_amount"]),
        "sales_delta": pct_delta(current["gross_sales"], baseline["gross_sales"]),
    }
    return snapshot


def decide_alerts(snapshot: dict) -> list[str]:
    """Return the list of alert types that fire for this brand's snapshot."""
    fired = []

    rate_delta = snapshot["discount_rate_delta"]
    if (
        is_enabled("DISCOUNT_RATE_SPIKE")
        and rate_delta is not None
        and rate_delta >= THRESHOLDS["discount_rate_delta_pct"]
    ):
        fired.append("DISCOUNT_RATE_SPIKE")

    disc_growth = snapshot["discount_amount_delta"]
    sales_growth = snapshot["sales_delta"]
    if (
        is_enabled("DISCOUNT_OUTPACING_SALES")
        and disc_growth is not None
        and sales_growth is not None
        and disc_growth >= THRESHOLDS["outpace_min_discount_growth_pct"]
        and (disc_growth - sales_growth) >= THRESHOLDS["outpace_gap_pct"]
    ):
        fired.append("DISCOUNT_OUTPACING_SALES")

    usage_delta = snapshot["usage_rate_delta"]
    if (
        is_enabled("USAGE_RATE_SPIKE")
        and usage_delta is not None
        and usage_delta >= THRESHOLDS["usage_rate_delta_pct"]
    ):
        fired.append("USAGE_RATE_SPIKE")

    return fired


# -- Top discount code (with cardinality guard) --------------------------

def top_code(conn, current_totals: dict, as_of: str = None):
    """Top discount code by amount this window, or None if codes look
    auto-generated per order rather than real campaign codes."""
    discounted = current_totals["discounted_orders"]
    unique_codes = current_totals["unique_codes"]
    if discounted and (unique_codes / discounted) > THRESHOLDS["code_cardinality_guard_ratio"]:
        log.info(
            "  Skipping top-code: %s unique codes across %s discounted orders "
            "looks auto-generated, not campaign codes.",
            unique_codes, discounted,
        )
        return None

    by_code = fetch_current_grouped(conn, CODE_EXPR, as_of=as_of)
    if not by_code:
        return None

    total_amount = sum(v["amount"] for v in by_code.values()) or 1
    top = max(by_code.items(), key=lambda kv: kv[1]["amount"])
    code, stats = top
    return {
        "code": code,
        "orders": stats["orders"],
        "amount": stats["amount"],
        "amount_share_pct": stats["amount"] / total_amount * 100,
        "orders_share_pct": stats["orders"] / discounted * 100 if discounted else 0,
    }


# -- New discount code detection ------------------------------------------

def new_code_check(conn, current_totals: dict, as_of: str = None):
    """Codes with ~zero presence in the 7-day baseline that are already
    taking a real share of today's discounted orders. Same cardinality
    guard as top_code: skipped for brands whose codes are auto-generated
    per order (every code would look 'new' there, which is meaningless)."""
    discounted = current_totals["discounted_orders"]
    unique_codes = current_totals["unique_codes"]
    if discounted and (unique_codes / discounted) > THRESHOLDS["code_cardinality_guard_ratio"]:
        return []

    current_by_code = fetch_current_grouped(conn, CODE_EXPR, as_of=as_of)
    if not current_by_code:
        return []
    baseline_codes = set(fetch_baseline_grouped_share(conn, CODE_EXPR, as_of=as_of).keys())

    flagged = []
    for code, stats in current_by_code.items():
        if code in baseline_codes:
            continue
        orders_share = stats["orders"] / discounted * 100 if discounted else 0
        if orders_share >= THRESHOLDS["new_code_min_share_pct"]:
            flagged.append({
                "code": code,
                "orders": stats["orders"],
                "amount": stats["amount"],
                "orders_share_pct": orders_share,
            })

    flagged.sort(key=lambda c: -c["orders_share_pct"])
    return flagged


# -- UTM source ranking + flagging ---------------------------------------

def utm_source_breakdown(conn, as_of: str = None):
    """Rank UTM sources by their share of today's discount amount vs
    their usual (7-day baseline) share. Returns a list sorted by current
    share desc, each item flagged if it clears the uplift threshold."""
    current = fetch_current_grouped(conn, SOURCE_EXPR, as_of=as_of)
    baseline_share = fetch_baseline_grouped_share(conn, SOURCE_EXPR, as_of=as_of)

    total_amount = sum(v["amount"] for v in current.values()) or 1
    rows = []
    for source, stats in current.items():
        current_share = stats["amount"] / total_amount * 100
        b_share = baseline_share.get(source, 0.0)
        delta = pct_delta(current_share, b_share)
        rows.append({
            "source": source,
            "current_share": current_share,
            "baseline_share": b_share,
            "delta_pct": delta,
            "orders": stats["orders"],
            "amount": stats["amount"],
        })

    rows.sort(key=lambda r: -r["current_share"])

    flagged = [
        r for r in rows
        if r["current_share"] >= THRESHOLDS["utm_min_current_share"]
        and r["delta_pct"] is not None
        and r["delta_pct"] >= THRESHOLDS["utm_uplift_delta_pct"]
    ]
    flagged.sort(key=lambda r: -r["delta_pct"])
    flagged = flagged[: THRESHOLDS["utm_max_flagged"]]
    flagged_sources = {r["source"] for r in flagged}
    for r in rows:
        r["flagged"] = r["source"] in flagged_sources

    return rows, flagged


# -- Campaign + code drill-down for a flagged source ---------------------

def campaign_drill(conn, source: str, as_of: str = None):
    """For a flagged UTM source, find its top campaign (current vs
    baseline share of that source's discount) and the codes under it."""
    escaped_source = source.replace("'", "''")
    is_unknown = source == "Direct/Unknown"
    source_filter = (
        "AND (utm_source IS NULL OR TRIM(utm_source) = '')" if is_unknown
        else f"AND TRIM(utm_source) = '{escaped_source}'"
    )

    current_campaigns = fetch_current_grouped(conn, CAMPAIGN_EXPR, source_filter, as_of=as_of)
    if not current_campaigns:
        return None
    baseline_campaign_share = fetch_baseline_grouped_share(conn, CAMPAIGN_EXPR, source_filter, as_of=as_of)

    source_total = sum(v["amount"] for v in current_campaigns.values()) or 1
    top_campaign, stats = max(current_campaigns.items(), key=lambda kv: kv[1]["amount"])
    current_share = stats["amount"] / source_total * 100
    baseline_share = baseline_campaign_share.get(top_campaign, 0.0)

    campaign_escaped = top_campaign.replace("'", "''")
    is_camp_unknown = top_campaign == "Unknown"
    campaign_filter = source_filter + (
        " AND (utm_campaign IS NULL OR TRIM(utm_campaign) = '')" if is_camp_unknown
        else f" AND TRIM(utm_campaign) = '{campaign_escaped}'"
    )
    codes_under_campaign = fetch_current_grouped(conn, CODE_EXPR, campaign_filter, as_of=as_of)

    campaign_amount_total = sum(v["amount"] for v in codes_under_campaign.values()) or 1
    campaign_orders_total = sum(v["orders"] for v in codes_under_campaign.values()) or 1
    codes = []
    for code, cstats in codes_under_campaign.items():
        amount_share = cstats["amount"] / campaign_amount_total * 100
        orders_share = cstats["orders"] / campaign_orders_total * 100
        codes.append({
            "code": code,
            "amount_share_pct": amount_share,
            "orders_share_pct": orders_share,
            "diverges": abs(amount_share - orders_share) >= THRESHOLDS["divergence_pp_to_split"],
        })
    codes.sort(key=lambda c: -c["amount_share_pct"])

    return {
        "campaign": top_campaign,
        "current_share": current_share,
        "baseline_share": baseline_share,
        "delta_pct": pct_delta(current_share, baseline_share),
        "codes": codes[:5],
    }