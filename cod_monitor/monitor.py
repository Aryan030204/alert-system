"""
monitor.py - Core monitoring engine.

Per brand:
    1. Overall baseline  -> overall_summary -> alert if COD spike > thresholds["overall"]
    2. Product baseline  -> shopify_orders  -> alert if COD spike > thresholds["product"]
       only runs if product_ids whitelist is non-empty
       product name pulled from line_item column
"""

from __future__ import annotations

import logging
from typing import Any

from db import get_connection, run_query
from queries import OVERALL_DOD_QUERY, product_baseline_query

logger = logging.getLogger(__name__)


def _fmt_delta(delta: float | None) -> str:
    if delta is None:
        return "N/A"
    sign = "+" if delta > 0 else ""
    return f"{sign}{delta}%"


def _overall_alert(row: dict, threshold: float, brand: str) -> dict | None:
    delta = row.get("delta_cod_pct")
    if delta is None:
        return None

    if delta > threshold:
        prepaid_delta = row.get("delta_prepaid_pct")
        partial_delta = row.get("delta_partial_pct")
        return {
            "brand": brand,
            "level": "overall",
            "emoji": "SPIKE",
            "delta": delta,
            "today_cod_pct": row.get("today_cod_pct"),
            "baseline_cod_pct": row.get("baseline_cod_pct"),
            "today_total_orders": row.get("today_total_orders"),
            "message": (
                f"Overall COD spike: +{delta}% "
                f"(today {row.get('today_cod_pct')}% vs 7d avg {row.get('baseline_cod_pct')}% "
                f"| prepaid delta {_fmt_delta(prepaid_delta)} "
                f"| partial delta {_fmt_delta(partial_delta)} "
                f"| orders today: {row.get('today_total_orders')})"
            ),
        }
    return None


def _product_alerts(rows: list[dict], threshold: float, brand: str) -> list[dict]:
    alerts = []
    for row in rows:
        delta = row.get("delta_cod_pct")
        if delta is None:
            continue

        if delta > threshold:
            product_id = row.get("product_id", "unknown")
            product_name = row.get("product_name") or "-"
            prepaid_delta = row.get("delta_prepaid_pct")
            partial_delta = row.get("delta_partial_pct")

            alerts.append(
                {
                    "brand": brand,
                    "level": "product",
                    "product_id": product_id,
                    "product_name": product_name,
                    "emoji": "SPIKE",
                    "delta": delta,
                    "today_cod_pct": row.get("today_cod_pct"),
                    "baseline_cod_pct": row.get("baseline_cod_pct"),
                    "today_total_orders": row.get("today_total_orders"),
                    "message": (
                        f"{product_name} (ID: {product_id}) | "
                        f"COD delta +{delta}% | "
                        f"today COD {row.get('today_cod_pct')}% vs "
                        f"7d avg {row.get('baseline_cod_pct')}% | "
                        f"prepaid delta {_fmt_delta(prepaid_delta)} | "
                        f"partial delta {_fmt_delta(partial_delta)} | "
                        f"orders: {row.get('today_total_orders')}"
                    ),
                }
            )
    return alerts


def process_brand(brand_name: str, brand_cfg: dict) -> dict[str, Any]:
    """
    Run all checks for a single brand and return structured results.
    """
    result: dict[str, Any] = {
        "brand": brand_name,
        "status": "ok",
        "overall_row": None,
        "product_rows": [],
        "alerts": [],
        "error": None,
    }

    thresholds = brand_cfg["thresholds"]
    min_orders = brand_cfg.get("product_min_orders", 20)
    min_baseline_days = brand_cfg.get("baseline_min_days", 4)
    product_ids = brand_cfg.get("product_ids", [])

    try:
        with get_connection(brand_cfg["db"]) as conn:
            overall_rows = run_query(conn, OVERALL_DOD_QUERY)
            if overall_rows:
                row = overall_rows[0]
                result["overall_row"] = row
                alert = _overall_alert(row, thresholds["overall"], brand_name)
                if alert:
                    result["alerts"].append(alert)
                logger.info(
                    "[%s] Overall -> today=%.1f%% baseline=%.1f%% delta=%+.1f%%",
                    brand_name,
                    row.get("today_cod_pct") or 0,
                    row.get("baseline_cod_pct") or 0,
                    row.get("delta_cod_pct") or 0,
                )
            else:
                logger.warning("[%s] No overall_summary data for baseline comparison", brand_name)

            if not product_ids:
                logger.info("[%s] No product_ids configured - skipping product-level check", brand_name)
            else:
                sql = product_baseline_query(product_ids)
                params = tuple(product_ids) + (min_orders, min_baseline_days)

                product_rows = run_query(conn, sql, params)
                result["product_rows"] = product_rows

                prod_alerts = _product_alerts(product_rows, thresholds["product"], brand_name)
                result["alerts"].extend(prod_alerts)

                logger.info(
                    "[%s] Products checked: %d | alerts triggered: %d",
                    brand_name,
                    len(product_rows),
                    len(prod_alerts),
                )

                found_ids = {str(r["product_id"]) for r in product_rows}
                missing = [pid for pid in product_ids if str(pid) not in found_ids]
                if missing:
                    logger.warning(
                        "[%s] These whitelisted products had no data (or were below min_orders): %s",
                        brand_name,
                        missing,
                    )

    except Exception as exc:  # noqa: BLE001
        result["status"] = "error"
        result["error"] = str(exc)
        logger.error("[%s] Processing failed: %s", brand_name, exc)

    return result
