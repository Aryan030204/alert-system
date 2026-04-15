"""
monitor.py — Core monitoring engine.

Per brand:
  1. Overall DoD  → overall_summary       → alert if |delta| > thresholds["overall"]
  2. Product DoD  → shopify_orders        → alert if |delta| > thresholds["product"]
                    only runs if product_ids whitelist is non-empty
                    product name pulled from line_item column
"""

from __future__ import annotations
import logging
from datetime import date, timedelta
from typing import Any

from db import get_connection, run_query
from queries import OVERALL_DOD_QUERY, product_dod_query

logger = logging.getLogger(__name__)

TODAY     = date.today().strftime("%Y-%m-%d")
YESTERDAY = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")


# ── Alert builders ────────────────────────────────────────────────────────────

def _overall_alert(row: dict, threshold: float, brand: str) -> dict | None:
    delta = row.get("delta_cod_pct")
    if delta is None:
        return None
    if abs(delta) > threshold:
        sign  = "+" if delta > 0 else ""
        emoji = "📈" if delta > 0 else "📉"
        return {
            "brand":              brand,
            "level":              "overall",
            "emoji":              emoji,
            "delta":              delta,
            "today_cod_pct":      row.get("today_cod_pct"),
            "yesterday_cod_pct":  row.get("yesterday_cod_pct"),
            "today_total_orders": row.get("today_total_orders"),
            "message": (
                f"{emoji} Overall COD change: {sign}{delta}%  "
                f"(today {row.get('today_cod_pct')}% ← yesterday {row.get('yesterday_cod_pct')}%  "
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
        if abs(delta) > threshold:
            sign         = "+" if delta > 0 else ""
            emoji        = "📈" if delta > 0 else "📉"
            product_id   = row.get("product_id", "unknown")
            product_name = row.get("product_name") or "—"

            alerts.append({
                "brand":              brand,
                "level":              "product",
                "product_id":         product_id,
                "product_name":       product_name,
                "emoji":              emoji,
                "delta":              delta,
                "today_cod_pct":      row.get("today_cod_pct"),
                "yesterday_cod_pct":  row.get("yesterday_cod_pct"),
                "today_total_orders": row.get("today_total_orders"),
                "message": (
                    f"{emoji} {product_name} (ID: {product_id}) | "
                    f"Δ {sign}{delta}% | "
                    f"today COD {row.get('today_cod_pct')}% ← "
                    f"yesterday {row.get('yesterday_cod_pct')}% | "
                    f"orders: {row.get('today_total_orders')}"
                ),
            })
    return alerts


# ── Main brand processor ──────────────────────────────────────────────────────

def process_brand(brand_name: str, brand_cfg: dict) -> dict[str, Any]:
    """
    Run all checks for a single brand and return structured results.

    Returns:
        {
            brand, status, overall_row,
            product_rows, alerts, error
        }
    """
    result: dict[str, Any] = {
        "brand":        brand_name,
        "status":       "ok",
        "overall_row":  None,
        "product_rows": [],
        "alerts":       [],
        "error":        None,
    }

    thresholds  = brand_cfg["thresholds"]
    min_orders  = brand_cfg.get("product_min_orders", 20)
    product_ids = brand_cfg.get("product_ids", [])

    try:
        with get_connection(brand_cfg["db"]) as conn:

            # ── 1. Overall level ──────────────────────────────────────────────
            overall_rows = run_query(conn, OVERALL_DOD_QUERY)
            if overall_rows:
                row = overall_rows[0]
                result["overall_row"] = row
                alert = _overall_alert(row, thresholds["overall"], brand_name)
                if alert:
                    result["alerts"].append(alert)
                logger.info(
                    "[%s] Overall → today=%.1f%%  yesterday=%.1f%%  delta=%+.1f%%",
                    brand_name,
                    row.get("today_cod_pct") or 0,
                    row.get("yesterday_cod_pct") or 0,
                    row.get("delta_cod_pct") or 0,
                )
            else:
                logger.warning("[%s] No overall_summary data for today/yesterday", brand_name)

            # ── 2. Product level (whitelist only) ─────────────────────────────
            if not product_ids:
                logger.info(
                    "[%s] No product_ids configured — skipping product-level check", brand_name
                )
            else:
                sql = product_dod_query(product_ids)

                # Params order matches query placeholders:
                #   %s, %s          → today, yesterday  (WHERE created_date IN)
                #   *product_ids    → whitelist          (WHERE product_id IN)
                #   %s              → min_orders         (HAVING)
                #   %s              → today              (today_p)
                #   %s              → yesterday          (yesterday_p)
                params = (
                    (TODAY, YESTERDAY)
                    + tuple(product_ids)
                    + (min_orders, TODAY, YESTERDAY)
                )

                product_rows = run_query(conn, sql, params)
                result["product_rows"] = product_rows

                prod_alerts = _product_alerts(product_rows, thresholds["product"], brand_name)
                result["alerts"].extend(prod_alerts)

                logger.info(
                    "[%s] Products checked: %d  |  alerts triggered: %d",
                    brand_name, len(product_rows), len(prod_alerts),
                )

                # Warn if a whitelisted product had no data today/yesterday
                found_ids = {str(r["product_id"]) for r in product_rows}
                missing   = [pid for pid in product_ids if pid not in found_ids]
                if missing:
                    logger.warning(
                        "[%s] These whitelisted products had no data (or below min_orders): %s",
                        brand_name, missing,
                    )

    except Exception as exc:          # noqa: BLE001
        result["status"] = "error"
        result["error"]  = str(exc)
        logger.error("[%s] Processing failed: %s", brand_name, exc)

    return result