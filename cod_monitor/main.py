"""
main.py - Entry point for the COD vs Prepaid monitoring system.

Usage:
    python main.py
    python main.py --brands TMC
    python main.py --dry-run
    python main.py --json-output    # emit JSON payload to stdout for the Node alert engine
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from decimal import Decimal

from alerts import (
    print_console_report,
    send_alert_engine_payload,
    send_email_alert,
    send_slack_alert,
)
from config import (
    ALERT_ENGINE_COD_MONITOR_URL,
    BRANDS,
    EMAIL_CONFIG,
    MAX_WORKERS,
    SLACK_WEBHOOK_URL,
)
from monitor import process_brand

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# Logging always goes to stderr (+ file) so stdout stays clean for --json-output.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stderr),
        logging.FileHandler(f"cod_monitor_{date.today()}.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(description="COD vs Prepaid DoD Monitor")
    parser.add_argument(
        "--brands",
        nargs="+",
        help="Limit run to specific brand keys (e.g. --brands TMC BBB)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run queries and print results but skip Slack/email alerts",
    )
    parser.add_argument(
        "--json-output",
        action="store_true",
        help="Emit the final run payload as JSON to stdout for machine processing",
    )
    return parser.parse_args()


def run_all_brands(brand_keys: list[str]) -> list[dict]:
    """
    Process all brands in parallel using ThreadPoolExecutor.
    Returns a list of result dicts in brand_keys order.
    """
    results_map: dict[str, dict] = {}
    futures = {}

    with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(brand_keys))) as pool:
        for key in brand_keys:
            cfg = BRANDS[key]
            future = pool.submit(process_brand, key, cfg)
            futures[future] = key

        for future in as_completed(futures):
            key = futures[future]
            try:
                results_map[key] = future.result()
                logger.info("Completed brand: %s", key)
            except Exception as exc:  # noqa: BLE001
                logger.error("Unexpected error for brand %s: %s", key, exc)
                results_map[key] = {
                    "brand": key,
                    "status": "error",
                    "overall_row": None,
                    "product_rows": [],
                    "alerts": [],
                    "error": str(exc),
                }

    return [results_map[k] for k in brand_keys if k in results_map]


def build_run_payload(brand_results: list[dict], dry_run: bool) -> dict:
    total_alerts = sum(len(r["alerts"]) for r in brand_results)
    total_errors = sum(1 for r in brand_results if r["status"] == "error")

    return {
        "event_type": "cod_monitor.run",
        "source": "cod_monitor",
        "run_date": date.today().isoformat(),
        "dry_run": dry_run,
        "brand_count": len(brand_results),
        "total_alerts": total_alerts,
        "total_errors": total_errors,
        "brand_results": [
            {
                "brand": result["brand"],
                "status": result["status"],
                "error": result.get("error"),
                "overall": result.get("overall_row"),
                "product_rows": result.get("product_rows", []),
                "alerts": result.get("alerts", []),
                "alert_count": len(result.get("alerts", [])),
                "thresholds": BRANDS[result["brand"]]["thresholds"],
                "product_min_orders": BRANDS[result["brand"]].get("product_min_orders", 20),
                "configured_product_ids": BRANDS[result["brand"]].get("product_ids", []),
            }
            for result in brand_results
        ],
    }


def _json_safe(value):
    if isinstance(value, dict):
        return {key: _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [_json_safe(item) for item in value]
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value


def main():
    args = parse_args()

    if args.brands:
        unknown = [b for b in args.brands if b not in BRANDS]
        if unknown:
            logger.error("Unknown brand(s): %s. Valid options: %s", unknown, list(BRANDS))
            sys.exit(1)
        brand_keys = args.brands
    else:
        brand_keys = list(BRANDS.keys())

    logger.info(
        "Starting COD monitor | brands=%s | workers=%d | dry_run=%s",
        brand_keys,
        MAX_WORKERS,
        args.dry_run,
    )

    brand_results = run_all_brands(brand_keys)

    run_payload = build_run_payload(brand_results, args.dry_run)
    if args.json_output:
        logger.info("JSON output mode enabled; console report suppressed.")
    else:
        print_console_report(brand_results)
    logger.info(
        "COD run payload prepared: %s",
        json.dumps({
            "run_date": run_payload["run_date"],
            "brand_count": run_payload["brand_count"],
            "total_alerts": run_payload["total_alerts"],
            "total_errors": run_payload["total_errors"],
            "dry_run": run_payload["dry_run"],
        }),
    )

    if args.json_output:
        logger.info("JSON output mode: alert engine POST suppressed (payload returned via stdout).")
    elif args.dry_run:
        logger.info("Dry-run mode: alert engine payload suppressed.")
    else:
        send_alert_engine_payload(run_payload, ALERT_ENGINE_COD_MONITOR_URL)

    if not args.dry_run and not args.json_output:
        total_alerts = sum(len(r["alerts"]) for r in brand_results)

        if total_alerts > 0:
            logger.info("Sending alerts (%d total)...", total_alerts)
            send_slack_alert(brand_results, SLACK_WEBHOOK_URL)
            send_email_alert(brand_results, EMAIL_CONFIG)
        else:
            logger.info("No alerts triggered - skipping Slack/email notifications.")
    elif args.json_output and not args.dry_run:
        logger.info("JSON output mode: local Slack/email notifications suppressed.")
    else:
        logger.info("Dry-run mode: notifications suppressed.")

    errors = [r for r in brand_results if r["status"] == "error"]
    if errors:
        logger.warning("%d brand(s) encountered errors.", len(errors))
        if args.json_output:
            print(json.dumps(_json_safe(run_payload)))
        sys.exit(1)

    if args.json_output:
        print(json.dumps(_json_safe(run_payload)))

    logger.info("Monitor run complete.")


if __name__ == "__main__":
    main()
