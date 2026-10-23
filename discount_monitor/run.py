# =============================================================
# run.py - Main entry point. Run this every 10-30 minutes.
#
# Usage:
#   python run.py
#   python run.py --brands BBB PTS
#   python run.py --dry-run
#   python run.py --json-output    # emit JSON payload to stdout for the Node alert engine
#
# Or call from your existing pipeline:
#   from run import run_monitor
#   run_monitor()
# =============================================================

import argparse
import json
import logging
import sys
from datetime import date, datetime
from decimal import Decimal

from config import BRANDS
from db import ensure_alerts_table, get_connection
from detect import (
    build_snapshot,
    decide_alerts,
    store_alert,
    top_code,
    new_code_check,
    utm_source_breakdown,
    campaign_drill,
)
from fetch import fetch_kpis
from format import format_digest
from notify import print_digest, send_digest_email

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# Logging always goes to stderr (+ file) so stdout stays clean for --json-output.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stderr),
        logging.FileHandler("discount_monitor.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(description="Discount Monitor")
    parser.add_argument(
        "--brands",
        nargs="+",
        help="Limit run to specific brand names (e.g. --brands BBB PTS)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run and print/log the digest but skip sending any email",
    )
    parser.add_argument(
        "--json-output",
        action="store_true",
        help="Emit the final run payload as JSON to stdout for machine processing",
    )
    return parser.parse_args()


HARD_ALERTS = {"DISCOUNT_RATE_SPIKE", "USAGE_RATE_SPIKE", "NEW_CODE"}


def _json_safe(value):
    if isinstance(value, dict):
        return {key: _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value


def _brand_result_payload(result: dict) -> dict:
    """Structured per-brand data for the Node email builder/state machine.
    Present for EVERY non-skipped brand, every run -- including brands with
    no breach this run -- so the Node-side state machine can see a
    TRIGGERED/CRITICAL -> NORMAL recovery, not just new breaches."""
    alerts_fired = result["alerts_fired"]
    if HARD_ALERTS & set(alerts_fired):
        tier = "ALERT"
    elif alerts_fired:
        tier = "WATCH"
    else:
        tier = "NORMAL"
    return {
        "brand": result["snapshot"]["brand"],
        "tier": tier,
        "alerts_fired": alerts_fired,
        "raw_eval": result["raw_eval"],
        "snapshot": result["snapshot"],
        "top_code": result.get("top_code"),
        "new_codes": result.get("new_codes", []),
        "utm_rows": result.get("utm_rows", []),
        "flagged_sources": result.get("flagged_sources", []),
        "kpis": result.get("kpis"),
    }


def _metric_for_alert_type(snapshot, alert_type, new_codes=None):
    if alert_type == "DISCOUNT_RATE_SPIKE":
        return snapshot["discount_rate_current"], snapshot["discount_rate_baseline"], snapshot["discount_rate_delta"]
    if alert_type == "DISCOUNT_OUTPACING_SALES":
        return snapshot["current"]["discount_amount"], snapshot["baseline"]["discount_amount"], snapshot["discount_amount_delta"]
    if alert_type == "USAGE_RATE_SPIKE":
        return snapshot["usage_rate_current"], snapshot["usage_rate_baseline"], snapshot["usage_rate_delta"]
    if alert_type == "NEW_CODE" and new_codes:
        top = new_codes[0]
        return top["orders_share_pct"], 0, None
    return None, None, None


def process_brand(brand_name: str, db_name: str):
    """Returns ('evaluated', result_dict) / ('skipped', brand_name).

    'evaluated' covers every brand with enough orders to compute a snapshot --
    including ones with zero breaches this run. The Node-side state machine
    needs that "currently clean" signal every run to detect a recovery
    transition, not just a new breach (see run_monitor()/alertEngine.js).
    Cooldown filtering is gone: every raw breach is reported and stored;
    whether an email actually fires is entirely the Node state machine's call.
    """
    conn = None
    try:
        conn = get_connection(db_name)
        ensure_alerts_table(conn)

        snapshot = build_snapshot(conn, brand_name)
        if snapshot is None:
            return "skipped", brand_name

        alerts_fired, raw_eval = decide_alerts(snapshot)

        new_codes = new_code_check(conn, snapshot["current"])
        if new_codes:
            alerts_fired = alerts_fired + ["NEW_CODE"]

        for alert_type in alerts_fired:
            current_val, baseline_val, delta = _metric_for_alert_type(snapshot, alert_type, new_codes)
            store_alert(
                conn, brand_name, alert_type,
                current_val or 0, baseline_val or 0, delta,
                f"{alert_type} for {brand_name}",
            )

        if not alerts_fired:
            log.info("[%s] No alerts this run.", brand_name)
            result = {
                "snapshot": snapshot,
                "alerts_fired": [],
                "raw_eval": raw_eval,
                "top_code": None,
                "new_codes": [],
                "utm_rows": [],
                "flagged_sources": [],
                "kpis": None,
            }
            return "evaluated", result

        code = top_code(conn, snapshot["current"])
        utm_rows, flagged_sources = utm_source_breakdown(conn)
        for fs in flagged_sources:
            fs["drill"] = campaign_drill(conn, fs["source"])

        result = {
            "snapshot": snapshot,
            "alerts_fired": alerts_fired,
            "raw_eval": raw_eval,
            "top_code": code,
            "new_codes": new_codes if "NEW_CODE" in alerts_fired else [],
            "utm_rows": utm_rows,
            "flagged_sources": flagged_sources,
            "kpis": fetch_kpis(conn),
        }
        return "evaluated", result

    except Exception as exc:
        log.error("[%s] Failed: %s", brand_name, exc, exc_info=True)
        return "skipped", brand_name
    finally:
        if conn is not None and conn.is_connected():
            conn.close()


def run_monitor(brand_filter=None, dry_run=False, json_output=False):
    run_time = datetime.now()
    log.info("=" * 60)
    log.info("Discount Monitor started - %s", run_time.strftime("%Y-%m-%d %H:%M:%S"))
    log.info("=" * 60)

    brands_to_run = (
        [b for b in BRANDS if b["name"] in brand_filter] if brand_filter else BRANDS
    )

    flagged_results = []
    normal_brands = []
    skipped_brands = []
    evaluated_results = []  # every evaluated brand, flagged or not -- for Node's state machine

    for brand in brands_to_run:
        log.info("\n[%s] Processing...", brand["name"])
        status, payload = process_brand(brand["name"], brand["db"])
        if status == "evaluated":
            payload["snapshot"]["brand"] = brand["name"]
            evaluated_results.append(payload)
            if payload["alerts_fired"]:
                flagged_results.append(payload)
            else:
                normal_brands.append(brand["name"])
        else:
            skipped_brands.append(brand["name"])

    digest = format_digest(run_time, flagged_results, normal_brands)
    if skipped_brands:
        digest += "\nSkipped (low traffic / error): " + ", ".join(skipped_brands)

    total_alerts = sum(len(r["alerts_fired"]) for r in flagged_results)

    if json_output:
        payload = {
            "event_type": "discount_monitor.run",
            "source": "discount_monitor",
            "run_date": run_time.strftime("%Y-%m-%d"),
            "run_time": run_time.strftime("%Y-%m-%d %H:%M:%S"),
            "dry_run": dry_run,
            "brand_count": len(brands_to_run),
            "flagged_count": len(flagged_results),
            "normal_count": len(normal_brands),
            "skipped_count": len(skipped_brands),
            "total_alerts": total_alerts,
            "digest_text": digest,
            "flagged_brands": [r["snapshot"]["brand"] for r in flagged_results],
            "normal_brands": normal_brands,
            "skipped_brands": skipped_brands,
            # Every evaluated brand, not just flagged ones -- Node's state
            # machine needs every brand's raw_eval every run to detect
            # recoveries, not just new breaches.
            "brand_results": [_brand_result_payload(r) for r in evaluated_results],
        }
        log.info(
            "Discount monitor JSON payload prepared: brands=%d flagged=%d alerts=%d dry_run=%s",
            len(brands_to_run), len(flagged_results), total_alerts, dry_run,
        )
        print(json.dumps(_json_safe(payload)))
    else:
        print_digest(digest)
        if dry_run:
            log.info("Dry-run mode: digest email suppressed.")
        else:
            send_digest_email(digest, len(flagged_results))

    log.info("Discount Monitor finished.\n")


if __name__ == "__main__":
    args = parse_args()

    if args.brands:
        known = {b["name"] for b in BRANDS}
        unknown = [b for b in args.brands if b not in known]
        if unknown:
            log.error("Unknown brand(s): %s. Valid options: %s", unknown, sorted(known))
            sys.exit(1)

    run_monitor(brand_filter=args.brands, dry_run=args.dry_run, json_output=args.json_output)
