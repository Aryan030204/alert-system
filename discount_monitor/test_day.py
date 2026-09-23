# =============================================================
# test_day.py - Ad hoc: replay the SAME digest run.py produces, but
# as-of a specific past day instead of "today so far".
#
# Uses the real pipeline (detect.py + format.py) via the `as_of`
# param those functions now accept -- so the output is identical in
# shape to a normal run. This is a throwaway helper: run.py never
# imports it, and it does NOT write to discount_alerts or check
# cooldown (a backtest shouldn't pollute alert history or get
# suppressed by it).
#
# Usage:
#   python test_day.py
# =============================================================

from datetime import datetime

from config import BRANDS
from db import get_connection
from detect import (
    build_snapshot,
    decide_alerts,
    top_code,
    new_code_check,
    utm_source_breakdown,
    campaign_drill,
)
from format import format_digest

TARGET_DATE = "2026-06-09"          # <-- change this to the day you want to test
AS_OF = f"{TARGET_DATE} 23:59:59"   # replay as the full day (midnight to end of day)


def process_brand(brand_name: str, db_name: str):
    conn = None
    try:
        conn = get_connection(db_name)
        snapshot = build_snapshot(conn, brand_name, as_of=AS_OF)
        if snapshot is None:
            return "skipped", brand_name

        alerts_fired = decide_alerts(snapshot)

        new_codes = new_code_check(conn, snapshot["current"], as_of=AS_OF)
        if new_codes:
            alerts_fired = alerts_fired + ["NEW_CODE"]

        if not alerts_fired:
            return "normal", brand_name

        code = top_code(conn, snapshot["current"], as_of=AS_OF)
        utm_rows, flagged_sources = utm_source_breakdown(conn, as_of=AS_OF)
        for fs in flagged_sources:
            fs["drill"] = campaign_drill(conn, fs["source"], as_of=AS_OF)

        result = {
            "snapshot": snapshot,
            "alerts_fired": alerts_fired,
            "top_code": code,
            "new_codes": new_codes,
            "utm_rows": utm_rows,
            "flagged_sources": flagged_sources,
        }
        return "flagged", result
    except Exception as exc:
        print(f"[{brand_name}] Failed: {exc}")
        return "skipped", brand_name
    finally:
        if conn is not None and conn.is_connected():
            conn.close()


def main():
    run_time = datetime.strptime(TARGET_DATE, "%Y-%m-%d")
    flagged_results, normal_brands, skipped_brands = [], [], []

    for brand in BRANDS:
        status, payload = process_brand(brand["name"], brand["db"])
        if status == "flagged":
            payload["snapshot"]["brand"] = brand["name"]
            flagged_results.append(payload)
        elif status == "normal":
            normal_brands.append(brand["name"])
        else:
            skipped_brands.append(brand["name"])

    digest = format_digest(run_time, flagged_results, normal_brands)
    if skipped_brands:
        digest += "\nSkipped (low traffic / error): " + ", ".join(skipped_brands)

    print(f"[TEST -- replaying {TARGET_DATE}, nothing written to discount_alerts]\n")
    print(digest)


if __name__ == "__main__":
    main()