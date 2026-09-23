

# =============================================================
# config.py - All settings in one place. Edit this file only.
# =============================================================

from __future__ import annotations

import os
from pathlib import Path


def _load_root_env() -> None:
    """Load simple KEY=VALUE pairs from the repo root .env into os.environ."""
    env_path = Path(__file__).resolve().parent.parent / ".env"
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'").strip('"')

        if key and key not in os.environ:
            os.environ[key] = value


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


_load_root_env()

# -- Database --------------------------------------------------
DB_HOST = _require_env("DISCOUNT_MONITOR_DB_HOST")
DB_PORT = int(os.getenv("DISCOUNT_MONITOR_DB_PORT", "3306"))
DB_USER = _require_env("DISCOUNT_MONITOR_DB_USER")
DB_PASSWORD = _require_env("DISCOUNT_MONITOR_DB_PASS")

ALERT_ENGINE_DISCOUNT_MONITOR_URL = os.getenv(
    "DISCOUNT_MONITOR_ALERT_ENGINE_URL",
    f"http://localhost:{os.getenv('PORT', '5000')}/discount-monitor/results",
)

# -- Brands: add/remove as needed -----------------------------
# NOTE: TMC is discount/bundle-led by design (very high baseline usage,
# and discount_codes are auto-generated per-order rather than clean
# campaign codes). Thresholds below adapt to each brand's own baseline,
# so this isn't hardcoded per-brand -- but keep it in mind when reading
# TMC's numbers vs the others. TMC is intentionally excluded below --
# it's not monitored or emailed for now.
BRANDS = [
    {"name": "BBB", "db": "BBB"},
    # {"name": "TMC", "db": "TMC"},
    {"name": "PTS", "db": "PTS"},
    {"name": "VAMA", "db": "VAMA"},
    {"name": "AJMAL", "db": "AJMAL"},
]

# -- Detection Thresholds -------------------------------------
# Comparison basis: "today so far" (midnight to now) vs the same
# midnight-to-[same clock time] cutoff on each of the prior N days,
# averaged. This tracks your live Shopify "today" dashboard directly,
# and a real anomaly stays visible in the day-to-date rate rather than
# getting diluted by a fixed short window.
THRESHOLDS = {
    "baseline_days": 7,          # how many prior days to average for baseline
    "min_total_orders": 50,      # skip a brand's run if fewer orders than this so far today

    # Alert 1: discount rate spike (discount_amount / gross_sales)
    # fires when current discount rate is this much HIGHER than baseline,
    # relative: (current - baseline) / baseline * 100
    "discount_rate_delta_pct": 25,

    # Alert 2: discount amount growing faster than sales
    # both growth numbers use the same (current-baseline)/baseline*100 formula.
    # fires when discount growth exceeds sales growth by at least this gap,
    # AND discount growth itself is at least this significant (avoids noise
    # when both are basically flat).
    "outpace_gap_pct": 15,
    "outpace_min_discount_growth_pct": 15,

    # Alert 3: discount USAGE rate spike (discounted_orders / total_orders)
    # distinct from Alert 1 -- a brand can have flat/lower discount AMOUNT
    # while more orders are using a code (smaller discounts, wider net).
    # Caught the AJMAL 9-Jun case: usage rate +20.5% vs baseline while
    # discount amount was actually flat/down -- Alert 1 alone missed it.
    "usage_rate_delta_pct": 20,

    # Alert 4: a discount code with (near) zero baseline presence suddenly
    # taking a real share of today's discounted orders.
    "new_code_min_share_pct": 15,   # % of today's discounted orders

    # Top discount code
    # if unique codes in the window make up more than this share of
    # discounted orders, codes are likely auto-generated per order (like
    # TMC) rather than real campaign codes -- skip the "top code" line
    # instead of showing 100+ near-1%-share noise.
    "code_cardinality_guard_ratio": 0.15,

    # UTM source flagging
    "utm_min_current_share": 5,       # ignore sources below this % of today's discount
    "utm_uplift_delta_pct": 30,       # relative uplift in share vs baseline to flag
    "utm_max_flagged": 2,             # cap how many sources get a full drill-down

    # when a code/product split by order-count differs from its amount
    # share by more than this many percentage points, show both numbers;
    # otherwise show one blended (amount) number to avoid clutter
    "divergence_pp_to_split": 15,

    "alert_cooldown_hrs": 1,      # same brand+alert_type won't re-notify within this window
}

ENABLED_ALERT_TYPES = {
    "DISCOUNT_RATE_SPIKE": True,
    "DISCOUNT_OUTPACING_SALES": True,
    "USAGE_RATE_SPIKE": True,
    "NEW_CODE": True,
}

# -- Email Alerts ---------------------------------------------
# Kept disabled -- the Node alert engine owns email delivery when this
# script is run via --json-output (see run.py / discountMonitorScheduler.js).
# These remain available as an optional standalone fallback (e.g. running
# run.py directly from cron without the Node integration).
EMAIL_ENABLED = os.getenv("DISCOUNT_MONITOR_EMAIL_ENABLED", "false").strip().lower() == "true"
SMTP_HOST = os.getenv("DISCOUNT_MONITOR_SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("DISCOUNT_MONITOR_SMTP_PORT", "587"))
SMTP_USER = os.getenv("DISCOUNT_MONITOR_SMTP_USER", "")
SMTP_PASSWORD = os.getenv("DISCOUNT_MONITOR_SMTP_PASSWORD", "")
EMAIL_FROM = os.getenv("DISCOUNT_MONITOR_EMAIL_FROM", "")
EMAIL_TO = [
    addr.strip()
    for addr in os.getenv("DISCOUNT_MONITOR_EMAIL_TO", "").split(",")
    if addr.strip()
]
