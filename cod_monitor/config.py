"""
config.py — Brand/tenant configuration for COD vs Prepaid Monitor
"""

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

DB_HOST = _require_env("MASTER_DB_HOST")
DB_USER = _require_env("MASTER_DB_USER")
DB_PASSWORD = _require_env("MASTER_DB_PASS")
DB_PORT = int(os.getenv("MASTER_DB_PORT", "3306"))
ALERT_ENGINE_COD_MONITOR_URL = os.getenv(
    "COD_MONITOR_ALERT_ENGINE_URL",
    f"http://localhost:{os.getenv('PORT', '5000')}/cod-monitor/results",
)

BRANDS = {
    "PTS": {
        "db": {
            "host":     DB_HOST,
            "port":     DB_PORT,
            "user":     DB_USER,
            "password": DB_PASSWORD,
            "database": "PTS",
        },
        "thresholds": {
            "overall": 3.0,
            "product": 10.0,
        },
        "product_min_orders": 20,
        "product_ids": [10097371775251,
                        8047927558419,
                        8047927558419,
                        8726032023827,
                        8047927558419],  # add your PTS product IDs here
    },

    "TMC": {
        "db": {
            "host":     DB_HOST,
            "port":     DB_PORT,
            "user":     DB_USER,
            "password": DB_PASSWORD,
            "database": "TMC",
        },
        "thresholds": {
            "overall": 3.0,
            "product": 10.0,
        },
        "product_min_orders": 20,
        "product_ids": [8417496367300,
                        8384621805764,
                        8374355493060,
                        7207918698692,
                        7207918895300],  # add your TMC product IDs here
    },

    "AJMAL": {
         "db": {
            "host":     DB_HOST,
            "port":     DB_PORT,
            "user":     DB_USER,
            "password": DB_PASSWORD,
            "database": "AJMAL",
        },
        "thresholds": {
            "overall": 3.0,
            "product": 10.0,
        },
        "product_min_orders": 20,
        "product_ids": [8058959921322,
                        8048126820522,
                        8010225516714,
                        7987789889706,
                        8308792164522],  # add your AJMAL product IDs here
    },

    "BBB": {
         "db": {
            "host":     DB_HOST,
            "port":     DB_PORT,
            "user":     DB_USER,
            "password": DB_PASSWORD,
            "database": "BBB",
        },
        "thresholds": {
            "overall": 3.0,
            "product": 10.0,
        },
        "product_min_orders": 20,
         "product_ids": [9870268498215,
                         9996616466727,
                         9863098564903,
                         9946307690791,
                         9870267023655],  # add your BBB product IDs here
    },

    "VAMA": {
         "db": {
            "host":     DB_HOST,
            "port":     DB_PORT,
            "user":     DB_USER,
            "password": DB_PASSWORD,
            "database": "VAMA",
        },
        "thresholds": {
            "overall": 3.0,
            "product": 10.0,
        },
        "product_min_orders": 20,
        "product_ids": [8622333100210,
                        8715135189170,
                        8740073111730,
                        8570090062002,
                        8657193566386],  # add your VAMA product IDs here
    },
}

# ---------------------------------------------------------------------------
# Notifications — leave blank for now, alerts will print to console only
# ---------------------------------------------------------------------------
SLACK_WEBHOOK_URL = ""

EMAIL_CONFIG = {
    "enabled": False,
}

# ---------------------------------------------------------------------------
# Parallel execution
# ---------------------------------------------------------------------------
MAX_WORKERS = 5
