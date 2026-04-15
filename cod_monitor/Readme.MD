# COD vs Prepaid Monitoring System

Automated day-over-day change detection for COD % across all brands — at both overall store level and per-product level.

---

## Project Structure

```
cod_monitor/
├── main.py           ← Entry point, parallel execution, CLI
├── monitor.py        ← Core processing engine per brand
├── queries.py        ← All SQL (overall DoD + product DoD)
├── db.py             ← MySQL connection wrapper
├── alerts.py         ← Console / Slack / Email output
├── config.py         ← Brand registry + thresholds + notification settings
└── requirements.txt
```

---

## Architecture

```
Multiple Shopify DBs (TMC, BBB, ...)
         ↓
   [ThreadPoolExecutor]        ← brands run in parallel
         ↓
   monitor.py per brand
     ├── OVERALL_DOD_QUERY     ← overall_summary table
     └── product_dod_query()   ← shopify_orders table
         ↓
   Threshold check
     ├── |delta| > overall_threshold  →  overall alert
     └── |delta| > product_threshold  →  product alert
         ↓
   alerts.py
     ├── Console (always)
     ├── Slack   (if SLACK_WEBHOOK_URL set)
     └── Email   (if EMAIL_CONFIG.enabled = True)
```

---

## Payment Classification

| Pattern in `payment_gateway_names`          | Type     |
|---------------------------------------------|----------|
| `%Gokwik PPCOD%`                            | Partial  |
| NULL / empty / `%Cash on Delivery (COD)%` / `%cash_on_delivery%` | COD |
| Anything else (Gokwik Cards, UPI, Razorpay…)| Prepaid  |

---

## Setup

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Configure brands
Edit `config.py` — add each brand with its DB credentials and thresholds:

```python
BRANDS = {
    "TMC": {
        "db": {
            "host": "your-db-host",
            "port": 3306,
            "user": "your-user",
            "password": "your-password",
            "database": "tmc_db",
        },
        "thresholds": {
            "overall": 8.0,   # % absolute DoD change
            "product": 15.0,
        },
        "product_min_orders": 20,
        "product_ids": [],    # [] = all products
    },
}
```

### 3. (Optional) Enable Slack alerts
```python
SLACK_WEBHOOK_URL = "https://hooks.slack.com/services/xxx/yyy/zzz"
```

### 4. (Optional) Enable Email alerts
```python
EMAIL_CONFIG = {
    "enabled": True,
    "smtp_host": "smtp.gmail.com",
    "smtp_port": 587,
    "sender": "alerts@yourcompany.com",
    "password": "your-app-password",
    "recipients": ["team@yourcompany.com"],
}
```

---

## Running

```bash
# All brands
python main.py

# Specific brands only
python main.py --brands TMC

# Dry run (no Slack/email)
python main.py --dry-run

# Multiple specific brands
python main.py --brands TMC BBB
```

### Sample Console Output

```
============================================================
  🔍  COD vs PREPAID MONITOR  |  2026-04-08
============================================================
  🚨  ALERT SUMMARY

  📊  [TMC]  today COD 61.3%  (Δ +12.3%)  | orders: 234
    ⚠️   📈 Overall COD % change: +12.3% (today 61.3% ← yesterday 49.0% | orders today: 234)

  📊  [BBB]  today COD 55.1%  (Δ +3.1%)  | orders: 189
    ✅  No alerts

============================================================
```

---

## Scheduling (Cron)

Run every day at 9:00 AM:

```bash
# Edit crontab
crontab -e

# Add this line (adjust paths)
0 9 * * * /usr/bin/python3 /path/to/cod_monitor/main.py >> /var/log/cod_monitor.log 2>&1
```

### Windows Task Scheduler

```
Program:   python.exe
Arguments: C:\path\to\cod_monitor\main.py
Trigger:   Daily at 09:00
```

---

## Thresholds (Reference)

| Level    | Default | Rationale                           |
|----------|---------|-------------------------------------|
| Overall  | 8–10%   | Store-wide behavioral shift         |
| Product  | 12–20%  | Higher variance expected per SKU    |

Tune per brand in `config.py`.

---

## Noise Filters

- **`product_min_orders`** — skip products with fewer than N orders (default: 20). Prevents low-volume SKUs from triggering false alerts.
- **`product_ids`** — optional whitelist of specific product IDs to monitor. Empty = all products.

---

## Future Enhancements

- [ ] Store alerts to DB (alert history table)
- [ ] AOV / Conversion monitoring
- [ ] Weekly trend summary emails
- [ ] Looker Studio dashboard integration
- [ ] Anomaly scoring (Z-score based) instead of fixed thresholds