# Discount Monitor v2 — Multi-Brand Digest

One alert run across all brands, printed as a single scannable digest instead
of a per-brand message. Only brands with a real anomaly get a full
drill-down block; the rest collapse into one "Normal: ..." line.

---

## Setup

```bash
pip install mysql-connector-python
```

Edit `config.py`: DB credentials, brand list, thresholds. Then:

```bash
python run.py
```

Schedule with cron (every 10-30 min):
```bash
*/15 * * * * cd /path/to/discount_monitor && python run.py
```

---

## Alert types (kept to the two that matter)

| Alert | Trigger |
|---|---|
| **DISCOUNT_RATE_SPIKE** | `discount_amount / gross_sales` today is up ≥25% (relative) vs its 7-day same-window baseline |
| **DISCOUNT_OUTPACING_SALES** | discount amount growth is meaningfully ahead of sales growth (both computed the same way) — margin give-away accelerating faster than revenue |

**Every delta everywhere** = `(current - baseline) / baseline * 100`. No percentage-point deltas.
`gross_sales` = `SUM(total_price)` from `shopify_orders` (order-level field, one row per order) — not `overall_summary.total_sales`, which on inspection didn't behave as a discount-rate denominator (values came out far smaller than gross/net sales for at least one brand — verify before using it anywhere else).

## Drill-down (only runs for a flagged brand)

1. Top discount code overall, by discount amount — **skipped automatically** if the brand's codes look auto-generated per order rather than real campaign codes (cardinality guard: unique codes > 15% of discounted orders). TMC hits this — its codes (e.g. `TMCFK4A499IRFP`) are per-order/bundle generated, not reusable campaign codes, so "top code" for TMC will often be blank by design, not a bug.
2. UTM source share of today's discount vs its usual (7-day) share — top 1-2 sources with real uplift get flagged.
3. For each flagged source → its top campaign (current vs baseline share of that source's discount).
4. Under that campaign → discount codes, blended as one amount-% number, split into amount% + orders% only when they diverge by ≥15pp (avoids clutter when they move together, which is most of the time).

No product-level breakdown: `line_item_total_discount` is unpopulated (0) across every brand checked, so there's no reliable way to split discount amount by product. Order/UTM/campaign/code all stay accurate because `discount_amount`, `utm_source`, `utm_campaign` are order-level fields (populated on exactly one line item per order).

---

## Files
```
discount_monitor/
├── config.py   ← credentials, brands, thresholds
├── db.py       ← connection, alerts table
├── fetch.py    ← current-window + baseline queries (generic grouped helpers)
├── detect.py   ← rate/growth math, alert rules, UTM+campaign+code drill-down
├── format.py   ← renders the digest text
├── notify.py   ← print/log + one batched email per run
└── run.py      ← main runner
```

## Notes / things worth re-checking against live data before trusting fully
- `discount_rate` denominator (`gross_sales`) — confirmed reasonable on TMC (~94% discount rate on `total_discount_amount`); worth a similar spot-check on BBB/AJMAL/VAMA/PTS since discount intensity varies a lot brand to brand (TMC is bundle/discount-led by design, so its baseline being consistently high is expected, not a bug).
- Cooldown (1hr default) is per brand+alert_type — a brand already flagged for the same alert type within the cooldown window is treated as "normal" for that run to avoid repeat noise; it's still logged to `discount_alerts` from the first firing.
- Email is off by default (`EMAIL_ENABLED = False`) — digest still prints/logs every run either way.
