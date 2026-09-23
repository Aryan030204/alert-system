# =============================================================
# format.py - Renders the multi-brand digest text.
#
# Only flagged brands get the full block; everything else collapses
# into one "Normal: ..." line so a 7-12 brand run stays scannable.
#
# ASCII-only output on purpose: Windows terminals default to cp1252,
# which can't encode rupee signs, em dashes, arrows or emoji -- that
# crashed logging on a prior run. Don't reintroduce non-ASCII here.
# =============================================================

from config import THRESHOLDS


def fmt_amount(x: float) -> str:
    """Compact INR amount: lakh/crore shorthand for readability."""
    sign = "-" if x < 0 else ""
    x = abs(x)
    if x >= 1e7:
        return f"{sign}Rs {x/1e7:.2f}Cr"
    if x >= 1e5:
        return f"{sign}Rs {x/1e5:.2f}L"
    return f"{sign}Rs {x:,.0f}"


def fmt_delta(x) -> str:
    if x is None:
        return "new"
    sign = "+" if x >= 0 else ""
    return f"{sign}{x:.0f}%"


def fmt_pct(x) -> str:
    return f"{x:.1f}%"


def _code_line(item: dict) -> str:
    """One code/product's share -- blended amount% unless order-count
    diverges enough to be worth calling out separately."""
    if item.get("diverges"):
        return f"{item['code']} {item['amount_share_pct']:.0f}% amt ({item['orders_share_pct']:.0f}% orders)"
    return f"{item['code']} {item['amount_share_pct']:.0f}%"


def format_brand_block(result: dict) -> str:
    s = result["snapshot"]
    lines = []
    hard_alerts = {"DISCOUNT_RATE_SPIKE", "USAGE_RATE_SPIKE", "NEW_CODE"}
    tag = "[ALERT]" if hard_alerts & set(result["alerts_fired"]) else "[WATCH]"
    lines.append(f"{tag} {s['brand']}")

    lines.append(
        f"Discount amount   {fmt_amount(s['current']['discount_amount'])}   "
        f"(baseline {fmt_amount(s['baseline']['discount_amount'])})   "
        f"delta {fmt_delta(s['discount_amount_delta'])}"
    )
    lines.append(
        f"Sales             {fmt_amount(s['current']['gross_sales'])}   "
        f"(baseline {fmt_amount(s['baseline']['gross_sales'])})   "
        f"delta {fmt_delta(s['sales_delta'])}"
    )
    lines.append(
        f"Discount rate     {fmt_pct(s['discount_rate_current'])}   "
        f"(baseline {fmt_pct(s['discount_rate_baseline'])})   "
        f"delta {fmt_delta(s['discount_rate_delta'])}"
    )
    lines.append(
        f"Discount usage    {fmt_pct(s['usage_rate_current'])} of orders   "
        f"(baseline {fmt_pct(s['usage_rate_baseline'])})   "
        f"delta {fmt_delta(s['usage_rate_delta'])}"
    )
    if "DISCOUNT_OUTPACING_SALES" in result["alerts_fired"]:
        lines.append("  -> discount amount rising far faster than sales")
    if "USAGE_RATE_SPIKE" in result["alerts_fired"]:
        lines.append("  -> more orders using a discount, even if the amount looks flat")

    if result.get("new_codes"):
        lines.append("")
        for nc in result["new_codes"][:3]:
            lines.append(
                f"New code   {nc['code']} - {nc['orders_share_pct']:.0f}% of today's discounted orders "
                f"(0 in the last {THRESHOLDS['baseline_days']} days)"
            )

    if result.get("top_code"):
        tc = result["top_code"]
        line = f"{tc['code']} - {tc['orders_share_pct']:.0f}% of discounted orders"
        if abs(tc["amount_share_pct"] - tc["orders_share_pct"]) >= THRESHOLDS["divergence_pp_to_split"]:
            line += f", {tc['amount_share_pct']:.0f}% of discount amount"
        lines.append("")
        lines.append(f"Top code : {line}")

    if result.get("utm_rows"):
        lines.append("")
        lines.append("UTM source share of today's discount (baseline -> today, delta)")
        for r in result["utm_rows"][:5]:
            flag = "  <FLAGGED>" if r["flagged"] else ""
            lines.append(
                f"  {r['source']:<10} {fmt_pct(r['baseline_share'])} -> "
                f"{fmt_pct(r['current_share'])}   delta {fmt_delta(r['delta_pct'])}{flag}"
            )

    for fs in result.get("flagged_sources", []):
        drill = fs.get("drill")
        if not drill:
            continue
        lines.append("")
        lines.append(
            f"-> {fs['source']} / campaign {drill['campaign']} - "
            f"{fmt_pct(drill['baseline_share'])} -> {fmt_pct(drill['current_share'])} "
            f"of {fs['source']}'s discount   delta {fmt_delta(drill['delta_pct'])}"
        )
        if drill["codes"]:
            codes_str = ", ".join(_code_line(c) for c in drill["codes"])
            lines.append(f"    codes   {codes_str}")

    return "\n".join(lines)


def format_digest(run_time, flagged_results: list, normal_brands: list) -> str:
    total = len(flagged_results) + len(normal_brands)
    header = [
        f"DISCOUNT SNAPSHOT - {run_time.strftime('%d %b %Y, %H:%M')}",
        f"{total} brands checked | {len(flagged_results)} flagged | {len(normal_brands)} normal",
        "-" * 50,
    ]
    body = []
    for result in flagged_results:
        body.append(format_brand_block(result))
        body.append("")

    footer = ["-" * 50]
    if normal_brands:
        footer.append("Normal: " + ", ".join(normal_brands))

    return "\n".join(header + body + footer)