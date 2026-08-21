"""
alerts.py - Alert delivery layer.

Supports:
  - Console (always on)
  - Slack  (optional, via webhook)
  - Email  (optional, via SMTP)
"""

from __future__ import annotations

import json
import logging
import smtplib
import urllib.request
import urllib.error
from datetime import date
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

logger = logging.getLogger(__name__)

TODAY_STR = date.today().strftime("%Y-%m-%d")


def _fmt_delta(delta) -> str:
    if delta is None:
        return "N/A"
    sign = "+" if delta > 0 else ""
    return f"{sign}{delta}%"


def _fmt_value(value) -> str:
    return "N/A" if value is None else str(value)


def _payment_line(
    label: str,
    today_orders,
    today_pct,
    baseline_pct,
    delta_pct,
) -> str:
    return (
        f"    {label:<8} orders {_fmt_value(today_orders):>6} | "
        f"today {_fmt_value(today_pct):>6}% | "
        f"7d avg {_fmt_value(baseline_pct):>6}% | "
        f"delta {_fmt_delta(delta_pct):>7}"
    )


def _spike_summary(row: dict) -> list[str]:
    spikes = []
    for label, delta_key, today_key, baseline_key in (
        ("COD", "delta_cod_pct", "today_cod_pct", "baseline_cod_pct"),
        ("Prepaid", "delta_prepaid_pct", "today_prepaid_pct", "baseline_prepaid_pct"),
        ("Partial", "delta_partial_pct", "today_partial_pct", "baseline_partial_pct"),
    ):
        delta = row.get(delta_key)
        if delta is None or delta <= 0:
            continue
        spikes.append(
            f"    {label} spike -> current {_fmt_value(row.get(today_key))}% "
            f"vs 7d avg {_fmt_value(row.get(baseline_key))}% "
            f"({ _fmt_delta(delta) })"
        )
    return spikes


def _product_alert_lines(alerts: list[dict]) -> list[str]:
    product_alerts = [a for a in alerts if a.get("level") == "product"]
    if not product_alerts:
        return []

    lines = ["  Product-level spikes:"]
    for alert in product_alerts:
        lines.append(f"    - {alert['message']}")
    return lines


def _product_summary_lines(product_rows: list[dict], alerts: list[dict]) -> list[str]:
    if not product_rows:
        return ["  Product watchlist: no qualifying product rows found for today."]

    alert_ids = {
        str(alert.get("product_id"))
        for alert in alerts
        if alert.get("level") == "product" and alert.get("product_id") is not None
    }

    lines = ["  Product watchlist summary:"]
    for row in product_rows:
        product_name = row.get("product_name") or "-"
        product_id = row.get("product_id", "unknown")
        spike_flag = "YES" if str(product_id) in alert_ids else "NO"
        lines.append(
            "    - "
            f"{product_name} (ID: {product_id}) | "
            f"orders {_fmt_value(row.get('today_total_orders'))} | "
            f"COD {_fmt_value(row.get('today_cod_pct'))}% vs 7d {_fmt_value(row.get('baseline_cod_pct'))}% | "
            f"Prepaid {_fmt_value(row.get('today_prepaid_pct'))}% | "
            f"Partial {_fmt_value(row.get('today_partial_pct'))}% | "
            f"COD delta {_fmt_delta(row.get('delta_cod_pct'))} | "
            f"Spike {spike_flag}"
        )
    return lines


def print_console_report(brand_results: list[dict]) -> None:
    """Pretty-print full alert summary to stdout."""
    any_alert = any(r["alerts"] for r in brand_results)

    print("\n" + "=" * 92)
    print(f"COD vs PREPAID MONITOR | {TODAY_STR}")
    print("=" * 92)

    if any_alert:
        print("Alert summary: spikes detected in one or more brands.\n")
    else:
        print("Alert summary: no threshold-breaking spikes detected.\n")

    for result in brand_results:
        brand = result["brand"]

        if result["status"] == "error":
            print(f"[{brand}] ERROR: {result['error']}\n")
            continue

        row = result.get("overall_row")
        if not row:
            print(f"[{brand}]")
            print("  No overall data available.\n")
            continue

        print(f"[{brand}]")
        print(f"  Today total orders : {_fmt_value(row.get('today_total_orders'))}")
        print(f"  Baseline days used : {_fmt_value(row.get('baseline_days'))}")
        print("  Split summary:")
        print(
            _payment_line(
                "COD",
                row.get("today_cod_orders"),
                row.get("today_cod_pct"),
                row.get("baseline_cod_pct"),
                row.get("delta_cod_pct"),
            )
        )
        print(
            _payment_line(
                "Prepaid",
                row.get("today_prepaid_orders"),
                row.get("today_prepaid_pct"),
                row.get("baseline_prepaid_pct"),
                row.get("delta_prepaid_pct"),
            )
        )
        print(
            _payment_line(
                "Partial",
                row.get("today_partial_orders"),
                row.get("today_partial_pct"),
                row.get("baseline_partial_pct"),
                row.get("delta_partial_pct"),
            )
        )

        spike_lines = _spike_summary(row)
        if spike_lines:
            print("  Brand-level spikes:")
            for line in spike_lines:
                print(line)
        else:
            print("  Brand-level spikes: none")

        overall_alerts = [a for a in result["alerts"] if a.get("level") == "overall"]
        if overall_alerts:
            print("  Alert triggered:")
            for alert in overall_alerts:
                print(f"    - {alert['message']}")
        else:
            print("  Alert triggered: none")

        for line in _product_summary_lines(result.get("product_rows", []), result["alerts"]):
            print(line)

        for line in _product_alert_lines(result["alerts"]):
            print(line)

        print("")

    print("=" * 92 + "\n")


def send_alert_engine_payload(run_payload: dict, endpoint_url: str) -> None:
    """Forward the COD monitor run payload to the alert engine as JSON."""
    if not endpoint_url:
        logger.info("Alert engine endpoint not configured; skipping COD payload forward.")
        return

    payload = json.dumps(run_payload).encode("utf-8")
    req = urllib.request.Request(
        endpoint_url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            logger.info("Alert engine payload sent: HTTP %s", resp.status)
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        logger.error(
            "Alert engine payload failed: HTTP %s | response=%s",
            exc.code,
            body,
        )
    except Exception as exc:  # noqa: BLE001
        logger.error("Alert engine payload send failed: %s", exc)


def send_slack_alert(brand_results: list[dict], webhook_url: str) -> None:
    """Post a consolidated Slack message via incoming webhook."""
    if not webhook_url:
        return

    blocks = []

    blocks.append(
        {
            "type": "header",
            "text": {"type": "plain_text", "text": f"COD Monitor | {TODAY_STR}"},
        }
    )

    any_alert = False
    for result in brand_results:
        brand = result["brand"]

        if result["status"] == "error":
            blocks.append(
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": f"*{brand}* - DB error: {result['error']}"},
                }
            )
            continue

        brand_alerts = result["alerts"]
        if not brand_alerts:
            continue

        any_alert = True
        lines = [f"*{brand}*"]
        for alert in brand_alerts:
            lines.append(f"  - {alert['message']}")

        blocks.append({"type": "divider"})
        blocks.append(
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": "\n".join(lines)},
            }
        )

    if not any_alert:
        blocks.append(
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": "No anomalies detected across all brands."},
            }
        )

    payload = json.dumps({"blocks": blocks}).encode("utf-8")
    req = urllib.request.Request(
        webhook_url,
        data=payload,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            logger.info("Slack alert sent: HTTP %s", resp.status)
    except Exception as exc:  # noqa: BLE001
        logger.error("Slack send failed: %s", exc)


def send_email_alert(brand_results: list[dict], email_cfg: dict) -> None:
    """Send alert summary via SMTP."""
    if not email_cfg.get("enabled"):
        return

    subject = f"[COD Monitor] Alert Summary - {TODAY_STR}"
    html_lines = [
        "<html><body>",
        f"<h2>COD vs Prepaid Monitor | {TODAY_STR}</h2>",
    ]

    any_alert = False
    for result in brand_results:
        brand = result["brand"]
        if result["status"] == "error":
            html_lines.append(f"<p><b>{brand}</b>: DB error - {result['error']}</p>")
            continue

        if not result["alerts"]:
            continue

        any_alert = True
        html_lines.append(f"<h3>{brand}</h3><ul>")
        for alert in result["alerts"]:
            html_lines.append(f"<li>{alert['message']}</li>")
        html_lines.append("</ul>")

    if not any_alert:
        html_lines.append("<p>No anomalies detected across all brands.</p>")

    html_lines.append("</body></html>")

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = email_cfg["sender"]
    msg["To"] = ", ".join(email_cfg["recipients"])
    msg.attach(MIMEText("\n".join(html_lines), "html"))

    try:
        with smtplib.SMTP(email_cfg["smtp_host"], email_cfg["smtp_port"]) as server:
            server.ehlo()
            server.starttls()
            server.login(email_cfg["sender"], email_cfg["password"])
            server.sendmail(
                email_cfg["sender"],
                email_cfg["recipients"],
                msg.as_string(),
            )
        logger.info("Email alert sent to %s", email_cfg["recipients"])
    except Exception as exc:  # noqa: BLE001
        logger.error("Email send failed: %s", exc)
