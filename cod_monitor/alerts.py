"""
alerts.py — Alert delivery layer.

Supports:
  • Console (always on)
  • Slack  (optional, via webhook)
  • Email  (optional, via SMTP)
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


# ── Console ───────────────────────────────────────────────────────────────────

def print_console_report(brand_results: list[dict]) -> None:
    """Pretty-print full alert summary to stdout."""
    any_alert = any(r["alerts"] for r in brand_results)

    print("\n" + "=" * 60)
    print(f"  🔍  COD vs PREPAID MONITOR  |  {TODAY_STR}")
    print("=" * 60)

    if not any_alert:
        print("  ✅  No anomalies detected across all brands.\n")
    else:
        print("  🚨  ALERT SUMMARY\n")

    for result in brand_results:
        brand = result["brand"]

        if result["status"] == "error":
            print(f"\n  ❌  [{brand}]  ERROR: {result['error']}")
            continue

        # Always show the overall snapshot
        row = result.get("overall_row")
        if row:
            print(
                f"\n  📊  [{brand}]  "
                f"today COD {row.get('today_cod_pct', 'N/A')}%  "
                f"(Δ {_fmt_delta(row.get('delta_cod_pct'))})  "
                f"| orders: {row.get('today_total_orders', 'N/A')}"
            )
        else:
            print(f"\n  [{brand}]  No overall data available")

        if not result["alerts"]:
            print(f"    ✅  No alerts")
        else:
            for alert in result["alerts"]:
                print(f"    ⚠️   {alert['message']}")

    print("\n" + "=" * 60 + "\n")


def _fmt_delta(delta) -> str:
    if delta is None:
        return "N/A"
    sign = "+" if delta > 0 else ""
    return f"{sign}{delta}%"


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


# ── Slack ─────────────────────────────────────────────────────────────────────

def send_slack_alert(brand_results: list[dict], webhook_url: str) -> None:
    """Post a consolidated Slack message via incoming webhook."""
    if not webhook_url:
        return

    blocks = []

    # Header
    blocks.append({
        "type": "header",
        "text": {"type": "plain_text", "text": f"🔍 COD Monitor | {TODAY_STR}"},
    })

    any_alert = False
    for result in brand_results:
        brand = result["brand"]

        if result["status"] == "error":
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"❌ *{brand}* — DB error: {result['error']}"},
            })
            continue

        brand_alerts = result["alerts"]
        if not brand_alerts:
            continue

        any_alert = True
        lines = [f"*{brand}*"]
        for a in brand_alerts:
            lines.append(f"  • {a['message']}")

        blocks.append({"type": "divider"})
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "\n".join(lines)},
        })

    if not any_alert:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": "✅ No anomalies detected across all brands."},
        })

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


# ── Email ─────────────────────────────────────────────────────────────────────

def send_email_alert(brand_results: list[dict], email_cfg: dict) -> None:
    """Send alert summary via SMTP."""
    if not email_cfg.get("enabled"):
        return

    subject = f"[COD Monitor] Alert Summary — {TODAY_STR}"
    html_lines = [
        "<html><body>",
        f"<h2>🔍 COD vs Prepaid Monitor | {TODAY_STR}</h2>",
    ]

    any_alert = False
    for result in brand_results:
        brand = result["brand"]
        if result["status"] == "error":
            html_lines.append(f"<p>❌ <b>{brand}</b>: DB error — {result['error']}</p>")
            continue

        if not result["alerts"]:
            continue

        any_alert = True
        html_lines.append(f"<h3>{brand}</h3><ul>")
        for a in result["alerts"]:
            html_lines.append(f"<li>{a['message']}</li>")
        html_lines.append("</ul>")

    if not any_alert:
        html_lines.append("<p>✅ No anomalies detected across all brands.</p>")

    html_lines.append("</body></html>")

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = email_cfg["sender"]
    msg["To"]      = ", ".join(email_cfg["recipients"])
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
