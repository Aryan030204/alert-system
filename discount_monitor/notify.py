# =============================================================
# notify.py - Prints the digest, optional single email per run
# =============================================================

import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import config

log = logging.getLogger(__name__)


def print_digest(digest_text: str):
    print(digest_text)
    for line in digest_text.splitlines():
        log.info(line)


def send_digest_email(digest_text: str, flagged_count: int):
    email_enabled = getattr(config, "EMAIL_ENABLED", False)
    if not email_enabled or flagged_count == 0:
        return

    smtp_host = getattr(config, "SMTP_HOST", "")
    smtp_port = getattr(config, "SMTP_PORT", 587)
    smtp_user = getattr(config, "SMTP_USER", "")
    smtp_password = getattr(config, "SMTP_PASSWORD", "")
    email_from = getattr(config, "EMAIL_FROM", "")
    email_to = getattr(config, "EMAIL_TO", [])

    subject = f"[Discount Monitor] {flagged_count} brand(s) flagged"

    try:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = email_from
        msg["To"] = ", ".join(email_to)
        msg.attach(MIMEText(digest_text, "plain", "utf-8"))

        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.ehlo()
            server.starttls()
            server.login(smtp_user, smtp_password)
            server.sendmail(email_from, email_to, msg.as_string())

        log.info("  Digest email sent to %s", email_to)
    except Exception as exc:
        log.error("  Digest email failed: %s", exc)
