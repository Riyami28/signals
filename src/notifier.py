from __future__ import annotations

from email.message import EmailMessage
from pathlib import Path
import smtplib

import requests

from src.settings import Settings
from src.utils import utc_now_iso


def _append_local_alert(path: Path, severity: str, title: str, body: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    line = f"{utc_now_iso()} severity={severity} title={title} body={body}\n"
    with path.open("a", encoding="utf-8") as handle:
        handle.write(line)


def _send_gchat(webhook_url: str, severity: str, title: str, body: str) -> None:
    payload = {"text": f"[signals:{severity}] {title}\n{body}"}
    response = requests.post(webhook_url, json=payload, timeout=10)
    response.raise_for_status()


def _send_email(settings: Settings, severity: str, title: str, body: str) -> None:
    if not (settings.alert_email_to and settings.alert_email_from and settings.alert_smtp_host):
        return

    message = EmailMessage()
    message["Subject"] = f"[signals:{severity}] {title}"
    message["From"] = settings.alert_email_from
    message["To"] = settings.alert_email_to
    message.set_content(body)

    with smtplib.SMTP(settings.alert_smtp_host, settings.alert_smtp_port, timeout=10) as smtp:
        smtp.starttls()
        if settings.alert_smtp_user:
            smtp.login(settings.alert_smtp_user, settings.alert_smtp_password)
        smtp.send_message(message)


def send_alert(settings: Settings, title: str, body: str, severity: str = "error") -> dict[str, object]:
    channels: list[str] = []
    errors: list[str] = []

    if settings.gchat_webhook_url:
        try:
            _send_gchat(settings.gchat_webhook_url, severity=severity, title=title, body=body)
            channels.append("gchat")
        except Exception as exc:
            errors.append(f"gchat:{str(exc)[:200]}")

    if settings.alert_email_to and settings.alert_email_from and settings.alert_smtp_host:
        try:
            _send_email(settings, severity=severity, title=title, body=body)
            channels.append("email")
        except Exception as exc:
            errors.append(f"email:{str(exc)[:200]}")

    fallback_path = settings.out_dir / "alerts.log"
    _append_local_alert(fallback_path, severity=severity, title=title, body=body)
    channels.append("local_log")

    return {"delivered_channels": channels, "errors": errors}
