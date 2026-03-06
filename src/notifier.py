from __future__ import annotations

import smtplib
from email.message import EmailMessage
from pathlib import Path

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


_TIER_ARROWS = {"high": "\u2b06", "medium": "\u2197", "low": "\u2b07"}


def send_tier_change_alerts(
    settings: Settings,
    changes: list[dict],
) -> dict[str, object]:
    """Send a consolidated tier-change alert.

    Each item in *changes*: ``{company_name, product, old_tier, new_tier, score,
    delta_7d, top_reason, velocity_category}``.
    """
    if not changes:
        return {"delivered_channels": [], "errors": []}

    upgrades = [c for c in changes if _tier_rank(c["new_tier"]) > _tier_rank(c["old_tier"])]
    downgrades = [c for c in changes if _tier_rank(c["new_tier"]) < _tier_rank(c["old_tier"])]

    lines: list[str] = []
    if upgrades:
        lines.append(f"=== {len(upgrades)} Tier Upgrade(s) ===")
        for c in upgrades:
            arrow = _TIER_ARROWS.get(c["new_tier"], "")
            reason = c.get("top_reason", "")
            lines.append(
                f"{arrow} {c['company_name']} [{c['product']}]: "
                f"{c['old_tier']} -> {c['new_tier']} "
                f"(score {c['score']:.1f}, 7d {c['delta_7d']:+.1f})" + (f" | {reason}" if reason else "")
            )

    if downgrades:
        lines.append(f"=== {len(downgrades)} Tier Downgrade(s) ===")
        for c in downgrades:
            arrow = _TIER_ARROWS.get(c["new_tier"], "")
            lines.append(
                f"{arrow} {c['company_name']} [{c['product']}]: "
                f"{c['old_tier']} -> {c['new_tier']} "
                f"(score {c['score']:.1f}, 7d {c['delta_7d']:+.1f})"
            )

    title = f"Tier changes: {len(upgrades)} up, {len(downgrades)} down"
    body = "\n".join(lines)
    return send_alert(settings, title=title, body=body, severity="info")


def _tier_rank(tier: str) -> int:
    return {"low": 0, "medium": 1, "high": 2}.get(tier, -1)
