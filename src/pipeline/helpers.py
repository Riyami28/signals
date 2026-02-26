"""Shared helpers for pipeline orchestration modules."""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timedelta, timezone

from src import db
from src.discovery.config import load_account_profiles, load_discovery_blocklist
from src.notifier import send_alert
from src.settings import Settings, load_settings
from src.utils import ensure_project_directories, normalize_domain

logger = logging.getLogger(__name__)

_RUN_DAILY_LOCK_NAME = "signals:run-daily"
_AUTONOMOUS_LOCK_NAME = "signals:run-autonomous-loop"
_RETRY_BACKOFF_SECONDS = [60, 300, 900]


class StageExecutionError(RuntimeError):
    def __init__(self, stage: str, duration_seconds: float, timed_out: bool, message: str):
        super().__init__(message)
        self.stage = stage
        self.duration_seconds = float(duration_seconds)
        self.timed_out = bool(timed_out)


def run_with_watchdog(stage: str, timeout_seconds: int, fn):
    started = time.monotonic()
    try:
        result = fn()
    except Exception as exc:
        elapsed = time.monotonic() - started
        raise StageExecutionError(
            stage=stage,
            duration_seconds=elapsed,
            timed_out=False,
            message=f"{stage} failed: {str(exc)[:240]}",
        ) from exc
    elapsed = time.monotonic() - started
    if elapsed > float(timeout_seconds):
        raise StageExecutionError(
            stage=stage,
            duration_seconds=elapsed,
            timed_out=True,
            message=f"{stage} exceeded timeout_seconds={timeout_seconds}",
        )
    return result, elapsed


def retry_due_iso(backoff_seconds: int) -> str:
    return (
        (datetime.now(timezone.utc) + timedelta(seconds=max(1, int(backoff_seconds))))
        .replace(microsecond=0)
        .isoformat()
    )


def enqueue_retry_task(
    conn,
    settings: Settings,
    task_type: str,
    payload: dict[str, object],
    reason: str,
) -> str:
    task_id = db.enqueue_retry_task(
        conn=conn,
        task_type=task_type,
        payload_json=json.dumps(payload, ensure_ascii=True, sort_keys=True),
        due_at=retry_due_iso(_RETRY_BACKOFF_SECONDS[0]),
        max_attempts=settings.retry_attempt_limit,
        commit=True,
    )
    retry_depth = db.fetch_retry_depth(conn)
    if retry_depth >= settings.alert_retry_depth_threshold:
        send_alert(
            settings,
            title="Retry depth threshold exceeded",
            body=f"retry_depth={retry_depth} threshold={settings.alert_retry_depth_threshold} task_id={task_id}",
            severity="warn",
        )
    if reason:
        send_alert(
            settings,
            title="Retry task enqueued",
            body=f"task_id={task_id} task_type={task_type} reason={reason[:300]}",
            severity="warn",
        )
    return task_id


def bootstrap(settings: Settings | None = None):
    local_settings = settings or load_settings()
    ensure_project_directories(
        [
            local_settings.project_root,
            local_settings.config_dir,
            local_settings.data_dir,
            local_settings.raw_dir,
            local_settings.out_dir,
        ]
    )
    conn = db.get_connection(local_settings.pg_dsn)
    db.init_db(conn)
    seeded_base = db.seed_accounts(conn, local_settings.seed_accounts_path)
    seeded_watchlist = db.seed_accounts(conn, local_settings.watchlist_accounts_path)
    seeded = seeded_base + seeded_watchlist
    return local_settings, conn, seeded


def review_queue_excluded_domains(settings: Settings) -> set[str]:
    excluded = {"zop.dev"}

    try:
        blocked_domains = load_discovery_blocklist(settings.discovery_blocklist_path)
        excluded.update(blocked_domains)
    except Exception:
        logger.warning("failed to load discovery blocklist from %s", settings.discovery_blocklist_path, exc_info=True)

    try:
        profiles = load_account_profiles(settings.account_profiles_path)
        for domain, profile in profiles.items():
            if profile.is_self or profile.exclude_from_crm:
                excluded.add(domain)
    except Exception:
        logger.warning("failed to load account profiles from %s", settings.account_profiles_path, exc_info=True)

    normalized: set[str] = set()
    for domain in excluded:
        value = normalize_domain(domain)
        if value:
            normalized.add(value)
    return normalized
