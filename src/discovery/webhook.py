from __future__ import annotations

import json
import logging
import time
from collections import defaultdict
from threading import Lock
from typing import Any, Optional
from urllib.parse import urlparse

from pydantic import BaseModel, Field

from src import db
from src.discovery.config import is_placeholder_domain
from src.settings import load_settings
from src.utils import normalize_domain, utc_now_iso

try:
    from fastapi import FastAPI, Header, HTTPException, Request
except Exception:  # pragma: no cover - dependency may be absent in lightweight envs.
    FastAPI = None  # type: ignore
    Header = None  # type: ignore
    HTTPException = Exception  # type: ignore
    Request = None  # type: ignore

logger = logging.getLogger(__name__)

# --- Simple in-memory rate limiter ---
_RATE_LIMIT_MAX_REQUESTS = 60  # per window
_RATE_LIMIT_WINDOW_SECONDS = 60

_rate_lock = Lock()
_rate_buckets: dict[str, list[float]] = defaultdict(list)


def _is_rate_limited(client_ip: str) -> bool:
    now = time.monotonic()
    cutoff = now - _RATE_LIMIT_WINDOW_SECONDS
    with _rate_lock:
        bucket = _rate_buckets[client_ip]
        # Prune expired entries.
        _rate_buckets[client_ip] = [t for t in bucket if t > cutoff]
        if len(_rate_buckets[client_ip]) >= _RATE_LIMIT_MAX_REQUESTS:
            return True
        _rate_buckets[client_ip].append(now)
        return False


class DiscoveryEventPayload(BaseModel):
    source: str = Field(default="huginn_webhook")
    source_event_id: str = ""
    observed_at: str = ""
    title: str = ""
    text: str = ""
    url: str = ""
    entry_url: str = ""
    url_type: str = ""
    language_hint: str = ""
    author_hint: str = ""
    published_at_hint: str = ""
    company_name_hint: str = ""
    domain_hint: str = ""
    raw_payload: Any = None


def _extract_domain_from_url(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""
    parsed = urlparse(raw)
    netloc = parsed.netloc or parsed.path
    return normalize_domain(netloc)


def _resolve_domain_hint(payload: DiscoveryEventPayload) -> str:
    hinted = normalize_domain(payload.domain_hint)
    if hinted:
        return hinted
    return _extract_domain_from_url(payload.url)


def _insert_event(payload: DiscoveryEventPayload) -> bool:
    settings = load_settings()
    conn = db.get_connection(settings.pg_dsn)
    db.init_db(conn)
    try:
        raw_payload = payload.raw_payload
        domain_hint = _resolve_domain_hint(payload)
        inserted = db.insert_external_discovery_event(
            conn=conn,
            source=(payload.source or "huginn_webhook"),
            source_event_id=payload.source_event_id,
            observed_at=payload.observed_at or utc_now_iso(),
            title=payload.title,
            text=payload.text,
            url=payload.url,
            entry_url=payload.entry_url,
            url_type=payload.url_type,
            language_hint=payload.language_hint,
            author_hint=payload.author_hint,
            published_at_hint=payload.published_at_hint,
            company_name_hint=payload.company_name_hint,
            domain_hint=domain_hint,
            raw_payload_json=json.dumps(
                raw_payload if raw_payload is not None else {},
                ensure_ascii=True,
                sort_keys=True,
            ),
        )
        return inserted
    finally:
        conn.close()


def _maybe_enqueue_rescore(domain: str) -> None:
    """If domain maps to a known account, queue an immediate rescore task."""
    try:
        settings = load_settings()
        conn = db.get_connection(settings.pg_dsn)
        try:
            account = db.get_account_by_domain(conn, domain)
            if account:
                db.enqueue_retry_task(
                    conn=conn,
                    task_type="rescore_account",
                    payload_json=json.dumps(
                        {"account_id": str(account["account_id"]), "domain": domain},
                        sort_keys=True,
                    ),
                    due_at=utc_now_iso(),
                    max_attempts=3,
                )
                logger.info("rescore_enqueued domain=%s account_id=%s", domain, account["account_id"])
        finally:
            conn.close()
    except Exception:
        logger.exception("_maybe_enqueue_rescore failed domain=%s", domain)


def create_app():
    if FastAPI is None:  # pragma: no cover - dependency may be absent in lightweight envs.
        raise RuntimeError("fastapi is required for webhook serving. Install dependencies first.")

    app = FastAPI(title="signals-discovery-webhook", version="0.1.0")

    @app.post("/v1/discovery/events")
    def receive_discovery_event(
        request: Request,
        payload: DiscoveryEventPayload,
        x_discovery_token: Optional[str] = Header(default=None, alias="X-Discovery-Token"),
    ):
        # Rate limiting.
        client_ip = request.client.host if request.client else "unknown"
        if _is_rate_limited(client_ip):
            logger.warning("rate_limited client_ip=%s", client_ip)
            raise HTTPException(status_code=429, detail="rate limit exceeded")

        settings = load_settings()
        expected = settings.discovery_webhook_token
        if expected and (x_discovery_token or "").strip() != expected:
            raise HTTPException(status_code=401, detail="invalid discovery token")
        resolved_domain = _resolve_domain_hint(payload)
        if resolved_domain and is_placeholder_domain(resolved_domain):
            raise HTTPException(status_code=422, detail="placeholder/test domains are not allowed")
        inserted = _insert_event(payload)
        if inserted and resolved_domain:
            _maybe_enqueue_rescore(resolved_domain)
        return {"accepted": 1, "inserted": int(inserted)}

    return app


app = create_app() if FastAPI is not None else None
