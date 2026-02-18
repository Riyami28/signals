from __future__ import annotations

import json
from typing import Any
from urllib.parse import urlparse

from pydantic import BaseModel, Field

from src import db
from src.discovery.config import is_placeholder_domain
from src.settings import load_settings
from src.utils import normalize_domain, utc_now_iso

try:
    from fastapi import FastAPI, Header, HTTPException
except Exception:  # pragma: no cover - dependency may be absent in lightweight envs.
    FastAPI = None  # type: ignore
    Header = None  # type: ignore
    HTTPException = Exception  # type: ignore


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
    raw_payload: dict[str, Any] | list[Any] | str | None = None


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
    conn = db.get_connection(settings.db_path)
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
            raw_payload_json=json.dumps(raw_payload if raw_payload is not None else {}, ensure_ascii=True, sort_keys=True),
        )
        return inserted
    finally:
        conn.close()


def create_app():
    if FastAPI is None:  # pragma: no cover - dependency may be absent in lightweight envs.
        raise RuntimeError("fastapi is required for webhook serving. Install dependencies first.")

    app = FastAPI(title="signals-discovery-webhook", version="0.1.0")

    @app.post("/v1/discovery/events")
    def receive_discovery_event(
        payload: DiscoveryEventPayload,
        x_discovery_token: str | None = Header(default=None, alias="X-Discovery-Token"),
    ):
        settings = load_settings()
        expected = settings.discovery_webhook_token
        if expected and (x_discovery_token or "").strip() != expected:
            raise HTTPException(status_code=401, detail="invalid discovery token")
        resolved_domain = _resolve_domain_hint(payload)
        if resolved_domain and is_placeholder_domain(resolved_domain):
            raise HTTPException(status_code=422, detail="placeholder/test domains are not allowed")
        inserted = _insert_event(payload)
        return {"accepted": 1, "inserted": int(inserted)}

    return app


app = create_app() if FastAPI is not None else None
