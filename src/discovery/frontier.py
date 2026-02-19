from __future__ import annotations

from datetime import date
import json
from urllib.parse import urlparse, urlunparse

from src import db
from src.discovery.config import is_placeholder_domain
from src.settings import Settings
from src.source_policy import load_source_execution_policy
from src.utils import normalize_domain

VALID_URL_TYPES = {"article", "listing", "profile", "other"}


def canonicalize_url(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""
    if "://" not in raw:
        raw = f"https://{raw}"
    parsed = urlparse(raw)
    scheme = (parsed.scheme or "https").lower()
    netloc = normalize_domain(parsed.netloc or parsed.path)
    if not netloc:
        return ""
    path = parsed.path or "/"
    if path != "/" and path.endswith("/"):
        path = path[:-1]
    # Canonical form removes query/fragment for stable dedupe.
    return urlunparse((scheme, netloc, path, "", "", ""))


def _infer_url_type(url: str, hint: str, title: str, text: str) -> str:
    normalized_hint = (hint or "").strip().lower()
    if normalized_hint in VALID_URL_TYPES:
        return normalized_hint

    path = (urlparse(url).path or "").lower()
    title_text = f"{title}\n{text}".lower()
    listing_tokens = ("newsroom", "news", "press", "media", "stories", "blog")
    profile_tokens = ("leadership", "team", "executive", "about")

    if any(token in path for token in profile_tokens):
        return "profile"
    if path in {"", "/"} or any(token in path for token in listing_tokens):
        return "listing"
    if "http" in title_text and "http" not in path:
        return "listing"
    return "article"


def _resolve_domain(url: str, domain_hint: str) -> str:
    hinted = normalize_domain(domain_hint)
    if hinted:
        return hinted
    parsed = urlparse(url)
    return normalize_domain(parsed.netloc or parsed.path)


def _build_priority(url_type: str, source: str, text: str) -> float:
    source_norm = (source or "").strip().lower()
    score = 0.5
    if url_type == "article":
        score += 0.3
    elif url_type == "listing":
        score += 0.15
    elif url_type == "profile":
        score += 0.1

    if source_norm in {"huginn_webhook", "first_party_csv"}:
        score += 0.1

    text_norm = (text or "").lower()
    if any(token in text_norm for token in ("erp", "s/4hana", "control tower", "audit readiness", "procurement")):
        score += 0.15
    return max(0.0, min(1.0, score))


def build_frontier(conn, settings: Settings, run_date: date, budget: int) -> dict[str, int | str]:
    run_date_str = run_date.isoformat()
    execution_policy = load_source_execution_policy(settings.source_execution_policy_path)
    webhook_policy = execution_policy.get("huginn_webhook")
    effective_budget = max(1, int(budget))
    if webhook_policy is not None and webhook_policy.batch_size > 0:
        effective_budget = min(effective_budget, int(webhook_policy.batch_size))

    events = db.fetch_pending_external_discovery_events(conn, run_date=run_date_str, limit=effective_budget)
    marker = f"discover_frontier_{run_date_str}"

    queued = 0
    duplicates = 0
    failed = 0

    for row in events:
        event_id = int(row["event_id"])
        source = str(row["source"] or "huginn_webhook").strip().lower()
        source_event_id = str(row["source_event_id"] or "")
        title = str(row["title"] or "")
        text = str(row["text"] or "")
        candidate_url = str(row["entry_url"] or row["url"] or "")
        canonical = canonicalize_url(candidate_url)

        if not canonical:
            db.mark_external_discovery_event_failed(
                conn,
                event_id=event_id,
                processed_run_id=marker,
                error_summary="invalid_url",
                commit=False,
            )
            failed += 1
            continue

        domain = _resolve_domain(canonical, str(row["domain_hint"] or ""))
        if not domain or is_placeholder_domain(domain):
            db.mark_external_discovery_event_failed(
                conn,
                event_id=event_id,
                processed_run_id=marker,
                error_summary="invalid_domain",
                commit=False,
            )
            failed += 1
            continue

        company_name = str(row["company_name_hint"] or "").strip() or domain
        account_id = db.upsert_account(
            conn,
            company_name=company_name,
            domain=domain,
            source_type="discovered",
            commit=False,
        )
        url_type = _infer_url_type(canonical, str(row["url_type"] or ""), title, text)
        priority = _build_priority(url_type, source, text)

        payload = {
            "event_id": event_id,
            "language_hint": str(row["language_hint"] or ""),
            "author_hint": str(row["author_hint"] or ""),
            "published_at_hint": str(row["published_at_hint"] or ""),
            "raw_payload_json": str(row["raw_payload_json"] or "{}"),
        }

        inserted = db.insert_crawl_frontier(
            conn=conn,
            run_date=run_date_str,
            source=source,
            source_event_id=source_event_id,
            account_id=account_id,
            domain=domain,
            url=candidate_url,
            canonical_url=canonical,
            url_type=url_type,
            depth=0,
            priority=priority,
            max_retries=2,
            payload_json=json.dumps(payload, ensure_ascii=True, separators=(",", ":")),
            commit=False,
        )
        if inserted:
            queued += 1
        else:
            duplicates += 1

        db.mark_external_discovery_event_processed(
            conn,
            event_id=event_id,
            processed_run_id=marker,
            commit=False,
        )

    conn.commit()
    return {
        "run_date": run_date_str,
        "events_seen": len(events),
        "frontier_queued": queued,
        "frontier_duplicates": duplicates,
        "events_failed": failed,
    }
