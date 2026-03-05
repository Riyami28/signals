"""BuiltWith Free API collector — enriches tech_fit from BuiltWith technology data.

Uses the BuiltWith Free API (1 req/sec, free account) to detect technology
groups and categories for each account's domain. Maps categories to our
signal codes (cloud_infrastructure_detected, modern_stack_detected, etc.).

Complements the HTML-based website_techscan collector with BuiltWith's
deeper technology fingerprinting database (85K+ technologies tracked).
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Any

import httpx

from src import db
from src.models import SignalObservation
from src.settings import Settings
from src.utils import stable_hash, utc_now_iso

logger = logging.getLogger(__name__)

BUILTWITH_FREE_API = "https://api.builtwith.com/free1/api.json"
SOURCE_NAME = "builtwith_free"

# ─── Category → Signal mapping ────────────────────────────────────────
# Maps BuiltWith category names to (signal_code, confidence).
# Only LIVE categories (currently detected on site) are used.

_CATEGORY_SIGNAL_MAP: dict[str, tuple[str, float]] = {
    # Cloud infrastructure
    "Cloud Hosting": ("cloud_infrastructure_detected", 0.75),
    "Cloud PaaS": ("cloud_infrastructure_detected", 0.80),
    "Edge Delivery Network": ("cloud_infrastructure_detected", 0.60),
    # Modern stack
    "Framework": ("modern_stack_detected", 0.65),
    "JavaScript Library": ("modern_stack_detected", 0.55),
    "Headless": ("modern_stack_detected", 0.70),
    "Error Tracking": ("modern_stack_detected", 0.65),
    "AI": ("modern_stack_detected", 0.70),
    # Enterprise SaaS
    "CRM": ("enterprise_saas_detected", 0.75),
    "Marketing Automation": ("enterprise_saas_detected", 0.70),
    "Customer Data Platform": ("enterprise_saas_detected", 0.75),
    "Live Chat": ("enterprise_saas_detected", 0.55),
    "Ticketing System": ("enterprise_saas_detected", 0.60),
    "Enterprise": ("enterprise_saas_detected", 0.65),
    # Data platform
    "A/B Testing": ("data_platform_detected", 0.65),
    "Application Performance": ("data_platform_detected", 0.70),
    "Audience Measurement": ("data_platform_detected", 0.55),
    "Conversion Optimization": ("data_platform_detected", 0.60),
    "Personalization": ("data_platform_detected", 0.65),
    "Lead Generation": ("data_platform_detected", 0.60),
}

# Groups that indicate high tooling count (for tooling_sprawl_detected)
_TOOLING_SPRAWL_THRESHOLD = 50  # total live techs across all groups

# Categories to include in tech stack summary (meaningful tech, not hosting locations)
_TECH_DISPLAY_CATEGORIES = frozenset(
    list(_CATEGORY_SIGNAL_MAP.keys())
    + [
        "CDN",
        "SSL Certificate",
        "DNS",
        "Enterprise DNS",
        "Tag Management",
        "DDoS Protection",
        "API",
        "Containerization",
        "CI/CD",
        "Web Server",
        "Programming Language",
        "Operating System",
        "Load Balancer",
        "Database",
        "Message Queue",
        "Search Engine",
        "Content Management",
        "Ecommerce",
        "Payment",
        "Hosting",
        "Monitoring",
        "Logging",
        "Caching",
        "API Gateway",
        "Service Mesh",
        "DMARC",
        "Privacy Compliance",
        "Schema",
        "Responsive",
    ]
)


def _build_observation(
    account_id: str,
    signal_code: str,
    observed_at: str,
    confidence: float,
    source_reliability: float,
    evidence_url: str,
    evidence_text: str,
    payload: dict[str, Any],
) -> SignalObservation:
    raw_hash = stable_hash(payload, prefix="raw")
    # Use account+signal+source for dedup (not observed_at) — each account
    # can only have one tech detection per signal per source.
    obs_id = stable_hash(
        {
            "account_id": account_id,
            "signal_code": signal_code,
            "source": SOURCE_NAME,
        },
        prefix="obs",
    )
    return SignalObservation(
        obs_id=obs_id,
        account_id=account_id,
        signal_code=signal_code,
        product="shared",
        source=SOURCE_NAME,
        observed_at=observed_at,
        evidence_url=evidence_url,
        evidence_text=evidence_text[:500],
        confidence=max(0.0, min(1.0, float(confidence))),
        source_reliability=max(0.0, min(1.0, float(source_reliability))),
        raw_payload_hash=raw_hash,
    )


def _map_categories_to_signals(
    data: dict[str, Any],
) -> list[tuple[str, float, str]]:
    """Extract signal matches from BuiltWith response.

    Returns list of (signal_code, confidence, evidence_text).
    """
    signals: list[tuple[str, float, str]] = []
    seen_signals: set[str] = set()
    total_live = 0

    # First pass: collect meaningful live category names for tech summary
    all_live_categories: list[str] = []
    for group in data.get("groups", []):
        group_live = group.get("live", 0)
        total_live += group_live
        for cat in group.get("categories", []):
            cat_name = cat.get("name", "")
            if cat.get("live", 0) > 0 and cat_name in _TECH_DISPLAY_CATEGORIES:
                all_live_categories.append(cat_name)

    # Build a clean tech summary from meaningful categories only
    tech_summary = ", ".join(all_live_categories[:20])

    # Second pass: emit signals for mapped categories
    for group in data.get("groups", []):
        for cat in group.get("categories", []):
            cat_name = cat.get("name", "")
            cat_live = cat.get("live", 0)

            if cat_live <= 0:
                continue

            mapping = _CATEGORY_SIGNAL_MAP.get(cat_name)
            if not mapping:
                continue

            signal_code, confidence = mapping
            if signal_code in seen_signals:
                continue

            seen_signals.add(signal_code)
            evidence = f"Tech Stack: {tech_summary}" if tech_summary else f"BuiltWith: {cat_name}"
            signals.append((signal_code, confidence, evidence))

    # Tooling sprawl detection — companies with lots of live tech often need consolidation
    if total_live >= _TOOLING_SPRAWL_THRESHOLD and "tooling_sprawl_detected" not in seen_signals:
        evidence = (
            f"Tech Stack ({total_live} technologies): {tech_summary}"
            if tech_summary
            else f"BuiltWith: {total_live} technologies"
        )
        signals.append(("tooling_sprawl_detected", 0.60, evidence))

    return signals


async def _fetch_builtwith(
    client: httpx.AsyncClient,
    api_key: str,
    domain: str,
    max_retries: int = 2,
) -> dict[str, Any] | None:
    """Fetch technology profile from BuiltWith Free API with 429 retry."""
    for attempt in range(max_retries + 1):
        try:
            resp = await client.get(
                BUILTWITH_FREE_API,
                params={"KEY": api_key, "LOOKUP": domain},
                timeout=15,
            )

            # Rate limited — back off and retry
            if resp.status_code == 429:
                if attempt < max_retries:
                    wait = 3 * (attempt + 1)
                    logger.debug("builtwith_rate_limited domain=%s waiting=%ds", domain, wait)
                    await asyncio.sleep(wait)
                    continue
                logger.warning("builtwith_rate_limited domain=%s exhausted_retries", domain)
                return None

            resp.raise_for_status()
            data = resp.json()

            # Check for API errors
            if "Errors" in data:
                errors = data["Errors"]
                if errors:
                    logger.warning("builtwith_api_error domain=%s errors=%s", domain, errors)
                    return None

            return data
        except httpx.HTTPStatusError as exc:
            logger.warning("builtwith_http_error domain=%s status=%s", domain, exc.response.status_code)
            return None
        except Exception as exc:
            logger.warning("builtwith_error domain=%s error=%s", domain, exc)
            return None
    return None


async def _collect_one_account(
    conn,
    client: httpx.AsyncClient,
    api_key: str,
    account: dict[str, Any],
    source_reliability: float,
) -> tuple[int, int]:
    """Scan one account via BuiltWith and insert signals."""
    account_id = str(account["account_id"])
    domain = str(account.get("domain", ""))

    if not domain or domain.endswith(".example"):
        return 0, 0

    endpoint = f"builtwith:{domain}"
    if db.was_crawled_today(conn, source=SOURCE_NAME, account_id=account_id, endpoint=endpoint):
        return 0, 0

    data = await _fetch_builtwith(client, api_key, domain)

    if data is None:
        db.record_crawl_attempt(
            conn,
            source=SOURCE_NAME,
            account_id=account_id,
            endpoint=endpoint,
            status="exception",
            error_summary="api_error_or_empty",
            commit=False,
        )
        db.mark_crawled(conn, source=SOURCE_NAME, account_id=account_id, endpoint=endpoint, commit=False)
        return 0, 0

    signals = _map_categories_to_signals(data)
    observed_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    inserted = 0
    seen = len(signals)

    for signal_code, confidence, evidence_text in signals:
        obs = _build_observation(
            account_id=account_id,
            signal_code=signal_code,
            observed_at=observed_at,
            confidence=confidence,
            source_reliability=source_reliability,
            evidence_url="",
            evidence_text=evidence_text,
            payload={"domain": domain, "signal_code": signal_code, "source": "builtwith_free"},
        )
        if db.insert_signal_observation(conn, obs, commit=False):
            inserted += 1

    db.record_crawl_attempt(
        conn,
        source=SOURCE_NAME,
        account_id=account_id,
        endpoint=endpoint,
        status="success",
        error_summary="",
        commit=False,
    )
    db.mark_crawled(conn, source=SOURCE_NAME, account_id=account_id, endpoint=endpoint, commit=False)

    return inserted, seen


async def collect(
    conn,
    settings: Settings,
    account_ids: list[str] | None = None,
    source_reliability: float = 0.70,
    **kwargs,
) -> dict[str, int]:
    """Main entry point: scan domains via BuiltWith Free API.

    Returns:
        {"inserted": N, "seen": N, "accounts_processed": N}
    """
    api_key = settings.builtwith_api_key
    if not api_key:
        logger.warning("builtwith_api_key is empty, skipping BuiltWith collection")
        return {"inserted": 0, "seen": 0, "accounts_processed": 0}

    # Fetch accounts
    cursor = conn.cursor()
    if account_ids:
        cursor.execute(
            "SELECT account_id, company_name, domain FROM signals.accounts WHERE account_id = ANY(%s)",
            (account_ids,),
        )
    else:
        cursor.execute(
            """SELECT a.account_id, a.company_name, a.domain
               FROM signals.accounts a
               LEFT JOIN signals.crawl_checkpoints cp
                 ON cp.account_id = a.account_id
                 AND cp.source = 'builtwith_free'
               WHERE COALESCE(a.domain, '') <> ''
               ORDER BY
                   CASE WHEN cp.last_crawled_at IS NULL THEN 0 ELSE 1 END,
                   cp.last_crawled_at ASC,
                   a.company_name ASC
               LIMIT %s""",
            (min(settings.live_max_accounts, 50),),
        )
    accounts = [dict(row) for row in cursor.fetchall()]

    if not accounts:
        return {"inserted": 0, "seen": 0, "accounts_processed": 0}

    logger.info("builtwith starting accounts=%d", len(accounts))
    t0 = time.monotonic()

    total_inserted = 0
    total_seen = 0
    processed = 0

    # Rate limit: 1 req/sec for Free API — process sequentially
    async with httpx.AsyncClient() as client:
        for i, account in enumerate(accounts, 1):
            try:
                ins, seen = await _collect_one_account(
                    conn=conn,
                    client=client,
                    api_key=api_key,
                    account=account,
                    source_reliability=source_reliability,
                )
                total_inserted += ins
                total_seen += seen
                processed += 1

                if i % 50 == 0:
                    conn.commit()
                    logger.info(
                        "builtwith progress %d/%d inserted=%d seen=%d",
                        i,
                        len(accounts),
                        total_inserted,
                        total_seen,
                    )

            except Exception as exc:
                logger.warning("builtwith_worker_error account=%s error=%s", account.get("domain"), exc)
                continue

            # Rate limit: 1 request per second (free tier) — 2s buffer to avoid 429
            await asyncio.sleep(2.0)

    conn.commit()

    dt = time.monotonic() - t0
    logger.info(
        "builtwith done accounts=%d inserted=%d seen=%d duration=%.1fs",
        processed,
        total_inserted,
        total_seen,
        dt,
    )

    return {"inserted": total_inserted, "seen": total_seen, "accounts_processed": processed}
