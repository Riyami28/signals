"""Serper.dev Google Search collector — fetches real news for watchlist companies."""

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
from src.utils import classify_text, stable_hash, utc_now_iso

logger = logging.getLogger(__name__)

SERPER_API_URL = "https://google.serper.dev/news"

# Broad search terms to find relevant news for cloud/devops/platform companies
_NEWS_TERMS = (
    "cloud migration OR kubernetes OR devops OR infrastructure OR "
    "SOC 2 OR compliance OR cost optimization OR platform engineering OR "
    "funding OR acquisition OR expansion OR hiring OR layoff"
)


def _build_observation(
    account_id: str,
    signal_code: str,
    source: str,
    observed_at: str,
    confidence: float,
    source_reliability: float,
    evidence_url: str,
    evidence_text: str,
    payload: dict[str, Any],
) -> SignalObservation:
    raw_hash = stable_hash(payload, prefix="raw")
    obs_id = stable_hash(
        {
            "account_id": account_id,
            "signal_code": signal_code,
            "source": source,
            "observed_at": observed_at,
            "raw": raw_hash,
        },
        prefix="obs",
    )
    return SignalObservation(
        obs_id=obs_id,
        account_id=account_id,
        signal_code=signal_code,
        product="shared",
        source=source,
        observed_at=observed_at,
        evidence_url=evidence_url,
        evidence_text=evidence_text[:500],
        confidence=max(0.0, min(1.0, float(confidence))),
        source_reliability=max(0.0, min(1.0, float(source_reliability))),
        raw_payload_hash=raw_hash,
    )


async def _fetch_serper_news(
    client: httpx.AsyncClient,
    query: str,
    api_key: str,
    num_results: int = 10,
) -> list[dict]:
    """Call Serper news API and return results list."""
    try:
        resp = await client.post(
            SERPER_API_URL,
            json={"q": query, "num": num_results},
            headers={
                "X-API-KEY": api_key,
                "Content-Type": "application/json",
            },
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        return data.get("news", [])
    except httpx.HTTPStatusError as exc:
        logger.warning("serper_http_error query=%s status=%s", query[:60], exc.response.status_code)
        return []
    except Exception as exc:
        logger.warning("serper_error query=%s error=%s", query[:60], exc)
        return []


def _parse_serper_date(date_str: str) -> str:
    """Parse Serper date string to ISO format. Falls back to now."""
    if not date_str:
        return utc_now_iso()
    # Serper returns relative dates like "2 hours ago", "3 days ago"
    # or ISO-like dates. We'll use utc_now for relative and try parse for absolute.
    try:
        dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        return dt.isoformat()
    except (ValueError, TypeError):
        return utc_now_iso()


async def _collect_one_account(
    conn,
    client: httpx.AsyncClient,
    account: dict,
    api_key: str,
    num_results: int,
    lexicon_rows: list[dict[str, str]],
    source_reliability: float,
) -> tuple[int, int]:
    """Fetch Serper news for one account and insert matching signals."""
    account_id = str(account["account_id"])
    company_name = str(account.get("company_name") or account.get("domain", ""))
    domain = str(account.get("domain", ""))

    if not company_name and not domain:
        return 0, 0

    # Check if already crawled today
    source_name = "serper_news"
    endpoint = f"serper:{domain}"
    if db.was_crawled_today(conn, source=source_name, account_id=account_id, endpoint=endpoint):
        return 0, 0

    # Build search query
    query = f'"{company_name}" OR "{domain}"'

    results = await _fetch_serper_news(client, query, api_key, num_results)

    if not results:
        db.record_crawl_attempt(
            conn,
            source=source_name,
            account_id=account_id,
            endpoint=endpoint,
            status="success",
            error_summary="no_results",
            commit=False,
        )
        db.mark_crawled(conn, source=source_name, account_id=account_id, endpoint=endpoint, commit=False)
        return 0, 0

    inserted = 0
    seen = 0

    for item in results:
        title = str(item.get("title", ""))
        snippet = str(item.get("snippet", ""))
        link = str(item.get("link", ""))
        date_str = str(item.get("date", ""))
        source_name_item = str(item.get("source", ""))

        text = f"{title}\n{snippet}".strip()
        if not text:
            continue

        # Try to match against keyword lexicon
        matches = classify_text(text, lexicon_rows)

        if matches:
            # Insert matched signal observations
            observed_at = _parse_serper_date(date_str)
            for signal_code, confidence, matched_keyword in matches:
                seen += 1
                observation = _build_observation(
                    account_id=account_id,
                    signal_code=signal_code,
                    source="serper_news",
                    observed_at=observed_at,
                    confidence=confidence,
                    source_reliability=source_reliability,
                    evidence_url=link,
                    evidence_text=text,
                    payload={
                        "title": title,
                        "snippet": snippet,
                        "link": link,
                        "date": date_str,
                        "news_source": source_name_item,
                        "matched_keyword": matched_keyword,
                    },
                )
                if db.insert_signal_observation(conn, observation, commit=False):
                    inserted += 1
        # Skip articles that don't match any keyword — general company news
        # without a specific signal is noise, not a buying signal.

    db.record_crawl_attempt(
        conn,
        source="serper_news",
        account_id=account_id,
        endpoint=endpoint,
        status="success",
        error_summary="",
        commit=False,
    )
    db.mark_crawled(conn, source="serper_news", account_id=account_id, endpoint=endpoint, commit=False)

    return inserted, seen


async def collect(
    conn,
    settings: Settings,
    lexicon_rows: list[dict[str, str]],
    source_reliability: float = 0.85,
    account_ids: list[str] | None = None,
) -> dict[str, int]:
    """
    Main entry point: fetch Serper news for accounts.

    Args:
        conn: DB connection
        settings: App settings (needs serper_api_key)
        lexicon_rows: Keyword lexicon for 'news' source
        source_reliability: Default reliability for serper source
        account_ids: Optional list of specific account IDs to process

    Returns:
        {"inserted": N, "seen": N, "accounts_processed": N}
    """
    api_key = settings.serper_api_key
    if not api_key:
        logger.warning("serper_api_key is empty, skipping Serper news collection")
        return {"inserted": 0, "seen": 0, "accounts_processed": 0}

    max_accounts = settings.serper_max_accounts
    num_results = settings.serper_results_per_query

    # Load accounts
    if account_ids:
        placeholders = ",".join(["%s"] * len(account_ids))
        accounts = [
            dict(r)
            for r in conn.execute(
                f"SELECT account_id, company_name, domain FROM accounts WHERE account_id IN ({placeholders})",
                tuple(account_ids),
            ).fetchall()
        ]
    else:
        accounts = [
            dict(r)
            for r in conn.execute(
                "SELECT account_id, company_name, domain FROM accounts ORDER BY company_name LIMIT %s",
                (max_accounts,),
            ).fetchall()
        ]

    if not accounts:
        return {"inserted": 0, "seen": 0, "accounts_processed": 0}

    logger.info("serper_news starting accounts=%d max_results_per=%d", len(accounts), num_results)
    t0 = time.monotonic()

    total_inserted = 0
    total_seen = 0
    processed = 0

    # Process with concurrency control (Serper allows ~100 req/min)
    concurrency = min(6, len(accounts))
    semaphore = asyncio.Semaphore(concurrency)

    async with httpx.AsyncClient() as client:

        async def _run_one(account: dict) -> tuple[int, int]:
            async with semaphore:
                result = await _collect_one_account(
                    conn=conn,
                    client=client,
                    account=account,
                    api_key=api_key,
                    num_results=num_results,
                    lexicon_rows=lexicon_rows,
                    source_reliability=source_reliability,
                )
                await asyncio.sleep(0.1)
                return result

        tasks = [_run_one(acct) for acct in accounts]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    conn.commit()

    for result in results:
        if isinstance(result, Exception):
            logger.warning("serper_worker_error: %s", result)
            continue
        ins, seen = result
        total_inserted += ins
        total_seen += seen
        processed += 1

    dt = time.monotonic() - t0
    logger.info(
        "serper_news done accounts=%d inserted=%d seen=%d duration=%.1fs",
        processed,
        total_inserted,
        total_seen,
        dt,
    )

    return {"inserted": total_inserted, "seen": total_seen, "accounts_processed": processed}
