"""GNews.io News collector — truly free news API, no credit card needed.

Free tier: 100 requests/day, 10 articles/request = 1000 articles/day.
Sign up at gnews.io — just email, no CC.

This supplements Serper news with additional trigger_intent signals.
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
from src.utils import classify_text, stable_hash, utc_now_iso

logger = logging.getLogger(__name__)

GNEWS_API_URL = "https://gnews.io/api/v4/search"


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


def _parse_date(date_str: str) -> str:
    """Parse GNews date format to ISO. Falls back to now."""
    if not date_str:
        return utc_now_iso()
    try:
        dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        return dt.isoformat()
    except (ValueError, TypeError):
        try:
            for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
                try:
                    dt = datetime.strptime(date_str.strip(), fmt).replace(tzinfo=timezone.utc)
                    return dt.isoformat()
                except ValueError:
                    continue
        except Exception:
            pass
        return utc_now_iso()


async def _fetch_gnews(
    client: httpx.AsyncClient,
    query: str,
    api_key: str,
    max_results: int = 10,
) -> list[dict]:
    """Call GNews search API."""
    try:
        resp = await client.get(
            GNEWS_API_URL,
            params={
                "q": query,
                "lang": "en",
                "max": min(max_results, 10),  # GNews free max is 10
                "apikey": api_key,
            },
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        return data.get("articles", [])
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 429:
            logger.warning("gnews_rate_limit query=%s — daily quota exhausted", query[:50])
        elif exc.response.status_code == 403:
            logger.warning("gnews_forbidden — check GNEWS_API_KEY")
        else:
            logger.warning("gnews_http_error query=%s status=%s", query[:50], exc.response.status_code)
        return []
    except Exception as exc:
        logger.warning("gnews_error query=%s error=%s", query[:50], exc)
        return []


async def _collect_one_account(
    conn,
    client: httpx.AsyncClient,
    account: dict,
    api_key: str,
    max_results: int,
    lexicon_rows: list[dict[str, str]],
    source_reliability: float,
) -> tuple[int, int]:
    """Fetch GNews articles for one account and insert matching signals."""
    account_id = str(account["account_id"])
    company_name = str(account.get("company_name") or account.get("domain", ""))
    domain = str(account.get("domain", ""))

    if not company_name and not domain:
        return 0, 0

    source_name = "gnews"
    endpoint = f"gnews:{domain}"
    if db.was_crawled_today(conn, source=source_name, account_id=account_id, endpoint=endpoint):
        return 0, 0

    query = f'"{company_name}"'
    articles = await _fetch_gnews(client, query, api_key, max_results)

    if not articles:
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

    for article in articles:
        title = str(article.get("title", ""))
        description = str(article.get("description", ""))
        _content = str(article.get("content", ""))  # truncated on free tier — not used
        link = str(article.get("url", ""))
        date_str = str(article.get("publishedAt", ""))
        source_info = article.get("source", {})
        news_source = str(source_info.get("name", "")) if isinstance(source_info, dict) else ""

        # Use title + description for classification (content is truncated on free tier)
        text = f"{title}\n{description}".strip()
        if not text:
            continue

        matches = classify_text(text, lexicon_rows)

        if matches:
            observed_at = _parse_date(date_str)
            for signal_code, confidence, matched_keyword in matches:
                seen += 1
                observation = _build_observation(
                    account_id=account_id,
                    signal_code=signal_code,
                    source="gnews",
                    observed_at=observed_at,
                    confidence=confidence,
                    source_reliability=source_reliability,
                    evidence_url=link,
                    evidence_text=text,
                    payload={
                        "title": title,
                        "description": description[:300],
                        "link": link,
                        "date": date_str,
                        "news_source": news_source,
                        "matched_keyword": matched_keyword,
                    },
                )
                if db.insert_signal_observation(conn, observation, commit=False):
                    inserted += 1
        else:
            # Generic company news mention
            seen += 1
            observed_at = _parse_date(date_str)
            observation = _build_observation(
                account_id=account_id,
                signal_code="company_news_mention",
                source="gnews",
                observed_at=observed_at,
                confidence=0.5,
                source_reliability=source_reliability,
                evidence_url=link,
                evidence_text=text,
                payload={
                    "title": title,
                    "description": description[:300],
                    "link": link,
                    "date": date_str,
                    "news_source": news_source,
                },
            )
            if db.insert_signal_observation(conn, observation, commit=False):
                inserted += 1

    db.record_crawl_attempt(
        conn,
        source="gnews",
        account_id=account_id,
        endpoint=endpoint,
        status="success",
        error_summary="",
        commit=False,
    )
    db.mark_crawled(conn, source="gnews", account_id=account_id, endpoint=endpoint, commit=False)

    return inserted, seen


async def collect(
    conn,
    settings: Settings,
    lexicon_rows: list[dict[str, str]],
    source_reliability: float = 0.78,
    account_ids: list[str] | None = None,
) -> dict[str, int]:
    """
    Main entry point: fetch news via GNews.io (100 req/day free).

    Args:
        conn: DB connection
        settings: App settings (needs gnews_api_key)
        lexicon_rows: Keyword lexicon for news classification
        source_reliability: Default reliability for this source
        account_ids: Optional list of specific account IDs to process

    Returns:
        {"inserted": N, "seen": N, "accounts_processed": N}
    """
    api_key = settings.gnews_api_key
    if not api_key:
        logger.warning("gnews_api_key is empty, skipping GNews collection")
        return {"inserted": 0, "seen": 0, "accounts_processed": 0}

    # Free tier: 100 req/day — be conservative (use ~80 per pipeline run)
    max_accounts = min(settings.gnews_max_accounts, 80)

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
        # Prioritize accounts without recent GNews crawl
        accounts = [
            dict(r)
            for r in conn.execute(
                """
                SELECT a.account_id, a.company_name, a.domain
                FROM accounts a
                LEFT JOIN crawl_checkpoints cp
                  ON cp.account_id = a.account_id
                  AND cp.source = 'gnews'
                WHERE COALESCE(a.company_name, '') <> ''
                ORDER BY
                    CASE WHEN cp.last_crawled_at IS NULL THEN 0 ELSE 1 END,
                    cp.last_crawled_at ASC
                LIMIT %s
                """,
                (max_accounts,),
            ).fetchall()
        ]

    if not accounts:
        return {"inserted": 0, "seen": 0, "accounts_processed": 0}

    logger.info("gnews starting accounts=%d", len(accounts))
    t0 = time.monotonic()

    total_inserted = 0
    total_seen = 0
    processed = 0

    # GNews free tier — ~1 req/sec is safe
    concurrency = min(3, len(accounts))
    semaphore = asyncio.Semaphore(concurrency)

    async with httpx.AsyncClient() as client:

        async def _run_one(account: dict) -> tuple[int, int]:
            async with semaphore:
                result = await _collect_one_account(
                    conn=conn,
                    client=client,
                    account=account,
                    api_key=api_key,
                    max_results=10,
                    lexicon_rows=lexicon_rows,
                    source_reliability=source_reliability,
                )
                await asyncio.sleep(0.5)
                return result

        tasks = [_run_one(acct) for acct in accounts]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    conn.commit()

    for result in results:
        if isinstance(result, Exception):
            logger.warning("gnews_worker_error: %s", result)
            continue
        ins, seen = result
        total_inserted += ins
        total_seen += seen
        processed += 1

    dt = time.monotonic() - t0
    logger.info(
        "gnews done accounts=%d inserted=%d seen=%d duration=%.1fs",
        processed,
        total_inserted,
        total_seen,
        dt,
    )

    return {"inserted": total_inserted, "seen": total_seen, "accounts_processed": processed}
