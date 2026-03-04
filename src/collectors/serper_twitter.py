"""Serper.dev Google Search collector — finds Twitter/X posts via Google's index.

This collector uses Google Search (via Serper API) to surface Twitter/X content
that is publicly indexed by Google, using `site:twitter.com OR site:x.com` queries.

Key advantages over direct Twitter API:
- No Twitter API quota consumed
- No Twitter ToS concerns (searching Google's public index)
- Complements RapidAPI coverage for the same accounts
- Finds high-signal tweets that Google has indexed prominently

Source name: serper_twitter
Reliability: 0.60 (lower than twitter_api since results are Google-cached, not real-time)
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

import httpx

from src import db
from src.models import SignalObservation
from src.settings import Settings
from src.utils import classify_text, stable_hash, utc_now_iso

logger = logging.getLogger(__name__)

SERPER_SEARCH_URL = "https://google.serper.dev/search"
SOURCE_NAME = "serper_twitter"

# Broad signal terms covering all high-value signal categories
_TWITTER_SIGNAL_TERMS = (
    "hiring OR devops OR kubernetes OR terraform OR finops "
    'OR "cloud cost" OR "cloud migration" OR "digital transformation" '
    'OR compliance OR soc2 OR "cost reduction" OR "cost optimization" '
    'OR "funding round" OR "series a" OR "series b" '
    'OR "product launch" OR "supply chain" OR "vendor consolidation" '
    'OR "security audit" OR outage OR ERP OR SAP '
    'OR "platform engineering" OR modernization'
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
    obs_id = stable_hash(
        {
            "account_id": account_id,
            "signal_code": signal_code,
            "source": SOURCE_NAME,
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
        source=SOURCE_NAME,
        observed_at=observed_at,
        evidence_url=evidence_url,
        evidence_text=evidence_text[:500],
        confidence=max(0.0, min(1.0, float(confidence))),
        source_reliability=max(0.0, min(1.0, float(source_reliability))),
        raw_payload_hash=raw_hash,
    )


def _lookback_to_tbs(lookback_days: int) -> str:
    """Convert lookback_days to Serper tbs (time-based search) parameter.

    Aligns Serper results with twitter_lookback_days so both sources cover
    the same time window.
      1  day  → qdr:d
      ≤ 7 days → qdr:w  (default — matches Twitter free tier)
      ≤ 30 days → qdr:m
      else    → qdr:y
    """
    if lookback_days <= 1:
        return "qdr:d"
    if lookback_days <= 7:
        return "qdr:w"
    if lookback_days <= 30:
        return "qdr:m"
    return "qdr:y"


async def _fetch_serper_twitter(
    client: httpx.AsyncClient,
    query: str,
    api_key: str,
    num_results: int = 10,
    lookback_days: int = 7,
) -> list[dict]:
    """Call Serper organic search and return results for twitter.com / x.com.

    Uses tbs parameter to restrict results to the same time window as
    twitter_lookback_days — keeps both sources temporally consistent.
    """
    tbs = _lookback_to_tbs(lookback_days)
    try:
        resp = await client.post(
            SERPER_SEARCH_URL,
            json={"q": query, "num": num_results, "tbs": tbs},
            headers={
                "X-API-KEY": api_key,
                "Content-Type": "application/json",
            },
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        # Return only results from twitter.com or x.com
        organic = data.get("organic", [])
        return [
            r
            for r in organic
            if "twitter.com" in str(r.get("link", "")).lower() or "x.com" in str(r.get("link", "")).lower()
        ]
    except httpx.HTTPStatusError as exc:
        logger.warning(
            "serper_twitter_http_error query=%s status=%s",
            query[:60],
            exc.response.status_code,
        )
        return []
    except Exception as exc:
        logger.warning("serper_twitter_error query=%s error=%s", query[:60], exc)
        return []


async def _collect_one_account(
    conn,
    client: httpx.AsyncClient,
    account: dict,
    api_key: str,
    num_results: int,
    lexicon_rows: list[dict[str, str]],
    reliability: float,
    lookback_days: int = 7,
) -> tuple[int, int]:
    """Fetch Google-indexed Twitter results for one account and insert matching signals."""
    account_id = str(account["account_id"])
    company_name = str(account.get("company_name") or account.get("domain", ""))
    domain = str(account.get("domain", ""))

    if not company_name and not domain:
        return 0, 0

    endpoint = f"serper_twitter:{domain}"
    if db.was_crawled_today(conn, source=SOURCE_NAME, account_id=account_id, endpoint=endpoint):
        return 0, 0

    # Build search query — site-restrict to Twitter/X AND include signal keywords
    query = f'(site:twitter.com OR site:x.com) "{company_name}" ({_TWITTER_SIGNAL_TERMS})'

    results = await _fetch_serper_twitter(client, query, api_key, num_results, lookback_days=lookback_days)

    if not results:
        db.record_crawl_attempt(
            conn,
            source=SOURCE_NAME,
            account_id=account_id,
            endpoint=endpoint,
            status="success",
            error_summary="no_results",
            commit=False,
        )
        db.mark_crawled(conn, source=SOURCE_NAME, account_id=account_id, endpoint=endpoint, commit=False)
        return 0, 0

    inserted = 0
    seen = 0

    for item in results:
        title = str(item.get("title", ""))
        snippet = str(item.get("snippet", ""))
        link = str(item.get("link", ""))

        text = f"{title}\n{snippet}".strip()
        if not text:
            continue

        matches = classify_text(text, lexicon_rows)

        if matches:
            for signal_code, confidence, matched_keyword in matches:
                seen += 1
                observation = _build_observation(
                    account_id=account_id,
                    signal_code=signal_code,
                    observed_at=utc_now_iso(),
                    confidence=confidence,
                    source_reliability=reliability,
                    evidence_url=link,
                    evidence_text=text,
                    payload={
                        "title": title,
                        "snippet": snippet,
                        "link": link,
                        "query": query,
                        "matched_keyword": matched_keyword,
                    },
                )
                if db.insert_signal_observation(conn, observation, commit=False):
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
    lexicon_by_source: dict[str, list[dict[str, str]]],
    source_reliability: dict[str, float],
    account_ids: list[str] | None = None,
    db_pool=None,
) -> dict[str, int]:
    """Main entry point for serper_twitter collector.

    Uses Google Search (via Serper API) to find publicly-indexed Twitter/X posts
    that mention a company alongside DevOps/FinOps/platform signal keywords.

    Returns:
        {"inserted": N, "seen": N}
    """
    api_key = settings.serper_api_key
    if not api_key:
        logger.debug("serper_api_key not set, skipping serper_twitter collection")
        return {"inserted": 0, "seen": 0}

    reliability = source_reliability.get(SOURCE_NAME, 0.60)
    if reliability <= 0:
        return {"inserted": 0, "seen": 0}

    # Use twitter-specific lexicon rows; fall back to news rows if none defined
    lexicon_rows = lexicon_by_source.get(SOURCE_NAME, [])
    if not lexicon_rows:
        # Fall back to twitter source rows (same signal keywords)
        lexicon_rows = lexicon_by_source.get("twitter", [])

    if not lexicon_rows:
        logger.warning("serper_twitter_no_lexicon no keyword rows found for source=%s", SOURCE_NAME)
        return {"inserted": 0, "seen": 0}

    max_accounts = getattr(settings, "serper_max_accounts", 50)
    num_results = getattr(settings, "serper_results_per_query", 10)
    # Align time window with twitter_lookback_days (default 7 → qdr:w = past week)
    lookback_days = getattr(settings, "twitter_lookback_days", 7)

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

    logger.info(
        "serper_twitter starting accounts=%d max_results_per=%d",
        len(accounts),
        num_results,
    )
    t0 = time.monotonic()

    total_inserted = 0
    total_seen = 0

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
                    reliability=reliability,
                    lookback_days=lookback_days,
                )
                await asyncio.sleep(0.1)  # light pacing between requests
                return result

        tasks = [_run_one(acct) for acct in accounts]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    conn.commit()

    for result in results:
        if isinstance(result, Exception):
            logger.warning("serper_twitter_worker_error: %s", result)
            continue
        ins, seen = result
        total_inserted += ins
        total_seen += seen

    dt = time.monotonic() - t0
    logger.info(
        "serper_twitter done accounts=%d inserted=%d seen=%d duration=%.1fs",
        len(accounts),
        total_inserted,
        total_seen,
        dt,
    )

    return {"inserted": total_inserted, "seen": total_seen, "accounts_processed": len(accounts)}
