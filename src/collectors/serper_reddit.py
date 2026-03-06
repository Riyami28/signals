"""Serper.dev Google Search collector — finds Reddit posts via Google's index.

This collector uses Google Search (via Serper API) to surface Reddit content
that is publicly indexed by Google, using `site:reddit.com` queries scoped to
each watchlist company combined with cloud/DevOps/infrastructure keywords.

Key advantages:
- No Reddit API quota consumed
- Finds company-specific discussions on Reddit about cloud/DevOps topics
- Complements existing reddit_collector (RapidAPI) and reddit_official
- Google's ranking surfaces the most relevant threads

Source name: serper_reddit
Reliability: 0.65
"""

from __future__ import annotations

import asyncio
import json
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
SOURCE_NAME = "serper_reddit"

# Broad cloud/DevOps/infrastructure signal terms for Reddit search
_REDDIT_SIGNAL_TERMS = (
    "kubernetes OR devops OR terraform OR finops "
    'OR "cloud cost" OR "cloud migration" OR "platform engineering" '
    'OR "digital transformation" OR modernization OR SRE '
    'OR "cost optimization" OR "vendor consolidation" OR compliance '
    'OR "infrastructure as code" OR docker OR "cloud native" '
    'OR "site reliability" OR microservices OR observability '
    'OR "ci cd" OR "continuous delivery" OR serverless'
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
    # Use evidence_url for dedup (not observed_at) so the same Reddit URL
    # across multiple runs doesn't create duplicate observations.
    obs_id = stable_hash(
        {
            "account_id": account_id,
            "signal_code": signal_code,
            "source": SOURCE_NAME,
            "evidence_url": evidence_url,
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


_RELEVANCE_PROMPT = """\
You are a buying-signal analyst for enterprise infrastructure software (DevOps, Platform Engineering, FinOps).

Target company: {company_name} (domain: {domain})

Is the Reddit post below meaningfully relevant to this specific target company?

Return a JSON object with exactly these fields:
{{
  "relevant": <true | false>,
  "signal_code": "<from: tech_evaluation_intent | infrastructure_pain | cloud_migration_intent | hiring_devops | vendor_evaluation | null>",
  "confidence": <float 0.0-1.0>,
  "evidence_sentence": "<1-2 sentence summary of the key signal, max 200 chars — empty string if not relevant>"
}}

Relevance rules:
- Set relevant=true ONLY if the post is clearly about the target company or authored by someone at the company
- A post is relevant if it explicitly names the company, its products, its domain, or its known subsidiaries
- A post that only mentions the company incidentally (e.g. in a list, in a URL, or in passing) is NOT relevant
- A post about a different company that happens to share a keyword is NOT relevant
- If relevant=false, set signal_code to null and confidence to 0.0

Post title: {title}
Post snippet: {snippet}
Post URL: {url}
"""


async def _claude_relevance_check(
    item: dict[str, Any],
    company_name: str,
    domain: str,
    api_key: str,
    client: httpx.AsyncClient,
) -> dict | None:
    """Use Claude Haiku to verify a Reddit post is actually relevant to the target company."""
    title = str(item.get("title", ""))[:300]
    snippet = str(item.get("snippet", ""))[:500]
    url = str(item.get("link", ""))

    if not title and not snippet:
        return None

    prompt = _RELEVANCE_PROMPT.format(
        company_name=company_name or "unknown",
        domain=domain or "unknown",
        title=title,
        snippet=snippet,
        url=url,
    )
    try:
        resp = await client.post(
            "https://api.anthropic.com/v1/messages",
            json={
                "model": "claude-haiku-4-5-20251001",
                "max_tokens": 200,
                "messages": [{"role": "user", "content": prompt}],
            },
            headers={
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            timeout=20,
        )
        resp.raise_for_status()
        text = resp.json()["content"][0]["text"].strip()
        if text.startswith("```"):
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
        return json.loads(text)
    except (json.JSONDecodeError, KeyError, httpx.HTTPError) as exc:
        logger.debug("serper_reddit_claude_check_failed url=%s error=%s", url[:80], exc)
        return None


def _is_reddit_post(link: str) -> bool:
    """Return True if the URL is a Reddit post/comment, not a profile or wiki page."""
    link_lower = link.lower()
    if "reddit.com" not in link_lower:
        return False
    # Skip non-post pages
    skip_patterns = [
        "/user/",
        "/wiki/",
        "/about/",
        "/search?",
        "/submit",
        "/message/",
        "/settings/",
        "/premium",
        "/coins",
    ]
    for pattern in skip_patterns:
        if pattern in link_lower:
            return False
    return True


async def _fetch_serper_reddit(
    client: httpx.AsyncClient,
    query: str,
    api_key: str,
    num_results: int = 10,
) -> list[dict]:
    """Call Serper organic search and return results from reddit.com only.

    Uses tbs=qdr:m to restrict results to the past month for freshness.
    """
    try:
        resp = await client.post(
            SERPER_SEARCH_URL,
            json={"q": query, "num": num_results, "tbs": "qdr:m"},
            headers={
                "X-API-KEY": api_key,
                "Content-Type": "application/json",
            },
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        organic = data.get("organic", [])
        return [r for r in organic if _is_reddit_post(str(r.get("link", "")))]
    except httpx.HTTPStatusError as exc:
        logger.warning(
            "serper_reddit_http_error query=%s status=%s",
            query[:60],
            exc.response.status_code,
        )
        return []
    except Exception as exc:
        logger.warning("serper_reddit_error query=%s error=%s", query[:60], exc)
        return []


async def _collect_one_account(
    conn,
    client: httpx.AsyncClient,
    account: dict,
    api_key: str,
    num_results: int,
    lexicon_rows: list[dict[str, str]],
    reliability: float,
    claude_api_key: str = "",
) -> tuple[int, int]:
    """Fetch Google-indexed Reddit results for one account and insert matching signals.

    When claude_api_key is set, uses Claude Haiku as a relevance gate before
    inserting observations — eliminates false positives from keyword matching.
    Falls back to keyword-only classification when no Claude key is configured.
    """
    account_id = str(account["account_id"])
    company_name = str(account.get("company_name") or account.get("domain", ""))
    domain = str(account.get("domain", ""))

    if not company_name and not domain:
        return 0, 0

    endpoint = f"serper_reddit:{domain}"
    if db.was_crawled_today(conn, source=SOURCE_NAME, account_id=account_id, endpoint=endpoint):
        return 0, 0

    # Build search query — site-restrict to reddit.com AND include signal keywords
    query = f'site:reddit.com "{company_name}" ({_REDDIT_SIGNAL_TERMS})'

    results = await _fetch_serper_reddit(client, query, api_key, num_results)

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

        if claude_api_key:
            # LLM relevance gate: verify this post is actually about the target company
            verdict = await _claude_relevance_check(item, company_name, domain, claude_api_key, client)
            if not verdict or not verdict.get("relevant"):
                continue

            signal_code = verdict.get("signal_code")
            if not signal_code:
                continue

            confidence = float(verdict.get("confidence", 0.65))
            evidence_text = str(verdict.get("evidence_sentence", text))[:500] or text[:500]

            seen += 1
            observation = _build_observation(
                account_id=account_id,
                signal_code=signal_code,
                observed_at=utc_now_iso(),
                confidence=confidence,
                source_reliability=reliability,
                evidence_url=link,
                evidence_text=evidence_text,
                payload={"title": title, "snippet": snippet, "link": link, "query": query[:100]},
            )
            if db.insert_signal_observation(conn, observation, commit=False):
                inserted += 1
            # Light rate-limit for Claude calls
            await asyncio.sleep(0.3)
        else:
            # Fallback: keyword-based classification (less precise)
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
                            "query": query[:100],
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
    """Main entry point for serper_reddit collector.

    Uses Google Search (via Serper API) to find publicly-indexed Reddit posts
    that mention a company alongside cloud/DevOps/infrastructure signal keywords.

    Returns:
        {"inserted": N, "seen": N, "accounts_processed": N}
    """
    api_key = settings.serper_api_key
    if not api_key:
        logger.debug("serper_api_key not set, skipping serper_reddit collection")
        return {"inserted": 0, "seen": 0, "accounts_processed": 0}

    reliability = source_reliability.get(SOURCE_NAME, 0.65)
    if reliability <= 0:
        return {"inserted": 0, "seen": 0, "accounts_processed": 0}

    # Use serper_reddit lexicon; fall back to serper_twitter (broad cloud/devops keywords)
    # then community if neither exists
    lexicon_rows = lexicon_by_source.get(SOURCE_NAME, [])
    if not lexicon_rows:
        lexicon_rows = lexicon_by_source.get("serper_twitter", [])
    if not lexicon_rows:
        lexicon_rows = lexicon_by_source.get("community", [])

    if not lexicon_rows:
        logger.warning("serper_reddit_no_lexicon no keyword rows found for source=%s", SOURCE_NAME)
        return {"inserted": 0, "seen": 0, "accounts_processed": 0}

    max_accounts = getattr(settings, "serper_max_accounts", 50)
    num_results = getattr(settings, "serper_results_per_query", 10)

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
                """SELECT a.account_id, a.company_name, a.domain
                   FROM accounts a
                   LEFT JOIN crawl_checkpoints cp
                     ON cp.account_id = a.account_id AND cp.source = 'serper_reddit'
                   WHERE COALESCE(a.domain, '') <> ''
                   ORDER BY CASE WHEN cp.last_crawled_at IS NULL THEN 0 ELSE 1 END,
                            cp.last_crawled_at ASC, a.company_name ASC
                   LIMIT %s""",
                (max_accounts,),
            ).fetchall()
        ]

    if not accounts:
        return {"inserted": 0, "seen": 0, "accounts_processed": 0}

    logger.info(
        "serper_reddit starting accounts=%d max_results_per=%d",
        len(accounts),
        num_results,
    )
    t0 = time.monotonic()

    total_inserted = 0
    total_seen = 0

    claude_api_key = settings.claude_api_key or ""
    if claude_api_key:
        logger.info("serper_reddit: Claude relevance gate enabled")

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
                    claude_api_key=claude_api_key,
                )
                await asyncio.sleep(0.1)  # light pacing between requests
                return result

        tasks = [_run_one(acct) for acct in accounts]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    conn.commit()

    processed = 0
    for result in results:
        if isinstance(result, Exception):
            logger.warning("serper_reddit_worker_error: %s", result)
            continue
        ins, seen = result
        total_inserted += ins
        total_seen += seen
        processed += 1

    dt = time.monotonic() - t0
    logger.info(
        "serper_reddit done accounts=%d inserted=%d seen=%d duration=%.1fs",
        processed,
        total_inserted,
        total_seen,
        dt,
    )

    return {"inserted": total_inserted, "seen": total_seen, "accounts_processed": processed}
