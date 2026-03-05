"""HackerNews MCP Collector — DevOps/infrastructure signals via HN Algolia API + Claude.

Uses the free HN Algolia search API (no auth required) to find:
- "Who is Hiring?" threads — hiring signals for DevOps, SRE, FinOps roles
- "Ask HN" infrastructure discussions — pain signals, tool evaluations
- Company-specific mentions in HN posts and comments

Then calls Claude Haiku to semantically classify buying intent rather than
relying on keyword matching alone.

Source name: hackernews_mcp
Reliability: 0.70
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
from src.utils import stable_hash, utc_now_iso

logger = logging.getLogger(__name__)

SOURCE_NAME = "hackernews_mcp"
SOURCE_RELIABILITY = 0.70

HN_ALGOLIA_SEARCH = "https://hn.algolia.com/api/v1/search"

# DevOps/infra keywords used to scope HN search results
_HN_SIGNAL_TERMS = (
    "devops OR kubernetes OR terraform OR finops OR "
    '"cloud cost" OR "cloud migration" OR "platform engineering" OR '
    '"cost optimization" OR "infrastructure" OR SRE OR '
    '"site reliability" OR "series a" OR "series b" OR funding OR '
    '"product launch" OR "vendor evaluation" OR outage OR microservices'
)

# Intent categories Claude classifies HN posts into
INTENT_CATEGORIES = {
    "active_evaluation": 0.85,
    "infrastructure_pain": 0.75,
    "hiring_signal": 0.65,
    "funding_signal": 0.80,
    "tool_launch": 0.70,
    "passing_mention": None,
}

INTENT_TO_SIGNAL = {
    "active_evaluation": "tech_evaluation_intent",
    "infrastructure_pain": "infrastructure_pain",
    "hiring_signal": "devops_role_open",
    "funding_signal": "recent_funding_event",
    "tool_launch": "launch_or_scale_event",
}

_CLASSIFY_PROMPT = """\
You are a buying-signal analyst for enterprise infrastructure software (DevOps, Platform Engineering, FinOps).

Target company: {company_name} (domain: {domain})

Analyze the HackerNews post below and classify it.

Return a JSON object with exactly these fields:
{{
  "relevant": <true | false>,
  "intent": "<one of: active_evaluation | infrastructure_pain | hiring_signal | funding_signal | tool_launch | passing_mention>",
  "confidence": <float 0.0-1.0>,
  "evidence_sentence": "<1-2 sentence summary of the key signal, max 200 chars>",
  "signal_code": "<from: tech_evaluation_intent | infrastructure_pain | devops_role_open | recent_funding_event | launch_or_scale_event | null>"
}}

Relevance rules:
- Set relevant=true ONLY if the post is clearly about the target company or authored by someone at the company
- A post about a different company that happens to share a keyword is NOT relevant
- If relevant=false, set intent to passing_mention, signal_code to null, confidence to 0.0

Definitions:
- active_evaluation: Company or employee is actively comparing or trialling DevOps/Platform/FinOps tools
- infrastructure_pain: Describes a specific infrastructure, cost, or reliability problem they need to solve
- hiring_signal: Company is hiring for DevOps, SRE, Platform Engineering, or FinOps roles
- funding_signal: Funding round, IPO, acquisition, or major investment announcement
- tool_launch: Company announces a new product, feature, or major milestone
- passing_mention: Mentions company but no clear buying intent

Post title: {title}
Post text/comment: {body}
Source URL: {url}
"""


async def _fetch_hn_posts(
    client: httpx.AsyncClient,
    company_name: str,
    lookback_days: int = 30,
    num_results: int = 10,
) -> list[dict[str, Any]]:
    """Search HN Algolia for posts mentioning a company alongside DevOps/infra terms."""
    query = f'"{company_name}" ({_HN_SIGNAL_TERMS})'
    # numericFilters: created_at_i > N days ago
    cutoff_ts = int(time.time()) - (lookback_days * 86400)

    try:
        resp = await client.get(
            HN_ALGOLIA_SEARCH,
            params={
                "query": query,
                "tags": "(story,comment)",
                "numericFilters": f"created_at_i>{cutoff_ts}",
                "hitsPerPage": num_results,
            },
            timeout=15,
        )
        resp.raise_for_status()
        hits = resp.json().get("hits", [])
        results = []
        for hit in hits:
            object_id = hit.get("objectID", "")
            story_id = hit.get("story_id") or hit.get("parent_id") or object_id
            url = f"https://news.ycombinator.com/item?id={object_id}"
            results.append(
                {
                    "title": hit.get("title") or hit.get("story_title") or "",
                    "body": (hit.get("comment_text") or hit.get("story_text") or "")[:600],
                    "url": url,
                    "story_id": story_id,
                    "author": hit.get("author", ""),
                    "created_at": hit.get("created_at", ""),
                }
            )
        return results
    except httpx.HTTPStatusError as exc:
        logger.warning(
            "hackernews_mcp_http_error company=%s status=%s",
            company_name[:40],
            exc.response.status_code,
        )
        return []
    except Exception as exc:
        logger.warning("hackernews_mcp_fetch_error company=%s error=%s", company_name[:40], exc)
        return []


async def _classify_with_claude(
    item: dict[str, Any],
    company_name: str,
    domain: str,
    api_key: str,
    client: httpx.AsyncClient,
) -> dict | None:
    """Use Claude Haiku to semantically classify an HN post."""
    title = str(item.get("title", ""))[:300]
    body = str(item.get("body", ""))[:600]
    url = str(item.get("url", ""))

    if not title and not body:
        return None

    prompt = _CLASSIFY_PROMPT.format(
        company_name=company_name or "unknown",
        domain=domain or "unknown",
        title=title,
        body=body,
        url=url,
    )
    try:
        resp = await client.post(
            "https://api.anthropic.com/v1/messages",
            json={
                "model": "claude-haiku-4-5-20251001",
                "max_tokens": 256,
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
        logger.debug("hackernews_mcp_classify_failed url=%s error=%s", url[:80], exc)
        return None


def _make_observation(
    account_id: str,
    classification: dict,
    item: dict[str, Any],
    source_reliability: float,
) -> SignalObservation | None:
    if not classification.get("relevant", False):
        return None

    intent = classification.get("intent", "passing_mention")
    if intent == "passing_mention" or intent not in INTENT_TO_SIGNAL:
        return None

    signal_code = classification.get("signal_code") or INTENT_TO_SIGNAL.get(intent)
    if not signal_code:
        return None

    confidence = float(classification.get("confidence", INTENT_CATEGORIES.get(intent, 0.65)))
    evidence_text = str(classification.get("evidence_sentence", ""))[:500]
    evidence_url = str(item.get("url", ""))

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
        observed_at=utc_now_iso(),
        evidence_url=evidence_url,
        evidence_text=evidence_text,
        confidence=max(0.0, min(1.0, confidence)),
        source_reliability=max(0.0, min(1.0, source_reliability)),
        raw_payload_hash=stable_hash(item, prefix="raw"),
    )


async def collect(
    conn,
    settings: Settings,
    lexicon_by_source: dict | None = None,
    source_reliability_dict: dict | None = None,
    account_ids: list[str] | None = None,
    db_pool=None,
) -> dict[str, int]:
    """Collect HackerNews signals via Algolia API + Claude semantic classification.

    Falls back to empty result if CLAUDE_API_KEY is missing.
    """
    claude_key = settings.claude_api_key
    if not claude_key:
        logger.info("hackernews_mcp: no SIGNALS_CLAUDE_API_KEY configured, skipping")
        return {"inserted": 0, "seen": 0, "accounts_processed": 0}

    source_reliability = (source_reliability_dict or {}).get(SOURCE_NAME, SOURCE_RELIABILITY)
    if source_reliability <= 0:
        return {"inserted": 0, "seen": 0, "accounts_processed": 0}

    max_accounts = getattr(settings, "serper_max_accounts", 50)
    num_results = getattr(settings, "serper_results_per_query", 10)
    lookback_days = getattr(settings, "hn_lookback_days", 30)

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
                     ON cp.account_id = a.account_id AND cp.source = %s
                   WHERE COALESCE(a.domain, '') <> ''
                   ORDER BY CASE WHEN cp.last_crawled_at IS NULL THEN 0 ELSE 1 END,
                            cp.last_crawled_at ASC, a.company_name ASC
                   LIMIT %s""",
                (SOURCE_NAME, max_accounts),
            ).fetchall()
        ]

    if not accounts:
        logger.info("hackernews_mcp: no accounts to scan")
        return {"inserted": 0, "seen": 0, "accounts_processed": 0}

    logger.info("hackernews_mcp: starting accounts=%d", len(accounts))
    t0 = time.monotonic()

    seen = 0
    inserted = 0

    async with httpx.AsyncClient() as client:
        for account in accounts:
            account_id = str(account["account_id"])
            company_name = str(account.get("company_name", ""))
            domain = str(account.get("domain", ""))

            if not company_name and not domain:
                continue

            search_name = company_name or domain
            endpoint = f"hackernews_mcp:{domain}"

            if db.was_crawled_today(conn, source=SOURCE_NAME, account_id=account_id, endpoint=endpoint):
                continue

            try:
                posts = await _fetch_hn_posts(client, search_name, lookback_days, num_results)

                if not posts:
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
                    continue

                for item in posts[:5]:  # max 5 posts per account
                    seen += 1
                    classification = await _classify_with_claude(item, company_name, domain, claude_key, client)
                    if not classification:
                        continue
                    obs = _make_observation(account_id, classification, item, source_reliability)
                    if obs and db.insert_signal_observation(conn, obs, commit=False):
                        inserted += 1

                    await asyncio.sleep(0.3)  # rate-limit Claude calls

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

            except Exception as exc:
                logger.debug("hackernews_mcp account=%s error=%s", account_id, exc)
                continue

            await asyncio.sleep(0.2)  # gentle pacing between accounts

    conn.commit()

    dt = time.monotonic() - t0
    logger.info("hackernews_mcp: seen=%d inserted=%d duration=%.1fs", seen, inserted, dt)
    return {"inserted": inserted, "seen": seen, "accounts_processed": len(accounts)}
