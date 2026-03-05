"""Twitter/X MCP Collector — semantic signal extraction via Serper API + Claude.

Uses Serper's Google Search API (site:twitter.com OR site:x.com) to surface
publicly-indexed tweets, then calls Claude Haiku to semantically classify
buying intent rather than relying on keyword matching.

This improves on serper_twitter.py (reliability 0.60) by replacing brittle
keyword matching with LLM-based semantic intent classification.

Source name: twitter_mcp
Reliability: 0.78

Setup:
  Set SIGNALS_SERPER_API_KEY (shared with existing serper collectors).
  Set SIGNALS_CLAUDE_API_KEY (shared with existing LLM research).
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

SOURCE_NAME = "twitter_mcp"
SOURCE_RELIABILITY = 0.78

SERPER_SEARCH_URL = "https://google.serper.dev/search"

# Broad query terms to find DevOps/FinOps/Platform signal tweets
_TWITTER_SIGNAL_TERMS = (
    "hiring OR devops OR kubernetes OR terraform OR finops "
    'OR "cloud cost" OR "cloud migration" OR "platform engineering" '
    'OR "cost optimization" OR "series a" OR "series b" OR "funding" '
    'OR "product launch" OR "vendor evaluation" OR outage OR SRE'
)

# Intent categories Claude classifies tweets into
INTENT_CATEGORIES = {
    "active_evaluation": 0.85,  # "We're evaluating tools for X" → high confidence
    "pain_signal": 0.75,  # "Our infra is killing us / struggling with X" → medium-high
    "hiring_signal": 0.65,  # "We're hiring DevOps/SRE/FinOps" → medium
    "product_launch": 0.70,  # Company announces new product/feature → medium-high
    "funding_signal": 0.80,  # Funding round announcement → high
    "passing_mention": None,  # No buying intent → skip
}

# Maps intent category → signal_code in our registry
INTENT_TO_SIGNAL = {
    "active_evaluation": "tech_evaluation_intent",
    "pain_signal": "high_intent_phrase_devops_toil",
    "hiring_signal": "devops_role_open",
    "product_launch": "launch_or_scale_event",
    "funding_signal": "recent_funding_event",
}

_CLASSIFY_PROMPT = """\
You are a buying-signal analyst for enterprise infrastructure software (DevOps, Platform Engineering, FinOps).

Analyze the tweet/X post below and classify it.

Return a JSON object with exactly these fields:
{{
  "intent": "<one of: active_evaluation | pain_signal | hiring_signal | product_launch | funding_signal | passing_mention>",
  "confidence": <float 0.0-1.0>,
  "evidence_sentence": "<1-2 sentence summary of the key signal, max 200 chars>",
  "signal_code": "<from: tech_evaluation_intent | high_intent_phrase_devops_toil | devops_role_open | launch_or_scale_event | recent_funding_event | null>"
}}

Definitions:
- active_evaluation: Company or employee is actively comparing or trialling tools for DevOps/Platform/FinOps
- pain_signal: Describes a specific infrastructure, cost, or reliability problem they need to solve
- hiring_signal: Company is hiring for DevOps, SRE, Platform Engineering, or FinOps roles
- product_launch: Company announces a new product, feature, or major milestone relevant to their tech journey
- funding_signal: Funding round, IPO, acquisition, or major investment announcement
- passing_mention: Mentions tech terms but has no buying intent or clear pain — skip this one

Tweet title/snippet: {title}
Full text: {body}
Source URL: {url}
"""


def _lookback_to_tbs(lookback_days: int) -> str:
    if lookback_days <= 1:
        return "qdr:d"
    if lookback_days <= 7:
        return "qdr:w"
    if lookback_days <= 30:
        return "qdr:m"
    return "qdr:y"


async def _fetch_serper_tweets(
    client: httpx.AsyncClient,
    company_name: str,
    api_key: str,
    num_results: int = 10,
    lookback_days: int = 7,
) -> list[dict[str, Any]]:
    """Call Serper organic search and return tweet results for a company."""
    tbs = _lookback_to_tbs(lookback_days)
    query = f'(site:twitter.com OR site:x.com) "{company_name}" ({_TWITTER_SIGNAL_TERMS})'
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
        organic = data.get("organic", [])
        filtered = []
        for r in organic:
            link = str(r.get("link", "")).lower()
            if not ("twitter.com" in link or "x.com" in link):
                continue
            # Only status/tweet URLs and hashtag pages
            if "/status/" in link or "/hashtag/" in link:
                filtered.append(r)
        return filtered
    except httpx.HTTPStatusError as exc:
        logger.warning(
            "twitter_mcp_http_error company=%s status=%s",
            company_name[:40],
            exc.response.status_code,
        )
        return []
    except Exception as exc:
        logger.warning("twitter_mcp_fetch_error company=%s error=%s", company_name[:40], exc)
        return []


async def _classify_with_claude(
    item: dict[str, Any],
    api_key: str,
    client: httpx.AsyncClient,
) -> dict | None:
    """Use Claude Haiku to semantically classify a tweet."""
    title = str(item.get("title", ""))[:300]
    body = str(item.get("snippet", ""))[:600]
    url = str(item.get("link", ""))

    if not title and not body:
        return None

    prompt = _CLASSIFY_PROMPT.format(title=title, body=body, url=url)
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
        # Strip markdown fences if present
        if text.startswith("```"):
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
        return json.loads(text)
    except (json.JSONDecodeError, KeyError, httpx.HTTPError) as exc:
        logger.debug("twitter_mcp_classify_failed url=%s error=%s", url[:80], exc)
        return None


def _make_observation(
    account_id: str,
    classification: dict,
    item: dict[str, Any],
    source_reliability: float,
) -> SignalObservation | None:
    intent = classification.get("intent", "passing_mention")
    if intent == "passing_mention" or intent not in INTENT_TO_SIGNAL:
        return None

    signal_code = classification.get("signal_code") or INTENT_TO_SIGNAL.get(intent)
    if not signal_code:
        return None

    confidence = float(classification.get("confidence", INTENT_CATEGORIES.get(intent, 0.65)))
    evidence_text = str(classification.get("evidence_sentence", ""))[:500]
    evidence_url = str(item.get("link", ""))

    obs_id = stable_hash(
        {"account_id": account_id, "signal_code": signal_code, "source": SOURCE_NAME, "evidence_url": evidence_url},
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
    """Collect Twitter/X signals via Serper API + Claude semantic classification.

    Falls back to empty result if either SERPER_API_KEY or CLAUDE_API_KEY is
    missing — never crashes the pipeline.
    """
    serper_key = settings.serper_api_key
    claude_key = settings.claude_api_key

    if not serper_key:
        logger.info("twitter_mcp: no SIGNALS_SERPER_API_KEY configured, skipping")
        return {"inserted": 0, "seen": 0}

    if not claude_key:
        logger.info("twitter_mcp: no SIGNALS_CLAUDE_API_KEY configured, skipping")
        return {"inserted": 0, "seen": 0}

    source_reliability = (source_reliability_dict or {}).get(SOURCE_NAME, SOURCE_RELIABILITY)
    if source_reliability <= 0:
        return {"inserted": 0, "seen": 0}

    max_accounts = getattr(settings, "serper_max_accounts", 50)
    num_results = getattr(settings, "serper_results_per_query", 10)
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
        logger.info("twitter_mcp: no accounts to scan")
        return {"inserted": 0, "seen": 0}

    logger.info("twitter_mcp: starting accounts=%d", len(accounts))
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
            endpoint = f"twitter_mcp:{domain}"

            if db.was_crawled_today(conn, source=SOURCE_NAME, account_id=account_id, endpoint=endpoint):
                continue

            try:
                tweets = await _fetch_serper_tweets(client, search_name, serper_key, num_results, lookback_days)

                if not tweets:
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

                for item in tweets[:5]:  # Max 5 tweets per account
                    seen += 1
                    classification = await _classify_with_claude(item, claude_key, client)
                    if not classification:
                        continue
                    obs = _make_observation(account_id, classification, item, source_reliability)
                    if obs and db.insert_signal_observation(conn, obs, commit=False):
                        inserted += 1

                    # Rate-limit Claude calls
                    await asyncio.sleep(0.3)

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
                logger.debug("twitter_mcp account=%s error=%s", account_id, exc)
                continue

            await asyncio.sleep(0.2)  # gentle pacing between accounts

    conn.commit()

    dt = time.monotonic() - t0
    logger.info("twitter_mcp: seen=%d inserted=%d duration=%.1fs", seen, inserted, dt)
    return {"inserted": inserted, "seen": seen, "accounts_processed": len(accounts)}
