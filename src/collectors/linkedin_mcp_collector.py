"""LinkedIn MCP Collector — semantic signal extraction via Serper API + Claude.

Uses Serper's Google Search API (site:linkedin.com) to surface
publicly-indexed LinkedIn posts, job listings, and company updates,
then calls Claude Haiku to semantically classify buying intent.

Covers hiring signals, executive changes, headcount growth, funding
announcements, and technology evaluation signals from LinkedIn.

Source name: linkedin_mcp
Reliability: 0.75

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

SOURCE_NAME = "linkedin_mcp"
SOURCE_RELIABILITY = 0.75

SERPER_SEARCH_URL = "https://google.serper.dev/search"

# Three query types to maximise LinkedIn signal coverage
_LINKEDIN_QUERY_HIRING = (
    '"hiring" OR "we\'re hiring" OR "join our team" OR "open role" '
    'OR "devops" OR "SRE" OR "platform engineer" OR "finops" OR "cloud engineer"'
)

_LINKEDIN_QUERY_EXEC = (
    '"new role" OR "excited to announce" OR "joined" OR "appointed" '
    'OR "promoted" OR "CTO" OR "VP Engineering" OR "Head of" OR "Chief"'
)

_LINKEDIN_QUERY_COMPANY = (
    '"series a" OR "series b" OR "funding" OR "raised" '
    'OR "product launch" OR "cloud migration" OR "kubernetes" '
    'OR "cost optimization" OR "vendor evaluation" OR "platform engineering"'
)

_LINKEDIN_QUERIES = [
    _LINKEDIN_QUERY_HIRING,
    _LINKEDIN_QUERY_EXEC,
    _LINKEDIN_QUERY_COMPANY,
]

# Intent categories Claude classifies LinkedIn posts into
INTENT_CATEGORIES = {
    "hiring_signal": 0.65,
    "exec_change": 0.75,
    "pain_signal": 0.75,
    "growth_signal": 0.70,
    "funding_signal": 0.80,
    "product_launch": 0.70,
    "active_evaluation": 0.85,
    "passing_mention": None,
}

# Maps intent category → signal_code in our registry (all existing codes)
INTENT_TO_SIGNAL = {
    "hiring_signal": "devops_role_open",
    "exec_change": "launch_or_scale_event",
    "pain_signal": "high_intent_phrase_devops_toil",
    "growth_signal": "employee_growth_positive",
    "funding_signal": "recent_funding_event",
    "product_launch": "launch_or_scale_event",
    "active_evaluation": "finops_tool_eval",
}

_CLASSIFY_PROMPT = """\
You are a buying-signal analyst for enterprise infrastructure software (DevOps, Platform Engineering, FinOps).

Analyze the LinkedIn post/page below and classify it.

Return a JSON object with exactly these fields:
{{
  "intent": "<one of: hiring_signal | exec_change | pain_signal | growth_signal | funding_signal | product_launch | active_evaluation | passing_mention>",
  "confidence": <float 0.0-1.0>,
  "evidence_sentence": "<1-2 sentence summary of the key signal, max 200 chars>",
  "signal_code": "<from: devops_role_open | launch_or_scale_event | high_intent_phrase_devops_toil | employee_growth_positive | recent_funding_event | finops_tool_eval | null>",
  "person_name": "<name of the person mentioned, or null if not applicable>",
  "person_role": "<job title/role of the person, or null if not applicable>"
}}

Definitions:
- hiring_signal: Company is hiring for DevOps, SRE, Platform Engineering, FinOps, or cloud infrastructure roles
- exec_change: Executive or senior leader (VP+, C-level, Director) joined, was promoted, or changed roles — indicates org change
- pain_signal: Describes a specific infrastructure, cost, or reliability problem they need to solve
- growth_signal: Company is growing headcount significantly, expanding teams, or opening new offices
- funding_signal: Funding round, IPO, acquisition, or major investment announcement
- product_launch: Company announces a new product, feature, or major milestone relevant to their tech journey
- active_evaluation: Company or employee is actively comparing or trialling tools for DevOps/Platform/FinOps
- passing_mention: Mentions tech terms but has no buying intent or clear pain — skip this one

LinkedIn title/snippet: {title}
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


async def _fetch_serper_linkedin(
    client: httpx.AsyncClient,
    company_name: str,
    api_key: str,
    num_results: int = 10,
    lookback_days: int = 7,
) -> list[dict[str, Any]]:
    """Call Serper organic search with multiple query types and return LinkedIn results."""
    tbs = _lookback_to_tbs(lookback_days)
    seen_links: set[str] = set()
    all_results: list[dict[str, Any]] = []

    for query_terms in _LINKEDIN_QUERIES:
        query = f'site:linkedin.com "{company_name}" ({query_terms})'
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
            for r in organic:
                link = str(r.get("link", "")).lower()
                if "linkedin.com" not in link:
                    continue
                # Accept posts, jobs, pulse articles, and company pages
                if not any(seg in link for seg in ("/posts/", "/pulse/", "/jobs/", "/in/", "/company/", "/feed/")):
                    continue
                if link in seen_links:
                    continue
                seen_links.add(link)
                all_results.append(r)
        except httpx.HTTPStatusError as exc:
            logger.warning(
                "linkedin_mcp_http_error company=%s status=%s query_type=%d",
                company_name[:40],
                exc.response.status_code,
                _LINKEDIN_QUERIES.index(query_terms),
            )
        except Exception as exc:
            logger.warning("linkedin_mcp_fetch_error company=%s error=%s", company_name[:40], exc)

        # Gentle pacing between Serper calls
        await asyncio.sleep(0.2)

    return all_results


async def _classify_with_claude(
    item: dict[str, Any],
    api_key: str,
    client: httpx.AsyncClient,
) -> dict | None:
    """Use Claude Haiku to semantically classify a LinkedIn post."""
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
        logger.debug("linkedin_mcp_classify_failed url=%s error=%s", url[:80], exc)
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


def _track_person(
    conn,
    account_id: str,
    classification: dict,
    item: dict[str, Any],
) -> None:
    """Track executive changes in people_watchlist and people_activity."""
    person_name = str(classification.get("person_name") or "")[:200]
    person_role = str(classification.get("person_role") or "")[:120]

    if not person_name or not person_role:
        return

    evidence_url = str(item.get("link", ""))
    try:
        db.upsert_people_watchlist_entry(
            conn,
            account_id=account_id,
            person_name=person_name,
            role_title=person_role,
            role_weight=_role_weight(person_role),
            source_url=evidence_url,
            is_active=True,
            commit=False,
        )
        db.insert_people_activity(
            conn,
            account_id=account_id,
            person_name=person_name,
            role_title=person_role,
            document_id=stable_hash({"url": evidence_url, "source": SOURCE_NAME}, prefix="doc"),
            activity_type="linkedin_post",
            summary=str(classification.get("evidence_sentence", ""))[:200],
            published_at=utc_now_iso(),
            url=evidence_url,
            commit=False,
        )
    except Exception as exc:
        logger.debug("linkedin_mcp: people tracking failed account=%s error=%s", account_id, exc)


def _role_weight(role: str) -> float:
    """Assign weight multiplier based on seniority of the role."""
    r = role.lower()
    if any(k in r for k in ("cto", "cio", "ceo", "chief")):
        return 1.8
    if any(k in r for k in ("vp", "vice president", "founder")):
        return 1.5
    if any(k in r for k in ("director", "head")):
        return 1.3
    if any(k in r for k in ("senior", "staff", "principal", "lead")):
        return 1.1
    return 1.0


async def collect(
    conn,
    settings: Settings,
    lexicon_by_source: dict | None = None,
    source_reliability_dict: dict | None = None,
    account_ids: list[str] | None = None,
    db_pool=None,
) -> dict[str, int]:
    """Collect LinkedIn signals via Serper API + Claude semantic classification.

    Falls back to empty result if either SERPER_API_KEY or CLAUDE_API_KEY is
    missing — never crashes the pipeline.
    """
    serper_key = settings.serper_api_key
    claude_key = settings.claude_api_key

    if not serper_key:
        logger.info("linkedin_mcp: no SIGNALS_SERPER_API_KEY configured, skipping")
        return {"inserted": 0, "seen": 0}

    if not claude_key:
        logger.info("linkedin_mcp: no SIGNALS_CLAUDE_API_KEY configured, skipping")
        return {"inserted": 0, "seen": 0}

    source_reliability = (source_reliability_dict or {}).get(SOURCE_NAME, SOURCE_RELIABILITY)
    if source_reliability <= 0:
        return {"inserted": 0, "seen": 0}

    max_accounts = getattr(settings, "serper_max_accounts", 50)
    num_results = getattr(settings, "serper_results_per_query", 10)
    lookback_days = getattr(settings, "linkedin_lookback_days", 7)

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
        logger.info("linkedin_mcp: no accounts to scan")
        return {"inserted": 0, "seen": 0}

    logger.info("linkedin_mcp: starting accounts=%d", len(accounts))
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
            endpoint = f"linkedin_mcp:{domain}"

            if db.was_crawled_today(conn, source=SOURCE_NAME, account_id=account_id, endpoint=endpoint):
                continue

            try:
                posts = await _fetch_serper_linkedin(client, search_name, serper_key, num_results, lookback_days)

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

                for item in posts[:5]:  # Max 5 posts per account
                    seen += 1
                    classification = await _classify_with_claude(item, claude_key, client)
                    if not classification:
                        continue

                    obs = _make_observation(account_id, classification, item, source_reliability)
                    if obs and db.insert_signal_observation(conn, obs, commit=False):
                        inserted += 1

                    # Track people for exec_change signals
                    intent = classification.get("intent")
                    if intent == "exec_change":
                        _track_person(conn, account_id, classification, item)

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
                logger.debug("linkedin_mcp account=%s error=%s", account_id, exc)
                continue

            await asyncio.sleep(0.2)  # gentle pacing between accounts

    conn.commit()

    dt = time.monotonic() - t0
    logger.info("linkedin_mcp: seen=%d inserted=%d duration=%.1fs", seen, inserted, dt)
    return {"inserted": inserted, "seen": seen, "accounts_processed": len(accounts)}
