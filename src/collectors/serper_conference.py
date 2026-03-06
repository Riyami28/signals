"""Serper.dev Google Search collector — finds conference/event attendance signals.

Uses Google Search (via Serper API) to find evidence of companies attending,
sponsoring, or speaking at tech conferences (KubeCon, AWS re:Invent, etc.).

Key advantages:
- No manual CSV entry needed — auto-discovers conference signals from the web
- Finds sponsor lists, speaker announcements, blog posts about conferences
- Claude Haiku verifies relevance + classifies signal type
- Complements CSV-based conference_events collector

Source name: serper_conference
Reliability: 0.75
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

SERPER_SEARCH_URL = "https://google.serper.dev/search"
SOURCE_NAME = "serper_conference"

# Major tech conferences to search for (combined in queries)
_CONFERENCE_TERMS = (
    'KubeCon OR "AWS re:Invent" OR "Google Cloud Next" OR "Microsoft Ignite" '
    'OR "Microsoft Build" OR HashiConf OR DockerCon OR GopherCon '
    'OR "DevOps Days" OR "Platform Summit" OR "SREcon" '
    'OR "CloudNativeCon" OR "Open Source Summit" OR "Gartner IT" '
    'OR "PlatformCon" OR "DevOps Enterprise Summit" OR "Config Management Camp" '
    'OR "Monitorama" OR "QCon" OR "CNCF" OR "Cloud Expo"'
)

# Conference participation signals in web content
_PARTICIPATION_TERMS = (
    'sponsor OR sponsoring OR "gold sponsor" OR "platinum sponsor" '
    'OR "silver sponsor" OR "diamond sponsor" '
    'OR speaker OR speaking OR keynote OR "tech talk" OR panelist '
    'OR attending OR booth OR exhibit OR "conference booth" '
    'OR "will be at" OR "join us at" OR "see us at" OR "meet us at" '
    'OR "proud sponsor" OR "excited to sponsor"'
)

# Valid signal codes for conference collector
VALID_SIGNAL_CODES = {
    "conference_attendance",
    "conference_sponsorship",
    "conference_speaking",
}

_RELEVANCE_PROMPT = """\
You are a buying-signal analyst for enterprise infrastructure software (DevOps, Platform Engineering, FinOps).

Target company: {company_name} (domain: {domain})

Determine if this web page provides evidence that the TARGET COMPANY is participating in a tech conference or event.

Return a JSON object with exactly these fields:
{{
  "relevant": <true | false>,
  "signal_code": "<from: conference_attendance | conference_sponsorship | conference_speaking | null>",
  "confidence": <float 0.0-1.0>,
  "event_name": "<name of the conference/event if mentioned, else empty string>",
  "evidence_sentence": "<1-2 sentence summary of the participation evidence, max 200 chars — empty if not relevant>"
}}

Classification rules:
- conference_sponsorship: company is a named sponsor (gold, platinum, silver, diamond, etc.) of the event
- conference_speaking: a company employee is speaking, presenting, or on a panel at the event
- conference_attendance: company is attending, has a booth, or is exhibiting (but NOT sponsoring or speaking)
- Set relevant=true ONLY if the target company is explicitly named as participating
- A page that only lists the conference without naming the company is NOT relevant
- A page about a different company that happens to mention the target is NOT relevant
- If the evidence is ambiguous or unclear, set relevant=false
- If relevant=false, set signal_code to null and confidence to 0.0

Web page title: {title}
Web page snippet: {snippet}
Web page URL: {url}
"""


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


async def _claude_classify(
    item: dict[str, Any],
    company_name: str,
    domain: str,
    api_key: str,
    client: httpx.AsyncClient,
) -> dict | None:
    """Use Claude Haiku to classify whether a search result is a conference signal."""
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
                "max_tokens": 250,
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
        # Strip markdown code fences if present
        if text.startswith("```"):
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
        return json.loads(text)
    except (json.JSONDecodeError, KeyError, httpx.HTTPError) as exc:
        logger.debug("serper_conference_classify_failed url=%s error=%s", url[:80], exc)
        return None


async def _fetch_serper(
    client: httpx.AsyncClient,
    query: str,
    api_key: str,
    num_results: int = 10,
) -> list[dict]:
    """Call Serper organic search and return results.

    Uses tbs=qdr:m (past month) for freshness — conferences are time-sensitive.
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
        # Filter out social media posts (those are handled by serper_twitter/reddit)
        filtered = []
        for r in organic:
            link = str(r.get("link", "")).lower()
            if "twitter.com" in link or "x.com" in link:
                continue
            if "reddit.com" in link:
                continue
            filtered.append(r)
        return filtered
    except httpx.HTTPStatusError as exc:
        logger.warning(
            "serper_conference_http_error query=%s status=%s",
            query[:60],
            exc.response.status_code,
        )
        return []
    except Exception as exc:
        logger.warning("serper_conference_error query=%s error=%s", query[:60], exc)
        return []


async def _collect_one_account(
    conn,
    client: httpx.AsyncClient,
    account: dict,
    serper_api_key: str,
    claude_api_key: str,
    num_results: int,
    reliability: float,
) -> tuple[int, int]:
    """Fetch conference participation evidence for one account."""
    account_id = str(account["account_id"])
    company_name = str(account.get("company_name") or account.get("domain", ""))
    domain = str(account.get("domain", ""))

    if not company_name and not domain:
        return 0, 0

    endpoint = f"serper_conference:{domain}"
    if db.was_crawled_today(conn, source=SOURCE_NAME, account_id=account_id, endpoint=endpoint):
        return 0, 0

    inserted = 0
    seen = 0

    # Query 1: Company + conference names
    q1 = f'"{company_name}" ({_CONFERENCE_TERMS})'
    # Query 2: Company + participation keywords
    q2 = f'"{company_name}" conference ({_PARTICIPATION_TERMS})'

    seen_links: set[str] = set()

    for query in (q1, q2):
        results = await _fetch_serper(client, query, serper_api_key, num_results)

        for item in results:
            link = str(item.get("link", ""))
            if not link:
                continue

            link_key = link.lower().rstrip("/")
            if link_key in seen_links:
                continue
            seen_links.add(link_key)

            if claude_api_key:
                # LLM classification: verify + classify conference signal
                verdict = await _claude_classify(item, company_name, domain, claude_api_key, client)
                if not verdict or not verdict.get("relevant"):
                    continue

                signal_code = verdict.get("signal_code")
                if not signal_code or signal_code not in VALID_SIGNAL_CODES:
                    continue

                confidence = float(verdict.get("confidence", 0.70))
                event_name = str(verdict.get("event_name", ""))
                evidence_sentence = str(verdict.get("evidence_sentence", ""))

                # Build evidence text from LLM output
                evidence_parts = []
                if event_name:
                    evidence_parts.append(event_name)
                if evidence_sentence:
                    evidence_parts.append(evidence_sentence)
                evidence_text = " — ".join(evidence_parts) if evidence_parts else str(item.get("title", ""))

                seen += 1
                observation = _build_observation(
                    account_id=account_id,
                    signal_code=signal_code,
                    observed_at=utc_now_iso(),
                    confidence=confidence,
                    source_reliability=reliability,
                    evidence_url=link,
                    evidence_text=evidence_text,
                    payload={
                        "title": str(item.get("title", "")),
                        "snippet": str(item.get("snippet", "")),
                        "link": link,
                        "query": query[:100],
                        "event_name": event_name,
                        "llm_signal": signal_code,
                    },
                )
                if db.insert_signal_observation(conn, observation, commit=False):
                    inserted += 1

                # Rate-limit for Claude calls
                await asyncio.sleep(0.3)
            else:
                # Fallback without Claude: keyword-based heuristic
                title = str(item.get("title", "")).lower()
                snippet = str(item.get("snippet", "")).lower()
                text = f"{title} {snippet}"

                # Simple keyword classification
                signal_code = None
                if any(kw in text for kw in ("sponsor", "sponsoring", "proud sponsor", "excited to sponsor")):
                    signal_code = "conference_sponsorship"
                elif any(
                    kw in text for kw in ("speaker", "speaking", "keynote", "panelist", "tech talk", "presenting")
                ):
                    signal_code = "conference_speaking"
                elif any(kw in text for kw in ("attending", "booth", "exhibit", "join us", "see us", "meet us")):
                    signal_code = "conference_attendance"

                if not signal_code:
                    continue

                seen += 1
                observation = _build_observation(
                    account_id=account_id,
                    signal_code=signal_code,
                    observed_at=utc_now_iso(),
                    confidence=0.60,
                    source_reliability=reliability,
                    evidence_url=link,
                    evidence_text=str(item.get("title", ""))[:500],
                    payload={
                        "title": str(item.get("title", "")),
                        "snippet": str(item.get("snippet", "")),
                        "link": link,
                        "query": query[:100],
                    },
                )
                if db.insert_signal_observation(conn, observation, commit=False):
                    inserted += 1

        # Brief pause between queries for same account
        await asyncio.sleep(0.05)

    db.record_crawl_attempt(
        conn,
        source=SOURCE_NAME,
        account_id=account_id,
        endpoint=endpoint,
        status="success",
        error_summary="" if seen else "no_results",
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
    """Main entry point for serper_conference collector.

    Uses Google Search (via Serper API) to find evidence of companies attending,
    sponsoring, or speaking at tech conferences. Claude Haiku classifies results.

    Returns:
        {"inserted": N, "seen": N, "accounts_processed": N}
    """
    del lexicon_by_source, db_pool  # not used — Claude handles classification

    api_key = settings.serper_api_key
    if not api_key:
        logger.debug("serper_api_key not set, skipping serper_conference collection")
        return {"inserted": 0, "seen": 0, "accounts_processed": 0}

    reliability = source_reliability.get(SOURCE_NAME, 0.75)
    if reliability <= 0:
        return {"inserted": 0, "seen": 0, "accounts_processed": 0}

    claude_api_key = settings.claude_api_key or ""
    if claude_api_key:
        logger.info("serper_conference: Claude classification enabled")
    else:
        logger.info("serper_conference: keyword-only mode (no Claude key)")

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
                     ON cp.account_id = a.account_id AND cp.source = %s
                   WHERE COALESCE(a.domain, '') <> ''
                   ORDER BY CASE WHEN cp.last_crawled_at IS NULL THEN 0 ELSE 1 END,
                            cp.last_crawled_at ASC, a.company_name ASC
                   LIMIT %s""",
                (SOURCE_NAME, max_accounts),
            ).fetchall()
        ]

    if not accounts:
        return {"inserted": 0, "seen": 0, "accounts_processed": 0}

    logger.info(
        "serper_conference starting accounts=%d max_results_per=%d",
        len(accounts),
        num_results,
    )
    t0 = time.monotonic()

    total_inserted = 0
    total_seen = 0

    concurrency = min(5, len(accounts))
    semaphore = asyncio.Semaphore(concurrency)

    async with httpx.AsyncClient() as client:

        async def _run_one(account: dict) -> tuple[int, int]:
            async with semaphore:
                result = await _collect_one_account(
                    conn=conn,
                    client=client,
                    account=account,
                    serper_api_key=api_key,
                    claude_api_key=claude_api_key,
                    num_results=num_results,
                    reliability=reliability,
                )
                await asyncio.sleep(0.1)  # pacing between accounts
                return result

        tasks = [_run_one(acct) for acct in accounts]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    conn.commit()

    processed = 0
    for result in results:
        if isinstance(result, Exception):
            logger.warning("serper_conference_worker_error: %s", result)
            continue
        ins, seen = result
        total_inserted += ins
        total_seen += seen
        processed += 1

    dt = time.monotonic() - t0
    logger.info(
        "serper_conference done accounts=%d inserted=%d seen=%d duration=%.1fs",
        processed,
        total_inserted,
        total_seen,
        dt,
    )

    return {"inserted": total_inserted, "seen": total_seen, "accounts_processed": processed}
