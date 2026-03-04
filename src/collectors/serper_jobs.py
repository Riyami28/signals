"""Serper.dev Google Jobs collector — fetches real job postings for watchlist companies."""

from __future__ import annotations

import asyncio
import logging
import re
import time
from typing import Any

import httpx

from src import db
from src.models import SignalObservation
from src.settings import Settings
from src.utils import classify_text, stable_hash, utc_now_iso

logger = logging.getLogger(__name__)

SERPER_SEARCH_URL = "https://google.serper.dev/search"

# Regex to detect stale job postings (e.g., "3 months ago", "5 months ago")
# Only flags "months" — "days ago" or "hours ago" are fine.
_STALE_AGE_RE = re.compile(r"\b(\d+)\s+months?\s+ago\b", re.IGNORECASE)


def _is_stale_posting(title: str, snippet: str) -> bool:
    """Return True if the snippet or title mentions the posting is months old."""
    for text in (title, snippet):
        m = _STALE_AGE_RE.search(text)
        if m and int(m.group(1)) >= 2:
            return True
    return False


# Job-specific search suffixes to find relevant engineering roles
_JOB_SEARCH_SUFFIXES = [
    "devops OR SRE OR platform engineer OR cloud engineer OR kubernetes",
    "finops OR cloud cost OR infrastructure engineer",
]

# Direct title-to-signal mapping — TITLE ONLY, no snippet matching.
# Only match explicit DevOps/Platform/FinOps role titles.
_ROLE_SIGNAL_MAP = {
    "finops": "finops_role_open",
    "cloud cost": "finops_role_open",
    "cost optimization": "finops_role_open",
    "platform engineer": "platform_role_open",
    "internal developer platform": "platform_role_open",
    "devops": "devops_role_open",
    "sre": "devops_role_open",
    "site reliability": "devops_role_open",
    "cloud engineer": "devops_role_open",
    "infrastructure engineer": "devops_role_open",
}

# Domains that are known job board sites
_JOB_BOARD_DOMAINS = frozenset(
    [
        "linkedin.com/jobs",
        "linkedin.com/in/",  # profiles with job posts
        "indeed.com",
        "indeed.co",
        "naukri.com",
        "glassdoor.com",
        "glassdoor.co",
        "lever.co",
        "greenhouse.io",
        "workday.com",
        "myworkdayjobs.com",
        "ashbyhq.com",
        "angel.co",
        "wellfound.com",
        "instahyre.com",
        "cutshort.io",
        "dice.com",
        "monster.com",
        "simplyhired.com",
        "ziprecruiter.com",
        "hired.com",
        "jobvite.com",
        "smartrecruiters.com",
        "icims.com",
        "breezy.hr",
        "recruitee.com",
    ]
)

# URL path segments that indicate a careers/jobs page
_CAREER_PATH_SEGMENTS = frozenset(
    [
        "/careers",
        "/jobs/",
        "/job/",
        "/openings",
        "/positions",
        "/hiring",
        "/vacancies",
        "/apply",
    ]
)


def _is_job_url(link: str) -> bool:
    """Check if a URL is from a known job board or a careers page."""
    link_lower = link.lower()

    # Check against known job board domains
    for domain in _JOB_BOARD_DOMAINS:
        if domain in link_lower:
            return True

    # Check for career-related path segments
    for segment in _CAREER_PATH_SEGMENTS:
        if segment in link_lower:
            return True

    return False


def _match_role_signal(title: str, snippet: str) -> tuple[str, float] | None:
    """Try to match job TITLE to a specific signal code.

    Only checks the job title — snippet is ignored to avoid false positives
    where a generic dev role mentions DevOps/K8s keywords in the description.

    Returns (signal_code, confidence) or None.
    """
    title_lower = title.lower()
    for keyword, signal_code in _ROLE_SIGNAL_MAP.items():
        if keyword in title_lower:
            return signal_code, 0.65
    return None


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


async def _fetch_serper_search(
    client: httpx.AsyncClient,
    query: str,
    api_key: str,
    num_results: int = 10,
) -> list[dict]:
    """Call Serper search API with job-related query and return organic results.

    Uses tbs=qdr:m to restrict results to the past month — avoids returning
    stale job postings that Google still has indexed from months ago.
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
        return data.get("organic", [])
    except httpx.HTTPStatusError as exc:
        logger.warning("serper_jobs_http_error query=%s status=%s", query[:60], exc.response.status_code)
        return []
    except Exception as exc:
        logger.warning("serper_jobs_error query=%s error=%s", query[:60], exc)
        return []


async def _collect_one_account(
    conn,
    client: httpx.AsyncClient,
    account: dict,
    api_key: str,
    num_results: int,
    lexicon_rows: list[dict[str, str]],
    source_reliability: float,
) -> tuple[int, int]:
    """Fetch job postings for one account via Serper and insert matching signals."""
    account_id = str(account["account_id"])
    company_name = str(account.get("company_name") or account.get("domain", ""))
    domain = str(account.get("domain", ""))

    if not company_name and not domain:
        return 0, 0

    source_name = "serper_jobs"
    endpoint = f"serper_jobs:{domain}"
    if db.was_crawled_today(conn, source=source_name, account_id=account_id, endpoint=endpoint):
        return 0, 0

    inserted = 0
    seen = 0

    # Search for jobs at this company using multiple queries
    for suffix in _JOB_SEARCH_SUFFIXES:
        query = f'"{company_name}" jobs {suffix}'
        results = await _fetch_serper_search(client, query, api_key, num_results)

        for item in results:
            title = str(item.get("title", ""))
            snippet = str(item.get("snippet", ""))
            link = str(item.get("link", ""))

            if not link:
                continue

            # Skip stale postings (e.g., "5 months ago" in snippet)
            if _is_stale_posting(title, snippet):
                continue

            # Filter: only keep results from job boards or career pages
            is_job_source = _is_job_url(link)

            # Also keep if company domain appears in the URL
            is_company_page = False
            if domain and len(domain) > 4:  # avoid matching tiny domains
                is_company_page = domain.lower() in link.lower()

            if not is_job_source and not is_company_page:
                continue

            text = f"{title}\n{snippet}".strip()
            if not text:
                continue

            # 1. First try keyword lexicon match (most specific)
            matches = classify_text(text, lexicon_rows)

            if matches:
                for signal_code, confidence, matched_keyword in matches:
                    seen += 1
                    observation = _build_observation(
                        account_id=account_id,
                        signal_code=signal_code,
                        source="serper_jobs",
                        observed_at=utc_now_iso(),
                        confidence=confidence,
                        source_reliability=source_reliability,
                        evidence_url=link,
                        evidence_text=text,
                        payload={
                            "title": title,
                            "snippet": snippet,
                            "link": link,
                            "matched_keyword": matched_keyword,
                            "query": query[:100],
                        },
                    )
                    if db.insert_signal_observation(conn, observation, commit=False):
                        inserted += 1
            else:
                # 2. Try direct role-to-signal mapping from title
                role_match = _match_role_signal(title, snippet)
                if role_match:
                    signal_code, confidence = role_match
                    seen += 1
                    observation = _build_observation(
                        account_id=account_id,
                        signal_code=signal_code,
                        source="serper_jobs",
                        observed_at=utc_now_iso(),
                        confidence=confidence,
                        source_reliability=source_reliability,
                        evidence_url=link,
                        evidence_text=text,
                        payload={
                            "title": title,
                            "snippet": snippet,
                            "link": link,
                            "matched_role": signal_code,
                            "query": query[:100],
                        },
                    )
                    if db.insert_signal_observation(conn, observation, commit=False):
                        inserted += 1
                # Skip non-DevOps/Platform/FinOps job postings — generic hiring
                # (Financial Auditor, Marketing Manager, etc.) is not a buying signal.

        # Brief pause between queries for same account
        await asyncio.sleep(0.05)

    db.record_crawl_attempt(
        conn,
        source="serper_jobs",
        account_id=account_id,
        endpoint=endpoint,
        status="success",
        error_summary="",
        commit=False,
    )
    db.mark_crawled(conn, source="serper_jobs", account_id=account_id, endpoint=endpoint, commit=False)

    return inserted, seen


async def collect(
    conn,
    settings: Settings,
    lexicon_rows: list[dict[str, str]],
    source_reliability: float = 0.80,
    account_ids: list[str] | None = None,
) -> dict[str, int]:
    """
    Main entry point: fetch job signals from Google via Serper.

    Returns:
        {"inserted": N, "seen": N, "accounts_processed": N}
    """
    api_key = settings.serper_api_key
    if not api_key:
        logger.warning("serper_api_key is empty, skipping Serper jobs collection")
        return {"inserted": 0, "seen": 0, "accounts_processed": 0}

    max_accounts = settings.serper_max_accounts
    num_results = settings.serper_results_per_query

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

    logger.info("serper_jobs starting accounts=%d", len(accounts))
    t0 = time.monotonic()

    total_inserted = 0
    total_seen = 0
    processed = 0

    # 5 concurrent requests (each account does 2 queries, Serper allows ~100 req/min)
    concurrency = min(5, len(accounts))
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
            logger.warning("serper_jobs_worker_error: %s", result)
            continue
        ins, seen = result
        total_inserted += ins
        total_seen += seen
        processed += 1

    dt = time.monotonic() - t0
    logger.info(
        "serper_jobs done accounts=%d inserted=%d seen=%d duration=%.1fs",
        processed,
        total_inserted,
        total_seen,
        dt,
    )

    return {"inserted": total_inserted, "seen": total_seen, "accounts_processed": processed}
