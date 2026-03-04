"""GitHub Stargazer collector — tracks who stars Zopdev repos.

Fetches stargazers from configured GitHub repos, extracts company info
from their profiles, and matches them against existing accounts.
Generates `repo_starred` signals when a match is found.

FREE — uses GitHub REST API (unauthenticated: 60 req/hr, with token: 5000 req/hr).
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
from src.utils import normalize_domain, stable_hash, utc_now_iso

logger = logging.getLogger(__name__)

GITHUB_API_URL = "https://api.github.com"

# Zopdev org repos to track stargazers — add more as needed
_DEFAULT_REPOS = [
    "zopdev/zop-api",
    "zopdev/zop-cli",
    "zopdev/zopdev",
]


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


def _extract_company_domain(user: dict) -> tuple[str, str]:
    """Extract company name and email domain from a GitHub user profile.

    Returns (company_name, email_domain). Either or both may be empty.
    """
    company = str(user.get("company") or "").strip().lstrip("@").strip()
    email = str(user.get("email") or "").strip()
    email_domain = ""
    if "@" in email:
        email_domain = email.split("@", 1)[1].lower()
        # Skip generic email providers
        generic = {
            "gmail.com",
            "yahoo.com",
            "hotmail.com",
            "outlook.com",
            "protonmail.com",
            "icloud.com",
            "mail.com",
            "aol.com",
            "live.com",
            "yandex.com",
            "qq.com",
            "163.com",
        }
        if email_domain in generic:
            email_domain = ""
    return company, email_domain


async def _fetch_stargazers(
    client: httpx.AsyncClient,
    repo: str,
    token: str | None,
    per_page: int = 100,
    max_pages: int = 10,
) -> list[dict]:
    """Fetch stargazers for a repo with their user details.

    Uses the star creation timestamp via Accept header.
    """
    headers: dict[str, str] = {
        "Accept": "application/vnd.github.v3.star+json",
        "User-Agent": "zopdev-signals/0.1",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"

    all_stargazers: list[dict] = []
    for page in range(1, max_pages + 1):
        try:
            resp = await client.get(
                f"{GITHUB_API_URL}/repos/{repo}/stargazers",
                params={"per_page": per_page, "page": page},
                headers=headers,
                timeout=15,
            )
            if resp.status_code == 404:
                logger.warning("github_stargazers repo_not_found repo=%s", repo)
                break
            resp.raise_for_status()
            data = resp.json()
            if not data:
                break
            all_stargazers.extend(data)
            # If less than per_page, we've hit the last page
            if len(data) < per_page:
                break
            # Rate limit courtesy
            await asyncio.sleep(0.5)
        except httpx.HTTPStatusError as exc:
            logger.warning(
                "github_stargazers_http_error repo=%s status=%s",
                repo,
                exc.response.status_code,
            )
            break
        except Exception as exc:
            logger.warning("github_stargazers_error repo=%s error=%s", repo, exc)
            break

    return all_stargazers


async def _fetch_user_detail(
    client: httpx.AsyncClient,
    username: str,
    token: str | None,
) -> dict | None:
    """Fetch full user profile (includes company & email)."""
    headers: dict[str, str] = {
        "Accept": "application/vnd.github.v3+json",
        "User-Agent": "zopdev-signals/0.1",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"

    try:
        resp = await client.get(
            f"{GITHUB_API_URL}/users/{username}",
            headers=headers,
            timeout=10,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as exc:
        logger.debug("github_user_fetch_error user=%s error=%s", username, exc)
        return None


def _match_user_to_account(
    company: str,
    email_domain: str,
    domain_to_account: dict[str, str],
    name_to_account: dict[str, str],
) -> str | None:
    """Try to match a GitHub user to an existing account.

    Matches by:
    1. Email domain → account domain
    2. Company name (normalized) → account company_name
    """
    # Match by email domain
    if email_domain and email_domain in domain_to_account:
        return domain_to_account[email_domain]

    # Match by company name (case-insensitive, stripped)
    if company:
        company_lower = company.lower().strip()
        if company_lower in name_to_account:
            return name_to_account[company_lower]
        # Try partial match — company field often has extra text
        for name, account_id in name_to_account.items():
            if name in company_lower or company_lower in name:
                return account_id

    return None


async def collect(
    conn,
    settings: Settings,
    source_reliability: float = 0.60,
    account_ids: list[str] | None = None,
) -> dict[str, int]:
    """Main entry: fetch stargazers from Zopdev repos, match to accounts.

    Returns {"inserted": N, "seen": N, "matched_users": N}
    """
    github_token = getattr(settings, "github_token", "") or ""

    # Build lookup maps: domain → account_id, company_name → account_id
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
                "SELECT account_id, company_name, domain FROM accounts ORDER BY company_name"
            ).fetchall()
        ]

    domain_to_account: dict[str, str] = {}
    name_to_account: dict[str, str] = {}
    for acct in accounts:
        domain = normalize_domain(str(acct.get("domain", "") or ""))
        if domain:
            domain_to_account[domain] = str(acct["account_id"])
        name = str(acct.get("company_name", "") or "").lower().strip()
        if name:
            name_to_account[name] = str(acct["account_id"])

    # Get repos to track from settings or use defaults
    repos = list(getattr(settings, "github_repos", None) or _DEFAULT_REPOS)

    logger.info("github_stargazers starting repos=%d accounts=%d", len(repos), len(accounts))
    t0 = time.monotonic()

    total_inserted = 0
    total_seen = 0
    matched_users = 0
    source_name = "github_stargazers"

    async with httpx.AsyncClient() as client:
        for repo in repos:
            # Check if already crawled today
            endpoint = f"github_stargazers:{repo}"
            if db.was_crawled_today(conn, source=source_name, account_id="__global__", endpoint=endpoint):
                logger.info("github_stargazers already_crawled repo=%s", repo)
                continue

            stargazers = await _fetch_stargazers(client, repo, github_token)
            logger.info("github_stargazers repo=%s stargazers=%d", repo, len(stargazers))

            # Process each stargazer — fetch profile + match
            semaphore = asyncio.Semaphore(5)  # 5 concurrent user lookups

            async def _process_stargazer(star_entry: dict) -> tuple[int, int, int]:
                async with semaphore:
                    user_basic = star_entry.get("user", star_entry)
                    username = str(user_basic.get("login", ""))
                    starred_at = str(star_entry.get("starred_at", "") or utc_now_iso())

                    if not username:
                        return 0, 0, 0

                    # Fetch full profile for company/email
                    user_detail = await _fetch_user_detail(client, username, github_token)
                    if not user_detail:
                        return 0, 0, 0

                    company, email_domain = _extract_company_domain(user_detail)
                    if not company and not email_domain:
                        return 0, 0, 0  # Can't match without company info

                    account_id = _match_user_to_account(company, email_domain, domain_to_account, name_to_account)
                    if not account_id:
                        return 0, 1, 0  # Seen but not matched

                    # Create signal
                    observation = _build_observation(
                        account_id=account_id,
                        signal_code="repo_starred",
                        source=source_name,
                        observed_at=starred_at,
                        confidence=0.55,
                        source_reliability=source_reliability,
                        evidence_url=f"https://github.com/{username}",
                        evidence_text=f"{username} ({company or email_domain}) starred {repo}",
                        payload={
                            "repo": repo,
                            "username": username,
                            "company": company,
                            "email_domain": email_domain,
                            "starred_at": starred_at,
                        },
                    )
                    inserted = 1 if db.insert_signal_observation(conn, observation, commit=False) else 0
                    return inserted, 1, 1

            tasks = [_process_stargazer(s) for s in stargazers]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for result in results:
                if isinstance(result, Exception):
                    logger.warning("github_stargazers_worker_error: %s", result)
                    continue
                ins, seen, matched = result
                total_inserted += ins
                total_seen += seen
                matched_users += matched

            # Record crawl attempt
            db.record_crawl_attempt(
                conn,
                source=source_name,
                account_id="__global__",
                endpoint=endpoint,
                status="success",
                error_summary="",
                commit=False,
            )
            db.mark_crawled(
                conn,
                source=source_name,
                account_id="__global__",
                endpoint=endpoint,
                commit=False,
            )

    conn.commit()

    dt = time.monotonic() - t0
    logger.info(
        "github_stargazers done repos=%d stargazers_seen=%d matched=%d inserted=%d duration=%.1fs",
        len(repos),
        total_seen,
        matched_users,
        total_inserted,
        dt,
    )

    return {
        "inserted": total_inserted,
        "seen": total_seen,
        "matched_users": matched_users,
        "accounts_processed": len(accounts),
    }
