from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote_plus

import httpx
from pydantic import BaseModel, ValidationError
from tenacity import retry, stop_after_attempt, wait_exponential

from src import db
from src.http_client import async_get
from src.models import SignalObservation
from src.settings import Settings
from src.utils import (
    classify_text,
    load_account_source_handles,
    stable_hash,
    utc_now_iso,
)

# Time window: only collect posts from last 14 days
REDDIT_DATA_WINDOW_DAYS = 14

logger = logging.getLogger(__name__)

# Reddit requires a specific User-Agent to avoid being blocked
REDDIT_USER_AGENT = "browser:zopdev-signals-collector:v1.0 (by /u/zopdev)"

_VERBOSE_PROGRESS = os.getenv("SIGNALS_VERBOSE_PROGRESS", "").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}


def _emit_progress(message: str) -> None:
    if _VERBOSE_PROGRESS:
        print(message, flush=True)


class RedditPost(BaseModel):
    """Pydantic model for validating raw Reddit API data."""

    title: str
    selftext: str
    url: str
    subreddit: str
    author: str
    created_utc: float
    score: int
    num_comments: int


def _build_observation(
    account_id: str, post: RedditPost, signal_code: str, confidence: float, reliability: float
) -> SignalObservation:
    """Transforms a Reddit post into a standard SignalObservation."""
    payload = post.model_dump()
    raw_hash = stable_hash(payload, prefix="raw")

    observed_at = datetime.fromtimestamp(post.created_utc, tz=timezone.utc).isoformat()

    obs_id = stable_hash(
        {
            "account_id": account_id,
            "signal_code": signal_code,
            "source": "reddit_api",
            "observed_at": observed_at,
            "raw": raw_hash,
        },
        prefix="obs",
    )

    return SignalObservation(
        obs_id=obs_id,
        account_id=account_id,
        signal_code=signal_code,
        source="reddit_api",
        observed_at=observed_at,
        evidence_url=post.url,
        evidence_text=f"[{post.subreddit}] {post.title}: {post.selftext[:300]}",
        confidence=confidence,
        source_reliability=reliability,
        raw_payload_hash=raw_hash,
    )


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    reraise=True,
)
async def _fetch_reddit_search_json(query: str, settings: Settings) -> dict[str, Any]:
    """Fetches search results from Reddit JSON API with retries."""
    # Support mock API for development (use SIGNALS_REDDIT_API_BASE_URL env var)
    reddit_api_base = os.getenv("SIGNALS_REDDIT_API_BASE_URL", "https://www.reddit.com")

    if reddit_api_base.startswith("http://"):  # Mock API - use httpx directly for testing
        search_url = f"{reddit_api_base}/search?q={quote_plus(query)}&sort=new&limit=25"
        async with httpx.AsyncClient(timeout=15) as client:
            response = await client.get(search_url)
            response.raise_for_status()
            return response.json()
    else:  # Real Reddit API
        # Use async_get to respect robots.txt and rate limiting per settings.respect_robots_txt
        search_url = f"{reddit_api_base}/search.json?q={quote_plus(query)}&sort=new&limit=25&t=month"

        logger.debug(f"_fetch_reddit_search_json: fetching {search_url}")
        response = await async_get(
            search_url,
            settings,
        )
        response.raise_for_status()
        return response.json()


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    reraise=True,
)
async def _fetch_reddit_subreddit_json(subreddit: str, settings: Settings) -> dict[str, Any]:
    """Fetches recent posts from a specific subreddit."""
    reddit_api_base = os.getenv("SIGNALS_REDDIT_API_BASE_URL", "https://www.reddit.com")

    if reddit_api_base.startswith("http://"):  # Mock API
        subreddit_url = f"{reddit_api_base}/r/{subreddit}/new.json?limit=25"
    else:  # Real Reddit API
        subreddit_url = f"{reddit_api_base}/r/{subreddit}/new.json?limit=25"

    logger.debug(f"_fetch_reddit_subreddit_json: fetching {subreddit_url}")
    response = await async_get(
        subreddit_url,
        settings,
    )
    response.raise_for_status()
    return response.json()


async def _collect_account(
    conn,
    settings: Settings,
    lexicon_rows: list[dict[str, str]],
    account: dict[str, Any],
    account_index: int,
    handles: dict[str, dict[str, str]],
    source_name: str,
    reliability: float,
) -> tuple[int, int, int]:
    logger.info(f"reddit_collector._collect_account() called for account #{account_index}")

    domain = str(account["domain"])
    account_id = str(account["account_id"])
    company_name = str(account["company_name"] or domain)
    handle_row = handles.get(domain, {})
    logger.debug(f"reddit_collector account #{account_index}: domain={domain}, company={company_name}")

    # Use override query if provided in handles, otherwise fallback to simple domain/company name
    query = handle_row.get("reddit_query", "").strip()
    if not query:
        # Try domain first (more likely to be unique), fall back to company name
        # Use simple substring search without OR operators (mock API does simple substring matching)
        domain_base = domain.replace(".com", "").replace(".io", "").split(".")[0] if domain else None
        company_base = company_name.lower().split()[0] if company_name else None

        if domain_base and domain_base != "example":
            query = domain_base
        elif company_base and company_base != "example":
            query = company_base
        else:
            # Fallback: use full domain/company name
            query = domain or company_name

    endpoint = f"reddit_search:{query}"

    logger.debug(f"reddit_collector account #{account_index}: domain={domain}, company={company_name}, query={query}")

    # Standard checkpointing - prevent redundant same-day crawls
    if db.was_crawled_today(conn, source=source_name, account_id=account_id, endpoint=endpoint):
        logger.debug(f"reddit_collector account #{account_index}: already crawled today, skipping")
        return 0, 0, 1

    logger.info(f"reddit_collector account #{account_index}: proceeding to API call with query='{query}'")

    try:
        logger.debug(
            f"reddit_collector account #{account_index}: attempting to fetch from official subreddit r/{query}"
        )

        # First, try to fetch from official company subreddit
        subreddit_candidates = [
            query.lower().replace(" ", ""),
            query.lower().replace(" ", "_"),
            f"r_{query.lower().replace(' ', '')}",
        ]

        data = None
        for subreddit_name in subreddit_candidates:
            try:
                logger.debug(f"reddit_collector account #{account_index}: trying subreddit r/{subreddit_name}")
                data = await _fetch_reddit_subreddit_json(subreddit_name, settings)
                if data and data.get("data", {}).get("children"):
                    logger.info(
                        f"reddit_collector account #{account_index}: found official subreddit r/{subreddit_name}"
                    )
                    break
            except Exception as e:
                logger.debug(f"reddit_collector account #{account_index}: subreddit r/{subreddit_name} not found: {e}")
                continue

        # Fall back to search if no official subreddit found
        if not data or not data.get("data", {}).get("children"):
            logger.debug(f"reddit_collector account #{account_index}: falling back to search for '{query}'")
            data = await _fetch_reddit_search_json(query, settings)

        # Debug response structure
        if not data:
            logger.warning(f"reddit_collector account #{account_index}: API returned empty response")
            return 0, 0, 1

        posts_raw = data.get("data", {}).get("children", [])
        logger.info(f"reddit_collector account #{account_index}: fetched {len(posts_raw)} posts from API")

        inserted = 0
        seen = 0

        if not posts_raw:
            logger.warning(
                f"reddit_collector account #{account_index}: no posts in response, data keys: {list(data.keys())}"
            )
            db.record_crawl_attempt(
                conn,
                source=source_name,
                account_id=account_id,
                endpoint=endpoint,
                status="success",
                error_summary="no_posts",
            )
            db.mark_crawled(conn, source=source_name, account_id=account_id, endpoint=endpoint, commit=False)
            return 0, 0, 1

        logger.info(f"reddit_collector account #{account_index}: processing {len(posts_raw)} posts to create signals")

        # Determine signals
        for entry_idx, entry in enumerate(posts_raw):
            item = entry.get("data", {})
            try:
                # Debug: log entry structure
                if entry_idx == 0:
                    logger.debug(
                        f"reddit_collector account #{account_index}: first entry keys: {list(item.keys())[:5]}"
                    )

                # Handle both 'url' and 'permalink' fields from Reddit API
                post_url = item.get("url", "")
                permalink = item.get("permalink", "")

                logger.debug(
                    f"reddit_collector account #{account_index}: entry {entry_idx} - "
                    f"url={post_url[:50] if post_url else 'NONE'}, "
                    f"permalink={permalink[:50] if permalink else 'NONE'}"
                )

                # Construct proper Reddit URL
                if not post_url or not post_url.startswith("http"):
                    if permalink:
                        post_url = f"https://reddit.com{permalink}"
                    else:
                        post_url = f"https://reddit.com/r/{item.get('subreddit', '')}/comments/{item.get('id', '')}"

                title = item.get("title", "")
                selftext = item.get("selftext", "")

                post = RedditPost(
                    title=title,
                    selftext=selftext,
                    url=post_url,
                    subreddit=item.get("subreddit", ""),
                    author=item.get("author", ""),
                    created_utc=float(item.get("created_utc", 0.0)),
                    score=int(item.get("score", 0)),
                    num_comments=int(item.get("num_comments", 0)),
                )

                # Filter: only collect posts from last 14 days
                now = datetime.now(timezone.utc)
                post_age_days = (now - datetime.fromtimestamp(post.created_utc, tz=timezone.utc)).days
                if post_age_days > REDDIT_DATA_WINDOW_DAYS:
                    logger.debug(
                        f"reddit_collector account #{account_index}: skipping old post "
                        f"(age={post_age_days} days, threshold={REDDIT_DATA_WINDOW_DAYS})"
                    )
                    continue

                text_to_classify = f"{post.title}\n{post.selftext}".strip()
                matches = classify_text(text_to_classify, lexicon_rows)

                # If no keyword matches, check subreddit and content for relevance
                if not matches:
                    # Relevant subreddits for FinOps/Infrastructure discussions
                    relevant_subreddits = [
                        "devops",
                        "kubernetes",
                        "devopsjobs",
                        "sre",
                        "finops",
                        "cloudarchitecture",
                        "aws",
                        "azure",
                        "gcp",
                        "cloud",
                        "infrastructure",
                        "terraform",
                        "docker",
                        "containerization",
                        "microservices",
                        "platformengineering",
                        "programming",
                        "learnprogramming",
                        "softwareengineering",
                        "webdev",
                        "enterprisearchitecture",
                        "sysadmin",
                        "itsecurity",
                    ]

                    subreddit_lower = post.subreddit.lower()
                    in_relevant_subreddit = any(sub in subreddit_lower for sub in relevant_subreddits)

                    # Infrastructure-related keywords for additional relevance checking
                    infra_keywords = [
                        "kubernetes",
                        "k8s",
                        "eks",
                        "gke",
                        "aks",
                        "terraform",
                        "infrastructure",
                        "iac",
                        "devops",
                        "cloud",
                        "finops",
                        "cost",
                        "serverless",
                        "docker",
                        "container",
                        "deployment",
                        "staging",
                        "production",
                        "cluster",
                        "microservices",
                        "scaling",
                        "api",
                        "backend",
                    ]
                    text_lower = text_to_classify.lower()
                    has_infra_keyword = any(keyword in text_lower for keyword in infra_keywords)

                    # Create signal if in relevant subreddit OR has infrastructure keywords
                    if in_relevant_subreddit or has_infra_keyword:
                        # Mark as community_mention (tech-relevant post about the company)
                        matches = [("community_mention", 0.6, "reddit_auto")]
                    else:
                        # Skip - not relevant to FinOps/infrastructure
                        continue

                logger.info(
                    f"reddit_collector account #{account_index}: post '{post.title[:50]}' -> {len(matches)} signal(s): {[m[0] for m in matches]}"
                )

                for signal_code, confidence, _ in matches:
                    seen += 1
                    obs = _build_observation(
                        account_id=account_id,
                        post=post,
                        signal_code=signal_code,
                        confidence=confidence,
                        reliability=reliability,
                    )
                    if db.insert_signal_observation(conn, obs, commit=False):
                        inserted += 1
                        logger.debug(
                            f"reddit_collector account #{account_index}: inserted signal obs_id={obs.obs_id}, signal_code={signal_code}"
                        )
                    else:
                        logger.debug(
                            f"reddit_collector account #{account_index}: failed to insert signal obs_id={obs.obs_id}"
                        )
            except ValidationError as e:
                logger.warning(f"reddit_collector account #{account_index}: validation error on post: {e}")
                logger.debug(
                    f"  title={item.get('title')}, created_utc={item.get('created_utc')}, permalink={item.get('permalink')}"
                )
                continue
            except Exception as e:
                logger.error(f"reddit_collector account #{account_index}: unexpected error on post: {e}")
                logger.debug(f"  item keys: {list(item.keys())}")
                continue

        db.record_crawl_attempt(
            conn, source=source_name, account_id=account_id, endpoint=endpoint, status="success", error_summary=""
        )
        db.mark_crawled(conn, source=source_name, account_id=account_id, endpoint=endpoint, commit=False)

        return inserted, seen, 1

    except Exception as exc:
        logger.error(
            f"reddit_collector account #{account_index}: exception during collection: {type(exc).__name__}: {exc}"
        )
        import traceback

        logger.debug(f"reddit_collector account #{account_index}: traceback: {traceback.format_exc()}")
        db.record_crawl_attempt(
            conn,
            source=source_name,
            account_id=account_id,
            endpoint=endpoint,
            status="exception",
            error_summary=str(exc)[:200],
        )
        db.mark_crawled(conn, source=source_name, account_id=account_id, endpoint=endpoint, commit=False)
        return 0, 0, 1


async def collect(
    conn,
    settings: Settings,
    lexicon_rows: list[dict[str, str]] | None = None,
    source_reliability: float = 0.65,
    account_ids: list[str] | None = None,
    lexicon_by_source: dict[str, list[dict[str, str]]] | None = None,
    source_reliability_dict: dict[str, float] | None = None,
    db_pool=None,
) -> dict[str, int]:
    """Main entry point for the Reddit Data Collector."""
    # Handle both old and new calling conventions
    if lexicon_by_source is None:
        lexicon_by_source = {}
    if source_reliability_dict is not None:
        source_reliability = source_reliability_dict.get("reddit_api", 0.65)
    if lexicon_rows is None:
        lexicon_rows = lexicon_by_source.get("community", [])

    logger.info(
        f"reddit_collector.collect() starting: enable_live_crawl={settings.enable_live_crawl}, "
        f"source_reliability={source_reliability}, lexicon_rows={len(lexicon_rows) if lexicon_rows else 0}"
    )

    if not settings.enable_live_crawl:
        logger.info("reddit_collector: live_crawl disabled, returning early")
        return {"inserted": 0, "seen": 0}

    source_name = "reddit_api"

    if source_reliability <= 0:
        logger.info(f"reddit_collector: source_reliability={source_reliability} <= 0, returning early")
        return {"inserted": 0, "seen": 0}

    # Get accounts - use provided account_ids or fetch all accounts
    if account_ids:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT account_id, company_name, domain, source_type FROM signals.accounts WHERE account_id = ANY(%s)",
            (account_ids,),
        )
        accounts = [dict(row) for row in cursor.fetchall()]
        logger.info(f"reddit_collector: fetched {len(accounts)} accounts from account_ids")
    else:
        # Fetch all accounts if no account_ids specified
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT account_id, company_name, domain, source_type
            FROM signals.accounts
            LIMIT %s
        """,
            (min(settings.live_max_accounts, 50),),
        )
        accounts = [dict(row) for row in cursor.fetchall()]
        logger.info(f"reddit_collector: fetched {len(accounts)} accounts from database")

    if not accounts:
        logger.warning("reddit_collector: no accounts found, returning early")
        return {"inserted": 0, "seen": 0}

    handles = load_account_source_handles(settings.account_source_handles_path)

    concurrency = min(max(1, int(settings.live_workers_per_source)), len(accounts))
    semaphore = asyncio.Semaphore(concurrency)

    inserted_total = 0
    seen_total = 0
    accounts_processed = 0

    logger.info(
        f"reddit_collector: starting with {len(accounts)} accounts, concurrency={concurrency}, "
        f"lexicon_size={len(lexicon_rows) if lexicon_rows else 0}"
    )

    async def _run_account(i: int, acct: dict):
        async with semaphore:
            return await _collect_account(
                conn=conn,
                settings=settings,
                lexicon_rows=lexicon_rows,
                account=acct,
                account_index=i,
                handles=handles,
                source_name=source_name,
                reliability=source_reliability,
            )

    tasks = [_run_account(i, acct) for i, acct in enumerate(accounts, start=1)]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    for result in results:
        if isinstance(result, Exception):
            logger.error(f"Reddit worker failed: {result}")
            continue
        ins, sn, processed = result
        inserted_total += ins
        seen_total += sn
        accounts_processed += processed
        logger.debug(f"reddit_collector: account result inserted={ins}, seen={sn}, processed={processed}")

    logger.info(
        f"reddit_collector: completed with inserted={inserted_total}, seen={seen_total}, accounts_processed={accounts_processed}"
    )
    conn.commit()
    return {"inserted": inserted_total, "seen": seen_total, "accounts_processed": accounts_processed}
