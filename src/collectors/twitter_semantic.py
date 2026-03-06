"""Twitter semantic signal collector — LLM-based intent classification.

Replaces keyword matching (classify_text) with batched LLM classification
for higher signal quality.  Reuses the RapidAPI fetching logic from twitter.py
and maps LLM classifications to existing signal codes in signal_registry.csv.

Source name: ``twitter_semantic``
"""

from __future__ import annotations

import asyncio
import logging
import random
from datetime import datetime, timezone
from typing import Any

import httpx

from src import db
from src.collectors.twitter import (
    DEFAULT_TWITTER_TERMS,
    _build_observation,
    _parse_tweet_observed_at,
    _rapidapi_headers,
    _rapidapi_search_url,
    load_twitter_handles,
)
from src.collectors.twitter_classify import (
    TweetClassification,
    classify_tweets_batch,
)
from src.research.client import create_research_client
from src.settings import Settings
from src.source_policy import load_source_execution_policy
from src.utils import stable_hash, utc_now_iso

logger = logging.getLogger(__name__)

SOURCE_NAME = "twitter_semantic"

# Decision-maker role keywords for weight assignment
_DM_ROLE_WEIGHTS: dict[str, float] = {
    "cto": 1.8,
    "vp": 1.5,
    "director": 1.3,
    "head": 1.3,
    "chief": 1.8,
    "founder": 1.5,
    "co-founder": 1.5,
    "principal": 1.1,
    "staff": 1.0,
    "lead": 1.0,
}


def _role_weight(role_guess: str) -> float:
    """Assign a weight to a decision-maker role guess."""
    lower = role_guess.lower()
    for keyword, weight in _DM_ROLE_WEIGHTS.items():
        if keyword in lower:
            return weight
    return 1.0


# ---------------------------------------------------------------------------
# RapidAPI response parsing with author info
# ---------------------------------------------------------------------------


def _parse_tweets_with_authors(data: dict[str, Any]) -> list[dict[str, Any]]:
    """Parse RapidAPI GraphQL response into tweets with author information.

    Returns list of dicts with keys: id, text, created_at, author, author_name.
    """
    tweets: list[dict[str, Any]] = []

    try:
        instructions = (
            data.get("result", {})
            .get("timeline_response", {})
            .get("timeline", {})
            .get("instructions", [])
        )
        for instruction in instructions:
            for entry in instruction.get("entries", []):
                content = entry.get("content", {})
                if content.get("__typename") != "TimelineTimelineItem":
                    continue
                inner = content.get("content", {})
                if inner.get("__typename") != "TimelineTweet":
                    continue
                result = inner.get("tweet_results", {}).get("result", {})
                details = result.get("details") or result.get("legacy") or {}
                text = str(details.get("full_text") or details.get("text") or "").strip()
                tweet_id = str(result.get("rest_id") or "")

                # Parse created_at
                created_at_ms = details.get("created_at_ms")
                if created_at_ms:
                    created_at = datetime.fromtimestamp(
                        int(created_at_ms) / 1000, tz=timezone.utc
                    ).isoformat()
                else:
                    created_at_str = details.get("created_at", "")
                    try:
                        created_at = (
                            datetime.strptime(created_at_str, "%a %b %d %H:%M:%S +0000 %Y")
                            .replace(tzinfo=timezone.utc)
                            .isoformat()
                            if created_at_str
                            else ""
                        )
                    except Exception:
                        created_at = created_at_str

                # Extract author info
                core = result.get("core", {})
                user_result = core.get("user_results", {}).get("result", {})
                legacy = user_result.get("legacy", {})
                screen_name = str(legacy.get("screen_name", "") or "")
                display_name = str(legacy.get("name", "") or "")

                if text:
                    tweets.append(
                        {
                            "id": tweet_id,
                            "text": text,
                            "created_at": created_at,
                            "author": screen_name,
                            "author_name": display_name,
                        }
                    )
        if tweets:
            return tweets
    except Exception:
        pass

    # Legacy flat shape fallback (no author info available)
    raw = data.get("timeline") or data.get("data") or []
    for item in raw:
        tweet_id = str(item.get("tweet_id") or item.get("id") or item.get("id_str") or "")
        text = str(item.get("text") or item.get("full_text") or "").strip()
        created_at = str(item.get("created_at") or "")
        if text:
            tweets.append(
                {
                    "id": tweet_id,
                    "text": text,
                    "created_at": created_at,
                    "author": "",
                    "author_name": "",
                }
            )
    return tweets


# ---------------------------------------------------------------------------
# Per-account collection
# ---------------------------------------------------------------------------


async def _fetch_tweets_for_account(
    client: httpx.AsyncClient,
    settings: Settings,
    account: dict[str, Any],
    twitter_handles: dict[str, str],
) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    """Fetch tweets via RapidAPI for one account.

    Returns (tweets_with_author_info, raw_response_or_None).
    """
    domain = str(account["domain"])
    company_name = str(account.get("company_name") or domain)

    rapidapi_key = getattr(settings, "twitter_rapidapi_key", "").strip()
    rapidapi_host = getattr(settings, "twitter_rapidapi_host", "twitter-api45.p.rapidapi.com").strip()

    if not rapidapi_key:
        return [], None

    official_handle = twitter_handles.get(domain, "").strip()
    handle_bare = official_handle.lstrip("@")
    since_id = db.get_twitter_since_id(None, account.get("account_id", "")) if False else ""

    if official_handle:
        query = f"from:{handle_bare} -is:retweet lang:en"
    else:
        query = f'("{company_name}" OR "{domain}") {DEFAULT_TWITTER_TERMS}'

    if len(query) > 512:
        query = query[:509] + "..."

    url = _rapidapi_search_url(rapidapi_host, query)
    headers = _rapidapi_headers(rapidapi_key, rapidapi_host)

    response = await client.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()
    tweets = _parse_tweets_with_authors(data)
    return tweets, data


async def _collect_account(
    conn,
    settings: Settings,
    account: dict[str, Any],
    account_index: int,
    twitter_handles: dict[str, str],
    reliability: float,
    llm_client,
    client: httpx.AsyncClient,
    batch_size: int,
) -> tuple[int, int, int]:
    """Collect and classify tweets for one account. Returns (inserted, seen, processed)."""
    domain = str(account["domain"])
    if domain.endswith(".example"):
        return 0, 0, 0

    account_id = str(account["account_id"])
    company_name = str(account.get("company_name") or domain)

    endpoint = f"twitter_semantic:{domain}"
    if db.was_crawled_today(conn, source=SOURCE_NAME, account_id=account_id, endpoint=endpoint):
        return 0, 0, 1

    # Fetch tweets
    try:
        tweets, raw_data = await _fetch_tweets_for_account(client, settings, account, twitter_handles)
    except httpx.HTTPStatusError as exc:
        status_code = exc.response.status_code if exc.response is not None else 0
        db.record_crawl_attempt(
            conn,
            source=SOURCE_NAME,
            account_id=account_id,
            endpoint=endpoint,
            status="rate_limited" if status_code == 429 else "http_error",
            error_summary=f"status_code={status_code}",
            commit=False,
        )
        if status_code == 429:
            raise  # Let caller stop all workers
        db.mark_crawled(conn, source=SOURCE_NAME, account_id=account_id, endpoint=endpoint, commit=False)
        return 0, 0, 1
    except Exception as exc:
        db.record_crawl_attempt(
            conn,
            source=SOURCE_NAME,
            account_id=account_id,
            endpoint=endpoint,
            status="exception",
            error_summary=str(exc)[:200],
            commit=False,
        )
        db.mark_crawled(conn, source=SOURCE_NAME, account_id=account_id, endpoint=endpoint, commit=False)
        return 0, 0, 1

    if not tweets:
        db.record_crawl_attempt(
            conn,
            source=SOURCE_NAME,
            account_id=account_id,
            endpoint=endpoint,
            status="success",
            error_summary="no_tweets",
            commit=False,
        )
        db.mark_crawled(conn, source=SOURCE_NAME, account_id=account_id, endpoint=endpoint, commit=False)
        return 0, 0, 1

    # Batch tweets for LLM classification
    inserted = 0
    seen = 0
    for batch_start in range(0, len(tweets), batch_size):
        batch = tweets[batch_start : batch_start + batch_size]
        classifications = classify_tweets_batch(llm_client, batch, company_name, domain)

        if not classifications:
            logger.warning(
                "twitter_semantic: LLM classification returned empty for domain=%s batch_start=%d",
                domain,
                batch_start,
            )
            continue

        for clf in classifications:
            if clf.signal_code == "none":
                continue
            seen += 1

            tweet = batch[clf.tweet_index] if clf.tweet_index < len(batch) else None
            if tweet is None:
                continue

            tweet_id = str(tweet.get("id", ""))
            evidence_url = f"https://twitter.com/i/web/status/{tweet_id}" if tweet_id else ""
            observed_at = _parse_tweet_observed_at(tweet)

            observation = _build_observation(
                account_id=account_id,
                signal_code=clf.signal_code,
                source=SOURCE_NAME,
                observed_at=observed_at,
                confidence=clf.confidence,
                source_reliability=reliability,
                evidence_url=evidence_url,
                evidence_text=tweet.get("text", ""),
                payload={
                    "tweet_id": tweet_id,
                    "text": tweet.get("text", ""),
                    "author": tweet.get("author", ""),
                    "llm_reasoning": clf.reasoning,
                    "is_decision_maker": clf.is_decision_maker,
                },
            )
            if db.insert_signal_observation(conn, observation, commit=False):
                inserted += 1

            # Track decision-makers in people_watchlist
            if clf.is_decision_maker and clf.author_role_guess:
                author_name = tweet.get("author_name") or tweet.get("author", "")
                if author_name:
                    try:
                        db.upsert_people_watchlist_entry(
                            conn,
                            account_id=account_id,
                            person_name=author_name[:200],
                            role_title=clf.author_role_guess[:120],
                            role_weight=_role_weight(clf.author_role_guess),
                            source_url=evidence_url,
                            is_active=True,
                            commit=False,
                        )
                        db.insert_people_activity(
                            conn,
                            account_id=account_id,
                            person_name=author_name[:200],
                            role_title=clf.author_role_guess[:120],
                            document_id=stable_hash(
                                {"tweet_id": tweet_id, "source": SOURCE_NAME}, prefix="doc"
                            ),
                            activity_type="twitter_post",
                            summary=clf.reasoning[:200],
                            published_at=observed_at,
                            url=evidence_url,
                            commit=False,
                        )
                    except Exception as exc:
                        logger.debug(
                            "twitter_semantic: people tracking failed account=%s error=%s",
                            account_id,
                            exc,
                        )

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

    logger.info(
        "twitter_semantic account=%s domain=%s tweets=%d classified_seen=%d inserted=%d",
        account_id,
        domain,
        len(tweets),
        seen,
        inserted,
    )
    return inserted, seen, 1


# ---------------------------------------------------------------------------
# Main collect() entry point
# ---------------------------------------------------------------------------


async def collect(
    conn,
    settings: Settings,
    lexicon_by_source: dict[str, list[dict[str, str]]],
    source_reliability: dict[str, float],
    account_ids: list[str] | None = None,
    db_pool=None,
) -> dict[str, int]:
    """Standard collector entry point for twitter_semantic.

    Requires both a Twitter API key (RapidAPI) and an LLM API key.
    Returns {"inserted": N, "seen": N, "accounts_processed": N}.
    """
    reliability = source_reliability.get(SOURCE_NAME, 0.80)

    # Check for required API keys
    rapidapi_key = getattr(settings, "twitter_rapidapi_key", "").strip()
    if not rapidapi_key:
        logger.info("twitter_semantic: skipped — no twitter_rapidapi_key")
        return {"inserted": 0, "seen": 0, "accounts_processed": 0}

    # Check for LLM key
    llm_provider = getattr(settings, "llm_provider", "claude")
    if llm_provider == "minimax":
        has_llm_key = bool(getattr(settings, "minimax_api_key", ""))
    else:
        has_llm_key = bool(getattr(settings, "claude_api_key", ""))

    if not has_llm_key:
        logger.info("twitter_semantic: skipped — no LLM API key (provider=%s)", llm_provider)
        return {"inserted": 0, "seen": 0, "accounts_processed": 0}

    if not settings.enable_live_crawl:
        logger.info("twitter_semantic: skipped — enable_live_crawl=false")
        return {"inserted": 0, "seen": 0, "accounts_processed": 0}

    # Create LLM client
    try:
        llm_client = create_research_client(settings)
    except ValueError as exc:
        logger.warning("twitter_semantic: failed to create LLM client: %s", exc)
        return {"inserted": 0, "seen": 0, "accounts_processed": 0}

    # Load accounts
    max_accounts = getattr(settings, "twitter_semantic_max_accounts", 50)
    batch_size = getattr(settings, "twitter_semantic_batch_size", 15)

    if account_ids:
        accounts = db.select_accounts_for_live_crawl(
            conn, source=SOURCE_NAME, limit=max_accounts, include_account_ids=account_ids
        )
    else:
        accounts = db.select_accounts_for_live_crawl(
            conn,
            source=SOURCE_NAME,
            limit=max_accounts,
            include_domains=list(settings.live_target_domains) if settings.live_target_domains else None,
        )

    if not accounts:
        logger.info("twitter_semantic: no accounts to process")
        return {"inserted": 0, "seen": 0, "accounts_processed": 0}

    # Load Twitter handles
    twitter_handles_path = settings.project_root / "config" / "company_twitter_handles.csv"
    twitter_handles = load_twitter_handles(twitter_handles_path)

    # Load execution policy for concurrency
    source_policies = load_source_execution_policy(
        settings.project_root / "config" / "source_execution_policy.csv"
    )
    policy = source_policies.get(SOURCE_NAME)
    if policy:
        concurrency = max(1, policy.max_parallel_workers)
        req_delay = 1.0 / policy.requests_per_second if policy.requests_per_second > 0 else 15.0
    else:
        concurrency = 2
        req_delay = 10.0

    semaphore = asyncio.Semaphore(min(concurrency, len(accounts)))
    rate_limited = asyncio.Event()

    inserted_total = 0
    seen_total = 0
    accounts_processed = 0

    logger.info(
        "twitter_semantic: starting accounts=%d concurrency=%d batch_size=%d",
        len(accounts),
        concurrency,
        batch_size,
    )

    async with httpx.AsyncClient(
        headers={"User-Agent": settings.http_user_agent},
        follow_redirects=True,
        timeout=settings.http_timeout_seconds,
    ) as client:

        async def _run_account(idx: int, account: dict) -> tuple[int, int, int]:
            if rate_limited.is_set():
                return 0, 0, 0
            async with semaphore:
                if rate_limited.is_set():
                    return 0, 0, 0
                if idx > 0:
                    await asyncio.sleep(random.uniform(0.8 * req_delay, 1.2 * req_delay))
                try:
                    return await _collect_account(
                        conn=conn,
                        settings=settings,
                        account=account,
                        account_index=idx,
                        twitter_handles=twitter_handles,
                        reliability=reliability,
                        llm_client=llm_client,
                        client=client,
                        batch_size=batch_size,
                    )
                except httpx.HTTPStatusError as exc:
                    if exc.response is not None and exc.response.status_code == 429:
                        rate_limited.set()
                        logger.warning("twitter_semantic: rate limit hit — stopping remaining workers")
                        return 0, 0, 0
                    return 0, 0, 1

        tasks = [_run_account(i, acct) for i, acct in enumerate(accounts)]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    conn.commit()

    for result in results:
        if isinstance(result, Exception):
            logger.error("twitter_semantic: worker failed error=%s", result, exc_info=True)
            continue
        ins, s, proc = result
        inserted_total += ins
        seen_total += s
        accounts_processed += proc

    logger.info(
        "twitter_semantic: complete inserted=%d seen=%d accounts_processed=%d",
        inserted_total,
        seen_total,
        accounts_processed,
    )
    return {
        "inserted": inserted_total,
        "seen": seen_total,
        "accounts_processed": accounts_processed,
    }
