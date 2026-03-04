"""
Reddit Official Subreddit Collector
Fetches posts from official company subreddits (r/companyname) to extract signals.
Optimized to avoid rate limiting by using direct subreddit URLs instead of search.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any

import httpx
from pydantic import BaseModel, ValidationError
from tenacity import retry, stop_after_attempt, wait_exponential

from src import db
from src.http_client import async_get
from src.models import SignalObservation
from src.settings import Settings
from src.utils import (
    classify_text,
    load_csv_rows,
    stable_hash,
)

logger = logging.getLogger(__name__)

REDDIT_USER_AGENT = "browser:zopdev-signals-collector:v1.0 (by /u/zopdev)"


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
    """Transforms a Reddit post into a SignalObservation."""
    payload = post.model_dump()
    raw_hash = stable_hash(payload, prefix="raw")

    observed_at = datetime.fromtimestamp(post.created_utc, tz=timezone.utc).isoformat()

    obs_id = stable_hash(
        {
            "account_id": account_id,
            "signal_code": signal_code,
            "source": "reddit_official",
            "observed_at": observed_at,
            "raw": raw_hash,
        },
        prefix="obs",
    )

    return SignalObservation(
        obs_id=obs_id,
        account_id=account_id,
        signal_code=signal_code,
        source="reddit_official",
        observed_at=observed_at,
        evidence_url=post.url,
        evidence_text=f"[{post.subreddit}] {post.title}: {post.selftext[:300]}",
        confidence=confidence,
        source_reliability=reliability,
        raw_payload_hash=raw_hash,
    )


@retry(stop=stop_after_attempt(2), wait=wait_exponential(multiplier=2, min=5, max=15), reraise=True)
async def _fetch_official_subreddit(subreddit: str, settings: Settings) -> dict[str, Any]:
    """Fetches recent posts from an official company subreddit."""
    reddit_base = os.getenv("SIGNALS_REDDIT_API_BASE_URL", "https://old.reddit.com")

    url = f"{reddit_base}/r/{subreddit}/new.json?limit=50"

    logger.debug(f"Fetching from r/{subreddit}: {url}")
    response = await async_get(
        url,
        respect_robots_txt=settings.respect_robots_txt,
    )
    response.raise_for_status()
    return response.json()


async def collect(
    conn,
    settings: Settings,
    lexicon_rows: list[dict[str, str]] | None = None,
    source_reliability: float = 0.70,
    account_ids: list[str] | None = None,
    lexicon_by_source: dict[str, list[dict[str, str]]] | None = None,
    source_reliability_dict: dict[str, float] | None = None,
    db_pool=None,
) -> dict[str, int]:
    """
    Collects signals from official company Reddit subreddits.

    This collector focuses on high-signal-quality sources by finding official
    company subreddits (r/stripe, r/notion, r/github, etc.) and extracting
    signals from their recent posts.
    """

    # Handle calling conventions
    if lexicon_by_source is None:
        lexicon_by_source = {}
    if source_reliability_dict is not None:
        source_reliability = source_reliability_dict.get("reddit_official", 0.70)
    if lexicon_rows is None:
        lexicon_rows = lexicon_by_source.get("community", [])

    logger.info(
        f"reddit_official.collect() starting: source_reliability={source_reliability}, lexicon_rows={len(lexicon_rows) if lexicon_rows else 0}"
    )

    if not settings.enable_live_crawl:
        logger.info("reddit_official: live_crawl disabled, returning early")
        return {"inserted": 0, "seen": 0, "accounts_processed": 0}

    # Load official subreddit mappings from CSV
    subreddit_rows = load_csv_rows(settings.subreddit_mapping_path)
    official_subreddits = {row["company_name"]: row["subreddit_name"] for row in subreddit_rows}

    inserted_total = 0
    seen_total = 0
    accounts_processed = 0

    # Get accounts - use provided account_ids or fetch all accounts
    if account_ids:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT account_id, company_name, domain FROM signals.accounts WHERE account_id = ANY(%s)", (account_ids,)
        )
        accounts = [dict(row) for row in cursor.fetchall()]
    else:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT account_id, company_name, domain FROM signals.accounts LIMIT %s", (settings.live_max_accounts,)
        )
        accounts = [dict(row) for row in cursor.fetchall()]

    if not accounts:
        logger.warning("reddit_official: no accounts found, returning early")
        return {"inserted": 0, "seen": 0, "accounts_processed": 0}

    logger.info(f"reddit_official: processing {len(accounts)} accounts, checking for official subreddits")

    # Create HTTP client with rate limiting
    headers = {
        "User-Agent": REDDIT_USER_AGENT,
    }

    async with httpx.AsyncClient(
        headers=headers,
        follow_redirects=True,
        timeout=settings.http_timeout_seconds,
    ) as _client:
        for account_index, account in enumerate(accounts, 1):
            account_id = str(account["account_id"])
            company_name = str(account["company_name"] or account["domain"])

            # Check if this company has an official subreddit
            subreddit = official_subreddits.get(company_name)

            if not subreddit:
                logger.debug(f"reddit_official #{account_index}: {company_name} - no official subreddit in mapping")
                continue

            # Skip if already crawled today
            if db.was_crawled_today(conn, account_id, endpoint=subreddit, source="reddit_official"):
                logger.debug(f"reddit_official #{account_index}: {company_name} - already crawled today")
                continue

            logger.info(f"reddit_official #{account_index}: {company_name} - fetching from r/{subreddit}")

            try:
                # Fetch posts from subreddit
                data = await _fetch_official_subreddit(subreddit, settings)

                if not data:
                    logger.warning(f"reddit_official #{account_index}: empty response for r/{subreddit}")
                    continue

                posts_raw = data.get("data", {}).get("children", [])
                logger.info(f"reddit_official #{account_index}: fetched {len(posts_raw)} posts from r/{subreddit}")

                if not posts_raw:
                    logger.warning(f"reddit_official #{account_index}: no posts in r/{subreddit}")
                    accounts_processed += 1
                    continue

                inserted = 0
                seen = 0

                # Process each post
                for entry in posts_raw:
                    item = entry.get("data", {})

                    try:
                        # Extract post data
                        post = RedditPost(
                            title=item.get("title", ""),
                            selftext=item.get("selftext", ""),
                            url=item.get("url", "") or f"https://reddit.com{item.get('permalink', '')}",
                            subreddit=item.get("subreddit", ""),
                            author=item.get("author", ""),
                            created_utc=float(item.get("created_utc", 0.0)),
                            score=int(item.get("score", 0)),
                            num_comments=int(item.get("num_comments", 0)),
                        )

                        # Classify the post
                        text_to_classify = f"{post.title}\n{post.selftext}".strip()
                        matches = classify_text(text_to_classify, lexicon_rows)

                        # If no keyword matches, mark as community_mention if it's official content
                        if not matches:
                            matches = [("community_mention", 0.7, "official_sub")]

                        for signal_code, confidence, _ in matches:
                            seen += 1
                            obs = _build_observation(
                                account_id=account_id,
                                post=post,
                                signal_code=signal_code,
                                confidence=confidence,
                                reliability=source_reliability,
                            )

                            if db.insert_signal_observation(conn, obs, commit=False):
                                inserted += 1
                                logger.debug(f"reddit_official #{account_index}: inserted signal {signal_code}")

                    except ValidationError as e:
                        logger.debug(f"reddit_official #{account_index}: validation error - {e}")
                        continue
                    except Exception as e:
                        logger.error(f"reddit_official #{account_index}: error processing post - {e}")
                        continue

                logger.info(f"reddit_official #{account_index}: {company_name} - {inserted} inserted, {seen} seen")

                inserted_total += inserted
                seen_total += seen
                accounts_processed += 1

                # Mark as crawled for today
                db.mark_crawled(conn, source="reddit_official", account_id=account_id, endpoint=subreddit, commit=False)

                # Respect rate limits - be nice to Reddit
                await asyncio.sleep(2)

            except Exception as exc:
                logger.error(
                    f"reddit_official #{account_index}: {company_name} - exception: {type(exc).__name__}: {exc}"
                )
                accounts_processed += 1
                continue

    conn.commit()
    logger.info(
        f"reddit_official: completed - inserted={inserted_total}, seen={seen_total}, accounts_processed={accounts_processed}"
    )

    return {"inserted": inserted_total, "seen": seen_total, "accounts_processed": accounts_processed}
