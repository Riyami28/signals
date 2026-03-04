from __future__ import annotations

import asyncio
import logging
import os
import random
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlencode

import httpx

from src import db
from src.models import SignalObservation
from src.settings import Settings
from src.source_policy import load_source_execution_policy
from src.utils import (
    classify_text,
    load_account_source_handles,
    load_csv_rows,
    stable_hash,
    utc_now_iso,
)

logger = logging.getLogger(__name__)


def load_twitter_handles(path) -> dict[str, str]:
    """Load domain -> Twitter handle mapping."""
    handles = {}
    rows = load_csv_rows(path)
    for row in rows:
        domain = row.get("domain", "").strip().lower()
        handle = row.get("twitter_handle", "").strip()
        if domain and handle:
            handles[domain] = handle
    return handles

TWITTER_OFFICIAL_SEARCH_URL = "https://api.twitter.com/2/tweets/search/recent"
DEFAULT_TWITTER_TERMS = (
    "(hiring OR \"we're hiring\" OR devops OR kubernetes OR terraform OR finops "
    "OR \"cloud cost\" OR \"cloud migration\" OR \"digital transformation\" "
    "OR compliance OR soc2 OR \"cost reduction\" OR \"cost optimization\" "
    "OR \"funding round\" OR \"series a\" OR \"series b\" "
    "OR \"product launch\" OR \"supply chain\" OR \"vendor consolidation\" "
    "OR \"security audit\" OR outage OR ERP OR SAP "
    "OR \"platform engineering\" OR \"tool consolidation\" "
    "OR \"growing team\" OR modernization) -is:retweet lang:en"
)
_LIVE_PROGRESS_COMMIT_EVERY = 25
_VERBOSE_PROGRESS = os.getenv("SIGNALS_VERBOSE_PROGRESS", "").strip().lower() in {"1", "true", "yes", "on"}


def _emit_progress(message: str) -> None:
    if _VERBOSE_PROGRESS:
        print(message, flush=True)


def _twitter_search_query_url(query: str, lookback_days: int, max_results: int = 10) -> str:
    """Build URL for official Twitter API v2."""
    start_time = (datetime.now(timezone.utc) - timedelta(days=lookback_days)).strftime("%Y-%m-%dT%H:%M:%SZ")
    params = urlencode(
        {
            "query": query,
            "max_results": max_results,
            "tweet.fields": "created_at,text,author_id",
            "start_time": start_time,
        }
    )
    return f"{TWITTER_OFFICIAL_SEARCH_URL}?{params}"


def _rapidapi_search_url(
    host: str, query: str, count: int = 20, from_handle: str = "", since_id: str = ""
) -> str:
    """Build URL for RapidAPI Twttr API (twitter241 / search-v3 endpoint).

    If from_handle is provided, searches tweets FROM that specific Twitter handle.
    If since_id is provided, only returns tweets newer than that tweet ID (incremental fetch).
    """
    if from_handle:
        bare_handle = from_handle.lstrip("@")  # Twitter from: operator requires no @ prefix
        search_query = f"from:{bare_handle} ({query})"
    else:
        search_query = query

    p: dict[str, Any] = {"query": search_query, "count": count, "type": "Latest"}
    if since_id:
        p["since_id"] = since_id  # only fetch tweets newer than last seen
    params = urlencode(p)
    return f"https://{host}/search-v3?{params}"


def _rapidapi_headers(key: str, host: str) -> dict[str, str]:
    return {"X-RapidAPI-Key": key, "X-RapidAPI-Host": host}


def _parse_rapidapi_tweets(data: dict[str, Any]) -> list[dict[str, Any]]:
    """Normalise RapidAPI response into the same shape as official API tweets.

    Handles two response shapes:
      - twitter241 search-v3: nested GraphQL shape under result.timeline_response.timeline
      - legacy flat shape: {"timeline": [...]} or {"data": [...]}
    We normalise to {"id": "...", "text": "...", "created_at": "..."}.
    """
    tweets: list[dict[str, Any]] = []

    # twitter241 search-v3: GraphQL nested shape
    try:
        instructions = (
            data.get("result", {})
            .get("timeline_response", {})
            .get("timeline", {})
            .get("instructions", [])
        )
        if instructions:
            for instruction in instructions:
                for entry in instruction.get("entries", []):
                    content = entry.get("content", {})
                    if content.get("__typename") != "TimelineTimelineItem":
                        continue
                    inner = content.get("content", {})
                    if inner.get("__typename") != "TimelineTweet":
                        continue
                    result = inner.get("tweet_results", {}).get("result", {})
                    # API may use 'details' (twitter241 custom) or 'legacy' (standard GraphQL)
                    details = result.get("details") or result.get("legacy") or {}
                    text = str(details.get("full_text") or details.get("text") or "").strip()
                    tweet_id = str(result.get("rest_id") or "")
                    created_at_ms = details.get("created_at_ms")
                    if created_at_ms:
                        created_at = datetime.fromtimestamp(
                            int(created_at_ms) / 1000, tz=timezone.utc
                        ).isoformat()
                    else:
                        # Standard Twitter date string: "Mon Mar 04 12:00:00 +0000 2026"
                        created_at_str = details.get("created_at", "")
                        try:
                            created_at = datetime.strptime(
                                created_at_str, "%a %b %d %H:%M:%S +0000 %Y"
                            ).replace(tzinfo=timezone.utc).isoformat() if created_at_str else ""
                        except Exception:
                            created_at = created_at_str
                    if text:
                        tweets.append({"id": tweet_id, "text": text, "created_at": created_at})
            return tweets
    except Exception:
        pass

    # Legacy flat shape fallback: {"timeline": [...]} or {"data": [...]}
    raw = data.get("timeline") or data.get("data") or []
    for item in raw:
        tweet_id = str(item.get("tweet_id") or item.get("id") or item.get("id_str") or "")
        text = str(item.get("text") or item.get("full_text") or "").strip()
        created_at = str(item.get("created_at") or "")
        if text:
            tweets.append({"id": tweet_id, "text": text, "created_at": created_at})
    return tweets


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


def _parse_tweet_observed_at(tweet: dict[str, Any]) -> str:
    created_at = tweet.get("created_at", "")
    if created_at:
        try:
            return datetime.fromisoformat(created_at.replace("Z", "+00:00")).isoformat()
        except Exception:
            logger.warning("failed to parse tweet created_at=%s", created_at)
    return utc_now_iso()


def _ingest_tweets(
    conn,
    account_id: str,
    source: str,
    reliability: float,
    lexicon_rows: list[dict[str, str]],
    tweets: list[dict[str, Any]],
    extra_payload: dict[str, str],
) -> tuple[int, int]:
    inserted = 0
    seen = 0
    for tweet in tweets:
        tweet_id = str(tweet.get("id", ""))
        text = str(tweet.get("text", "")).strip()
        if not text:
            continue
        matches = classify_text(text, lexicon_rows)
        for signal_code, confidence, matched_keyword in matches:
            seen += 1
            evidence_url = f"https://twitter.com/i/web/status/{tweet_id}" if tweet_id else ""
            observation = _build_observation(
                account_id=account_id,
                signal_code=signal_code,
                source=source,
                observed_at=_parse_tweet_observed_at(tweet),
                confidence=confidence,
                source_reliability=reliability,
                evidence_url=evidence_url,
                evidence_text=text,
                payload={
                    "tweet_id": tweet_id,
                    "text": text,
                    "matched_keyword": matched_keyword,
                    **extra_payload,
                },
            )
            if db.insert_signal_observation(conn, observation, commit=False):
                inserted += 1
    return inserted, seen


def _extract_handle_from_response(data: dict, company_name: str, domain: str) -> str:
    """Auto-discover the official Twitter handle for a company from search results.

    Looks through tweet results for an author whose screen name or display name
    closely matches the company name or domain. Returns '@handle' or ''.
    """
    try:
        instructions = (
            data.get("result", {})
            .get("timeline_response", {})
            .get("timeline", {})
            .get("instructions", [])
        )
        company_lower = company_name.lower().replace(" ", "").replace("-", "").replace(".", "")
        domain_root = domain.split(".")[0].replace("-", "").lower()

        for instruction in instructions:
            for entry in instruction.get("entries", []):
                content = entry.get("content", {})
                if content.get("__typename") != "TimelineTimelineItem":
                    continue
                inner = content.get("content", {})
                if inner.get("__typename") != "TimelineTweet":
                    continue
                result = inner.get("tweet_results", {}).get("result", {})
                # Try to get author info
                core = result.get("core", {})
                user_result = core.get("user_results", {}).get("result", {})
                legacy = user_result.get("legacy", {})
                screen_name = str(legacy.get("screen_name", "") or "")
                display_name = str(legacy.get("name", "") or "")

                if not screen_name:
                    continue

                sn_lower = screen_name.lower().replace("_", "").replace("-", "")
                dn_lower = display_name.lower().replace(" ", "").replace("-", "")

                # Match if screen name or display name contains company name / domain root
                if (company_lower in sn_lower or sn_lower in company_lower or
                        domain_root in sn_lower or sn_lower in domain_root or
                        company_lower in dn_lower or dn_lower in company_lower):
                    return f"@{screen_name}"
    except Exception as exc:
        logger.debug("handle_autodiscovery_failed company=%s error=%s", company_name, exc)
    return ""


def _save_handle_to_csv(domain: str, handle: str, path) -> None:
    """Append newly discovered Twitter handle to the CSV (avoid duplicates)."""
    try:
        existing = load_csv_rows(path)
        existing_domains = {r.get("domain", "").strip().lower() for r in existing}
        if domain.lower() in existing_domains:
            return  # Already exists
        with open(path, "a", encoding="utf-8") as f:
            f.write(f"{domain},{handle}\n")
        logger.info("twitter_handle_saved domain=%s handle=%s", domain, handle)
    except Exception as exc:
        logger.warning("twitter_handle_save_failed domain=%s error=%s", domain, exc)


async def _collect_live_twitter_account(
    conn,
    settings: Settings,
    lexicon_rows: list[dict[str, str]],
    account: dict[str, Any],
    account_index: int,
    twitter_handles: dict[str, str],
    api_source: str,
    api_reliability: float,
    bearer_token: str,
    rapidapi_key: str,
    rapidapi_host: str,
    client: httpx.AsyncClient,
) -> tuple[int, int, int]:
    domain = str(account["domain"])
    if domain.endswith(".example"):
        return 0, 0, 0

    account_id = str(account["account_id"])
    _emit_progress(f"collector=twitter_live status=account_started account_index={account_index} domain={domain}")

    company_name = str(account["company_name"] or domain)

    # Check CSV/cache for known official Twitter handle
    official_handle = twitter_handles.get(domain, "").strip()
    # Twitter from: operator requires handle WITHOUT @ (e.g. from:netflix not from:@netflix)
    handle_bare = official_handle.lstrip("@")

    search_keywords = DEFAULT_TWITTER_TERMS

    # Load incremental cursor — only fetch tweets newer than last seen
    since_id = db.get_twitter_since_id(conn, account_id)

    # Choose API backend
    use_rapidapi = bool(rapidapi_key)
    lookback_days = getattr(settings, "twitter_lookback_days", 7)
    if use_rapidapi:
        if official_handle:
            # ✓ Known handle → get ALL tweets from official account, let lexicon classify
            query = f"from:{handle_bare} -is:retweet lang:en"
        else:
            # No known handle → search company name + domain WITH signal keywords
            query = f'("{company_name}" OR "{domain}") {search_keywords}'

        # Validate query length — Twitter API rejects > 512 chars with cryptic error
        if len(query) > 512:
            query = query[:509] + "..."
            logger.warning("twitter_query_truncated domain=%s len=%d", domain, 512)

        if official_handle:
            search_url = _rapidapi_search_url(
                rapidapi_host, "-is:retweet lang:en", from_handle=handle_bare, since_id=since_id
            )
        else:
            search_url = _rapidapi_search_url(rapidapi_host, query, since_id=since_id)
        req_headers = _rapidapi_headers(rapidapi_key, rapidapi_host)
    else:
        if official_handle:
            # ✓ Known handle → get ALL tweets, lexicon will classify
            full_query = f"from:{handle_bare} -is:retweet lang:en"
        else:
            full_query = f'("{company_name}" OR "{domain}") {search_keywords}'

        # Validate query length — Twitter API rejects > 512 chars with cryptic error
        if len(full_query) > 512:
            full_query = full_query[:509] + "..."
            logger.warning("twitter_query_truncated domain=%s len=%d", domain, 512)

        query = full_query
        search_url = _twitter_search_query_url(full_query, lookback_days)
        req_headers = {"Authorization": f"Bearer {bearer_token}"}

    if db.was_crawled_today(conn, source=api_source, account_id=account_id, endpoint=search_url):
        db.record_crawl_attempt(
            conn,
            source=api_source,
            account_id=account_id,
            endpoint=search_url,
            status="skipped",
            error_summary="checkpoint_recent",
            commit=False,
        )
        return 0, 0, 1

    try:
        response = await client.get(search_url, headers=req_headers)
        response.raise_for_status()
        data = response.json()
    except httpx.HTTPStatusError as exc:
        status_code = exc.response.status_code if exc.response is not None else 0
        if status_code == 429:
            logger.warning(
                "twitter_rate_limit_hit domain=%s source=%s backend=%s — "
                "STOPPING remaining requests to preserve hourly quota",
                domain,
                api_source,
                "rapidapi" if use_rapidapi else "official",
            )
            # DO NOT mark as crawled — let it retry next run when quota resets
            db.record_crawl_attempt(
                conn,
                source=api_source,
                account_id=account_id,
                endpoint=search_url,
                status="rate_limited",
                error_summary="429_rate_limit",
                commit=False,
            )
            raise  # Propagate to stop further requests
        db.record_crawl_attempt(
            conn,
            source=api_source,
            account_id=account_id,
            endpoint=search_url,
            status="http_error",
            error_summary=f"status_code={status_code}",
            commit=False,
        )
        db.mark_crawled(conn, source=api_source, account_id=account_id, endpoint=search_url, commit=False)
        return 0, 0, 1
    except Exception as exc:
        db.record_crawl_attempt(
            conn,
            source=api_source,
            account_id=account_id,
            endpoint=search_url,
            status="exception",
            error_summary=str(exc),
            commit=False,
        )
        db.mark_crawled(conn, source=api_source, account_id=account_id, endpoint=search_url, commit=False)
        return 0, 0, 1

    db.record_crawl_attempt(
        conn,
        source=api_source,
        account_id=account_id,
        endpoint=search_url,
        status="success",
        error_summary="",
        commit=False,
    )
    db.mark_crawled(conn, source=api_source, account_id=account_id, endpoint=search_url, commit=False)

    # Normalise response — RapidAPI and official API have different shapes
    tweets = _parse_rapidapi_tweets(data) if use_rapidapi else (data.get("data", []) or [])

    # AUTO-DISCOVER Twitter handle from search results (for companies without known handle)
    # If tweets were found and no official handle is known, extract author handle from results
    if tweets and not official_handle and use_rapidapi:
        discovered = _extract_handle_from_response(data, company_name, domain)
        if discovered:
            twitter_handles[domain] = discovered  # Cache in memory for this run
            logger.info(
                "twitter_handle_autodiscovered domain=%s handle=%s", domain, discovered
            )
            _save_handle_to_csv(
                domain, discovered,
                settings.project_root / "config" / "company_twitter_handles.csv"
            )

    inserted_delta, seen_delta = _ingest_tweets(
        conn=conn,
        account_id=account_id,
        source=api_source,
        reliability=api_reliability,
        lexicon_rows=lexicon_rows,
        tweets=tweets,
        extra_payload={"query": query, "search_url": search_url, "handle": official_handle or "auto"},
    )

    # Save max tweet_id seen so next run only fetches NEW tweets (incremental cursor)
    if tweets:
        valid_ids = [t["id"] for t in tweets if t.get("id") and str(t["id"]).isdigit()]
        if valid_ids:
            max_tweet_id = max(valid_ids, key=lambda tid: int(tid))
            db.save_twitter_since_id(conn, account_id, max_tweet_id, commit=False)

    _emit_progress(
        "collector=twitter_live status=account_completed "
        f"account_index={account_index} domain={domain} handle={official_handle or 'fallback'} "
        f"tweets={len(tweets)} inserted_delta={inserted_delta} seen_delta={seen_delta} "
        f"since_id_updated={bool(tweets and any(t.get('id') for t in tweets))}"
    )
    return inserted_delta, seen_delta, 1


async def _collect_live_twitter_async(
    conn,
    settings: Settings,
    lexicon_rows: list[dict[str, str]],
    accounts: list[dict[str, Any]],
    twitter_handles: dict[str, str],
    api_source: str,
    api_reliability: float,
    bearer_token: str,
    rapidapi_key: str,
    rapidapi_host: str,
    db_pool=None,
) -> tuple[int, int]:
    if not accounts:
        return 0, 0

    # Load source-specific execution policy (twitter_api = 1 worker, 0.067 req/sec)
    source_policies = load_source_execution_policy(
        settings.project_root / "config" / "source_execution_policy.csv"
    )
    twitter_policy = source_policies.get(api_source)
    if twitter_policy:
        policy_workers = twitter_policy.max_parallel_workers
        req_delay = 1.0 / twitter_policy.requests_per_second if twitter_policy.requests_per_second > 0 else 15.0
    else:
        policy_workers = 1  # Default: always sequential for Twitter to respect rate limits
        req_delay = 15.0    # 15 seconds between requests = 4 per minute = safe for free tier

    concurrency = min(max(1, policy_workers), len(accounts))
    semaphore = asyncio.Semaphore(concurrency)
    inserted_total = 0
    seen_total = 0
    failed_workers = 0
    logger.info(
        "twitter_async_config concurrency=%d req_delay_secs=%.1f accounts=%d",
        concurrency, req_delay, len(accounts),
    )

    # Rate-limit flag: once we hit 429, stop all remaining workers
    rate_limited = asyncio.Event()

    async with httpx.AsyncClient(
        headers={"User-Agent": settings.http_user_agent},
        follow_redirects=True,
        timeout=settings.http_timeout_seconds,
    ) as client:

        async def _run_account(account_index: int, account: dict) -> tuple[int, int, int]:
            if rate_limited.is_set():
                return 0, 0, 0  # Skip — quota exhausted
            async with semaphore:
                if rate_limited.is_set():
                    return 0, 0, 0
                # Pace requests with jitter to avoid predictable API hit patterns
                if account_index > 1:
                    jitter_delay = random.uniform(0.8 * req_delay, 1.2 * req_delay)
                    await asyncio.sleep(jitter_delay)
                try:
                    return await _collect_live_twitter_account(
                        conn=conn,
                        settings=settings,
                        lexicon_rows=lexicon_rows,
                        account=account,
                        account_index=account_index,
                        twitter_handles=twitter_handles,
                        api_source=api_source,
                        api_reliability=api_reliability,
                        bearer_token=bearer_token,
                        rapidapi_key=rapidapi_key,
                        rapidapi_host=rapidapi_host,
                        client=client,
                    )
                except httpx.HTTPStatusError as exc:
                    if exc.response is not None and exc.response.status_code == 429:
                        rate_limited.set()
                        logger.warning(
                            "twitter_rate_limit_global quota_exhausted=true "
                            "stopping all remaining workers"
                        )
                        return 0, 0, 0
                    return 0, 0, 1

        tasks = [_run_account(i, acct) for i, acct in enumerate(accounts, start=1)]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    conn.commit()
    for result in results:
        if isinstance(result, Exception):
            logger.error("collector_worker_failed source=twitter error=%s", result, exc_info=True)
            failed_workers += 1
            continue
        inserted_delta, seen_delta, _ = result
        inserted_total += inserted_delta
        seen_total += seen_delta

    logger.info(
        "collection_complete source=twitter inserted=%d seen=%d failed_workers=%d",
        inserted_total,
        seen_total,
        failed_workers,
    )
    return inserted_total, seen_total


async def collect(
    conn,
    settings: Settings,
    lexicon_by_source: dict[str, list[dict[str, str]]],
    source_reliability: dict[str, float],
    account_ids: list[str] | None = None,
    db_pool=None,
) -> dict[str, int]:
    inserted = 0
    seen = 0
    accounts_processed = 0

    lexicon_rows = lexicon_by_source.get("twitter", [])
    csv_source = "twitter_csv"
    csv_reliability = source_reliability.get(csv_source, 0.70)

    if csv_reliability > 0:
        for row in load_csv_rows(settings.raw_dir / "twitter.csv"):
            domain = row.get("domain", "")
            if not domain:
                continue
            company_name = row.get("company_name", "") or domain
            account_id = db.upsert_account(
                conn,
                company_name=company_name,
                domain=domain,
                source_type="discovered",
                commit=False,
            )

            text = row.get("text", "")
            explicit_signal = row.get("signal_code", "")
            if explicit_signal:
                try:
                    explicit_confidence = float(row.get("confidence", "0.7") or 0.7)
                except ValueError:
                    explicit_confidence = 0.7
                matches = [(explicit_signal, explicit_confidence, "explicit")]
            else:
                matches = classify_text(text, lexicon_rows)

            observed_at = row.get("observed_at", "") or utc_now_iso()
            for signal_code, confidence, matched_keyword in matches:
                seen += 1
                observation = _build_observation(
                    account_id=account_id,
                    signal_code=signal_code,
                    source=csv_source,
                    observed_at=observed_at,
                    confidence=confidence,
                    source_reliability=csv_reliability,
                    evidence_url=row.get("url", ""),
                    evidence_text=text,
                    payload={"row": row, "matched_keyword": matched_keyword},
                )
                if db.insert_signal_observation(conn, observation, commit=False):
                    inserted += 1

    bearer_token = getattr(settings, "twitter_bearer_token", "").strip()
    rapidapi_key = getattr(settings, "twitter_rapidapi_key", "").strip()
    rapidapi_host = getattr(settings, "twitter_rapidapi_host", "twitter-api45.p.rapidapi.com").strip()
    has_any_key = bool(rapidapi_key or bearer_token)

    if settings.enable_live_crawl and has_any_key:
        # Load official Twitter handle mappings (ACTUAL TWEETS FROM OFFICIAL ACCOUNTS)
        twitter_handles_path = settings.project_root / "config" / "company_twitter_handles.csv"
        twitter_handles = load_twitter_handles(twitter_handles_path)

        api_source = "twitter_api"
        api_reliability = source_reliability.get(api_source, 0.75)

        if api_reliability <= 0:
            conn.commit()
            return {"inserted": inserted, "seen": seen}

        backend = "rapidapi" if rapidapi_key else "official"
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
            accounts = db.select_accounts_for_live_crawl(
                conn,
                source=api_source,
                limit=settings.live_max_accounts,
                include_domains=list(settings.live_target_domains),
            )

        # Count how many have official handles vs will use fallback search
        accounts_processed = len(accounts)
        accounts_with_handles = [a for a in accounts if a["domain"] in twitter_handles]
        accounts_fallback = [a for a in accounts if a["domain"] not in twitter_handles]

        logger.info(
            "twitter_live_plan total=%d with_official_handle=%d using_fallback_search=%d",
            len(accounts), len(accounts_with_handles), len(accounts_fallback),
        )
        # ✓ ALL companies get searched — handles → official tweets, others → company mentions with keywords
        _emit_progress(
            f"collector=twitter_live status=started backend={backend} "
            f"accounts_total={len(accounts)} with_handles={len(accounts_with_handles)} "
            f"fallback_search={len(accounts_fallback)}"
        )
        live_inserted, live_seen = await _collect_live_twitter_async(
            conn=conn,
            settings=settings,
            lexicon_rows=lexicon_rows,
            accounts=accounts,
            twitter_handles=twitter_handles,
            api_source=api_source,
            api_reliability=api_reliability,
            bearer_token=bearer_token,
            rapidapi_key=rapidapi_key,
            rapidapi_host=rapidapi_host,
            db_pool=db_pool,
        )
        inserted += live_inserted
        seen += live_seen
        _emit_progress(
            "collector=twitter_live status=completed "
            f"accounts_targeted={len(accounts)} inserted_total={inserted} seen_total={seen}"
        )
    elif settings.enable_live_crawl and not has_any_key:
        logger.warning(
            "twitter_live_crawl_skipped reason=no_api_key "
            "set SIGNALS_TWITTER_RAPIDAPI_KEY (RapidAPI) or SIGNALS_TWITTER_BEARER_TOKEN (official)"
        )

    conn.commit()
    return {"inserted": inserted, "seen": seen, "accounts_processed": accounts_processed}
