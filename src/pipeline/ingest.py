"""Collector orchestration — ingest stage."""

from __future__ import annotations

import asyncio
import logging
from datetime import date

from src import db
from src.collectors import (
    community,
    first_party,
    gnews_collector,
    jobs,
    news,
    reddit_collector,
    reddit_official,
    serper_reddit,
    serper_twitter,
    technographics,
    twitter,
    website_techscan,
)
from src.integrations.crunchbase import CrunchbaseClient, enrich_firmographics, evaluate_firmographic_signals
from src.models import SignalObservation
from src.pipeline.helpers import bootstrap
from src.scoring.rules import load_keyword_lexicon, load_source_registry
from src.settings import Settings
from src.source_policy import load_source_execution_policy
from src.utils import stable_hash, utc_now_iso

logger = logging.getLogger(__name__)


async def _collect_all_async(conn, settings: Settings) -> dict[str, dict[str, int]]:
    lexicon = load_keyword_lexicon(settings.keyword_lexicon_path)
    source_reliability = load_source_registry(settings.source_registry_path)
    execution_policy = load_source_execution_policy(settings.source_execution_policy_path)

    def _collector_enabled(policy_key: str) -> bool:
        policy = execution_policy.get(policy_key.strip().lower())
        return bool(policy.enabled) if policy is not None else True

    results: dict[str, dict[str, int]] = {}
    results["jobs"] = (
        await jobs.collect(conn, settings, lexicon, source_reliability)
        if _collector_enabled("jobs_pages")
        else {"inserted": 0, "seen": 0}
    )
    results["news"] = (
        await news.collect(conn, settings, lexicon, source_reliability)
        if _collector_enabled("news_rss")
        else {"inserted": 0, "seen": 0}
    )
    results["technographics"] = (
        await technographics.collect(conn, settings, lexicon, source_reliability)
        if _collector_enabled("technographics")
        else {"inserted": 0, "seen": 0}
    )
    results["community"] = (
        await community.collect(conn, settings, lexicon, source_reliability)
        if _collector_enabled("reddit_api")
        else {"inserted": 0, "seen": 0}
    )
    results["reddit"] = (
        await reddit_collector.collect(
            conn=conn, settings=settings, lexicon_by_source=lexicon, source_reliability_dict=source_reliability
        )
        if _collector_enabled("reddit_api")
        else {"inserted": 0, "seen": 0}
    )
    results["reddit_official"] = (
        await reddit_official.collect(
            conn=conn,
            settings=settings,
            lexicon_rows=lexicon.get("community", []),
            source_reliability=source_reliability.get("reddit_official", 0.75),
        )
        if _collector_enabled("reddit_official")
        else {"inserted": 0, "seen": 0}
    )
    results["first_party"] = (
        first_party.collect(conn, settings, lexicon, source_reliability)
        if _collector_enabled("first_party_csv")
        else {"inserted": 0, "seen": 0}
    )

    # Website tech scan (FREE — no API key needed)
    if _collector_enabled("website_techscan"):
        results["website_techscan"] = await website_techscan.collect(
            conn,
            settings,
            source_reliability=source_reliability.get("website_techscan", 0.70),
        )
    else:
        results["website_techscan"] = {"inserted": 0, "seen": 0}

    # GNews (free tier: 100 req/day)
    if _collector_enabled("gnews") and settings.gnews_api_key:
        all_news_lexicon = []
        for source_key in ("news", "technographics", "community"):
            all_news_lexicon.extend(r for r in lexicon.get(source_key, []) if r.get("keyword"))
        results["gnews"] = await gnews_collector.collect(
            conn,
            settings,
            lexicon_rows=all_news_lexicon,
            source_reliability=source_reliability.get("gnews", 0.78),
        )
    else:
        results["gnews"] = {"inserted": 0, "seen": 0}

    # Twitter API (RapidAPI / official — incremental with since_id cursor)
    results["twitter"] = (
        await twitter.collect(conn, settings, lexicon, source_reliability)
        if _collector_enabled("twitter_api")
        else {"inserted": 0, "seen": 0}
    )

    # Serper Twitter (Google-indexed Twitter content — complements RapidAPI coverage)
    results["serper_twitter"] = (
        await serper_twitter.collect(conn, settings, lexicon, source_reliability)
        if _collector_enabled("serper_twitter")
        else {"inserted": 0, "seen": 0}
    )

    # Serper Reddit (Google-indexed Reddit posts about companies + cloud/devops)
    results["serper_reddit"] = (
        await serper_reddit.collect(conn, settings, lexicon, source_reliability)
        if _collector_enabled("serper_reddit")
        else {"inserted": 0, "seen": 0}
    )

    # Crunchbase firmographic enrichment (paid API — skipped when no key)
    results["crunchbase"] = _collect_crunchbase(conn, settings, source_reliability)

    return results


def _collect_crunchbase(
    conn,
    settings: Settings,
    source_reliability: dict[str, float],
) -> dict[str, int]:
    """Collect firmographic signals from Crunchbase API.

    Requires ``settings.crunchbase_api_key`` to be set.  Only processes
    HIGH/MEDIUM tier accounts that don't already have firmographic signals.
    """
    if not settings.crunchbase_api_key:
        return {"inserted": 0, "seen": 0}

    client = CrunchbaseClient(
        api_key=settings.crunchbase_api_key,
        rate_limit=settings.crunchbase_rate_limit,
    )
    reliability = source_reliability.get("crunchbase", 0.82)

    # Only enrich HIGH/MEDIUM accounts without recent firmographic signals
    rows = conn.execute(
        """
        SELECT DISTINCT a.account_id, a.domain
        FROM accounts a
        JOIN account_scores s ON a.account_id = s.account_id
        WHERE s.tier IN ('high', 'medium')
          AND a.domain != ''
          AND NOT EXISTS (
              SELECT 1 FROM signal_observations so
              WHERE so.account_id = a.account_id
                AND so.source = 'crunchbase'
                AND so.observed_at > CURRENT_TIMESTAMP - INTERVAL '30 days'
          )
        ORDER BY a.domain
        LIMIT 100
        """,
    ).fetchall()

    inserted = 0
    seen = 0
    for row in rows:
        account_id = str(row["account_id"])
        domain = str(row["domain"])
        company = enrich_firmographics(domain, client)
        if company is None:
            continue
        signals = evaluate_firmographic_signals(company)
        for sig in signals:
            seen += 1
            obs = SignalObservation(
                observation_id=stable_hash(
                    {"account": account_id, "signal": sig["signal_code"], "source": "crunchbase"},
                    prefix="obs",
                ),
                account_id=account_id,
                signal_code=sig["signal_code"],
                product=sig.get("product", "shared"),
                source="crunchbase",
                observed_at=sig.get("observed_at", utc_now_iso()),
                evidence_url=f"https://www.crunchbase.com/organization/{domain}",
                evidence_text=sig.get("evidence_text", ""),
                confidence=sig.get("confidence", 0.8),
                source_reliability=reliability,
                raw_payload_hash=stable_hash({"domain": domain, "signal": sig["signal_code"]}, prefix="raw"),
            )
            if db.insert_signal_observation(conn, obs, commit=False):
                inserted += 1

    if inserted:
        conn.commit()
    logger.info("crunchbase_collect seen=%d inserted=%d accounts=%d", seen, inserted, len(rows))
    return {"inserted": inserted, "seen": seen}


def collect_all(conn, settings: Settings) -> dict[str, dict[str, int]]:
    return asyncio.run(_collect_all_async(conn, settings))


def run_ingest_cycle(run_date: date) -> dict[str, int | str]:
    settings, conn, seeded = bootstrap()
    del seeded
    try:
        collect_results = collect_all(conn, settings)
        collect_inserted = sum(result["inserted"] for result in collect_results.values())
        collect_seen = sum(result["seen"] for result in collect_results.values())
        return {
            "run_date": run_date.isoformat(),
            "observations_seen": collect_seen,
            "observations_inserted": collect_inserted,
        }
    finally:
        conn.close()
