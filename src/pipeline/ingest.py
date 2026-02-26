"""Collector orchestration — ingest stage."""

from __future__ import annotations

import asyncio
from datetime import date

from src import db
from src.collectors import community, first_party, jobs, news, technographics
from src.pipeline.helpers import bootstrap
from src.scoring.rules import load_keyword_lexicon, load_source_registry
from src.settings import Settings
from src.source_policy import load_source_execution_policy


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
    results["first_party"] = (
        first_party.collect(conn, settings, lexicon, source_reliability)
        if _collector_enabled("first_party_csv")
        else {"inserted": 0, "seen": 0}
    )
    return results


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
