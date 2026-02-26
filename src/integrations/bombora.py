"""
Bombora Company Surge intent data integration.

Fetches topic-level intent surge scores for scored accounts from the
Bombora API.  Surge scores indicate anonymous B2B content consumption
across 5,000+ websites.

Signal mapping:
    surge_score > 70   -> bombora_surge_high     (weight=20, confidence=0.85)
    surge_score 50-70  -> bombora_surge_moderate  (weight=10, confidence=0.70)
    3+ related topics  -> bombora_topic_cluster   (weight=15, confidence=0.75)

All signals use dimension=trigger_intent and product=shared.
"""

from __future__ import annotations

import logging
from typing import Any

import requests

from src import db
from src.models import SignalObservation
from src.settings import Settings
from src.utils import stable_hash, utc_now_iso

logger = logging.getLogger(__name__)

_TIMEOUT = 30  # seconds for HTTP calls

# Topics that map to the three products (zopdev, zopday, zopnight).
RELEVANT_TOPICS: dict[str, list[str]] = {
    "cloud_cost": [
        "cloud cost optimization",
        "finops",
        "cloud financial management",
        "cloud spend management",
    ],
    "kubernetes": [
        "kubernetes",
        "container orchestration",
        "docker containers",
        "container management",
    ],
    "devops": [
        "devops transformation",
        "devops",
        "ci/cd",
        "continuous integration",
    ],
    "platform_eng": [
        "developer platforms",
        "internal developer platform",
        "platform engineering",
    ],
    "infra_automation": [
        "infrastructure automation",
        "infrastructure as code",
        "terraform",
    ],
    "cloud_migration": [
        "cloud migration",
        "cloud transformation",
        "hybrid cloud",
    ],
}

# Flat set for quick lookup.
_ALL_RELEVANT_TOPICS: set[str] = set()
for _topics in RELEVANT_TOPICS.values():
    _ALL_RELEVANT_TOPICS.update(_topics)


# ---------------------------------------------------------------------------
# Bombora API client helpers
# ---------------------------------------------------------------------------


def _fetch_surge_data(
    domain: str,
    api_key: str,
    base_url: str,
) -> list[dict[str, Any]]:
    """Fetch surge scores for a single domain from the Bombora API.

    Returns a list of topic dicts:
        [{"topic": "...", "surge_score": 82, "topic_id": "..."}, ...]
    """
    resp = requests.get(
        f"{base_url}/surge/company",
        params={"domain": domain},
        headers={"Authorization": f"Bearer {api_key}", "Accept": "application/json"},
        timeout=_TIMEOUT,
    )
    resp.raise_for_status()
    data = resp.json()
    return data.get("topics", [])


def _is_relevant_topic(topic_name: str) -> bool:
    """Check if a topic name matches any of our relevant topics."""
    topic_lower = topic_name.lower().strip()
    for relevant in _ALL_RELEVANT_TOPICS:
        if relevant in topic_lower or topic_lower in relevant:
            return True
    return False


def _find_topic_clusters(
    topics: list[dict[str, Any]],
    surge_threshold: int,
) -> list[str]:
    """Find clusters of related surging topics.

    A cluster is a RELEVANT_TOPICS category where 2+ topics are surging
    above the moderate threshold.
    """
    clusters: list[str] = []
    for cluster_name, cluster_keywords in RELEVANT_TOPICS.items():
        surging = 0
        for topic in topics:
            topic_name = topic.get("topic", "").lower().strip()
            score = int(topic.get("surge_score", 0))
            if score < surge_threshold:
                continue
            for kw in cluster_keywords:
                if kw in topic_name or topic_name in kw:
                    surging += 1
                    break
        if surging >= 2:
            clusters.append(cluster_name)
    return clusters


def _build_observation(
    account_id: str,
    signal_code: str,
    confidence: float,
    source_reliability: float,
    evidence_text: str,
    payload: dict[str, Any],
) -> SignalObservation:
    """Build a SignalObservation for a Bombora surge signal."""
    raw_hash = stable_hash(payload, prefix="raw")
    obs_id = stable_hash(
        {"account_id": account_id, "signal_code": signal_code, "hash": raw_hash},
        prefix="obs",
    )
    return SignalObservation(
        obs_id=obs_id,
        account_id=account_id,
        signal_code=signal_code,
        product="shared",
        source="bombora_api",
        observed_at=utc_now_iso(),
        evidence_url="",
        evidence_text=evidence_text[:500],
        confidence=max(0.0, min(1.0, confidence)),
        source_reliability=max(0.0, min(1.0, source_reliability)),
        raw_payload_hash=raw_hash,
    )


# ---------------------------------------------------------------------------
# Public collector entry point
# ---------------------------------------------------------------------------


def collect(
    conn,
    settings: Settings,
    lexicon_by_source: dict[str, list[dict[str, str]]],
    source_reliability: dict[str, float],
) -> dict[str, int]:
    """Collect Bombora surge intent signals for all scored accounts.

    Returns ``{"inserted": N, "seen": M}``.
    """
    api_key = settings.bombora_api_key
    if not api_key:
        logger.info("bombora: skipped — no SIGNALS_BOMBORA_API_KEY configured")
        return {"inserted": 0, "seen": 0}

    base_url = settings.bombora_api_base_url
    threshold_high = settings.bombora_surge_threshold_high
    threshold_moderate = settings.bombora_surge_threshold_moderate
    cluster_min = settings.bombora_topic_cluster_min
    reliability = source_reliability.get("bombora_api", 0.82)

    # Fetch all accounts to check surge data for.
    accounts = conn.execute("SELECT account_id, domain FROM accounts").fetchall()
    logger.info("bombora: checking surge data for %d accounts", len(accounts))

    inserted = 0
    seen = 0
    errors = 0

    for row in accounts:
        account_id = str(row["account_id"])
        domain = str(row["domain"])
        if not domain:
            continue

        # Skip if already crawled today.
        if db.was_crawled_today(conn, "bombora_api", account_id, "surge"):
            continue

        try:
            topics = _fetch_surge_data(domain, api_key, base_url)
        except requests.RequestException as exc:
            logger.warning("bombora: API error for %s: %s", domain, exc)
            errors += 1
            db.record_crawl_attempt(
                conn, "bombora_api", account_id, "surge", "error", str(exc), commit=False
            )
            continue

        db.mark_crawled(conn, "bombora_api", account_id, "surge", commit=False)

        # Filter to relevant topics only.
        relevant = [t for t in topics if _is_relevant_topic(t.get("topic", ""))]
        if not relevant:
            continue

        # Process individual surge signals.
        for topic in relevant:
            topic_name = topic.get("topic", "unknown")
            surge_score = int(topic.get("surge_score", 0))
            seen += 1

            if surge_score >= threshold_high:
                signal_code = "bombora_surge_high"
                confidence = 0.85
            elif surge_score >= threshold_moderate:
                signal_code = "bombora_surge_moderate"
                confidence = 0.70
            else:
                continue

            evidence = f"Bombora surge: {topic_name} (score={surge_score})"
            obs = _build_observation(
                account_id=account_id,
                signal_code=signal_code,
                confidence=confidence,
                source_reliability=reliability,
                evidence_text=evidence,
                payload={"domain": domain, "topic": topic_name, "surge_score": surge_score},
            )
            if db.insert_signal_observation(conn, obs, commit=False):
                inserted += 1

        # Check for topic clusters.
        clusters = _find_topic_clusters(relevant, threshold_moderate)
        if len(clusters) >= 1:
            cluster_topics = [
                t.get("topic", "") for t in relevant if int(t.get("surge_score", 0)) >= threshold_moderate
            ]
            if len(cluster_topics) >= cluster_min:
                evidence = f"Bombora topic cluster: {', '.join(clusters)} ({len(cluster_topics)} surging topics)"
                obs = _build_observation(
                    account_id=account_id,
                    signal_code="bombora_topic_cluster",
                    confidence=0.75,
                    source_reliability=reliability,
                    evidence_text=evidence,
                    payload={
                        "domain": domain,
                        "clusters": clusters,
                        "surging_topic_count": len(cluster_topics),
                    },
                )
                if db.insert_signal_observation(conn, obs, commit=False):
                    inserted += 1
                seen += 1

    conn.commit()
    logger.info("bombora: inserted=%d seen=%d errors=%d", inserted, seen, errors)
    return {"inserted": inserted, "seen": seen}
