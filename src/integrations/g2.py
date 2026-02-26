"""
G2 review intelligence and competitor signal integration.

Fetches intent data, review activity, and satisfaction signals for scored
accounts from the G2 API.  Surfaces competitor mentions, evaluation
activity, and dissatisfaction signals that indicate buying windows.

Signal mapping:
    Active research on G2       -> g2_active_research          (weight=15, confidence=0.65)
    Competitor review posted     -> competitor_review_activity   (weight=12, confidence=0.60)
    Low satisfaction detected    -> competitor_dissatisfaction   (weight=18, confidence=0.70)

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

# Competitor products to track — reviews mentioning these indicate
# dissatisfaction or evaluation activity relevant to our products.
COMPETITOR_PRODUCTS: dict[str, list[str]] = {
    "devops": [
        "jenkins",
        "circleci",
        "travis ci",
        "bamboo",
        "teamcity",
        "azure devops",
        "gitlab ci",
    ],
    "platform_eng": [
        "backstage",
        "port",
        "cortex",
        "humanitec",
        "kratix",
        "qovery",
    ],
    "finops": [
        "cloudhealth",
        "cloudability",
        "spot.io",
        "kubecost",
        "vantage",
        "anodot",
        "cast ai",
    ],
    "cloud_infra": [
        "terraform cloud",
        "pulumi",
        "spacelift",
        "env0",
        "scalr",
    ],
}

# Flat set for quick lookup.
_ALL_COMPETITOR_NAMES: set[str] = set()
for _products in COMPETITOR_PRODUCTS.values():
    _ALL_COMPETITOR_NAMES.update(_products)

# Satisfaction score thresholds.
_DISSATISFACTION_RATING_MAX = 3.0  # star rating <= this counts as dissatisfied
_MIN_REVIEW_CONFIDENCE = 0.60


# ---------------------------------------------------------------------------
# G2 API client helpers
# ---------------------------------------------------------------------------


def _fetch_intent_data(
    domain: str,
    api_key: str,
    base_url: str,
) -> list[dict[str, Any]]:
    """Fetch buyer intent signals for a domain from the G2 Intent API.

    Returns a list of intent records:
        [{"category": "...", "signal_strength": "high", "activity_count": 5, ...}, ...]
    """
    resp = requests.get(
        f"{base_url}/intent/signals",
        params={"domain": domain},
        headers={
            "Authorization": f"Token token={api_key}",
            "Accept": "application/vnd.api+json",
        },
        timeout=_TIMEOUT,
    )
    resp.raise_for_status()
    data = resp.json()
    return data.get("data", [])


def _fetch_competitor_reviews(
    product_ids: list[str],
    api_key: str,
    base_url: str,
    since_days: int = 30,
) -> list[dict[str, Any]]:
    """Fetch recent reviews for tracked competitor product IDs.

    Returns review records with rating, text snippet, and reviewer company.
    """
    all_reviews: list[dict[str, Any]] = []
    for product_id in product_ids:
        try:
            resp = requests.get(
                f"{base_url}/products/{product_id}/reviews",
                params={"since_days": since_days, "per_page": 50},
                headers={
                    "Authorization": f"Token token={api_key}",
                    "Accept": "application/vnd.api+json",
                },
                timeout=_TIMEOUT,
            )
            resp.raise_for_status()
            data = resp.json()
            all_reviews.extend(data.get("data", []))
        except requests.RequestException as exc:
            logger.warning("g2: error fetching reviews for product %s: %s", product_id, exc)
    return all_reviews


# ---------------------------------------------------------------------------
# Signal classification helpers
# ---------------------------------------------------------------------------


def _is_competitor_product(product_name: str) -> bool:
    """Check if a product name matches a tracked competitor."""
    name_lower = product_name.lower().strip()
    for competitor in _ALL_COMPETITOR_NAMES:
        if competitor in name_lower or name_lower in competitor:
            return True
    return False


def _map_competitor_to_category(product_name: str) -> str:
    """Map a competitor product name to its category."""
    name_lower = product_name.lower().strip()
    for category, products in COMPETITOR_PRODUCTS.items():
        for prod in products:
            if prod in name_lower or name_lower in prod:
                return category
    return "unknown"


def _classify_intent_strength(signal_strength: str) -> tuple[str, float] | None:
    """Map G2 intent signal strength to signal code and confidence.

    Returns (signal_code, confidence) or None if not actionable.
    """
    strength = signal_strength.lower().strip()
    if strength == "high":
        return "g2_active_research", 0.80
    if strength == "medium":
        return "g2_active_research", 0.65
    # Low strength is not actionable.
    return None


def _is_dissatisfied_review(review: dict[str, Any]) -> bool:
    """Check if a review indicates dissatisfaction with the product."""
    rating = float(review.get("star_rating", 5.0))
    return rating <= _DISSATISFACTION_RATING_MAX


def _build_observation(
    account_id: str,
    signal_code: str,
    confidence: float,
    source_reliability: float,
    evidence_text: str,
    evidence_url: str,
    payload: dict[str, Any],
) -> SignalObservation:
    """Build a SignalObservation for a G2 signal."""
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
        source="g2_api",
        observed_at=utc_now_iso(),
        evidence_url=evidence_url,
        evidence_text=evidence_text[:500],
        confidence=max(0.0, min(1.0, confidence)),
        source_reliability=max(0.0, min(1.0, source_reliability)),
        raw_payload_hash=raw_hash,
    )


# ---------------------------------------------------------------------------
# Internal processing steps
# ---------------------------------------------------------------------------


def _process_intent_signals(
    conn,
    account_id: str,
    domain: str,
    intent_data: list[dict[str, Any]],
    reliability: float,
) -> tuple[int, int]:
    """Process intent signals for a single account.

    Returns (inserted, seen).
    """
    inserted = 0
    seen = 0

    for record in intent_data:
        signal_strength = record.get("signal_strength", "")
        result = _classify_intent_strength(signal_strength)
        if result is None:
            continue

        signal_code, confidence = result
        seen += 1

        category = record.get("category", "unknown")
        activity_count = record.get("activity_count", 0)
        evidence = f"G2 intent: {category} research (strength={signal_strength}, activities={activity_count})"

        obs = _build_observation(
            account_id=account_id,
            signal_code=signal_code,
            confidence=confidence,
            source_reliability=reliability,
            evidence_text=evidence,
            evidence_url=record.get("url", ""),
            payload={
                "domain": domain,
                "category": category,
                "signal_strength": signal_strength,
                "activity_count": activity_count,
            },
        )
        if db.insert_signal_observation(conn, obs, commit=False):
            inserted += 1

    return inserted, seen


def _process_competitor_reviews(
    conn,
    reviews: list[dict[str, Any]],
    account_lookup: dict[str, str],
    reliability: float,
) -> tuple[int, int]:
    """Process competitor reviews and match to tracked accounts.

    ``account_lookup`` maps domain -> account_id for reverse matching
    reviewer companies back to our accounts.

    Returns (inserted, seen).
    """
    inserted = 0
    seen = 0

    for review in reviews:
        reviewer_domain = review.get("reviewer_company_domain", "")
        if not reviewer_domain:
            continue

        reviewer_domain = reviewer_domain.lower().strip()
        account_id = account_lookup.get(reviewer_domain)
        if account_id is None:
            continue

        product_name = review.get("product_name", "")
        if not _is_competitor_product(product_name):
            continue

        seen += 1
        category = _map_competitor_to_category(product_name)
        star_rating = float(review.get("star_rating", 5.0))
        review_title = review.get("title", "")[:100]
        review_url = review.get("url", "")

        # Dissatisfied review -> competitor_dissatisfaction signal.
        if _is_dissatisfied_review(review):
            evidence = (
                f"G2 review: {product_name} rated {star_rating}/5 "
                f'by {reviewer_domain} — "{review_title}" (category={category})'
            )
            obs = _build_observation(
                account_id=account_id,
                signal_code="competitor_dissatisfaction",
                confidence=0.70,
                source_reliability=reliability,
                evidence_text=evidence,
                evidence_url=review_url,
                payload={
                    "reviewer_domain": reviewer_domain,
                    "product_name": product_name,
                    "star_rating": star_rating,
                    "category": category,
                },
            )
            if db.insert_signal_observation(conn, obs, commit=False):
                inserted += 1
        else:
            # Non-dissatisfied review still counts as competitor activity.
            evidence = (
                f"G2 review: {product_name} reviewed by {reviewer_domain} (rating={star_rating}/5, category={category})"
            )
            obs = _build_observation(
                account_id=account_id,
                signal_code="competitor_review_activity",
                confidence=0.60,
                source_reliability=reliability,
                evidence_text=evidence,
                evidence_url=review_url,
                payload={
                    "reviewer_domain": reviewer_domain,
                    "product_name": product_name,
                    "star_rating": star_rating,
                    "category": category,
                },
            )
            if db.insert_signal_observation(conn, obs, commit=False):
                inserted += 1

    return inserted, seen


# ---------------------------------------------------------------------------
# Public collector entry point
# ---------------------------------------------------------------------------


def collect(
    conn,
    settings: Settings,
    lexicon_by_source: dict[str, list[dict[str, str]]],
    source_reliability: dict[str, float],
) -> dict[str, int]:
    """Collect G2 intent and review intelligence signals for all scored accounts.

    Returns ``{"inserted": N, "seen": M}``.
    """
    api_key = settings.g2_api_key
    if not api_key:
        logger.info("g2: skipped — no SIGNALS_G2_API_KEY configured")
        return {"inserted": 0, "seen": 0}

    base_url = settings.g2_api_base_url
    product_ids = list(settings.g2_competitor_product_ids)
    review_lookback_days = settings.g2_review_lookback_days
    reliability = source_reliability.get("g2_api", 0.78)

    # Fetch all accounts.
    accounts = conn.execute("SELECT account_id, domain FROM accounts").fetchall()
    logger.info("g2: checking intent data for %d accounts", len(accounts))

    # Build domain -> account_id lookup for review matching.
    account_lookup: dict[str, str] = {}
    for row in accounts:
        domain = str(row["domain"]).lower().strip()
        if domain:
            account_lookup[domain] = str(row["account_id"])

    total_inserted = 0
    total_seen = 0
    errors = 0

    # --- Phase 1: Intent signals per account ---
    for row in accounts:
        account_id = str(row["account_id"])
        domain = str(row["domain"])
        if not domain:
            continue

        # Skip if already crawled today.
        if db.was_crawled_today(conn, "g2_api", account_id, "intent"):
            continue

        try:
            intent_data = _fetch_intent_data(domain, api_key, base_url)
        except requests.RequestException as exc:
            logger.warning("g2: intent API error for %s: %s", domain, exc)
            errors += 1
            db.record_crawl_attempt(conn, "g2_api", account_id, "intent", "error", str(exc), commit=False)
            continue

        db.mark_crawled(conn, "g2_api", account_id, "intent", commit=False)

        ins, seen = _process_intent_signals(conn, account_id, domain, intent_data, reliability)
        total_inserted += ins
        total_seen += seen

    # --- Phase 2: Competitor review signals (batch) ---
    if product_ids:
        if not db.was_crawled_today(conn, "g2_api", "__global__", "reviews"):
            try:
                reviews = _fetch_competitor_reviews(product_ids, api_key, base_url, since_days=review_lookback_days)
                db.mark_crawled(conn, "g2_api", "__global__", "reviews", commit=False)

                ins, seen = _process_competitor_reviews(conn, reviews, account_lookup, reliability)
                total_inserted += ins
                total_seen += seen
            except requests.RequestException as exc:
                logger.warning("g2: reviews API error: %s", exc)
                errors += 1
                db.record_crawl_attempt(conn, "g2_api", "__global__", "reviews", "error", str(exc), commit=False)
    else:
        logger.info("g2: no competitor product IDs configured — skipping review phase")

    conn.commit()
    logger.info("g2: inserted=%d seen=%d errors=%d", total_inserted, total_seen, errors)
    return {"inserted": total_inserted, "seen": total_seen}
