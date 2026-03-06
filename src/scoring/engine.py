from __future__ import annotations

import json
import logging
import math
from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from typing import Callable

from src.models import AccountScore, ComponentScore
from src.scoring.explain import rank_top_reasons, reasons_to_json
from src.scoring.rules import (
    TIER_ORDER,
    VALID_DIMENSIONS,
    DimensionWeight,
    SignalRule,
    Thresholds,
    TierUpgradeRule,
    VelocityCategory,
    legacy_tier_from_v2,
)
from src.utils import parse_datetime

logger = logging.getLogger(__name__)

PRODUCTS = ("zopdev", "zopday", "zopnight")
MAX_OBSERVATIONS_PER_SIGNAL = 3
MAX_OBSERVATIONS_PER_SOURCE_PER_SIGNAL = 1
DEFAULT_DIMENSION_WEIGHTS: dict[str, DimensionWeight] = {
    "trigger_intent": DimensionWeight(dimension="trigger_intent", weight=0.40, ceiling=70.0),
    "engagement_pql": DimensionWeight(dimension="engagement_pql", weight=0.30, ceiling=60.0),
    "hiring_growth": DimensionWeight(dimension="hiring_growth", weight=0.15, ceiling=40.0),
    "tech_fit": DimensionWeight(dimension="tech_fit", weight=0.10, ceiling=30.0),
    "firmographic": DimensionWeight(dimension="firmographic", weight=0.05, ceiling=20.0),
}


@dataclass
class EngineOutput:
    component_scores: list[ComponentScore]
    account_scores: list[AccountScore]


def recency_decay(days_since_observed: int, half_life_days: float) -> float:
    if half_life_days <= 0:
        return 1.0
    return math.pow(0.5, max(0, days_since_observed) / half_life_days)


def _promote_one_tier(tier_name: str) -> str:
    current = str(tier_name or "").strip().lower()
    if current not in TIER_ORDER:
        return "tier_4"
    idx = TIER_ORDER.index(current)
    return TIER_ORDER[max(0, idx - 1)]


def _apply_upgrade_rules(
    base_tier: str,
    dimension_scores: dict[str, float],
    rules: tuple[TierUpgradeRule, ...],
) -> str:
    if base_tier not in TIER_ORDER or not rules:
        return base_tier if base_tier in TIER_ORDER else "tier_4"

    upgraded = base_tier
    for _ in range(len(TIER_ORDER)):
        changed = False
        for rule in rules:
            if rule.current_tier != "*" and upgraded != rule.current_tier:
                continue
            if float(dimension_scores.get(rule.condition_dimension, 0.0)) < float(rule.condition_threshold):
                continue
            target = _promote_one_tier(upgraded) if rule.promote_to_tier == "+1" else rule.promote_to_tier
            if target not in TIER_ORDER:
                continue
            if TIER_ORDER.index(target) < TIER_ORDER.index(upgraded):
                upgraded = target
                changed = True
        if not changed:
            break
    return upgraded


def classify_tier(score: float, thresholds: Thresholds, dimension_scores: dict[str, float] | None = None) -> str:
    if score >= float(thresholds.tier_1):
        base_tier = "tier_1"
    elif score >= float(thresholds.tier_2):
        base_tier = "tier_2"
    elif score >= float(thresholds.tier_3):
        base_tier = "tier_3"
    else:
        base_tier = "tier_4"
    return _apply_upgrade_rules(base_tier, dimension_scores or {}, thresholds.upgrade_rules)


def classify_velocity(velocity_7d: float) -> VelocityCategory:
    """Classify velocity based on 7-day score change.

    Surging:       velocity_7d > +20
    Accelerating:  velocity_7d > +10
    Decelerating:  velocity_7d < -5
    Stable:        -5 <= velocity_7d <= +10
    """
    if velocity_7d > 20:
        return "surging"
    if velocity_7d > 10:
        return "accelerating"
    if velocity_7d < -5:
        return "decelerating"
    return "stable"


def classify_confidence_band(distinct_source_count: int) -> str:
    """Classify confidence band based on source diversity.

    3+ distinct sources → high, 2 → medium, 1 → low.
    """
    if distinct_source_count >= 3:
        return "high"
    if distinct_source_count == 2:
        return "medium"
    return "low"


_BAND_RANK = {"high": 2, "medium": 1, "low": 0}


def overall_confidence_band(dimension_bands: dict[str, str]) -> str:
    """Overall confidence = lowest band across all non-zero dimensions."""
    if not dimension_bands:
        return "low"
    return min(dimension_bands.values(), key=lambda b: _BAND_RANK.get(b, 0))


def _resolve_products(observation_product: str, rule_scope: str) -> tuple[str, ...]:
    if observation_product == "shared":
        observed_products = set(PRODUCTS)
    elif observation_product in PRODUCTS:
        observed_products = {observation_product}
    else:
        return tuple()

    if rule_scope in {"shared", "all"}:
        scoped_products = set(PRODUCTS)
    elif rule_scope in PRODUCTS:
        scoped_products = {rule_scope}
    else:
        scoped_products = set(PRODUCTS)

    resolved = tuple(product for product in PRODUCTS if product in observed_products & scoped_products)
    return resolved


def _resolve_dimension_weights(
    configured_weights: dict[str, DimensionWeight] | None,
) -> dict[str, DimensionWeight]:
    merged = dict(DEFAULT_DIMENSION_WEIGHTS)
    if configured_weights:
        for dimension, details in configured_weights.items():
            if dimension not in VALID_DIMENSIONS:
                continue
            if details.weight <= 0 or details.ceiling <= 0:
                continue
            merged[dimension] = details

    total = sum(item.weight for item in merged.values())
    if total <= 0:
        return dict(DEFAULT_DIMENSION_WEIGHTS)
    if abs(total - 1.0) < 0.000001:
        return merged
    return {
        dimension: DimensionWeight(
            dimension=dimension,
            weight=details.weight / total,
            ceiling=details.ceiling,
        )
        for dimension, details in merged.items()
    }


def run_scoring(
    run_id: str,
    run_date: date,
    observations: list[dict],
    rules: dict[str, SignalRule],
    thresholds: Thresholds,
    source_reliability_defaults: dict[str, float],
    dimension_weights: dict[str, DimensionWeight] | None = None,
    delta_lookup: Callable[[str, str], float] | None = None,
    velocity_lookup: Callable[[str, str, float], tuple[float, float, float]] | None = None,
) -> EngineOutput:
    resolved_dimension_weights = _resolve_dimension_weights(dimension_weights)
    component_totals: dict[tuple[str, str, str], float] = {}
    reason_candidates: dict[tuple[str, str, str], dict] = {}
    component_contributions: dict[tuple[str, str, str], list[dict[str, float | str]]] = defaultdict(list)
    # Track distinct sources per (account_id, product, dimension/category)
    dimension_sources: dict[tuple[str, str, str], set[str]] = defaultdict(set)

    skipped_count = 0
    for observation in observations:
        try:
            signal_code = str(observation.get("signal_code", ""))
            if not signal_code:
                skipped_count += 1
                continue

            rule = rules.get(signal_code)
            if not rule or not rule.enabled:
                continue

            confidence_raw = observation.get("confidence")
            try:
                confidence = float(confidence_raw or 0.0)
            except (TypeError, ValueError):
                logger.warning(
                    "observation_skipped signal=%s error=invalid_confidence value=%r", signal_code, confidence_raw
                )
                skipped_count += 1
                continue
            if confidence < rule.min_confidence:
                continue

            observed_at = parse_datetime(str(observation.get("observed_at", "")))
            days_since = max(0, (run_date - observed_at.date()).days)

            source = str(observation.get("source", ""))
            registry_reliability = source_reliability_defaults.get(source)
            if registry_reliability is not None and registry_reliability <= 0:
                continue

            source_reliability = observation.get("source_reliability")
            if source_reliability is None:
                source_reliability = registry_reliability if registry_reliability is not None else 0.6
            source_reliability = float(source_reliability)
            if registry_reliability is not None:
                source_reliability = min(source_reliability, float(registry_reliability))

            component = (
                rule.base_weight
                * confidence
                * source_reliability
                * recency_decay(days_since_observed=days_since, half_life_days=rule.half_life_days)
            )

            if component <= 0:
                continue

            account_id = str(observation["account_id"])
            dimension = rule.dimension
            resolved_products = _resolve_products(str(observation.get("product", "")), rule.product_scope)
            for product in resolved_products:
                dimension_sources[(account_id, product, dimension)].add(source)
                key = (account_id, product, signal_code)
                component_contributions[key].append(
                    {
                        "component_score": component,
                        "source": source,
                        "evidence_url": str(observation.get("evidence_url", "") or ""),
                        "evidence_text": str(observation.get("evidence_text", "") or "")[:280],
                        "evidence_sentence": str(observation.get("evidence_sentence", "") or "")[:500],
                        "evidence_sentence_en": str(observation.get("evidence_sentence_en", "") or "")[:500],
                        "matched_phrase": str(observation.get("matched_phrase", "") or "")[:200],
                        "language": str(observation.get("language", "") or "")[:20],
                        "speaker_name": str(observation.get("speaker_name", "") or "")[:120],
                        "speaker_role": str(observation.get("speaker_role", "") or "")[:80],
                        "evidence_quality": float(observation.get("evidence_quality", 0.0) or 0.0),
                        "relevance_score": float(observation.get("relevance_score", 0.0) or 0.0),
                        "document_id": str(observation.get("document_id", "") or "")[:64],
                        "mention_id": str(observation.get("mention_id", "") or "")[:64],
                    }
                )
        except (KeyError, TypeError, ValueError) as e:
            logger.warning("observation_skipped signal=%s error=%s", observation.get("signal_code"), e)
            skipped_count += 1
            continue

    if skipped_count:
        logger.warning("scoring_run skipped_observations=%d", skipped_count)

    for key, contributions in component_contributions.items():
        ranked = sorted(contributions, key=lambda row: float(row["component_score"]), reverse=True)
        selected: list[dict[str, float | str]] = []
        source_counts: dict[str, int] = defaultdict(int)

        for row in ranked:
            source = str(row["source"])
            if source_counts[source] >= MAX_OBSERVATIONS_PER_SOURCE_PER_SIGNAL:
                continue
            selected.append(row)
            source_counts[source] += 1
            if len(selected) >= MAX_OBSERVATIONS_PER_SIGNAL:
                break

        if not selected:
            continue

        total_component = round(sum(float(item["component_score"]) for item in selected), 4)
        component_totals[key] = total_component

        account_id, product, signal_code = key
        best = selected[0]
        reason_candidates[key] = {
            "signal_code": signal_code,
            "component_score": total_component,
            "source": str(best["source"]),
            "evidence_url": str(best["evidence_url"]),
            "evidence_text": str(best["evidence_text"]),
            "evidence_sentence": str(best.get("evidence_sentence", "")),
            "evidence_sentence_en": str(best.get("evidence_sentence_en", "")),
            "matched_phrase": str(best.get("matched_phrase", "")),
            "language": str(best.get("language", "")),
            "speaker_name": str(best.get("speaker_name", "")),
            "speaker_role": str(best.get("speaker_role", "")),
            "evidence_quality": float(best.get("evidence_quality", 0.0) or 0.0),
            "relevance_score": float(best.get("relevance_score", 0.0) or 0.0),
            "document_id": str(best.get("document_id", "")),
            "mention_id": str(best.get("mention_id", "")),
        }

    grouped_components: dict[tuple[str, str], list[tuple[str, float]]] = defaultdict(list)
    for (account_id, product, signal_code), component_score in component_totals.items():
        grouped_components[(account_id, product)].append((signal_code, component_score))

    component_models: list[ComponentScore] = []
    account_models: list[AccountScore] = []

    for (account_id, product, signal_code), component_score in component_totals.items():
        component_models.append(
            ComponentScore(
                run_id=run_id,
                account_id=account_id,
                product=product,
                signal_code=signal_code,
                component_score=round(component_score, 4),
            )
        )

    for (account_id, product), items in grouped_components.items():
        raw_dimension_scores = {dimension: 0.0 for dimension in resolved_dimension_weights}
        for signal_code, component_score in items:
            dimension = "trigger_intent"
            rule = rules.get(signal_code)
            if rule and rule.dimension in VALID_DIMENSIONS:
                dimension = rule.dimension
            raw_dimension_scores[dimension] = raw_dimension_scores.get(dimension, 0.0) + component_score

        dimension_scores = {
            dimension: min(100.0, round((raw_dimension_scores.get(dimension, 0.0) / details.ceiling) * 100.0, 2))
            for dimension, details in resolved_dimension_weights.items()
        }
        total_score = min(
            100.0,
            round(
                sum(
                    dimension_scores[dimension] * details.weight
                    for dimension, details in resolved_dimension_weights.items()
                ),
                2,
            ),
        )
        tier_v2 = classify_tier(total_score, thresholds, dimension_scores=dimension_scores)
        tier = legacy_tier_from_v2(tier_v2)

        reasons: list[dict] = []
        for signal_code, _ in items:
            reason = reason_candidates.get((account_id, product, signal_code))
            if reason:
                reasons.append(reason)
        top_reasons = rank_top_reasons(reasons, limit=3)

        delta = 0.0
        if delta_lookup:
            try:
                delta = round(delta_lookup(account_id, product), 2)
            except Exception as e:
                logger.warning("delta_lookup_failed account=%s product=%s error=%s", account_id, product, e)

        if velocity_lookup:
            v7, v14, v30 = velocity_lookup(account_id, product, total_score)
            vel_7d = round(v7, 2)
            _vel_14d = round(v14, 2)  # noqa: F841 — stored when velocity columns land
            _vel_30d = round(v30, 2)  # noqa: F841
        else:
            vel_7d = delta
            _vel_14d = 0.0  # noqa: F841
            _vel_30d = 0.0  # noqa: F841

        _vel_cat = classify_velocity(vel_7d)  # noqa: F841

        # Compute per-dimension confidence bands and source lists
        dim_bands: dict[str, str] = {}
        dim_sources_detail: dict[str, list[str]] = {}
        for (a_id, prod, dim), sources in dimension_sources.items():
            if a_id == account_id and prod == product:
                dim_bands[dim] = classify_confidence_band(len(sources))
                dim_sources_detail[dim] = sorted(sources)
        conf_band = overall_confidence_band(dim_bands)

        dim_conf_data: dict[str, dict] = {}
        for dim, band in dim_bands.items():
            src_list = dim_sources_detail.get(dim, [])
            dim_conf_data[dim] = {
                "band": band,
                "source_count": len(src_list),
                "sources": src_list,
            }

        account_models.append(
            AccountScore(
                run_id=run_id,
                account_id=account_id,
                product=product,
                score=total_score,
                tier=tier,
                tier_v2=tier_v2,
                top_reasons_json=reasons_to_json(top_reasons),
                delta_7d=delta,
                dimension_scores_json=json.dumps(dimension_scores, sort_keys=True),
                confidence_band=conf_band,
                dimension_confidence_json=json.dumps(dim_conf_data, sort_keys=True),
            )
        )

    return EngineOutput(component_scores=component_models, account_scores=account_models)
