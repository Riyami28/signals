from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date
import math
from typing import Callable

from src.models import AccountScore, ComponentScore
from src.scoring.explain import rank_top_reasons, reasons_to_json
from src.scoring.rules import SignalRule, Thresholds
from src.utils import parse_datetime

PRODUCTS = ("zopdev", "zopday", "zopnight")
MAX_OBSERVATIONS_PER_SIGNAL = 3
MAX_OBSERVATIONS_PER_SOURCE_PER_SIGNAL = 1


@dataclass
class EngineOutput:
    component_scores: list[ComponentScore]
    account_scores: list[AccountScore]


def recency_decay(days_since_observed: int, half_life_days: float) -> float:
    if half_life_days <= 0:
        return 1.0
    return math.pow(0.5, max(0, days_since_observed) / half_life_days)


def classify_tier(score: float, thresholds: Thresholds) -> str:
    if score >= thresholds.high:
        return "high"
    if score >= thresholds.medium:
        return "medium"
    return "low"


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


def run_scoring(
    run_id: str,
    run_date: date,
    observations: list[dict],
    rules: dict[str, SignalRule],
    thresholds: Thresholds,
    source_reliability_defaults: dict[str, float],
    delta_lookup: Callable[[str, str], float] | None = None,
) -> EngineOutput:
    component_totals: dict[tuple[str, str, str], float] = {}
    reason_candidates: dict[tuple[str, str, str], dict] = {}
    component_contributions: dict[tuple[str, str, str], list[dict[str, float | str]]] = defaultdict(list)

    for observation in observations:
        signal_code = str(observation["signal_code"])
        rule = rules.get(signal_code)
        if not rule or not rule.enabled:
            continue

        confidence = float(observation["confidence"] or 0.0)
        if confidence < rule.min_confidence:
            continue

        observed_at = parse_datetime(str(observation["observed_at"]))
        days_since = max(0, (run_date - observed_at.date()).days)

        source = str(observation["source"])
        registry_reliability = source_reliability_defaults.get(source)
        if registry_reliability is not None and registry_reliability <= 0:
            continue

        source_reliability = observation["source_reliability"]
        if source_reliability is None:
            source_reliability = registry_reliability if registry_reliability is not None else 0.6
        source_reliability = float(source_reliability)

        component = (
            rule.base_weight
            * confidence
            * source_reliability
            * recency_decay(days_since_observed=days_since, half_life_days=rule.half_life_days)
        )

        if component <= 0:
            continue

        account_id = str(observation["account_id"])
        resolved_products = _resolve_products(str(observation["product"]), rule.product_scope)
        for product in resolved_products:
            key = (account_id, product, signal_code)
            component_contributions[key].append(
                {
                    "component_score": component,
                    "source": source,
                    "evidence_url": str(observation["evidence_url"] or ""),
                    "evidence_text": str(observation["evidence_text"] or "")[:280],
                }
            )

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
        total_score = min(100.0, round(sum(value for _, value in items), 2))
        tier = classify_tier(total_score, thresholds)

        reasons: list[dict] = []
        for signal_code, _ in items:
            reason = reason_candidates.get((account_id, product, signal_code))
            if reason:
                reasons.append(reason)
        top_reasons = rank_top_reasons(reasons, limit=3)

        delta = round(delta_lookup(account_id, product), 2) if delta_lookup else 0.0

        account_models.append(
            AccountScore(
                run_id=run_id,
                account_id=account_id,
                product=product,
                score=total_score,
                tier=tier,
                top_reasons_json=reasons_to_json(top_reasons),
                delta_7d=delta,
            )
        )

    return EngineOutput(component_scores=component_models, account_scores=account_models)
