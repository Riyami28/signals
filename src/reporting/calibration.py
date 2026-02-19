from __future__ import annotations

from dataclasses import dataclass
import math
from pathlib import Path
from typing import Any

from src.scoring.rules import Thresholds
from src.utils import load_csv_rows, normalize_domain, write_csv_rows

TIER_ORDER = {"low": 0, "medium": 1, "high": 2}


@dataclass(frozen=True)
class ThresholdSuggestion:
    high: float
    medium: float
    low: float
    icp_accounts: int
    non_icp_accounts: int
    icp_high_coverage: float
    icp_medium_coverage: float
    non_icp_high_hit_rate: float
    non_icp_medium_hit_rate: float


@dataclass(frozen=True)
class ScenarioRule:
    scenario_name: str
    max_score: float
    expected_min_tier: str
    expected_max_tier: str
    weight: float = 1.0


@dataclass(frozen=True)
class ProfileSuggestion:
    high: float
    medium: float
    low: float
    icp_accounts: int
    non_icp_accounts: int
    icp_high_coverage: float
    icp_medium_coverage: float
    non_icp_high_hit_rate: float
    non_icp_medium_hit_rate: float
    scenario_count: int
    scenario_pass_rate: float
    constraints_satisfied: bool


@dataclass(frozen=True)
class ScoreSegments:
    icp_scores: list[float]
    non_icp_scores: list[float]


DEFAULT_SCENARIOS = [
    ScenarioRule("strong_buying_signal", 14.0, "high", "high", 1.0),
    ScenarioRule("credible_signal_bundle", 9.0, "medium", "high", 1.0),
    ScenarioRule("borderline_interest", 5.0, "low", "medium", 1.0),
    ScenarioRule("weak_signal_noise", 2.0, "low", "low", 1.0),
    ScenarioRule("ambient_noise", 0.8, "low", "low", 1.0),
]


def _threshold_for_target_coverage(scores: list[float], target_coverage: float) -> float:
    if not scores:
        return 0.0
    clamped = max(0.0, min(1.0, target_coverage))
    if clamped <= 0:
        return round(max(scores) + 0.01, 2)
    ranked = sorted(scores, reverse=True)
    k = min(len(ranked), max(1, math.ceil(clamped * len(ranked))))
    return round(float(ranked[k - 1]), 2)


def _rate(scores: list[float], threshold: float) -> float:
    if not scores:
        return 0.0
    return round(sum(1 for score in scores if score >= threshold) / len(scores), 4)


def _classify_tier(score: float, high: float, medium: float) -> str:
    if score >= high:
        return "high"
    if score >= medium:
        return "medium"
    return "low"


def _load_score_segments(conn: Any, run_id: str, reference_csv_path: Path) -> ScoreSegments:
    reference_rows = load_csv_rows(reference_csv_path)
    icp_domains = {
        normalize_domain(str(row.get("domain", "")))
        for row in reference_rows
        if normalize_domain(str(row.get("domain", "")))
    }

    score_rows = conn.execute(
        """
        SELECT a.domain, MAX(s.score) AS max_score
        FROM account_scores s
        JOIN accounts a ON a.account_id = s.account_id
        WHERE s.run_id = ?
        GROUP BY a.domain
        """,
        (run_id,),
    ).fetchall()

    domain_max_scores: dict[str, float] = {
        normalize_domain(str(row["domain"])): float(row["max_score"]) for row in score_rows
    }

    icp_scores = [score for domain, score in domain_max_scores.items() if domain in icp_domains]
    non_icp_scores = [score for domain, score in domain_max_scores.items() if domain not in icp_domains]
    return ScoreSegments(icp_scores=icp_scores, non_icp_scores=non_icp_scores)


def load_scenarios(path: Path | None = None) -> list[ScenarioRule]:
    if path is None or not path.exists():
        return list(DEFAULT_SCENARIOS)

    rows = load_csv_rows(path)
    scenarios: list[ScenarioRule] = []
    for row in rows:
        name = (row.get("scenario_name", "") or "").strip()
        if not name:
            continue
        try:
            max_score = float(row.get("max_score", "0") or 0)
            weight = float(row.get("weight", "1") or 1)
        except ValueError:
            continue
        min_tier = (row.get("expected_min_tier", "low") or "low").strip().lower()
        max_tier = (row.get("expected_max_tier", "high") or "high").strip().lower()
        if min_tier not in TIER_ORDER or max_tier not in TIER_ORDER:
            continue
        if TIER_ORDER[min_tier] > TIER_ORDER[max_tier]:
            continue
        scenarios.append(
            ScenarioRule(
                scenario_name=name,
                max_score=max_score,
                expected_min_tier=min_tier,
                expected_max_tier=max_tier,
                weight=max(0.0, weight),
            )
        )
    return scenarios or list(DEFAULT_SCENARIOS)


def _scenario_pass_rate(scenarios: list[ScenarioRule], high: float, medium: float) -> float:
    if not scenarios:
        return 1.0
    total_weight = sum(max(0.0, scenario.weight) for scenario in scenarios)
    if total_weight <= 0:
        return 1.0
    passing_weight = 0.0
    for scenario in scenarios:
        predicted = _classify_tier(scenario.max_score, high=high, medium=medium)
        if TIER_ORDER[scenario.expected_min_tier] <= TIER_ORDER[predicted] <= TIER_ORDER[scenario.expected_max_tier]:
            passing_weight += max(0.0, scenario.weight)
    return round(passing_weight / total_weight, 4)


def suggest_thresholds_for_run(
    conn: Any,
    run_id: str,
    reference_csv_path: Path,
    medium_target_coverage: float,
    high_target_coverage: float,
    current_thresholds: Thresholds,
) -> ThresholdSuggestion:
    segments = _load_score_segments(conn, run_id, reference_csv_path)
    icp_scores = segments.icp_scores
    non_icp_scores = segments.non_icp_scores

    if not icp_scores:
        return ThresholdSuggestion(
            high=current_thresholds.high,
            medium=current_thresholds.medium,
            low=current_thresholds.low,
            icp_accounts=0,
            non_icp_accounts=len(non_icp_scores),
            icp_high_coverage=0.0,
            icp_medium_coverage=0.0,
            non_icp_high_hit_rate=_rate(non_icp_scores, current_thresholds.high),
            non_icp_medium_hit_rate=_rate(non_icp_scores, current_thresholds.medium),
        )

    high = _threshold_for_target_coverage(icp_scores, high_target_coverage)
    medium = _threshold_for_target_coverage(icp_scores, medium_target_coverage)
    if high < medium:
        high = medium

    low = float(current_thresholds.low)
    return ThresholdSuggestion(
        high=high,
        medium=medium,
        low=low,
        icp_accounts=len(icp_scores),
        non_icp_accounts=len(non_icp_scores),
        icp_high_coverage=_rate(icp_scores, high),
        icp_medium_coverage=_rate(icp_scores, medium),
        non_icp_high_hit_rate=_rate(non_icp_scores, high),
        non_icp_medium_hit_rate=_rate(non_icp_scores, medium),
    )


def suggest_profile_for_run(
    conn: Any,
    run_id: str,
    reference_csv_path: Path,
    scenarios: list[ScenarioRule],
    min_icp_medium_coverage: float = 0.6,
    max_non_icp_medium_hit_rate: float = 0.5,
    max_non_icp_high_hit_rate: float = 0.25,
    min_scenario_pass_rate: float = 0.9,
    current_thresholds: Thresholds | None = None,
) -> ProfileSuggestion:
    segments = _load_score_segments(conn, run_id, reference_csv_path)
    icp_scores = segments.icp_scores
    non_icp_scores = segments.non_icp_scores

    baseline = current_thresholds or Thresholds(high=70.0, medium=45.0, low=0.0)
    candidate_scores = sorted(
        {
            round(float(value), 2)
            for value in (icp_scores + non_icp_scores + [scenario.max_score for scenario in scenarios])
        }
    )

    if not candidate_scores:
        return ProfileSuggestion(
            high=baseline.high,
            medium=baseline.medium,
            low=baseline.low,
            icp_accounts=0,
            non_icp_accounts=0,
            icp_high_coverage=0.0,
            icp_medium_coverage=0.0,
            non_icp_high_hit_rate=0.0,
            non_icp_medium_hit_rate=0.0,
            scenario_count=len(scenarios),
            scenario_pass_rate=1.0,
            constraints_satisfied=False,
        )

    best: tuple[float, int, float, float, ProfileSuggestion] | None = None
    for medium in candidate_scores:
        for high in [score for score in candidate_scores if score >= medium]:
            icp_high = _rate(icp_scores, high)
            icp_medium = _rate(icp_scores, medium)
            non_icp_high = _rate(non_icp_scores, high)
            non_icp_medium = _rate(non_icp_scores, medium)
            scenario_pass = _scenario_pass_rate(scenarios, high=high, medium=medium)

            constraints_satisfied = (
                icp_medium >= min_icp_medium_coverage
                and non_icp_medium <= max_non_icp_medium_hit_rate
                and non_icp_high <= max_non_icp_high_hit_rate
                and scenario_pass >= min_scenario_pass_rate
            )
            objective = (
                (4.0 * icp_medium)
                + (2.0 * icp_high)
                + (3.0 * scenario_pass)
                - (3.0 * non_icp_medium)
                - (2.0 * non_icp_high)
            )
            suggestion = ProfileSuggestion(
                high=round(high, 2),
                medium=round(medium, 2),
                low=float(baseline.low),
                icp_accounts=len(icp_scores),
                non_icp_accounts=len(non_icp_scores),
                icp_high_coverage=icp_high,
                icp_medium_coverage=icp_medium,
                non_icp_high_hit_rate=non_icp_high,
                non_icp_medium_hit_rate=non_icp_medium,
                scenario_count=len(scenarios),
                scenario_pass_rate=scenario_pass,
                constraints_satisfied=constraints_satisfied,
            )

            ranking = (
                1 if constraints_satisfied else 0,
                objective,
                medium,
                high,
            )
            if best is None or ranking > best[:4]:
                best = (*ranking, suggestion)

    assert best is not None
    return best[4]


def write_thresholds(path: Path, high: float, medium: float, low: float = 0.0) -> None:
    write_csv_rows(
        path,
        rows=[
            {"key": "high", "value": round(float(high), 2)},
            {"key": "medium", "value": round(float(medium), 2)},
            {"key": "low", "value": round(float(low), 2)},
        ],
        fieldnames=["key", "value"],
    )
