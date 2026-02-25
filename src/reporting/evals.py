from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.reporting.calibration import ScenarioRule, load_scenarios
from src.scoring.rules import Thresholds
from src.utils import load_csv_rows, normalize_domain

TIER_ORDER = {"low": 0, "medium": 1, "high": 2}


@dataclass(frozen=True)
class OutputQualityBar:
    min_icp_medium_coverage: float = 0.6
    min_icp_high_coverage: float = 0.2
    max_non_icp_medium_hit_rate: float = 0.5
    max_non_icp_high_hit_rate: float = 0.25
    min_scenario_pass_rate: float = 0.9


@dataclass(frozen=True)
class OutputQualityEval:
    thresholds: Thresholds
    icp_accounts: int
    non_icp_accounts: int
    icp_high_coverage: float
    icp_medium_coverage: float
    non_icp_high_hit_rate: float
    non_icp_medium_hit_rate: float
    scenario_pass_rate: float
    passed: bool
    failed_checks: list[str]


def _threshold_rate(scores: list[float], threshold: float) -> float:
    if not scores:
        return 0.0
    return round(sum(1 for score in scores if score >= threshold) / len(scores), 4)


def _classify_tier(score: float, thresholds: Thresholds) -> str:
    if score >= thresholds.high:
        return "high"
    if score >= thresholds.medium:
        return "medium"
    return "low"


def _scenario_pass_rate(scenarios: list[ScenarioRule], thresholds: Thresholds) -> float:
    if not scenarios:
        return 1.0
    total_weight = sum(max(0.0, float(scenario.weight)) for scenario in scenarios)
    if total_weight <= 0:
        return 1.0

    passed_weight = 0.0
    for scenario in scenarios:
        predicted_tier = _classify_tier(float(scenario.max_score), thresholds)
        if TIER_ORDER[scenario.expected_min_tier] <= TIER_ORDER[predicted_tier] <= TIER_ORDER[scenario.expected_max_tier]:
            passed_weight += max(0.0, float(scenario.weight))
    return round(passed_weight / total_weight, 4)


def _load_score_segments(conn: Any, run_id: str, reference_csv_path: Path) -> tuple[list[float], list[float]]:
    reference_rows = load_csv_rows(reference_csv_path)
    icp_domains = {
        normalize_domain(str(row.get("domain", "") or ""))
        for row in reference_rows
        if normalize_domain(str(row.get("domain", "") or ""))
    }

    score_rows = conn.execute(
        """
        SELECT a.domain, MAX(s.score) AS max_score
        FROM account_scores s
        JOIN accounts a ON a.account_id = s.account_id
        WHERE s.run_id = %s
        GROUP BY a.domain
        """,
        (run_id,),
    ).fetchall()

    icp_scores: list[float] = []
    non_icp_scores: list[float] = []
    for row in score_rows:
        domain = normalize_domain(str(row["domain"] or ""))
        score = float(row["max_score"] or 0.0)
        if domain in icp_domains:
            icp_scores.append(score)
        else:
            non_icp_scores.append(score)
    return icp_scores, non_icp_scores


def evaluate_run_output_quality(
    conn: Any,
    run_id: str,
    reference_csv_path: Path,
    thresholds: Thresholds,
    quality_bar: OutputQualityBar,
    scenarios: list[ScenarioRule] | None = None,
) -> OutputQualityEval:
    selected_scenarios = scenarios if scenarios is not None else load_scenarios(None)
    icp_scores, non_icp_scores = _load_score_segments(conn, run_id, reference_csv_path)

    icp_high_coverage = _threshold_rate(icp_scores, thresholds.high)
    icp_medium_coverage = _threshold_rate(icp_scores, thresholds.medium)
    non_icp_high_hit_rate = _threshold_rate(non_icp_scores, thresholds.high)
    non_icp_medium_hit_rate = _threshold_rate(non_icp_scores, thresholds.medium)
    scenario_pass_rate = _scenario_pass_rate(selected_scenarios, thresholds)

    failed_checks: list[str] = []
    if icp_scores and icp_medium_coverage < quality_bar.min_icp_medium_coverage:
        failed_checks.append("icp_medium_coverage")
    if icp_scores and icp_high_coverage < quality_bar.min_icp_high_coverage:
        failed_checks.append("icp_high_coverage")
    if non_icp_medium_hit_rate > quality_bar.max_non_icp_medium_hit_rate:
        failed_checks.append("non_icp_medium_hit_rate")
    if non_icp_high_hit_rate > quality_bar.max_non_icp_high_hit_rate:
        failed_checks.append("non_icp_high_hit_rate")
    if scenario_pass_rate < quality_bar.min_scenario_pass_rate:
        failed_checks.append("scenario_pass_rate")

    return OutputQualityEval(
        thresholds=thresholds,
        icp_accounts=len(icp_scores),
        non_icp_accounts=len(non_icp_scores),
        icp_high_coverage=icp_high_coverage,
        icp_medium_coverage=icp_medium_coverage,
        non_icp_high_hit_rate=non_icp_high_hit_rate,
        non_icp_medium_hit_rate=non_icp_medium_hit_rate,
        scenario_pass_rate=scenario_pass_rate,
        passed=not failed_checks,
        failed_checks=failed_checks,
    )
