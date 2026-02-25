from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.reporting import calibration
from src.reporting.evals import OutputQualityBar, OutputQualityEval, evaluate_run_output_quality
from src.scoring.rules import Thresholds


@dataclass(frozen=True)
class ImprovementIteration:
    iteration: int
    thresholds: Thresholds
    evaluation: OutputQualityEval


@dataclass(frozen=True)
class ThresholdSelfImprovementResult:
    passed: bool
    converged: bool
    final_thresholds: Thresholds
    iterations: list[ImprovementIteration]


def _same_thresholds(left: Thresholds, right: Thresholds) -> bool:
    return (
        round(float(left.high), 4) == round(float(right.high), 4)
        and round(float(left.medium), 4) == round(float(right.medium), 4)
        and round(float(left.low), 4) == round(float(right.low), 4)
    )


def run_threshold_self_improvement(
    conn: Any,
    run_id: str,
    reference_csv_path: Path,
    current_thresholds: Thresholds,
    quality_bar: OutputQualityBar,
    max_iterations: int = 5,
    scenarios: list[calibration.ScenarioRule] | None = None,
) -> ThresholdSelfImprovementResult:
    if max_iterations < 1:
        raise ValueError("max_iterations must be >= 1")

    selected_scenarios = scenarios if scenarios is not None else calibration.load_scenarios(None)
    thresholds = Thresholds(
        high=float(current_thresholds.high),
        medium=float(current_thresholds.medium),
        low=float(current_thresholds.low),
    )
    iterations: list[ImprovementIteration] = []
    converged = False

    for iteration in range(1, max_iterations + 1):
        evaluation = evaluate_run_output_quality(
            conn=conn,
            run_id=run_id,
            reference_csv_path=reference_csv_path,
            thresholds=thresholds,
            quality_bar=quality_bar,
            scenarios=selected_scenarios,
        )
        iterations.append(
            ImprovementIteration(
                iteration=iteration,
                thresholds=thresholds,
                evaluation=evaluation,
            )
        )
        if evaluation.passed:
            converged = True
            break
        if iteration >= max_iterations:
            break

        suggestion = calibration.suggest_profile_for_run(
            conn=conn,
            run_id=run_id,
            reference_csv_path=reference_csv_path,
            scenarios=selected_scenarios,
            min_icp_medium_coverage=quality_bar.min_icp_medium_coverage,
            max_non_icp_medium_hit_rate=quality_bar.max_non_icp_medium_hit_rate,
            max_non_icp_high_hit_rate=quality_bar.max_non_icp_high_hit_rate,
            min_scenario_pass_rate=quality_bar.min_scenario_pass_rate,
            current_thresholds=thresholds,
        )
        coverage_suggestion = calibration.suggest_thresholds_for_run(
            conn=conn,
            run_id=run_id,
            reference_csv_path=reference_csv_path,
            medium_target_coverage=quality_bar.min_icp_medium_coverage,
            high_target_coverage=quality_bar.min_icp_high_coverage,
            current_thresholds=thresholds,
        )
        next_high = float(suggestion.high)
        next_medium = float(suggestion.medium)
        if int(coverage_suggestion.icp_accounts) > 0:
            next_high = min(next_high, float(coverage_suggestion.high))
            next_medium = min(next_medium, float(coverage_suggestion.medium))
            if next_high < next_medium:
                next_high = next_medium
        next_thresholds = Thresholds(
            high=next_high,
            medium=next_medium,
            low=float(suggestion.low),
        )
        if _same_thresholds(next_thresholds, thresholds):
            converged = False
            break
        thresholds = next_thresholds

    passed = bool(iterations and iterations[-1].evaluation.passed)
    return ThresholdSelfImprovementResult(
        passed=passed,
        converged=converged,
        final_thresholds=iterations[-1].thresholds if iterations else thresholds,
        iterations=iterations,
    )
