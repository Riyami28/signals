from __future__ import annotations

from dataclasses import dataclass
import math
from pathlib import Path
import sqlite3

from src.scoring.rules import Thresholds
from src.utils import load_csv_rows, normalize_domain, write_csv_rows


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


def suggest_thresholds_for_run(
    conn: sqlite3.Connection,
    run_id: str,
    reference_csv_path: Path,
    medium_target_coverage: float,
    high_target_coverage: float,
    current_thresholds: Thresholds,
) -> ThresholdSuggestion:
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
