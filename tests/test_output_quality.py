from __future__ import annotations

from pathlib import Path

from src import db
from src.models import AccountScore
from src.reporting.evals import OutputQualityBar, evaluate_run_output_quality
from src.reporting.improvement import run_threshold_self_improvement
from src.scoring.rules import Thresholds


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _seed_scores(conn, run_date: str, scores_by_domain: dict[str, float]) -> str:
    account_ids: dict[str, str] = {}
    for domain in scores_by_domain:
        account_ids[domain] = db.upsert_account(conn, company_name=domain, domain=domain, source_type="seed")

    run_id = db.create_score_run(conn, run_date)
    db.replace_run_scores(
        conn,
        run_id,
        component_scores=[],
        account_scores=[
            AccountScore(
                run_id=run_id,
                account_id=account_ids[domain],
                product="zopdev",
                score=score,
                tier="low",
                top_reasons_json="[]",
                delta_7d=0.0,
            )
            for domain, score in scores_by_domain.items()
        ],
    )
    db.finish_score_run(conn, run_id, status="completed")
    return run_id


def _reference_csv(tmp_path: Path) -> Path:
    reference_path = tmp_path / "config" / "icp_reference_accounts.csv"
    _write(
        reference_path,
        "company_name,domain,relationship_stage,notes\n"
        "ICP 1,icp1.example,customer,\n"
        "ICP 2,icp2.example,customer,\n"
        "ICP 3,icp3.example,customer,\n"
        "ICP 4,icp4.example,poc,\n"
        "ICP 5,icp5.example,poc,\n",
    )
    return reference_path


def test_evaluate_run_output_quality_passes_when_metrics_clear_bar(tmp_path: Path):
    conn = db.get_connection()
    db.init_db(conn)
    run_id = _seed_scores(
        conn,
        "2026-02-16",
        {
            "icp1.example": 12.0,
            "icp2.example": 10.0,
            "icp3.example": 8.0,
            "icp4.example": 6.0,
            "icp5.example": 4.0,
            "non1.example": 14.0,
            "non2.example": 9.0,
            "non3.example": 5.0,
            "non4.example": 3.0,
            "non5.example": 2.0,
            "non6.example": 1.0,
        },
    )
    reference_csv = _reference_csv(tmp_path)

    result = evaluate_run_output_quality(
        conn=conn,
        run_id=run_id,
        reference_csv_path=reference_csv,
        thresholds=Thresholds(tier_1=12.0, tier_2=8.0, tier_3=4.0, tier_4=0.0),
        quality_bar=OutputQualityBar(),
    )

    assert result.passed is True
    assert result.failed_checks == []
    assert result.icp_medium_coverage >= 0.6
    assert result.icp_high_coverage >= 0.2
    assert result.non_icp_medium_hit_rate <= 0.5
    assert result.non_icp_high_hit_rate <= 0.25
    assert result.scenario_pass_rate >= 0.9


def test_evaluate_run_output_quality_reports_failed_checks(tmp_path: Path):
    conn = db.get_connection()
    db.init_db(conn)
    run_id = _seed_scores(
        conn,
        "2026-02-16",
        {
            "icp1.example": 12.0,
            "icp2.example": 10.0,
            "icp3.example": 8.0,
            "icp4.example": 6.0,
            "icp5.example": 4.0,
            "non1.example": 14.0,
            "non2.example": 9.0,
            "non3.example": 5.0,
            "non4.example": 3.0,
            "non5.example": 2.0,
            "non6.example": 1.0,
        },
    )
    reference_csv = _reference_csv(tmp_path)

    result = evaluate_run_output_quality(
        conn=conn,
        run_id=run_id,
        reference_csv_path=reference_csv,
        thresholds=Thresholds(tier_1=13.0, tier_2=12.0, tier_3=6.0, tier_4=0.0),
        quality_bar=OutputQualityBar(),
    )

    assert result.passed is False
    assert "icp_medium_coverage" in result.failed_checks
    assert "icp_high_coverage" in result.failed_checks
    assert "scenario_pass_rate" in result.failed_checks


def test_threshold_self_improvement_loop_converges_to_passing_profile(tmp_path: Path):
    conn = db.get_connection()
    db.init_db(conn)
    run_id = _seed_scores(
        conn,
        "2026-02-16",
        {
            "icp1.example": 12.0,
            "icp2.example": 10.0,
            "icp3.example": 8.0,
            "icp4.example": 6.0,
            "icp5.example": 4.0,
            "non1.example": 14.0,
            "non2.example": 9.0,
            "non3.example": 5.0,
            "non4.example": 3.0,
            "non5.example": 2.0,
            "non6.example": 1.0,
        },
    )
    reference_csv = _reference_csv(tmp_path)

    result = run_threshold_self_improvement(
        conn=conn,
        run_id=run_id,
        reference_csv_path=reference_csv,
        current_thresholds=Thresholds(tier_1=70.0, tier_2=45.0, tier_3=20.0, tier_4=0.0),
        quality_bar=OutputQualityBar(),
        max_iterations=4,
    )

    assert result.passed is True
    assert len(result.iterations) >= 2
    assert result.final_thresholds.tier_1 < 70.0
    assert result.final_thresholds.tier_2 < 45.0
