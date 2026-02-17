from __future__ import annotations

from pathlib import Path

from src import db
from src.models import AccountScore
from src.reporting import calibration
from src.scoring.rules import Thresholds
from src.utils import load_csv_rows


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def test_suggest_thresholds_for_run_targets_requested_icp_coverage(tmp_path: Path):
    conn = db.get_connection(tmp_path / "signals.db")
    db.init_db(conn)

    scores_by_domain = {
        "icp1.example": 100.0,
        "icp2.example": 80.0,
        "icp3.example": 60.0,
        "icp4.example": 40.0,
        "icp5.example": 20.0,
        "non1.example": 90.0,
        "non2.example": 30.0,
        "non3.example": 10.0,
    }
    account_ids: dict[str, str] = {}
    for domain in scores_by_domain:
        account_ids[domain] = db.upsert_account(conn, company_name=domain, domain=domain, source_type="seed")

    run_id = db.create_score_run(conn, "2026-02-16")
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

    suggestion = calibration.suggest_thresholds_for_run(
        conn=conn,
        run_id=run_id,
        reference_csv_path=reference_path,
        medium_target_coverage=0.6,
        high_target_coverage=0.2,
        current_thresholds=Thresholds(high=70.0, medium=45.0, low=0.0),
    )

    assert suggestion.high == 100.0
    assert suggestion.medium == 60.0
    assert suggestion.icp_high_coverage == 0.2
    assert suggestion.icp_medium_coverage == 0.6
    assert suggestion.non_icp_high_hit_rate == 0.0
    assert suggestion.non_icp_medium_hit_rate == 0.3333


def test_write_thresholds_persists_csv(tmp_path: Path):
    path = tmp_path / "thresholds.csv"
    calibration.write_thresholds(path, high=12.34, medium=5.67, low=0.0)
    rows = load_csv_rows(path)
    assert rows == [
        {"key": "high", "value": "12.34"},
        {"key": "medium", "value": "5.67"},
        {"key": "low", "value": "0.0"},
    ]


def test_load_scenarios_uses_defaults_when_missing(tmp_path: Path):
    scenarios = calibration.load_scenarios(tmp_path / "missing.csv")
    assert len(scenarios) >= 3
    assert scenarios[0].scenario_name


def test_suggest_profile_for_run_balances_icp_and_non_icp(tmp_path: Path):
    conn = db.get_connection(tmp_path / "signals.db")
    db.init_db(conn)

    scores_by_domain = {
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
    }

    account_ids: dict[str, str] = {}
    for domain in scores_by_domain:
        account_ids[domain] = db.upsert_account(conn, company_name=domain, domain=domain, source_type="seed")

    run_id = db.create_score_run(conn, "2026-02-16")
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
    scenario_path = tmp_path / "config" / "profile_scenarios.csv"
    _write(
        scenario_path,
        "scenario_name,max_score,expected_min_tier,expected_max_tier,weight\n"
        "strong,14,high,high,1\n"
        "credible,9,medium,high,1\n"
        "borderline,5,low,medium,1\n"
        "weak,2,low,low,1\n",
    )
    scenarios = calibration.load_scenarios(scenario_path)

    suggestion = calibration.suggest_profile_for_run(
        conn=conn,
        run_id=run_id,
        reference_csv_path=reference_path,
        scenarios=scenarios,
        min_icp_medium_coverage=0.6,
        max_non_icp_medium_hit_rate=0.5,
        max_non_icp_high_hit_rate=0.2,
        min_scenario_pass_rate=0.9,
        current_thresholds=Thresholds(high=70.0, medium=45.0, low=0.0),
    )

    assert suggestion.constraints_satisfied is True
    assert suggestion.icp_medium_coverage >= 0.6
    assert suggestion.non_icp_medium_hit_rate <= 0.5
    assert suggestion.non_icp_high_hit_rate <= 0.2
    assert suggestion.scenario_pass_rate >= 0.9
