from __future__ import annotations

from pathlib import Path

from src import db
from src.models import AccountScore, ComponentScore
from src.reporting import icp_playbook


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def test_compute_icp_signal_gaps_surfaces_missing_priority_signals(tmp_path: Path):
    conn = db.get_connection()
    db.init_db(conn)

    customer_id = db.upsert_account(conn, company_name="Customer One", domain="customer.example", source_type="seed")
    poc_id = db.upsert_account(conn, company_name="POC One", domain="poc.example", source_type="seed")

    run_id = db.create_score_run(conn, "2026-02-17")
    db.replace_run_scores(
        conn,
        run_id=run_id,
        component_scores=[
            ComponentScore(
                run_id=run_id,
                account_id=customer_id,
                product="zopdev",
                signal_code="poc_stage_progression",
                component_score=10.0,
            ),
            ComponentScore(
                run_id=run_id,
                account_id=customer_id,
                product="zopdev",
                signal_code="compliance_initiative",
                component_score=6.0,
            ),
            ComponentScore(
                run_id=run_id,
                account_id=poc_id,
                product="zopnight",
                signal_code="cost_reduction_mandate",
                component_score=7.0,
            ),
        ],
        account_scores=[
            AccountScore(
                run_id=run_id,
                account_id=customer_id,
                product="zopdev",
                score=20.0,
                tier="high",
                top_reasons_json="[]",
                delta_7d=0.0,
            ),
            AccountScore(
                run_id=run_id,
                account_id=poc_id,
                product="zopnight",
                score=11.0,
                tier="medium",
                top_reasons_json="[]",
                delta_7d=0.0,
            ),
        ],
    )
    db.finish_score_run(conn, run_id, status="completed")

    reference_path = tmp_path / "config" / "icp_reference_accounts.csv"
    _write(
        reference_path,
        "company_name,domain,relationship_stage,notes\n"
        "Customer One,customer.example,customer,\n"
        "POC One,poc.example,poc,\n",
    )
    playbook_path = tmp_path / "config" / "icp_signal_playbook.csv"
    _write(
        playbook_path,
        "relationship_stage,product,signal_code,priority,recommended_source,action_hint\n"
        "customer,shared,poc_stage_progression,p0,first_party_csv,track stage changes\n"
        "customer,zopdev,compliance_initiative,p0,news_csv,track compliance projects\n"
        "customer,zopnight,cost_reduction_mandate,p1,first_party_csv,track cost pressure\n"
        "poc,shared,poc_stage_progression,p0,first_party_csv,track stage changes\n"
        "poc,zopnight,cost_reduction_mandate,p1,news_csv,track cost pressure\n",
    )

    rows, summary = icp_playbook.compute_icp_signal_gaps(
        conn=conn,
        run_id=run_id,
        reference_csv_path=reference_path,
        playbook_path=playbook_path,
    )

    assert len(rows) == 5
    assert summary == {
        "total_accounts": 2,
        "expected_signals": 5,
        "observed_signals": 3,
        "coverage_rate": 0.6,
        "high_priority_gaps": 2,
        "accounts_with_full_coverage": 0,
    }

    by_key = {
        (str(row["domain"]), str(row["target_product"]), str(row["signal_code"])): row
        for row in rows
    }
    missing_customer_cost = by_key[("customer.example", "zopnight", "cost_reduction_mandate")]
    assert int(missing_customer_cost["present"]) == 0
    assert str(missing_customer_cost["recommended_source"]) == "first_party_csv"

    missing_poc_progression = by_key[("poc.example", "shared", "poc_stage_progression")]
    assert int(missing_poc_progression["present"]) == 0
    assert str(missing_poc_progression["priority"]) == "p0"
