from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from src import db
from src.models import AccountScore, ReviewLabel, SignalObservation
from src.reporting import quality
from src.utils import stable_hash


def test_source_metrics_use_scored_sources_not_all_account_sources(tmp_path: Path):
    conn = db.get_connection()
    db.init_db(conn)

    account_id = db.upsert_account(conn, company_name="Acme", domain="acme.example", source_type="seed")
    obs = SignalObservation(
        obs_id=stable_hash({"obs": 1}, prefix="obs"),
        account_id=account_id,
        signal_code="compliance_initiative",
        product="shared",
        source="news_csv",
        observed_at="2026-02-16T00:00:00Z",
        evidence_url="https://example.com/news",
        evidence_text="soc 2",
        confidence=0.8,
        source_reliability=0.75,
        raw_payload_hash=stable_hash({"payload": 1}, prefix="raw"),
    )
    db.insert_signal_observation(conn, obs)

    run_id = db.create_score_run(conn, "2026-02-16")
    db.replace_run_scores(
        conn,
        run_id,
        component_scores=[],
        account_scores=[
            AccountScore(
                run_id=run_id,
                account_id=account_id,
                product="zopdev",
                score=75.0,
                tier="high",
                top_reasons_json=json.dumps(
                    [
                        {
                            "signal_code": "devops_role_open",
                            "component_score": 10.0,
                            "source": "jobs_csv",
                            "evidence_url": "https://example.com/jobs",
                            "evidence_text": "Hiring DevOps Engineer",
                        }
                    ]
                ),
                delta_7d=0.0,
            )
        ],
    )
    db.finish_score_run(conn, run_id, status="completed")

    label = ReviewLabel(
        review_id=stable_hash({"review": 1}, prefix="rev"),
        run_id=run_id,
        account_id=account_id,
        decision="approved",
        reviewer="qa",
        notes="",
        created_at="2026-02-16T01:00:00+00:00",
    )
    assert db.insert_review_label(conn, label) is True

    rows = quality.compute_and_persist_source_metrics(conn, date(2026, 2, 16))
    by_source = {str(row["source"]): row for row in rows}

    assert "jobs_csv" in by_source
    assert int(by_source["jobs_csv"]["sample_size"]) == 1
    assert "news_csv" not in by_source
