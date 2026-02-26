from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from src import db
from src.main import app
from src.utils import load_csv_rows


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _bootstrap_hunt_fixture(root: Path) -> None:
    _write(root / "config" / "seed_accounts.csv", "company_name,domain,source_type\nZopdev,zop.dev,seed\n")
    _write(root / "config" / "watchlist_accounts.csv", "company_name,domain,source_type\n")
    _write(
        root / "config" / "source_registry.csv",
        "source,reliability,enabled\n"
        "first_party_csv,0.9,true\n"
        "huginn_webhook,0.72,true\n"
        "story_hunt,0.78,true\n"
        "story_hunt_js,0.74,true\n",
    )
    _write(root / "config" / "thresholds.csv", "key,value\ntier_1,20\ntier_2,10\ntier_3,10\ntier_4,0\n")
    _write(root / "config" / "discovery_thresholds.csv", "key,value\nhigh,20\nmedium,10\nexplore,6\nlow,0\n")
    _write(
        root / "config" / "signal_registry.csv",
        "signal_code,product_scope,category,base_weight,half_life_days,min_confidence,enabled\n"
        "supply_chain_platform_rollout,shared,platform_demand,22,30,0.6,true\n"
        "cost_reduction_mandate,shared,spend_variance,18,30,0.6,true\n"
        "governance_enforcement_need,shared,governance,16,30,0.6,true\n"
        "compliance_governance_messaging,shared,governance,12,30,0.6,true\n"
        "poc_stage_progression,shared,pql,20,14,0.7,true\n",
    )
    _write(
        root / "config" / "signal_classes.csv",
        "signal_code,class,vertical_scope,promotion_critical\n"
        "supply_chain_platform_rollout,primary,cpg,true\n"
        "cost_reduction_mandate,primary,cpg,true\n"
        "governance_enforcement_need,primary,all,true\n"
        "compliance_governance_messaging,primary,all,true\n"
        "poc_stage_progression,primary,all,true\n",
    )
    _write(
        root / "config" / "keyword_lexicon.csv",
        "source,signal_code,keyword,confidence\n"
        "news,supply_chain_platform_rollout,control tower,0.88\n"
        "news,cost_reduction_mandate,margin improvement program,0.8\n"
        "news,governance_enforcement_need,policy enforcement,0.82\n"
        "news,compliance_governance_messaging,audit readiness,0.72\n"
        "news,poc_stage_progression,go-live date set,0.9\n",
    )
    _write(
        root / "config" / "account_profiles.csv",
        "domain,relationship_stage,vertical_tag,is_self,exclude_from_crm\n"
        "zop.dev,customer,internal,1,1\n"
        "unilever.com,customer,cpg,0,0\n"
        "noisyco.com,customer,cpg,0,0\n",
    )
    _write(root / "config" / "discovery_blocklist.csv", "domain,reason\nzop.dev,self\n")
    _write(
        root / "config" / "icp_reference_accounts.csv",
        "company_name,domain,relationship_stage,notes\nUnilever,unilever.com,customer,\nNoisyCo,noisyco.com,customer,\n",
    )
    _write(root / "config" / "account_source_handles.csv", "domain,website_url\n")
    _write(
        root / "config" / "profile_scenarios.csv",
        "scenario_name,max_score,expected_min_tier,expected_max_tier,weight\n",
    )
    _write(
        root / "config" / "icp_signal_playbook.csv",
        "relationship_stage,product,signal_code,priority,recommended_source,action_hint\n",
    )
    _write(
        root / "data" / "raw" / "first_party_events.csv",
        "company_name,domain,product,signal_code,source,evidence_url,evidence_text,confidence,observed_at\n",
    )
    _write(
        root / "data" / "raw" / "jobs.csv",
        "company_name,domain,title,description,url,observed_at,signal_code,confidence\n",
    )
    _write(
        root / "data" / "raw" / "news.csv", "company_name,domain,title,content,url,observed_at,signal_code,confidence\n"
    )
    _write(root / "data" / "raw" / "community.csv", "company_name,domain,text,url,observed_at,signal_code,confidence\n")
    _write(
        root / "data" / "raw" / "technographics.csv",
        "company_name,domain,text,url,observed_at,signal_code,confidence\n",
    )
    _write(root / "data" / "raw" / "news_feeds.csv", "company_name,domain,feed_url\n")


def test_listing_expansion_enqueues_story_urls(tmp_path: Path, monkeypatch):
    root = tmp_path / "signals"
    _bootstrap_hunt_fixture(root)
    monkeypatch.setenv("SIGNALS_PROJECT_ROOT", str(root))

    conn = db.get_connection()
    db.init_db(conn)
    listing_html = """
    <html><head><title>Unilever Newsroom</title></head>
    <body>
      <a href="/news/story-1">Story 1</a>
      <a href="/news/story-2">Story 2</a>
      <a href="/news/story-3">Story 3</a>
    </body></html>
    """.strip()
    inserted = db.insert_external_discovery_event(
        conn=conn,
        source="huginn_webhook",
        source_event_id="evt-listing",
        observed_at="2026-02-17T00:00:00Z",
        title="Unilever newsroom",
        text="Latest newsroom index",
        url="https://unilever.com/news",
        entry_url="https://unilever.com/news",
        url_type="listing",
        company_name_hint="Unilever",
        domain_hint="unilever.com",
        raw_payload_json=json.dumps({"html_content": listing_html}, ensure_ascii=True),
    )
    conn.close()
    assert inserted is True

    runner = CliRunner()
    frontier_result = runner.invoke(app, ["discover-frontier", "--date", "2026-02-17"])
    assert frontier_result.exit_code == 0
    fetch_result = runner.invoke(app, ["discover-fetch", "--date", "2026-02-17"])
    assert fetch_result.exit_code == 0
    extract_result = runner.invoke(app, ["discover-extract", "--date", "2026-02-17"])
    assert extract_result.exit_code == 0

    conn2 = db.get_connection()
    queued_links = conn2.execute(
        """
        SELECT COUNT(*) AS c
        FROM crawl_frontier
        WHERE run_date = '2026-02-17'
          AND depth = 1
        """
    ).fetchone()
    conn2.close()
    assert queued_links is not None
    assert int(queued_links["c"]) >= 1


def test_run_hunt_writes_lineage_and_applies_quality_gate(tmp_path: Path, monkeypatch):
    root = tmp_path / "signals"
    _bootstrap_hunt_fixture(root)
    monkeypatch.setenv("SIGNALS_PROJECT_ROOT", str(root))

    long_body = (
        "control tower program, margin improvement program, policy enforcement, audit readiness, "
        "go-live date set, procurement workflow modernization, cloud platform resilience, "
        "ERP modernization phase-2, vendor consolidation and risk controls."
    )
    long_body = " ".join([long_body for _ in range(30)])
    high_quality_html = (
        "<html><head><title>Unilever supply chain rollout</title>"
        "<meta name='author' content='Anita Rao'/>"
        "<meta property='article:published_time' content='2026-02-17T09:00:00Z'/>"
        "</head><body><p>" + long_body + "</p></body></html>"
    )
    low_quality_html = (
        "<html><head><title>NoisyCo update</title></head><body>"
        "<p>control tower margin improvement program policy enforcement go-live date set</p>"
        "</body></html>"
    )

    conn = db.get_connection()
    db.init_db(conn)
    ok1 = db.insert_external_discovery_event(
        conn=conn,
        source="huginn_webhook",
        source_event_id="evt-unilever-article",
        observed_at="2026-02-17T00:00:00Z",
        title="Unilever transformation",
        text="story article",
        url="https://unilever.com/news/transform",
        entry_url="https://unilever.com/news/transform",
        url_type="article",
        company_name_hint="Unilever",
        domain_hint="unilever.com",
        raw_payload_json=json.dumps({"html_content": high_quality_html}, ensure_ascii=True),
    )
    ok2 = db.insert_external_discovery_event(
        conn=conn,
        source="huginn_webhook",
        source_event_id="evt-noisy-article",
        observed_at="2026-02-17T00:00:00Z",
        title="NoisyCo transformation",
        text="short teaser",
        url="https://noisyco.com/news/brief",
        entry_url="https://noisyco.com/news/brief",
        url_type="article",
        company_name_hint="NoisyCo",
        domain_hint="noisyco.com",
        raw_payload_json=json.dumps({"html_content": low_quality_html}, ensure_ascii=True),
    )
    conn.close()
    assert ok1 is True
    assert ok2 is True

    runner = CliRunner()
    run_result = runner.invoke(app, ["run-hunt", "--date", "2026-02-17", "--profile", "light"])
    assert run_result.exit_code == 0
    assert "signal_lineage_rows=" in run_result.stdout

    queue_path = root / "data" / "out" / "discovery_queue_20260217.csv"
    story_path = root / "data" / "out" / "story_evidence_20260217.csv"
    lineage_path = root / "data" / "out" / "signal_lineage_20260217.csv"
    metrics_path = root / "data" / "out" / "hunt_quality_metrics_20260217.csv"

    assert queue_path.exists()
    assert story_path.exists()
    assert lineage_path.exists()
    assert metrics_path.exists()

    queue_rows = load_csv_rows(queue_path)
    unilever_rows = [row for row in queue_rows if row.get("domain") == "unilever.com"]
    assert unilever_rows, "expected unilever.com candidate from high-quality article"

    noisy_rows = [row for row in queue_rows if row.get("domain") == "noisyco.com"]
    if noisy_rows:
        assert all(row.get("confidence_band") == "explore" for row in noisy_rows)

    lineage_rows = load_csv_rows(lineage_path)
    assert len(lineage_rows) >= 1
    assert any(row.get("domain") == "unilever.com" for row in lineage_rows)


def test_hunt_respects_promotion_policy_config(tmp_path: Path, monkeypatch):
    root = tmp_path / "signals"
    _bootstrap_hunt_fixture(root)
    _write(
        root / "config" / "promotion_policy.csv",
        "key,value\n"
        "auto_push_bands,explore\n"
        "manual_review_bands,high|medium\n"
        "require_strict_evidence_for_auto_push,false\n"
        "min_auto_push_evidence_quality,0.0\n"
        "min_auto_push_relevance_score,0.0\n",
    )
    monkeypatch.setenv("SIGNALS_PROJECT_ROOT", str(root))

    long_body = (
        "control tower program, margin improvement program, policy enforcement, audit readiness, "
        "go-live date set, procurement workflow modernization, cloud platform resilience, "
        "ERP modernization phase-2, vendor consolidation and risk controls."
    )
    long_body = " ".join([long_body for _ in range(30)])
    high_quality_html = (
        "<html><head><title>Unilever supply chain rollout</title>"
        "<meta name='author' content='Anita Rao'/>"
        "<meta property='article:published_time' content='2026-02-17T09:00:00Z'/>"
        "</head><body><p>" + long_body + "</p></body></html>"
    )

    conn = db.get_connection()
    db.init_db(conn)
    inserted = db.insert_external_discovery_event(
        conn=conn,
        source="huginn_webhook",
        source_event_id="evt-unilever-policy",
        observed_at="2026-02-17T00:00:00Z",
        title="Unilever transformation",
        text="story article",
        url="https://unilever.com/news/transform",
        entry_url="https://unilever.com/news/transform",
        url_type="article",
        company_name_hint="Unilever",
        domain_hint="unilever.com",
        raw_payload_json=json.dumps({"html_content": high_quality_html}, ensure_ascii=True),
    )
    conn.close()
    assert inserted is True

    runner = CliRunner()
    run_result = runner.invoke(app, ["run-hunt", "--date", "2026-02-17", "--profile", "light"])
    assert run_result.exit_code == 0

    crm_rows = load_csv_rows(root / "data" / "out" / "crm_candidates_20260217.csv")
    manual_rows = load_csv_rows(root / "data" / "out" / "manual_review_queue_20260217.csv")
    assert "unilever.com" not in {row.get("domain") for row in crm_rows}
    assert "unilever.com" in {row.get("domain") for row in manual_rows}
    assert any(row.get("policy_decision") == "manual_review" for row in manual_rows)
