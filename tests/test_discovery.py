from __future__ import annotations

from pathlib import Path
import json
import sqlite3

import pytest
from typer.testing import CliRunner

from src import db
from src.main import app
from src.models import SignalObservation
from src.utils import load_csv_rows, stable_hash


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _bootstrap_core_config(root: Path) -> None:
    _write(root / "config" / "seed_accounts.csv", "company_name,domain,source_type\nZopdev,zop.dev,seed\n")
    _write(
        root / "config" / "source_registry.csv",
        "source,reliability,enabled\ntechnographics_csv,0.8,true\nhuginn_webhook,0.9,true\nfirst_party_csv,0.9,true\n",
    )
    _write(root / "config" / "thresholds.csv", "key,value\nhigh,20\nmedium,10\nlow,0\n")
    _write(root / "config" / "discovery_thresholds.csv", "key,value\nhigh,20\nmedium,10\nexplore,6\nlow,0\n")
    _write(
        root / "config" / "signal_registry.csv",
        "signal_code,product_scope,category,base_weight,half_life_days,min_confidence,enabled\n"
        "kubernetes_detected,zopdev,technographic,90,45,0.6,true\n"
        "cost_reduction_mandate,zopnight,spend_variance,20,30,0.6,true\n"
        "supply_chain_platform_rollout,zopday,platform_demand,20,30,0.6,true\n"
        "governance_enforcement_need,zopnight,governance,18,30,0.6,true\n"
        "poc_stage_progression,shared,pql,22,14,0.7,true\n",
    )
    _write(
        root / "config" / "signal_classes.csv",
        "signal_code,class,vertical_scope,promotion_critical\n"
        "kubernetes_detected,secondary,all,false\n"
        "cost_reduction_mandate,primary,all,true\n"
        "supply_chain_platform_rollout,primary,cpg,true\n"
        "governance_enforcement_need,primary,all,true\n"
        "poc_stage_progression,primary,all,true\n",
    )
    _write(
        root / "config" / "keyword_lexicon.csv",
        "source,signal_code,keyword,confidence\n"
        "news,cost_reduction_mandate,cost transformation office,0.9\n"
        "news,supply_chain_platform_rollout,control tower,0.88\n"
        "news,governance_enforcement_need,audit readiness,0.86\n"
        "news,poc_stage_progression,go-live date set,0.9\n",
    )
    _write(
        root / "config" / "account_profiles.csv",
        "domain,relationship_stage,vertical_tag,is_self,exclude_from_crm\nzop.dev,customer,internal,1,1\n",
    )
    _write(root / "config" / "discovery_blocklist.csv", "domain,reason\nzop.dev,self_domain\n")
    _write(root / "config" / "account_source_handles.csv", "domain,website_url\n")
    _write(root / "config" / "icp_reference_accounts.csv", "company_name,domain,relationship_stage,notes\n")
    _write(root / "config" / "profile_scenarios.csv", "scenario_name,max_score,expected_min_tier,expected_max_tier,weight\n")
    _write(root / "data" / "raw" / "first_party_events.csv", "company_name,domain,product,signal_code,source,evidence_url,evidence_text,confidence,observed_at\n")
    _write(root / "data" / "raw" / "jobs.csv", "company_name,domain,title,description,url,observed_at,signal_code,confidence\n")
    _write(root / "data" / "raw" / "news.csv", "company_name,domain,title,content,url,observed_at,signal_code,confidence\n")
    _write(root / "data" / "raw" / "community.csv", "company_name,domain,text,url,observed_at,signal_code,confidence\n")
    _write(root / "data" / "raw" / "technographics.csv", "company_name,domain,text,url,observed_at,signal_code,confidence\n")
    _write(root / "data" / "raw" / "news_feeds.csv", "company_name,domain,feed_url\n")


def test_secondary_only_signal_downgrades_tier(tmp_path: Path, monkeypatch):
    root = tmp_path / "signals"
    _bootstrap_core_config(root)
    monkeypatch.setenv("SIGNALS_PROJECT_ROOT", str(root))
    monkeypatch.setenv("SIGNALS_DB_PATH", str(root / "data" / "signals.db"))

    conn = db.get_connection(root / "data" / "signals.db")
    db.init_db(conn)
    account_id = db.upsert_account(conn, company_name="Noise Co", domain="noise.example", source_type="discovered")
    observation = SignalObservation(
        obs_id=stable_hash({"obs": "noise"}, prefix="obs"),
        account_id=account_id,
        signal_code="kubernetes_detected",
        product="shared",
        source="technographics_csv",
        observed_at="2026-02-17T00:00:00Z",
        evidence_url="https://noise.example/stack",
        evidence_text="kubernetes footprint detected",
        confidence=0.95,
        source_reliability=0.9,
        raw_payload_hash=stable_hash({"payload": "noise"}, prefix="raw"),
    )
    assert db.insert_signal_observation(conn, observation) is True
    conn.close()

    runner = CliRunner()
    result = runner.invoke(app, ["score", "--date", "2026-02-17"])
    assert result.exit_code == 0

    conn2 = sqlite3.connect(root / "data" / "signals.db")
    conn2.row_factory = sqlite3.Row
    row = conn2.execute(
        """
        SELECT s.tier, s.score
        FROM account_scores s
        JOIN score_runs r ON r.run_id = s.run_id
        JOIN accounts a ON a.account_id = s.account_id
        WHERE date(r.run_date) = date('2026-02-17')
          AND a.domain = 'noise.example'
        LIMIT 1
        """
    ).fetchone()
    conn2.close()

    assert row is not None
    assert float(row["score"]) >= 20.0
    assert str(row["tier"]) == "low"


def test_run_discovery_outputs_and_excludes_self_domain(tmp_path: Path, monkeypatch):
    root = tmp_path / "signals"
    _bootstrap_core_config(root)
    monkeypatch.setenv("SIGNALS_PROJECT_ROOT", str(root))
    monkeypatch.setenv("SIGNALS_DB_PATH", str(root / "data" / "signals.db"))

    conn = db.get_connection(root / "data" / "signals.db")
    db.init_db(conn)
    long_signal_text = (
        "control tower program and cost transformation office with audit readiness and go-live date set "
        "plus policy enforcement and procurement readiness across supply chain and cloud operations. "
    )
    long_signal_text = " ".join([long_signal_text for _ in range(30)])
    consumer_html = (
        "<html><head><title>ConsumerCo launches control tower and cost office</title>"
        "<meta name='author' content='Asha Gupta'/>"
        "<meta property='article:published_time' content='2026-02-17T08:00:00Z'/>"
        "</head><body><p>"
        + long_signal_text
        + "</p></body></html>"
    )
    zop_html = (
        "<html><head><title>Zop internal rollout</title></head>"
        "<body><p>" + long_signal_text + "</p></body></html>"
    )

    inserted_primary = db.insert_external_discovery_event(
        conn=conn,
        source="huginn_webhook",
        source_event_id="evt-1",
        observed_at="2026-02-17T00:00:00Z",
        title="ConsumerCo launches control tower and cost office",
        text="control tower program and cost transformation office with audit readiness and go-live date set",
        url="https://consumerco.com/news",
        entry_url="https://consumerco.com/news",
        url_type="article",
        company_name_hint="ConsumerCo",
        domain_hint="consumerco.com",
        raw_payload_json=json.dumps({"id": "evt-1", "html_content": consumer_html}, ensure_ascii=True),
    )
    inserted_self = db.insert_external_discovery_event(
        conn=conn,
        source="huginn_webhook",
        source_event_id="evt-2",
        observed_at="2026-02-17T00:00:00Z",
        title="Zop internal rollout",
        text="control tower program and cost transformation office with audit readiness and go-live date set",
        url="https://zop.dev/blog",
        entry_url="https://zop.dev/blog",
        url_type="article",
        company_name_hint="Zopdev",
        domain_hint="zop.dev",
        raw_payload_json=json.dumps({"id": "evt-2", "html_content": zop_html}, ensure_ascii=True),
    )
    conn.close()
    assert inserted_primary is True
    assert inserted_self is True

    runner = CliRunner()
    result = runner.invoke(app, ["run-discovery", "--date", "2026-02-17"])
    assert result.exit_code == 0
    assert "total_candidates=" in result.stdout

    queue_path = root / "data" / "out" / "discovery_queue_20260217.csv"
    crm_path = root / "data" / "out" / "crm_candidates_20260217.csv"
    metrics_path = root / "data" / "out" / "discovery_metrics_20260217.csv"
    assert queue_path.exists()
    assert crm_path.exists()
    assert metrics_path.exists()

    queue_rows = load_csv_rows(queue_path)
    domains = {row["domain"] for row in queue_rows}
    assert "consumerco.com" in domains
    assert "zop.dev" not in domains

    crm_rows = load_csv_rows(crm_path)
    crm_domains = {row["domain"] for row in crm_rows}
    assert "consumerco.com" in crm_domains


def test_webhook_auth_and_dedupe(tmp_path: Path, monkeypatch):
    fastapi = pytest.importorskip("fastapi")
    _ = fastapi
    _ = pytest.importorskip("fastapi.testclient")

    root = tmp_path / "signals"
    _bootstrap_core_config(root)
    monkeypatch.setenv("SIGNALS_PROJECT_ROOT", str(root))
    monkeypatch.setenv("SIGNALS_DB_PATH", str(root / "data" / "signals.db"))
    monkeypatch.setenv("SIGNALS_DISCOVERY_WEBHOOK_TOKEN", "secret-token")

    from fastapi.testclient import TestClient
    from src.discovery.webhook import create_app

    app_instance = create_app()
    client = TestClient(app_instance)

    payload = {
        "source": "huginn_webhook",
        "source_event_id": "evt-100",
        "observed_at": "2026-02-17T00:00:00Z",
        "title": "Discovery sample",
        "text": "control tower with audit readiness and go-live date set",
        "url": "https://sampleco.com/post",
        "company_name_hint": "SampleCo",
        "domain_hint": "sampleco.com",
        "raw_payload": {"id": "evt-100"},
    }

    unauthorized = client.post("/v1/discovery/events", json=payload, headers={"X-Discovery-Token": "bad"})
    assert unauthorized.status_code == 401

    ok_first = client.post("/v1/discovery/events", json=payload, headers={"X-Discovery-Token": "secret-token"})
    assert ok_first.status_code == 200
    assert ok_first.json()["inserted"] == 1

    ok_duplicate = client.post("/v1/discovery/events", json=payload, headers={"X-Discovery-Token": "secret-token"})
    assert ok_duplicate.status_code == 200
    assert ok_duplicate.json()["inserted"] == 0


def test_webhook_rejects_placeholder_domain(tmp_path: Path, monkeypatch):
    fastapi = pytest.importorskip("fastapi")
    _ = fastapi
    _ = pytest.importorskip("fastapi.testclient")

    root = tmp_path / "signals"
    _bootstrap_core_config(root)
    monkeypatch.setenv("SIGNALS_PROJECT_ROOT", str(root))
    monkeypatch.setenv("SIGNALS_DB_PATH", str(root / "data" / "signals.db"))
    monkeypatch.setenv("SIGNALS_DISCOVERY_WEBHOOK_TOKEN", "secret-token")

    from fastapi.testclient import TestClient
    from src.discovery.webhook import create_app

    app_instance = create_app()
    client = TestClient(app_instance)

    payload = {
        "source": "huginn_webhook",
        "source_event_id": "evt-placeholder",
        "observed_at": "2026-02-17T00:00:00Z",
        "title": "Placeholder domain event",
        "text": "ERP modernization phase-2 rollout",
        "url": "https://freshmart.example/press/rollout",
        "company_name_hint": "FreshMart",
        "domain_hint": "freshmart.example",
        "raw_payload": {"id": "evt-placeholder"},
    }

    rejected = client.post("/v1/discovery/events", json=payload, headers={"X-Discovery-Token": "secret-token"})
    assert rejected.status_code == 422

    conn = db.get_connection(root / "data" / "signals.db")
    db.init_db(conn)
    count = conn.execute("SELECT COUNT(*) FROM external_discovery_events").fetchone()[0]
    conn.close()
    assert int(count) == 0
