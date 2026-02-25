from __future__ import annotations

import importlib
from pathlib import Path

import pytest


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _bootstrap_ui_fixture(root: Path) -> None:
    _write(
        root / "config" / "watchlist_accounts.csv",
        "company_name,domain,country\nAcme,acme.example,United States\nBeta,beta.example,United States\n",
    )
    _write(root / "config" / "seed_accounts.csv", "company_name,domain,country\nCore,core.example,United States\n")
    _write(root / "config" / "account_source_handles.csv", "domain,news_query\nacme.example,acme cloud\n")
    _write(
        root / "config" / "signal_registry.csv",
        "signal_code,product_scope,category,base_weight,half_life_days,min_confidence,enabled\n"
        "cloud_connected,shared,pql,90,14,0.5,true\n",
    )

    _write(
        root / "data" / "raw" / "first_party_events.csv",
        "domain,company_name,product,signal_code,source,evidence_url,evidence_text,confidence,observed_at\n",
    )
    _write(root / "data" / "raw" / "jobs.csv", "domain,title,text\n")
    _write(root / "data" / "raw" / "news.csv", "domain,title,text\n")
    _write(root / "data" / "raw" / "technographics.csv", "domain,text\n")
    _write(root / "data" / "raw" / "community.csv", "domain,text\n")
    _write(root / "data" / "raw" / "news_feeds.csv", "domain,feed_url\n")
    _write(root / "data" / "raw" / "review_input.csv", "run_date,account_id,decision\n")

    output_stub = "run_date,value\n2026-02-21,1\n"
    _write(root / "data" / "out" / "review_queue_20260221.csv", output_stub)
    _write(root / "data" / "out" / "daily_scores_20260221.csv", output_stub)
    _write(root / "data" / "out" / "promotion_readiness_20260221.csv", output_stub)
    _write(root / "data" / "out" / "icp_coverage_20260221.csv", output_stub)
    _write(root / "data" / "out" / "ops_metrics_20260221.csv", output_stub)


def _client_for_root(monkeypatch: pytest.MonkeyPatch, root: Path):
    _ = pytest.importorskip("fastapi")
    _ = pytest.importorskip("fastapi.testclient")
    from fastapi.testclient import TestClient
    from src.ui import local_app

    monkeypatch.setenv("SIGNALS_PROJECT_ROOT", str(root))
    reloaded = importlib.reload(local_app)
    return TestClient(reloaded.app)


def test_tracked_companies_defaults_to_watchlist_with_source_counts(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    root = tmp_path / "signals"
    _bootstrap_ui_fixture(root)
    client = _client_for_root(monkeypatch, root)

    response = client.get("/api/tracked-companies?offset=0&limit=20")
    assert response.status_code == 200
    payload = response.json()

    assert payload["source"] == "watchlist"
    assert payload["total"] == 2
    assert payload["source_counts"]["watchlist"] == 2
    assert payload["source_counts"]["seed"] == 1
    assert payload["source_counts"]["all"] == 3


def test_overview_includes_output_bundle_and_extended_term_glossary(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    root = tmp_path / "signals"
    _bootstrap_ui_fixture(root)
    client = _client_for_root(monkeypatch, root)

    response = client.get("/api/overview")
    assert response.status_code == 200
    payload = response.json()

    assert payload["output_bundle_status"]["available"] is True
    assert payload["output_bundle_status"]["status"] == "complete"
    terms = {row["term"] for row in payload["term_glossary"]}
    assert "Daily Scores (Output Sheet)" in terms
    assert "Signal Half-life" in terms
