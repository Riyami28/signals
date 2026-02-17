from pathlib import Path

from typer.testing import CliRunner

from src.main import app


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _bootstrap_fixture(root: Path) -> None:
    _write(
        root / "config" / "seed_accounts.csv",
        "company_name,domain,source_type\nAcme,acme.example,seed\n",
    )
    _write(
        root / "config" / "source_registry.csv",
        "source,reliability,enabled\nfirst_party_csv,0.9,true\n",
    )
    _write(
        root / "config" / "thresholds.csv",
        "key,value\nhigh,70\nmedium,45\nlow,0\n",
    )
    _write(
        root / "config" / "keyword_lexicon.csv",
        "source,signal_code,keyword,confidence\n",
    )
    _write(
        root / "config" / "signal_registry.csv",
        "signal_code,product_scope,category,base_weight,half_life_days,min_confidence,enabled\ncloud_connected,shared,pql,90,14,0.5,true\n",
    )
    _write(
        root / "data" / "raw" / "first_party_events.csv",
        "domain,company_name,product,signal_code,source,evidence_url,evidence_text,confidence,observed_at\n"
        "acme.example,Acme,zopdev,cloud_connected,first_party_csv,https://app.example/connect,connected cloud,0.95,2026-02-16T00:00:00Z\n",
    )
    (root / "data" / "out").mkdir(parents=True, exist_ok=True)


def test_run_daily_creates_outputs(tmp_path, monkeypatch):
    root = tmp_path / "signals"
    _bootstrap_fixture(root)
    monkeypatch.setenv("SIGNALS_PROJECT_ROOT", str(root))
    monkeypatch.setenv("SIGNALS_DB_PATH", str(root / "data" / "signals.db"))

    runner = CliRunner()
    result = runner.invoke(app, ["run-daily", "--date", "2026-02-16"])

    assert result.exit_code == 0
    assert "run_id=" in result.stdout

    assert (root / "data" / "out" / "review_queue_20260216.csv").exists()
    assert (root / "data" / "out" / "daily_scores_20260216.csv").exists()
    assert (root / "data" / "out" / "source_quality_20260216.csv").exists()
    assert (root / "data" / "out" / "promotion_readiness_20260216.csv").exists()


def test_ingest_no_all_rejected(tmp_path, monkeypatch):
    root = tmp_path / "signals"
    _bootstrap_fixture(root)
    monkeypatch.setenv("SIGNALS_PROJECT_ROOT", str(root))
    monkeypatch.setenv("SIGNALS_DB_PATH", str(root / "data" / "signals.db"))

    runner = CliRunner()
    result = runner.invoke(app, ["ingest", "--no-all"])

    assert result.exit_code == 2


def test_prepare_review_input_command_creates_local_template(tmp_path, monkeypatch):
    root = tmp_path / "signals"
    _write(
        root / "data" / "out" / "review_queue_20260216.csv",
        "run_date,account_id,company_name,domain,product,score,tier,top_reason_1,top_reason_2,top_reason_3,evidence_links\n"
        "2026-02-16,acc_1,Acme,acme.example,zopdev,80,high,cloud_connected,,,\n",
    )
    monkeypatch.setenv("SIGNALS_PROJECT_ROOT", str(root))
    monkeypatch.setenv("SIGNALS_RUN_TIMEZONE", "America/Los_Angeles")

    runner = CliRunner()
    result = runner.invoke(app, ["prepare-review-input", "--date", "2026-02-16"])

    assert result.exit_code == 0
    assert "prepared_review_rows=1" in result.stdout
    assert (root / "data" / "raw" / "review_input.csv").exists()
