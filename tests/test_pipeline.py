import json
from pathlib import Path

from typer.testing import CliRunner

from src import db
from src.main import app
from src.utils import load_csv_rows


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
        "key,value\ntier_1,70\ntier_2,45\ntier_3,45\ntier_4,0\n",
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

    runner = CliRunner()
    result = runner.invoke(app, ["run-daily", "--date", "2026-02-16"])

    assert result.exit_code == 0
    assert "run_id=" in result.stdout

    assert (root / "data" / "out" / "review_queue_20260216.csv").exists()
    assert (root / "data" / "out" / "daily_scores_20260216.csv").exists()
    assert (root / "data" / "out" / "source_quality_20260216.csv").exists()
    assert (root / "data" / "out" / "promotion_readiness_20260216.csv").exists()


def test_run_daily_accepts_runtime_overrides(tmp_path, monkeypatch):
    root = tmp_path / "signals"
    _bootstrap_fixture(root)
    monkeypatch.setenv("SIGNALS_PROJECT_ROOT", str(root))

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "run-daily",
            "--date",
            "2026-02-16",
            "--live-max-accounts",
            "5",
            "--stage-timeout-seconds",
            "120",
        ],
    )

    assert result.exit_code == 0
    assert "stage=ingest status=started live_max_accounts=5" in result.stdout
    assert "timeout_seconds=120" in result.stdout
    assert "stage=score status=completed" in result.stdout


def test_ingest_no_all_rejected(tmp_path, monkeypatch):
    root = tmp_path / "signals"
    _bootstrap_fixture(root)
    monkeypatch.setenv("SIGNALS_PROJECT_ROOT", str(root))

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


def test_crawl_diagnostics_command_handles_empty_day(tmp_path, monkeypatch):
    root = tmp_path / "signals"
    _bootstrap_fixture(root)
    monkeypatch.setenv("SIGNALS_PROJECT_ROOT", str(root))

    runner = CliRunner()
    result = runner.invoke(app, ["crawl-diagnostics", "--date", "2026-02-16"])

    assert result.exit_code == 0
    assert "crawl_attempts=0" in result.stdout


def test_calibrate_thresholds_command_emits_suggestion(tmp_path, monkeypatch):
    root = tmp_path / "signals"
    _bootstrap_fixture(root)
    _write(
        root / "config" / "icp_reference_accounts.csv",
        "company_name,domain,relationship_stage,notes\nAcme,acme.example,customer,\n",
    )
    monkeypatch.setenv("SIGNALS_PROJECT_ROOT", str(root))

    runner = CliRunner()
    daily_result = runner.invoke(app, ["run-daily", "--date", "2026-02-16"])
    assert daily_result.exit_code == 0

    result = runner.invoke(app, ["calibrate-thresholds", "--date", "2026-02-16"])
    assert result.exit_code == 0
    assert "suggested_high=" in result.stdout
    assert "suggested_medium=" in result.stdout


def test_tune_profile_command_emits_profile_suggestion(tmp_path, monkeypatch):
    root = tmp_path / "signals"
    _bootstrap_fixture(root)
    _write(
        root / "config" / "icp_reference_accounts.csv",
        "company_name,domain,relationship_stage,notes\nAcme,acme.example,customer,\n",
    )
    monkeypatch.setenv("SIGNALS_PROJECT_ROOT", str(root))

    runner = CliRunner()
    daily_result = runner.invoke(app, ["run-daily", "--date", "2026-02-16"])
    assert daily_result.exit_code == 0

    result = runner.invoke(app, ["tune-profile", "--date", "2026-02-16"])
    assert result.exit_code == 0
    assert "suggested_high=" in result.stdout
    assert "scenario_pass_rate=" in result.stdout


def test_icp_signal_gaps_command_writes_report(tmp_path, monkeypatch):
    root = tmp_path / "signals"
    _bootstrap_fixture(root)
    _write(
        root / "config" / "icp_reference_accounts.csv",
        "company_name,domain,relationship_stage,notes\nAcme,acme.example,customer,\n",
    )
    _write(
        root / "config" / "icp_signal_playbook.csv",
        "relationship_stage,product,signal_code,priority,recommended_source,action_hint\n"
        "customer,shared,cloud_connected,p0,first_party_csv,track product usage\n"
        "customer,zopnight,cost_reduction_mandate,p1,first_party_csv,track budget pressure\n",
    )
    monkeypatch.setenv("SIGNALS_PROJECT_ROOT", str(root))

    runner = CliRunner()
    daily_result = runner.invoke(app, ["run-daily", "--date", "2026-02-16"])
    assert daily_result.exit_code == 0

    result = runner.invoke(app, ["icp-signal-gaps", "--date", "2026-02-16"])
    assert result.exit_code == 0
    assert "coverage_rate=" in result.stdout
    assert (root / "data" / "out" / "icp_signal_gaps_20260216.csv").exists()


def test_review_queue_is_account_level_and_excludes_internal_domain(tmp_path, monkeypatch):
    root = tmp_path / "signals"
    _bootstrap_fixture(root)
    _write(
        root / "config" / "thresholds.csv",
        "key,value\ntier_1,70\ntier_2,30\ntier_3,30\ntier_4,0\n",
    )
    _write(
        root / "config" / "seed_accounts.csv",
        "company_name,domain,source_type\nAcme,acme.example,seed\nZopdev,zop.dev,seed\n",
    )
    _write(
        root / "config" / "signal_classes.csv",
        "signal_code,class,vertical_scope,promotion_critical\ncloud_connected,primary,all,false\n",
    )
    _write(
        root / "config" / "account_profiles.csv",
        "domain,relationship_stage,vertical_tag,is_self,exclude_from_crm\nzop.dev,customer,internal,1,1\n",
    )
    _write(root / "config" / "discovery_blocklist.csv", "domain,reason\nzop.dev,self\n")
    _write(
        root / "data" / "raw" / "first_party_events.csv",
        "domain,company_name,product,signal_code,source,evidence_url,evidence_text,confidence,observed_at\n"
        "acme.example,Acme,shared,cloud_connected,first_party_csv,https://acme.example/connect,connected cloud,0.95,2026-02-16T00:00:00Z\n"
        "zop.dev,Zopdev,shared,cloud_connected,first_party_csv,https://zop.dev/connect,connected cloud,0.95,2026-02-16T00:00:00Z\n",
    )
    monkeypatch.setenv("SIGNALS_PROJECT_ROOT", str(root))

    runner = CliRunner()
    result = runner.invoke(app, ["run-daily", "--date", "2026-02-16"])
    assert result.exit_code == 0

    queue_rows = load_csv_rows(root / "data" / "out" / "review_queue_20260216.csv")
    assert len(queue_rows) == 1
    assert queue_rows[0]["domain"] == "acme.example"


def test_daily_scores_include_zero_rows_for_unobserved_accounts(tmp_path, monkeypatch):
    root = tmp_path / "signals"
    _bootstrap_fixture(root)
    _write(
        root / "config" / "seed_accounts.csv",
        "company_name,domain,source_type\nAcme,acme.example,seed\nBeta,beta.example,seed\n",
    )
    _write(
        root / "data" / "raw" / "first_party_events.csv",
        "domain,company_name,product,signal_code,source,evidence_url,evidence_text,confidence,observed_at\n"
        "acme.example,Acme,zopdev,cloud_connected,first_party_csv,https://app.example/connect,connected cloud,0.95,2026-02-16T00:00:00Z\n",
    )
    monkeypatch.setenv("SIGNALS_PROJECT_ROOT", str(root))

    runner = CliRunner()
    result = runner.invoke(app, ["run-daily", "--date", "2026-02-16"])
    assert result.exit_code == 0

    score_rows = load_csv_rows(root / "data" / "out" / "daily_scores_20260216.csv")
    beta_rows = [row for row in score_rows if row.get("domain") == "beta.example"]
    assert len(beta_rows) == 3
    assert all(float(row["score"]) == 0.0 for row in beta_rows)
    assert all(row["tier"] == "low" for row in beta_rows)


def test_source_execution_policy_can_disable_collector(tmp_path, monkeypatch):
    root = tmp_path / "signals"
    _bootstrap_fixture(root)
    _write(
        root / "config" / "source_execution_policy.csv",
        "source,max_parallel_workers,requests_per_second,timeout_seconds,retry_attempts,backoff_seconds,batch_size,enabled\n"
        "first_party_csv,4,5.0,10,1,1,500,false\n",
    )
    monkeypatch.setenv("SIGNALS_PROJECT_ROOT", str(root))

    runner = CliRunner()
    result = runner.invoke(app, ["ingest", "--all"])

    assert result.exit_code == 0
    assert "collector=first_party seen=0 inserted=0" in result.stdout


def test_run_daily_skips_when_lock_is_held(tmp_path, monkeypatch):
    root = tmp_path / "signals"
    _bootstrap_fixture(root)
    monkeypatch.setenv("SIGNALS_PROJECT_ROOT", str(root))

    conn = db.get_connection()
    db.init_db(conn)
    assert db.try_advisory_lock(conn, lock_name="signals:run-daily", owner_id="test-owner") is True

    runner = CliRunner()
    result = runner.invoke(app, ["run-daily", "--date", "2026-02-16"])
    assert result.exit_code == 0
    assert "status=skipped reason=lock_busy" in result.stdout

    assert db.release_advisory_lock(conn, lock_name="signals:run-daily", owner_id="test-owner") is True
    conn.close()


def test_retry_failures_processes_due_task(tmp_path, monkeypatch):
    root = tmp_path / "signals"
    _bootstrap_fixture(root)
    monkeypatch.setenv("SIGNALS_PROJECT_ROOT", str(root))

    conn = db.get_connection()
    db.init_db(conn)
    _ = db.enqueue_retry_task(
        conn,
        task_type="ingest_cycle",
        payload_json=json.dumps({"run_date": "2026-02-16"}, ensure_ascii=True),
        due_at="2026-02-16T00:00:00+00:00",
        max_attempts=3,
    )
    conn.close()

    runner = CliRunner()
    result = runner.invoke(app, ["retry-failures", "--limit", "10"])
    assert result.exit_code == 0
    assert "completed=1" in result.stdout


def test_ops_metrics_command_writes_output(tmp_path, monkeypatch):
    root = tmp_path / "signals"
    _bootstrap_fixture(root)
    monkeypatch.setenv("SIGNALS_PROJECT_ROOT", str(root))

    runner = CliRunner()
    daily_result = runner.invoke(app, ["run-daily", "--date", "2026-02-16"])
    assert daily_result.exit_code == 0

    metrics_result = runner.invoke(app, ["ops-metrics", "--date", "2026-02-16"])
    assert metrics_result.exit_code == 0
    assert "ops_metrics_rows=" in metrics_result.stdout
    assert (root / "data" / "out" / "ops_metrics_20260216.csv").exists()


def test_alert_test_command_writes_local_log(tmp_path, monkeypatch):
    root = tmp_path / "signals"
    _bootstrap_fixture(root)
    monkeypatch.setenv("SIGNALS_PROJECT_ROOT", str(root))

    runner = CliRunner()
    result = runner.invoke(app, ["alert-test", "--title", "test-alert", "--body", "hello"])
    assert result.exit_code == 0
    assert "local_log" in result.stdout
    assert (root / "data" / "out" / "alerts.log").exists()


def test_backfill_run_daily_command_single_day(tmp_path, monkeypatch):
    root = tmp_path / "signals"
    _bootstrap_fixture(root)
    monkeypatch.setenv("SIGNALS_PROJECT_ROOT", str(root))

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "backfill-run-daily",
            "--start-date",
            "2026-02-16",
            "--end-date",
            "2026-02-16",
        ],
    )
    assert result.exit_code == 0
    assert "succeeded=1" in result.stdout


def test_eval_output_command_reports_quality_status(tmp_path, monkeypatch):
    root = tmp_path / "signals"
    _bootstrap_fixture(root)
    _write(root / "config" / "thresholds.csv", "key,value\ntier_1,95\ntier_2,90\ntier_3,90\ntier_4,0\n")
    _write(
        root / "config" / "icp_reference_accounts.csv",
        "company_name,domain,relationship_stage,notes\nAcme,acme.example,customer,\n",
    )
    monkeypatch.setenv("SIGNALS_PROJECT_ROOT", str(root))

    runner = CliRunner()
    daily_result = runner.invoke(app, ["run-daily", "--date", "2026-02-16"])
    assert daily_result.exit_code == 0

    result = runner.invoke(app, ["eval-output", "--date", "2026-02-16"])
    assert result.exit_code == 0
    assert "quality_passed=0" in result.stdout
    assert "failed_checks=" in result.stdout


def test_self_improve_output_command_updates_thresholds(tmp_path, monkeypatch):
    root = tmp_path / "signals"
    _bootstrap_fixture(root)
    _write(root / "config" / "thresholds.csv", "key,value\ntier_1,95\ntier_2,90\ntier_3,90\ntier_4,0\n")
    _write(
        root / "config" / "icp_reference_accounts.csv",
        "company_name,domain,relationship_stage,notes\nAcme,acme.example,customer,\n",
    )
    monkeypatch.setenv("SIGNALS_PROJECT_ROOT", str(root))

    runner = CliRunner()
    daily_result = runner.invoke(app, ["run-daily", "--date", "2026-02-16"])
    assert daily_result.exit_code == 0

    result = runner.invoke(
        app,
        [
            "self-improve-output",
            "--date",
            "2026-02-16",
            "--max-iterations",
            "4",
            "--write",
        ],
    )
    assert result.exit_code == 0
    assert "quality_passed=1" in result.stdout
    assert "written=1" in result.stdout

    threshold_rows = load_csv_rows(root / "config" / "thresholds.csv")
    threshold_map = {row["key"]: float(row["value"]) for row in threshold_rows}
    assert threshold_map["tier_1"] < 95.0
    assert threshold_map["tier_2"] < 90.0
