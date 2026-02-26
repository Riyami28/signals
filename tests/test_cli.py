from typer.testing import CliRunner

from src import cli


def test_start_uses_monitor_script_with_live_enabled(tmp_path, monkeypatch):
    fake_monitor = tmp_path / "run_daily_live_monitor.sh"
    fake_monitor.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
    fake_monitor.chmod(0o755)

    captured: dict[str, object] = {}

    def fake_run_subprocess(cmd: list[str], env: dict[str, str]) -> int:
        captured["cmd"] = cmd
        captured["env"] = env
        return 0

    monkeypatch.setattr(cli, "MONITOR_SCRIPT", fake_monitor)
    monkeypatch.setattr(cli, "_run_subprocess", fake_run_subprocess)

    runner = CliRunner()
    result = runner.invoke(
        cli.app,
        [
            "start",
            "--date",
            "2026-02-22",
            "--live-max-accounts",
            "12",
            "--poll-interval-seconds",
            "3",
        ],
    )

    assert result.exit_code == 0
    assert captured["cmd"] == [str(fake_monitor), "2026-02-22", "12", "900", "3", "auto"]
    env = captured["env"]
    assert isinstance(env, dict)
    assert env["SIGNALS_ENABLE_LIVE_CRAWL"] == "1"
    assert env["SIGNALS_VERBOSE_PROGRESS"] == "1"


def test_run_live_sets_env_and_args(monkeypatch):
    captured: dict[str, object] = {}

    def fake_run_subprocess(cmd: list[str], env: dict[str, str]) -> int:
        captured["cmd"] = cmd
        captured["env"] = env
        return 0

    monkeypatch.setattr(cli, "_python_bin", lambda: "python-test")
    monkeypatch.setattr(cli, "_run_subprocess", fake_run_subprocess)

    runner = CliRunner()
    result = runner.invoke(
        cli.app,
        [
            "run",
            "--date",
            "2026-02-22",
            "--live",
            "--workers-per-source",
            "16",
            "--live-max-accounts",
            "200",
            "--stage-timeout-seconds",
            "1200",
            "--fast-fail-network",
        ],
    )

    assert result.exit_code == 0
    assert captured["cmd"] == [
        "python-test",
        "-m",
        "src.main",
        "run-daily",
        "--date",
        "2026-02-22",
        "--live-max-accounts",
        "200",
        "--stage-timeout-seconds",
        "1200",
    ]
    env = captured["env"]
    assert isinstance(env, dict)
    assert env["SIGNALS_ENABLE_LIVE_CRAWL"] == "1"
    assert env["SIGNALS_VERBOSE_PROGRESS"] == "1"
    assert env["SIGNALS_LIVE_WORKERS_PER_SOURCE"] == "16"
    assert env["SIGNALS_HTTP_TIMEOUT_SECONDS"] == "2"
    assert env["SIGNALS_MIN_DOMAIN_REQUEST_INTERVAL_MS"] == "0"
    assert env["SIGNALS_RESPECT_ROBOTS_TXT"] == "0"


def test_ui_runs_main_serve_local_ui(monkeypatch):
    captured: dict[str, object] = {}

    def fake_run_subprocess(cmd: list[str], env: dict[str, str]) -> int:
        captured["cmd"] = cmd
        captured["env"] = env
        return 0

    monkeypatch.setattr(cli, "_python_bin", lambda: "python-test")
    monkeypatch.setattr(cli, "_run_subprocess", fake_run_subprocess)

    runner = CliRunner()
    result = runner.invoke(cli.app, ["ui", "--host", "0.0.0.0", "--port", "9999", "--log-level", "warning"])

    assert result.exit_code == 0
    assert captured["cmd"] == [
        "python-test",
        "-m",
        "src.main",
        "serve-local-ui",
        "--host",
        "0.0.0.0",
        "--port",
        "9999",
        "--log-level",
        "warning",
    ]
    env = captured["env"]
    assert isinstance(env, dict)


def test_company_watch_scopes_target_domain(tmp_path, monkeypatch):
    fake_monitor = tmp_path / "run_daily_live_monitor.sh"
    fake_monitor.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
    fake_monitor.chmod(0o755)

    captured: dict[str, object] = {}

    def fake_run_subprocess(cmd: list[str], env: dict[str, str]) -> int:
        captured["cmd"] = cmd
        captured["env"] = env
        return 0

    monkeypatch.setattr(cli, "MONITOR_SCRIPT", fake_monitor)
    monkeypatch.setattr(cli, "_run_subprocess", fake_run_subprocess)

    runner = CliRunner()
    result = runner.invoke(
        cli.app, ["company", "ConagraBrands.com", "--date", "2026-02-22", "--workers-per-source", "3"]
    )

    assert result.exit_code == 0
    assert captured["cmd"] == [str(fake_monitor), "2026-02-22", "1", "900", "1", "3"]
    env = captured["env"]
    assert isinstance(env, dict)
    assert env["SIGNALS_ENABLE_LIVE_CRAWL"] == "1"
    assert env["SIGNALS_LIVE_TARGET_DOMAINS"] == "conagrabrands.com"
    assert env["SIGNALS_LIVE_WORKERS_PER_SOURCE"] == "3"


def test_hunt_runs_ingest_score_then_conviction(monkeypatch):
    calls: list[tuple[list[str], dict[str, str]]] = []

    def fake_run_subprocess(cmd: list[str], env: dict[str, str]) -> int:
        calls.append((cmd, env))
        return 0

    monkeypatch.setattr(cli, "_python_bin", lambda: "python-test")
    monkeypatch.setattr(cli, "_run_subprocess", fake_run_subprocess)

    runner = CliRunner()
    result = runner.invoke(
        cli.app,
        ["hunt", "ConagraBrands.com", "--date", "2026-02-22", "--top", "3", "--workers-per-source", "5"],
    )

    assert result.exit_code == 0
    assert len(calls) == 3

    ingest_cmd, ingest_env = calls[0]
    assert ingest_cmd == ["python-test", "-m", "src.main", "ingest", "--all"]
    assert ingest_env["SIGNALS_ENABLE_LIVE_CRAWL"] == "1"
    assert ingest_env["SIGNALS_LIVE_MAX_ACCOUNTS"] == "1"
    assert ingest_env["SIGNALS_LIVE_TARGET_DOMAINS"] == "conagrabrands.com"
    assert ingest_env["SIGNALS_LIVE_WORKERS_PER_SOURCE"] == "5"

    score_cmd, _ = calls[1]
    assert score_cmd == ["python-test", "-m", "src.main", "score", "--date", "2026-02-22"]

    conviction_cmd, _ = calls[2]
    assert conviction_cmd == [
        "python-test",
        "-m",
        "src.cli",
        "conviction",
        "--date",
        "2026-02-22",
        "--domain",
        "conagrabrands.com",
        "--all",
        "--min-tier",
        "low",
        "--top",
        "3",
        "--write-csv",
    ]
