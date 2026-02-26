"""Tests for src/settings.py — env var parsing, defaults, path derivation, DSN construction."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from src.settings import Settings, _auto_live_workers, load_settings


class TestAutoLiveWorkers:
    def test_high_interval_gets_multiplier_1(self):
        result = _auto_live_workers(2000)
        assert result >= 4
        assert result <= 128

    def test_medium_interval_gets_multiplier_2(self):
        result = _auto_live_workers(1000)
        high = _auto_live_workers(2000)
        assert result >= high

    def test_low_interval_gets_multiplier_3(self):
        result = _auto_live_workers(100)
        medium = _auto_live_workers(1000)
        assert result >= medium

    def test_minimum_is_4(self):
        result = _auto_live_workers(5000)
        assert result >= 4


class TestSettingsDefaults:
    def test_default_pg_components(self, monkeypatch, tmp_path):
        monkeypatch.delenv("SIGNALS_PG_DSN", raising=False)
        monkeypatch.delenv("SIGNALS_PG_HOST", raising=False)
        monkeypatch.delenv("SIGNALS_PG_PORT", raising=False)
        s = Settings(project_root=tmp_path, pg_dsn="")
        assert "postgresql://" in s.pg_dsn
        assert "signals" in s.pg_dsn

    def test_explicit_pg_dsn_not_overridden(self, monkeypatch, tmp_path):
        explicit = "postgresql://user:pass@host:5432/mydb"
        s = Settings(project_root=tmp_path, pg_dsn=explicit)
        assert s.pg_dsn == explicit

    def test_path_derivation_from_project_root(self, monkeypatch, tmp_path):
        s = Settings(project_root=tmp_path)
        assert s.config_dir == tmp_path / "config"
        assert s.data_dir == tmp_path / "data"
        assert s.raw_dir == tmp_path / "data" / "raw"
        assert s.out_dir == tmp_path / "data" / "out"

    def test_config_file_paths_derived(self, monkeypatch, tmp_path):
        s = Settings(project_root=tmp_path)
        assert s.seed_accounts_path == tmp_path / "config" / "seed_accounts.csv"
        assert s.signal_registry_path == tmp_path / "config" / "signal_registry.csv"
        assert s.thresholds_path == tmp_path / "config" / "thresholds.csv"
        assert s.keyword_lexicon_path == tmp_path / "config" / "keyword_lexicon.csv"
        assert s.promotion_policy_path == tmp_path / "config" / "promotion_policy.csv"

    def test_live_workers_auto_calculated_when_zero(self, monkeypatch, tmp_path):
        s = Settings(project_root=tmp_path, live_workers_per_source=0)
        assert s.live_workers_per_source >= 4

    def test_live_workers_explicit_preserved(self, monkeypatch, tmp_path):
        s = Settings(project_root=tmp_path, live_workers_per_source=16)
        assert s.live_workers_per_source == 16

    def test_default_http_settings(self, tmp_path):
        s = Settings(project_root=tmp_path)
        assert s.http_timeout_seconds == 10
        assert s.http_user_agent == "zopdev-signals/0.1"
        assert s.respect_robots_txt is True
        assert s.min_domain_request_interval_ms == 2000

    def test_default_llm_settings(self, tmp_path):
        s = Settings(project_root=tmp_path)
        assert s.claude_model == "claude-sonnet-4-5"
        assert s.llm_provider == "minimax"
        assert s.research_max_accounts == 20

    def test_google_sheet_id_empty_becomes_none(self, tmp_path):
        s = Settings(project_root=tmp_path, google_sheet_id="  ")
        assert s.google_sheet_id is None

    def test_google_sheet_id_valid_preserved(self, monkeypatch, tmp_path):
        monkeypatch.setenv("GOOGLE_SHEETS_SPREADSHEET_ID", "abc123")
        s = Settings(project_root=tmp_path)
        assert s.google_sheet_id == "abc123"

    def test_dsn_construction_from_components(self, tmp_path):
        s = Settings(
            project_root=tmp_path,
            pg_dsn="",
            pg_host="myhost",
            pg_port="5433",
            pg_user="myuser",
            pg_password="mypass",
            pg_db="mydb",
        )
        assert s.pg_dsn == "postgresql://myuser:mypass@myhost:5433/mydb"


class TestLoadSettings:
    def test_load_settings_with_project_root(self, tmp_path):
        s = load_settings(project_root=tmp_path)
        assert s.project_root == tmp_path
        assert s.config_dir == tmp_path / "config"

    def test_load_settings_default(self):
        s = load_settings()
        assert s.project_root.exists()


class TestSettingsValidation:
    def test_http_timeout_minimum(self, tmp_path):
        with pytest.raises(Exception):
            Settings(project_root=tmp_path, http_timeout_seconds=0)

    def test_live_max_accounts_minimum(self, tmp_path):
        with pytest.raises(Exception):
            Settings(project_root=tmp_path, live_max_accounts=0)

    def test_enable_live_crawl_default_false(self, tmp_path):
        s = Settings(project_root=tmp_path)
        assert s.enable_live_crawl is False

    def test_retry_attempt_limit_default(self, tmp_path):
        s = Settings(project_root=tmp_path)
        assert s.retry_attempt_limit == 3

    def test_stage_timeout_default(self, tmp_path):
        s = Settings(project_root=tmp_path)
        assert s.stage_timeout_seconds == 1800
