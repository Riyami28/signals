"""Tests for src/source_policy.py — CSV loading, defaults, validation."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.source_policy import SourceExecutionPolicy, _to_bool, _to_int, load_source_execution_policy

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class TestToBool:
    def test_true_values(self):
        for val in ("1", "true", "yes", "on"):
            assert _to_bool(val) is True

    def test_false_values(self):
        for val in ("0", "false", "no", "off"):
            assert _to_bool(val) is False

    def test_none_returns_default(self):
        assert _to_bool(None) is True
        assert _to_bool(None, default=False) is False


class TestToInt:
    def test_valid_int(self):
        assert _to_int("10", 1) == 10

    def test_float_string(self):
        assert _to_int("3.7", 1) == 3

    def test_empty_returns_default(self):
        assert _to_int("", 5) == 5

    def test_none_returns_default(self):
        assert _to_int(None, 5) == 5

    def test_invalid_returns_default(self):
        assert _to_int("abc", 5) == 5

    def test_minimum_is_1(self):
        assert _to_int("0", 5) == 1
        assert _to_int("-5", 5) == 1


# ---------------------------------------------------------------------------
# SourceExecutionPolicy
# ---------------------------------------------------------------------------


class TestSourceExecutionPolicy:
    def test_frozen_dataclass(self):
        policy = SourceExecutionPolicy(
            source="jobs_greenhouse",
            max_parallel_workers=4,
            requests_per_second=2.0,
            timeout_seconds=20,
            retry_attempts=3,
            backoff_seconds=2,
            batch_size=100,
            enabled=True,
        )
        assert policy.source == "jobs_greenhouse"
        assert policy.max_parallel_workers == 4
        with pytest.raises(AttributeError):
            policy.source = "other"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# load_source_execution_policy
# ---------------------------------------------------------------------------


class TestLoadSourceExecutionPolicy:
    def test_loads_from_csv(self, tmp_path):
        csv_path = tmp_path / "policy.csv"
        csv_path.write_text(
            "source,max_parallel_workers,requests_per_second,timeout_seconds,retry_attempts,backoff_seconds,batch_size,enabled\n"
            "jobs_greenhouse,4,2.0,30,3,5,50,true\n"
            "news_google,2,1.0,20,2,2,100,true\n"
            "disabled_source,1,0.5,10,1,1,10,false\n",
            encoding="utf-8",
        )
        policies = load_source_execution_policy(csv_path)
        assert "jobs_greenhouse" in policies
        assert "news_google" in policies
        assert "disabled_source" in policies

        jobs = policies["jobs_greenhouse"]
        assert jobs.max_parallel_workers == 4
        assert jobs.requests_per_second == 2.0
        assert jobs.timeout_seconds == 30
        assert jobs.enabled is True

        disabled = policies["disabled_source"]
        assert disabled.enabled is False

    def test_empty_csv(self, tmp_path):
        csv_path = tmp_path / "policy.csv"
        csv_path.write_text(
            "source,max_parallel_workers,requests_per_second,timeout_seconds,retry_attempts,backoff_seconds,batch_size,enabled\n",
            encoding="utf-8",
        )
        policies = load_source_execution_policy(csv_path)
        assert policies == {}

    def test_missing_file(self, tmp_path):
        csv_path = tmp_path / "nonexistent.csv"
        policies = load_source_execution_policy(csv_path)
        assert policies == {}

    def test_source_lowercased(self, tmp_path):
        csv_path = tmp_path / "policy.csv"
        csv_path.write_text(
            "source,max_parallel_workers,requests_per_second,timeout_seconds,retry_attempts,backoff_seconds,batch_size,enabled\n"
            "Jobs_Greenhouse,4,2.0,30,3,5,50,true\n",
            encoding="utf-8",
        )
        policies = load_source_execution_policy(csv_path)
        assert "jobs_greenhouse" in policies

    def test_invalid_rps_defaults_to_1(self, tmp_path):
        csv_path = tmp_path / "policy.csv"
        csv_path.write_text(
            "source,max_parallel_workers,requests_per_second,timeout_seconds,retry_attempts,backoff_seconds,batch_size,enabled\n"
            "test,4,invalid,30,3,5,50,true\n",
            encoding="utf-8",
        )
        policies = load_source_execution_policy(csv_path)
        assert policies["test"].requests_per_second == 1.0

    def test_skips_empty_source(self, tmp_path):
        csv_path = tmp_path / "policy.csv"
        csv_path.write_text(
            "source,max_parallel_workers,requests_per_second,timeout_seconds,retry_attempts,backoff_seconds,batch_size,enabled\n"
            ",4,2.0,30,3,5,50,true\n",
            encoding="utf-8",
        )
        policies = load_source_execution_policy(csv_path)
        assert policies == {}
