"""Tests for src/utils.py — all utility functions."""

from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

from src.utils import (
    classify_text,
    load_csv_rows,
    normalize_domain,
    parse_date,
    stable_hash,
)


class TestUtcNowIso:
    def test_returns_iso_format(self):
        result = utc_now_iso()
        assert "T" in result
        assert result.endswith("+00:00")

    def test_no_microseconds(self):
        result = utc_now_iso()
        assert "." not in result


# ---------------------------------------------------------------------------
# parse_date
# ---------------------------------------------------------------------------


class TestParseDate:
    def test_with_value(self):
        assert parse_date("2026-02-16", "America/Los_Angeles") == date(2026, 2, 16)

    def test_none_returns_today(self):
        result = parse_date(None, "UTC")
        assert isinstance(result, date)

    def test_empty_string_returns_today(self):
        result = parse_date("", "UTC")
        assert isinstance(result, date)

    def test_invalid_timezone_falls_back_to_utc(self):
        result = parse_date(None, "Invalid/Timezone")
        assert isinstance(result, date)


# ---------------------------------------------------------------------------
# parse_datetime
# ---------------------------------------------------------------------------


class TestParseDatetime:
    def test_with_value(self):
        result = parse_datetime("2026-02-16T12:00:00Z")
        assert result.year == 2026
        assert result.month == 2
        assert result.tzinfo is not None

    def test_none_returns_now(self):
        result = parse_datetime(None)
        assert isinstance(result, datetime)
        assert result.tzinfo is not None

    def test_empty_returns_now(self):
        result = parse_datetime("")
        assert isinstance(result, datetime)

    def test_naive_datetime_gets_utc(self):
        result = parse_datetime("2026-02-16T12:00:00")
        assert result.tzinfo == timezone.utc

    def test_non_utc_converted(self):
        result = parse_datetime("2026-02-16T12:00:00-05:00")
        assert result.tzinfo == timezone.utc


# ---------------------------------------------------------------------------
# normalize_domain
# ---------------------------------------------------------------------------


class TestNormalizeDomain:
    def test_strips_protocol_and_path(self):
        assert normalize_domain("https://www.Example.com/path") == "example.com"

    def test_strips_http(self):
        assert normalize_domain("http://example.com") == "example.com"

    def test_strips_www(self):
        assert normalize_domain("www.example.com") == "example.com"

    def test_lowercase(self):
        assert normalize_domain("EXAMPLE.COM") == "example.com"

    def test_empty(self):
        assert normalize_domain("") == ""

    def test_none(self):
        assert normalize_domain(None) == ""

    def test_strips_path(self):
        assert normalize_domain("example.com/page/1") == "example.com"


# ---------------------------------------------------------------------------
# stable_hash
# ---------------------------------------------------------------------------


class TestStableHash:
    def test_deterministic(self):
        payload = {"b": 2, "a": 1}
        assert stable_hash(payload, prefix="x") == stable_hash(payload, prefix="x")

    def test_with_prefix(self):
        result = stable_hash({"key": "val"}, prefix="obs")
        assert result.startswith("obs_")

    def test_without_prefix(self):
        result = stable_hash({"key": "val"})
        assert "_" not in result[:4]

    def test_custom_length(self):
        result = stable_hash({"key": "val"}, length=8)
        assert len(result) == 8

    def test_different_payloads_differ(self):
        a = stable_hash({"a": 1})
        b = stable_hash({"a": 2})
        assert a != b


# ---------------------------------------------------------------------------
# ensure_project_directories
# ---------------------------------------------------------------------------


class TestEnsureProjectDirectories:
    def test_creates_directories(self, tmp_path):
        dirs = [tmp_path / "a" / "b", tmp_path / "c"]
        ensure_project_directories(dirs)
        assert (tmp_path / "a" / "b").is_dir()
        assert (tmp_path / "c").is_dir()

    def test_idempotent(self, tmp_path):
        d = tmp_path / "test"
        ensure_project_directories([d])
        ensure_project_directories([d])
        assert d.is_dir()


# ---------------------------------------------------------------------------
# load_csv_rows
# ---------------------------------------------------------------------------


class TestLoadCsvRows:
    def test_basic(self, tmp_path):
        path = tmp_path / "test.csv"
        path.write_text("a,b\n1,2\n3,4\n", encoding="utf-8")
        rows = load_csv_rows(path)
        assert len(rows) == 2
        assert rows[0] == {"a": "1", "b": "2"}

    def test_missing_file(self, tmp_path):
        rows = load_csv_rows(tmp_path / "missing.csv")
        assert rows == []

    def test_extra_columns_ignored(self, tmp_path):
        path = tmp_path / "rows.csv"
        path.write_text("a,b\n1,2,3\n", encoding="utf-8")
        rows = load_csv_rows(path)
        assert rows == [{"a": "1", "b": "2"}]

    def test_empty_csv(self, tmp_path):
        path = tmp_path / "empty.csv"
        path.write_text("a,b\n", encoding="utf-8")
        rows = load_csv_rows(path)
        assert rows == []

    def test_strips_whitespace(self, tmp_path):
        path = tmp_path / "ws.csv"
        path.write_text("a,b\n  hello ,  world  \n", encoding="utf-8")
        rows = load_csv_rows(path)
        assert rows[0]["a"] == "hello"
        assert rows[0]["b"] == "world"


# ---------------------------------------------------------------------------
# load_account_source_handles
# ---------------------------------------------------------------------------


class TestLoadAccountSourceHandles:
    def test_loads_by_domain(self, tmp_path):
        path = tmp_path / "handles.csv"
        path.write_text("domain,greenhouse_slug\nacme.com,acme\nbeta.io,beta\n", encoding="utf-8")
        handles = load_account_source_handles(path)
        assert "acme.com" in handles
        assert handles["acme.com"]["greenhouse_slug"] == "acme"

    def test_skips_empty_domain(self, tmp_path):
        path = tmp_path / "handles.csv"
        path.write_text("domain,slug\n,empty\nacme.com,acme\n", encoding="utf-8")
        handles = load_account_source_handles(path)
        assert "" not in handles
        assert "acme.com" in handles


# ---------------------------------------------------------------------------
# write_csv_rows
# ---------------------------------------------------------------------------


class TestWriteCsvRows:
    def test_writes_csv(self, tmp_path):
        path = tmp_path / "out.csv"
        rows = [{"a": "1", "b": "2"}, {"a": "3", "b": "4"}]
        write_csv_rows(path, rows, fieldnames=["a", "b"])
        assert path.exists()
        content = path.read_text()
        assert "a,b" in content
        assert "1,2" in content

    def test_creates_parent_dirs(self, tmp_path):
        path = tmp_path / "deep" / "nested" / "out.csv"
        write_csv_rows(path, [{"x": "1"}], fieldnames=["x"])
        assert path.exists()

    def test_missing_fields_default_empty(self, tmp_path):
        path = tmp_path / "out.csv"
        rows = [{"a": "1"}]
        write_csv_rows(path, rows, fieldnames=["a", "b"])
        content = path.read_text().strip().split("\n")
        assert content[1] == "1,"


# ---------------------------------------------------------------------------
# classify_text
# ---------------------------------------------------------------------------


class TestClassifyText:
    def test_matches_keyword(self):
        rows = [{"signal_code": "cost_reduction_mandate", "keyword": "cost transformation office", "confidence": "0.9"}]
        matches = classify_text("Board approved the cost transformation office plan.", rows)
        assert matches
        assert matches[0][0] == "cost_reduction_mandate"

    def test_no_partial_match_inside_word(self):
        rows = [{"signal_code": "erp_signal", "keyword": "erp", "confidence": "0.9"}]
        matches = classify_text("This describes a sharperpops migration.", rows)
        assert matches == []

    def test_case_insensitive(self):
        rows = [{"signal_code": "test", "keyword": "kubernetes", "confidence": "0.8"}]
        matches = classify_text("Running KUBERNETES in production", rows)
        assert len(matches) == 1

    def test_empty_text(self):
        rows = [{"signal_code": "test", "keyword": "k8s", "confidence": "0.8"}]
        assert classify_text("", rows) == []

    def test_empty_lexicon(self):
        assert classify_text("some text", []) == []

    def test_missing_keyword_skipped(self):
        rows = [{"signal_code": "test", "keyword": "", "confidence": "0.8"}]
        assert classify_text("some text", rows) == []

    def test_missing_signal_code_skipped(self):
        rows = [{"signal_code": "", "keyword": "test", "confidence": "0.8"}]
        assert classify_text("test phrase", rows) == []

    def test_invalid_confidence_defaults(self):
        rows = [{"signal_code": "test", "keyword": "cloud", "confidence": "bad"}]
        matches = classify_text("cloud migration", rows)
        assert matches[0][1] == 0.6

    def test_multiple_matches(self):
        rows = [
            {"signal_code": "sig1", "keyword": "devops", "confidence": "0.8"},
            {"signal_code": "sig2", "keyword": "cloud", "confidence": "0.7"},
        ]
        matches = classify_text("devops and cloud migration", rows)
        assert len(matches) == 2
