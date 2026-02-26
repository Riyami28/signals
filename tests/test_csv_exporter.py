"""Tests for src/export/csv_exporter.py — CSV output format, helpers, filtering."""

from __future__ import annotations

import csv
import json
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.export.csv_exporter import (
    _enrichment_field,
    _extract_starters_from_profile,
    _format_delta,
    _iso_date,
    _parse_reasons,
    date_suffix,
    export_daily_scores,
    export_ops_metrics,
    export_promotion_readiness,
    export_review_queue,
    export_source_quality,
    output_paths,
)

# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


class TestDateSuffix:
    def test_formats_correctly(self):
        assert date_suffix(date(2026, 2, 25)) == "20260225"

    def test_single_digit_month_day(self):
        assert date_suffix(date(2026, 1, 5)) == "20260105"


class TestOutputPaths:
    def test_all_keys_present(self, tmp_path):
        paths = output_paths(tmp_path, date(2026, 2, 25))
        assert "review_queue" in paths
        assert "daily_scores" in paths
        assert "source_quality" in paths
        assert "promotion_readiness" in paths
        assert "ops_metrics" in paths

    def test_suffix_in_filenames(self, tmp_path):
        paths = output_paths(tmp_path, date(2026, 2, 25))
        for key, path in paths.items():
            assert "20260225" in str(path)


class TestParseReasons:
    def test_valid_json_list(self):
        data = json.dumps([{"signal_code": "test", "score": 10}])
        result = _parse_reasons(data)
        assert len(result) == 1
        assert result[0]["signal_code"] == "test"

    def test_empty_string(self):
        assert _parse_reasons("") == []

    def test_invalid_json(self):
        assert _parse_reasons("not json") == []

    def test_non_list_json(self):
        assert _parse_reasons('{"key": "value"}') == []

    def test_filters_non_dict_items(self):
        data = json.dumps([{"signal_code": "a"}, "string_item", 42])
        result = _parse_reasons(data)
        assert len(result) == 1


class TestEnrichmentField:
    def test_extracts_string_field(self):
        enrichment = {"industry": "SaaS", "industry_confidence": 0.9}
        assert _enrichment_field(enrichment, "industry") == "SaaS"

    def test_low_confidence_returns_empty(self):
        enrichment = {"industry": "SaaS", "industry_confidence": 0.3}
        assert _enrichment_field(enrichment, "industry") == ""

    def test_missing_confidence_defaults_high(self):
        enrichment = {"industry": "SaaS"}
        assert _enrichment_field(enrichment, "industry") == "SaaS"

    def test_none_value_returns_empty(self):
        enrichment = {"industry": None}
        assert _enrichment_field(enrichment, "industry") == ""

    def test_list_value_joined(self):
        enrichment = {"tags": ["cloud", "devops"], "tags_confidence": 0.9}
        assert _enrichment_field(enrichment, "tags") == "cloud, devops"

    def test_missing_field_returns_empty(self):
        assert _enrichment_field({}, "missing") == ""

    def test_invalid_confidence_defaults_high(self):
        enrichment = {"industry": "SaaS", "industry_confidence": "bad"}
        assert _enrichment_field(enrichment, "industry") == "SaaS"


class TestFormatDelta:
    def test_positive(self):
        assert _format_delta(5.23) == "+5.2"

    def test_negative(self):
        assert _format_delta(-3.14) == "-3.1"

    def test_zero(self):
        assert _format_delta(0) == "+0.0"

    def test_none(self):
        assert _format_delta(None) == "+0.0"

    def test_invalid(self):
        assert _format_delta("bad") == ""


class TestExtractStartersFromProfile:
    def test_extracts_starters(self):
        profile = "## Overview\nSome text\n## Conversation Starters\n- Ask about cloud\n- Discuss DevOps\n## Next"
        result = _extract_starters_from_profile(profile)
        assert "Ask about cloud" in result
        assert "Discuss DevOps" in result

    def test_empty_profile(self):
        assert _extract_starters_from_profile("") == ""

    def test_no_starters_section(self):
        assert _extract_starters_from_profile("## Overview\nJust overview") == ""

    def test_starters_at_end(self):
        profile = "## Conversation Starters\n- Topic one\n- Topic two"
        result = _extract_starters_from_profile(profile)
        assert "Topic one" in result
        assert "Topic two" in result

    def test_bullet_styles(self):
        profile = "## Conversation Starters\n* Star bullet\n- Dash bullet"
        result = _extract_starters_from_profile(profile)
        assert "Star bullet" in result
        assert "Dash bullet" in result


class TestIsoDate:
    def test_full_timestamp(self):
        assert _iso_date("2026-02-25T12:30:00Z") == "2026-02-25"

    def test_date_only(self):
        assert _iso_date("2026-02-25") == "2026-02-25"

    def test_empty(self):
        assert _iso_date("") == ""

    def test_none(self):
        assert _iso_date(None) == ""

    def test_short_string(self):
        assert _iso_date("2026") == "2026"


# ---------------------------------------------------------------------------
# Export functions (mock DB)
# ---------------------------------------------------------------------------


class TestExportDailyScores:
    @patch("src.export.csv_exporter.db.fetch_scores_for_run")
    def test_writes_csv(self, mock_fetch, tmp_path):
        mock_fetch.return_value = [
            {
                "run_date": "2026-02-25",
                "account_id": "a1",
                "company_name": "Acme",
                "domain": "acme.com",
                "product": "zopdev",
                "score": 25.0,
                "tier": "high",
                "delta_7d": 2.0,
                "velocity_7d": 1.0,
                "velocity_14d": 0.5,
                "velocity_30d": 0.3,
                "velocity_category": "accelerating",
                "top_reasons_json": "[]",
                "confidence_band": "high",
                "dimension_confidence_json": "{}",
            }
        ]
        out = tmp_path / "daily_scores.csv"
        count = export_daily_scores(MagicMock(), "run1", out)
        assert count == 1
        assert out.exists()
        with out.open() as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            assert len(rows) == 1
            assert rows[0]["company_name"] == "Acme"
            assert rows[0]["tier"] == "high"

    @patch("src.export.csv_exporter.db.fetch_scores_for_run")
    def test_empty_run(self, mock_fetch, tmp_path):
        mock_fetch.return_value = []
        out = tmp_path / "daily_scores.csv"
        count = export_daily_scores(MagicMock(), "run1", out)
        assert count == 0


class TestExportReviewQueue:
    @patch("src.export.csv_exporter.db.fetch_scores_for_run")
    def test_filters_low_tier(self, mock_fetch, tmp_path):
        mock_fetch.return_value = [
            {
                "run_date": "2026-02-25",
                "account_id": "a1",
                "company_name": "Low Co",
                "domain": "low.com",
                "product": "zopdev",
                "score": 5.0,
                "tier": "low",
                "top_reasons_json": "[]",
                "velocity_7d": 0,
                "velocity_14d": 0,
                "velocity_30d": 0,
                "velocity_category": "stable",
                "confidence_band": "low",
            }
        ]
        out = tmp_path / "review_queue.csv"
        count = export_review_queue(MagicMock(), "run1", out)
        assert count == 0

    @patch("src.export.csv_exporter.db.fetch_scores_for_run")
    def test_excludes_domains(self, mock_fetch, tmp_path):
        mock_fetch.return_value = [
            {
                "run_date": "2026-02-25",
                "account_id": "a1",
                "company_name": "Excluded",
                "domain": "excluded.com",
                "product": "zopdev",
                "score": 25.0,
                "tier": "high",
                "top_reasons_json": "[]",
                "velocity_7d": 0,
                "velocity_14d": 0,
                "velocity_30d": 0,
                "velocity_category": "stable",
                "confidence_band": "high",
            }
        ]
        out = tmp_path / "review_queue.csv"
        count = export_review_queue(MagicMock(), "run1", out, excluded_domains={"excluded.com"})
        assert count == 0

    @patch("src.export.csv_exporter.db.fetch_scores_for_run")
    def test_deduplicates_accounts(self, mock_fetch, tmp_path):
        mock_fetch.return_value = [
            {
                "run_date": "2026-02-25",
                "account_id": "a1",
                "company_name": "Acme",
                "domain": "acme.com",
                "product": "zopdev",
                "score": 25.0,
                "tier": "high",
                "top_reasons_json": "[]",
                "velocity_7d": 0,
                "velocity_14d": 0,
                "velocity_30d": 0,
                "velocity_category": "stable",
                "confidence_band": "high",
            },
            {
                "run_date": "2026-02-25",
                "account_id": "a1",
                "company_name": "Acme",
                "domain": "acme.com",
                "product": "zopday",
                "score": 15.0,
                "tier": "medium",
                "top_reasons_json": "[]",
                "velocity_7d": 0,
                "velocity_14d": 0,
                "velocity_30d": 0,
                "velocity_category": "stable",
                "confidence_band": "medium",
            },
        ]
        out = tmp_path / "review_queue.csv"
        count = export_review_queue(MagicMock(), "run1", out)
        assert count == 1  # deduped to best


class TestExportSourceQuality:
    @patch("src.export.csv_exporter.db.fetch_source_metrics")
    def test_writes_csv(self, mock_fetch, tmp_path):
        mock_fetch.return_value = [
            {"run_date": "2026-02-25", "source": "jobs_greenhouse", "approved_rate": 0.8, "sample_size": 10}
        ]
        out = tmp_path / "source_quality.csv"
        count = export_source_quality(MagicMock(), "2026-02-25", out)
        assert count == 1


class TestExportPromotionReadiness:
    def test_writes_rows(self, tmp_path):
        rows = [
            {
                "run_date": "2026-02-25",
                "window": "7d",
                "approved_rate": 0.75,
                "sample_size": 20,
                "meets_rate": True,
                "meets_sample": True,
                "ready_for_promotion": True,
            }
        ]
        out = tmp_path / "promotion.csv"
        count = export_promotion_readiness(rows, out)
        assert count == 1
        assert out.exists()


class TestExportOpsMetrics:
    @patch("src.export.csv_exporter.db.fetch_ops_metrics")
    def test_writes_csv(self, mock_fetch, tmp_path):
        mock_fetch.return_value = [
            {
                "run_date": "2026-02-25",
                "recorded_at": "2026-02-25T12:00:00Z",
                "metric": "ingest_lag",
                "value": 5.0,
                "meta_json": "{}",
            }
        ]
        out = tmp_path / "ops.csv"
        count = export_ops_metrics(MagicMock(), "2026-02-25", out)
        assert count == 1
