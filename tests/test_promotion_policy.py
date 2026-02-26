"""Tests for src/promotion_policy.py — eligibility logic, CSV parsing, defaults."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.promotion_policy import (
    PromotionPolicy,
    _parse_bands,
    _parse_confidence_bands,
    _to_bool,
    _to_float,
    default_promotion_policy,
    is_promotion_eligible,
    load_promotion_policy,
)

# ---------------------------------------------------------------------------
# default_promotion_policy
# ---------------------------------------------------------------------------


class TestDefaultPolicy:
    def test_returns_valid_policy(self):
        policy = default_promotion_policy()
        assert isinstance(policy, PromotionPolicy)
        assert "high" in policy.auto_push_bands
        assert "medium" in policy.manual_review_bands
        assert policy.require_strict_evidence_for_auto_push is True
        assert policy.min_auto_push_evidence_quality == 0.8
        assert policy.min_auto_push_relevance_score == 0.65


# ---------------------------------------------------------------------------
# is_promotion_eligible
# ---------------------------------------------------------------------------


class TestIsPromotionEligible:
    @pytest.fixture
    def policy(self):
        return default_promotion_policy()

    def test_high_confidence_high_tier_auto_push(self, policy):
        assert is_promotion_eligible("high", "high", policy) == "auto_push"

    def test_high_confidence_medium_tier_manual_review(self, policy):
        assert is_promotion_eligible("medium", "high", policy) == "manual_review"

    def test_medium_confidence_any_tier_manual_review(self, policy):
        assert is_promotion_eligible("high", "medium", policy) == "manual_review"

    def test_low_confidence_blocked(self, policy):
        assert is_promotion_eligible("high", "low", policy) == "blocked"

    def test_low_confidence_low_tier_blocked(self, policy):
        assert is_promotion_eligible("low", "low", policy) == "blocked"


# ---------------------------------------------------------------------------
# Helper parsers
# ---------------------------------------------------------------------------


class TestToBool:
    def test_true_values(self):
        for val in ("1", "true", "True", "yes", "on", " TRUE "):
            assert _to_bool(val, False) is True

    def test_false_values(self):
        for val in ("0", "false", "no", "off"):
            assert _to_bool(val, True) is False

    def test_none_returns_default(self):
        assert _to_bool(None, True) is True
        assert _to_bool(None, False) is False


class TestToFloat:
    def test_valid_float(self):
        assert _to_float("0.75", 0.0) == 0.75

    def test_clamps_high(self):
        assert _to_float("1.5", 0.0) == 1.0

    def test_clamps_low(self):
        assert _to_float("-0.5", 0.5) == 0.0

    def test_empty_returns_default(self):
        assert _to_float("", 0.8) == 0.8

    def test_none_returns_default(self):
        assert _to_float(None, 0.8) == 0.8

    def test_invalid_returns_default(self):
        assert _to_float("abc", 0.5) == 0.5


class TestParseBands:
    def test_valid_bands(self):
        result = _parse_bands("high|medium", {"high"})
        assert result == {"high", "medium"}

    def test_empty_returns_default(self):
        result = _parse_bands("", {"high"})
        assert result == {"high"}

    def test_none_returns_default(self):
        result = _parse_bands(None, {"high"})
        assert result == {"high"}

    def test_invalid_bands_returns_default(self):
        result = _parse_bands("invalid|bad", {"high"})
        assert result == {"high"}

    def test_mixed_valid_invalid(self):
        result = _parse_bands("high|invalid", {"medium"})
        assert result == {"high"}


class TestParseConfidenceBands:
    def test_valid_bands(self):
        result = _parse_confidence_bands("high|medium|low", {"high"})
        assert result == {"high", "medium", "low"}

    def test_empty_returns_default(self):
        result = _parse_confidence_bands("", {"high"})
        assert result == {"high"}


# ---------------------------------------------------------------------------
# load_promotion_policy from CSV
# ---------------------------------------------------------------------------


class TestLoadPromotionPolicy:
    def test_loads_from_csv(self, tmp_path):
        csv_path = tmp_path / "promotion_policy.csv"
        csv_path.write_text(
            "key,value\n"
            "auto_push_bands,high|medium\n"
            "manual_review_bands,explore\n"
            "require_strict_evidence_for_auto_push,false\n"
            "min_auto_push_evidence_quality,0.6\n"
            "min_auto_push_relevance_score,0.5\n"
            "confidence_auto_push_bands,high|medium\n"
            "confidence_manual_review_bands,low\n",
            encoding="utf-8",
        )
        policy = load_promotion_policy(csv_path)
        assert "high" in policy.auto_push_bands
        assert "medium" in policy.auto_push_bands
        assert policy.require_strict_evidence_for_auto_push is False
        assert policy.min_auto_push_evidence_quality == 0.6

    def test_empty_csv_returns_defaults(self, tmp_path):
        csv_path = tmp_path / "promotion_policy.csv"
        csv_path.write_text("key,value\n", encoding="utf-8")
        policy = load_promotion_policy(csv_path)
        defaults = default_promotion_policy()
        assert policy.auto_push_bands == defaults.auto_push_bands

    def test_missing_file_returns_defaults(self, tmp_path):
        csv_path = tmp_path / "nonexistent.csv"
        policy = load_promotion_policy(csv_path)
        defaults = default_promotion_policy()
        assert policy == defaults
