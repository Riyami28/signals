"""Tests for src/models.py — Pydantic model validation, defaults, and serialization."""

from __future__ import annotations

import json

import pytest
from pydantic import ValidationError

from src.models import (
    Account,
    AccountScore,
    ComponentScore,
    Reason,
    ReviewLabel,
    RunResult,
    SignalObservation,
)

# ---------------------------------------------------------------------------
# Account
# ---------------------------------------------------------------------------


class TestAccount:
    def test_valid_account(self):
        a = Account(account_id="a1", company_name="Acme", domain="acme.com")
        assert a.account_id == "a1"
        assert a.source_type == "seed"
        assert a.created_at  # auto-generated

    def test_source_type_discovered(self):
        a = Account(account_id="a2", company_name="Beta", domain="beta.io", source_type="discovered")
        assert a.source_type == "discovered"

    def test_invalid_source_type_rejected(self):
        with pytest.raises(ValidationError):
            Account(account_id="a3", company_name="Bad", domain="bad.com", source_type="unknown")

    def test_serialization_roundtrip(self):
        a = Account(account_id="a1", company_name="Acme", domain="acme.com")
        data = a.model_dump()
        restored = Account(**data)
        assert restored == a

    def test_json_roundtrip(self):
        a = Account(account_id="a1", company_name="Acme", domain="acme.com")
        raw = a.model_dump_json()
        restored = Account.model_validate_json(raw)
        assert restored.account_id == a.account_id


# ---------------------------------------------------------------------------
# SignalObservation
# ---------------------------------------------------------------------------


class TestSignalObservation:
    def test_minimal_valid(self):
        obs = SignalObservation(
            obs_id="obs1",
            account_id="a1",
            signal_code="devops_role_open",
            source="jobs_greenhouse",
            observed_at="2026-01-01T00:00:00Z",
            confidence=0.8,
            source_reliability=0.9,
            raw_payload_hash="raw_abc",
        )
        assert obs.product == "shared"
        assert obs.evidence_url == ""
        assert obs.evidence_quality == 0.0

    def test_all_products_valid(self):
        for product in ("zopdev", "zopday", "zopnight", "shared"):
            obs = SignalObservation(
                obs_id="obs1",
                account_id="a1",
                signal_code="test",
                product=product,
                source="test",
                observed_at="2026-01-01T00:00:00Z",
                confidence=0.5,
                source_reliability=0.5,
                raw_payload_hash="hash",
            )
            assert obs.product == product

    def test_invalid_product_rejected(self):
        with pytest.raises(ValidationError):
            SignalObservation(
                obs_id="obs1",
                account_id="a1",
                signal_code="test",
                product="invalid_product",
                source="test",
                observed_at="2026-01-01T00:00:00Z",
                confidence=0.5,
                source_reliability=0.5,
                raw_payload_hash="hash",
            )

    def test_optional_fields_default_empty(self):
        obs = SignalObservation(
            obs_id="obs1",
            account_id="a1",
            signal_code="test",
            source="test",
            observed_at="2026-01-01T00:00:00Z",
            confidence=0.5,
            source_reliability=0.5,
            raw_payload_hash="hash",
        )
        assert obs.document_id == ""
        assert obs.mention_id == ""
        assert obs.language == ""
        assert obs.speaker_name == ""
        assert obs.matched_phrase == ""


# ---------------------------------------------------------------------------
# ComponentScore
# ---------------------------------------------------------------------------


class TestComponentScore:
    def test_valid(self):
        cs = ComponentScore(
            run_id="r1", account_id="a1", product="zopdev", signal_code="devops_role_open", component_score=12.5
        )
        assert cs.component_score == 12.5

    def test_invalid_product(self):
        with pytest.raises(ValidationError):
            ComponentScore(run_id="r1", account_id="a1", product="shared", signal_code="test", component_score=1.0)


# ---------------------------------------------------------------------------
# AccountScore
# ---------------------------------------------------------------------------


class TestAccountScore:
    def test_valid_with_defaults(self):
        score = AccountScore(
            run_id="r1",
            account_id="a1",
            product="zopdev",
            score=25.0,
            tier="high",
            top_reasons_json="[]",
            delta_7d=2.0,
        )
        assert score.velocity_7d == 0.0
        assert score.velocity_category == "stable"
        assert score.confidence_band == "low"
        assert score.dimension_confidence_json == "{}"

    def test_all_tiers(self):
        for tier in ("high", "medium", "low"):
            score = AccountScore(
                run_id="r1",
                account_id="a1",
                product="zopday",
                score=10.0,
                tier=tier,
                top_reasons_json="[]",
                delta_7d=0.0,
            )
            assert score.tier == tier

    def test_invalid_tier(self):
        with pytest.raises(ValidationError):
            AccountScore(
                run_id="r1",
                account_id="a1",
                product="zopdev",
                score=10.0,
                tier="critical",
                top_reasons_json="[]",
                delta_7d=0.0,
            )

    def test_all_velocity_categories(self):
        for cat in ("surging", "accelerating", "stable", "decelerating"):
            score = AccountScore(
                run_id="r1",
                account_id="a1",
                product="zopnight",
                score=10.0,
                tier="medium",
                top_reasons_json="[]",
                delta_7d=0.0,
                velocity_category=cat,
            )
            assert score.velocity_category == cat

    def test_all_confidence_bands(self):
        for band in ("high", "medium", "low"):
            score = AccountScore(
                run_id="r1",
                account_id="a1",
                product="zopdev",
                score=10.0,
                tier="low",
                top_reasons_json="[]",
                delta_7d=0.0,
                confidence_band=band,
            )
            assert score.confidence_band == band


# ---------------------------------------------------------------------------
# ReviewLabel
# ---------------------------------------------------------------------------


class TestReviewLabel:
    def test_valid(self):
        label = ReviewLabel(
            review_id="rev1", run_id="r1", account_id="a1", decision="approved", reviewer="analyst@co.com"
        )
        assert label.notes == ""
        assert label.created_at  # auto-generated

    def test_all_decisions(self):
        for decision in ("approved", "rejected", "needs_more_info"):
            label = ReviewLabel(review_id="rev1", run_id="r1", account_id="a1", decision=decision, reviewer="test")
            assert label.decision == decision

    def test_invalid_decision(self):
        with pytest.raises(ValidationError):
            ReviewLabel(review_id="rev1", run_id="r1", account_id="a1", decision="maybe", reviewer="test")


# ---------------------------------------------------------------------------
# Reason
# ---------------------------------------------------------------------------


class TestReason:
    def test_valid_with_defaults(self):
        r = Reason(signal_code="devops_role_open", component_score=12.5, source="jobs_greenhouse")
        assert r.evidence_url == ""
        assert r.evidence_text == ""

    def test_with_evidence(self):
        r = Reason(
            signal_code="compliance_initiative",
            component_score=8.0,
            source="news_google",
            evidence_url="https://example.com/news",
            evidence_text="SOC 2 audit initiated",
        )
        assert r.evidence_url == "https://example.com/news"


# ---------------------------------------------------------------------------
# RunResult
# ---------------------------------------------------------------------------


class TestRunResult:
    def test_valid_with_defaults(self):
        rr = RunResult(run_id="r1", run_date="2026-02-25", status="running")
        assert rr.details == {}

    def test_all_statuses(self):
        for status in ("running", "completed", "failed"):
            rr = RunResult(run_id="r1", run_date="2026-02-25", status=status)
            assert rr.status == status

    def test_invalid_status(self):
        with pytest.raises(ValidationError):
            RunResult(run_id="r1", run_date="2026-02-25", status="unknown")

    def test_details_dict(self):
        rr = RunResult(run_id="r1", run_date="2026-02-25", status="completed", details={"rows": 100})
        assert rr.details["rows"] == 100
