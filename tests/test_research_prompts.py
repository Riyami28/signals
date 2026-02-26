"""Tests for src/research/prompts.py — pure unit tests, no DB needed."""

from __future__ import annotations

from src.research.prompts import (
    build_extraction_prompt,
    build_scoring_prompt,
    prompt_hash,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_SAMPLE_ACCOUNT = {
    "account_id": "acc_abc123",
    "company_name": "Acme Corp",
    "domain": "acme.com",
    "signal_score": 42.5,
    "signal_tier": "high",
    "delta_7d": 5.2,
    "top_reasons_json": "[]",
}

_SAMPLE_SIGNALS = [
    {
        "signal_code": "devops_role_open",
        "source": "greenhouse_api",
        "evidence_url": "https://greenhouse.io/jobs/12345",
        "evidence_text": "Senior DevOps Engineer role posted 3 days ago",
    },
    {
        "signal_code": "kubernetes_detected",
        "source": "technographics_csv",
        "evidence_url": "",
        "evidence_text": "Kubernetes detected via BuiltWith scan",
    },
]


# ===========================================================================
# TestBuildExtractionPrompt
# ===========================================================================


class TestBuildExtractionPrompt:
    def test_prompt_includes_company_name(self):
        system, user = build_extraction_prompt(
            account=_SAMPLE_ACCOUNT,
            signals=[],
        )
        assert "Acme Corp" in user

    def test_prompt_includes_domain(self):
        system, user = build_extraction_prompt(
            account=_SAMPLE_ACCOUNT,
            signals=[],
        )
        assert "acme.com" in user

    def test_prompt_includes_signal_codes(self):
        system, user = build_extraction_prompt(
            account=_SAMPLE_ACCOUNT,
            signals=_SAMPLE_SIGNALS,
        )
        assert "devops_role_open" in user
        assert "greenhouse_api" in user

    def test_prompt_includes_evidence_urls(self):
        system, user = build_extraction_prompt(
            account=_SAMPLE_ACCOUNT,
            signals=_SAMPLE_SIGNALS,
        )
        assert "https://greenhouse.io/jobs/12345" in user

    def test_prompt_handles_account_with_no_signals(self):
        system, user = build_extraction_prompt(
            account=_SAMPLE_ACCOUNT,
            signals=[],
        )
        # Should still produce valid prompts with a fallback message
        assert "Acme Corp" in user
        assert isinstance(system, str)
        assert len(system) > 0
        assert "(No signal observations available)" in user

    def test_prompt_includes_pre_filled_enrichment_when_provided(self):
        pre_enrichment = {"industry": "SaaS", "industry_confidence": 0.9}
        system, user = build_extraction_prompt(
            account=_SAMPLE_ACCOUNT,
            signals=[],
            pre_enrichment=pre_enrichment,
        )
        assert "industry" in user.lower()
        assert "SaaS" in user
        assert "Already Known" in user


# ===========================================================================
# TestBuildScoringPrompt
# ===========================================================================


class TestBuildScoringPrompt:
    def test_prompt_includes_research_brief(self):
        brief = "Acme is a cloud company with strong Kubernetes adoption."
        system, user = build_scoring_prompt(
            account=_SAMPLE_ACCOUNT,
            research_brief=brief,
        )
        assert "Acme is a cloud company" in user
        assert isinstance(system, str)
        assert len(system) > 0

    def test_prompt_does_not_include_raw_signal_data(self):
        brief = "Acme is a cloud company."
        system, user = build_scoring_prompt(
            account=_SAMPLE_ACCOUNT,
            research_brief=brief,
        )
        # Signal codes from the extraction pass should NOT appear in scoring prompt
        assert "devops_role_open" not in user
        assert "greenhouse_api" not in user
        assert "kubernetes_detected" not in user


# ===========================================================================
# TestPromptHash
# ===========================================================================


class TestPromptHash:
    def test_same_templates_produce_same_hash(self):
        h1 = prompt_hash("template1", "template2")
        h2 = prompt_hash("template1", "template2")
        assert h1 == h2

    def test_different_templates_produce_different_hash(self):
        h1 = prompt_hash("template1", "template2")
        h2 = prompt_hash("template1_changed", "template2")
        assert h1 != h2

    def test_hash_is_16_chars(self):
        h = prompt_hash("a", "b")
        assert len(h) == 16

    def test_hash_is_hex_string(self):
        h = prompt_hash("x", "y")
        assert len(h) == 16
        # Should be valid hex characters
        int(h, 16)
