"""Tests for dossier template rendering and persistence."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

from src.export.dossier import render_dossier

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_SAMPLE_ACCOUNT = {
    "account_id": "acc_test",
    "company_name": "Acme Corp",
    "domain": "acme.com",
}

_SAMPLE_RESEARCH = {
    "research_brief": "Acme Corp is a fast-growing SaaS company focused on cloud infrastructure.",
    "research_profile": (
        "Acme Corp is a fast-growing SaaS company.\n\n"
        "## Conversation Starters\n"
        "- How are you handling multi-cloud complexity?\n"
        "- What is your approach to FinOps?\n"
    ),
    "enrichment_json": json.dumps(
        {
            "industry": "Technology",
            "sub_industry": "Cloud Computing",
            "employees": 500,
            "employee_range": "201-500",
            "revenue_range": "$50M-$100M",
            "city": "San Francisco",
            "state": "CA",
            "country": "US",
            "website": "https://acme.com",
            "tech_stack": ["AWS", "Kubernetes", "Terraform"],
            "company_linkedin_url": "https://linkedin.com/company/acme",
        }
    ),
}

_SAMPLE_ENRICHMENT = {
    "industry": "Technology",
    "sub_industry": "Cloud Computing",
    "employees": 500,
    "employee_range": "201-500",
    "revenue_range": "$50M-$100M",
    "city": "San Francisco",
    "state": "CA",
    "country": "US",
    "website": "https://acme.com",
    "tech_stack": ["AWS", "Kubernetes", "Terraform"],
    "company_linkedin_url": "https://linkedin.com/company/acme",
}

_SAMPLE_CONTACTS = [
    {
        "first_name": "Jane",
        "last_name": "Doe",
        "title": "CTO",
        "email": "jane@acme.com",
        "linkedin_url": "https://linkedin.com/in/janedoe",
        "management_level": "C-Level",
    },
    {
        "first_name": "John",
        "last_name": "Smith",
        "title": "VP Engineering",
        "email": "john@acme.com",
        "linkedin_url": "",
        "management_level": "VP",
    },
]

_SAMPLE_SCORES = {
    "score": 85.0,
    "tier": "high",
    "tier_v2": "tier_1",
    "top_reasons_json": json.dumps(
        [
            {"signal_code": "hiring_devops", "reason": "Active DevOps hiring"},
            {"signal_code": "cloud_migration", "reason": "Cloud migration initiative"},
        ]
    ),
}

_SAMPLE_DIMENSION_SCORES = {
    "trigger_intent": 90,
    "tech_fit": 75,
    "engagement_pql": 60,
    "firmographic": 50,
    "hiring_growth": 80,
}

_SAMPLE_SIGNALS = [
    {
        "signal_code": "hiring_devops",
        "source": "jobs",
        "observed_at": "2025-01-15",
        "evidence_text": "DevOps engineer posting",
    },
    {
        "signal_code": "cloud_migration",
        "source": "news",
        "observed_at": "2025-01-10",
        "evidence_text": "Company announces cloud-first strategy",
    },
]


# ---------------------------------------------------------------------------
# render_dossier — full dossier
# ---------------------------------------------------------------------------


class TestRenderDossierFull:
    def test_full_dossier_has_9_sections(self):
        result = render_dossier(
            account=_SAMPLE_ACCOUNT,
            research=_SAMPLE_RESEARCH,
            enrichment=_SAMPLE_ENRICHMENT,
            contacts=_SAMPLE_CONTACTS,
            scores=_SAMPLE_SCORES,
            dimension_scores=_SAMPLE_DIMENSION_SCORES,
            signals=_SAMPLE_SIGNALS,
            dossier_type="full",
        )

        assert result["dossier_type"] == "full"
        assert len(result["sections"]) == 9
        titles = [s["title"] for s in result["sections"]]
        assert "Executive Summary" in titles
        assert "Company Overview" in titles
        assert "Cloud Infrastructure Intelligence" in titles
        assert "Buying Signals & Triggers" in titles
        assert "Key Decision Makers" in titles
        assert "Pain Hypothesis" in titles
        assert "Competitive Landscape" in titles
        assert "Recommended Approach" in titles
        assert "ICP Fit Analysis" in titles

    def test_executive_summary_from_research_brief(self):
        result = render_dossier(
            account=_SAMPLE_ACCOUNT,
            research=_SAMPLE_RESEARCH,
            dossier_type="full",
        )
        exec_section = result["sections"][0]
        assert exec_section["title"] == "Executive Summary"
        assert "Acme Corp" in exec_section["content"]

    def test_company_overview_includes_industry_and_size(self):
        result = render_dossier(
            account=_SAMPLE_ACCOUNT,
            enrichment=_SAMPLE_ENRICHMENT,
            dossier_type="full",
        )
        overview = next(s for s in result["sections"] if s["title"] == "Company Overview")
        assert "Technology" in overview["content"]
        assert "500" in overview["content"]

    def test_cloud_infrastructure_shows_tech_stack(self):
        result = render_dossier(
            account=_SAMPLE_ACCOUNT,
            enrichment=_SAMPLE_ENRICHMENT,
            dossier_type="full",
        )
        cloud = next(s for s in result["sections"] if s["title"] == "Cloud Infrastructure Intelligence")
        assert "AWS" in cloud["content"]
        assert "Kubernetes" in cloud["content"]

    def test_key_decision_makers_lists_contacts(self):
        result = render_dossier(
            account=_SAMPLE_ACCOUNT,
            contacts=_SAMPLE_CONTACTS,
            dossier_type="full",
        )
        dm = next(s for s in result["sections"] if s["title"] == "Key Decision Makers")
        assert "Jane" in dm["content"]
        assert "CTO" in dm["content"]
        assert "John" in dm["content"]

    def test_buying_signals_from_top_reasons(self):
        result = render_dossier(
            account=_SAMPLE_ACCOUNT,
            scores=_SAMPLE_SCORES,
            signals=_SAMPLE_SIGNALS,
            dossier_type="full",
        )
        signals_section = next(s for s in result["sections"] if s["title"] == "Buying Signals & Triggers")
        assert "hiring_devops" in signals_section["content"]

    def test_icp_fit_shows_dimension_scores(self):
        result = render_dossier(
            account=_SAMPLE_ACCOUNT,
            dimension_scores=_SAMPLE_DIMENSION_SCORES,
            scores=_SAMPLE_SCORES,
            dossier_type="full",
        )
        icp = next(s for s in result["sections"] if s["title"] == "ICP Fit Analysis")
        assert "Trigger / Intent" in icp["content"]
        assert "90" in icp["content"]
        assert "High" in icp["content"]

    def test_markdown_output_includes_company_name(self):
        result = render_dossier(
            account=_SAMPLE_ACCOUNT,
            research=_SAMPLE_RESEARCH,
            enrichment=_SAMPLE_ENRICHMENT,
            contacts=_SAMPLE_CONTACTS,
            scores=_SAMPLE_SCORES,
            dimension_scores=_SAMPLE_DIMENSION_SCORES,
            signals=_SAMPLE_SIGNALS,
        )
        assert "# GTM Dossier: Acme Corp" in result["markdown"]
        assert "Executive Summary" in result["markdown"]


# ---------------------------------------------------------------------------
# render_dossier — brief and summary variants
# ---------------------------------------------------------------------------


class TestDossierVariants:
    def test_brief_dossier_has_5_sections(self):
        result = render_dossier(
            account=_SAMPLE_ACCOUNT,
            research=_SAMPLE_RESEARCH,
            enrichment=_SAMPLE_ENRICHMENT,
            contacts=_SAMPLE_CONTACTS,
            scores=_SAMPLE_SCORES,
            dimension_scores=_SAMPLE_DIMENSION_SCORES,
            signals=_SAMPLE_SIGNALS,
            dossier_type="brief",
        )
        assert result["dossier_type"] == "brief"
        assert len(result["sections"]) == 5
        titles = [s["title"] for s in result["sections"]]
        assert "Executive Summary" in titles
        assert "Company Overview" in titles
        assert "ICP Fit Analysis" in titles

    def test_summary_dossier_has_3_sections(self):
        result = render_dossier(
            account=_SAMPLE_ACCOUNT,
            research=_SAMPLE_RESEARCH,
            enrichment=_SAMPLE_ENRICHMENT,
            scores=_SAMPLE_SCORES,
            dimension_scores=_SAMPLE_DIMENSION_SCORES,
            dossier_type="summary",
        )
        assert result["dossier_type"] == "summary"
        assert len(result["sections"]) == 3

    def test_dossier_type_inferred_from_tier_v2(self):
        result = render_dossier(
            account=_SAMPLE_ACCOUNT,
            scores={"tier_v2": "tier_2"},
        )
        assert result["dossier_type"] == "brief"

    def test_dossier_type_defaults_to_full(self):
        result = render_dossier(account=_SAMPLE_ACCOUNT)
        assert result["dossier_type"] == "full"


# ---------------------------------------------------------------------------
# Graceful fallbacks with missing data
# ---------------------------------------------------------------------------


class TestGracefulFallbacks:
    def test_empty_data_still_renders(self):
        result = render_dossier(account=_SAMPLE_ACCOUNT)
        assert result["account_id"] == "acc_test"
        assert len(result["sections"]) == 9
        assert result["markdown"] != ""

    def test_missing_research_shows_fallback(self):
        result = render_dossier(account=_SAMPLE_ACCOUNT, dossier_type="full")
        exec_section = result["sections"][0]
        assert "No research brief" in exec_section["content"] or "Unknown" in exec_section["content"]

    def test_missing_contacts_shows_fallback(self):
        result = render_dossier(account=_SAMPLE_ACCOUNT, contacts=[], dossier_type="full")
        dm = next(s for s in result["sections"] if s["title"] == "Key Decision Makers")
        assert "No contacts" in dm["content"]

    def test_missing_dimension_scores_shows_fallback(self):
        result = render_dossier(account=_SAMPLE_ACCOUNT, dossier_type="full")
        icp = next(s for s in result["sections"] if s["title"] == "ICP Fit Analysis")
        assert "No dimension score" in icp["content"]

    def test_missing_tech_stack_shows_fallback(self):
        result = render_dossier(account=_SAMPLE_ACCOUNT, enrichment={}, dossier_type="full")
        cloud = next(s for s in result["sections"] if s["title"] == "Cloud Infrastructure Intelligence")
        assert "No cloud infrastructure" in cloud["content"]


# ---------------------------------------------------------------------------
# Dossier DB functions
# ---------------------------------------------------------------------------


class TestDossierDB:
    def test_save_and_get_dossier(self):
        from src.db.accounts import get_latest_dossier, save_dossier

        conn = MagicMock()
        call_log = []

        def _execute(sql, params=None):
            call_log.append((sql, params))
            cursor = MagicMock()
            sql_lower = (sql or "").lower().strip()
            if "max(version)" in sql_lower:
                cursor.fetchone.return_value = {"max_v": 2}
            elif sql_lower.startswith("select * from dossiers"):
                cursor.fetchone.return_value = {
                    "dossier_id": "dos_abc",
                    "account_id": "acc_test",
                    "dossier_type": "full",
                    "version": 3,
                    "sections_json": "[]",
                    "markdown": "# Dossier",
                    "generated_at": "2025-01-15",
                }
            else:
                cursor.fetchone.return_value = None
                cursor.fetchall.return_value = []
            return cursor

        conn.execute = _execute
        conn.commit = MagicMock()

        # Save
        dossier_data = {
            "account_id": "acc_test",
            "dossier_type": "full",
            "sections": [],
            "markdown": "# Test",
            "generated_at": "2025-01-15",
        }
        dossier_id = save_dossier(conn, dossier_data)
        assert dossier_id.startswith("dos_")
        conn.commit.assert_called()

        # Get
        result = get_latest_dossier(conn, "acc_test")
        assert result is not None
        assert result["dossier_id"] == "dos_abc"
        assert result["version"] == 3

    def test_get_dossier_history(self):
        from src.db.accounts import get_dossier_history

        conn = MagicMock()

        def _execute(sql, params=None):
            cursor = MagicMock()
            cursor.fetchall.return_value = [
                {"dossier_id": "dos_1", "version": 2},
                {"dossier_id": "dos_2", "version": 1},
            ]
            return cursor

        conn.execute = _execute

        results = get_dossier_history(conn, "acc_test")
        assert len(results) == 2
        assert results[0]["version"] == 2


# ---------------------------------------------------------------------------
# API endpoint registration
# ---------------------------------------------------------------------------


class TestDossierEndpoints:
    def test_dossier_endpoints_registered(self):
        from src.web.routes.accounts import router

        paths = [r.path for r in router.routes]
        assert "/accounts/{account_id}/dossier" in paths
