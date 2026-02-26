"""Tests for tier-driven enrichment orchestrator."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.enrichment.orchestrator import (
    EnrichmentResult,
    _resolve_effective_tier,
    enrich_account,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_settings(**overrides):
    """Return a mock Settings object with sensible defaults."""
    s = MagicMock()
    s.apollo_api_key = overrides.get("apollo_api_key", "test-apollo-key")
    s.apollo_rate_limit = overrides.get("apollo_rate_limit", 50)
    s.hunter_api_key = overrides.get("hunter_api_key", "test-hunter-key")
    return s


def _make_conn(domain="example.com", llm_contacts=None):
    """Return a mock DB connection with canned responses."""
    conn = MagicMock()
    call_log = []

    def _execute(sql, params=None):
        call_log.append((sql, params))
        cursor = MagicMock()
        sql_lower = (sql or "").lower().strip()

        if "select domain from accounts" in sql_lower:
            if domain:
                cursor.fetchone.return_value = {"domain": domain}
            else:
                cursor.fetchone.return_value = None
        elif "contact_research" in sql_lower:
            cursor.fetchall.return_value = llm_contacts or []
        elif "insert into contacts" in sql_lower:
            pass  # just record the call
        else:
            cursor.fetchone.return_value = None
            cursor.fetchall.return_value = []
        return cursor

    conn.execute = _execute
    conn.commit = MagicMock()
    conn._call_log = call_log
    return conn


# ---------------------------------------------------------------------------
# _resolve_effective_tier
# ---------------------------------------------------------------------------


class TestResolveEffectiveTier:
    def test_tier_1_stays_tier_1(self):
        assert _resolve_effective_tier("tier_1", {}) == "tier_1"

    def test_tier_2_upgrades_with_strong_trigger(self):
        assert _resolve_effective_tier("tier_2", {"trigger_intent": 80}) == "tier_1"

    def test_tier_2_stays_with_weak_trigger(self):
        assert _resolve_effective_tier("tier_2", {"trigger_intent": 50}) == "tier_2"

    def test_tier_2_stays_with_no_trigger(self):
        assert _resolve_effective_tier("tier_2", {}) == "tier_2"

    def test_tier_3_stays_tier_3(self):
        assert _resolve_effective_tier("tier_3", {"trigger_intent": 90}) == "tier_3"

    def test_tier_4_stays_tier_4(self):
        assert _resolve_effective_tier("tier_4", {}) == "tier_4"

    def test_invalid_tier_defaults_to_tier_4(self):
        assert _resolve_effective_tier("unknown", {}) == "tier_4"

    def test_empty_tier_defaults_to_tier_4(self):
        assert _resolve_effective_tier("", {}) == "tier_4"

    def test_boundary_trigger_exactly_70(self):
        assert _resolve_effective_tier("tier_2", {"trigger_intent": 70}) == "tier_1"

    def test_boundary_trigger_just_below_70(self):
        assert _resolve_effective_tier("tier_2", {"trigger_intent": 69.9}) == "tier_2"


# ---------------------------------------------------------------------------
# EnrichmentResult.skip
# ---------------------------------------------------------------------------


class TestEnrichmentResultSkip:
    def test_skip_returns_skipped_result(self):
        result = EnrichmentResult.skip("acc_123", "tier_4")
        assert result.account_id == "acc_123"
        assert result.tier == "tier_4"
        assert result.contacts_found == 0
        assert result.dossier_type == "skipped"
        assert result.contacts == []
        assert result.skipped is True


# ---------------------------------------------------------------------------
# enrich_account — tier dispatch
# ---------------------------------------------------------------------------


class TestEnrichAccountTierDispatch:
    @patch("src.enrichment.orchestrator.search_contacts_for_account")
    @patch("src.enrichment.orchestrator.ApolloClient")
    def test_tier_4_skips_entirely(self, mock_apollo_cls, mock_search):
        conn = _make_conn()
        settings = _make_settings()

        result = enrich_account(conn, "acc_1", "tier_4", {}, settings)

        assert result.skipped is True
        assert result.dossier_type == "skipped"
        assert result.contacts_found == 0
        mock_search.assert_not_called()

    @patch("src.enrichment.orchestrator.search_contacts_for_account")
    @patch("src.enrichment.orchestrator.ApolloClient")
    def test_tier_3_no_contacts_summary_dossier(self, mock_apollo_cls, mock_search):
        conn = _make_conn()
        settings = _make_settings()

        result = enrich_account(conn, "acc_2", "tier_3", {}, settings)

        assert result.skipped is False
        assert result.dossier_type == "summary"
        assert result.contacts_found == 0
        assert result.contacts == []
        mock_search.assert_not_called()

    @patch("src.enrichment.orchestrator.search_contacts_for_account")
    @patch("src.enrichment.orchestrator.ApolloClient")
    def test_tier_2_finds_1_contact_brief_dossier(self, mock_apollo_cls, mock_search):
        mock_search.return_value = [
            {
                "first_name": "Alice",
                "last_name": "Smith",
                "email": "alice@ex.com",
                "title": "CTO",
                "linkedin_url": "",
                "management_level": "C-Level",
            }
        ]
        conn = _make_conn()
        settings = _make_settings()

        result = enrich_account(conn, "acc_3", "tier_2", {}, settings)

        assert result.dossier_type == "brief"
        assert result.contacts_found == 1
        mock_search.assert_called_once()
        # Verify limit=1 and tier="medium" in the call
        _, kwargs = mock_search.call_args
        assert kwargs.get("limit") == 1 or mock_search.call_args[0][4] == 1

    @patch("src.enrichment.orchestrator.search_contacts_for_account")
    @patch("src.enrichment.orchestrator.ApolloClient")
    def test_tier_1_finds_3_contacts_full_dossier(self, mock_apollo_cls, mock_search):
        mock_search.return_value = [
            {
                "first_name": "A",
                "last_name": "One",
                "email": "a@ex.com",
                "title": "CTO",
                "linkedin_url": "",
                "management_level": "C-Level",
            },
            {
                "first_name": "B",
                "last_name": "Two",
                "email": "b@ex.com",
                "title": "VP Eng",
                "linkedin_url": "",
                "management_level": "VP",
            },
            {
                "first_name": "C",
                "last_name": "Three",
                "email": "c@ex.com",
                "title": "FinOps Lead",
                "linkedin_url": "",
                "management_level": "Manager",
            },
        ]
        conn = _make_conn()
        settings = _make_settings()

        result = enrich_account(conn, "acc_4", "tier_1", {}, settings)

        assert result.dossier_type == "full"
        assert result.contacts_found == 3
        mock_search.assert_called_once()

    @patch("src.enrichment.orchestrator.search_contacts_for_account")
    @patch("src.enrichment.orchestrator.ApolloClient")
    def test_tier_2_upgrade_to_tier_1(self, mock_apollo_cls, mock_search):
        """Tier 2 + trigger_intent >= 70 should behave like tier 1."""
        mock_search.return_value = [
            {
                "first_name": "X",
                "last_name": "Y",
                "email": "x@ex.com",
                "title": "CTO",
                "linkedin_url": "",
                "management_level": "C-Level",
            },
            {
                "first_name": "A",
                "last_name": "B",
                "email": "a@ex.com",
                "title": "VP",
                "linkedin_url": "",
                "management_level": "VP",
            },
            {
                "first_name": "C",
                "last_name": "D",
                "email": "c@ex.com",
                "title": "Lead",
                "linkedin_url": "",
                "management_level": "Manager",
            },
        ]
        conn = _make_conn()
        settings = _make_settings()
        dims = {"trigger_intent": 80}

        result = enrich_account(conn, "acc_5", "tier_2", dims, settings)

        assert result.dossier_type == "full"
        assert result.contacts_found == 3


# ---------------------------------------------------------------------------
# enrich_account — contact waterfall
# ---------------------------------------------------------------------------


class TestContactWaterfall:
    @patch("src.enrichment.orchestrator.search_contacts_for_account")
    @patch("src.enrichment.orchestrator.ApolloClient")
    def test_apollo_contacts_stored_in_db(self, mock_apollo_cls, mock_search):
        mock_search.return_value = [
            {
                "first_name": "Alice",
                "last_name": "Smith",
                "email": "alice@ex.com",
                "title": "CTO",
                "linkedin_url": "",
                "management_level": "C-Level",
            }
        ]
        conn = _make_conn()
        settings = _make_settings()

        enrich_account(conn, "acc_6", "tier_2", {}, settings)

        # Verify commit was called (contacts stored)
        conn.commit.assert_called()

    @patch("src.enrichment.orchestrator.search_contacts_for_account")
    @patch("src.enrichment.orchestrator.ApolloClient")
    def test_no_apollo_key_skips_apollo_client(self, mock_apollo_cls, mock_search):
        mock_search.return_value = []
        conn = _make_conn()
        settings = _make_settings(apollo_api_key="")

        enrich_account(conn, "acc_7", "tier_1", {}, settings)

        # ApolloClient should NOT be instantiated
        mock_apollo_cls.assert_not_called()
        # search_contacts_for_account still called with apollo_client=None
        mock_search.assert_called_once()
        call_kwargs = mock_search.call_args
        assert call_kwargs[1].get("apollo_client") is None or call_kwargs[0][1] is None

    @patch("src.enrichment.orchestrator.search_contacts_for_account")
    @patch("src.enrichment.orchestrator.ApolloClient")
    def test_llm_supplements_when_apollo_under_limit(self, mock_apollo_cls, mock_search):
        """When Apollo returns fewer contacts than the limit, supplement with LLM."""
        mock_search.return_value = [
            {
                "first_name": "Apollo",
                "last_name": "Contact",
                "email": "apollo@ex.com",
                "title": "CTO",
                "linkedin_url": "",
                "management_level": "C-Level",
            }
        ]
        llm_contacts = [
            {
                "first_name": "LLM",
                "last_name": "Person",
                "title": "VP Eng",
                "email": "llm@ex.com",
                "linkedin_url": "",
                "management_level": "VP",
            },
        ]
        conn = _make_conn(llm_contacts=llm_contacts)
        settings = _make_settings()

        result = enrich_account(conn, "acc_8", "tier_1", {}, settings)

        assert result.contacts_found == 2
        sources = {c.get("first_name") for c in result.contacts}
        assert "Apollo" in sources
        assert "LLM" in sources

    @patch("src.enrichment.orchestrator.search_contacts_for_account")
    @patch("src.enrichment.orchestrator.ApolloClient")
    def test_llm_deduplicates_by_email(self, mock_apollo_cls, mock_search):
        """LLM contacts with duplicate emails should not be added."""
        mock_search.return_value = [
            {
                "first_name": "Same",
                "last_name": "Person",
                "email": "same@ex.com",
                "title": "CTO",
                "linkedin_url": "",
                "management_level": "C-Level",
            }
        ]
        llm_contacts = [
            {
                "first_name": "Same",
                "last_name": "Person",
                "title": "CTO",
                "email": "same@ex.com",
                "linkedin_url": "",
                "management_level": "C-Level",
            },
        ]
        conn = _make_conn(llm_contacts=llm_contacts)
        settings = _make_settings()

        result = enrich_account(conn, "acc_9", "tier_1", {}, settings)

        assert result.contacts_found == 1

    @patch("src.enrichment.orchestrator.search_contacts_for_account")
    @patch("src.enrichment.orchestrator.ApolloClient")
    def test_no_domain_returns_empty_contacts(self, mock_apollo_cls, mock_search):
        conn = _make_conn(domain="")
        settings = _make_settings()

        result = enrich_account(conn, "acc_10", "tier_1", {}, settings)

        assert result.contacts_found == 0
        mock_search.assert_not_called()


# ---------------------------------------------------------------------------
# enrich_account — edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    @patch("src.enrichment.orchestrator.search_contacts_for_account")
    @patch("src.enrichment.orchestrator.ApolloClient")
    def test_invalid_tier_treated_as_tier_4(self, mock_apollo_cls, mock_search):
        conn = _make_conn()
        settings = _make_settings()

        result = enrich_account(conn, "acc_11", "garbage", {}, settings)

        assert result.skipped is True
        assert result.dossier_type == "skipped"

    @patch("src.enrichment.orchestrator.search_contacts_for_account")
    @patch("src.enrichment.orchestrator.ApolloClient")
    def test_empty_dimension_scores(self, mock_apollo_cls, mock_search):
        mock_search.return_value = []
        conn = _make_conn()
        settings = _make_settings()

        result = enrich_account(conn, "acc_12", "tier_2", {}, settings)

        assert result.dossier_type == "brief"
        assert result.skipped is False

    @patch("src.enrichment.orchestrator.search_contacts_for_account")
    @patch("src.enrichment.orchestrator.ApolloClient")
    def test_contacts_capped_at_limit(self, mock_apollo_cls, mock_search):
        """Even if Apollo + LLM return more, should cap at the limit."""
        mock_search.return_value = [
            {
                "first_name": f"P{i}",
                "last_name": "X",
                "email": f"p{i}@ex.com",
                "title": "Eng",
                "linkedin_url": "",
                "management_level": "IC",
            }
            for i in range(5)
        ]
        conn = _make_conn()
        settings = _make_settings()

        result = enrich_account(conn, "acc_13", "tier_1", {}, settings)

        # tier_1 limit is 3
        assert result.contacts_found == 3
