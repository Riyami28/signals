"""Tests for all collectors — helper functions, build_observation, edge cases."""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from src.collectors.community import (
    _build_observation as community_build_observation,
)
from src.collectors.community import (
    _parse_entry_observed_at as community_parse_observed_at,
)
from src.collectors.community import (
    _reddit_search_rss_url,
)
from src.collectors.first_party import VALID_PRODUCTS
from src.collectors.jobs import (
    FALLBACK_ROLE_SIGNALS,
    _derive_slug_candidates,
    _extract_job_titles_from_html,
    _extract_job_titles_from_jsonld_payload,
    _matches_from_text,
)
from src.collectors.jobs import (
    _build_observation as jobs_build_observation,
)
from src.collectors.news import (
    _build_observation as news_build_observation,
)
from src.collectors.news import (
    _google_news_rss_url,
    _match_signals,
)
from src.collectors.news import (
    _parse_entry_observed_at as news_parse_observed_at,
)
from src.collectors.technographics import (
    DISCOVERY_LINK_TOKENS,
    MAX_SCAN_TEXT_CHARS,
)
from src.collectors.technographics import (
    _build_observation as tech_build_observation,
)

# ---------------------------------------------------------------------------
# Jobs collector
# ---------------------------------------------------------------------------


class TestJobsFallbackMatches:
    def test_devops_match(self):
        matches = _matches_from_text("Hiring senior DevOps engineer", [])
        assert any(s == "devops_role_open" for s, _, _ in matches)

    def test_sre_match(self):
        matches = _matches_from_text("We need an SRE to join the team", [])
        assert any(s == "devops_role_open" for s, _, _ in matches)

    def test_platform_engineer_match(self):
        matches = _matches_from_text("Looking for a platform engineer", [])
        assert any(s == "platform_role_open" for s, _, _ in matches)

    def test_finops_match(self):
        matches = _matches_from_text("FinOps analyst needed", [])
        assert any(s == "finops_role_open" for s, _, _ in matches)

    def test_no_match_unrelated(self):
        matches = _matches_from_text("Marketing manager position", [])
        assert matches == []

    def test_lexicon_takes_precedence(self):
        lexicon = [{"signal_code": "custom_signal", "keyword": "devops", "confidence": "0.9"}]
        matches = _matches_from_text("DevOps engineer needed", lexicon)
        assert matches[0][0] == "custom_signal"

    def test_fallback_confidence_065(self):
        matches = _matches_from_text("DevOps role open", [])
        for signal, confidence, _ in matches:
            if signal == "devops_role_open":
                assert confidence == 0.65


class TestExtractJobTitlesFromJsonldPayload:
    def test_single_job_posting(self):
        payload = {"@type": "JobPosting", "title": "SRE Lead"}
        titles = _extract_job_titles_from_jsonld_payload(payload)
        assert "SRE Lead" in titles

    def test_list_of_postings(self):
        payload = [
            {"@type": "JobPosting", "title": "DevOps"},
            {"@type": "JobPosting", "title": "SRE"},
        ]
        titles = _extract_job_titles_from_jsonld_payload(payload)
        assert "DevOps" in titles
        assert "SRE" in titles

    def test_graph_container(self):
        payload = {"@graph": [{"@type": "JobPosting", "title": "Engineer"}]}
        titles = _extract_job_titles_from_jsonld_payload(payload)
        assert "Engineer" in titles

    def test_type_as_list(self):
        payload = {"@type": ["JobPosting", "Thing"], "title": "Manager"}
        titles = _extract_job_titles_from_jsonld_payload(payload)
        assert "Manager" in titles

    def test_non_job_posting_ignored(self):
        payload = {"@type": "Organization", "name": "Acme"}
        titles = _extract_job_titles_from_jsonld_payload(payload)
        assert titles == []

    def test_empty_title_skipped(self):
        payload = {"@type": "JobPosting", "title": ""}
        titles = _extract_job_titles_from_jsonld_payload(payload)
        assert titles == []

    def test_non_dict_input(self):
        assert _extract_job_titles_from_jsonld_payload("string") == []
        assert _extract_job_titles_from_jsonld_payload(42) == []


class TestExtractJobTitlesFromHtml:
    def test_jsonld_in_html(self):
        html = """
        <html><head>
        <script type="application/ld+json">
          {"@type":"JobPosting","title":"Cloud Engineer"}
        </script>
        </head><body></body></html>
        """
        titles = _extract_job_titles_from_html(html)
        assert "Cloud Engineer" in titles

    def test_multiple_scripts(self):
        html = """
        <html><head>
        <script type="application/ld+json">{"@type":"JobPosting","title":"DevOps"}</script>
        <script type="application/ld+json">{"@type":"JobPosting","title":"SRE"}</script>
        </head><body></body></html>
        """
        titles = _extract_job_titles_from_html(html)
        assert "DevOps" in titles
        assert "SRE" in titles

    def test_deduplicates_titles(self):
        html = """
        <html><head>
        <script type="application/ld+json">{"@type":"JobPosting","title":"DevOps"}</script>
        <script type="application/ld+json">{"@type":"JobPosting","title":"devops"}</script>
        </head><body></body></html>
        """
        titles = _extract_job_titles_from_html(html)
        assert len(titles) == 1

    def test_invalid_json_skipped(self):
        html = """
        <html><head>
        <script type="application/ld+json">not valid json</script>
        </head><body></body></html>
        """
        titles = _extract_job_titles_from_html(html)
        assert titles == []

    def test_empty_html(self):
        titles = _extract_job_titles_from_html("")
        assert titles == []


class TestDeriveSlugCandidates:
    def test_simple_domain(self):
        slugs = _derive_slug_candidates("acme.com")
        assert "acme" in slugs

    def test_hyphenated_domain(self):
        slugs = _derive_slug_candidates("my-company.io")
        assert "my-company" in slugs
        assert "mycompany" in slugs

    def test_underscored_domain(self):
        slugs = _derive_slug_candidates("my_company.com")
        assert "my_company" in slugs
        assert "mycompany" in slugs

    def test_max_three_candidates(self):
        slugs = _derive_slug_candidates("a-b_c.com")
        assert len(slugs) <= 3

    def test_short_domain_filtered(self):
        slugs = _derive_slug_candidates("a.io")
        assert slugs == []


class TestJobsBuildObservation:
    def test_creates_valid_observation(self):
        obs = jobs_build_observation(
            account_id="a1",
            signal_code="devops_role_open",
            source="jobs_greenhouse",
            observed_at="2026-01-01T00:00:00Z",
            confidence=0.8,
            source_reliability=0.9,
            evidence_url="https://example.com/job/123",
            evidence_text="Senior DevOps Engineer",
            payload={"title": "DevOps"},
        )
        assert obs.account_id == "a1"
        assert obs.signal_code == "devops_role_open"
        assert obs.obs_id.startswith("obs_")
        assert obs.raw_payload_hash.startswith("raw_")

    def test_clamps_confidence(self):
        obs = jobs_build_observation(
            account_id="a1",
            signal_code="test",
            source="test",
            observed_at="2026-01-01T00:00:00Z",
            confidence=1.5,
            source_reliability=0.9,
            evidence_url="",
            evidence_text="",
            payload={},
        )
        assert obs.confidence == 1.0

    def test_truncates_evidence_text(self):
        long_text = "x" * 1000
        obs = jobs_build_observation(
            account_id="a1",
            signal_code="test",
            source="test",
            observed_at="2026-01-01T00:00:00Z",
            confidence=0.8,
            source_reliability=0.9,
            evidence_url="",
            evidence_text=long_text,
            payload={},
        )
        assert len(obs.evidence_text) == 500


# ---------------------------------------------------------------------------
# News collector
# ---------------------------------------------------------------------------


class TestNewsKeywordMatching:
    def test_uses_lexicon(self):
        lexicon = [{"signal_code": "compliance_initiative", "keyword": "soc 2", "confidence": "0.8"}]
        matches = _match_signals("Company starts SOC 2 project", lexicon)
        assert matches
        assert matches[0][0] == "compliance_initiative"

    def test_no_match(self):
        lexicon = [{"signal_code": "test", "keyword": "kubernetes", "confidence": "0.9"}]
        matches = _match_signals("Quarterly earnings report released", lexicon)
        assert matches == []


class TestGoogleNewsRssUrl:
    def test_contains_google_domain(self):
        url = _google_news_rss_url('"acme.com" cloud cost')
        assert url.startswith("https://news.google.com/rss/search?q=")
        assert "acme.com" in url

    def test_url_encodes_query(self):
        url = _google_news_rss_url("term with spaces")
        assert "+" in url or "%20" in url


class TestNewsParseEntryObservedAt:
    def test_published_parsed(self):
        ts = time.gmtime(1735689600)
        result = news_parse_observed_at({"published_parsed": ts})
        assert result.startswith("2025-01-01")

    def test_falls_back_to_published_string(self):
        result = news_parse_observed_at({"published": "2025-06-15T12:00:00Z"})
        assert result == "2025-06-15T12:00:00Z"

    def test_falls_back_to_utc_now(self):
        result = news_parse_observed_at({})
        assert "T" in result  # ISO format


class TestNewsBuildObservation:
    def test_creates_valid_observation(self):
        obs = news_build_observation(
            account_id="a1",
            signal_code="compliance_initiative",
            source="news_google",
            observed_at="2026-01-01T00:00:00Z",
            confidence=0.8,
            source_reliability=0.85,
            evidence_url="https://example.com/news",
            evidence_text="SOC 2 audit",
            payload={"title": "audit"},
        )
        assert obs.source == "news_google"
        assert obs.obs_id.startswith("obs_")


# ---------------------------------------------------------------------------
# Community collector
# ---------------------------------------------------------------------------


class TestRedditSearchRssUrl:
    def test_contains_reddit_domain(self):
        url = _reddit_search_rss_url('"acme.com" devops')
        assert url.startswith("https://www.reddit.com/search.rss?q=")
        assert "acme.com" in url

    def test_sort_and_time_params(self):
        url = _reddit_search_rss_url("test")
        assert "sort=new" in url
        assert "t=month" in url


class TestCommunityParseEntryObservedAt:
    def test_published_parsed(self):
        ts = time.gmtime(1735689600)
        result = community_parse_observed_at({"published_parsed": ts})
        assert result.startswith("2025-01-01")

    def test_falls_back_to_published_string(self):
        result = community_parse_observed_at({"published": "Mon, 15 Jun 2025 12:00:00 GMT"})
        assert "Mon, 15 Jun 2025" in result

    def test_falls_back_to_utc_now(self):
        result = community_parse_observed_at({})
        assert "T" in result


class TestCommunityBuildObservation:
    def test_creates_valid_observation(self):
        obs = community_build_observation(
            account_id="a1",
            signal_code="community_mention",
            source="community_reddit",
            observed_at="2026-01-01T00:00:00Z",
            confidence=0.7,
            source_reliability=0.6,
            evidence_url="https://reddit.com/r/devops/post",
            evidence_text="Discussion about devops",
            payload={"entry": {"title": "DevOps post"}},
        )
        assert obs.source == "community_reddit"


# ---------------------------------------------------------------------------
# Technographics collector
# ---------------------------------------------------------------------------


class TestTechnographicsConstants:
    def test_discovery_link_tokens(self):
        assert "technology" in DISCOVERY_LINK_TOKENS
        assert "cloud" in DISCOVERY_LINK_TOKENS
        assert "careers" in DISCOVERY_LINK_TOKENS

    def test_max_scan_text_chars(self):
        assert MAX_SCAN_TEXT_CHARS == 8000


class TestTechnographicsBuildObservation:
    def test_creates_valid_observation(self):
        obs = tech_build_observation(
            account_id="a1",
            signal_code="kubernetes_usage",
            source="technographics_scan",
            observed_at="2026-01-01T00:00:00Z",
            confidence=0.9,
            source_reliability=0.8,
            evidence_url="https://example.com",
            evidence_text="K8s detected",
            payload={"domain": "example.com"},
        )
        assert obs.signal_code == "kubernetes_usage"


# ---------------------------------------------------------------------------
# First party collector
# ---------------------------------------------------------------------------


class TestFirstPartyConstants:
    def test_valid_products(self):
        assert VALID_PRODUCTS == {"zopdev", "zopday", "zopnight", "shared"}
