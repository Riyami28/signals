"""Tests for src/research/parser.py — pure unit tests, no DB needed."""

from __future__ import annotations

from src.research.parser import (
    parse_extraction_response,
    parse_scoring_response,
    CompanyEnrichment,
    ParsedExtractionResponse,
    ParsedScoringResponse,
)


# ---------------------------------------------------------------------------
# Helpers to build realistic Claude responses
# ---------------------------------------------------------------------------

def _make_extraction_response(
    enrichment_json: str = "",
    brief: str = "",
    *,
    preamble: str = "",
) -> str:
    """Build a multi-section extraction response string."""
    parts: list[str] = []
    if preamble:
        parts.append(preamble)
    if enrichment_json:
        parts.append(f"### ENRICHMENT_JSON\n```json\n{enrichment_json}\n```")
    if brief:
        parts.append(f"### RESEARCH_BRIEF\n{brief}")
    return "\n\n".join(parts)


def _make_scoring_response(
    contacts_json: str = "",
    starters: str = "",
) -> str:
    parts: list[str] = []
    if contacts_json:
        parts.append(f"### CONTACTS_JSON\n```json\n{contacts_json}\n```")
    if starters:
        parts.append(f"### CONVERSATION_STARTERS\n{starters}")
    return "\n\n".join(parts)


_WELL_FORMED_ENRICHMENT = """{
    "website": "https://acme.com",
    "website_confidence": 0.95,
    "industry": "SaaS",
    "industry_confidence": 0.9,
    "sub_industry": "DevOps",
    "sub_industry_confidence": 0.85,
    "employees": 250,
    "employees_confidence": 0.8,
    "employee_range": "201-500",
    "employee_range_confidence": 0.8,
    "revenue_range": "$50M-$100M",
    "revenue_range_confidence": 0.7,
    "company_linkedin_url": "https://linkedin.com/company/acme",
    "company_linkedin_url_confidence": 0.95,
    "city": "San Francisco",
    "city_confidence": 0.9,
    "state": "CA",
    "state_confidence": 0.9,
    "country": "US",
    "country_confidence": 0.95,
    "tech_stack": ["Kubernetes", "Terraform", "AWS"],
    "tech_stack_confidence": 0.75
}"""


_WELL_FORMED_BRIEF = (
    "Acme Corp is a mid-market SaaS company specializing in DevOps tooling. "
    "They have recently expanded their cloud infrastructure team and show "
    "strong indicators of Kubernetes adoption across multiple environments."
)


# ===========================================================================
# TestParseExtractionResponse
# ===========================================================================


class TestParseExtractionResponse:
    def test_well_formed_response_parses_all_fields(self):
        raw = _make_extraction_response(_WELL_FORMED_ENRICHMENT, _WELL_FORMED_BRIEF)
        result = parse_extraction_response(raw)

        assert isinstance(result, ParsedExtractionResponse)
        e = result.enrichment
        assert e.website == "https://acme.com"
        assert e.industry == "SaaS"
        assert e.sub_industry == "DevOps"
        assert e.employees == 250
        assert e.employee_range == "201-500"
        assert e.revenue_range == "$50M-$100M"
        assert e.company_linkedin_url == "https://linkedin.com/company/acme"
        assert e.city == "San Francisco"
        assert e.state == "CA"
        assert e.country == "US"
        assert e.tech_stack == ["Kubernetes", "Terraform", "AWS"]
        assert "Acme Corp" in result.research_brief

    def test_missing_enrichment_section_returns_empty_enrichment_with_error(self):
        raw = "### RESEARCH_BRIEF\nAcme is a great company."
        result = parse_extraction_response(raw)

        assert result.enrichment.website == ""
        assert result.enrichment.industry == ""
        assert any("ENRICHMENT_JSON" in e or "enrichment" in e.lower() for e in result.parse_errors)

    def test_missing_brief_section_returns_empty_brief_with_error(self):
        raw = f"### ENRICHMENT_JSON\n```json\n{_WELL_FORMED_ENRICHMENT}\n```"
        result = parse_extraction_response(raw)

        # Enrichment should be parsed
        assert result.enrichment.industry == "SaaS"
        # Brief should be empty (or fallback might catch some text)
        # Parse errors should mention the brief
        assert any("RESEARCH_BRIEF" in e or "brief" in e.lower() for e in result.parse_errors)

    def test_malformed_json_in_enrichment_returns_empty_with_error(self):
        raw = _make_extraction_response(
            enrichment_json='{"industry": "SaaS", broken!!!',
            brief=_WELL_FORMED_BRIEF,
        )
        result = parse_extraction_response(raw)

        assert result.enrichment.industry == ""
        assert any("parse" in e.lower() or "json" in e.lower() for e in result.parse_errors)
        assert "Acme Corp" in result.research_brief

    def test_confidence_below_0_5_zeros_out_field(self):
        low_conf_json = """{
            "industry": "SaaS",
            "industry_confidence": 0.3,
            "website": "https://acme.com",
            "website_confidence": 0.9
        }"""
        raw = _make_extraction_response(low_conf_json, _WELL_FORMED_BRIEF)
        result = parse_extraction_response(raw)

        assert result.enrichment.industry == ""
        assert result.enrichment.website == "https://acme.com"
        assert any("industry" in e and "0.30" in e for e in result.parse_errors)

    def test_extra_prose_before_sections_is_tolerated(self):
        raw = _make_extraction_response(
            _WELL_FORMED_ENRICHMENT,
            _WELL_FORMED_BRIEF,
            preamble="Here's what I found after researching the company:\n",
        )
        result = parse_extraction_response(raw)

        assert result.enrichment.industry == "SaaS"
        assert "Acme Corp" in result.research_brief

    def test_never_raises_on_completely_empty_string(self):
        result = parse_extraction_response("")
        assert result.parse_errors
        assert isinstance(result.enrichment, CompanyEnrichment)
        assert result.research_brief == ""


# ===========================================================================
# TestParseScoringResponse
# ===========================================================================


class TestParseScoringResponse:
    def test_well_formed_response_parses_contacts_and_starters(self):
        contacts_json = """[
            {
                "first_name": "Jane",
                "last_name": "Doe",
                "title": "VP Engineering",
                "email": "jane@acme.com",
                "linkedin_url": "https://linkedin.com/in/janedoe",
                "management_level": "VP",
                "year_joined": 2020
            },
            {
                "first_name": "John",
                "last_name": "Smith",
                "title": "CTO",
                "management_level": "C-Level"
            }
        ]"""
        starters = (
            "- Their recent Kubernetes migration is a perfect entry point for discussing ZopDev.\n"
            "- The VP Engineering hire suggests they are scaling their platform team.\n"
            "- Ask about their multi-cloud strategy given the Terraform adoption.\n"
        )
        raw = _make_scoring_response(contacts_json, starters)
        result = parse_scoring_response(raw)

        assert len(result.contacts) == 2
        assert result.contacts[0].first_name == "Jane"
        assert result.contacts[0].management_level == "VP"
        assert result.contacts[0].year_joined == 2020
        assert result.contacts[1].management_level == "C-Level"
        assert len(result.conversation_starters) == 3

    def test_empty_contacts_array_is_valid(self):
        raw = _make_scoring_response(
            contacts_json="[]",
            starters="- No specific conversation starters available.\n",
        )
        result = parse_scoring_response(raw)
        assert result.contacts == []
        assert not any("contacts" in e.lower() for e in result.parse_errors)

    def test_contact_missing_linkedin_url_is_still_included(self):
        contacts_json = """[
            {
                "first_name": "Alice",
                "last_name": "Wong",
                "title": "Director of Engineering"
            }
        ]"""
        raw = _make_scoring_response(contacts_json, "- Starter one\n")
        result = parse_scoring_response(raw)

        assert len(result.contacts) == 1
        assert result.contacts[0].first_name == "Alice"
        assert result.contacts[0].linkedin_url == ""
        assert result.contacts[0].management_level == "IC"  # default

    def test_malformed_contacts_json_returns_empty_list_with_error(self):
        raw = _make_scoring_response(
            contacts_json='[{"first_name": "broken',
            starters="- A conversation starter\n",
        )
        result = parse_scoring_response(raw)

        assert result.contacts == []
        assert any("parse" in e.lower() or "json" in e.lower() for e in result.parse_errors)

    def test_numbered_conversation_starters_are_parsed(self):
        starters = "1. First starter about Kubernetes\n2. Second starter about Terraform\n3. Third starter about cloud costs\n"
        raw = _make_scoring_response(contacts_json="[]", starters=starters)
        result = parse_scoring_response(raw)

        assert len(result.conversation_starters) == 3
        assert "Kubernetes" in result.conversation_starters[0]

    def test_bulleted_conversation_starters_are_parsed(self):
        starters = "- First starter\n- Second starter\n"
        raw = _make_scoring_response(contacts_json="[]", starters=starters)
        result = parse_scoring_response(raw)

        assert len(result.conversation_starters) == 2
        assert result.conversation_starters[0] == "First starter"

    def test_never_raises_on_garbage_input(self):
        result = parse_scoring_response("!@#$%^&*() random garbage")
        assert isinstance(result.contacts, list)
        assert isinstance(result.conversation_starters, list)
        assert isinstance(result, ParsedScoringResponse)
