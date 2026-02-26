"""Tests for src.integrations.apollo — Apollo.io client + Hunter fallback."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.integrations.apollo import (
    ApolloClient,
    ApolloContact,
    ApolloSearchResult,
    _infer_management_level,
    _is_generic_email,
    find_email_via_hunter,
    search_contacts_for_account,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _mock_response(status_code: int = 200, json_data: dict | None = None):
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data or {}
    return resp


# ---------------------------------------------------------------------------
# _is_generic_email
# ---------------------------------------------------------------------------


class TestIsGenericEmail:
    def test_generic_prefixes(self):
        for prefix in ("info", "sales", "support", "contact", "noreply", "no-reply"):
            assert _is_generic_email(f"{prefix}@example.com") is True

    def test_personal_email(self):
        assert _is_generic_email("jane.doe@example.com") is False

    def test_empty_string(self):
        assert _is_generic_email("") is False


# ---------------------------------------------------------------------------
# _infer_management_level
# ---------------------------------------------------------------------------


class TestInferManagementLevel:
    @pytest.mark.parametrize(
        "title,expected",
        [
            ("Chief Technology Officer", "C-Level"),
            ("CTO", "C-Level"),
            ("CEO & Co-Founder", "C-Level"),
            ("VP Engineering", "VP"),
            ("Vice President of Sales", "VP"),
            ("Director of Engineering", "Director"),
            ("Head of Cloud", "Director"),
            ("Engineering Manager", "Manager"),
            ("DevOps Lead", "Manager"),
            ("Software Engineer", "IC"),
            ("", "IC"),
        ],
    )
    def test_levels(self, title: str, expected: str):
        assert _infer_management_level(title) == expected


# ---------------------------------------------------------------------------
# ApolloClient — search_people
# ---------------------------------------------------------------------------


class TestApolloClientSearchPeople:
    def test_returns_contacts_on_success(self):
        client = ApolloClient(api_key="test-key", rate_limit=100)
        api_response = {
            "people": [
                {
                    "first_name": "Jane",
                    "last_name": "Doe",
                    "title": "CTO",
                    "email": "jane.doe@example.com",
                    "linkedin_url": "https://linkedin.com/in/janedoe",
                },
            ],
            "pagination": {"total_entries": 1},
        }
        with patch("src.integrations.apollo.requests.post", return_value=_mock_response(200, api_response)):
            result = client.search_people("example.com", title_keywords=["CTO"], limit=3)

        assert isinstance(result, ApolloSearchResult)
        assert len(result.contacts) == 1
        assert result.contacts[0].first_name == "Jane"
        assert result.contacts[0].management_level == "C-Level"
        assert result.total_found == 1
        assert result.api_credits_used == 1

    def test_filters_generic_emails(self):
        client = ApolloClient(api_key="test-key", rate_limit=100)
        api_response = {
            "people": [
                {
                    "first_name": "John",
                    "last_name": "Smith",
                    "title": "VP Engineering",
                    "email": "info@example.com",
                    "linkedin_url": "",
                },
            ],
            "pagination": {"total_entries": 1},
        }
        with patch("src.integrations.apollo.requests.post", return_value=_mock_response(200, api_response)):
            result = client.search_people("example.com")

        assert result.contacts[0].email == ""

    def test_returns_empty_on_api_error(self):
        client = ApolloClient(api_key="test-key", rate_limit=100)
        with patch("src.integrations.apollo.requests.post", return_value=_mock_response(429)):
            result = client.search_people("example.com")

        assert result.contacts == []
        assert result.total_found == 0

    def test_returns_empty_when_no_api_key(self):
        client = ApolloClient(api_key="", rate_limit=100)
        result = client.search_people("example.com")
        assert result.contacts == []

    def test_returns_empty_on_exception(self):
        client = ApolloClient(api_key="test-key", rate_limit=100)
        with patch("src.integrations.apollo.requests.post", side_effect=ConnectionError("timeout")):
            result = client.search_people("example.com")

        assert result.contacts == []


# ---------------------------------------------------------------------------
# ApolloClient — enrich_person
# ---------------------------------------------------------------------------


class TestApolloClientEnrichPerson:
    def test_returns_contact_on_success(self):
        client = ApolloClient(api_key="test-key", rate_limit=100)
        api_response = {
            "person": {
                "first_name": "Alice",
                "last_name": "Wong",
                "title": "Director of Engineering",
                "email": "alice@example.com",
                "linkedin_url": "https://linkedin.com/in/alicewong",
            },
        }
        with patch("src.integrations.apollo.requests.post", return_value=_mock_response(200, api_response)):
            contact = client.enrich_person("alice@example.com")

        assert contact is not None
        assert contact.first_name == "Alice"
        assert contact.management_level == "Director"

    def test_returns_none_on_missing_person(self):
        client = ApolloClient(api_key="test-key", rate_limit=100)
        with patch("src.integrations.apollo.requests.post", return_value=_mock_response(200, {})):
            contact = client.enrich_person("nobody@example.com")

        assert contact is None

    def test_returns_none_when_no_api_key(self):
        client = ApolloClient(api_key="", rate_limit=100)
        assert client.enrich_person("test@example.com") is None

    def test_returns_none_when_empty_email(self):
        client = ApolloClient(api_key="test-key", rate_limit=100)
        assert client.enrich_person("") is None


# ---------------------------------------------------------------------------
# find_email_via_hunter
# ---------------------------------------------------------------------------


class TestFindEmailViaHunter:
    def test_returns_email_on_success(self):
        hunter_response = {"data": {"email": "jane@example.com"}}
        with patch("src.integrations.apollo.requests.get", return_value=_mock_response(200, hunter_response)):
            email = find_email_via_hunter("example.com", "Jane", "Doe", "hunter-key")

        assert email == "jane@example.com"

    def test_filters_generic_email(self):
        hunter_response = {"data": {"email": "info@example.com"}}
        with patch("src.integrations.apollo.requests.get", return_value=_mock_response(200, hunter_response)):
            email = find_email_via_hunter("example.com", "Jane", "Doe", "hunter-key")

        assert email == ""

    def test_returns_empty_on_api_error(self):
        with patch("src.integrations.apollo.requests.get", return_value=_mock_response(404)):
            email = find_email_via_hunter("example.com", "Jane", "Doe", "hunter-key")

        assert email == ""

    def test_returns_empty_when_no_api_key(self):
        assert find_email_via_hunter("example.com", "Jane", "Doe", "") == ""

    def test_returns_empty_when_missing_name(self):
        assert find_email_via_hunter("example.com", "", "Doe", "hunter-key") == ""
        assert find_email_via_hunter("example.com", "Jane", "", "hunter-key") == ""

    def test_returns_empty_on_exception(self):
        with patch("src.integrations.apollo.requests.get", side_effect=ConnectionError("fail")):
            email = find_email_via_hunter("example.com", "Jane", "Doe", "hunter-key")

        assert email == ""


# ---------------------------------------------------------------------------
# search_contacts_for_account (integration of Apollo + Hunter)
# ---------------------------------------------------------------------------


class TestSearchContactsForAccount:
    def test_returns_dicts_with_hunter_fallback(self):
        apollo_response = {
            "people": [
                {
                    "first_name": "Bob",
                    "last_name": "Lee",
                    "title": "CTO",
                    "email": "",
                    "linkedin_url": "https://linkedin.com/in/boblee",
                },
            ],
            "pagination": {"total_entries": 1},
        }
        hunter_response = {"data": {"email": "bob.lee@example.com"}}
        client = ApolloClient(api_key="test-key", rate_limit=100)

        with (
            patch("src.integrations.apollo.requests.post", return_value=_mock_response(200, apollo_response)),
            patch("src.integrations.apollo.requests.get", return_value=_mock_response(200, hunter_response)),
        ):
            contacts = search_contacts_for_account(
                domain="example.com",
                apollo_client=client,
                hunter_api_key="hunter-key",
                tier="high",
            )

        assert len(contacts) == 1
        assert contacts[0]["email"] == "bob.lee@example.com"
        assert contacts[0]["first_name"] == "Bob"
        assert contacts[0]["management_level"] == "C-Level"

    def test_skips_contacts_missing_name(self):
        apollo_response = {
            "people": [
                {
                    "first_name": "",
                    "last_name": "",
                    "title": "CTO",
                    "email": "anon@example.com",
                    "linkedin_url": "",
                },
            ],
            "pagination": {"total_entries": 1},
        }
        client = ApolloClient(api_key="test-key", rate_limit=100)

        with patch("src.integrations.apollo.requests.post", return_value=_mock_response(200, apollo_response)):
            contacts = search_contacts_for_account(
                domain="example.com",
                apollo_client=client,
            )

        assert contacts == []

    def test_works_without_apollo_client(self):
        contacts = search_contacts_for_account(
            domain="example.com",
            apollo_client=None,
        )
        assert contacts == []

    def test_tier2_uses_fewer_role_groups(self):
        client = ApolloClient(api_key="test-key", rate_limit=100)
        apollo_response = {"people": [], "pagination": {"total_entries": 0}}

        with patch(
            "src.integrations.apollo.requests.post", return_value=_mock_response(200, apollo_response)
        ) as mock_post:
            search_contacts_for_account(
                domain="example.com",
                apollo_client=client,
                tier="tier_2",
            )

        call_payload = mock_post.call_args[1]["json"]
        # Tier 2 should only have titles from TIER_1_ROLES[0]
        assert "person_titles" in call_payload
        assert "CTO" in call_payload["person_titles"]
        assert "CFO" not in call_payload["person_titles"]
