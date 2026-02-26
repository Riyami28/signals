"""Tests for Zoho CRM dedup integration (src/integrations/zoho_dedup.py)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.integrations.zoho_dedup import (
    ZohoAuthError,
    ZohoCRMDedupClient,
    ZohoDedupError,
    check_crm_dedup,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def mock_settings():
    """Return a mock Settings object with Zoho credentials configured."""
    settings = MagicMock()
    settings.zoho_client_id = "test_client_id"
    settings.zoho_client_secret = "test_client_secret"
    settings.zoho_refresh_token = "test_refresh_token"
    settings.zoho_api_base_url = "https://www.zohoapis.com/crm/v3"
    settings.zoho_auth_url = "https://accounts.zoho.com/oauth/v2/token"
    settings.zoho_dedup_enabled = True
    return settings


@pytest.fixture()
def mock_settings_unconfigured():
    """Return a mock Settings object with Zoho credentials NOT configured."""
    settings = MagicMock()
    settings.zoho_client_id = ""
    settings.zoho_client_secret = ""
    settings.zoho_refresh_token = ""
    settings.zoho_api_base_url = "https://www.zohoapis.com/crm/v3"
    settings.zoho_auth_url = "https://accounts.zoho.com/oauth/v2/token"
    settings.zoho_dedup_enabled = False
    return settings


@pytest.fixture()
def client(mock_settings):
    """Return a ZohoCRMDedupClient with pre-set access token (skip auth)."""
    c = ZohoCRMDedupClient(mock_settings)
    c._access_token = "test_token"
    c._token_expires_at = 9999999999.0
    return c


# ---------------------------------------------------------------------------
# ZohoCRMDedupClient tests
# ---------------------------------------------------------------------------


class TestZohoCRMDedupClient:
    def test_is_configured_when_all_creds_present(self, client):
        assert client.is_configured is True

    def test_is_not_configured_when_creds_missing(self, mock_settings_unconfigured):
        c = ZohoCRMDedupClient(mock_settings_unconfigured)
        assert c.is_configured is False

    @patch("src.integrations.zoho_dedup.requests.post")
    def test_refresh_access_token_success(self, mock_post, mock_settings):
        mock_post.return_value.status_code = 200
        mock_post.return_value.json.return_value = {
            "access_token": "new_token",
            "expires_in": 3600,
        }
        mock_post.return_value.raise_for_status = MagicMock()
        c = ZohoCRMDedupClient(mock_settings)
        token = c._refresh_access_token()
        assert token == "new_token"
        assert c._access_token == "new_token"

    @patch("src.integrations.zoho_dedup.requests.post")
    def test_refresh_access_token_failure(self, mock_post, mock_settings):
        mock_post.return_value.status_code = 200
        mock_post.return_value.json.return_value = {"error": "invalid_grant"}
        mock_post.return_value.raise_for_status = MagicMock()
        c = ZohoCRMDedupClient(mock_settings)
        with pytest.raises(ZohoAuthError):
            c._refresh_access_token()

    @patch("src.integrations.zoho_dedup.requests.request")
    def test_search_account_found(self, mock_request, client):
        mock_request.return_value.status_code = 200
        mock_request.return_value.json.return_value = {
            "data": [{"id": "12345", "Account_Name": "Acme Corp", "Website": "acme.com"}]
        }
        mock_request.return_value.raise_for_status = MagicMock()

        result = client.search_account("acme.com")
        assert result is not None
        assert result["id"] == "12345"

    @patch("src.integrations.zoho_dedup.requests.request")
    def test_search_account_not_found(self, mock_request, client):
        mock_request.return_value.status_code = 204
        mock_request.return_value.json.return_value = {"data": []}

        result = client.search_account("unknown.com")
        assert result is None

    @patch("src.integrations.zoho_dedup.requests.request")
    def test_search_lead_found(self, mock_request, client):
        mock_request.return_value.status_code = 200
        mock_request.return_value.json.return_value = {"data": [{"id": "67890", "Company": "Beta Inc"}]}
        mock_request.return_value.raise_for_status = MagicMock()

        result = client.search_lead("beta.com")
        assert result is not None
        assert result["id"] == "67890"

    @patch("src.integrations.zoho_dedup.requests.request")
    def test_search_lead_not_found(self, mock_request, client):
        mock_request.return_value.status_code = 204
        mock_request.return_value.json.return_value = {"data": []}

        result = client.search_lead("unknown.com")
        assert result is None

    @patch("src.integrations.zoho_dedup.requests.request")
    def test_search_lead_by_company_name(self, mock_request, client):
        mock_request.return_value.status_code = 200
        mock_request.return_value.json.return_value = {"data": [{"id": "11111", "Company": "Gamma LLC"}]}
        mock_request.return_value.raise_for_status = MagicMock()

        result = client.search_lead_by_company_name("Gamma LLC")
        assert result is not None
        assert result["id"] == "11111"

    def test_search_lead_by_company_name_empty(self, client):
        result = client.search_lead_by_company_name("")
        assert result is None


# ---------------------------------------------------------------------------
# is_existing() integration tests
# ---------------------------------------------------------------------------


class TestIsExisting:
    @patch("src.integrations.zoho_dedup.requests.request")
    def test_found_as_account(self, mock_request, client):
        """Domain found in Accounts → existing_customer."""
        mock_request.return_value.status_code = 200
        mock_request.return_value.json.return_value = {"data": [{"id": "acc_1", "Website": "acme.com"}]}
        mock_request.return_value.raise_for_status = MagicMock()

        is_exist, status = client.is_existing("acme.com")
        assert is_exist is True
        assert status == "existing_customer"

    @patch("src.integrations.zoho_dedup.requests.request")
    def test_found_as_lead_by_domain(self, mock_request, client):
        """Domain not in Accounts, found in Leads → existing_lead."""
        call_count = 0

        def side_effect(method, url, **kwargs):
            nonlocal call_count
            call_count += 1
            resp = MagicMock()
            if "Accounts" in url:
                resp.status_code = 204
                resp.json.return_value = {"data": []}
            else:
                resp.status_code = 200
                resp.json.return_value = {"data": [{"id": "lead_1", "Company": "beta.com"}]}
                resp.raise_for_status = MagicMock()
            return resp

        mock_request.side_effect = side_effect

        is_exist, status = client.is_existing("beta.com")
        assert is_exist is True
        assert status == "existing_lead"

    @patch("src.integrations.zoho_dedup.requests.request")
    def test_found_as_lead_by_company_name(self, mock_request, client):
        """Domain not found anywhere, company name match in Leads."""
        call_count = 0

        def side_effect(method, url, **kwargs):
            nonlocal call_count
            call_count += 1
            resp = MagicMock()
            if "Company:equals:Gamma" in url:
                resp.status_code = 200
                resp.json.return_value = {"data": [{"id": "lead_2", "Company": "Gamma"}]}
                resp.raise_for_status = MagicMock()
            else:
                resp.status_code = 204
                resp.json.return_value = {"data": []}
            return resp

        mock_request.side_effect = side_effect

        is_exist, status = client.is_existing("gamma.com", company_name="Gamma")
        assert is_exist is True
        assert status == "existing_lead"

    @patch("src.integrations.zoho_dedup.requests.request")
    def test_not_found_anywhere(self, mock_request, client):
        """Domain not in Accounts or Leads → new."""
        mock_request.return_value.status_code = 204
        mock_request.return_value.json.return_value = {"data": []}

        is_exist, status = client.is_existing("brand-new.com")
        assert is_exist is False
        assert status == "new"


# ---------------------------------------------------------------------------
# check_crm_dedup() high-level function tests
# ---------------------------------------------------------------------------


class TestCheckCrmDedup:
    def test_unconfigured_returns_new(self, mock_settings_unconfigured):
        result = check_crm_dedup("example.com", "Example", mock_settings_unconfigured)
        assert result == "new"

    @patch("src.integrations.zoho_dedup.requests.request")
    def test_returns_existing_customer(self, mock_request, mock_settings):
        # Pre-create a client with valid token.
        client = ZohoCRMDedupClient(mock_settings)
        client._access_token = "test_token"
        client._token_expires_at = 9999999999.0

        mock_request.return_value.status_code = 200
        mock_request.return_value.json.return_value = {"data": [{"id": "acc_1", "Website": "acme.com"}]}
        mock_request.return_value.raise_for_status = MagicMock()

        result = check_crm_dedup("acme.com", "Acme Corp", mock_settings, client=client)
        assert result == "existing_customer"

    @patch("src.integrations.zoho_dedup.requests.request")
    def test_returns_new_when_not_found(self, mock_request, mock_settings):
        client = ZohoCRMDedupClient(mock_settings)
        client._access_token = "test_token"
        client._token_expires_at = 9999999999.0

        mock_request.return_value.status_code = 204
        mock_request.return_value.json.return_value = {"data": []}

        result = check_crm_dedup("brand-new.com", "Brand New", mock_settings, client=client)
        assert result == "new"


# ---------------------------------------------------------------------------
# Rate limiter tests
# ---------------------------------------------------------------------------


class TestRateLimiter:
    def test_rate_limiter_doesnt_block_first_call(self):
        from src.integrations.zoho_dedup import _RateLimiter

        limiter = _RateLimiter(min_interval=0.01)
        import time

        start = time.monotonic()
        limiter.wait()
        elapsed = time.monotonic() - start
        # First call should be essentially instant.
        assert elapsed < 0.05
