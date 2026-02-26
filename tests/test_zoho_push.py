"""Tests for Zoho CRM push integration."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

from src.integrations.zoho import (
    ZohoClient,
    build_account_payload,
    build_contact_payload,
    build_deal_payload,
    build_tags,
)
from src.sync.zoho_push import (
    _classify_confidence,
    _should_auto_push,
    run_zoho_push,
)

# ---------------------------------------------------------------------------
# Payload builder tests
# ---------------------------------------------------------------------------


class TestBuildAccountPayload:
    def test_basic_fields(self):
        payload = build_account_payload(
            company_name="Acme Corp",
            domain="acme.com",
            score=85.5,
            tier="high",
            enrichment={"employees": 500, "industry": "Technology"},
            top_reasons=[
                {"signal_code": "devops_hiring", "evidence_text": "Hiring DevOps engineers"},
                {"signal_code": "cloud_cost_spike", "evidence_text": "Cloud costs rising"},
            ],
        )
        assert payload["Account_Name"] == "Acme Corp"
        assert payload["Website"] == "acme.com"
        assert payload["ICP_Score"] == 85.5
        assert payload["ICP_Tier"] == "high"
        assert payload["Lead_Source"] == "Signals Pipeline"
        assert payload["Employee_Count"] == 500
        assert payload["Industry"] == "Technology"
        assert "devops_hiring" in payload["Trigger_Signals"]
        assert "cloud_cost_spike" in payload["Trigger_Signals"]

    def test_empty_enrichment(self):
        payload = build_account_payload(
            company_name="Test Co",
            domain="test.com",
            score=10.0,
            tier="medium",
            enrichment={},
            top_reasons=[],
        )
        assert payload["Account_Name"] == "Test Co"
        assert "Employee_Count" not in payload
        assert "Industry" not in payload
        assert payload["Trigger_Signals"] == ""

    def test_custom_lead_source(self):
        payload = build_account_payload(
            company_name="X",
            domain="x.com",
            score=50.0,
            tier="high",
            enrichment={},
            top_reasons=[],
            lead_source="Custom Source",
        )
        assert payload["Lead_Source"] == "Custom Source"

    def test_dimension_scores_included(self):
        dimensions = {"trigger_intent": 85, "tech_fit": 70}
        payload = build_account_payload(
            company_name="D",
            domain="d.com",
            score=90.0,
            tier="high",
            enrichment={},
            top_reasons=[],
            dimension_scores=dimensions,
        )
        assert "Dimension_Scores" in payload
        parsed = json.loads(payload["Dimension_Scores"])
        assert parsed["trigger_intent"] == 85


class TestBuildContactPayload:
    def test_basic_contact(self):
        contact = {
            "first_name": "Jane",
            "last_name": "Doe",
            "email": "jane@acme.com",
            "title": "VP Engineering",
            "linkedin_url": "https://linkedin.com/in/janedoe",
            "management_level": "VP",
        }
        payload = build_contact_payload(contact, "zoho_acc_123")
        assert payload["First_Name"] == "Jane"
        assert payload["Last_Name"] == "Doe"
        assert payload["Email"] == "jane@acme.com"
        assert payload["Title"] == "VP Engineering"
        assert payload["Account_Name"] == {"id": "zoho_acc_123"}

    def test_minimal_contact(self):
        contact = {"first_name": "", "last_name": "Smith"}
        payload = build_contact_payload(contact, "zoho_acc_456")
        assert payload["Last_Name"] == "Smith"
        assert "Email" not in payload
        assert "Title" not in payload


class TestBuildDealPayload:
    def test_basic_deal(self):
        payload = build_deal_payload(
            company_name="Acme Corp",
            zoho_account_id="zoho_acc_123",
            score=90.0,
            tier="high",
            stage="New Lead",
            close_days=60,
        )
        assert "Acme Corp" in payload["Deal_Name"]
        assert payload["Account_Name"] == {"id": "zoho_acc_123"}
        assert payload["Stage"] == "New Lead"
        assert payload["ICP_Score"] == 90.0
        assert payload["ICP_Tier"] == "high"
        assert payload["Closing_Date"]  # Should be a date string


class TestBuildTags:
    def test_basic_tags(self):
        tags = build_tags("high", [{"signal_code": "devops_hiring"}, {"signal_code": "cloud_cost"}])
        assert "icp_high" in tags
        assert "signals_pipeline" in tags
        assert "devops_hiring" in tags
        assert "cloud_cost" in tags

    def test_empty_reasons(self):
        tags = build_tags("medium", [])
        assert tags == ["icp_medium", "signals_pipeline"]


# ---------------------------------------------------------------------------
# Push policy tests
# ---------------------------------------------------------------------------


class TestClassifyConfidence:
    def test_high_tier_high_score(self):
        assert _classify_confidence(30.0, "high") == "high"

    def test_high_tier_low_score(self):
        assert _classify_confidence(22.0, "high") == "low"

    def test_medium_tier_high_score(self):
        assert _classify_confidence(16.0, "medium") == "high"

    def test_medium_tier_low_score(self):
        assert _classify_confidence(11.0, "medium") == "low"

    def test_low_tier(self):
        assert _classify_confidence(5.0, "low") == "low"


class TestShouldAutoPush:
    def test_high_tier_high_confidence(self):
        assert _should_auto_push("high", "high", ("high", "medium")) is True

    def test_high_tier_low_confidence(self):
        assert _should_auto_push("high", "low", ("high", "medium")) is False

    def test_medium_tier_high_confidence(self):
        assert _should_auto_push("medium", "high", ("high", "medium")) is True

    def test_medium_tier_low_confidence(self):
        assert _should_auto_push("medium", "low", ("high", "medium")) is False

    def test_low_tier_excluded(self):
        assert _should_auto_push("low", "high", ("high", "medium")) is False

    def test_custom_tiers(self):
        assert _should_auto_push("high", "high", ("high",)) is True
        assert _should_auto_push("medium", "high", ("high",)) is False


# ---------------------------------------------------------------------------
# ZohoClient tests (mocked HTTP)
# ---------------------------------------------------------------------------


class TestZohoClient:
    def _make_settings(self):
        settings = MagicMock()
        settings.zoho_client_id = "test_client_id"
        settings.zoho_client_secret = "test_client_secret"
        settings.zoho_refresh_token = "test_refresh_token"
        settings.zoho_api_base_url = "https://www.zohoapis.com/crm/v3"
        settings.zoho_auth_url = "https://accounts.zoho.com/oauth/v2/token"
        return settings

    def test_is_configured(self):
        settings = self._make_settings()
        client = ZohoClient(settings)
        assert client.is_configured is True

    def test_not_configured_missing_client_id(self):
        settings = self._make_settings()
        settings.zoho_client_id = ""
        client = ZohoClient(settings)
        assert client.is_configured is False

    @patch("src.integrations.zoho.requests.post")
    def test_token_refresh(self, mock_post):
        mock_post.return_value = MagicMock(
            status_code=200,
            json=lambda: {"access_token": "new_token", "expires_in": 3600},
        )
        mock_post.return_value.raise_for_status = MagicMock()

        settings = self._make_settings()
        client = ZohoClient(settings)
        token = client._refresh_access_token()
        assert token == "new_token"
        assert client._access_token == "new_token"

    @patch("src.integrations.zoho.requests.request")
    @patch("src.integrations.zoho.requests.post")
    def test_upsert_account(self, mock_post, mock_request):
        mock_post.return_value = MagicMock(
            status_code=200,
            json=lambda: {"access_token": "tok", "expires_in": 3600},
        )
        mock_post.return_value.raise_for_status = MagicMock()

        mock_request.return_value = MagicMock(
            status_code=200,
            json=lambda: {"data": [{"code": "SUCCESS", "details": {"id": "zoho_123"}, "action": "insert"}]},
        )
        mock_request.return_value.raise_for_status = MagicMock()

        settings = self._make_settings()
        client = ZohoClient(settings)
        result = client.upsert_account({"Account_Name": "Test", "Website": "test.com"})
        assert result["data"][0]["details"]["id"] == "zoho_123"

    @patch("src.integrations.zoho.requests.request")
    @patch("src.integrations.zoho.requests.post")
    def test_create_deal(self, mock_post, mock_request):
        mock_post.return_value = MagicMock(
            status_code=200,
            json=lambda: {"access_token": "tok", "expires_in": 3600},
        )
        mock_post.return_value.raise_for_status = MagicMock()

        mock_request.return_value = MagicMock(
            status_code=200,
            json=lambda: {"data": [{"code": "SUCCESS", "details": {"id": "deal_456"}}]},
        )
        mock_request.return_value.raise_for_status = MagicMock()

        settings = self._make_settings()
        client = ZohoClient(settings)
        result = client.create_deal({"Deal_Name": "Test Deal"})
        assert result["data"][0]["details"]["id"] == "deal_456"


# ---------------------------------------------------------------------------
# Integration test: run_zoho_push with mocked Zoho API
# ---------------------------------------------------------------------------


class TestRunZohoPush:
    def _make_settings(self):
        settings = MagicMock()
        settings.zoho_push_enabled = True
        settings.zoho_client_id = "cid"
        settings.zoho_client_secret = "csecret"
        settings.zoho_refresh_token = "rtoken"
        settings.zoho_api_base_url = "https://www.zohoapis.com/crm/v3"
        settings.zoho_auth_url = "https://accounts.zoho.com/oauth/v2/token"
        settings.zoho_auto_push_tiers = ("high", "medium")
        settings.zoho_deal_stage = "New Lead"
        settings.zoho_deal_close_days = 60
        settings.zoho_lead_source = "Signals Pipeline"
        return settings

    def test_disabled_returns_zeros(self):
        settings = self._make_settings()
        settings.zoho_push_enabled = False
        conn = MagicMock()
        result = run_zoho_push(conn, settings, "run_001")
        assert result["pushed"] == 0
        assert result["skipped"] == 0

    def test_not_configured_returns_zeros(self):
        settings = self._make_settings()
        settings.zoho_client_id = ""
        conn = MagicMock()
        result = run_zoho_push(conn, settings, "run_001")
        assert result["pushed"] == 0

    @patch("src.sync.zoho_push.ZohoClient")
    @patch("src.sync.zoho_push.db")
    def test_eligible_account_pushed(self, mock_db, mock_client_cls):
        settings = self._make_settings()
        conn = MagicMock()

        # Mock eligible accounts query.
        mock_db.get_accounts_eligible_for_crm_push.return_value = [
            {
                "account_id": "acc_001",
                "company_name": "Acme Corp",
                "domain": "acme.com",
                "product": "zopdev",
                "score": 30.0,
                "tier": "high",
                "top_reasons_json": json.dumps([{"signal_code": "devops_hiring"}]),
                "enrichment_json": json.dumps({"employees": 500}),
                "research_brief": "",
            },
        ]
        mock_db.was_account_pushed_to_crm.return_value = False
        mock_db.get_contacts_for_account.return_value = []
        mock_db.insert_crm_push_log = MagicMock()
        mock_db.update_crm_push_status = MagicMock()

        # Mock Zoho client.
        mock_client = MagicMock()
        mock_client.is_configured = True
        mock_client.upsert_account.return_value = {
            "data": [{"code": "SUCCESS", "details": {"id": "z_acc_1"}, "action": "insert"}]
        }
        mock_client.create_deal.return_value = {"data": [{"code": "SUCCESS", "details": {"id": "z_deal_1"}}]}
        mock_client.add_tags.return_value = None
        mock_client_cls.return_value = mock_client

        result = run_zoho_push(conn, settings, "run_001")
        assert result["pushed"] == 1
        assert result["deals"] == 1  # high tier -> deal created

    @patch("src.sync.zoho_push.ZohoClient")
    @patch("src.sync.zoho_push.db")
    def test_low_confidence_skipped(self, mock_db, mock_client_cls):
        settings = self._make_settings()
        conn = MagicMock()

        # Medium tier, low score -> low confidence -> skipped.
        mock_db.get_accounts_eligible_for_crm_push.return_value = [
            {
                "account_id": "acc_002",
                "company_name": "Small Co",
                "domain": "small.com",
                "product": "zopdev",
                "score": 11.0,
                "tier": "medium",
                "top_reasons_json": "[]",
                "enrichment_json": "{}",
                "research_brief": "",
            },
        ]
        mock_db.insert_crm_push_log = MagicMock()

        mock_client = MagicMock()
        mock_client.is_configured = True
        mock_client_cls.return_value = mock_client

        result = run_zoho_push(conn, settings, "run_002")
        assert result["pushed"] == 0
        assert result["skipped"] == 1

    @patch("src.sync.zoho_push.ZohoClient")
    @patch("src.sync.zoho_push.db")
    def test_medium_tier_no_deal(self, mock_db, mock_client_cls):
        settings = self._make_settings()
        conn = MagicMock()

        # Medium tier, high confidence -> pushed, but no deal (deals only for high tier).
        mock_db.get_accounts_eligible_for_crm_push.return_value = [
            {
                "account_id": "acc_003",
                "company_name": "Med Corp",
                "domain": "med.com",
                "product": "zopdev",
                "score": 18.0,
                "tier": "medium",
                "top_reasons_json": "[]",
                "enrichment_json": "{}",
                "research_brief": "",
            },
        ]
        mock_db.was_account_pushed_to_crm.return_value = False
        mock_db.get_contacts_for_account.return_value = []
        mock_db.insert_crm_push_log = MagicMock()
        mock_db.update_crm_push_status = MagicMock()

        mock_client = MagicMock()
        mock_client.is_configured = True
        mock_client.upsert_account.return_value = {
            "data": [{"code": "SUCCESS", "details": {"id": "z_acc_3"}, "action": "insert"}]
        }
        mock_client.add_tags.return_value = None
        mock_client_cls.return_value = mock_client

        result = run_zoho_push(conn, settings, "run_003")
        assert result["pushed"] == 1
        assert result["deals"] == 0  # medium tier -> no deal

    @patch("src.sync.zoho_push.ZohoClient")
    @patch("src.sync.zoho_push.db")
    def test_already_pushed_skipped(self, mock_db, mock_client_cls):
        settings = self._make_settings()
        conn = MagicMock()

        mock_db.get_accounts_eligible_for_crm_push.return_value = [
            {
                "account_id": "acc_004",
                "company_name": "Old Corp",
                "domain": "old.com",
                "product": "zopdev",
                "score": 30.0,
                "tier": "high",
                "top_reasons_json": "[]",
                "enrichment_json": "{}",
                "research_brief": "",
            },
        ]
        # Already pushed.
        mock_db.was_account_pushed_to_crm.return_value = True

        mock_client = MagicMock()
        mock_client.is_configured = True
        mock_client_cls.return_value = mock_client

        result = run_zoho_push(conn, settings, "run_004")
        assert result["pushed"] == 0
        assert result["skipped"] == 1

    @patch("src.sync.zoho_push.ZohoClient")
    @patch("src.sync.zoho_push.db")
    def test_contacts_pushed_with_account(self, mock_db, mock_client_cls):
        settings = self._make_settings()
        conn = MagicMock()

        mock_db.get_accounts_eligible_for_crm_push.return_value = [
            {
                "account_id": "acc_005",
                "company_name": "Contact Corp",
                "domain": "contact.com",
                "product": "zopdev",
                "score": 28.0,
                "tier": "high",
                "top_reasons_json": "[]",
                "enrichment_json": "{}",
                "research_brief": "",
            },
        ]
        mock_db.was_account_pushed_to_crm.return_value = False
        mock_db.get_contacts_for_account.return_value = [
            {
                "contact_id": "ct_001",
                "first_name": "Jane",
                "last_name": "Doe",
                "email": "jane@contact.com",
                "title": "CTO",
                "linkedin_url": "",
                "management_level": "C-Level",
            },
            {
                "contact_id": "ct_002",
                "first_name": "John",
                "last_name": "Smith",
                "email": "john@contact.com",
                "title": "VP Eng",
                "linkedin_url": "",
                "management_level": "VP",
            },
        ]
        mock_db.insert_crm_push_log = MagicMock()
        mock_db.update_crm_push_status = MagicMock()

        mock_client = MagicMock()
        mock_client.is_configured = True
        mock_client.upsert_account.return_value = {
            "data": [{"code": "SUCCESS", "details": {"id": "z_acc_5"}, "action": "insert"}]
        }
        mock_client.upsert_contact.return_value = {"data": [{"code": "SUCCESS", "details": {"id": "z_ct_1"}}]}
        mock_client.create_deal.return_value = {"data": [{"code": "SUCCESS", "details": {"id": "z_deal_5"}}]}
        mock_client.add_tags.return_value = None
        mock_client_cls.return_value = mock_client

        result = run_zoho_push(conn, settings, "run_005")
        assert result["pushed"] == 1
        assert result["contacts"] == 2
        assert result["deals"] == 1
