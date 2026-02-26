"""Tests for CSV upload endpoint with AI parsing (Issue #37)."""

from __future__ import annotations

import io
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

from src.web.app import create_app
from src.web.routes.upload import (
    _match_headers_by_alias,
    _parse_and_validate_csv,
    _parse_employee_count,
)

# ---------------------------------------------------------------------------
# Unit tests: header alias matching
# ---------------------------------------------------------------------------


class TestHeaderAliasMatching:
    """Test the alias-based column detection."""

    def test_standard_headers(self):
        headers = ["Company Name", "Website", "Industry", "Employees"]
        mapping = _match_headers_by_alias(headers)
        assert mapping["company_name"] == "Company Name"
        assert mapping["domain"] == "Website"
        assert mapping["industry"] == "Industry"
        assert mapping["employee_count"] == "Employees"

    def test_lowercase_headers(self):
        headers = ["company", "domain", "sector", "headcount"]
        mapping = _match_headers_by_alias(headers)
        assert mapping["company_name"] == "company"
        assert mapping["domain"] == "domain"
        assert mapping["industry"] == "sector"
        assert mapping["employee_count"] == "headcount"

    def test_underscore_headers(self):
        headers = ["company_name", "company_url", "business_type", "num_employees"]
        mapping = _match_headers_by_alias(headers)
        assert mapping["company_name"] == "company_name"
        assert mapping["domain"] == "company_url"
        assert mapping["industry"] == "business_type"
        assert mapping["employee_count"] == "num_employees"

    def test_no_match_returns_empty(self):
        headers = ["col_a", "col_b", "col_c"]
        mapping = _match_headers_by_alias(headers)
        assert mapping == {}

    def test_partial_match(self):
        headers = ["company", "some_random_col"]
        mapping = _match_headers_by_alias(headers)
        assert "company_name" in mapping
        assert "domain" not in mapping

    def test_location_mapping(self):
        headers = ["name", "url", "headquarters"]
        mapping = _match_headers_by_alias(headers)
        assert mapping.get("location") == "headquarters"


# ---------------------------------------------------------------------------
# Unit tests: employee count parsing
# ---------------------------------------------------------------------------


class TestParseEmployeeCount:
    def test_simple_number(self):
        assert _parse_employee_count("500") == 500

    def test_comma_separated(self):
        assert _parse_employee_count("1,500") == 1500

    def test_with_plus(self):
        assert _parse_employee_count("1000+") == 1000

    def test_with_tilde(self):
        assert _parse_employee_count("~250") == 250

    def test_range(self):
        assert _parse_employee_count("100-500") == 100

    def test_empty(self):
        assert _parse_employee_count("") is None

    def test_non_numeric(self):
        assert _parse_employee_count("many") is None


# ---------------------------------------------------------------------------
# Unit tests: CSV parsing and validation
# ---------------------------------------------------------------------------


class TestParseAndValidateCSV:
    def test_valid_csv_with_standard_headers(self):
        content = "Company Name,Website,Industry\nAcme Inc,acme.com,SaaS\nBeta Corp,beta.io,FinTech\n"
        rows, mapping, errors = _parse_and_validate_csv(content, "", "")
        assert len(rows) == 2
        assert mapping["company_name"] == "Company Name"
        assert mapping["domain"] == "Website"
        assert rows[0]["company_name"] == "Acme Inc"
        assert rows[0]["domain"] == "acme.com"
        assert rows[1]["domain"] == "beta.io"

    def test_domain_normalization(self):
        content = "company,domain\nAcme,https://www.acme.com/about\n"
        rows, mapping, errors = _parse_and_validate_csv(content, "", "")
        assert len(rows) == 1
        assert rows[0]["domain"] == "acme.com"

    def test_dedup_by_domain(self):
        content = "company,domain\nAcme,acme.com\nAcme Duplicate,acme.com\nBeta,beta.io\n"
        rows, mapping, errors = _parse_and_validate_csv(content, "", "")
        assert len(rows) == 2
        assert any("duplicate" in e.lower() for e in errors)

    def test_missing_both_name_and_domain(self):
        content = "company,domain\nAcme,acme.com\n,\n"
        rows, mapping, errors = _parse_and_validate_csv(content, "", "")
        assert len(rows) == 1
        assert any("missing both" in e.lower() for e in errors)

    def test_missing_domain_flagged(self):
        content = "company,domain\nAcme,\nBeta,beta.io\n"
        rows, mapping, errors = _parse_and_validate_csv(content, "", "")
        assert len(rows) == 2  # Row with missing domain is still included
        assert rows[0]["domain"] == ""
        assert any("missing domain" in e.lower() for e in errors)

    def test_empty_csv(self):
        content = "company,domain\n"
        rows, mapping, errors = _parse_and_validate_csv(content, "", "")
        assert len(rows) == 0
        assert any("no data" in e.lower() for e in errors)

    def test_no_headers(self):
        content = ""
        rows, mapping, errors = _parse_and_validate_csv(content, "", "")
        assert len(rows) == 0
        assert any("no headers" in e.lower() or "no data" in e.lower() for e in errors)

    def test_unrecognized_headers_no_api_key(self):
        content = "col_a,col_b\nfoo,bar\n"
        rows, mapping, errors = _parse_and_validate_csv(content, "", "")
        assert len(rows) == 0
        assert any("could not detect" in e.lower() for e in errors)

    def test_extra_columns_as_metadata(self):
        content = "company,domain,custom_field,notes\nAcme,acme.com,value1,some note\n"
        rows, mapping, errors = _parse_and_validate_csv(content, "", "")
        assert len(rows) == 1
        assert "custom_field" in rows[0]["metadata"]
        assert rows[0]["metadata"]["custom_field"] == "value1"

    def test_employee_count_parsing(self):
        content = "company,domain,employees\nAcme,acme.com,1500\nBeta,beta.io,~250\n"
        rows, mapping, errors = _parse_and_validate_csv(content, "", "")
        assert rows[0]["employee_count"] == 1500
        assert rows[1]["employee_count"] == 250

    def test_too_many_rows(self):
        header = "company,domain\n"
        data_rows = "".join(f"Company{i},company{i}.com\n" for i in range(10_001))
        content = header + data_rows
        rows, mapping, errors = _parse_and_validate_csv(content, "", "")
        assert len(rows) == 0
        assert any("exceeds" in e.lower() for e in errors)


# ---------------------------------------------------------------------------
# Unit tests: AI column detection
# ---------------------------------------------------------------------------


class TestAIColumnDetection:
    @patch("src.web.routes.upload.anthropic.Anthropic")
    def test_ai_detect_columns_success(self, mock_anthropic_cls):
        """AI detection returns valid mapping."""
        mock_client = MagicMock()
        mock_anthropic_cls.return_value = mock_client

        mock_message = MagicMock()
        mock_content = MagicMock()
        mock_content.text = '{"company_name": "Org Name", "domain": "Web Address"}'
        mock_message.content = [mock_content]
        mock_client.messages.create.return_value = mock_message

        from src.web.routes.upload import _ai_detect_columns

        headers = ["Org Name", "Web Address", "Notes"]
        result = _ai_detect_columns(headers, [{"Org Name": "Acme", "Web Address": "acme.com"}], "fake-key")
        assert result["company_name"] == "Org Name"
        assert result["domain"] == "Web Address"

    @patch("src.web.routes.upload.anthropic.Anthropic")
    def test_ai_detect_columns_invalid_json(self, mock_anthropic_cls):
        """AI returns unparseable response — graceful fallback."""
        mock_client = MagicMock()
        mock_anthropic_cls.return_value = mock_client

        mock_message = MagicMock()
        mock_content = MagicMock()
        mock_content.text = "I cannot determine the columns"
        mock_message.content = [mock_content]
        mock_client.messages.create.return_value = mock_message

        from src.web.routes.upload import _ai_detect_columns

        result = _ai_detect_columns(["a", "b"], [{"a": "1", "b": "2"}], "fake-key")
        assert result == {}

    @patch("src.web.routes.upload.anthropic.Anthropic")
    def test_ai_detect_strips_markdown_fencing(self, mock_anthropic_cls):
        """AI wraps JSON in markdown code blocks — we strip them."""
        mock_client = MagicMock()
        mock_anthropic_cls.return_value = mock_client

        mock_message = MagicMock()
        mock_content = MagicMock()
        mock_content.text = '```json\n{"company_name": "Name", "domain": "URL"}\n```'
        mock_message.content = [mock_content]
        mock_client.messages.create.return_value = mock_message

        from src.web.routes.upload import _ai_detect_columns

        result = _ai_detect_columns(["Name", "URL"], [{"Name": "Acme", "URL": "acme.com"}], "fake-key")
        assert result["company_name"] == "Name"

    def test_csv_with_ai_fallback(self):
        """When alias matching fails, AI is used if API key is available."""
        content = "Firmenname,Webseite\nAcme,acme.com\n"

        with patch("src.web.routes.upload._ai_detect_columns") as mock_ai:
            mock_ai.return_value = {"company_name": "Firmenname", "domain": "Webseite"}
            rows, mapping, errors = _parse_and_validate_csv(content, "fake-key", "claude-sonnet-4-5")

        assert len(rows) == 1
        assert rows[0]["company_name"] == "Acme"
        mock_ai.assert_called_once()


# ---------------------------------------------------------------------------
# API-level tests (using FastAPI TestClient)
# ---------------------------------------------------------------------------


class TestUploadAPI:
    """Test the POST /api/v1/upload/csv endpoint."""

    def test_upload_valid_csv(self):
        app = create_app()
        client = TestClient(app)

        csv_content = "Company Name,Website,Industry\nAcme Inc,acme.com,SaaS\nBeta Corp,beta.io,FinTech\n"
        file = io.BytesIO(csv_content.encode("utf-8"))

        response = client.post(
            "/api/v1/upload/csv",
            files={"file": ("companies.csv", file, "text/csv")},
        )
        assert response.status_code == 200
        data = response.json()
        assert "batch_id" in data
        assert data["row_count"] == 2
        assert "company_name" in data["parsed_columns"]
        assert "domain" in data["parsed_columns"]

    def test_upload_non_csv_rejected(self):
        app = create_app()
        client = TestClient(app)

        response = client.post(
            "/api/v1/upload/csv",
            files={"file": ("data.xlsx", io.BytesIO(b"not csv"), "application/octet-stream")},
        )
        assert response.status_code == 400

    def test_upload_empty_file(self):
        app = create_app()
        client = TestClient(app)

        response = client.post(
            "/api/v1/upload/csv",
            files={"file": ("empty.csv", io.BytesIO(b""), "text/csv")},
        )
        assert response.status_code == 400

    def test_upload_dedup_within_batch(self):
        app = create_app()
        client = TestClient(app)

        csv_content = "company,domain\nAcme,acme.com\nAcme Dup,acme.com\nBeta,beta.io\n"
        file = io.BytesIO(csv_content.encode("utf-8"))

        response = client.post(
            "/api/v1/upload/csv",
            files={"file": ("companies.csv", file, "text/csv")},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["row_count"] == 2  # Deduplicated
        assert any("duplicate" in e.lower() for e in data["validation_errors"])

    def test_upload_missing_required_columns(self):
        app = create_app()
        client = TestClient(app)

        csv_content = "random_col_a,random_col_b\nfoo,bar\n"
        file = io.BytesIO(csv_content.encode("utf-8"))

        response = client.post(
            "/api/v1/upload/csv",
            files={"file": ("companies.csv", file, "text/csv")},
        )
        assert response.status_code == 400

    def test_upload_batch_stored_in_db(self):
        """Verify that uploaded batch is retrievable via the existing batch API."""
        app = create_app()
        client = TestClient(app)

        csv_content = "company,domain\nAcme,acme.com\nBeta,beta.io\n"
        file = io.BytesIO(csv_content.encode("utf-8"))

        upload_resp = client.post(
            "/api/v1/upload/csv",
            files={"file": ("companies.csv", file, "text/csv")},
        )
        assert upload_resp.status_code == 200
        batch_id = upload_resp.json()["batch_id"]

        # Query the batch via existing batches API.
        batch_resp = client.get(f"/api/batches/{batch_id}")
        assert batch_resp.status_code == 200
        batch_data = batch_resp.json()
        assert batch_data["batch_id"] == batch_id
        assert batch_data["status"] == "pending"
        assert batch_data["row_count"] == 2

    def test_upload_csv_with_bom(self):
        """CSV files from Excel often have BOM markers."""
        app = create_app()
        client = TestClient(app)

        csv_content = "\ufeffCompany,Domain\nAcme,acme.com\n"
        file = io.BytesIO(csv_content.encode("utf-8-sig"))

        response = client.post(
            "/api/v1/upload/csv",
            files={"file": ("companies.csv", file, "text/csv")},
        )
        assert response.status_code == 200
        assert response.json()["row_count"] == 1
