"""Tests for src/research/web_scraper.py."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest


class TestFetchPage:
    def test_returns_text_on_200(self):
        from src.research.web_scraper import _fetch_page

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.text = "<html><body><p>Hello world</p><script>js</script></body></html>"

        with patch("src.research.web_scraper.requests.get", return_value=mock_resp):
            result = _fetch_page("https://example.com")

        assert "Hello world" in result
        assert "js" not in result  # script removed

    def test_returns_empty_on_non_200(self):
        from src.research.web_scraper import _fetch_page

        mock_resp = MagicMock()
        mock_resp.status_code = 404

        with patch("src.research.web_scraper.requests.get", return_value=mock_resp):
            result = _fetch_page("https://example.com/notfound")

        assert result == ""

    def test_returns_empty_on_exception(self):
        from src.research.web_scraper import _fetch_page

        with patch("src.research.web_scraper.requests.get", side_effect=Exception("timeout")):
            result = _fetch_page("https://unreachable.example.com")

        assert result == ""

    def test_truncates_at_8000_chars(self):
        from src.research.web_scraper import _fetch_page

        long_text = "word " * 2000  # >8000 chars
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.text = f"<html><body><p>{long_text}</p></body></html>"

        with patch("src.research.web_scraper.requests.get", return_value=mock_resp):
            result = _fetch_page("https://example.com")

        assert len(result) <= 8000


class TestScrapeCompanyInfo:
    def _mock_fetch(self, homepage="Company homepage content", about="About us content"):
        """Returns a side_effect function for _fetch_page calls."""
        calls = {"count": 0}

        def fetch(url, timeout=10):
            calls["count"] += 1
            if "/about" in url:
                return about
            return homepage

        return fetch

    def test_returns_parsed_data_on_success(self):
        from src.research.web_scraper import scrape_company_info

        data = {"industry": "Software", "employees": 100, "city": "San Francisco"}

        mock_response = MagicMock()
        mock_response.raw_text = json.dumps(data)

        mock_client = MagicMock()
        mock_client.research_company.return_value = mock_response

        with patch("src.research.web_scraper._fetch_page", side_effect=self._mock_fetch()):
            result = scrape_company_info("example.com", mock_client)

        assert result["industry"] == "Software"
        assert result["employees"] == 100

    def test_returns_empty_dict_when_no_content(self):
        from src.research.web_scraper import scrape_company_info

        mock_client = MagicMock()

        with patch("src.research.web_scraper._fetch_page", return_value=""):
            result = scrape_company_info("empty.com", mock_client)

        assert result == {}
        mock_client.research_company.assert_not_called()

    def test_handles_markdown_code_block(self):
        from src.research.web_scraper import scrape_company_info

        data = {"industry": "Fintech", "city": "NYC"}
        fenced = f"```json\n{json.dumps(data)}\n```"

        mock_response = MagicMock()
        mock_response.raw_text = fenced

        mock_client = MagicMock()
        mock_client.research_company.return_value = mock_response

        with patch("src.research.web_scraper._fetch_page", side_effect=self._mock_fetch()):
            result = scrape_company_info("fintech.com", mock_client)

        assert result["industry"] == "Fintech"

    def test_handles_generic_code_block(self):
        from src.research.web_scraper import scrape_company_info

        data = {"industry": "Healthcare"}
        fenced = f"```\n{json.dumps(data)}\n```"

        mock_response = MagicMock()
        mock_response.raw_text = fenced

        mock_client = MagicMock()
        mock_client.research_company.return_value = mock_response

        with patch("src.research.web_scraper._fetch_page", side_effect=self._mock_fetch()):
            result = scrape_company_info("health.com", mock_client)

        assert result["industry"] == "Healthcare"

    def test_returns_empty_on_llm_exception(self):
        from src.research.web_scraper import scrape_company_info

        mock_client = MagicMock()
        mock_client.research_company.side_effect = RuntimeError("LLM error")

        with patch("src.research.web_scraper._fetch_page", side_effect=self._mock_fetch()):
            result = scrape_company_info("fail.com", mock_client)

        assert result == {}

    def test_returns_empty_on_invalid_json(self):
        from src.research.web_scraper import scrape_company_info

        mock_response = MagicMock()
        mock_response.raw_text = "not valid json {{"

        mock_client = MagicMock()
        mock_client.research_company.return_value = mock_response

        with patch("src.research.web_scraper._fetch_page", side_effect=self._mock_fetch()):
            result = scrape_company_info("badjson.com", mock_client)

        assert result == {}

    def test_only_homepage_when_about_empty(self):
        from src.research.web_scraper import scrape_company_info

        data = {"industry": "Retail"}
        mock_response = MagicMock()
        mock_response.raw_text = json.dumps(data)

        mock_client = MagicMock()
        mock_client.research_company.return_value = mock_response

        with patch("src.research.web_scraper._fetch_page", side_effect=self._mock_fetch(about="")):
            result = scrape_company_info("retail.com", mock_client)

        assert result["industry"] == "Retail"
        # Confirm LLM was called with only homepage content
        call_args = mock_client.research_company.call_args
        assert "About Page" not in call_args[0][1]
