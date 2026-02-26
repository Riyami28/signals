"""Tests for src/http_client.py — robots.txt, rate limiting, sync/async HTTP."""

from __future__ import annotations

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from src.http_client import (
    RobotsDisallowedError,
    _host_from_url,
    _proxy_url,
    _sleep_for_rate_limit_sync,
    _sync_last_request,
    get,
)
from src.settings import Settings


@pytest.fixture
def settings(tmp_path):
    return Settings(
        project_root=tmp_path,
        http_timeout_seconds=5,
        http_user_agent="test-bot/1.0",
        respect_robots_txt=True,
        min_domain_request_interval_ms=0,
    )


@pytest.fixture
def settings_no_robots(tmp_path):
    return Settings(
        project_root=tmp_path,
        http_timeout_seconds=5,
        http_user_agent="test-bot/1.0",
        respect_robots_txt=False,
        min_domain_request_interval_ms=0,
    )


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------


class TestHostFromUrl:
    def test_extracts_domain(self):
        assert _host_from_url("https://www.example.com/path") == "example.com"

    def test_strips_www(self):
        assert _host_from_url("https://www.test.io/") == "test.io"

    def test_no_www(self):
        assert _host_from_url("https://api.example.com/v1") == "api.example.com"

    def test_with_port(self):
        result = _host_from_url("https://example.com:8080/path")
        assert "example.com" in result


class TestProxyUrl:
    def test_empty_returns_none(self, tmp_path):
        s = Settings(project_root=tmp_path, http_proxy_url="")
        assert _proxy_url(s) is None

    def test_whitespace_returns_none(self, tmp_path):
        s = Settings(project_root=tmp_path, http_proxy_url="  ")
        assert _proxy_url(s) is None

    def test_valid_url_returned(self, tmp_path):
        s = Settings(project_root=tmp_path, http_proxy_url="http://proxy:8080")
        assert _proxy_url(s) == "http://proxy:8080"


# ---------------------------------------------------------------------------
# Sync rate limiting
# ---------------------------------------------------------------------------


class TestSyncRateLimit:
    def test_no_delay_with_zero_interval(self, settings):
        settings_zero = Settings(
            project_root=settings.project_root,
            min_domain_request_interval_ms=0,
        )
        start = time.monotonic()
        _sleep_for_rate_limit_sync("ratelimit-test-zero.com", settings_zero)
        elapsed = time.monotonic() - start
        assert elapsed < 0.1

    def test_no_delay_for_empty_host(self, settings):
        start = time.monotonic()
        _sleep_for_rate_limit_sync("", settings)
        elapsed = time.monotonic() - start
        assert elapsed < 0.1


# ---------------------------------------------------------------------------
# Sync GET
# ---------------------------------------------------------------------------


class TestSyncGet:
    @patch("src.http_client._is_allowed_by_robots_sync", return_value=True)
    @patch("src.http_client.httpx.get")
    def test_successful_get(self, mock_get, mock_robots, settings):
        mock_response = MagicMock(spec=httpx.Response)
        mock_response.status_code = 200
        mock_response.text = "Hello"
        mock_get.return_value = mock_response

        response = get("https://example.com/test", settings)
        assert response.status_code == 200
        mock_get.assert_called_once()

    @patch("src.http_client._is_allowed_by_robots_sync", return_value=False)
    def test_robots_disallowed_raises(self, mock_robots, settings):
        with pytest.raises(RobotsDisallowedError):
            get("https://example.com/secret", settings)

    @patch("src.http_client._is_allowed_by_robots_sync", return_value=True)
    @patch("src.http_client.httpx.get")
    def test_uses_custom_timeout(self, mock_get, mock_robots, settings):
        mock_response = MagicMock(spec=httpx.Response)
        mock_response.status_code = 200
        mock_get.return_value = mock_response

        get("https://example.com/test", settings, timeout_seconds=30)
        call_kwargs = mock_get.call_args
        assert call_kwargs.kwargs.get("timeout") == 30 or call_kwargs[1].get("timeout") == 30

    @patch("src.http_client._is_allowed_by_robots_sync", return_value=True)
    @patch("src.http_client.httpx.get")
    def test_uses_custom_user_agent(self, mock_get, mock_robots, settings):
        mock_response = MagicMock(spec=httpx.Response)
        mock_response.status_code = 200
        mock_get.return_value = mock_response

        get("https://example.com/test", settings)
        call_kwargs = mock_get.call_args
        headers = call_kwargs.kwargs.get("headers") or call_kwargs[1].get("headers")
        assert headers["User-Agent"] == "test-bot/1.0"

    @patch("src.http_client._is_allowed_by_robots_sync", return_value=True)
    @patch("src.http_client.httpx.get")
    def test_default_timeout_from_settings(self, mock_get, mock_robots, settings):
        mock_response = MagicMock(spec=httpx.Response)
        mock_response.status_code = 200
        mock_get.return_value = mock_response

        get("https://example.com/test", settings)
        call_kwargs = mock_get.call_args
        timeout = call_kwargs.kwargs.get("timeout") or call_kwargs[1].get("timeout")
        assert timeout == 5


# ---------------------------------------------------------------------------
# Robots.txt bypass
# ---------------------------------------------------------------------------


class TestRobotsBypass:
    @patch("src.http_client.httpx.get")
    def test_robots_disabled_skips_check(self, mock_get, settings_no_robots):
        mock_response = MagicMock(spec=httpx.Response)
        mock_response.status_code = 200
        mock_get.return_value = mock_response

        response = get("https://example.com/blocked-path", settings_no_robots)
        assert response.status_code == 200


# ---------------------------------------------------------------------------
# RobotsDisallowedError
# ---------------------------------------------------------------------------


class TestRobotsDisallowedError:
    def test_is_runtime_error(self):
        err = RobotsDisallowedError("test")
        assert isinstance(err, RuntimeError)

    def test_message(self):
        err = RobotsDisallowedError("robots_disallowed url=https://example.com")
        assert "example.com" in str(err)
