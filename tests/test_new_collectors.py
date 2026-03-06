"""Tests for new collectors added to main: serper_news, serper_jobs, builtwith, github_stargazers.

These tests cover the easy early-exit paths (no API key, no accounts, zero reliability)
to boost overall coverage above the 60% threshold.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# serper_news collector
# ---------------------------------------------------------------------------


class TestSerperNewsCollect:
    def _settings(self, tmp_path: Path, api_key: str = ""):
        from src.settings import Settings

        return Settings(project_root=tmp_path, serper_api_key=api_key)

    @pytest.mark.asyncio
    async def test_no_api_key_returns_zero(self, tmp_path):
        from src.collectors.serper_news import collect

        settings = self._settings(tmp_path, api_key="")
        conn = MagicMock()
        conn.commit = MagicMock()

        result = await collect(conn, settings, lexicon_rows=[])
        assert result == {"inserted": 0, "seen": 0, "accounts_processed": 0}

    @pytest.mark.asyncio
    async def test_no_accounts_returns_zero(self, tmp_path):
        from src.collectors.serper_news import collect

        settings = self._settings(tmp_path, api_key="test-key")
        conn = MagicMock()
        conn.commit = MagicMock()
        conn.execute.return_value.fetchall.return_value = []

        result = await collect(conn, settings, lexicon_rows=[])
        assert result == {"inserted": 0, "seen": 0, "accounts_processed": 0}

    @pytest.mark.asyncio
    async def test_already_crawled_skipped(self, tmp_path):
        from src.collectors.serper_news import collect

        settings = self._settings(tmp_path, api_key="test-key")
        conn = MagicMock()
        conn.commit = MagicMock()
        conn.execute.return_value.fetchall.return_value = [
            {"account_id": "acc_1", "company_name": "Acme Corp", "domain": "acme.com"}
        ]

        with patch("src.collectors.serper_news.db.was_crawled_today", return_value=True):
            result = await collect(conn, settings, lexicon_rows=[])

        assert result["inserted"] == 0
        assert result["seen"] == 0

    @pytest.mark.asyncio
    async def test_no_results_records_crawl_attempt(self, tmp_path):
        from src.collectors.serper_news import collect

        settings = self._settings(tmp_path, api_key="test-key")
        conn = MagicMock()
        conn.commit = MagicMock()
        conn.execute.return_value.fetchall.return_value = [
            {"account_id": "acc_1", "company_name": "Acme Corp", "domain": "acme.com"}
        ]

        with (
            patch("src.collectors.serper_news.db.was_crawled_today", return_value=False),
            patch("src.collectors.serper_news._fetch_serper_news", new_callable=AsyncMock, return_value=[]),
            patch("src.collectors.serper_news.db.record_crawl_attempt") as mock_record,
            patch("src.collectors.serper_news.db.mark_crawled"),
        ):
            result = await collect(conn, settings, lexicon_rows=[])

        assert result["inserted"] == 0
        mock_record.assert_called_once()

    @pytest.mark.asyncio
    async def test_specific_account_ids_used(self, tmp_path):
        from src.collectors.serper_news import collect

        settings = self._settings(tmp_path, api_key="test-key")
        conn = MagicMock()
        conn.commit = MagicMock()
        conn.execute.return_value.fetchall.return_value = []

        result = await collect(conn, settings, lexicon_rows=[], account_ids=["acc_1"])
        assert result["accounts_processed"] == 0


# ---------------------------------------------------------------------------
# serper_jobs collector
# ---------------------------------------------------------------------------


class TestSerperJobsCollect:
    def _settings(self, tmp_path: Path, api_key: str = ""):
        from src.settings import Settings

        return Settings(project_root=tmp_path, serper_api_key=api_key)

    @pytest.mark.asyncio
    async def test_no_api_key_returns_zero(self, tmp_path):
        from src.collectors.serper_jobs import collect

        settings = self._settings(tmp_path, api_key="")
        conn = MagicMock()
        conn.commit = MagicMock()

        result = await collect(conn, settings, lexicon_rows=[])
        assert result == {"inserted": 0, "seen": 0, "accounts_processed": 0}

    @pytest.mark.asyncio
    async def test_no_accounts_returns_zero(self, tmp_path):
        from src.collectors.serper_jobs import collect

        settings = self._settings(tmp_path, api_key="test-key")
        conn = MagicMock()
        conn.commit = MagicMock()
        conn.execute.return_value.fetchall.return_value = []

        result = await collect(conn, settings, lexicon_rows=[])
        assert result == {"inserted": 0, "seen": 0, "accounts_processed": 0}

    @pytest.mark.asyncio
    async def test_already_crawled_skipped(self, tmp_path):
        from src.collectors.serper_jobs import collect

        settings = self._settings(tmp_path, api_key="test-key")
        conn = MagicMock()
        conn.commit = MagicMock()
        conn.execute.return_value.fetchall.return_value = [
            {"account_id": "acc_1", "company_name": "Acme Corp", "domain": "acme.com"}
        ]

        with patch("src.collectors.serper_jobs.db.was_crawled_today", return_value=True):
            result = await collect(conn, settings, lexicon_rows=[])

        assert result["inserted"] == 0

    @pytest.mark.asyncio
    async def test_specific_account_ids_used(self, tmp_path):
        from src.collectors.serper_jobs import collect

        settings = self._settings(tmp_path, api_key="test-key")
        conn = MagicMock()
        conn.commit = MagicMock()
        conn.execute.return_value.fetchall.return_value = []

        result = await collect(conn, settings, lexicon_rows=[], account_ids=["acc_1"])
        assert result["accounts_processed"] == 0


# ---------------------------------------------------------------------------
# builtwith collector
# ---------------------------------------------------------------------------


class TestBuiltWithCollect:
    def _settings(self, tmp_path: Path, api_key: str = ""):
        from src.settings import Settings

        return Settings(project_root=tmp_path, builtwith_api_key=api_key)

    @pytest.mark.asyncio
    async def test_no_api_key_returns_zero(self, tmp_path):
        from src.collectors.builtwith import collect

        settings = self._settings(tmp_path, api_key="")
        conn = MagicMock()
        conn.commit = MagicMock()

        result = await collect(conn, settings)
        assert result == {"inserted": 0, "seen": 0, "accounts_processed": 0}

    @pytest.mark.asyncio
    async def test_no_accounts_returns_zero(self, tmp_path):
        from src.collectors.builtwith import collect

        settings = self._settings(tmp_path, api_key="test-key")
        conn = MagicMock()
        conn.commit = MagicMock()
        conn.execute.return_value.fetchall.return_value = []

        result = await collect(conn, settings)
        assert result == {"inserted": 0, "seen": 0, "accounts_processed": 0}

    @pytest.mark.asyncio
    async def test_already_crawled_skipped(self, tmp_path):
        from src.collectors.builtwith import collect

        settings = self._settings(tmp_path, api_key="test-key")
        conn = MagicMock()
        conn.commit = MagicMock()
        conn.execute.return_value.fetchall.return_value = [
            {"account_id": "acc_1", "company_name": "Acme Corp", "domain": "acme.com"}
        ]

        with patch("src.collectors.builtwith.db.was_crawled_today", return_value=True):
            result = await collect(conn, settings)

        assert result["inserted"] == 0

    @pytest.mark.asyncio
    async def test_specific_account_ids_used(self, tmp_path):
        from src.collectors.builtwith import collect

        settings = self._settings(tmp_path, api_key="test-key")
        conn = MagicMock()
        conn.commit = MagicMock()
        conn.execute.return_value.fetchall.return_value = []

        result = await collect(conn, settings, account_ids=["acc_1"])
        assert result["accounts_processed"] == 0


# ---------------------------------------------------------------------------
# github_stargazers collector
# ---------------------------------------------------------------------------


class TestGithubStargazersCollect:
    @pytest.mark.asyncio
    async def test_no_accounts_returns_zero(self, tmp_path):
        from src.collectors.github_stargazers import collect
        from src.settings import Settings

        settings = Settings(project_root=tmp_path)
        conn = MagicMock()
        conn.commit = MagicMock()
        conn.execute.return_value.fetchall.return_value = []

        mock_response = AsyncMock()
        mock_response.status_code = 200
        mock_response.json.return_value = []
        mock_response.raise_for_status = MagicMock()

        mock_client = AsyncMock()
        mock_client.get.return_value = mock_response
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("src.collectors.github_stargazers.db.was_crawled_today", return_value=False),
            patch("src.collectors.github_stargazers.httpx.AsyncClient", return_value=mock_client),
            patch("src.collectors.github_stargazers.db.mark_crawled"),
        ):
            result = await collect(conn, settings)
        assert result["inserted"] == 0
        assert result["seen"] == 0
        assert result["matched_users"] == 0

    @pytest.mark.asyncio
    async def test_specific_account_ids_used(self, tmp_path):
        from src.collectors.github_stargazers import collect
        from src.settings import Settings

        settings = Settings(project_root=tmp_path)
        conn = MagicMock()
        conn.commit = MagicMock()
        conn.execute.return_value.fetchall.return_value = []

        mock_response = AsyncMock()
        mock_response.status_code = 200
        mock_response.json.return_value = []
        mock_response.raise_for_status = MagicMock()

        mock_client = AsyncMock()
        mock_client.get.return_value = mock_response
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)

        with (
            patch("src.collectors.github_stargazers.db.was_crawled_today", return_value=False),
            patch("src.collectors.github_stargazers.httpx.AsyncClient", return_value=mock_client),
            patch("src.collectors.github_stargazers.db.mark_crawled"),
        ):
            result = await collect(conn, settings, account_ids=["acc_1"])
        assert result["inserted"] == 0


# ---------------------------------------------------------------------------
# firmographic_google collector
# ---------------------------------------------------------------------------


class TestFirmographicGoogleCollect:
    @pytest.mark.asyncio
    async def test_no_serper_key_returns_zero(self, tmp_path):
        from src.collectors.firmographic_google import collect
        from src.settings import Settings

        settings = Settings(project_root=tmp_path, serper_api_key="", minimax_api_key="key")
        conn = MagicMock()
        conn.commit = MagicMock()

        result = await collect(conn, settings)
        assert result["enriched"] == 0
        assert result["accounts_processed"] == 0

    @pytest.mark.asyncio
    async def test_no_minimax_key_returns_zero(self, tmp_path):
        from src.collectors.firmographic_google import collect
        from src.settings import Settings

        settings = Settings(project_root=tmp_path, serper_api_key="test-key", minimax_api_key="")
        conn = MagicMock()
        conn.commit = MagicMock()

        result = await collect(conn, settings)
        assert result["enriched"] == 0
        assert result["accounts_processed"] == 0

    @pytest.mark.asyncio
    async def test_no_accounts_returns_zero(self, tmp_path):
        from src.collectors.firmographic_google import collect
        from src.settings import Settings

        settings = Settings(project_root=tmp_path, serper_api_key="test-key", minimax_api_key="test-key2")
        conn = MagicMock()
        conn.commit = MagicMock()
        cursor = MagicMock()
        cursor.fetchall.return_value = []
        conn.cursor.return_value = cursor

        result = await collect(conn, settings)
        assert result["enriched"] == 0
        assert result["accounts_processed"] == 0
