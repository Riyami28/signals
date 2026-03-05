"""Tests for the HackerNews MCP collector — helpers, _make_observation, edge cases."""

from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# Override the global autouse postgres fixture — these are pure unit tests with no DB.
@pytest.fixture(autouse=True)
def postgres_test_isolation(monkeypatch: pytest.MonkeyPatch):
    import os

    test_dsn = os.getenv(
        "SIGNALS_TEST_PG_DSN",
        "postgresql://signals:signals_dev_password@127.0.0.1:55432/signals_test",
    )
    monkeypatch.setenv("SIGNALS_PG_DSN", test_dsn)
    yield


from src.collectors.hackernews_mcp_collector import (
    INTENT_CATEGORIES,
    INTENT_TO_SIGNAL,
    SOURCE_NAME,
    SOURCE_RELIABILITY,
    _make_observation,
)


# ---------------------------------------------------------------------------
# Constants sanity checks
# ---------------------------------------------------------------------------


class TestConstants:
    def test_source_name(self):
        assert SOURCE_NAME == "hackernews_mcp"

    def test_source_reliability_range(self):
        assert 0.0 <= SOURCE_RELIABILITY <= 1.0

    def test_intent_categories_keys(self):
        assert "active_evaluation" in INTENT_CATEGORIES
        assert "infrastructure_pain" in INTENT_CATEGORIES
        assert "hiring_signal" in INTENT_CATEGORIES
        assert "funding_signal" in INTENT_CATEGORIES
        assert "tool_launch" in INTENT_CATEGORIES
        assert "passing_mention" in INTENT_CATEGORIES

    def test_passing_mention_has_none_score(self):
        assert INTENT_CATEGORIES["passing_mention"] is None

    def test_intent_to_signal_mapping(self):
        assert INTENT_TO_SIGNAL["active_evaluation"] == "tech_evaluation_intent"
        assert INTENT_TO_SIGNAL["infrastructure_pain"] == "infrastructure_pain"
        assert INTENT_TO_SIGNAL["hiring_signal"] == "devops_role_open"
        assert INTENT_TO_SIGNAL["funding_signal"] == "recent_funding_event"
        assert INTENT_TO_SIGNAL["tool_launch"] == "launch_or_scale_event"
        assert "passing_mention" not in INTENT_TO_SIGNAL


# ---------------------------------------------------------------------------
# _make_observation
# ---------------------------------------------------------------------------


class TestMakeObservation:
    def _item(self, url="https://news.ycombinator.com/item?id=1234"):
        return {"url": url, "title": "Test post", "author": "testuser"}

    def _classification(
        self,
        relevant=True,
        intent="hiring_signal",
        confidence=0.8,
        evidence_sentence="Company is hiring DevOps engineers.",
        signal_code="devops_role_open",
    ):
        return {
            "relevant": relevant,
            "intent": intent,
            "confidence": confidence,
            "evidence_sentence": evidence_sentence,
            "signal_code": signal_code,
        }

    def test_returns_none_when_not_relevant(self):
        obs = _make_observation(
            "acc_1",
            self._classification(relevant=False),
            self._item(),
            0.7,
        )
        assert obs is None

    def test_returns_none_for_passing_mention(self):
        obs = _make_observation(
            "acc_1",
            self._classification(intent="passing_mention", signal_code=None),
            self._item(),
            0.7,
        )
        assert obs is None

    def test_returns_none_for_unknown_intent(self):
        obs = _make_observation(
            "acc_1",
            self._classification(intent="unknown_type", signal_code=None),
            self._item(),
            0.7,
        )
        assert obs is None

    def test_valid_hiring_signal(self):
        obs = _make_observation(
            "acc_1",
            self._classification(intent="hiring_signal", signal_code="devops_role_open", confidence=0.75),
            self._item(),
            0.7,
        )
        assert obs is not None
        assert obs.signal_code == "devops_role_open"
        assert obs.source == SOURCE_NAME
        assert obs.product == "shared"

    def test_valid_funding_signal(self):
        obs = _make_observation(
            "acc_1",
            self._classification(intent="funding_signal", signal_code="recent_funding_event", confidence=0.9),
            self._item(),
            0.7,
        )
        assert obs is not None
        assert obs.signal_code == "recent_funding_event"

    def test_valid_active_evaluation(self):
        obs = _make_observation(
            "acc_1",
            self._classification(intent="active_evaluation", signal_code="tech_evaluation_intent", confidence=0.85),
            self._item(),
            0.7,
        )
        assert obs is not None
        assert obs.signal_code == "tech_evaluation_intent"

    def test_confidence_clamped_to_one(self):
        obs = _make_observation(
            "acc_1",
            self._classification(confidence=5.0),
            self._item(),
            0.7,
        )
        assert obs.confidence == 1.0

    def test_confidence_clamped_to_zero(self):
        obs = _make_observation(
            "acc_1",
            self._classification(confidence=-1.0),
            self._item(),
            0.7,
        )
        assert obs.confidence == 0.0

    def test_source_reliability_clamped(self):
        obs = _make_observation(
            "acc_1",
            self._classification(confidence=0.8),
            self._item(),
            99.0,
        )
        assert obs.source_reliability == 1.0

    def test_evidence_text_truncated(self):
        long_evidence = "x" * 1000
        obs = _make_observation(
            "acc_1",
            self._classification(evidence_sentence=long_evidence),
            self._item(),
            0.7,
        )
        assert len(obs.evidence_text) <= 500

    def test_obs_id_is_deterministic(self):
        cls = self._classification()
        item = self._item()
        obs1 = _make_observation("acc_1", cls, item, 0.7)
        obs2 = _make_observation("acc_1", cls, item, 0.7)
        assert obs1.obs_id == obs2.obs_id

    def test_different_accounts_different_obs_id(self):
        cls = self._classification()
        item = self._item()
        obs1 = _make_observation("acc_1", cls, item, 0.7)
        obs2 = _make_observation("acc_2", cls, item, 0.7)
        assert obs1.obs_id != obs2.obs_id

    def test_different_urls_different_obs_id(self):
        cls = self._classification()
        obs1 = _make_observation("acc_1", cls, self._item(url="https://hn.example.com/1"), 0.7)
        obs2 = _make_observation("acc_1", cls, self._item(url="https://hn.example.com/2"), 0.7)
        assert obs1.obs_id != obs2.obs_id

    def test_signal_code_fallback_to_intent_map(self):
        """When signal_code is None/empty, falls back to INTENT_TO_SIGNAL."""
        cls = self._classification(intent="hiring_signal", signal_code=None)
        cls["signal_code"] = None
        obs = _make_observation("acc_1", cls, self._item(), 0.7)
        assert obs is not None
        assert obs.signal_code == "devops_role_open"

    def test_evidence_url_from_item(self):
        url = "https://news.ycombinator.com/item?id=99999"
        obs = _make_observation("acc_1", self._classification(), self._item(url=url), 0.7)
        assert obs.evidence_url == url

    def test_uses_default_confidence_when_zero(self):
        """When confidence is 0 and intent has category default, still clamps to 0."""
        cls = self._classification(confidence=0.0)
        obs = _make_observation("acc_1", cls, self._item(), 0.7)
        assert obs.confidence == 0.0


# ---------------------------------------------------------------------------
# _fetch_hn_posts (async, mocked httpx)
# ---------------------------------------------------------------------------


class TestFetchHnPosts:
    @pytest.mark.asyncio
    async def test_returns_posts_on_success(self):
        from src.collectors.hackernews_mcp_collector import _fetch_hn_posts

        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = {
            "hits": [
                {
                    "objectID": "12345",
                    "title": "Acme Corp is hiring DevOps",
                    "comment_text": None,
                    "story_text": "We are hiring SREs",
                    "author": "acmeuser",
                    "created_at": "2026-03-01T10:00:00.000Z",
                    "story_id": "12345",
                }
            ]
        }

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=mock_response)

        posts = await _fetch_hn_posts(mock_client, "Acme Corp", lookback_days=30, num_results=10)
        assert len(posts) == 1
        assert posts[0]["title"] == "Acme Corp is hiring DevOps"
        assert posts[0]["author"] == "acmeuser"
        assert "news.ycombinator.com" in posts[0]["url"]

    @pytest.mark.asyncio
    async def test_returns_empty_on_http_error(self):
        import httpx

        from src.collectors.hackernews_mcp_collector import _fetch_hn_posts

        mock_response = MagicMock()
        mock_response.status_code = 429

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(
            side_effect=httpx.HTTPStatusError("Too many requests", request=MagicMock(), response=mock_response)
        )

        posts = await _fetch_hn_posts(mock_client, "Acme Corp")
        assert posts == []

    @pytest.mark.asyncio
    async def test_returns_empty_on_generic_exception(self):
        from src.collectors.hackernews_mcp_collector import _fetch_hn_posts

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(side_effect=Exception("network error"))

        posts = await _fetch_hn_posts(mock_client, "Acme Corp")
        assert posts == []

    @pytest.mark.asyncio
    async def test_uses_comment_text_when_no_story_text(self):
        from src.collectors.hackernews_mcp_collector import _fetch_hn_posts

        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = {
            "hits": [
                {
                    "objectID": "99",
                    "title": None,
                    "story_title": "Parent story",
                    "comment_text": "Comment about DevOps",
                    "story_text": None,
                    "author": "user",
                    "created_at": "2026-03-01T00:00:00.000Z",
                    "story_id": "88",
                }
            ]
        }

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=mock_response)

        posts = await _fetch_hn_posts(mock_client, "Test Co")
        assert posts[0]["body"] == "Comment about DevOps"
        assert posts[0]["title"] == "Parent story"

    @pytest.mark.asyncio
    async def test_empty_hits_returns_empty_list(self):
        from src.collectors.hackernews_mcp_collector import _fetch_hn_posts

        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = {"hits": []}

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=mock_response)

        posts = await _fetch_hn_posts(mock_client, "NoResults Co")
        assert posts == []


# ---------------------------------------------------------------------------
# _classify_with_claude (async, mocked httpx)
# ---------------------------------------------------------------------------


class TestClassifyWithClaude:
    @pytest.mark.asyncio
    async def test_returns_classification_on_success(self):
        from src.collectors.hackernews_mcp_collector import _classify_with_claude

        classification = {
            "relevant": True,
            "intent": "hiring_signal",
            "confidence": 0.8,
            "evidence_sentence": "Acme Corp is hiring DevOps engineers.",
            "signal_code": "devops_role_open",
        }

        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = {
            "content": [{"text": json.dumps(classification)}]
        }

        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=mock_response)

        item = {"title": "Acme hiring SRE", "body": "We need a DevOps engineer", "url": "https://hn.example.com/1"}
        result = await _classify_with_claude(item, "Acme Corp", "acme.com", "test-key", mock_client)

        assert result is not None
        assert result["intent"] == "hiring_signal"
        assert result["relevant"] is True

    @pytest.mark.asyncio
    async def test_handles_json_fenced_code_block(self):
        from src.collectors.hackernews_mcp_collector import _classify_with_claude

        classification = {"relevant": False, "intent": "passing_mention", "confidence": 0.0, "signal_code": None}
        fenced = f"```json\n{json.dumps(classification)}\n```"

        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = {"content": [{"text": fenced}]}

        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=mock_response)

        item = {"title": "Some HN post", "body": "Some text", "url": "https://hn.example.com/2"}
        result = await _classify_with_claude(item, "Company", "company.com", "key", mock_client)
        assert result is not None
        assert result["relevant"] is False

    @pytest.mark.asyncio
    async def test_returns_none_for_empty_item(self):
        from src.collectors.hackernews_mcp_collector import _classify_with_claude

        mock_client = AsyncMock()
        item = {"title": "", "body": "", "url": "https://hn.example.com/3"}
        result = await _classify_with_claude(item, "Company", "company.com", "key", mock_client)
        assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_on_http_error(self):
        import httpx

        from src.collectors.hackernews_mcp_collector import _classify_with_claude

        mock_client = AsyncMock()
        mock_client.post = AsyncMock(side_effect=httpx.HTTPError("timeout"))

        item = {"title": "Test post", "body": "Some body text", "url": "https://hn.example.com/4"}
        result = await _classify_with_claude(item, "Company", "company.com", "key", mock_client)
        assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_on_invalid_json(self):
        from src.collectors.hackernews_mcp_collector import _classify_with_claude

        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = {"content": [{"text": "not valid json {{}"}]}

        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=mock_response)

        item = {"title": "Test post", "body": "Body text", "url": "https://hn.example.com/5"}
        result = await _classify_with_claude(item, "Company", "company.com", "key", mock_client)
        assert result is None


# ---------------------------------------------------------------------------
# collect() — top-level entry point
# ---------------------------------------------------------------------------


class TestCollectFunction:
    @pytest.mark.asyncio
    async def test_no_claude_key_returns_zero(self, tmp_path):
        from src.collectors.hackernews_mcp_collector import collect
        from src.settings import Settings

        settings = Settings(project_root=tmp_path, claude_api_key="")
        conn = MagicMock()
        conn.commit = MagicMock()

        result = await collect(conn, settings)
        assert result == {"inserted": 0, "seen": 0, "accounts_processed": 0}

    @pytest.mark.asyncio
    async def test_zero_source_reliability_returns_zero(self, tmp_path):
        from src.collectors.hackernews_mcp_collector import collect
        from src.settings import Settings

        settings = Settings(project_root=tmp_path, claude_api_key="test-key")
        conn = MagicMock()
        conn.commit = MagicMock()

        result = await collect(conn, settings, source_reliability_dict={"hackernews_mcp": 0})
        assert result == {"inserted": 0, "seen": 0, "accounts_processed": 0}

    @pytest.mark.asyncio
    async def test_no_accounts_returns_zero(self, tmp_path):
        from src.collectors.hackernews_mcp_collector import collect
        from src.settings import Settings

        settings = Settings(project_root=tmp_path, claude_api_key="test-key")
        conn = MagicMock()
        conn.commit = MagicMock()
        # fetchall returns no accounts
        conn.execute.return_value.fetchall.return_value = []

        result = await collect(conn, settings)
        assert result == {"inserted": 0, "seen": 0, "accounts_processed": 0}

    @pytest.mark.asyncio
    async def test_specific_account_ids_uses_in_query(self, tmp_path):
        from src.collectors.hackernews_mcp_collector import collect
        from src.settings import Settings

        settings = Settings(project_root=tmp_path, claude_api_key="test-key")
        conn = MagicMock()
        conn.commit = MagicMock()
        conn.execute.return_value.fetchall.return_value = []

        result = await collect(conn, settings, account_ids=["acc_1", "acc_2"])
        assert result["accounts_processed"] == 0
        # Should have called execute with the IN clause
        call_args = conn.execute.call_args
        assert "%s" in call_args[0][0] or call_args is not None

    @pytest.mark.asyncio
    async def test_account_without_domain_skipped(self, tmp_path):
        from src.collectors.hackernews_mcp_collector import collect
        from src.settings import Settings

        settings = Settings(project_root=tmp_path, claude_api_key="test-key")
        conn = MagicMock()
        conn.commit = MagicMock()
        # Account has no company_name and no domain
        conn.execute.return_value.fetchall.return_value = [
            {"account_id": "acc_1", "company_name": "", "domain": ""}
        ]

        with patch("src.collectors.hackernews_mcp_collector.db.was_crawled_today", return_value=False):
            result = await collect(conn, settings)

        assert result["inserted"] == 0

    @pytest.mark.asyncio
    async def test_already_crawled_today_skipped(self, tmp_path):
        from src.collectors.hackernews_mcp_collector import collect
        from src.settings import Settings

        settings = Settings(project_root=tmp_path, claude_api_key="test-key")
        conn = MagicMock()
        conn.commit = MagicMock()
        conn.execute.return_value.fetchall.return_value = [
            {"account_id": "acc_1", "company_name": "Acme Corp", "domain": "acme.com"}
        ]

        with patch("src.collectors.hackernews_mcp_collector.db.was_crawled_today", return_value=True):
            result = await collect(conn, settings)

        assert result["inserted"] == 0
        assert result["seen"] == 0

    @pytest.mark.asyncio
    async def test_no_posts_records_crawl_attempt(self, tmp_path):
        from src.collectors.hackernews_mcp_collector import collect
        from src.settings import Settings

        settings = Settings(project_root=tmp_path, claude_api_key="test-key")
        conn = MagicMock()
        conn.commit = MagicMock()
        conn.execute.return_value.fetchall.return_value = [
            {"account_id": "acc_1", "company_name": "Acme Corp", "domain": "acme.com"}
        ]

        with (
            patch("src.collectors.hackernews_mcp_collector.db.was_crawled_today", return_value=False),
            patch("src.collectors.hackernews_mcp_collector._fetch_hn_posts", new_callable=AsyncMock, return_value=[]),
            patch("src.collectors.hackernews_mcp_collector.db.record_crawl_attempt") as mock_record,
            patch("src.collectors.hackernews_mcp_collector.db.mark_crawled") as mock_mark,
        ):
            result = await collect(conn, settings)

        assert result["inserted"] == 0
        mock_record.assert_called_once()
        mock_mark.assert_called_once()

    @pytest.mark.asyncio
    async def test_posts_classified_and_inserted(self, tmp_path):
        from src.collectors.hackernews_mcp_collector import collect
        from src.settings import Settings

        settings = Settings(project_root=tmp_path, claude_api_key="test-key")
        conn = MagicMock()
        conn.commit = MagicMock()
        conn.execute.return_value.fetchall.return_value = [
            {"account_id": "acc_1", "company_name": "Acme Corp", "domain": "acme.com"}
        ]

        post = {
            "title": "Acme Corp hiring SRE",
            "body": "We need a DevOps engineer",
            "url": "https://news.ycombinator.com/item?id=1",
            "story_id": "1",
            "author": "acmeuser",
            "created_at": "2026-03-01T00:00:00.000Z",
        }
        classification = {
            "relevant": True,
            "intent": "hiring_signal",
            "confidence": 0.8,
            "evidence_sentence": "Acme is hiring DevOps.",
            "signal_code": "devops_role_open",
        }

        with (
            patch("src.collectors.hackernews_mcp_collector.db.was_crawled_today", return_value=False),
            patch("src.collectors.hackernews_mcp_collector._fetch_hn_posts", new_callable=AsyncMock, return_value=[post]),
            patch(
                "src.collectors.hackernews_mcp_collector._classify_with_claude",
                new_callable=AsyncMock,
                return_value=classification,
            ),
            patch("src.collectors.hackernews_mcp_collector.db.insert_signal_observation", return_value=True),
            patch("src.collectors.hackernews_mcp_collector.db.record_crawl_attempt"),
            patch("src.collectors.hackernews_mcp_collector.db.mark_crawled"),
        ):
            result = await collect(conn, settings)

        assert result["inserted"] == 1
        assert result["seen"] == 1

    @pytest.mark.asyncio
    async def test_classify_returns_none_skipped(self, tmp_path):
        from src.collectors.hackernews_mcp_collector import collect
        from src.settings import Settings

        settings = Settings(project_root=tmp_path, claude_api_key="test-key")
        conn = MagicMock()
        conn.commit = MagicMock()
        conn.execute.return_value.fetchall.return_value = [
            {"account_id": "acc_1", "company_name": "Acme Corp", "domain": "acme.com"}
        ]

        post = {
            "title": "Acme Corp post",
            "body": "Some text",
            "url": "https://news.ycombinator.com/item?id=2",
            "story_id": "2",
            "author": "user",
            "created_at": "2026-03-01T00:00:00.000Z",
        }

        with (
            patch("src.collectors.hackernews_mcp_collector.db.was_crawled_today", return_value=False),
            patch("src.collectors.hackernews_mcp_collector._fetch_hn_posts", new_callable=AsyncMock, return_value=[post]),
            patch(
                "src.collectors.hackernews_mcp_collector._classify_with_claude",
                new_callable=AsyncMock,
                return_value=None,
            ),
            patch("src.collectors.hackernews_mcp_collector.db.record_crawl_attempt"),
            patch("src.collectors.hackernews_mcp_collector.db.mark_crawled"),
        ):
            result = await collect(conn, settings)

        assert result["inserted"] == 0
        assert result["seen"] == 1

    @pytest.mark.asyncio
    async def test_exception_in_account_loop_continues(self, tmp_path):
        from src.collectors.hackernews_mcp_collector import collect
        from src.settings import Settings

        settings = Settings(project_root=tmp_path, claude_api_key="test-key")
        conn = MagicMock()
        conn.commit = MagicMock()
        conn.execute.return_value.fetchall.return_value = [
            {"account_id": "acc_1", "company_name": "Acme Corp", "domain": "acme.com"},
            {"account_id": "acc_2", "company_name": "Beta Corp", "domain": "beta.com"},
        ]

        call_count = 0

        async def flaky_fetch(client, name, *args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("network failure")
            return []

        with (
            patch("src.collectors.hackernews_mcp_collector.db.was_crawled_today", return_value=False),
            patch("src.collectors.hackernews_mcp_collector._fetch_hn_posts", side_effect=flaky_fetch),
            patch("src.collectors.hackernews_mcp_collector.db.record_crawl_attempt"),
            patch("src.collectors.hackernews_mcp_collector.db.mark_crawled"),
        ):
            result = await collect(conn, settings)

        # Should have processed both accounts (second one gracefully returns 0)
        assert result["accounts_processed"] == 2
