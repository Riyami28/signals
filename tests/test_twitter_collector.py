"""Tests for the Twitter collector — helpers, build_observation, edge cases."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest


# Override the global autouse postgres fixture — these are pure unit tests with no DB.
@pytest.fixture(autouse=True)
def postgres_test_isolation(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv(
        "SIGNALS_PG_DSN",
        "postgresql://signals:signals_dev_password@127.0.0.1:55432/signals_test",
    )
    yield


from src.collectors.twitter import (
    _build_observation,
    _ingest_tweets,
    _parse_tweet_observed_at,
    _twitter_search_query_url,
)

# ---------------------------------------------------------------------------
# _build_observation
# ---------------------------------------------------------------------------


class TestBuildObservation:
    def _make_obs(self, confidence=0.8, source_reliability=0.75):
        return _build_observation(
            account_id="acc_123",
            signal_code="kubernetes_detected",
            source="twitter_api",
            observed_at="2026-03-01T12:00:00+00:00",
            confidence=confidence,
            source_reliability=source_reliability,
            evidence_url="https://twitter.com/i/web/status/1234",
            evidence_text="We love Kubernetes at our company",
            payload={"tweet_id": "1234", "text": "We love Kubernetes", "matched_keyword": "kubernetes"},
        )

    def test_confidence_clamped_above_one(self):
        obs = self._make_obs(confidence=2.5)
        assert obs.confidence == 1.0

    def test_confidence_clamped_below_zero(self):
        obs = self._make_obs(confidence=-0.5)
        assert obs.confidence == 0.0

    def test_source_reliability_clamped(self):
        obs = self._make_obs(source_reliability=99.0)
        assert obs.source_reliability == 1.0

    def test_product_is_shared(self):
        obs = self._make_obs()
        assert obs.product == "shared"

    def test_evidence_text_truncated_to_500(self):
        long_text = "x" * 1000
        obs = _build_observation(
            account_id="acc_1",
            signal_code="kubernetes_detected",
            source="twitter_api",
            observed_at="2026-03-01T12:00:00+00:00",
            confidence=0.7,
            source_reliability=0.75,
            evidence_url="",
            evidence_text=long_text,
            payload={"tweet_id": "9999"},
        )
        assert len(obs.evidence_text) == 500

    def test_deterministic_obs_id(self):
        obs1 = self._make_obs()
        obs2 = self._make_obs()
        assert obs1.obs_id == obs2.obs_id

    def test_different_inputs_different_obs_id(self):
        obs1 = self._make_obs(confidence=0.7)
        obs2 = _build_observation(
            account_id="acc_999",
            signal_code="terraform_detected",
            source="twitter_api",
            observed_at="2026-03-02T12:00:00+00:00",
            confidence=0.7,
            source_reliability=0.75,
            evidence_url="",
            evidence_text="terraform",
            payload={"tweet_id": "5678"},
        )
        assert obs1.obs_id != obs2.obs_id


# ---------------------------------------------------------------------------
# _parse_tweet_observed_at
# ---------------------------------------------------------------------------


class TestParseTweetObservedAt:
    def test_valid_z_suffix(self):
        tweet = {"created_at": "2026-03-01T10:30:00Z"}
        result = _parse_tweet_observed_at(tweet)
        assert "2026-03-01" in result
        assert "10:30:00" in result

    def test_valid_iso_with_offset(self):
        tweet = {"created_at": "2026-03-01T10:30:00+00:00"}
        result = _parse_tweet_observed_at(tweet)
        assert "2026-03-01" in result

    def test_missing_created_at_returns_fallback(self):
        tweet = {}
        result = _parse_tweet_observed_at(tweet)
        assert result  # non-empty string
        assert "2026" in result or "T" in result  # looks like an ISO timestamp

    def test_invalid_created_at_returns_fallback(self):
        tweet = {"created_at": "not-a-date"}
        result = _parse_tweet_observed_at(tweet)
        assert result  # non-empty fallback


# ---------------------------------------------------------------------------
# _twitter_search_query_url
# ---------------------------------------------------------------------------


class TestTwitterSearchQueryUrl:
    def test_contains_required_params(self):
        url = _twitter_search_query_url("kubernetes", lookback_days=7, max_results=10)
        assert "max_results=10" in url
        assert "tweet.fields=" in url
        assert "start_time=" in url
        assert "api.twitter.com" in url

    def test_query_included(self):
        url = _twitter_search_query_url("devops OR terraform", lookback_days=7)
        assert "devops" in url

    def test_start_time_is_recent(self):
        url = _twitter_search_query_url("test", lookback_days=7)
        assert "2026" in url  # start_time should reference current year


# ---------------------------------------------------------------------------
# _ingest_tweets
# ---------------------------------------------------------------------------


class TestIngestTweets:
    def _make_conn(self):
        conn = MagicMock()
        conn.execute.return_value = MagicMock()
        return conn

    def test_no_match_returns_zero(self):
        conn = self._make_conn()
        lexicon = [{"signal_code": "kubernetes_detected", "keyword": "kubernetes", "confidence": "0.7"}]
        tweets = [{"id": "1", "text": "We love pizza", "created_at": "2026-03-01T12:00:00Z"}]
        with patch("src.collectors.twitter.db.insert_signal_observation", return_value=False):
            inserted, seen = _ingest_tweets(conn, "acc_1", "twitter_api", 0.75, lexicon, tweets, {})
        assert inserted == 0
        assert seen == 0

    def test_match_increments_seen(self):
        conn = self._make_conn()
        lexicon = [{"signal_code": "kubernetes_detected", "keyword": "kubernetes", "confidence": "0.7"}]
        tweets = [{"id": "1", "text": "We use kubernetes in production", "created_at": "2026-03-01T12:00:00Z"}]
        with patch("src.collectors.twitter.db.insert_signal_observation", return_value=True):
            inserted, seen = _ingest_tweets(conn, "acc_1", "twitter_api", 0.75, lexicon, tweets, {})
        assert seen == 1
        assert inserted == 1

    def test_empty_text_tweet_skipped(self):
        conn = self._make_conn()
        lexicon = [{"signal_code": "kubernetes_detected", "keyword": "kubernetes", "confidence": "0.7"}]
        tweets = [{"id": "1", "text": "", "created_at": "2026-03-01T12:00:00Z"}]
        with patch("src.collectors.twitter.db.insert_signal_observation", return_value=False):
            inserted, seen = _ingest_tweets(conn, "acc_1", "twitter_api", 0.75, lexicon, tweets, {})
        assert inserted == 0
        assert seen == 0

    def test_evidence_url_contains_tweet_id(self):
        conn = self._make_conn()
        captured_obs = []

        def capture(c, obs, commit=False):
            captured_obs.append(obs)
            return True

        lexicon = [{"signal_code": "kubernetes_detected", "keyword": "kubernetes", "confidence": "0.7"}]
        tweets = [{"id": "tweet999", "text": "kubernetes rocks", "created_at": "2026-03-01T12:00:00Z"}]
        with patch("src.collectors.twitter.db.insert_signal_observation", side_effect=capture):
            _ingest_tweets(conn, "acc_1", "twitter_api", 0.75, lexicon, tweets, {})

        assert len(captured_obs) == 1
        assert "tweet999" in captured_obs[0].evidence_url


# ---------------------------------------------------------------------------
# collect() — CSV phase without DB
# ---------------------------------------------------------------------------


class TestCollectFunction:
    @pytest.mark.asyncio
    async def test_collect_missing_csv_returns_zero(self, tmp_path):
        """CSV file doesn't exist → load_csv_rows returns [] → no crash."""
        from src.collectors.twitter import collect
        from src.settings import Settings

        settings = Settings(
            project_root=tmp_path,
            enable_live_crawl=False,
        )
        # raw_dir will be tmp_path/data/raw — no twitter.csv there
        conn = MagicMock()
        conn.commit = MagicMock()

        result = await collect(
            conn=conn,
            settings=settings,
            lexicon_by_source={},
            source_reliability={},
        )
        assert result == {"inserted": 0, "seen": 0}

    @pytest.mark.asyncio
    async def test_collect_live_skipped_without_bearer_token(self, tmp_path, caplog):
        """Live crawl enabled but no token → logs warning, no crash."""
        import logging

        from src.collectors.twitter import collect
        from src.settings import Settings

        settings = Settings(
            project_root=tmp_path,
            enable_live_crawl=True,
            twitter_bearer_token="",
        )
        conn = MagicMock()
        conn.commit = MagicMock()

        with caplog.at_level(logging.WARNING, logger="src.collectors.twitter"):
            result = await collect(
                conn=conn,
                settings=settings,
                lexicon_by_source={},
                source_reliability={},
            )

        assert result == {"inserted": 0, "seen": 0}
        assert any("no_api_key" in r.message for r in caplog.records)
