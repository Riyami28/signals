"""Integration tests for the Twitter collector — real DB, mocked HTTP.

These tests use the live PostgreSQL test database (signals_test) via the
autouse postgres_test_isolation fixture in conftest.py.  HTTP calls to
the RapidAPI/official Twitter endpoints are mocked with httpx.MockTransport
so no network access is required.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from src import db
from src.collectors.twitter import (
    _ingest_tweets,
    _parse_rapidapi_tweets,
    collect,
)
from src.settings import Settings

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_twitter241_response(tweets: list[dict]) -> dict:
    """Build a minimal twitter241 search-v3 GraphQL response."""
    entries = []
    for t in tweets:
        created_at_ms = int(datetime.fromisoformat(t["created_at"].replace("Z", "+00:00")).timestamp() * 1000)
        entries.append(
            {
                "__typename": "TimelineTimelineEntry",
                "entry_id": f"tweet-{t['id']}",
                "sort_index": t["id"],
                "content": {
                    "__typename": "TimelineTimelineItem",
                    "content": {
                        "__typename": "TimelineTweet",
                        "tweet_display_type": "Tweet",
                        "tweet_results": {
                            "__typename": "TweetResults",
                            "result": {
                                "__typename": "Tweet",
                                "rest_id": t["id"],
                                "details": {
                                    "__typename": "TweetDetails",
                                    "full_text": t["text"],
                                    "created_at_ms": created_at_ms,
                                },
                                "legacy": {"__typename": "TweetLegacy"},
                            },
                        },
                    },
                },
            }
        )
    return {
        "cursor": {"top": "abc", "bottom": "xyz"},
        "result": {
            "__typename": "SearchQuery",
            "rest_id": "test query",
            "timeline_response": {
                "__typename": "Timeline",
                "id": "VGltZWxpbmU6test",
                "timeline": {
                    "__typename": "Timeline",
                    "instructions": [
                        {
                            "__typename": "TimelineAddEntries",
                            "entries": entries,
                        }
                    ],
                },
            },
        },
    }


def _make_settings(tmp_path: Path, enable_live: bool = False) -> Settings:
    return Settings(
        project_root=tmp_path,
        enable_live_crawl=enable_live,
        twitter_rapidapi_key="test_key",
        twitter_rapidapi_host="twitter241.p.rapidapi.com",
    )


def _get_conn():
    import os

    dsn = os.getenv(
        "SIGNALS_TEST_PG_DSN",
        "postgresql://signals:signals_dev_password@127.0.0.1:55432/signals_test",
    )
    return db.get_connection(dsn)


# ---------------------------------------------------------------------------
# Integration: _parse_rapidapi_tweets with twitter241 shape
# ---------------------------------------------------------------------------


class TestParseRapidapiTweetsIntegration:
    def test_parses_twitter241_graphql_shape(self):
        tweets_in = [
            {"id": "111", "text": "kubernetes is amazing", "created_at": "2026-03-01T10:00:00Z"},
            {"id": "222", "text": "terraform devops rocks", "created_at": "2026-03-02T11:00:00Z"},
        ]
        data = _make_twitter241_response(tweets_in)
        result = _parse_rapidapi_tweets(data)
        assert len(result) == 2
        assert result[0]["id"] == "111"
        assert "kubernetes" in result[0]["text"]
        assert "2026-03-01" in result[0]["created_at"]

    def test_skips_entries_without_text(self):
        data = _make_twitter241_response([])
        result = _parse_rapidapi_tweets(data)
        assert result == []

    def test_falls_back_to_flat_shape(self):
        data = {"data": [{"id": "999", "text": "flat tweet", "created_at": "2026-03-01T00:00:00Z"}]}
        result = _parse_rapidapi_tweets(data)
        assert len(result) == 1
        assert result[0]["text"] == "flat tweet"

    def test_timeline_flat_shape(self):
        data = {"timeline": [{"tweet_id": "888", "text": "timeline tweet", "created_at": ""}]}
        result = _parse_rapidapi_tweets(data)
        assert len(result) == 1
        assert result[0]["id"] == "888"


# ---------------------------------------------------------------------------
# Integration: _ingest_tweets with real DB
# ---------------------------------------------------------------------------


class TestIngestTweetsIntegration:
    def test_inserts_matched_tweet_to_db(self):
        conn = _get_conn()
        account_id = db.upsert_account(
            conn, company_name="Test Corp", domain="testcorp.com", source_type="seed", commit=False
        )
        lexicon = [{"signal_code": "kubernetes_detected", "keyword": "kubernetes", "confidence": "0.7"}]
        tweets = [{"id": "tweet_001", "text": "We love kubernetes in prod", "created_at": "2026-03-01T10:00:00Z"}]

        inserted, seen = _ingest_tweets(conn, account_id, "twitter_api", 0.75, lexicon, tweets, {})
        conn.commit()

        assert inserted == 1
        assert seen == 1

        # Verify it's in the DB
        row = conn.execute(
            "SELECT signal_code, source, confidence FROM signal_observations WHERE account_id = %s",
            (account_id,),
        ).fetchone()
        conn.close()
        assert row is not None
        assert row["signal_code"] == "kubernetes_detected"
        assert row["source"] == "twitter_api"

    def test_dedup_same_tweet_not_inserted_twice(self):
        conn = _get_conn()
        account_id = db.upsert_account(
            conn, company_name="Dedup Corp", domain="dedupcorp.com", source_type="seed", commit=False
        )
        lexicon = [{"signal_code": "terraform_detected", "keyword": "terraform", "confidence": "0.7"}]
        tweets = [{"id": "tweet_dedup", "text": "using terraform everywhere", "created_at": "2026-03-01T10:00:00Z"}]

        inserted1, _ = _ingest_tweets(conn, account_id, "twitter_api", 0.75, lexicon, tweets, {})
        conn.commit()
        inserted2, _ = _ingest_tweets(conn, account_id, "twitter_api", 0.75, lexicon, tweets, {})
        conn.commit()

        count = conn.execute(
            "SELECT COUNT(*) AS c FROM signal_observations WHERE account_id = %s AND signal_code = 'terraform_detected'",
            (account_id,),
        ).fetchone()["c"]
        conn.close()

        assert inserted1 == 1
        assert inserted2 == 0  # deduped
        assert count == 1

    def test_multiple_keywords_per_tweet(self):
        conn = _get_conn()
        account_id = db.upsert_account(
            conn, company_name="Multi Corp", domain="multicorp.com", source_type="seed", commit=False
        )
        lexicon = [
            {"signal_code": "kubernetes_detected", "keyword": "kubernetes", "confidence": "0.7"},
            {"signal_code": "terraform_detected", "keyword": "terraform", "confidence": "0.7"},
        ]
        tweets = [
            {
                "id": "tweet_multi",
                "text": "using kubernetes and terraform together",
                "created_at": "2026-03-01T10:00:00Z",
            }
        ]

        inserted, seen = _ingest_tweets(conn, account_id, "twitter_api", 0.75, lexicon, tweets, {})
        conn.commit()
        conn.close()

        assert seen == 2
        assert inserted == 2

    def test_evidence_url_format(self):
        conn = _get_conn()
        account_id = db.upsert_account(
            conn, company_name="URL Corp", domain="urlcorp.com", source_type="seed", commit=False
        )
        lexicon = [{"signal_code": "kubernetes_detected", "keyword": "kubernetes", "confidence": "0.7"}]
        tweets = [{"id": "tweet_url123", "text": "kubernetes cluster setup", "created_at": "2026-03-01T10:00:00Z"}]

        _ingest_tweets(conn, account_id, "twitter_api", 0.75, lexicon, tweets, {})
        conn.commit()

        row = conn.execute(
            "SELECT evidence_url FROM signal_observations WHERE account_id = %s",
            (account_id,),
        ).fetchone()
        conn.close()
        assert row["evidence_url"] == "https://twitter.com/i/web/status/tweet_url123"


# ---------------------------------------------------------------------------
# Integration: collect() CSV phase with real DB
# ---------------------------------------------------------------------------


class TestCollectCsvPhaseIntegration:
    @pytest.mark.asyncio
    async def test_csv_phase_inserts_signal_to_db(self, tmp_path):
        # Write a CSV with a matching tweet
        raw_dir = tmp_path / "data" / "raw"
        raw_dir.mkdir(parents=True)
        csv_file = raw_dir / "twitter.csv"
        csv_file.write_text(
            "domain,company_name,url,text,signal_code,confidence,observed_at\n"
            "stripe.com,Stripe,,we use kubernetes in prod,kubernetes_detected,0.75,2026-03-01T10:00:00+00:00\n"
        )

        settings = _make_settings(tmp_path, enable_live=False)
        conn = _get_conn()
        lexicon = {"twitter": [{"signal_code": "kubernetes_detected", "keyword": "kubernetes", "confidence": "0.7"}]}
        source_reliability = {"twitter_csv": 0.70}

        result = await collect(conn, settings, lexicon, source_reliability)
        conn.commit()

        assert result["inserted"] >= 1
        assert result["seen"] >= 1

        row = conn.execute(
            "SELECT signal_code, source FROM signal_observations WHERE source = 'twitter_csv' LIMIT 1"
        ).fetchone()
        conn.close()
        assert row is not None
        assert row["signal_code"] == "kubernetes_detected"

    @pytest.mark.asyncio
    async def test_csv_with_explicit_signal_code(self, tmp_path):
        raw_dir = tmp_path / "data" / "raw"
        raw_dir.mkdir(parents=True)
        csv_file = raw_dir / "twitter.csv"
        csv_file.write_text(
            "domain,company_name,url,text,signal_code,confidence,observed_at\n"
            "hashicorp.com,HashiCorp,,company tweet text,terraform_detected,0.80,2026-03-01T12:00:00+00:00\n"
        )

        settings = _make_settings(tmp_path, enable_live=False)
        conn = _get_conn()

        result = await collect(conn, settings, {}, {"twitter_csv": 0.70})
        conn.commit()

        assert result["inserted"] == 1
        row = conn.execute(
            "SELECT signal_code, confidence FROM signal_observations WHERE source = 'twitter_csv' LIMIT 1"
        ).fetchone()
        conn.close()
        assert row["signal_code"] == "terraform_detected"
        assert abs(row["confidence"] - 0.80) < 0.01

    @pytest.mark.asyncio
    async def test_csv_skips_row_without_domain(self, tmp_path):
        raw_dir = tmp_path / "data" / "raw"
        raw_dir.mkdir(parents=True)
        (raw_dir / "twitter.csv").write_text(
            "domain,company_name,url,text,signal_code,confidence,observed_at\n"
            ",,,,kubernetes_detected,0.7,2026-03-01T10:00:00+00:00\n"
        )
        settings = _make_settings(tmp_path, enable_live=False)
        conn = _get_conn()
        result = await collect(conn, settings, {}, {"twitter_csv": 0.70})
        conn.commit()
        conn.close()
        assert result["inserted"] == 0

    @pytest.mark.asyncio
    async def test_csv_dedup_on_second_run(self, tmp_path):
        raw_dir = tmp_path / "data" / "raw"
        raw_dir.mkdir(parents=True)
        (raw_dir / "twitter.csv").write_text(
            "domain,company_name,url,text,signal_code,confidence,observed_at\n"
            "acme.com,Acme,,kubernetes cluster,kubernetes_detected,0.7,2026-03-01T10:00:00+00:00\n"
        )
        settings = _make_settings(tmp_path, enable_live=False)
        lexicon = {"twitter": [{"signal_code": "kubernetes_detected", "keyword": "kubernetes", "confidence": "0.7"}]}

        conn = _get_conn()
        r1 = await collect(conn, settings, lexicon, {"twitter_csv": 0.70})
        conn.commit()
        r2 = await collect(conn, settings, lexicon, {"twitter_csv": 0.70})
        conn.commit()
        conn.close()

        assert r1["inserted"] == 1
        assert r2["inserted"] == 0  # same obs_id → ON CONFLICT DO NOTHING


# ---------------------------------------------------------------------------
# Integration: collect() live phase with mocked HTTP
# ---------------------------------------------------------------------------


class TestCollectLivePhaseIntegration:
    @pytest.mark.asyncio
    async def test_live_phase_inserts_from_api(self, tmp_path):
        """collect() with live crawl enabled → mocked API → observations in DB."""
        raw_dir = tmp_path / "data" / "raw"
        raw_dir.mkdir(parents=True)
        (raw_dir / "twitter.csv").write_text("domain,company_name,url,text,signal_code,confidence,observed_at\n")

        settings = _make_settings(tmp_path, enable_live=True)

        # Seed an account to crawl
        conn = _get_conn()
        db.upsert_account(conn, company_name="Stripe", domain="stripe.com", source_type="seed", commit=True)

        # Build a fake API response
        api_response = _make_twitter241_response(
            [
                {"id": "live_001", "text": "stripe uses kubernetes extensively", "created_at": "2026-03-03T09:00:00Z"},
            ]
        )

        def _mock_transport(request):
            return httpx.Response(200, json=api_response)

        lexicon = {"twitter": [{"signal_code": "kubernetes_detected", "keyword": "kubernetes", "confidence": "0.7"}]}
        source_reliability = {"twitter_csv": 0.70, "twitter_api": 0.75}

        with patch("httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_resp = MagicMock()
            mock_resp.status_code = 200
            mock_resp.raise_for_status = MagicMock()
            mock_resp.json.return_value = api_response
            mock_client.get = AsyncMock(return_value=mock_resp)
            mock_client_cls.return_value = mock_client

            result = await collect(conn, settings, lexicon, source_reliability)

        conn.commit()

        assert result["inserted"] >= 1
        row = conn.execute(
            "SELECT signal_code, source FROM signal_observations WHERE source = 'twitter_api' LIMIT 1"
        ).fetchone()
        conn.close()
        assert row is not None
        assert row["signal_code"] == "kubernetes_detected"

    @pytest.mark.asyncio
    async def test_live_phase_rate_limit_handled(self, tmp_path):
        """429 from API → crawl attempt recorded, no crash."""
        raw_dir = tmp_path / "data" / "raw"
        raw_dir.mkdir(parents=True)
        (raw_dir / "twitter.csv").write_text("domain,company_name,url,text,signal_code,confidence,observed_at\n")
        settings = _make_settings(tmp_path, enable_live=True)

        conn = _get_conn()
        db.upsert_account(conn, company_name="RateTest", domain="ratetest.com", source_type="seed", commit=True)

        with patch("httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            error_response = MagicMock()
            error_response.status_code = 429
            mock_client.get = AsyncMock(
                side_effect=httpx.HTTPStatusError("429", request=MagicMock(), response=error_response)
            )
            mock_client_cls.return_value = mock_client

            result = await collect(conn, settings, {}, {"twitter_csv": 0.70, "twitter_api": 0.75})

        conn.commit()
        conn.close()

        # Should not crash; inserted count stays 0
        assert result["inserted"] == 0

    @pytest.mark.asyncio
    async def test_live_skipped_when_no_api_key(self, tmp_path, caplog):
        """enable_live_crawl=True but no key → warning logged, inserted=0."""
        import logging

        raw_dir = tmp_path / "data" / "raw"
        raw_dir.mkdir(parents=True)
        (raw_dir / "twitter.csv").write_text("domain,company_name,url,text,signal_code,confidence,observed_at\n")
        settings = Settings(
            project_root=tmp_path,
            enable_live_crawl=True,
            twitter_rapidapi_key="",
            twitter_bearer_token="",
        )
        conn = _get_conn()
        with caplog.at_level(logging.WARNING, logger="src.collectors.twitter"):
            result = await collect(conn, settings, {}, {})
        conn.commit()
        conn.close()

        assert result["inserted"] == 0
        assert any("no_api_key" in r.message for r in caplog.records)
