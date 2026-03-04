"""System tests for Twitter collector — full pipeline flow, black-box style.

These tests treat the Twitter collector as a black box: they only set up
inputs (DB accounts, env config, mocked HTTP) and assert on observable
outputs (DB state, return values, log messages).  No internal functions
are called directly.
"""

from __future__ import annotations

import json
import logging
from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from src import db
from src.collectors import twitter
from src.settings import Settings

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _conn():
    return db.get_connection(
        "postgresql://signals:signals_dev_password@127.0.0.1:55432/signals_test"
    )


def _settings(tmp_path: Path, **kwargs) -> Settings:
    defaults = dict(
        project_root=tmp_path,
        enable_live_crawl=False,
        twitter_rapidapi_key="test_key_sys",
        twitter_rapidapi_host="twitter241.p.rapidapi.com",
    )
    defaults.update(kwargs)
    return Settings(**defaults)


def _empty_csv(tmp_path: Path) -> None:
    raw = tmp_path / "data" / "raw"
    raw.mkdir(parents=True, exist_ok=True)
    (raw / "twitter.csv").write_text(
        "domain,company_name,url,text,signal_code,confidence,observed_at\n"
    )


def _graphql_response(*tweet_texts: str) -> dict:
    """Build a twitter241-style GraphQL response from plain tweet texts."""
    entries = []
    for i, text in enumerate(tweet_texts):
        tweet_id = f"sys_tweet_{i:04d}"
        entries.append({
            "__typename": "TimelineTimelineEntry",
            "entry_id": f"tweet-{tweet_id}",
            "sort_index": str(i),
            "content": {
                "__typename": "TimelineTimelineItem",
                "content": {
                    "__typename": "TimelineTweet",
                    "tweet_display_type": "Tweet",
                    "tweet_results": {
                        "__typename": "TweetResults",
                        "result": {
                            "__typename": "Tweet",
                            "rest_id": tweet_id,
                            "details": {
                                "__typename": "TweetDetails",
                                "full_text": text,
                                "created_at_ms": 1741000000000,
                            },
                            "legacy": {"__typename": "TweetLegacy"},
                        },
                    },
                },
            },
        })
    return {
        "cursor": {"top": "", "bottom": ""},
        "result": {
            "__typename": "SearchQuery",
            "rest_id": "test",
            "timeline_response": {
                "__typename": "Timeline",
                "id": "test_id",
                "timeline": {
                    "__typename": "Timeline",
                    "instructions": [{"__typename": "TimelineAddEntries", "entries": entries}],
                },
            },
        },
    }


def _mock_http_client(response_json: dict):
    """Return a context-manager-compatible AsyncMock that returns response_json."""
    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.raise_for_status = MagicMock()
    mock_resp.json.return_value = response_json
    mock_client.get = AsyncMock(return_value=mock_resp)
    return mock_client


# ---------------------------------------------------------------------------
# SYS-01: No CSV, no live crawl → zero output
# ---------------------------------------------------------------------------


class TestSys01NoInputNoOutput:
    @pytest.mark.asyncio
    async def test_empty_csv_no_live_returns_zero(self, tmp_path):
        _empty_csv(tmp_path)
        settings = _settings(tmp_path, enable_live_crawl=False)
        conn = _conn()

        result = await twitter.collect(conn, settings, {}, {})
        conn.commit()
        conn.close()

        assert result == {"inserted": 0, "seen": 0}


# ---------------------------------------------------------------------------
# SYS-02: CSV-only signal ingestion
# ---------------------------------------------------------------------------


class TestSys02CsvSignalIngestion:
    @pytest.mark.asyncio
    async def test_kubernetes_signal_from_csv_reaches_db(self, tmp_path):
        raw = tmp_path / "data" / "raw"
        raw.mkdir(parents=True)
        (raw / "twitter.csv").write_text(
            "domain,company_name,url,text,signal_code,confidence,observed_at\n"
            "acme.com,Acme Inc,,We are moving everything to Kubernetes,kubernetes_detected,0.75,2026-03-01T09:00:00+00:00\n"
        )
        settings = _settings(tmp_path)
        conn = _conn()

        result = await twitter.collect(conn, settings, {}, {"twitter_csv": 0.70})
        conn.commit()

        count = conn.execute(
            "SELECT COUNT(*) AS c FROM signal_observations "
            "WHERE source = 'twitter_csv' AND signal_code = 'kubernetes_detected'"
        ).fetchone()["c"]
        conn.close()

        assert result["inserted"] == 1
        assert count == 1

    @pytest.mark.asyncio
    async def test_multiple_csv_rows_multiple_accounts(self, tmp_path):
        raw = tmp_path / "data" / "raw"
        raw.mkdir(parents=True)
        (raw / "twitter.csv").write_text(
            "domain,company_name,url,text,signal_code,confidence,observed_at\n"
            "stripe.com,Stripe,,kubernetes at stripe,kubernetes_detected,0.7,2026-03-01T10:00:00+00:00\n"
            "hashicorp.com,HashiCorp,,terraform everywhere,terraform_detected,0.8,2026-03-01T11:00:00+00:00\n"
        )
        settings = _settings(tmp_path)
        conn = _conn()
        result = await twitter.collect(conn, settings, {}, {"twitter_csv": 0.70})
        conn.commit()

        accounts = conn.execute("SELECT COUNT(DISTINCT account_id) AS c FROM signal_observations WHERE source = 'twitter_csv'").fetchone()["c"]
        conn.close()

        assert result["inserted"] == 2
        assert accounts == 2

    @pytest.mark.asyncio
    async def test_csv_keyword_match_via_lexicon(self, tmp_path):
        raw = tmp_path / "data" / "raw"
        raw.mkdir(parents=True)
        (raw / "twitter.csv").write_text(
            "domain,company_name,url,text,signal_code,confidence,observed_at\n"
            "myco.com,MyCo,,our terraform pipeline is slow,,0.0,2026-03-01T10:00:00+00:00\n"
        )
        settings = _settings(tmp_path)
        conn = _conn()
        lexicon = {"twitter": [{"signal_code": "terraform_detected", "keyword": "terraform", "confidence": "0.7"}]}
        result = await twitter.collect(conn, settings, lexicon, {"twitter_csv": 0.70})
        conn.commit()
        conn.close()

        assert result["inserted"] == 1

    @pytest.mark.asyncio
    async def test_csv_zero_reliability_skips_all(self, tmp_path):
        raw = tmp_path / "data" / "raw"
        raw.mkdir(parents=True)
        (raw / "twitter.csv").write_text(
            "domain,company_name,url,text,signal_code,confidence,observed_at\n"
            "acme.com,Acme,,kubernetes cluster,kubernetes_detected,0.7,2026-03-01T10:00:00+00:00\n"
        )
        settings = _settings(tmp_path)
        conn = _conn()
        result = await twitter.collect(conn, settings, {}, {"twitter_csv": 0.0})
        conn.commit()
        conn.close()

        assert result["inserted"] == 0


# ---------------------------------------------------------------------------
# SYS-03: Live API signal ingestion (mocked HTTP)
# ---------------------------------------------------------------------------


class TestSys03LiveApiIngestion:
    @pytest.mark.asyncio
    async def test_live_tweet_with_keyword_stored_in_db(self, tmp_path):
        _empty_csv(tmp_path)
        settings = _settings(tmp_path, enable_live_crawl=True)
        conn = _conn()
        db.upsert_account(conn, company_name="Acme", domain="acme.com", source_type="seed", commit=True)

        api_resp = _graphql_response("acme uses kubernetes and terraform heavily")
        lexicon = {
            "twitter": [
                {"signal_code": "kubernetes_detected", "keyword": "kubernetes", "confidence": "0.7"},
                {"signal_code": "terraform_detected", "keyword": "terraform", "confidence": "0.7"},
            ]
        }

        with patch("httpx.AsyncClient", return_value=_mock_http_client(api_resp)):
            result = await twitter.collect(conn, settings, lexicon, {"twitter_csv": 0.70, "twitter_api": 0.75})
        conn.commit()

        rows = conn.execute(
            "SELECT signal_code FROM signal_observations WHERE source = 'twitter_api' ORDER BY signal_code"
        ).fetchall()
        conn.close()

        signal_codes = [r["signal_code"] for r in rows]
        assert result["inserted"] == 2
        assert "kubernetes_detected" in signal_codes
        assert "terraform_detected" in signal_codes

    @pytest.mark.asyncio
    async def test_live_tweet_no_keyword_match_zero_inserted(self, tmp_path):
        _empty_csv(tmp_path)
        settings = _settings(tmp_path, enable_live_crawl=True)
        conn = _conn()
        db.upsert_account(conn, company_name="NoMatch Co", domain="nomatch.com", source_type="seed", commit=True)

        api_resp = _graphql_response("Our quarterly earnings beat expectations by 10%")
        lexicon = {"twitter": [{"signal_code": "kubernetes_detected", "keyword": "kubernetes", "confidence": "0.7"}]}

        with patch("httpx.AsyncClient", return_value=_mock_http_client(api_resp)):
            result = await twitter.collect(conn, settings, lexicon, {"twitter_csv": 0.70, "twitter_api": 0.75})
        conn.commit()
        conn.close()

        assert result["inserted"] == 0

    @pytest.mark.asyncio
    async def test_live_example_domain_skipped(self, tmp_path):
        _empty_csv(tmp_path)
        settings = _settings(tmp_path, enable_live_crawl=True)
        conn = _conn()
        db.upsert_account(conn, company_name="Example", domain="skip.example", source_type="seed", commit=True)

        api_resp = _graphql_response("kubernetes everywhere")

        with patch("httpx.AsyncClient", return_value=_mock_http_client(api_resp)):
            result = await twitter.collect(
                conn, settings,
                {"twitter": [{"signal_code": "kubernetes_detected", "keyword": "kubernetes", "confidence": "0.7"}]},
                {"twitter_csv": 0.70, "twitter_api": 0.75},
            )
        conn.commit()
        conn.close()

        # .example domain must be skipped entirely
        assert result["inserted"] == 0

    @pytest.mark.asyncio
    async def test_live_crawl_checkpoint_prevents_second_fetch(self, tmp_path):
        """Same account + endpoint fetched twice → second call skipped via crawl checkpoint."""
        _empty_csv(tmp_path)
        settings = _settings(tmp_path, enable_live_crawl=True)
        conn = _conn()
        db.upsert_account(conn, company_name="Dup Inc", domain="dupinc.com", source_type="seed", commit=True)

        api_resp = _graphql_response("kubernetes at dupinc")
        lexicon = {"twitter": [{"signal_code": "kubernetes_detected", "keyword": "kubernetes", "confidence": "0.7"}]}
        reliability = {"twitter_csv": 0.70, "twitter_api": 0.75}

        with patch("httpx.AsyncClient", return_value=_mock_http_client(api_resp)):
            r1 = await twitter.collect(conn, settings, lexicon, reliability)
        conn.commit()

        with patch("httpx.AsyncClient", return_value=_mock_http_client(api_resp)):
            r2 = await twitter.collect(conn, settings, lexicon, reliability)
        conn.commit()
        conn.close()

        assert r1["inserted"] >= 1
        # Second run: either deduped by obs_id OR skipped by checkpoint
        assert r2["inserted"] == 0


# ---------------------------------------------------------------------------
# SYS-04: Error handling (black-box)
# ---------------------------------------------------------------------------


class TestSys04ErrorHandling:
    @pytest.mark.asyncio
    async def test_http_500_does_not_crash(self, tmp_path):
        _empty_csv(tmp_path)
        settings = _settings(tmp_path, enable_live_crawl=True)
        conn = _conn()
        db.upsert_account(conn, company_name="Err Co", domain="errco.com", source_type="seed", commit=True)

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        err_resp = MagicMock()
        err_resp.status_code = 500
        mock_client.get = AsyncMock(
            side_effect=httpx.HTTPStatusError("500", request=MagicMock(), response=err_resp)
        )

        with patch("httpx.AsyncClient", return_value=mock_client):
            result = await twitter.collect(conn, settings, {}, {"twitter_csv": 0.70, "twitter_api": 0.75})
        conn.commit()
        conn.close()

        assert result["inserted"] == 0

    @pytest.mark.asyncio
    async def test_network_exception_does_not_crash(self, tmp_path):
        _empty_csv(tmp_path)
        settings = _settings(tmp_path, enable_live_crawl=True)
        conn = _conn()
        db.upsert_account(conn, company_name="Net Co", domain="netco.com", source_type="seed", commit=True)

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.get = AsyncMock(side_effect=httpx.ConnectError("timeout"))

        with patch("httpx.AsyncClient", return_value=mock_client):
            result = await twitter.collect(conn, settings, {}, {"twitter_csv": 0.70, "twitter_api": 0.75})
        conn.commit()
        conn.close()

        assert result["inserted"] == 0

    @pytest.mark.asyncio
    async def test_malformed_api_response_does_not_crash(self, tmp_path):
        _empty_csv(tmp_path)
        settings = _settings(tmp_path, enable_live_crawl=True)
        conn = _conn()
        db.upsert_account(conn, company_name="Bad JSON Co", domain="badjson.com", source_type="seed", commit=True)

        mock_client = _mock_http_client({"unexpected": "format", "no_result_key": True})

        with patch("httpx.AsyncClient", return_value=mock_client):
            result = await twitter.collect(conn, settings, {}, {"twitter_csv": 0.70, "twitter_api": 0.75})
        conn.commit()
        conn.close()

        assert result["inserted"] == 0


# ---------------------------------------------------------------------------
# SYS-05: Signal scoring picks up twitter observations
# ---------------------------------------------------------------------------


class TestSys05ScoringPicksUpTwitter:
    @pytest.mark.asyncio
    async def test_twitter_observation_contributes_to_score(self, tmp_path):
        """Twitter observation stored → scoring engine produces a non-zero score."""
        from src.scoring.engine import run_scoring
        from src.scoring.rules import load_signal_rules, load_source_registry, load_thresholds

        raw = tmp_path / "data" / "raw"
        raw.mkdir(parents=True)
        (raw / "twitter.csv").write_text(
            "domain,company_name,url,text,signal_code,confidence,observed_at\n"
            "scoretest.com,ScoreTest Inc,,we love kubernetes,kubernetes_detected,0.8,2026-03-01T10:00:00+00:00\n"
        )
        settings = _settings(tmp_path)
        conn = _conn()

        # Ingest signal
        await twitter.collect(conn, settings, {}, {"twitter_csv": 0.70})
        conn.commit()

        # Run scoring using the same pattern as main.py _run_scoring()
        project_root = Path(__file__).resolve().parents[1]
        signal_rules = load_signal_rules(project_root / "config" / "signal_registry.csv")
        source_registry = load_source_registry(project_root / "config" / "source_registry.csv")
        thresholds = load_thresholds(project_root / "config" / "thresholds.csv")

        run_date = date(2026, 3, 1)
        run_id = db.create_score_run(conn, "2026-03-01")
        observations = db.fetch_observations_for_scoring(conn, "2026-03-01")
        result = run_scoring(
            run_id=run_id,
            run_date=run_date,
            observations=[dict(row) for row in observations],
            rules=signal_rules,
            thresholds=thresholds,
            source_reliability_defaults=source_registry,
        )
        db.replace_run_scores(conn, run_id, result.component_scores, result.account_scores)
        conn.commit()

        # Find the scoretest.com account
        acct = conn.execute(
            "SELECT account_id FROM accounts WHERE domain = 'scoretest.com'"
        ).fetchone()
        assert acct is not None

        score_row = conn.execute(
            "SELECT score, tier FROM account_scores WHERE account_id = %s AND run_id = %s LIMIT 1",
            (acct["account_id"], run_id),
        ).fetchone()
        conn.close()

        assert score_row is not None
        assert score_row["score"] > 0
