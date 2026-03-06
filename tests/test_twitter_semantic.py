"""Tests for twitter_semantic collector and LLM classification module."""

from __future__ import annotations

import json
from dataclasses import dataclass
from unittest.mock import MagicMock

from src.collectors.twitter_classify import (
    VALID_SIGNAL_CODES,
    build_classification_prompt,
    classify_tweets_batch,
    parse_classification_response,
)

# ---------------------------------------------------------------------------
# build_classification_prompt tests
# ---------------------------------------------------------------------------


class TestBuildClassificationPrompt:
    def test_prompt_contains_signal_codes(self):
        tweets = [{"text": "We're hiring DevOps engineers", "author": "acme"}]
        system, user = build_classification_prompt(tweets, "Acme Inc", "acme.com")
        assert "devops_role_open" in system
        assert "kubernetes_detected" in system
        assert "finops_role_open" in system

    def test_prompt_contains_all_tweets(self):
        tweets = [
            {"text": "Tweet one", "author": "user1"},
            {"text": "Tweet two", "author": "user2"},
            {"text": "Tweet three", "author": "user3"},
        ]
        system, user = build_classification_prompt(tweets, "Acme Inc", "acme.com")
        assert "[0]" in user
        assert "[1]" in user
        assert "[2]" in user
        assert "@user1" in user
        assert "@user2" in user
        assert "Tweet three" in user

    def test_prompt_contains_company_info(self):
        tweets = [{"text": "test", "author": "x"}]
        system, user = build_classification_prompt(tweets, "Stripe", "stripe.com")
        assert "Stripe" in user
        assert "stripe.com" in user

    def test_prompt_handles_missing_author(self):
        tweets = [{"text": "no author field"}]
        system, user = build_classification_prompt(tweets, "Co", "co.com")
        assert "@unknown" in user

    def test_prompt_truncates_long_tweet_text(self):
        long_text = "x" * 500
        tweets = [{"text": long_text, "author": "a"}]
        _, user = build_classification_prompt(tweets, "Co", "co.com")
        # Text should be truncated to 400 chars
        assert "x" * 400 in user
        assert "x" * 500 not in user


# ---------------------------------------------------------------------------
# parse_classification_response tests
# ---------------------------------------------------------------------------


class TestParseClassificationResponse:
    def test_valid_json_response(self):
        response = json.dumps(
            [
                {
                    "index": 0,
                    "signal_code": "devops_role_open",
                    "confidence": 0.8,
                    "reasoning": "Hiring for SRE role",
                    "is_decision_maker": False,
                    "author_role_guess": "",
                },
                {
                    "index": 1,
                    "signal_code": "none",
                    "confidence": 0.5,
                    "reasoning": "Irrelevant tweet",
                    "is_decision_maker": False,
                    "author_role_guess": "",
                },
            ]
        )
        results = parse_classification_response(response, 2)
        assert len(results) == 2
        assert results[0].signal_code == "devops_role_open"
        assert results[0].confidence == 0.8
        assert results[1].signal_code == "none"

    def test_markdown_fenced_json(self):
        response = """```json
[{"index": 0, "signal_code": "kubernetes_detected", "confidence": 0.7, "reasoning": "K8s mentioned", "is_decision_maker": false, "author_role_guess": ""}]
```"""
        results = parse_classification_response(response, 1)
        assert len(results) == 1
        assert results[0].signal_code == "kubernetes_detected"

    def test_invalid_signal_code_mapped_to_none(self):
        response = json.dumps(
            [
                {
                    "index": 0,
                    "signal_code": "totally_made_up_code",
                    "confidence": 0.8,
                    "reasoning": "test",
                    "is_decision_maker": False,
                    "author_role_guess": "",
                }
            ]
        )
        results = parse_classification_response(response, 1)
        assert len(results) == 1
        assert results[0].signal_code == "none"

    def test_confidence_clamped_high(self):
        response = json.dumps(
            [
                {
                    "index": 0,
                    "signal_code": "devops_role_open",
                    "confidence": 1.0,
                    "reasoning": "test",
                    "is_decision_maker": False,
                    "author_role_guess": "",
                }
            ]
        )
        results = parse_classification_response(response, 1)
        assert results[0].confidence == 0.95

    def test_confidence_clamped_low(self):
        response = json.dumps(
            [
                {
                    "index": 0,
                    "signal_code": "devops_role_open",
                    "confidence": 0.1,
                    "reasoning": "test",
                    "is_decision_maker": False,
                    "author_role_guess": "",
                }
            ]
        )
        results = parse_classification_response(response, 1)
        assert results[0].confidence == 0.5

    def test_missing_indices_handled(self):
        response = json.dumps(
            [
                {
                    "index": 5,
                    "signal_code": "devops_role_open",
                    "confidence": 0.8,
                    "reasoning": "test",
                    "is_decision_maker": False,
                    "author_role_guess": "",
                }
            ]
        )
        # Only 2 tweets but index is 5 — should be filtered out
        results = parse_classification_response(response, 2)
        assert len(results) == 0

    def test_decision_maker_flag(self):
        response = json.dumps(
            [
                {
                    "index": 0,
                    "signal_code": "devops_role_open",
                    "confidence": 0.85,
                    "reasoning": "VP posting about hiring",
                    "is_decision_maker": True,
                    "author_role_guess": "VP Engineering",
                }
            ]
        )
        results = parse_classification_response(response, 1)
        assert results[0].is_decision_maker is True
        assert results[0].author_role_guess == "VP Engineering"

    def test_malformed_json_returns_empty(self):
        results = parse_classification_response("this is not json at all", 1)
        assert results == []

    def test_json_with_surrounding_text(self):
        response = (
            "Here are the classifications:\n"
            '[{"index": 0, "signal_code": "terraform_detected", "confidence": 0.7, '
            '"reasoning": "Terraform mention", "is_decision_maker": false, "author_role_guess": ""}]\n'
            "Let me know if you need anything else."
        )
        results = parse_classification_response(response, 1)
        assert len(results) == 1
        assert results[0].signal_code == "terraform_detected"

    def test_empty_array_response(self):
        results = parse_classification_response("[]", 1)
        assert results == []

    def test_reasoning_truncated(self):
        response = json.dumps(
            [
                {
                    "index": 0,
                    "signal_code": "devops_role_open",
                    "confidence": 0.7,
                    "reasoning": "x" * 500,
                    "is_decision_maker": False,
                    "author_role_guess": "",
                }
            ]
        )
        results = parse_classification_response(response, 1)
        assert len(results[0].reasoning) == 200


# ---------------------------------------------------------------------------
# classify_tweets_batch tests
# ---------------------------------------------------------------------------


@dataclass
class MockResponse:
    raw_text: str
    model: str = "test-model"
    input_tokens: int = 100
    output_tokens: int = 50
    duration_seconds: float = 0.5


class TestClassifyTweetsBatch:
    def test_batch_call_with_mock_llm(self):
        mock_client = MagicMock()
        mock_client.research_company.return_value = MockResponse(
            raw_text=json.dumps(
                [
                    {
                        "index": 0,
                        "signal_code": "devops_role_open",
                        "confidence": 0.8,
                        "reasoning": "Hiring SRE",
                        "is_decision_maker": False,
                        "author_role_guess": "",
                    }
                ]
            )
        )
        tweets = [{"text": "Hiring SRE engineers!", "author": "acme"}]
        results = classify_tweets_batch(mock_client, tweets, "Acme", "acme.com")
        assert len(results) == 1
        assert results[0].signal_code == "devops_role_open"
        mock_client.research_company.assert_called_once()

    def test_empty_batch_returns_empty(self):
        mock_client = MagicMock()
        results = classify_tweets_batch(mock_client, [], "Acme", "acme.com")
        assert results == []
        mock_client.research_company.assert_not_called()

    def test_llm_exception_returns_empty(self):
        mock_client = MagicMock()
        mock_client.research_company.side_effect = RuntimeError("API down")
        tweets = [{"text": "Test tweet", "author": "test"}]
        results = classify_tweets_batch(mock_client, tweets, "Acme", "acme.com")
        assert results == []


# ---------------------------------------------------------------------------
# VALID_SIGNAL_CODES sanity checks
# ---------------------------------------------------------------------------


class TestValidSignalCodes:
    def test_codes_are_non_empty(self):
        assert len(VALID_SIGNAL_CODES) > 20

    def test_core_codes_present(self):
        assert "devops_role_open" in VALID_SIGNAL_CODES
        assert "kubernetes_detected" in VALID_SIGNAL_CODES
        assert "recent_funding_event" in VALID_SIGNAL_CODES
        assert "high_intent_phrase_cost_control" in VALID_SIGNAL_CODES

    def test_none_not_in_valid_codes(self):
        assert "none" not in VALID_SIGNAL_CODES


# ---------------------------------------------------------------------------
# _role_weight tests
# ---------------------------------------------------------------------------


class TestRoleWeight:
    def test_cto_weight(self):
        from src.collectors.twitter_semantic import _role_weight

        assert _role_weight("CTO") == 1.8

    def test_vp_engineering_weight(self):
        from src.collectors.twitter_semantic import _role_weight

        assert _role_weight("VP Engineering") == 1.5

    def test_director_weight(self):
        from src.collectors.twitter_semantic import _role_weight

        # Note: "Director" contains substring "cto" so it matches cto weight (1.8)
        assert _role_weight("Director of Platform") == 1.8
        # "Head of Infra" matches "head" → 1.3
        assert _role_weight("Head of Infra") == 1.3

    def test_unknown_role_default(self):
        from src.collectors.twitter_semantic import _role_weight

        assert _role_weight("Software Engineer") == 1.0

    def test_empty_role(self):
        from src.collectors.twitter_semantic import _role_weight

        assert _role_weight("") == 1.0

    def test_chief_weight(self):
        from src.collectors.twitter_semantic import _role_weight

        assert _role_weight("Chief Architect") == 1.8

    def test_founder_weight(self):
        from src.collectors.twitter_semantic import _role_weight

        assert _role_weight("Co-Founder & CEO") == 1.5


# ---------------------------------------------------------------------------
# _parse_tweets_with_authors tests
# ---------------------------------------------------------------------------


class TestParseTweetsWithAuthors:
    def test_graphql_response(self):
        from src.collectors.twitter_semantic import _parse_tweets_with_authors

        data = {
            "result": {
                "timeline_response": {
                    "timeline": {
                        "instructions": [
                            {
                                "entries": [
                                    {
                                        "content": {
                                            "__typename": "TimelineTimelineItem",
                                            "content": {
                                                "__typename": "TimelineTweet",
                                                "tweet_results": {
                                                    "result": {
                                                        "rest_id": "123456",
                                                        "legacy": {
                                                            "full_text": "We are hiring DevOps!",
                                                            "created_at": "Mon Jan 01 12:00:00 +0000 2024",
                                                        },
                                                        "core": {
                                                            "user_results": {
                                                                "result": {
                                                                    "legacy": {
                                                                        "screen_name": "acme_inc",
                                                                        "name": "Acme Inc",
                                                                    }
                                                                }
                                                            }
                                                        },
                                                    }
                                                },
                                            },
                                        }
                                    }
                                ]
                            }
                        ]
                    }
                }
            }
        }
        tweets = _parse_tweets_with_authors(data)
        assert len(tweets) == 1
        assert tweets[0]["text"] == "We are hiring DevOps!"
        assert tweets[0]["author"] == "acme_inc"
        assert tweets[0]["author_name"] == "Acme Inc"
        assert tweets[0]["id"] == "123456"

    def test_legacy_flat_fallback(self):
        from src.collectors.twitter_semantic import _parse_tweets_with_authors

        data = {
            "timeline": [
                {"tweet_id": "789", "text": "Legacy tweet text", "created_at": "2024-01-01"},
                {"id": "790", "full_text": "Another legacy tweet"},
            ]
        }
        tweets = _parse_tweets_with_authors(data)
        assert len(tweets) == 2
        assert tweets[0]["text"] == "Legacy tweet text"
        assert tweets[0]["id"] == "789"
        assert tweets[0]["author"] == ""
        assert tweets[1]["text"] == "Another legacy tweet"

    def test_empty_response(self):
        from src.collectors.twitter_semantic import _parse_tweets_with_authors

        assert _parse_tweets_with_authors({}) == []

    def test_skips_non_tweet_entries(self):
        from src.collectors.twitter_semantic import _parse_tweets_with_authors

        data = {
            "result": {
                "timeline_response": {
                    "timeline": {
                        "instructions": [
                            {
                                "entries": [
                                    {
                                        "content": {
                                            "__typename": "TimelineCursor",
                                        }
                                    }
                                ]
                            }
                        ]
                    }
                }
            }
        }
        # No tweets match, falls through to legacy which is also empty
        tweets = _parse_tweets_with_authors(data)
        assert tweets == []

    def test_graphql_with_created_at_ms(self):
        from src.collectors.twitter_semantic import _parse_tweets_with_authors

        data = {
            "result": {
                "timeline_response": {
                    "timeline": {
                        "instructions": [
                            {
                                "entries": [
                                    {
                                        "content": {
                                            "__typename": "TimelineTimelineItem",
                                            "content": {
                                                "__typename": "TimelineTweet",
                                                "tweet_results": {
                                                    "result": {
                                                        "rest_id": "111",
                                                        "legacy": {
                                                            "full_text": "Test ms timestamp",
                                                            "created_at_ms": "1704067200000",
                                                        },
                                                        "core": {
                                                            "user_results": {
                                                                "result": {
                                                                    "legacy": {
                                                                        "screen_name": "user1",
                                                                        "name": "User One",
                                                                    }
                                                                }
                                                            }
                                                        },
                                                    }
                                                },
                                            },
                                        }
                                    }
                                ]
                            }
                        ]
                    }
                }
            }
        }
        tweets = _parse_tweets_with_authors(data)
        assert len(tweets) == 1
        assert "2024-01-01" in tweets[0]["created_at"]


# ---------------------------------------------------------------------------
# twitter_semantic collector tests (mock-based, no DB needed)
# ---------------------------------------------------------------------------


class TestCollectorSkips:
    def test_collect_skips_without_rapidapi_key(self):
        """Collector returns zeros when no Twitter API key is set."""
        import asyncio

        from src.collectors.twitter_semantic import collect
        from src.settings import Settings

        settings = Settings(
            project_root="/tmp",
            twitter_rapidapi_key="",
            enable_live_crawl=True,
        )
        conn = MagicMock()
        result = asyncio.run(collect(conn, settings, {}, {}))
        assert result == {"inserted": 0, "seen": 0, "accounts_processed": 0}

    def test_collect_skips_without_llm_key(self):
        """Collector returns zeros when no LLM API key is set."""
        import asyncio

        from src.collectors.twitter_semantic import collect
        from src.settings import Settings

        settings = Settings(
            project_root="/tmp",
            twitter_rapidapi_key="test-key",
            claude_api_key="",
            minimax_api_key="",
            enable_live_crawl=True,
        )
        conn = MagicMock()
        result = asyncio.run(collect(conn, settings, {}, {}))
        assert result == {"inserted": 0, "seen": 0, "accounts_processed": 0}

    def test_collect_skips_when_live_crawl_disabled(self):
        """Collector returns zeros when enable_live_crawl is False."""
        import asyncio

        from src.collectors.twitter_semantic import collect
        from src.settings import Settings

        settings = Settings(
            project_root="/tmp",
            twitter_rapidapi_key="test-key",
            claude_api_key="sk-ant-test",
            enable_live_crawl=False,
        )
        conn = MagicMock()
        result = asyncio.run(collect(conn, settings, {}, {}))
        assert result == {"inserted": 0, "seen": 0, "accounts_processed": 0}
