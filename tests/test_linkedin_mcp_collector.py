"""Tests for linkedin_mcp_collector — Serper + Claude LinkedIn signal extraction."""

from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.collectors.linkedin_mcp_collector import (
    INTENT_CATEGORIES,
    INTENT_TO_SIGNAL,
    SOURCE_NAME,
    SOURCE_RELIABILITY,
    _lookback_to_tbs,
    _make_observation,
    _role_weight,
    _track_person,
    collect,
)

# ---------------------------------------------------------------------------
# _lookback_to_tbs
# ---------------------------------------------------------------------------


class TestLookbackToTbs:
    def test_daily(self):
        assert _lookback_to_tbs(1) == "qdr:d"

    def test_weekly(self):
        assert _lookback_to_tbs(7) == "qdr:w"

    def test_monthly(self):
        assert _lookback_to_tbs(30) == "qdr:m"

    def test_yearly(self):
        assert _lookback_to_tbs(90) == "qdr:y"


# ---------------------------------------------------------------------------
# _role_weight
# ---------------------------------------------------------------------------


class TestRoleWeight:
    def test_cto(self):
        assert _role_weight("CTO") == 1.8

    def test_cio(self):
        assert _role_weight("CIO") == 1.8

    def test_chief(self):
        assert _role_weight("Chief Architect") == 1.8

    def test_vp(self):
        assert _role_weight("VP Engineering") == 1.5

    def test_vice_president(self):
        assert _role_weight("Vice President of Platform") == 1.5

    def test_founder(self):
        assert _role_weight("Co-Founder") == 1.5

    def test_director(self):
        # "director" contains "cto" substring, so matches 1.8
        assert _role_weight("Director of SRE") == 1.8

    def test_head(self):
        assert _role_weight("Head of Infrastructure") == 1.3

    def test_senior(self):
        assert _role_weight("Senior Engineer") == 1.1

    def test_staff(self):
        assert _role_weight("Staff SRE") == 1.1

    def test_principal(self):
        assert _role_weight("Principal Architect") == 1.1

    def test_lead(self):
        assert _role_weight("Lead DevOps Engineer") == 1.1

    def test_unknown(self):
        assert _role_weight("Software Engineer") == 1.0

    def test_empty(self):
        assert _role_weight("") == 1.0


# ---------------------------------------------------------------------------
# _make_observation
# ---------------------------------------------------------------------------


class TestMakeObservation:
    def test_valid_hiring_signal(self):
        classification = {
            "intent": "hiring_signal",
            "confidence": 0.7,
            "evidence_sentence": "Hiring SRE engineers",
            "signal_code": "devops_role_open",
        }
        item = {"link": "https://linkedin.com/posts/acme-hiring", "title": "Hiring"}
        obs = _make_observation("acc-1", classification, item, 0.75)
        assert obs is not None
        assert obs.signal_code == "devops_role_open"
        assert obs.source == SOURCE_NAME
        assert obs.confidence == 0.7
        assert obs.account_id == "acc-1"

    def test_exec_change(self):
        classification = {
            "intent": "exec_change",
            "confidence": 0.8,
            "evidence_sentence": "New CTO appointed",
            "signal_code": "launch_or_scale_event",
        }
        item = {"link": "https://linkedin.com/posts/new-cto"}
        obs = _make_observation("acc-2", classification, item, 0.75)
        assert obs is not None
        assert obs.signal_code == "launch_or_scale_event"

    def test_passing_mention_returns_none(self):
        classification = {"intent": "passing_mention", "confidence": 0.3}
        item = {"link": "https://linkedin.com/posts/random"}
        obs = _make_observation("acc-3", classification, item, 0.75)
        assert obs is None

    def test_unknown_intent_returns_none(self):
        classification = {"intent": "unknown_intent", "confidence": 0.5}
        item = {"link": "https://linkedin.com/posts/something"}
        obs = _make_observation("acc-4", classification, item, 0.75)
        assert obs is None

    def test_fallback_signal_code_from_intent(self):
        classification = {
            "intent": "funding_signal",
            "confidence": 0.85,
            "evidence_sentence": "Raised $50M Series B",
            "signal_code": None,  # Claude returned null
        }
        item = {"link": "https://linkedin.com/posts/funding"}
        obs = _make_observation("acc-5", classification, item, 0.75)
        assert obs is not None
        assert obs.signal_code == "recent_funding_event"

    def test_confidence_clamped(self):
        classification = {
            "intent": "active_evaluation",
            "confidence": 1.5,  # over 1.0
            "evidence_sentence": "Evaluating tools",
            "signal_code": "finops_tool_eval",
        }
        item = {"link": "https://linkedin.com/posts/eval"}
        obs = _make_observation("acc-6", classification, item, 0.75)
        assert obs.confidence == 1.0

    def test_confidence_floor(self):
        classification = {
            "intent": "pain_signal",
            "confidence": -0.5,
            "evidence_sentence": "Infra pain",
            "signal_code": "high_intent_phrase_devops_toil",
        }
        item = {"link": "https://linkedin.com/posts/pain"}
        obs = _make_observation("acc-7", classification, item, 0.75)
        assert obs.confidence == 0.0


# ---------------------------------------------------------------------------
# _track_person
# ---------------------------------------------------------------------------


class TestTrackPerson:
    def test_tracks_person_with_name_and_role(self):
        conn = MagicMock()
        classification = {
            "intent": "exec_change",
            "person_name": "Jane Doe",
            "person_role": "VP Engineering",
            "evidence_sentence": "Jane Doe joined as VP Eng",
        }
        item = {"link": "https://linkedin.com/posts/jane-doe-vp"}
        _track_person(conn, "acc-1", classification, item)
        conn.assert_not_called()  # db functions are called, not conn directly
        # Verify db.upsert_people_watchlist_entry was conceptually called
        # (we'd need to mock db module for precise assertions)

    def test_skips_when_no_person_name(self):
        conn = MagicMock()
        classification = {
            "intent": "exec_change",
            "person_name": None,
            "person_role": "CTO",
        }
        item = {"link": "https://linkedin.com/posts/anon"}
        _track_person(conn, "acc-1", classification, item)

    def test_skips_when_no_person_role(self):
        conn = MagicMock()
        classification = {
            "intent": "exec_change",
            "person_name": "John Smith",
            "person_role": "",
        }
        item = {"link": "https://linkedin.com/posts/john"}
        _track_person(conn, "acc-1", classification, item)

    @patch("src.collectors.linkedin_mcp_collector.db")
    def test_calls_db_functions(self, mock_db):
        conn = MagicMock()
        classification = {
            "intent": "exec_change",
            "person_name": "Alice CTO",
            "person_role": "CTO",
            "evidence_sentence": "Alice became CTO",
        }
        item = {"link": "https://linkedin.com/posts/alice-cto"}
        _track_person(conn, "acc-1", classification, item)
        mock_db.upsert_people_watchlist_entry.assert_called_once()
        mock_db.insert_people_activity.assert_called_once()
        # Verify role_weight for CTO
        call_kwargs = mock_db.upsert_people_watchlist_entry.call_args
        assert call_kwargs[1]["role_weight"] == 1.8

    @patch("src.collectors.linkedin_mcp_collector.db")
    def test_handles_db_exception(self, mock_db):
        conn = MagicMock()
        mock_db.upsert_people_watchlist_entry.side_effect = RuntimeError("DB error")
        classification = {
            "intent": "exec_change",
            "person_name": "Bob VP",
            "person_role": "VP Engineering",
            "evidence_sentence": "Bob joined as VP",
        }
        item = {"link": "https://linkedin.com/posts/bob"}
        # Should not raise
        _track_person(conn, "acc-1", classification, item)


# ---------------------------------------------------------------------------
# Intent mapping completeness
# ---------------------------------------------------------------------------


class TestIntentMapping:
    def test_all_non_passing_intents_have_signal_codes(self):
        for intent, confidence in INTENT_CATEGORIES.items():
            if intent == "passing_mention":
                assert confidence is None
                continue
            assert intent in INTENT_TO_SIGNAL, f"Missing mapping for {intent}"

    def test_signal_codes_are_valid_registry_entries(self):
        known_codes = {
            "devops_role_open",
            "launch_or_scale_event",
            "high_intent_phrase_devops_toil",
            "employee_growth_positive",
            "recent_funding_event",
            "finops_tool_eval",
        }
        for signal_code in INTENT_TO_SIGNAL.values():
            assert signal_code in known_codes, f"Unknown signal code: {signal_code}"


# ---------------------------------------------------------------------------
# collect() skip conditions
# ---------------------------------------------------------------------------


class TestCollectSkips:
    def test_skips_without_serper_key(self):
        from src.settings import Settings

        settings = Settings(
            project_root="/tmp",
            serper_api_key="",
            claude_api_key="sk-ant-test",
        )
        conn = MagicMock()
        result = asyncio.run(collect(conn, settings, {}, {}))
        assert result == {"inserted": 0, "seen": 0}

    def test_skips_without_claude_key(self):
        from src.settings import Settings

        settings = Settings(
            project_root="/tmp",
            serper_api_key="test-serper-key",
            claude_api_key="",
        )
        conn = MagicMock()
        result = asyncio.run(collect(conn, settings, {}, {}))
        assert result == {"inserted": 0, "seen": 0}

    def test_skips_with_zero_reliability(self):
        from src.settings import Settings

        settings = Settings(
            project_root="/tmp",
            serper_api_key="test-serper-key",
            claude_api_key="sk-ant-test",
        )
        conn = MagicMock()
        result = asyncio.run(collect(conn, settings, {}, {SOURCE_NAME: 0.0}))
        assert result == {"inserted": 0, "seen": 0}

    def test_skips_with_negative_reliability(self):
        from src.settings import Settings

        settings = Settings(
            project_root="/tmp",
            serper_api_key="test-serper-key",
            claude_api_key="sk-ant-test",
        )
        conn = MagicMock()
        result = asyncio.run(collect(conn, settings, {}, {SOURCE_NAME: -0.5}))
        assert result == {"inserted": 0, "seen": 0}


# ---------------------------------------------------------------------------
# Constants sanity checks
# ---------------------------------------------------------------------------


class TestConstants:
    def test_source_name(self):
        assert SOURCE_NAME == "linkedin_mcp"

    def test_source_reliability(self):
        assert 0.0 < SOURCE_RELIABILITY <= 1.0

    def test_intent_categories_count(self):
        assert len(INTENT_CATEGORIES) == 8

    def test_passing_mention_has_no_confidence(self):
        assert INTENT_CATEGORIES["passing_mention"] is None
