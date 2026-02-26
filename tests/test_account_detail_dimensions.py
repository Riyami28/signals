"""Tests for dimension scores, velocity, and signal timeline in account detail."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _mock_conn_with_rows(rows_by_query: dict | None = None, default_rows=None):
    """Return a mock connection whose execute().fetchone/fetchall return canned data."""
    conn = MagicMock()

    def _execute(sql, params=None):
        cursor = MagicMock()
        sql_lower = (sql or "").lower().strip()

        if default_rows is not None:
            cursor.fetchall.return_value = default_rows
            cursor.fetchone.return_value = default_rows[0] if default_rows else None
            return cursor

        if rows_by_query:
            for key, val in rows_by_query.items():
                if key.lower() in sql_lower:
                    cursor.fetchall.return_value = val
                    cursor.fetchone.return_value = val[0] if val else None
                    return cursor

        cursor.fetchall.return_value = []
        cursor.fetchone.return_value = None
        return cursor

    conn.execute = _execute
    return conn


# ---------------------------------------------------------------------------
# get_dimension_scores
# ---------------------------------------------------------------------------


class TestGetDimensionScores:
    def test_returns_parsed_dimension_json(self):
        from src.db.accounts import get_dimension_scores

        dims = {"trigger_intent": 85, "tech_fit": 70, "firmographic": 60}
        conn = _mock_conn_with_rows({"dimension_scores_json": [{"dimension_scores_json": json.dumps(dims)}]})
        result = get_dimension_scores(conn, "acc_test123")
        assert result == dims

    def test_returns_empty_dict_when_no_rows(self):
        from src.db.accounts import get_dimension_scores

        conn = _mock_conn_with_rows()
        result = get_dimension_scores(conn, "acc_missing")
        assert result == {}

    def test_returns_empty_dict_for_invalid_json(self):
        from src.db.accounts import get_dimension_scores

        conn = _mock_conn_with_rows({"dimension_scores_json": [{"dimension_scores_json": "not-valid-json"}]})
        result = get_dimension_scores(conn, "acc_badjson")
        assert result == {}

    def test_returns_empty_dict_for_non_dict_json(self):
        from src.db.accounts import get_dimension_scores

        conn = _mock_conn_with_rows({"dimension_scores_json": [{"dimension_scores_json": "[1,2,3]"}]})
        result = get_dimension_scores(conn, "acc_list")
        assert result == {}

    def test_handles_empty_string(self):
        from src.db.accounts import get_dimension_scores

        conn = _mock_conn_with_rows({"dimension_scores_json": [{"dimension_scores_json": ""}]})
        result = get_dimension_scores(conn, "acc_empty")
        assert result == {}


# ---------------------------------------------------------------------------
# get_account_velocity
# ---------------------------------------------------------------------------


class TestGetAccountVelocity:
    def test_returns_stable_when_no_rows(self):
        from src.db.accounts import get_account_velocity

        conn = _mock_conn_with_rows()
        result = get_account_velocity(conn, "acc_none")
        assert result["category"] == "stable"
        assert result["7d"] == 0.0

    def test_accelerating_when_positive_delta(self):
        from src.db.accounts import get_account_velocity

        rows = [
            {"score": 30.0, "run_date": "2025-01-15"},
            {"score": 20.0, "run_date": "2025-01-08"},
            {"score": 15.0, "run_date": "2025-01-01"},
        ]
        conn = _mock_conn_with_rows(default_rows=rows)
        result = get_account_velocity(conn, "acc_up")
        assert result["7d"] == 10.0
        assert result["category"] == "accelerating"

    def test_decelerating_when_negative_delta(self):
        from src.db.accounts import get_account_velocity

        rows = [
            {"score": 10.0, "run_date": "2025-01-15"},
            {"score": 20.0, "run_date": "2025-01-08"},
            {"score": 25.0, "run_date": "2025-01-01"},
        ]
        conn = _mock_conn_with_rows(default_rows=rows)
        result = get_account_velocity(conn, "acc_down")
        assert result["7d"] == -10.0
        assert result["category"] == "decelerating"

    def test_stable_when_small_delta(self):
        from src.db.accounts import get_account_velocity

        rows = [
            {"score": 21.0, "run_date": "2025-01-15"},
            {"score": 20.0, "run_date": "2025-01-08"},
        ]
        conn = _mock_conn_with_rows(default_rows=rows)
        result = get_account_velocity(conn, "acc_flat")
        assert result["category"] == "stable"

    def test_handles_bad_date_format(self):
        from src.db.accounts import get_account_velocity

        rows = [{"score": 10.0, "run_date": "not-a-date"}]
        conn = _mock_conn_with_rows(default_rows=rows)
        result = get_account_velocity(conn, "acc_baddate")
        assert result["category"] == "stable"


# ---------------------------------------------------------------------------
# get_signal_timeline
# ---------------------------------------------------------------------------


class TestGetSignalTimeline:
    def test_returns_items_and_total(self):
        from src.db.accounts import get_signal_timeline

        signal_rows = [
            {
                "signal_code": "hiring_devops",
                "source": "jobs",
                "evidence_url": "https://example.com",
                "evidence_text": "DevOps engineer posting",
                "observed_at": "2025-01-15",
                "confidence": 0.85,
                "source_reliability": 0.9,
                "product": "zopdev",
            }
        ]

        conn = MagicMock()
        call_count = [0]

        def _execute(sql, params=None):
            cursor = MagicMock()
            call_count[0] += 1
            if "count(*)" in sql.lower():
                cursor.fetchone.return_value = {"total": 1}
            else:
                cursor.fetchall.return_value = signal_rows
            return cursor

        conn.execute = _execute

        items, total = get_signal_timeline(conn, "acc_test")
        assert total == 1
        assert len(items) == 1
        assert items[0]["signal_code"] == "hiring_devops"

    def test_applies_signal_code_filter(self):
        from src.db.accounts import get_signal_timeline

        conn = MagicMock()
        executed_sqls = []

        def _execute(sql, params=None):
            executed_sqls.append((sql, params))
            cursor = MagicMock()
            cursor.fetchone.return_value = {"total": 0}
            cursor.fetchall.return_value = []
            return cursor

        conn.execute = _execute

        get_signal_timeline(conn, "acc_test", signal_code="hiring_devops")

        # The filter query should include signal_code param
        all_params = []
        for _, params in executed_sqls:
            if params:
                all_params.extend(params)
        assert "hiring_devops" in all_params

    def test_applies_source_filter(self):
        from src.db.accounts import get_signal_timeline

        conn = MagicMock()
        executed_sqls = []

        def _execute(sql, params=None):
            executed_sqls.append((sql, params))
            cursor = MagicMock()
            cursor.fetchone.return_value = {"total": 0}
            cursor.fetchall.return_value = []
            return cursor

        conn.execute = _execute

        get_signal_timeline(conn, "acc_test", source="news")

        all_params = []
        for _, params in executed_sqls:
            if params:
                all_params.extend(params)
        assert "news" in all_params

    def test_empty_result(self):
        from src.db.accounts import get_signal_timeline

        conn = MagicMock()

        def _execute(sql, params=None):
            cursor = MagicMock()
            cursor.fetchone.return_value = {"total": 0}
            cursor.fetchall.return_value = []
            return cursor

        conn.execute = _execute

        items, total = get_signal_timeline(conn, "acc_empty")
        assert total == 0
        assert items == []


# ---------------------------------------------------------------------------
# Timeline endpoint
# ---------------------------------------------------------------------------


class TestTimelineEndpoint:
    def test_timeline_endpoint_exists(self):
        """Verify the timeline route is registered on the accounts router."""
        from src.web.routes.accounts import router

        paths = [r.path for r in router.routes]
        assert "/accounts/{account_id}/timeline" in paths

    def test_account_detail_includes_dimensions(self):
        """Verify get_account_detail returns dimension_scores and velocity keys."""
        from src.db.accounts import get_account_detail

        conn = MagicMock()
        call_count = [0]

        def _execute(sql, params=None):
            cursor = MagicMock()
            call_count[0] += 1
            sql_lower = sql.lower().strip()

            if sql_lower.startswith("select * from accounts"):
                cursor.fetchone.return_value = {
                    "account_id": "acc_test",
                    "company_name": "Test Co",
                    "domain": "test.com",
                    "source_type": "seed",
                    "created_at": "2025-01-01",
                }
            elif "dimension_scores_json" in sql_lower and "account_scores" in sql_lower and "score_runs" in sql_lower:
                cursor.fetchone.return_value = {"dimension_scores_json": '{"trigger_intent": 80}'}
            elif "account_scores" in sql_lower and "dimension_scores_json" in sql_lower:
                cursor.fetchall.return_value = [
                    {
                        "product": "zopdev",
                        "score": 25.0,
                        "tier": "high",
                        "dimension_scores_json": '{"trigger_intent": 80}',
                    }
                ]
            elif "signal_observations" in sql_lower:
                cursor.fetchall.return_value = []
            elif "company_research" in sql_lower:
                cursor.fetchone.return_value = None
            elif "contact_research" in sql_lower:
                cursor.fetchall.return_value = []
            elif "account_labels" in sql_lower:
                cursor.fetchall.return_value = []
            elif "score_runs" in sql_lower:
                cursor.fetchall.return_value = []
                cursor.fetchone.return_value = None
            else:
                cursor.fetchone.return_value = None
                cursor.fetchall.return_value = []
            return cursor

        conn.execute = _execute

        result = get_account_detail(conn, "acc_test")
        assert result is not None
        assert "dimension_scores" in result
        assert "velocity" in result

    def test_account_detail_returns_none_for_missing(self):
        from src.db.accounts import get_account_detail

        conn = MagicMock()

        def _execute(sql, params=None):
            cursor = MagicMock()
            cursor.fetchone.return_value = None
            cursor.fetchall.return_value = []
            return cursor

        conn.execute = _execute

        result = get_account_detail(conn, "acc_nonexistent")
        assert result is None
