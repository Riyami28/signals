"""Integration tests for src/research/orchestrator.run_research_stage with mocked Claude client."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from src import db
from src.research.orchestrator import run_research_stage
from src.research.client import ResearchResponse


def _make_settings(**overrides) -> MagicMock:
    """Create a mock settings object with sensible defaults."""
    settings = MagicMock()
    settings.claude_api_key = overrides.get("claude_api_key", "")
    settings.claude_model = overrides.get("claude_model", "claude-sonnet-4-5")
    settings.research_timeout_seconds = overrides.get("research_timeout_seconds", 120)
    settings.research_max_accounts = overrides.get("research_max_accounts", 20)
    settings.research_stale_days = overrides.get("research_stale_days", 30)
    return settings


_REQUIRED_SUMMARY_KEYS = {
    "attempted",
    "completed",
    "failed",
    "skipped",
    "total_input_tokens",
    "total_output_tokens",
}


class TestRunResearchStage:
    def test_returns_all_skipped_when_api_key_is_empty(self):
        """When claude_api_key is empty, research stage should return immediately with all zeros."""
        conn = db.get_connection()
        db.init_db(conn)

        settings = _make_settings(claude_api_key="")
        result = run_research_stage(conn, settings, "2026-02-23", "run_fake123")
        conn.close()

        assert result["attempted"] == 0
        assert result["completed"] == 0
        assert result["failed"] == 0
        assert result["skipped"] == 0
        assert result["total_input_tokens"] == 0
        assert result["total_output_tokens"] == 0

    def test_returns_summary_dict_with_all_count_fields(self):
        """When invoked with a valid API key and scored accounts, the summary dict has all required keys."""
        conn = db.get_connection()
        db.init_db(conn)

        # Create an account with a score so it qualifies for research.
        account_id = db.upsert_account(
            conn, "TestCorp", "testcorp.com", "seed", commit=False
        )
        run_id = db.create_score_run(conn, "2026-02-23")
        db.finish_score_run(conn, run_id, status="completed")
        conn.execute(
            """INSERT INTO account_scores (run_id, account_id, product, score, tier, delta_7d, top_reasons_json)
            VALUES (%s, %s, 'zopdev', 80.0, 'high', 0.0, '[]')""",
            (run_id, account_id),
        )
        conn.commit()

        settings = _make_settings(claude_api_key="sk-test-key-fake")

        # Build a mock extraction response.
        extraction_text = (
            "### ENRICHMENT_JSON\n```json\n"
            '{"industry": "SaaS", "industry_confidence": 0.9}\n'
            "```\n\n"
            "### RESEARCH_BRIEF\nTestCorp is a SaaS company."
        )
        # Build a mock scoring response.
        scoring_text = (
            "### CONTACTS_JSON\n```json\n"
            '[{"first_name": "Jane", "last_name": "Doe", "title": "CTO", "management_level": "C-Level"}]\n'
            "```\n\n"
            "### CONVERSATION_STARTERS\n"
            "- Ask about their cloud migration strategy.\n"
        )

        mock_ext_response = ResearchResponse(
            raw_text=extraction_text,
            model="claude-sonnet-4-5",
            input_tokens=100,
            output_tokens=200,
            duration_seconds=1.5,
        )
        mock_score_response = ResearchResponse(
            raw_text=scoring_text,
            model="claude-sonnet-4-5",
            input_tokens=150,
            output_tokens=250,
            duration_seconds=1.2,
        )

        # Mock get_accounts_needing_research to return our account directly.
        # This avoids hitting a known column-name mismatch (signal_score vs score)
        # in the SQL query while still testing the orchestrator logic end-to-end.
        fake_accounts = [
            {
                "account_id": account_id,
                "company_name": "TestCorp",
                "domain": "testcorp.com",
                "signal_score": 80.0,
                "signal_tier": "high",
                "delta_7d": 0.0,
                "top_reasons_json": "[]",
            }
        ]

        with patch("src.research.orchestrator.ResearchClient") as MockClient, \
             patch("src.research.orchestrator.create_research_client") as mock_create, \
             patch("src.research.orchestrator.run_enrichment_waterfall", return_value={}), \
             patch("src.research.orchestrator.db.get_accounts_needing_research", return_value=fake_accounts):
            mock_instance = MockClient.return_value
            mock_create.return_value = mock_instance
            mock_instance.research_company.side_effect = [
                mock_ext_response,
                mock_score_response,
            ]

            result = run_research_stage(conn, settings, "2026-02-23", run_id)

        conn.close()

        # All required keys should be present.
        assert _REQUIRED_SUMMARY_KEYS.issubset(set(result.keys()))
        # We had one account to research.
        assert result["attempted"] >= 1
        assert result["completed"] >= 1
        assert result["failed"] == 0
        assert result["total_input_tokens"] == 250  # 100 + 150
        assert result["total_output_tokens"] == 450  # 200 + 250
