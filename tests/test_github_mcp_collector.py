"""Tests for src/collectors/github_mcp_collector.py and src/mcp_sources/github_mcp.py.

Covers:
- _build_observation: field clamping, determinism, dedup key
- _kw_classify: keyword signal routing and confidence
- _kw_classify_migration: migration PR detection
- _match_to_account: email domain and company name matching
- _parse_iso_ts: ISO timestamp parsing
- collect(): DB interaction, crawl-today guard, no-accounts short-circuit,
             missing token behaviour, Claude fallback path
- github_mcp module: GitHubSignal dataclass, _is_infra_pain, _is_evaluation,
                     _is_migration_pr
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Disable the global postgres autouse fixture — all tests here are unit tests.
# ---------------------------------------------------------------------------
@pytest.fixture(autouse=True)
def postgres_test_isolation(monkeypatch: pytest.MonkeyPatch):
    import os

    test_dsn = os.getenv(
        "SIGNALS_TEST_PG_DSN",
        "postgresql://signals:signals_dev_password@127.0.0.1:55432/signals_test",
    )
    monkeypatch.setenv("SIGNALS_PG_DSN", test_dsn)
    yield


# ---------------------------------------------------------------------------
# Imports
# ---------------------------------------------------------------------------

from src.collectors.github_mcp_collector import (
    SIGNAL_EVALUATION,
    SIGNAL_INFRA_ISSUE,
    SIGNAL_MIGRATION_PR,
    SIGNAL_STARGAZER_VELOCITY,
    SOURCE_NAME,
    SOURCE_RELIABILITY,
    _build_observation,
    _kw_classify,
    _kw_classify_migration,
    _match_to_account,
    _parse_iso_ts,
)
from src.mcp_sources.github_mcp import GitHubSignal, _is_evaluation, _is_infra_pain, _is_migration_pr

# ===========================================================================
# _build_observation
# ===========================================================================


class TestBuildObservation:
    def _obs(self, **kwargs):
        defaults = dict(
            account_id="acc_abc",
            signal_code=SIGNAL_INFRA_ISSUE,
            source=SOURCE_NAME,
            observed_at="2026-03-06T10:00:00Z",
            confidence=0.7,
            source_reliability=0.75,
            evidence_url="https://github.com/org/repo/issues/1",
            evidence_text="Some infra pain",
            payload={"repo": "org/repo"},
        )
        defaults.update(kwargs)
        return _build_observation(**defaults)

    def test_product_is_shared(self):
        assert self._obs().product == "shared"

    def test_source_is_github_mcp(self):
        assert self._obs().source == SOURCE_NAME

    def test_confidence_clamped_max(self):
        assert self._obs(confidence=5.0).confidence == 1.0

    def test_confidence_clamped_min(self):
        assert self._obs(confidence=-1.0).confidence == 0.0

    def test_source_reliability_clamped_max(self):
        assert self._obs(source_reliability=99.0).source_reliability == 1.0

    def test_evidence_text_truncated_to_500(self):
        obs = self._obs(evidence_text="x" * 1000)
        assert len(obs.evidence_text) == 500

    def test_obs_id_deterministic(self):
        assert self._obs().obs_id == self._obs().obs_id

    def test_obs_id_differs_on_different_url(self):
        obs1 = self._obs(evidence_url="https://github.com/org/repo/issues/1")
        obs2 = self._obs(evidence_url="https://github.com/org/repo/issues/99")
        assert obs1.obs_id != obs2.obs_id

    def test_obs_id_differs_on_different_signal(self):
        obs1 = self._obs(signal_code=SIGNAL_INFRA_ISSUE)
        obs2 = self._obs(signal_code=SIGNAL_MIGRATION_PR)
        assert obs1.obs_id != obs2.obs_id


# ===========================================================================
# _kw_classify
# ===========================================================================


class TestKwClassify:
    def test_evaluation_detected_above_infra_pain(self):
        title = "Evaluating kubernetes vs nomad for our platform"
        body = "We are comparing kubernetes, terraform, and infrastructure tooling."
        code, conf = _kw_classify(title, body)
        assert code == SIGNAL_EVALUATION
        assert conf >= 0.5

    def test_infra_pain_with_many_keywords(self):
        title = "Kubernetes outage — toil killing us"
        body = "Our kubernetes pipeline and terraform infrastructure keep failing. SRE team at limit."
        code, conf = _kw_classify(title, body)
        assert code in (SIGNAL_EVALUATION, SIGNAL_INFRA_ISSUE)
        assert conf >= 0.5

    def test_single_infra_keyword_low_confidence(self):
        title = "Kubernetes slow"
        body = ""
        code, conf = _kw_classify(title, body)
        # Should get infra_issue but at low confidence (< threshold)
        assert code == SIGNAL_INFRA_ISSUE
        assert conf < 0.65  # below claude threshold

    def test_no_keywords_returns_none(self):
        code, conf = _kw_classify("Birthday party planning", "cake and balloons")
        assert code is None
        assert conf == 0.0

    def test_eval_keyword_alone_without_infra_no_match(self):
        # Evaluation keyword alone, no infra context
        code, conf = _kw_classify("comparing apples to oranges", "fruit is good")
        assert code is None

    def test_high_infra_density_high_confidence(self):
        title = "kubernetes terraform helm ci/cd pipeline outage incident"
        body = "devops platform engineering observability sre toil container microservice"
        _, conf = _kw_classify(title, body)
        assert conf >= 0.65


# ===========================================================================
# _kw_classify_migration
# ===========================================================================


class TestKwClassifyMigration:
    def test_both_techs_with_migrate_verb_high_confidence(self):
        title = "Migrate from Terraform to OpenTofu"
        body = "We're moving away from terraform and adopting opentofu."
        is_mig, conf = _kw_classify_migration(title, body, "terraform", "opentofu")
        assert is_mig is True
        assert conf >= 0.8

    def test_remove_old_tech_without_new_tech(self):
        is_mig, conf = _kw_classify_migration(
            "Remove terraform configuration", "We are dropping terraform", "terraform", "opentofu"
        )
        assert is_mig is True
        assert conf < 0.8  # partial match

    def test_only_new_tech_mentioned(self):
        is_mig, conf = _kw_classify_migration("Add opentofu support", "We want opentofu", "terraform", "opentofu")
        assert is_mig is True

    def test_neither_tech_no_migration(self):
        is_mig, _ = _kw_classify_migration(
            "Fix CI pipeline", "Update github actions workflows", "terraform", "opentofu"
        )
        assert is_mig is False

    def test_case_insensitive(self):
        is_mig, conf = _kw_classify_migration("MIGRATE FROM TERRAFORM TO OPENTOFU", "", "terraform", "opentofu")
        assert is_mig is True
        assert conf >= 0.8

    def test_jenkins_to_github_actions(self):
        title = "Replace Jenkins with GitHub Actions"
        body = "migrate from jenkins, use github actions instead"
        is_mig, conf = _kw_classify_migration(title, body, "jenkins", "github-actions")
        assert is_mig is True


# ===========================================================================
# _match_to_account
# ===========================================================================


class TestMatchToAccount:
    def setup_method(self):
        self.domain_map = {
            "acme.com": "acc_001",
            "bigcorp.io": "acc_002",
        }
        self.name_map = {
            "acme inc": "acc_001",
            "bigcorp": "acc_002",
            "startup co": "acc_003",
        }

    def test_exact_email_domain_match(self):
        result = _match_to_account("", "acme.com", self.domain_map, self.name_map)
        assert result == "acc_001"

    def test_exact_company_name_match(self):
        result = _match_to_account("bigcorp", "", self.domain_map, self.name_map)
        assert result == "acc_002"

    def test_email_domain_takes_priority_over_name(self):
        result = _match_to_account("bigcorp", "acme.com", self.domain_map, self.name_map)
        assert result == "acc_001"

    def test_partial_company_name_match(self):
        # "startup" is in "startup co"
        result = _match_to_account("startup co ltd", "", self.domain_map, self.name_map)
        assert result == "acc_003"

    def test_no_match_returns_none(self):
        result = _match_to_account("unknown corp", "unknowncorp.xyz", self.domain_map, self.name_map)
        assert result is None

    def test_empty_inputs_returns_none(self):
        result = _match_to_account("", "", self.domain_map, self.name_map)
        assert result is None

    def test_generic_email_domain_not_matched_as_company(self):
        # gmail.com is not in domain_map → no match
        result = _match_to_account("", "gmail.com", self.domain_map, self.name_map)
        assert result is None


# ===========================================================================
# _parse_iso_ts
# ===========================================================================


class TestParseIsoTs:
    def test_standard_z_suffix(self):
        ts = _parse_iso_ts("2026-03-06T10:30:00Z")
        assert ts > 0
        # Roughly 2026 — anything > 2025-01-01 epoch
        assert ts > 1735689600

    def test_datetime_without_z(self):
        ts = _parse_iso_ts("2026-03-06T10:30:00")
        assert ts > 0

    def test_date_only(self):
        ts = _parse_iso_ts("2026-03-06")
        assert ts > 0

    def test_invalid_returns_zero(self):
        ts = _parse_iso_ts("not-a-date")
        assert ts == 0.0

    def test_empty_returns_zero(self):
        ts = _parse_iso_ts("")
        assert ts == 0.0


# ===========================================================================
# github_mcp module: keyword classifiers
# ===========================================================================


class TestGithubMcpKeywords:
    def test_is_infra_pain_high_density(self):
        is_pain, conf = _is_infra_pain(
            "Kubernetes outage incident toil", "terraform helm infrastructure devops ci/cd pipeline"
        )
        assert is_pain is True
        assert conf >= 0.6

    def test_is_infra_pain_single_keyword(self):
        is_pain, conf = _is_infra_pain("kubernetes slow", "")
        assert is_pain is True
        assert conf < 0.65  # low confidence

    def test_is_infra_pain_no_keywords(self):
        is_pain, _ = _is_infra_pain("birthday cake", "party fun")
        assert is_pain is False

    def test_is_evaluation_with_infra_context(self):
        is_eval, conf = _is_evaluation(
            "Evaluating kubernetes vs nomad",
            "comparing terraform and pulumi for infrastructure",
        )
        assert is_eval is True
        assert conf >= 0.55

    def test_is_evaluation_without_infra_no_match(self):
        is_eval, _ = _is_evaluation("comparing apples vs oranges", "fruit is good")
        assert is_eval is False

    def test_is_migration_pr_both_techs_and_verb(self):
        is_mig, conf = _is_migration_pr(
            "Migrate terraform configs to opentofu",
            "Remove terraform, replace with opentofu",
            "terraform",
            "opentofu",
        )
        assert is_mig is True
        assert conf >= 0.8

    def test_is_migration_pr_partial(self):
        is_mig, conf = _is_migration_pr("Add opentofu support", "Initial opentofu config", "terraform", "opentofu")
        assert is_mig is True
        assert conf < 0.85  # only partial match

    def test_is_migration_pr_no_match(self):
        is_mig, _ = _is_migration_pr("Fix CI bug", "Update linter", "terraform", "opentofu")
        assert is_mig is False


# ===========================================================================
# GitHubSignal dataclass
# ===========================================================================


class TestGitHubSignal:
    def test_defaults(self):
        sig = GitHubSignal(
            signal_type="github_migration_pr",
            repo="org/repo",
            url="https://github.com/org/repo/pull/1",
            title="Migrate terraform to opentofu",
            body_excerpt="We are replacing terraform with opentofu.",
            author_login="jdoe",
            author_company="Acme Inc",
            author_email_domain="acme.com",
            created_at="2026-03-06T10:00:00Z",
            confidence=0.85,
            evidence_text="Migration PR: terraform→opentofu",
        )
        assert sig.tech_from == ""
        assert sig.tech_to == ""
        assert sig.item_type == ""
        assert sig.extra == {}

    def test_with_tech_pair(self):
        sig = GitHubSignal(
            signal_type="github_migration_pr",
            repo="org/repo",
            url="",
            title="",
            body_excerpt="",
            author_login="",
            author_company="",
            author_email_domain="",
            created_at="",
            confidence=0.8,
            evidence_text="",
            tech_from="terraform",
            tech_to="opentofu",
        )
        assert sig.tech_from == "terraform"
        assert sig.tech_to == "opentofu"


# ===========================================================================
# collect() — unit tests with mocked DB and HTTP
# ===========================================================================


def _make_settings(**kwargs):
    from src.settings import Settings

    defaults = dict(
        github_token="",
        claude_api_key="",
        github_signal_repos=(),
        github_signal_lookback_days=7,
        github_stargazer_velocity_days=7,
    )
    defaults.update(kwargs)
    return Settings(**defaults)


class TestCollectFunction:
    @pytest.mark.asyncio
    async def test_no_accounts_returns_zero(self):
        from src.collectors.github_mcp_collector import collect

        conn = MagicMock()
        conn.execute.return_value.fetchall.return_value = []
        conn.commit = MagicMock()
        settings = _make_settings()

        result = await collect(conn=conn, settings=settings)
        assert result["inserted"] == 0
        assert result["seen"] == 0
        assert result["accounts_processed"] == 0

    @pytest.mark.asyncio
    async def test_source_reliability_zero_skips(self):
        from src.collectors.github_mcp_collector import collect

        conn = MagicMock()
        conn.commit = MagicMock()
        settings = _make_settings()

        result = await collect(
            conn=conn,
            settings=settings,
            source_reliability_dict={SOURCE_NAME: 0},
        )
        assert result == {"inserted": 0, "seen": 0, "accounts_processed": 0}

    @pytest.mark.asyncio
    async def test_crawled_today_skips_repo(self):
        """If crawl checkpoint exists for a repo, no HTTP requests made."""
        from src.collectors.github_mcp_collector import collect

        conn = MagicMock()
        conn.execute.return_value.fetchall.return_value = [
            {"account_id": "acc_1", "company_name": "Acme", "domain": "acme.com"}
        ]
        conn.commit = MagicMock()
        settings = _make_settings(
            github_signal_repos=("opentofu/opentofu",),
        )

        with (
            patch("src.collectors.github_mcp_collector.db.was_crawled_today", return_value=True),
            patch("src.collectors.github_mcp_collector._search_issues", new_callable=AsyncMock) as mock_search,
        ):
            await collect(conn=conn, settings=settings)
            mock_search.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_matching_authors_produces_no_observations(self):
        """Issues found but no author matches known accounts → zero insertions."""
        from src.collectors.github_mcp_collector import collect

        conn = MagicMock()
        conn.execute.return_value.fetchall.return_value = [
            {"account_id": "acc_1", "company_name": "Acme", "domain": "acme.com"}
        ]
        conn.commit = MagicMock()
        settings = _make_settings(github_signal_repos=("opentofu/opentofu",))

        fake_issues = [
            {
                "title": "Kubernetes outage incident toil bottleneck",
                "body": "Our terraform infrastructure and devops pipeline is terrible",
                "html_url": "https://github.com/opentofu/opentofu/issues/1",
                "created_at": "2026-03-06T10:00:00Z",
                "user": {"login": "unknowndev"},
            }
        ]

        with (
            patch("src.collectors.github_mcp_collector.db.was_crawled_today", return_value=False),
            patch("src.collectors.github_mcp_collector.db.record_crawl_attempt"),
            patch("src.collectors.github_mcp_collector.db.mark_crawled"),
            patch(
                "src.collectors.github_mcp_collector._search_issues", new_callable=AsyncMock, return_value=fake_issues
            ),
            patch("src.collectors.github_mcp_collector._fetch_discussions", new_callable=AsyncMock, return_value=[]),
            # Author from unknown company → no match
            patch(
                "src.collectors.github_mcp_collector._get_user_profile",
                new_callable=AsyncMock,
                return_value={"company": "UnknownCorp", "email_domain": "unknown.io"},
            ),
            patch("src.collectors.github_mcp_collector.db.insert_signal_observation", return_value=True),
        ):
            result = await collect(conn=conn, settings=settings)

        assert result["inserted"] == 0

    @pytest.mark.asyncio
    async def test_matching_author_inserts_observation(self):
        """Issues with matching author email domain → observation inserted."""
        from src.collectors.github_mcp_collector import collect

        conn = MagicMock()
        conn.execute.return_value.fetchall.return_value = [
            {"account_id": "acc_1", "company_name": "Acme", "domain": "acme.com"}
        ]
        conn.commit = MagicMock()
        settings = _make_settings(github_signal_repos=("opentofu/opentofu",))

        fake_issues = [
            {
                "title": "Kubernetes outage incident toil platform engineering bottleneck",
                "body": "Our terraform infrastructure, devops pipeline, and ci/cd keep failing. SRE team.",
                "html_url": "https://github.com/opentofu/opentofu/issues/42",
                "created_at": "2026-03-06T10:00:00Z",
                "user": {"login": "jdoe"},
            }
        ]

        with (
            patch("src.collectors.github_mcp_collector.db.was_crawled_today", return_value=False),
            patch("src.collectors.github_mcp_collector.db.record_crawl_attempt"),
            patch("src.collectors.github_mcp_collector.db.mark_crawled"),
            patch(
                "src.collectors.github_mcp_collector._search_issues", new_callable=AsyncMock, return_value=fake_issues
            ),
            patch("src.collectors.github_mcp_collector._fetch_discussions", new_callable=AsyncMock, return_value=[]),
            patch(
                "src.collectors.github_mcp_collector._get_user_profile",
                new_callable=AsyncMock,
                return_value={"company": "Acme Inc", "email_domain": "acme.com"},
            ),
            patch("src.collectors.github_mcp_collector.db.insert_signal_observation", return_value=True) as mock_insert,
        ):
            result = await collect(conn=conn, settings=settings)

        assert result["inserted"] >= 1
        assert mock_insert.called

    @pytest.mark.asyncio
    async def test_migration_pr_inserted_for_matching_author(self):
        """Migration PR by an author from a known account → inserted as github_migration_pr."""
        from src.collectors.github_mcp_collector import collect

        conn = MagicMock()
        conn.execute.return_value.fetchall.return_value = [
            {"account_id": "acc_1", "company_name": "Acme", "domain": "acme.com"}
        ]
        conn.commit = MagicMock()
        # No signal repos so Part 1 is skipped; migration PRs in Part 2 will run
        settings = _make_settings(github_signal_repos=())

        fake_pr = {
            "title": "Migrate from terraform to opentofu",
            "body": "Remove terraform, replace with opentofu. Moving away from terraform.",
            "html_url": "https://github.com/opentofu/opentofu/pull/99",
            "created_at": "2026-03-05T08:00:00Z",
            "user": {"login": "jdoe"},
        }

        captured_obs = []

        def capture_insert(conn, obs, commit=False):
            captured_obs.append(obs)
            return True

        with (
            patch("src.collectors.github_mcp_collector.db.was_crawled_today", return_value=False),
            patch("src.collectors.github_mcp_collector.db.record_crawl_attempt"),
            patch("src.collectors.github_mcp_collector.db.mark_crawled"),
            patch("src.collectors.github_mcp_collector._search_issues", new_callable=AsyncMock, return_value=[fake_pr]),
            patch(
                "src.collectors.github_mcp_collector._get_user_profile",
                new_callable=AsyncMock,
                return_value={"company": "Acme", "email_domain": "acme.com"},
            ),
            patch("src.collectors.github_mcp_collector.db.insert_signal_observation", side_effect=capture_insert),
        ):
            await collect(conn=conn, settings=settings)

        migration_obs = [o for o in captured_obs if o.signal_code == SIGNAL_MIGRATION_PR]
        assert len(migration_obs) >= 1
        assert migration_obs[0].source == SOURCE_NAME

    @pytest.mark.asyncio
    async def test_claude_called_for_low_confidence_item(self):
        """Items with keyword confidence < threshold should trigger Claude classification."""
        from src.collectors.github_mcp_collector import collect

        conn = MagicMock()
        conn.execute.return_value.fetchall.return_value = [
            {"account_id": "acc_1", "company_name": "Acme", "domain": "acme.com"}
        ]
        conn.commit = MagicMock()
        settings = _make_settings(
            claude_api_key="sk-ant-test",
            github_signal_repos=("opentofu/opentofu",),
        )

        # Single keyword → confidence 0.50 → below 0.65 threshold → Claude called
        fake_issues = [
            {
                "title": "kubernetes slow",
                "body": "",
                "html_url": "https://github.com/opentofu/opentofu/issues/5",
                "created_at": "2026-03-06T09:00:00Z",
                "user": {"login": "jdoe"},
            }
        ]

        claude_response = {
            "relevant": True,
            "signal_type": "github_infra_issue",
            "confidence": 0.72,
            "tech_from": "",
            "tech_to": "",
            "evidence_sentence": "Acme is struggling with Kubernetes performance.",
        }

        with (
            patch("src.collectors.github_mcp_collector.db.was_crawled_today", return_value=False),
            patch("src.collectors.github_mcp_collector.db.record_crawl_attempt"),
            patch("src.collectors.github_mcp_collector.db.mark_crawled"),
            patch(
                "src.collectors.github_mcp_collector._search_issues", new_callable=AsyncMock, return_value=fake_issues
            ),
            patch("src.collectors.github_mcp_collector._fetch_discussions", new_callable=AsyncMock, return_value=[]),
            patch(
                "src.collectors.github_mcp_collector._get_user_profile",
                new_callable=AsyncMock,
                return_value={"company": "Acme Inc", "email_domain": "acme.com"},
            ),
            patch(
                "src.collectors.github_mcp_collector._claude_classify",
                new_callable=AsyncMock,
                return_value=claude_response,
            ) as mock_claude,
            patch("src.collectors.github_mcp_collector.db.insert_signal_observation", return_value=True),
        ):
            await collect(conn=conn, settings=settings)

        mock_claude.assert_called_once()

    @pytest.mark.asyncio
    async def test_no_claude_call_without_api_key(self):
        """Low-confidence items without Claude key should not call Claude and should be filtered."""
        from src.collectors.github_mcp_collector import collect

        conn = MagicMock()
        conn.execute.return_value.fetchall.return_value = [
            {"account_id": "acc_1", "company_name": "Acme", "domain": "acme.com"}
        ]
        conn.commit = MagicMock()
        settings = _make_settings(github_signal_repos=("opentofu/opentofu",))

        fake_issues = [
            {
                "title": "kubernetes slow",
                "body": "",
                "html_url": "https://github.com/opentofu/opentofu/issues/5",
                "created_at": "2026-03-06T09:00:00Z",
                "user": {"login": "jdoe"},
            }
        ]

        with (
            patch("src.collectors.github_mcp_collector.db.was_crawled_today", return_value=False),
            patch("src.collectors.github_mcp_collector.db.record_crawl_attempt"),
            patch("src.collectors.github_mcp_collector.db.mark_crawled"),
            patch(
                "src.collectors.github_mcp_collector._search_issues", new_callable=AsyncMock, return_value=fake_issues
            ),
            patch("src.collectors.github_mcp_collector._fetch_discussions", new_callable=AsyncMock, return_value=[]),
            patch(
                "src.collectors.github_mcp_collector._get_user_profile",
                new_callable=AsyncMock,
                return_value={"company": "Acme Inc", "email_domain": "acme.com"},
            ),
            patch(
                "src.collectors.github_mcp_collector._claude_classify",
                new_callable=AsyncMock,
            ) as mock_claude,
            patch("src.collectors.github_mcp_collector.db.insert_signal_observation", return_value=True),
        ):
            await collect(conn=conn, settings=settings)

        mock_claude.assert_not_called()

    @pytest.mark.asyncio
    async def test_discussion_classified_as_evaluation(self):
        """GraphQL discussions that match evaluation keywords produce github_evaluation signal."""
        from src.collectors.github_mcp_collector import collect

        conn = MagicMock()
        conn.execute.return_value.fetchall.return_value = [
            {"account_id": "acc_1", "company_name": "Acme", "domain": "acme.com"}
        ]
        conn.commit = MagicMock()
        settings = _make_settings(
            github_token="ghp_fake",
            github_signal_repos=("backstage/backstage",),
        )

        fake_discussion = {
            "title": "Evaluating kubernetes versus nomad for our platform infrastructure",
            "body": "comparing terraform and pulumi options. Looking for recommendation on infrastructure tooling.",
            "url": "https://github.com/backstage/backstage/discussions/1",
            "createdAt": "2026-03-05T12:00:00Z",
            "author": {"login": "jdoe"},
            "category": {"name": "General"},
        }

        captured_obs = []

        def capture_insert(conn, obs, commit=False):
            captured_obs.append(obs)
            return True

        with (
            patch("src.collectors.github_mcp_collector.db.was_crawled_today", return_value=False),
            patch("src.collectors.github_mcp_collector.db.record_crawl_attempt"),
            patch("src.collectors.github_mcp_collector.db.mark_crawled"),
            patch("src.collectors.github_mcp_collector._search_issues", new_callable=AsyncMock, return_value=[]),
            patch(
                "src.collectors.github_mcp_collector._fetch_discussions",
                new_callable=AsyncMock,
                return_value=[fake_discussion],
            ),
            patch(
                "src.collectors.github_mcp_collector._get_user_profile",
                new_callable=AsyncMock,
                return_value={"company": "Acme Inc", "email_domain": "acme.com"},
            ),
            patch("src.collectors.github_mcp_collector.db.insert_signal_observation", side_effect=capture_insert),
        ):
            await collect(conn=conn, settings=settings)

        eval_obs = [o for o in captured_obs if o.signal_code == SIGNAL_EVALUATION]
        assert len(eval_obs) >= 1

    @pytest.mark.asyncio
    async def test_commit_called_on_success(self):
        """conn.commit() is called after the main scan loop completes."""
        from src.collectors.github_mcp_collector import collect

        conn = MagicMock()
        conn.execute.return_value.fetchall.return_value = [
            {"account_id": "acc_1", "company_name": "Acme", "domain": "acme.com"}
        ]
        conn.commit = MagicMock()
        settings = _make_settings()

        with (
            patch("src.collectors.github_mcp_collector.db.was_crawled_today", return_value=True),
        ):
            await collect(conn=conn, settings=settings)

        conn.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_result_keys_present(self):
        from src.collectors.github_mcp_collector import collect

        conn = MagicMock()
        conn.execute.return_value.fetchall.return_value = []
        conn.commit = MagicMock()
        settings = _make_settings()

        result = await collect(conn=conn, settings=settings)
        assert "inserted" in result
        assert "seen" in result
        assert "accounts_processed" in result


# ===========================================================================
# Source metadata checks
# ===========================================================================


class TestSourceMetadata:
    def test_source_name(self):
        assert SOURCE_NAME == "github_mcp"

    def test_source_reliability(self):
        assert SOURCE_RELIABILITY == 0.75

    def test_signal_code_constants(self):
        assert SIGNAL_MIGRATION_PR == "github_migration_pr"
        assert SIGNAL_INFRA_ISSUE == "github_infra_issue"
        assert SIGNAL_EVALUATION == "github_evaluation"
        assert SIGNAL_STARGAZER_VELOCITY == "github_stargazer_velocity"
