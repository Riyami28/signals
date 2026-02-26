"""Tests for Bombora Company Surge intent data integration."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from src.integrations.bombora import (
    RELEVANT_TOPICS,
    _build_observation,
    _find_topic_clusters,
    _is_relevant_topic,
    collect,
)  # noqa: I001

# ---------------------------------------------------------------------------
# Topic relevance helpers
# ---------------------------------------------------------------------------


def test_is_relevant_topic_exact_match():
    assert _is_relevant_topic("kubernetes") is True


def test_is_relevant_topic_case_insensitive():
    assert _is_relevant_topic("Kubernetes") is True
    assert _is_relevant_topic("FINOPS") is True


def test_is_relevant_topic_partial_match():
    assert _is_relevant_topic("cloud cost optimization strategies") is True


def test_is_relevant_topic_irrelevant():
    assert _is_relevant_topic("office furniture") is False
    assert _is_relevant_topic("marketing automation") is False


# ---------------------------------------------------------------------------
# Topic cluster detection
# ---------------------------------------------------------------------------


def test_find_topic_clusters_returns_cluster():
    topics = [
        {"topic": "kubernetes", "surge_score": 80},
        {"topic": "container orchestration", "surge_score": 75},
        {"topic": "docker containers", "surge_score": 60},
    ]
    clusters = _find_topic_clusters(topics, surge_threshold=50)
    assert "kubernetes" in clusters


def test_find_topic_clusters_no_cluster_below_threshold():
    topics = [
        {"topic": "kubernetes", "surge_score": 80},
        {"topic": "container orchestration", "surge_score": 30},
    ]
    clusters = _find_topic_clusters(topics, surge_threshold=50)
    assert "kubernetes" not in clusters


def test_find_topic_clusters_multiple_clusters():
    topics = [
        {"topic": "kubernetes", "surge_score": 80},
        {"topic": "container orchestration", "surge_score": 75},
        {"topic": "cloud cost optimization", "surge_score": 70},
        {"topic": "finops", "surge_score": 65},
    ]
    clusters = _find_topic_clusters(topics, surge_threshold=50)
    assert "kubernetes" in clusters
    assert "cloud_cost" in clusters


def test_find_topic_clusters_empty_input():
    clusters = _find_topic_clusters([], surge_threshold=50)
    assert clusters == []


# ---------------------------------------------------------------------------
# Observation building
# ---------------------------------------------------------------------------


def test_build_observation_returns_valid_signal():
    obs = _build_observation(
        account_id="acc_123",
        signal_code="bombora_surge_high",
        confidence=0.85,
        source_reliability=0.82,
        evidence_text="Bombora surge: kubernetes (score=90)",
        payload={"domain": "example.com", "topic": "kubernetes", "surge_score": 90},
    )
    assert obs.account_id == "acc_123"
    assert obs.signal_code == "bombora_surge_high"
    assert obs.product == "shared"
    assert obs.source == "bombora_api"
    assert obs.confidence == 0.85
    assert obs.source_reliability == 0.82
    assert "kubernetes" in obs.evidence_text


def test_build_observation_clamps_confidence():
    obs = _build_observation(
        account_id="acc_1",
        signal_code="bombora_surge_high",
        confidence=1.5,
        source_reliability=-0.1,
        evidence_text="test",
        payload={"x": 1},
    )
    assert obs.confidence == 1.0
    assert obs.source_reliability == 0.0


def test_build_observation_truncates_evidence():
    long_text = "x" * 1000
    obs = _build_observation(
        account_id="acc_1",
        signal_code="bombora_surge_high",
        confidence=0.8,
        source_reliability=0.8,
        evidence_text=long_text,
        payload={"x": 1},
    )
    assert len(obs.evidence_text) <= 500


# ---------------------------------------------------------------------------
# Full collector (mocked API + DB)
# ---------------------------------------------------------------------------


def _mock_settings(**overrides):
    defaults = {
        "bombora_api_key": "test-key-123",
        "bombora_api_base_url": "https://api.bombora.com/v1",
        "bombora_surge_threshold_high": 70,
        "bombora_surge_threshold_moderate": 50,
        "bombora_topic_cluster_min": 3,
    }
    defaults.update(overrides)
    s = MagicMock()
    for k, v in defaults.items():
        setattr(s, k, v)
    return s


def _mock_conn(accounts=None):
    """Build a mock connection with accounts query + observation insert."""
    if accounts is None:
        accounts = [
            {"account_id": "acc_1", "domain": "example.com"},
            {"account_id": "acc_2", "domain": "acme.io"},
        ]
    conn = MagicMock()
    cursor = MagicMock()
    cursor.fetchall.return_value = accounts
    conn.execute.return_value = cursor
    return conn


_SAMPLE_SURGE_RESPONSE = [
    {"topic": "kubernetes", "surge_score": 90, "topic_id": "t1"},
    {"topic": "container orchestration", "surge_score": 82, "topic_id": "t2"},
    {"topic": "devops transformation", "surge_score": 65, "topic_id": "t3"},
    {"topic": "cloud cost optimization", "surge_score": 55, "topic_id": "t4"},
    {"topic": "finops", "surge_score": 72, "topic_id": "t5"},
    {"topic": "office furniture", "surge_score": 95, "topic_id": "t6"},
]


@patch("src.integrations.bombora._fetch_surge_data")
@patch("src.integrations.bombora.db")
def test_collect_creates_high_surge_signals(mock_db, mock_fetch):
    mock_fetch.return_value = _SAMPLE_SURGE_RESPONSE
    mock_db.was_crawled_today.return_value = False
    mock_db.insert_signal_observation.return_value = True
    mock_db.mark_crawled.return_value = None

    conn = _mock_conn([{"account_id": "acc_1", "domain": "example.com"}])
    settings = _mock_settings()
    result = collect(conn, settings, {}, {"bombora_api": 0.82})

    assert result["inserted"] > 0
    assert result["seen"] > 0

    # Verify observation inserts were called.
    calls = mock_db.insert_signal_observation.call_args_list
    signal_codes = [c[0][1].signal_code for c in calls]
    assert "bombora_surge_high" in signal_codes


@patch("src.integrations.bombora._fetch_surge_data")
@patch("src.integrations.bombora.db")
def test_collect_creates_moderate_surge_signals(mock_db, mock_fetch):
    mock_fetch.return_value = [
        {"topic": "devops transformation", "surge_score": 60, "topic_id": "t1"},
    ]
    mock_db.was_crawled_today.return_value = False
    mock_db.insert_signal_observation.return_value = True
    mock_db.mark_crawled.return_value = None

    conn = _mock_conn([{"account_id": "acc_1", "domain": "example.com"}])
    settings = _mock_settings()
    collect(conn, settings, {}, {"bombora_api": 0.82})

    calls = mock_db.insert_signal_observation.call_args_list
    signal_codes = [c[0][1].signal_code for c in calls]
    assert "bombora_surge_moderate" in signal_codes


@patch("src.integrations.bombora._fetch_surge_data")
@patch("src.integrations.bombora.db")
def test_collect_creates_topic_cluster_signal(mock_db, mock_fetch):
    mock_fetch.return_value = [
        {"topic": "kubernetes", "surge_score": 80},
        {"topic": "container orchestration", "surge_score": 75},
        {"topic": "docker containers", "surge_score": 60},
        {"topic": "devops transformation", "surge_score": 65},
        {"topic": "cloud cost optimization", "surge_score": 55},
    ]
    mock_db.was_crawled_today.return_value = False
    mock_db.insert_signal_observation.return_value = True
    mock_db.mark_crawled.return_value = None

    conn = _mock_conn([{"account_id": "acc_1", "domain": "example.com"}])
    settings = _mock_settings(bombora_topic_cluster_min=3)
    collect(conn, settings, {}, {"bombora_api": 0.82})

    calls = mock_db.insert_signal_observation.call_args_list
    signal_codes = [c[0][1].signal_code for c in calls]
    assert "bombora_topic_cluster" in signal_codes


@patch("src.integrations.bombora._fetch_surge_data")
@patch("src.integrations.bombora.db")
def test_collect_skips_already_crawled(mock_db, mock_fetch):
    mock_db.was_crawled_today.return_value = True

    conn = _mock_conn([{"account_id": "acc_1", "domain": "example.com"}])
    settings = _mock_settings()
    result = collect(conn, settings, {}, {"bombora_api": 0.82})

    mock_fetch.assert_not_called()
    assert result["inserted"] == 0


def test_collect_skips_when_no_api_key():
    conn = _mock_conn()
    settings = _mock_settings(bombora_api_key="")
    result = collect(conn, settings, {}, {})

    assert result == {"inserted": 0, "seen": 0}


@patch("src.integrations.bombora._fetch_surge_data")
@patch("src.integrations.bombora.db")
def test_collect_handles_api_errors_gracefully(mock_db, mock_fetch):
    import requests

    mock_fetch.side_effect = requests.RequestException("timeout")
    mock_db.was_crawled_today.return_value = False
    mock_db.record_crawl_attempt.return_value = None

    conn = _mock_conn([{"account_id": "acc_1", "domain": "example.com"}])
    settings = _mock_settings()
    result = collect(conn, settings, {}, {"bombora_api": 0.82})

    assert result["inserted"] == 0
    mock_db.record_crawl_attempt.assert_called_once()


@patch("src.integrations.bombora._fetch_surge_data")
@patch("src.integrations.bombora.db")
def test_collect_ignores_irrelevant_topics(mock_db, mock_fetch):
    mock_fetch.return_value = [
        {"topic": "office furniture", "surge_score": 95},
        {"topic": "marketing automation", "surge_score": 88},
    ]
    mock_db.was_crawled_today.return_value = False
    mock_db.mark_crawled.return_value = None

    conn = _mock_conn([{"account_id": "acc_1", "domain": "example.com"}])
    settings = _mock_settings()
    result = collect(conn, settings, {}, {"bombora_api": 0.82})

    assert result["inserted"] == 0
    mock_db.insert_signal_observation.assert_not_called()


@patch("src.integrations.bombora._fetch_surge_data")
@patch("src.integrations.bombora.db")
def test_collect_skips_empty_domains(mock_db, mock_fetch):
    mock_db.was_crawled_today.return_value = False

    conn = _mock_conn([{"account_id": "acc_1", "domain": ""}])
    settings = _mock_settings()
    collect(conn, settings, {}, {"bombora_api": 0.82})

    mock_fetch.assert_not_called()


def test_relevant_topics_dict_has_expected_categories():
    expected = {"cloud_cost", "kubernetes", "devops", "platform_eng", "infra_automation", "cloud_migration"}
    assert set(RELEVANT_TOPICS.keys()) == expected
