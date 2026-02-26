"""Tests for G2 review intelligence and competitor signal integration."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from src.integrations.g2 import (
    COMPETITOR_PRODUCTS,
    _build_observation,
    _classify_intent_strength,
    _is_competitor_product,
    _is_dissatisfied_review,
    _map_competitor_to_category,
    collect,
)

# ---------------------------------------------------------------------------
# Competitor matching helpers
# ---------------------------------------------------------------------------


def test_is_competitor_product_exact_match():
    assert _is_competitor_product("jenkins") is True


def test_is_competitor_product_case_insensitive():
    assert _is_competitor_product("Jenkins") is True
    assert _is_competitor_product("KUBECOST") is True


def test_is_competitor_product_partial_match():
    assert _is_competitor_product("terraform cloud enterprise") is True


def test_is_competitor_product_not_competitor():
    assert _is_competitor_product("microsoft word") is False
    assert _is_competitor_product("salesforce") is False


def test_map_competitor_to_category():
    assert _map_competitor_to_category("jenkins") == "devops"
    assert _map_competitor_to_category("kubecost") == "finops"
    assert _map_competitor_to_category("backstage") == "platform_eng"
    assert _map_competitor_to_category("terraform cloud") == "cloud_infra"


def test_map_competitor_unknown():
    assert _map_competitor_to_category("unknown product") == "unknown"


# ---------------------------------------------------------------------------
# Intent classification
# ---------------------------------------------------------------------------


def test_classify_intent_strength_high():
    result = _classify_intent_strength("high")
    assert result == ("g2_active_research", 0.80)


def test_classify_intent_strength_medium():
    result = _classify_intent_strength("medium")
    assert result == ("g2_active_research", 0.65)


def test_classify_intent_strength_low_returns_none():
    assert _classify_intent_strength("low") is None


def test_classify_intent_strength_case_insensitive():
    assert _classify_intent_strength("HIGH") == ("g2_active_research", 0.80)
    assert _classify_intent_strength("Medium") == ("g2_active_research", 0.65)


# ---------------------------------------------------------------------------
# Dissatisfaction detection
# ---------------------------------------------------------------------------


def test_is_dissatisfied_review_low_rating():
    assert _is_dissatisfied_review({"star_rating": 2.0}) is True
    assert _is_dissatisfied_review({"star_rating": 3.0}) is True


def test_is_dissatisfied_review_high_rating():
    assert _is_dissatisfied_review({"star_rating": 4.0}) is False
    assert _is_dissatisfied_review({"star_rating": 5.0}) is False


def test_is_dissatisfied_review_missing_rating():
    assert _is_dissatisfied_review({}) is False


# ---------------------------------------------------------------------------
# Observation building
# ---------------------------------------------------------------------------


def test_build_observation_returns_valid_signal():
    obs = _build_observation(
        account_id="acc_123",
        signal_code="g2_active_research",
        confidence=0.80,
        source_reliability=0.78,
        evidence_text="G2 intent: DevOps research (strength=high)",
        evidence_url="https://www.g2.com/products/example",
        payload={"domain": "example.com", "category": "DevOps", "signal_strength": "high"},
    )
    assert obs.account_id == "acc_123"
    assert obs.signal_code == "g2_active_research"
    assert obs.product == "shared"
    assert obs.source == "g2_api"
    assert obs.confidence == 0.80
    assert obs.source_reliability == 0.78
    assert "DevOps" in obs.evidence_text
    assert obs.evidence_url == "https://www.g2.com/products/example"


def test_build_observation_clamps_confidence():
    obs = _build_observation(
        account_id="acc_1",
        signal_code="g2_active_research",
        confidence=1.5,
        source_reliability=-0.1,
        evidence_text="test",
        evidence_url="",
        payload={"x": 1},
    )
    assert obs.confidence == 1.0
    assert obs.source_reliability == 0.0


def test_build_observation_truncates_evidence():
    long_text = "x" * 1000
    obs = _build_observation(
        account_id="acc_1",
        signal_code="g2_active_research",
        confidence=0.8,
        source_reliability=0.8,
        evidence_text=long_text,
        evidence_url="",
        payload={"x": 1},
    )
    assert len(obs.evidence_text) <= 500


# ---------------------------------------------------------------------------
# Full collector (mocked API + DB)
# ---------------------------------------------------------------------------


def _mock_settings(**overrides):
    defaults = {
        "g2_api_key": "test-g2-key-123",
        "g2_api_base_url": "https://data.g2.com/api/v1",
        "g2_competitor_product_ids": ["prod_1", "prod_2"],
        "g2_review_lookback_days": 30,
    }
    defaults.update(overrides)
    s = MagicMock()
    for k, v in defaults.items():
        setattr(s, k, v)
    return s


def _mock_conn(accounts=None):
    """Build a mock connection with accounts query."""
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


_SAMPLE_INTENT_RESPONSE = [
    {"category": "DevOps", "signal_strength": "high", "activity_count": 12, "url": ""},
    {"category": "Cloud Cost Management", "signal_strength": "medium", "activity_count": 5, "url": ""},
    {"category": "HR Software", "signal_strength": "low", "activity_count": 2, "url": ""},
]

_SAMPLE_REVIEWS_RESPONSE = [
    {
        "reviewer_company_domain": "example.com",
        "product_name": "Jenkins",
        "star_rating": 2.0,
        "title": "Too complex for our team",
        "url": "https://www.g2.com/reviews/1",
    },
    {
        "reviewer_company_domain": "acme.io",
        "product_name": "Kubecost",
        "star_rating": 4.5,
        "title": "Good but missing features",
        "url": "https://www.g2.com/reviews/2",
    },
    {
        "reviewer_company_domain": "unknown-corp.com",
        "product_name": "Jenkins",
        "star_rating": 1.0,
        "title": "Terrible experience",
        "url": "https://www.g2.com/reviews/3",
    },
]


@patch("src.integrations.g2._fetch_competitor_reviews")
@patch("src.integrations.g2._fetch_intent_data")
@patch("src.integrations.g2.db")
def test_collect_creates_intent_signals(mock_db, mock_fetch_intent, mock_fetch_reviews):
    mock_fetch_intent.return_value = _SAMPLE_INTENT_RESPONSE
    mock_fetch_reviews.return_value = []
    mock_db.was_crawled_today.return_value = False
    mock_db.insert_signal_observation.return_value = True
    mock_db.mark_crawled.return_value = None

    conn = _mock_conn([{"account_id": "acc_1", "domain": "example.com"}])
    settings = _mock_settings()
    result = collect(conn, settings, {}, {"g2_api": 0.78})

    assert result["inserted"] > 0
    assert result["seen"] > 0

    calls = mock_db.insert_signal_observation.call_args_list
    signal_codes = [c[0][1].signal_code for c in calls]
    assert "g2_active_research" in signal_codes


@patch("src.integrations.g2._fetch_competitor_reviews")
@patch("src.integrations.g2._fetch_intent_data")
@patch("src.integrations.g2.db")
def test_collect_creates_dissatisfaction_signals(mock_db, mock_fetch_intent, mock_fetch_reviews):
    mock_fetch_intent.return_value = []
    mock_fetch_reviews.return_value = _SAMPLE_REVIEWS_RESPONSE
    mock_db.was_crawled_today.return_value = False
    mock_db.insert_signal_observation.return_value = True
    mock_db.mark_crawled.return_value = None

    conn = _mock_conn(
        [
            {"account_id": "acc_1", "domain": "example.com"},
            {"account_id": "acc_2", "domain": "acme.io"},
        ]
    )
    settings = _mock_settings()
    collect(conn, settings, {}, {"g2_api": 0.78})

    calls = mock_db.insert_signal_observation.call_args_list
    signal_codes = [c[0][1].signal_code for c in calls]
    # example.com reviewed Jenkins with rating 2.0 -> dissatisfaction
    assert "competitor_dissatisfaction" in signal_codes


@patch("src.integrations.g2._fetch_competitor_reviews")
@patch("src.integrations.g2._fetch_intent_data")
@patch("src.integrations.g2.db")
def test_collect_creates_review_activity_signals(mock_db, mock_fetch_intent, mock_fetch_reviews):
    mock_fetch_intent.return_value = []
    mock_fetch_reviews.return_value = _SAMPLE_REVIEWS_RESPONSE
    mock_db.was_crawled_today.return_value = False
    mock_db.insert_signal_observation.return_value = True
    mock_db.mark_crawled.return_value = None

    conn = _mock_conn(
        [
            {"account_id": "acc_1", "domain": "example.com"},
            {"account_id": "acc_2", "domain": "acme.io"},
        ]
    )
    settings = _mock_settings()
    collect(conn, settings, {}, {"g2_api": 0.78})

    calls = mock_db.insert_signal_observation.call_args_list
    signal_codes = [c[0][1].signal_code for c in calls]
    # acme.io reviewed Kubecost with rating 4.5 -> activity (not dissatisfied)
    assert "competitor_review_activity" in signal_codes


@patch("src.integrations.g2._fetch_intent_data")
@patch("src.integrations.g2.db")
def test_collect_skips_already_crawled(mock_db, mock_fetch_intent):
    mock_db.was_crawled_today.return_value = True

    conn = _mock_conn([{"account_id": "acc_1", "domain": "example.com"}])
    settings = _mock_settings(g2_competitor_product_ids=[])
    result = collect(conn, settings, {}, {"g2_api": 0.78})

    mock_fetch_intent.assert_not_called()
    assert result["inserted"] == 0


def test_collect_skips_when_no_api_key():
    conn = _mock_conn()
    settings = _mock_settings(g2_api_key="")
    result = collect(conn, settings, {}, {})

    assert result == {"inserted": 0, "seen": 0}


@patch("src.integrations.g2._fetch_competitor_reviews")
@patch("src.integrations.g2._fetch_intent_data")
@patch("src.integrations.g2.db")
def test_collect_handles_api_errors_gracefully(mock_db, mock_fetch_intent, mock_fetch_reviews):
    import requests

    mock_fetch_intent.side_effect = requests.RequestException("timeout")
    mock_fetch_reviews.return_value = []
    mock_db.was_crawled_today.return_value = False
    mock_db.record_crawl_attempt.return_value = None
    mock_db.mark_crawled.return_value = None

    conn = _mock_conn([{"account_id": "acc_1", "domain": "example.com"}])
    settings = _mock_settings(g2_competitor_product_ids=[])
    result = collect(conn, settings, {}, {"g2_api": 0.78})

    assert result["inserted"] == 0
    mock_db.record_crawl_attempt.assert_called_once()


@patch("src.integrations.g2._fetch_competitor_reviews")
@patch("src.integrations.g2._fetch_intent_data")
@patch("src.integrations.g2.db")
def test_collect_ignores_unmatched_reviewer_domains(mock_db, mock_fetch_intent, mock_fetch_reviews):
    mock_fetch_intent.return_value = []
    mock_fetch_reviews.return_value = [
        {
            "reviewer_company_domain": "unknown-corp.com",
            "product_name": "Jenkins",
            "star_rating": 1.0,
            "title": "Bad",
            "url": "",
        },
    ]
    mock_db.was_crawled_today.return_value = False
    mock_db.insert_signal_observation.return_value = True
    mock_db.mark_crawled.return_value = None

    conn = _mock_conn([{"account_id": "acc_1", "domain": "example.com"}])
    settings = _mock_settings()
    result = collect(conn, settings, {}, {"g2_api": 0.78})

    # unknown-corp.com is not in our accounts, so no signal should be created.
    assert result["inserted"] == 0
    assert result["seen"] == 0


@patch("src.integrations.g2._fetch_competitor_reviews")
@patch("src.integrations.g2._fetch_intent_data")
@patch("src.integrations.g2.db")
def test_collect_skips_empty_domains(mock_db, mock_fetch_intent, mock_fetch_reviews):
    mock_db.was_crawled_today.return_value = False
    mock_fetch_reviews.return_value = []

    conn = _mock_conn([{"account_id": "acc_1", "domain": ""}])
    settings = _mock_settings(g2_competitor_product_ids=[])
    collect(conn, settings, {}, {"g2_api": 0.78})

    mock_fetch_intent.assert_not_called()


@patch("src.integrations.g2._fetch_competitor_reviews")
@patch("src.integrations.g2._fetch_intent_data")
@patch("src.integrations.g2.db")
def test_collect_skips_low_intent_signals(mock_db, mock_fetch_intent, mock_fetch_reviews):
    mock_fetch_intent.return_value = [
        {"category": "DevOps", "signal_strength": "low", "activity_count": 1, "url": ""},
    ]
    mock_fetch_reviews.return_value = []
    mock_db.was_crawled_today.return_value = False
    mock_db.mark_crawled.return_value = None

    conn = _mock_conn([{"account_id": "acc_1", "domain": "example.com"}])
    settings = _mock_settings(g2_competitor_product_ids=[])
    result = collect(conn, settings, {}, {"g2_api": 0.78})

    assert result["inserted"] == 0
    assert result["seen"] == 0


def test_competitor_products_has_expected_categories():
    expected = {"devops", "platform_eng", "finops", "cloud_infra"}
    assert set(COMPETITOR_PRODUCTS.keys()) == expected
