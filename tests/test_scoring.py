from datetime import date
from pathlib import Path

from src.scoring.engine import recency_decay, run_scoring
from src.scoring.rules import VALID_DIMENSIONS, SignalRule, Thresholds, load_signal_rules


def test_recency_decay_half_life():
    assert round(recency_decay(0, 14), 4) == 1.0
    assert round(recency_decay(14, 14), 4) == 0.5


def test_scoring_high_intent_signal_reaches_high_tier():
    rules = {
        "cost_reduction_mandate": SignalRule(
            signal_code="cost_reduction_mandate",
            product_scope="zopnight",
            category="spend_variance",
            base_weight=120,
            half_life_days=30,
            min_confidence=0.5,
            enabled=True,
        )
    }
    observations = [
        {
            "account_id": "acc_1",
            "signal_code": "cost_reduction_mandate",
            "product": "shared",
            "source": "news_csv",
            "observed_at": "2026-02-15T12:00:00Z",
            "evidence_url": "https://example.com/news",
            "evidence_text": "Cloud cost reduction mandate",
            "confidence": 0.95,
            "source_reliability": 0.95,
        }
    ]

    output = run_scoring(
        run_id="run_x",
        run_date=date(2026, 2, 16),
        observations=observations,
        rules=rules,
        thresholds=Thresholds(high=70, medium=45, low=0),
        source_reliability_defaults={"news_csv": 0.8},
    )

    assert len(output.account_scores) == 1
    score = output.account_scores[0]
    assert score.product == "zopnight"
    assert score.tier == "high"
    assert score.score >= 70


def test_single_weak_signal_does_not_reach_high():
    rules = {
        "kubernetes_detected": SignalRule(
            signal_code="kubernetes_detected",
            product_scope="zopdev",
            category="technographic",
            base_weight=10,
            half_life_days=45,
            min_confidence=0.5,
            enabled=True,
        )
    }
    observations = [
        {
            "account_id": "acc_2",
            "signal_code": "kubernetes_detected",
            "product": "shared",
            "source": "technographics_csv",
            "observed_at": "2026-02-16T00:00:00Z",
            "evidence_url": "",
            "evidence_text": "kubernetes",
            "confidence": 0.55,
            "source_reliability": 0.6,
        }
    ]

    output = run_scoring(
        run_id="run_y",
        run_date=date(2026, 2, 16),
        observations=observations,
        rules=rules,
        thresholds=Thresholds(high=70, medium=45, low=0),
        source_reliability_defaults={"technographics_csv": 0.8},
    )

    assert len(output.account_scores) == 1
    assert output.account_scores[0].tier in {"low", "medium"}
    assert output.account_scores[0].score < 70


def test_disabled_source_from_registry_does_not_contribute():
    rules = {
        "kubernetes_detected": SignalRule(
            signal_code="kubernetes_detected",
            product_scope="zopdev",
            category="technographic",
            base_weight=20,
            half_life_days=30,
            min_confidence=0.5,
            enabled=True,
        )
    }
    observations = [
        {
            "account_id": "acc_3",
            "signal_code": "kubernetes_detected",
            "product": "shared",
            "source": "website_scan",
            "observed_at": "2026-02-16T00:00:00Z",
            "evidence_url": "",
            "evidence_text": "kubernetes",
            "confidence": 0.9,
            "source_reliability": 0.95,
        }
    ]

    output = run_scoring(
        run_id="run_z",
        run_date=date(2026, 2, 16),
        observations=observations,
        rules=rules,
        thresholds=Thresholds(high=70, medium=45, low=0),
        source_reliability_defaults={"website_scan": 0.0},
    )

    assert len(output.account_scores) == 0


def test_registry_reliability_caps_observation_reliability():
    rules = {
        "kubernetes_detected": SignalRule(
            signal_code="kubernetes_detected",
            product_scope="zopdev",
            category="technographic",
            base_weight=10,
            half_life_days=30,
            min_confidence=0.5,
            enabled=True,
        )
    }
    observations = [
        {
            "account_id": "acc_cap",
            "signal_code": "kubernetes_detected",
            "product": "shared",
            "source": "website_scan",
            "observed_at": "2026-02-16T00:00:00Z",
            "evidence_url": "",
            "evidence_text": "kubernetes",
            "confidence": 1.0,
            "source_reliability": 0.9,
        }
    ]

    capped = run_scoring(
        run_id="run_cap_1",
        run_date=date(2026, 2, 16),
        observations=observations,
        rules=rules,
        thresholds=Thresholds(high=70, medium=45, low=0),
        source_reliability_defaults={"website_scan": 0.3},
    )
    uncapped = run_scoring(
        run_id="run_cap_2",
        run_date=date(2026, 2, 16),
        observations=observations,
        rules=rules,
        thresholds=Thresholds(high=70, medium=45, low=0),
        source_reliability_defaults={"website_scan": 1.0},
    )

    assert len(capped.account_scores) == 1
    assert len(uncapped.account_scores) == 1
    assert capped.account_scores[0].score < uncapped.account_scores[0].score


# --- Issue #17: dimension mapping tests ---


def test_signal_rules_include_dimension():
    """All rules loaded from the real CSV should have a dimension field."""
    registry_path = Path("config/signal_registry.csv")
    rules = load_signal_rules(registry_path)
    # Issue #17 mapping table defines 35 signals (not 36).
    assert len(rules) == 35
    for signal_code, rule in rules.items():
        assert hasattr(rule, "dimension"), f"{signal_code} missing dimension"
        assert rule.dimension, f"{signal_code} has empty dimension"


def test_all_signals_have_valid_dimension():
    """Every signal in signal_registry.csv must map to one of the 5 valid dimensions."""
    registry_path = Path("config/signal_registry.csv")
    rules = load_signal_rules(registry_path)
    invalid = {code: rule.dimension for code, rule in rules.items() if rule.dimension not in VALID_DIMENSIONS}
    assert not invalid, f"Signals with invalid dimensions: {invalid}"


def test_dimension_default_when_missing():
    """When dimension column is absent from a row, load_signal_rules defaults to trigger_intent."""
    from unittest.mock import patch

    fake_rows = [
        {
            "signal_code": "test_signal",
            "product_scope": "zopdev",
            "category": "test",
            "base_weight": "10",
            "half_life_days": "30",
            "min_confidence": "0.5",
            "enabled": "true",
            # dimension column intentionally absent
        }
    ]
    with patch("src.scoring.rules.load_csv_rows", return_value=fake_rows):
        rules = load_signal_rules(Path("config/signal_registry.csv"))

    assert "test_signal" in rules
    assert rules["test_signal"].dimension == "trigger_intent"


# --- Issue #26: defensive error handling tests ---


def test_scoring_completes_with_partial_malformed_input():
    """Valid observations scored; malformed ones skipped without crashing."""
    rules = {
        "kubernetes_detected": SignalRule(
            signal_code="kubernetes_detected",
            product_scope="zopdev",
            category="technographic",
            base_weight=20,
            half_life_days=30,
            min_confidence=0.5,
            enabled=True,
        )
    }
    observations = [
        # Valid observation
        {
            "account_id": "acc_ok",
            "signal_code": "kubernetes_detected",
            "product": "shared",
            "source": "technographics_csv",
            "observed_at": "2026-02-16T00:00:00Z",
            "evidence_url": "",
            "evidence_text": "kubernetes",
            "confidence": 0.9,
            "source_reliability": 0.8,
        },
        # Malformed: confidence=None
        {
            "account_id": "acc_bad1",
            "signal_code": "kubernetes_detected",
            "product": "shared",
            "source": "technographics_csv",
            "observed_at": "2026-02-16T00:00:00Z",
            "evidence_url": "",
            "evidence_text": "kubernetes",
            "confidence": None,
            "source_reliability": 0.8,
        },
        # Malformed: empty signal_code
        {
            "account_id": "acc_bad2",
            "signal_code": "",
            "product": "shared",
            "source": "technographics_csv",
            "observed_at": "2026-02-16T00:00:00Z",
            "evidence_url": "",
            "evidence_text": "kubernetes",
            "confidence": 0.9,
            "source_reliability": 0.8,
        },
    ]

    output = run_scoring(
        run_id="run_partial",
        run_date=date(2026, 2, 16),
        observations=observations,
        rules=rules,
        thresholds=Thresholds(high=70, medium=45, low=0),
        source_reliability_defaults={"technographics_csv": 0.8},
    )

    # Only the valid observation should produce a score
    assert len(output.account_scores) == 1
    assert output.account_scores[0].account_id == "acc_ok"


def test_delta_lookup_failure_defaults_to_zero():
    """If delta_lookup raises, scoring still completes with delta_7d=0.0."""
    rules = {
        "kubernetes_detected": SignalRule(
            signal_code="kubernetes_detected",
            product_scope="zopdev",
            category="technographic",
            base_weight=20,
            half_life_days=30,
            min_confidence=0.5,
            enabled=True,
        )
    }
    observations = [
        {
            "account_id": "acc_delta",
            "signal_code": "kubernetes_detected",
            "product": "shared",
            "source": "technographics_csv",
            "observed_at": "2026-02-16T00:00:00Z",
            "evidence_url": "",
            "evidence_text": "kubernetes",
            "confidence": 0.9,
            "source_reliability": 0.8,
        }
    ]

    def broken_delta_lookup(account_id, product):
        raise RuntimeError("DB connection lost")

    output = run_scoring(
        run_id="run_delta_fail",
        run_date=date(2026, 2, 16),
        observations=observations,
        rules=rules,
        thresholds=Thresholds(high=70, medium=45, low=0),
        source_reliability_defaults={"technographics_csv": 0.8},
        delta_lookup=broken_delta_lookup,
    )

    assert len(output.account_scores) == 1
    assert output.account_scores[0].delta_7d == 0.0


def test_observation_with_invalid_confidence_type_skipped():
    """Observation with non-numeric confidence is skipped, not crash."""
    rules = {
        "kubernetes_detected": SignalRule(
            signal_code="kubernetes_detected",
            product_scope="zopdev",
            category="technographic",
            base_weight=20,
            half_life_days=30,
            min_confidence=0.5,
            enabled=True,
        )
    }
    observations = [
        {
            "account_id": "acc_bad_conf",
            "signal_code": "kubernetes_detected",
            "product": "shared",
            "source": "technographics_csv",
            "observed_at": "2026-02-16T00:00:00Z",
            "evidence_url": "",
            "evidence_text": "kubernetes",
            "confidence": "not_a_number",
            "source_reliability": 0.8,
        }
    ]

    output = run_scoring(
        run_id="run_bad_conf",
        run_date=date(2026, 2, 16),
        observations=observations,
        rules=rules,
        thresholds=Thresholds(high=70, medium=45, low=0),
        source_reliability_defaults={"technographics_csv": 0.8},
    )

    # Bad confidence observation skipped — no scores produced
    assert len(output.account_scores) == 0
