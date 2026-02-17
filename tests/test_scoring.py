from datetime import date

from src.scoring.engine import recency_decay, run_scoring
from src.scoring.rules import SignalRule, Thresholds


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
