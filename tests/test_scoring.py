import json
from datetime import date
from pathlib import Path

from src.scoring.engine import classify_tier, recency_decay, run_scoring
from src.scoring.rules import (
    VALID_DIMENSIONS,
    DimensionWeight,
    SignalRule,
    Thresholds,
    TierUpgradeRule,
    load_dimension_weights,
    load_signal_rules,
    load_thresholds,
)


def test_recency_decay_half_life():
    assert round(recency_decay(0, 14), 4) == 1.0
    assert round(recency_decay(14, 14), 4) == 0.5


def test_scoring_high_intent_signal_alone_does_not_reach_high_tier():
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
        thresholds=Thresholds(tier_1=70, tier_2=45, tier_3=20, tier_4=0),
        source_reliability_defaults={"news_csv": 0.8},
    )

    assert len(output.account_scores) == 1
    score = output.account_scores[0]
    assert score.product == "zopnight"
    # Dimension-weighted scoring: single trigger_intent signal capped at 35% max → "medium" legacy tier
    assert score.tier == "medium"
    assert score.score < 45


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
        thresholds=Thresholds(tier_1=70, tier_2=45, tier_3=20, tier_4=0),
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
        thresholds=Thresholds(tier_1=70, tier_2=45, tier_3=20, tier_4=0),
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
        thresholds=Thresholds(tier_1=70, tier_2=45, tier_3=20, tier_4=0),
        source_reliability_defaults={"website_scan": 0.3},
    )
    uncapped = run_scoring(
        run_id="run_cap_2",
        run_date=date(2026, 2, 16),
        observations=observations,
        rules=rules,
        thresholds=Thresholds(tier_1=70, tier_2=45, tier_3=20, tier_4=0),
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
    assert len(rules) > 0
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

    from src.utils import load_csv_rows

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


def test_dimension_weighted_composite_and_persisted_dimension_scores():
    rules = {
        "intent_a": SignalRule(
            signal_code="intent_a",
            product_scope="zopdev",
            category="trigger_events",
            base_weight=60,
            half_life_days=30,
            min_confidence=0.5,
            enabled=True,
            dimension="trigger_intent",
        ),
        "tech_a": SignalRule(
            signal_code="tech_a",
            product_scope="zopdev",
            category="technographic",
            base_weight=20,
            half_life_days=30,
            min_confidence=0.5,
            enabled=True,
            dimension="tech_fit",
        ),
    }
    observations = [
        {
            "account_id": "acc_dim",
            "signal_code": "intent_a",
            "product": "shared",
            "source": "news_csv",
            "observed_at": "2026-02-16T00:00:00Z",
            "evidence_url": "",
            "evidence_text": "intent",
            "confidence": 1.0,
            "source_reliability": 1.0,
        },
        {
            "account_id": "acc_dim",
            "signal_code": "tech_a",
            "product": "shared",
            "source": "news_csv",
            "observed_at": "2026-02-16T00:00:00Z",
            "evidence_url": "",
            "evidence_text": "tech",
            "confidence": 1.0,
            "source_reliability": 1.0,
        },
    ]
    weights = {
        "trigger_intent": DimensionWeight("trigger_intent", 0.40, 70.0),
        "engagement_pql": DimensionWeight("engagement_pql", 0.30, 60.0),
        "hiring_growth": DimensionWeight("hiring_growth", 0.15, 40.0),
        "tech_fit": DimensionWeight("tech_fit", 0.10, 30.0),
        "firmographic": DimensionWeight("firmographic", 0.05, 20.0),
    }

    output = run_scoring(
        run_id="run_dim",
        run_date=date(2026, 2, 16),
        observations=observations,
        rules=rules,
        thresholds=Thresholds(tier_1=70, tier_2=45, tier_3=20, tier_4=0),
        source_reliability_defaults={"news_csv": 1.0},
        dimension_weights=weights,
    )

    assert len(output.account_scores) == 1
    score = output.account_scores[0]
    dimensions = json.loads(score.dimension_scores_json)
    # trigger_intent: raw=60, ceiling=70 → 85.71; tech_fit: raw=20, ceiling=30 → 66.67
    assert round(dimensions["trigger_intent"], 1) == 85.7
    assert round(dimensions["tech_fit"], 1) == 66.7
    # 85.71 * 0.40 + 66.67 * 0.10 = 40.95
    assert round(score.score, 2) == 40.95
    # tier_2 (score=40.95 < threshold 45) → legacy "medium"
    assert score.tier == "medium"


def test_dimension_ceiling_caps_inflation():
    rules = {
        "intent_big": SignalRule(
            signal_code="intent_big",
            product_scope="zopdev",
            category="trigger_events",
            base_weight=500,
            half_life_days=30,
            min_confidence=0.5,
            enabled=True,
            dimension="trigger_intent",
        )
    }
    observations = [
        {
            "account_id": "acc_cap",
            "signal_code": "intent_big",
            "product": "shared",
            "source": "news_csv",
            "observed_at": "2026-02-16T00:00:00Z",
            "evidence_url": "",
            "evidence_text": "big intent",
            "confidence": 1.0,
            "source_reliability": 1.0,
        }
    ]

    output = run_scoring(
        run_id="run_dim_cap",
        run_date=date(2026, 2, 16),
        observations=observations,
        rules=rules,
        thresholds=Thresholds(tier_1=70, tier_2=45, tier_3=20, tier_4=0),
        source_reliability_defaults={"news_csv": 1.0},
    )

    assert len(output.account_scores) == 1
    dimensions = json.loads(output.account_scores[0].dimension_scores_json)
    assert dimensions["trigger_intent"] == 100.0


def test_load_dimension_weights_reads_config_file():
    weights = load_dimension_weights(Path("config/dimension_weights.csv"))
    assert set(weights.keys()) == VALID_DIMENSIONS
    assert round(sum(value.weight for value in weights.values()), 6) == 1.0


def test_load_thresholds_supports_new_4_tier_format(tmp_path: Path):
    path = tmp_path / "thresholds.csv"
    path.write_text("key,value\ntier_1,80\ntier_2,60\ntier_3,40\ntier_4,0\n", encoding="utf-8")
    rules_path = tmp_path / "tier_upgrade_rules.csv"
    rules_path.write_text(
        (
            "rule_name,condition_dimension,condition_threshold,current_tier,promote_to_tier\n"
            "strong_trigger_upgrade,trigger_intent,70,tier_2,tier_1\n"
        ),
        encoding="utf-8",
    )

    thresholds = load_thresholds(path)
    assert thresholds.tier_1 == 80.0
    assert thresholds.tier_2 == 60.0
    assert thresholds.tier_3 == 40.0
    assert thresholds.tier_4 == 0.0
    assert len(thresholds.upgrade_rules) == 1


def test_classify_tier_applies_upgrade_rules():
    thresholds = Thresholds(
        tier_1=80.0,
        tier_2=60.0,
        tier_3=40.0,
        tier_4=0.0,
        upgrade_rules=(
            # PQL score upgrades tier_3 -> tier_2.
            TierUpgradeRule(
                rule_name="pql_engagement_upgrade",
                condition_dimension="engagement_pql",
                condition_threshold=80.0,
                current_tier="*",
                promote_to_tier="+1",
            ),
            # Trigger intent can then upgrade tier_2 -> tier_1.
            TierUpgradeRule(
                rule_name="strong_trigger_upgrade",
                condition_dimension="trigger_intent",
                condition_threshold=70.0,
                current_tier="tier_2",
                promote_to_tier="tier_1",
            ),
        ),
    )

    tier = classify_tier(
        58.0,
        thresholds,
        dimension_scores={"engagement_pql": 81.0, "trigger_intent": 75.0},
    )
    assert tier == "tier_1"


# --- Issue #57: tier-change alert tests ---


def test_tier_rank_ordering():
    from src.notifier import _tier_rank

    assert _tier_rank("low") < _tier_rank("medium") < _tier_rank("high")
    assert _tier_rank("unknown") == -1


def test_send_tier_change_alerts_builds_message(tmp_path):
    from src.notifier import send_tier_change_alerts
    from src.settings import Settings

    settings = Settings(
        project_root=str(tmp_path),
        out_dir=str(tmp_path / "out"),
        gchat_webhook_url="",
        alert_email_to="",
    )
    changes = [
        {
            "company_name": "Acme Corp",
            "product": "zopdev",
            "old_tier": "medium",
            "new_tier": "high",
            "score": 75.0,
            "delta_7d": 15.2,
            "top_reason": "cost_reduction_mandate",
            "velocity_category": "surging",
        },
        {
            "company_name": "Beta Inc",
            "product": "zopnight",
            "old_tier": "high",
            "new_tier": "medium",
            "score": 35.0,
            "delta_7d": -12.0,
            "top_reason": "",
            "velocity_category": "decelerating",
        },
    ]
    result = send_tier_change_alerts(settings, changes)
    assert "local_log" in result["delivered_channels"]
    # Verify alert was written
    log_path = tmp_path / "out" / "alerts.log"
    assert log_path.exists()
    content = log_path.read_text()
    assert "Acme Corp" in content
    assert "Beta Inc" in content
    assert "1 up" in content
    assert "1 down" in content


def test_send_tier_change_alerts_empty_list(tmp_path):
    from src.notifier import send_tier_change_alerts
    from src.settings import Settings

    settings = Settings(
        project_root=str(tmp_path),
        out_dir=str(tmp_path / "out"),
    )
    result = send_tier_change_alerts(settings, [])
    assert result["delivered_channels"] == []
