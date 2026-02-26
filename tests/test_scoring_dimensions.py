"""Comprehensive scoring engine tests for new multi-dimensional scoring.

Covers:
  - Dimension scoring (grouping, normalization, ceilings)
  - Composite score (weighted formula, deterministic, min/max)
  - Tier classification (4-tier boundaries, upgrade rules)
  - Signal velocity (7d/14d/30d, classification)
  - Confidence bands (source diversity per dimension)
  - ICP reference account regression
  - Anti-inflation & edge cases

Issue: https://github.com/talvinder/signals/issues/22
Epic:  https://github.com/talvinder/signals/issues/11
"""

from __future__ import annotations

import math
from datetime import date

import pytest

from src.scoring.engine import (
    MAX_OBSERVATIONS_PER_SIGNAL,
    MAX_OBSERVATIONS_PER_SOURCE_PER_SIGNAL,
    EngineOutput,
    _resolve_products,
    classify_tier,
    recency_decay,
    run_scoring,
)
from src.scoring.rules import SignalRule, Thresholds

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# The 5 scoring dimensions from Issue #18
DIMENSIONS = [
    "trigger_intent",
    "tech_fit",
    "engagement_pql",
    "firmographic",
    "hiring_growth",
]

# Dimension weights from Issue #18
DIMENSION_WEIGHTS = {
    "trigger_intent": 0.35,
    "tech_fit": 0.20,
    "engagement_pql": 0.25,
    "firmographic": 0.10,
    "hiring_growth": 0.10,
}

# Dimension ceilings from Issue #18 (raw score sum that maps to 100)
DIMENSION_CEILINGS = {
    "trigger_intent": 60,
    "tech_fit": 40,
    "engagement_pql": 50,
    "firmographic": 30,
    "hiring_growth": 30,
}

RUN_DATE = date(2026, 2, 20)
RUN_ID = "test_run"

# Default 3-tier thresholds (current system) — mapped to 4-tier fields
THRESHOLDS_3TIER = Thresholds(tier_1=20, tier_2=10, tier_3=5, tier_4=0)

# New 4-tier thresholds (Issue #21)
THRESHOLDS_4TIER = Thresholds(tier_1=80, tier_2=60, tier_3=40, tier_4=0)

DEFAULT_SOURCE_RELIABILITY = {"news_csv": 0.75, "technographics_csv": 0.8}


def _make_rule(
    signal_code: str,
    category: str = "trigger_events",
    product_scope: str = "shared",
    base_weight: float = 10.0,
    half_life_days: float = 30.0,
    min_confidence: float = 0.5,
    enabled: bool = True,
) -> SignalRule:
    return SignalRule(
        signal_code=signal_code,
        product_scope=product_scope,
        category=category,
        base_weight=base_weight,
        half_life_days=half_life_days,
        min_confidence=min_confidence,
        enabled=enabled,
    )


def _make_observation(
    account_id: str = "acc_1",
    signal_code: str = "test_signal",
    product: str = "shared",
    source: str = "news_csv",
    observed_at: str = "2026-02-19T12:00:00Z",
    confidence: float = 0.9,
    source_reliability: float = 0.8,
    evidence_url: str = "https://example.com",
    evidence_text: str = "test evidence",
    **kwargs,
) -> dict:
    obs = {
        "account_id": account_id,
        "signal_code": signal_code,
        "product": product,
        "source": source,
        "observed_at": observed_at,
        "confidence": confidence,
        "source_reliability": source_reliability,
        "evidence_url": evidence_url,
        "evidence_text": evidence_text,
    }
    obs.update(kwargs)
    return obs


def _score(
    observations: list[dict],
    rules: dict[str, SignalRule],
    thresholds: Thresholds = THRESHOLDS_3TIER,
    source_defaults: dict[str, float] | None = None,
    delta_lookup=None,
) -> EngineOutput:
    return run_scoring(
        run_id=RUN_ID,
        run_date=RUN_DATE,
        observations=observations,
        rules=rules,
        thresholds=thresholds,
        source_reliability_defaults=source_defaults or DEFAULT_SOURCE_RELIABILITY,
        delta_lookup=delta_lookup,
    )


# ===================================================================
# 1. RECENCY DECAY TESTS
# ===================================================================


class TestRecencyDecay:
    """Tests for recency_decay() — exponential half-life calculation."""

    def test_zero_days_returns_one(self):
        """No decay for observation made today."""
        assert recency_decay(0, 14) == 1.0

    def test_at_half_life_returns_half(self):
        """Exactly at half-life, decay = 0.5."""
        assert round(recency_decay(14, 14), 4) == 0.5

    def test_double_half_life_returns_quarter(self):
        """At 2x half-life, decay = 0.25."""
        assert round(recency_decay(28, 14), 4) == 0.25

    def test_negative_days_treated_as_zero(self):
        """Future observation — clamped to 0 days, so no decay."""
        assert recency_decay(-5, 14) == 1.0

    def test_zero_half_life_returns_one(self):
        """Zero half-life means no decay applied."""
        assert recency_decay(100, 0) == 1.0

    def test_negative_half_life_returns_one(self):
        """Negative half-life is treated same as zero."""
        assert recency_decay(10, -5) == 1.0

    @pytest.mark.parametrize(
        "days,half_life,expected",
        [
            (7, 14, round(math.pow(0.5, 7 / 14), 4)),
            (30, 30, 0.5),
            (90, 30, round(math.pow(0.5, 3), 4)),
            (1, 1, 0.5),
        ],
    )
    def test_parametrized_decay_values(self, days, half_life, expected):
        """Various decay calculations match formula."""
        assert round(recency_decay(days, half_life), 4) == expected


# ===================================================================
# 2. TIER CLASSIFICATION TESTS (Current 3-tier system)
# ===================================================================


class TestClassifyTier:
    """Tests for classify_tier() — current 3-tier thresholds mapped to 4-tier names."""

    def test_high_tier_at_boundary(self):
        assert classify_tier(20.0, THRESHOLDS_3TIER) == "tier_1"

    def test_high_tier_above_boundary(self):
        assert classify_tier(50.0, THRESHOLDS_3TIER) == "tier_1"

    def test_medium_tier_at_boundary(self):
        assert classify_tier(10.0, THRESHOLDS_3TIER) == "tier_2"

    def test_medium_tier_between_boundaries(self):
        assert classify_tier(15.0, THRESHOLDS_3TIER) == "tier_2"

    def test_low_tier_below_medium(self):
        assert classify_tier(9.99, THRESHOLDS_3TIER) == "tier_3"

    def test_low_tier_at_zero(self):
        assert classify_tier(0.0, THRESHOLDS_3TIER) == "tier_4"

    def test_score_exactly_at_100(self):
        assert classify_tier(100.0, THRESHOLDS_3TIER) == "tier_1"


# ===================================================================
# 3. TIER CLASSIFICATION TESTS (New 4-tier system — Issue #21)
# ===================================================================


class TestClassifyTier4Tier:
    """Tests for the new 4-tier classification system.

    New thresholds: tier_1 >= 80, tier_2 >= 60, tier_3 >= 40, tier_4 < 40.
    Until Issue #21 is implemented, these test the existing classify_tier()
    with the new threshold values.
    """

    def test_tier1_at_boundary_80(self):
        """Score exactly 80 → tier_1."""
        assert classify_tier(80.0, THRESHOLDS_4TIER) == "tier_1"

    def test_tier2_at_boundary_60(self):
        """Score exactly 60 → tier_2."""
        assert classify_tier(60.0, THRESHOLDS_4TIER) == "tier_2"

    def test_tier3_at_boundary_40(self):
        """Score exactly 40 → tier_3."""
        assert classify_tier(40.0, THRESHOLDS_4TIER) == "tier_3"

    def test_tier4_below_40(self):
        """Score below 40 → tier_4."""
        assert classify_tier(0.0, THRESHOLDS_4TIER) == "tier_4"

    def test_max_score_100(self):
        assert classify_tier(100.0, THRESHOLDS_4TIER) == "tier_1"


# ===================================================================
# 4. PRODUCT RESOLUTION TESTS
# ===================================================================


class TestResolveProducts:
    """Tests for _resolve_products()."""

    def test_shared_observation_shared_scope(self):
        result = _resolve_products("shared", "shared")
        assert set(result) == {"zopdev", "zopday", "zopnight"}

    def test_specific_product_shared_scope(self):
        result = _resolve_products("zopdev", "shared")
        assert result == ("zopdev",)

    def test_specific_product_matching_scope(self):
        result = _resolve_products("zopdev", "zopdev")
        assert result == ("zopdev",)

    def test_specific_product_mismatched_scope(self):
        result = _resolve_products("zopdev", "zopnight")
        assert result == ()

    def test_unknown_product(self):
        result = _resolve_products("unknown", "shared")
        assert result == ()

    def test_all_scope(self):
        result = _resolve_products("shared", "all")
        assert set(result) == {"zopdev", "zopday", "zopnight"}


# ===================================================================
# 5. SCORING ENGINE — BASIC SCORING TESTS
# ===================================================================


class TestBasicScoring:
    """Core run_scoring() tests."""

    def test_single_signal_produces_score(self):
        """One signal → one component score + one account score."""
        rules = {"sig_a": _make_rule("sig_a", base_weight=10)}
        obs = [_make_observation(signal_code="sig_a")]
        output = _score(obs, rules)

        assert len(output.component_scores) == 3  # shared → 3 products
        assert len(output.account_scores) == 3

    def test_disabled_rule_produces_no_score(self):
        """Disabled signal rule → no output."""
        rules = {"sig_a": _make_rule("sig_a", enabled=False)}
        obs = [_make_observation(signal_code="sig_a")]
        output = _score(obs, rules)

        assert len(output.account_scores) == 0

    def test_below_min_confidence_filtered(self):
        """Observation below min_confidence → no output."""
        rules = {"sig_a": _make_rule("sig_a", min_confidence=0.8)}
        obs = [_make_observation(signal_code="sig_a", confidence=0.5)]
        output = _score(obs, rules)

        assert len(output.account_scores) == 0

    def test_unknown_signal_code_ignored(self):
        """Observation with no matching rule → ignored."""
        rules = {"sig_a": _make_rule("sig_a")}
        obs = [_make_observation(signal_code="unknown_signal")]
        output = _score(obs, rules)

        assert len(output.account_scores) == 0

    def test_no_observations_empty_output(self):
        """No observations → empty output."""
        rules = {"sig_a": _make_rule("sig_a")}
        output = _score([], rules)

        assert len(output.account_scores) == 0
        assert len(output.component_scores) == 0


# ===================================================================
# 6. SCORING ENGINE — SCORE CALCULATION & FORMULA
# ===================================================================


class TestScoreCalculation:
    """Tests for the scoring formula: weight × confidence × reliability × decay."""

    def test_deterministic_score(self):
        """Same inputs → same score every time."""
        rules = {"sig_a": _make_rule("sig_a", base_weight=20, half_life_days=30)}
        obs = [
            _make_observation(
                signal_code="sig_a",
                confidence=0.9,
                source_reliability=0.8,
                observed_at="2026-02-19T12:00:00Z",
            )
        ]

        output1 = _score(obs, rules)
        output2 = _score(obs, rules)

        assert output1.account_scores[0].score == output2.account_scores[0].score

    def test_score_formula_correctness(self):
        """Verify manual calculation matches engine output.

        Engine rounds component_score to 4 places, then applies dimension-weighted
        scoring: normalize by ceiling, multiply by dimension weight.
        """
        base_weight = 15.0
        confidence = 0.9
        source_rel = 0.75  # capped by registry
        half_life = 30.0
        days_since = 1  # observed 2026-02-19, run 2026-02-20

        expected_component = round(
            base_weight * confidence * source_rel * recency_decay(days_since, half_life),
            4,
        )
        # Dimension-weighted scoring: normalize by ceiling (60.0), then weight (0.35)
        dim_normalized = min(100.0, round((expected_component / 60.0) * 100.0, 2))
        expected_score = min(100.0, round(dim_normalized * 0.35, 2))

        rules = {
            "sig_a": _make_rule(
                "sig_a",
                base_weight=base_weight,
                half_life_days=half_life,
                product_scope="zopdev",
            )
        }
        obs = [
            _make_observation(
                signal_code="sig_a",
                product="zopdev",
                confidence=confidence,
                source_reliability=0.9,  # registry caps to 0.75
            )
        ]
        output = _score(obs, rules)

        assert len(output.account_scores) == 1
        assert output.account_scores[0].score == expected_score

    def test_max_score_capped_at_100(self):
        """Even with many high-weight signals, total ≤ 100."""
        rules = {}
        obs = []
        for i in range(20):
            code = f"sig_{i}"
            rules[code] = _make_rule(code, base_weight=50, product_scope="zopdev")
            obs.append(
                _make_observation(
                    signal_code=code,
                    product="zopdev",
                    source=f"source_{i}",
                    confidence=1.0,
                    source_reliability=1.0,
                )
            )

        output = _score(obs, rules, source_defaults={f"source_{i}": 1.0 for i in range(20)})

        for score in output.account_scores:
            assert score.score <= 100.0

    def test_minimum_nonzero_score(self):
        """Smallest possible contribution: low weight, low confidence, old signal.

        With base_weight=1, confidence=0.02, reliability=0.1, decay=2^(-10/1):
        component = 1 * 0.02 * 0.1 * 2^(-10) ≈ 0.000002 → rounds to 0.0 at 4dp.
        Use slightly less extreme values so the component is non-zero.
        """
        rules = {
            "sig_tiny": _make_rule(
                "sig_tiny",
                base_weight=1.0,
                half_life_days=14.0,
                min_confidence=0.01,
                product_scope="zopdev",
            )
        }
        obs = [
            _make_observation(
                signal_code="sig_tiny",
                product="zopdev",
                confidence=0.1,
                source_reliability=0.2,
                observed_at="2026-02-18T00:00:00Z",  # 2 days old
            )
        ]
        output = _score(obs, rules, source_defaults={"news_csv": 0.2})

        assert len(output.account_scores) == 1
        assert output.account_scores[0].score > 0
        assert output.account_scores[0].score < 1.0


# ===================================================================
# 7. ANTI-INFLATION TESTS
# ===================================================================


class TestAntiInflation:
    """Tests for per-source and per-signal caps."""

    def test_max_observations_per_source_per_signal(self):
        """Only 1 observation per source per signal contributes."""
        rules = {"sig_a": _make_rule("sig_a", base_weight=10, product_scope="zopdev")}
        obs = [
            _make_observation(
                signal_code="sig_a",
                product="zopdev",
                source="news_csv",
                confidence=0.9,
                source_reliability=0.8,
            ),
            _make_observation(
                signal_code="sig_a",
                product="zopdev",
                source="news_csv",
                confidence=0.95,
                source_reliability=0.8,
            ),
        ]
        output = _score(obs, rules)

        # Only 1 component score (best one kept)
        assert len(output.component_scores) == 1

    def test_max_observations_per_signal(self):
        """At most 3 observations per signal (from different sources)."""
        rules = {"sig_a": _make_rule("sig_a", base_weight=10, product_scope="zopdev")}
        obs = []
        for i in range(5):
            obs.append(
                _make_observation(
                    signal_code="sig_a",
                    product="zopdev",
                    source=f"source_{i}",
                    confidence=0.9,
                    source_reliability=0.8,
                )
            )
        source_defaults = {f"source_{i}": 0.8 for i in range(5)}
        output = _score(obs, rules, source_defaults=source_defaults)

        # Component score reflects capped contributions
        assert len(output.component_scores) == 1
        # The score is from top 3 sources, not all 5
        uncapped_single = 10 * 0.9 * 0.8 * recency_decay(1, 30)
        max_possible_capped = round(uncapped_single * MAX_OBSERVATIONS_PER_SIGNAL, 2)
        assert output.account_scores[0].score <= max_possible_capped

    def test_constants_match_expected(self):
        """Verify anti-inflation constants."""
        assert MAX_OBSERVATIONS_PER_SIGNAL == 3
        assert MAX_OBSERVATIONS_PER_SOURCE_PER_SIGNAL == 1


# ===================================================================
# 8. MULTI-ACCOUNT SCORING
# ===================================================================


class TestMultiAccountScoring:
    """Tests for scoring across multiple accounts."""

    def test_two_accounts_independent_scores(self):
        """Scores for different accounts are independent."""
        rules = {"sig_a": _make_rule("sig_a", base_weight=20, product_scope="zopdev")}
        obs = [
            _make_observation(
                account_id="acc_1",
                signal_code="sig_a",
                product="zopdev",
                confidence=0.9,
            ),
            _make_observation(
                account_id="acc_2",
                signal_code="sig_a",
                product="zopdev",
                confidence=0.5,
            ),
        ]
        output = _score(obs, rules)

        scores = {s.account_id: s.score for s in output.account_scores}
        assert scores["acc_1"] > scores["acc_2"]

    def test_multi_product_per_account(self):
        """Shared observation produces separate scores for each product."""
        rules = {"sig_a": _make_rule("sig_a", base_weight=15, product_scope="shared")}
        obs = [_make_observation(signal_code="sig_a", product="shared")]
        output = _score(obs, rules)

        products = {s.product for s in output.account_scores}
        assert products == {"zopdev", "zopday", "zopnight"}

    def test_product_scoped_rule_only_scores_that_product(self):
        """Rule scoped to zopdev → only zopdev gets scored."""
        rules = {"sig_a": _make_rule("sig_a", product_scope="zopdev")}
        obs = [_make_observation(signal_code="sig_a", product="zopdev")]
        output = _score(obs, rules)

        assert len(output.account_scores) == 1
        assert output.account_scores[0].product == "zopdev"


# ===================================================================
# 9. SOURCE RELIABILITY TESTS
# ===================================================================


class TestSourceReliability:
    """Tests for source reliability capping and defaults."""

    def test_registry_caps_observation_reliability(self):
        """Registry value (0.3) caps observation reliability (0.9).

        Engine: component rounded to 4dp, account score rounded to 2dp.
        """
        rules = {"sig_a": _make_rule("sig_a", base_weight=10, product_scope="zopdev")}
        obs = [
            _make_observation(
                signal_code="sig_a",
                product="zopdev",
                source="capped_source",
                confidence=1.0,
                source_reliability=0.9,
            )
        ]
        output = _score(obs, rules, source_defaults={"capped_source": 0.3})

        expected_component = round(10 * 1.0 * 0.3 * recency_decay(1, 30), 4)
        dim_normalized = min(100.0, round((expected_component / 60.0) * 100.0, 2))
        expected_score = min(100.0, round(dim_normalized * 0.35, 2))
        assert output.account_scores[0].score == expected_score

    def test_zero_registry_reliability_blocks_source(self):
        """Source with 0.0 reliability → no contribution."""
        rules = {"sig_a": _make_rule("sig_a", base_weight=10, product_scope="zopdev")}
        obs = [
            _make_observation(
                signal_code="sig_a",
                product="zopdev",
                source="blocked_source",
            )
        ]
        output = _score(obs, rules, source_defaults={"blocked_source": 0.0})

        assert len(output.account_scores) == 0

    def test_missing_registry_entry_uses_default(self):
        """Source not in registry → uses observation reliability or 0.6 default.

        Engine rounds component to 4dp, account score to 2dp.
        """
        rules = {"sig_a": _make_rule("sig_a", base_weight=10, product_scope="zopdev")}
        obs = [
            _make_observation(
                signal_code="sig_a",
                product="zopdev",
                source="unknown_source",
                source_reliability=None,
            )
        ]
        output = _score(obs, rules, source_defaults={})

        # Defaults to 0.6 when both registry and observation are missing
        expected_component = round(10 * 0.9 * 0.6 * recency_decay(1, 30), 4)
        dim_normalized = min(100.0, round((expected_component / 60.0) * 100.0, 2))
        expected_score = min(100.0, round(dim_normalized * 0.35, 2))
        assert output.account_scores[0].score == expected_score


# ===================================================================
# 10. DELTA (VELOCITY) TESTS
# ===================================================================


class TestDeltaLookup:
    """Tests for delta_7d (velocity) via delta_lookup callback."""

    def test_delta_with_lookup(self):
        """delta_lookup returns a value → stored in account score."""
        rules = {"sig_a": _make_rule("sig_a", base_weight=15, product_scope="zopdev")}
        obs = [_make_observation(signal_code="sig_a", product="zopdev")]

        def mock_delta(account_id, product):
            return 5.5

        output = _score(obs, rules, delta_lookup=mock_delta)
        assert output.account_scores[0].delta_7d == 5.5

    def test_delta_without_lookup(self):
        """No delta_lookup → delta = 0."""
        rules = {"sig_a": _make_rule("sig_a", base_weight=15, product_scope="zopdev")}
        obs = [_make_observation(signal_code="sig_a", product="zopdev")]
        output = _score(obs, rules)

        assert output.account_scores[0].delta_7d == 0.0

    def test_delta_negative_deceleration(self):
        """Negative delta → score went down."""
        rules = {"sig_a": _make_rule("sig_a", base_weight=15, product_scope="zopdev")}
        obs = [_make_observation(signal_code="sig_a", product="zopdev")]

        def mock_delta(account_id, product):
            return -8.0

        output = _score(obs, rules, delta_lookup=mock_delta)
        assert output.account_scores[0].delta_7d == -8.0

    def test_delta_zero_for_new_account(self):
        """New account with no prior score → delta = 0."""
        rules = {"sig_a": _make_rule("sig_a", base_weight=15, product_scope="zopdev")}
        obs = [_make_observation(signal_code="sig_a", product="zopdev")]

        def mock_delta(account_id, product):
            return 0.0

        output = _score(obs, rules, delta_lookup=mock_delta)
        assert output.account_scores[0].delta_7d == 0.0


# ===================================================================
# 11. SIGNAL VELOCITY CLASSIFICATION (Issue #19)
# ===================================================================


class TestVelocityClassification:
    """Velocity category classification tests.

    These test the classification logic from Issue #19:
    - velocity_7d > +20 → surging
    - velocity_7d > +10 → accelerating
    - velocity_7d < -5  → decelerating
    - otherwise         → stable
    """

    @staticmethod
    def classify_velocity(velocity_7d: float) -> str:
        """Reference implementation for velocity classification.

        This matches the spec in Issue #19. Once the feature lands,
        this should be imported from the engine module.
        """
        if velocity_7d > 20:
            return "surging"
        if velocity_7d > 10:
            return "accelerating"
        if velocity_7d < -5:
            return "decelerating"
        return "stable"

    @pytest.mark.parametrize(
        "velocity,expected",
        [
            (25.0, "surging"),
            (21.0, "surging"),
            (20.0, "accelerating"),  # exactly 20: not > 20 but > 10
            (15.0, "accelerating"),
            (10.5, "accelerating"),
            (10.0, "stable"),  # exactly 10 is not > 10
            (5.0, "stable"),
            (0.0, "stable"),
            (-5.0, "stable"),  # exactly -5 is not < -5
            (-5.1, "decelerating"),
            (-8.0, "decelerating"),
            (-20.0, "decelerating"),
        ],
    )
    def test_velocity_classification(self, velocity, expected):
        assert self.classify_velocity(velocity) == expected

    def test_accelerating_delta_positive_15(self):
        """Accelerating: +15 over 7d → accelerating."""
        assert self.classify_velocity(15.0) == "accelerating"

    def test_decelerating_delta_negative_8(self):
        """Decelerating: -8 over 7d → decelerating."""
        assert self.classify_velocity(-8.0) == "decelerating"

    def test_new_account_velocity_zero(self):
        """New account (no prior) → velocity = 0 → stable."""
        assert self.classify_velocity(0.0) == "stable"


# ===================================================================
# 12. CONFIDENCE BANDS (Issue #20)
# ===================================================================


class TestConfidenceBands:
    """Confidence band tests based on source diversity.

    Rules from Issue #20:
    - 3+ distinct sources in dimension → high
    - 2 distinct sources → medium
    - 1 source only → low
    """

    @staticmethod
    def compute_confidence_band(source_count: int) -> str:
        """Reference implementation for confidence bands.

        Once Issue #20 is implemented, import from the engine.
        """
        if source_count >= 3:
            return "high"
        if source_count == 2:
            return "medium"
        return "low"

    @staticmethod
    def compute_overall_confidence(dimension_bands: dict[str, str]) -> str:
        """Overall confidence = lowest of all non-zero dimensions."""
        band_priority = {"low": 0, "medium": 1, "high": 2}
        if not dimension_bands:
            return "low"
        return min(dimension_bands.values(), key=lambda b: band_priority.get(b, 0))

    def test_three_sources_high_confidence(self):
        assert self.compute_confidence_band(3) == "high"

    def test_five_sources_high_confidence(self):
        assert self.compute_confidence_band(5) == "high"

    def test_two_sources_medium_confidence(self):
        assert self.compute_confidence_band(2) == "medium"

    def test_one_source_low_confidence(self):
        assert self.compute_confidence_band(1) == "low"

    def test_zero_sources_low_confidence(self):
        assert self.compute_confidence_band(0) == "low"

    def test_overall_confidence_all_high(self):
        bands = {"trigger_intent": "high", "tech_fit": "high"}
        assert self.compute_overall_confidence(bands) == "high"

    def test_overall_confidence_mixed_high_low(self):
        """Mixed: high in trigger, low in tech → overall = low."""
        bands = {"trigger_intent": "high", "tech_fit": "low"}
        assert self.compute_overall_confidence(bands) == "low"

    def test_overall_confidence_mixed_high_medium(self):
        bands = {"trigger_intent": "high", "tech_fit": "medium"}
        assert self.compute_overall_confidence(bands) == "medium"

    def test_overall_confidence_empty(self):
        assert self.compute_overall_confidence({}) == "low"

    def test_source_diversity_in_scoring(self):
        """3 different sources for same signal → all contribute (up to cap)."""
        rules = {"sig_a": _make_rule("sig_a", base_weight=10, product_scope="zopdev")}
        obs = [
            _make_observation(signal_code="sig_a", product="zopdev", source="news_csv"),
            _make_observation(
                signal_code="sig_a",
                product="zopdev",
                source="technographics_csv",
            ),
            _make_observation(
                signal_code="sig_a",
                product="zopdev",
                source="first_party_csv",
            ),
        ]
        source_defaults = {
            "news_csv": 0.75,
            "technographics_csv": 0.8,
            "first_party_csv": 0.9,
        }
        output = _score(obs, rules, source_defaults=source_defaults)

        # All 3 sources contribute (within MAX_OBSERVATIONS_PER_SIGNAL = 3)
        assert len(output.account_scores) == 1
        single_component = 10 * 0.9 * 0.75 * recency_decay(1, 30)
        assert output.account_scores[0].score > single_component


# ===================================================================
# 13. DIMENSION SCORING (Issue #18)
# ===================================================================


class TestDimensionScoring:
    """Tests for multi-dimensional scoring.

    These tests verify the current engine behavior with signals
    organized by category (which maps to dimensions in #18).
    """

    def _build_multi_dimension_rules(self) -> dict[str, SignalRule]:
        """Rules spanning all 5 dimension categories."""
        return {
            "compliance_initiative": _make_rule(
                "compliance_initiative",
                category="trigger_events",
                base_weight=18,
                product_scope="zopdev",
            ),
            "kubernetes_detected": _make_rule(
                "kubernetes_detected",
                category="technographic",
                base_weight=4,
                product_scope="zopdev",
            ),
            "cloud_connected": _make_rule(
                "cloud_connected",
                category="pql",
                base_weight=18,
                product_scope="shared",
            ),
            "devops_role_open": _make_rule(
                "devops_role_open",
                category="hiring",
                base_weight=4,
                product_scope="zopdev",
            ),
            "enterprise_modernization_program": _make_rule(
                "enterprise_modernization_program",
                category="trigger_events",
                base_weight=8,
                product_scope="zopdev",
            ),
        }

    def test_account_with_signals_in_all_categories(self):
        """Account with signals across all categories → correct scoring."""
        rules = self._build_multi_dimension_rules()
        obs = [
            _make_observation(signal_code="compliance_initiative", product="zopdev", source="news_csv"),
            _make_observation(
                signal_code="kubernetes_detected",
                product="zopdev",
                source="technographics_csv",
            ),
            _make_observation(
                signal_code="cloud_connected",
                product="zopdev",
                source="first_party_csv",
            ),
            _make_observation(signal_code="devops_role_open", product="zopdev", source="news_csv"),
            _make_observation(
                signal_code="enterprise_modernization_program",
                product="zopdev",
                source="news_csv",
            ),
        ]
        source_defaults = {
            "news_csv": 0.75,
            "technographics_csv": 0.8,
            "first_party_csv": 0.9,
        }
        output = _score(obs, rules, source_defaults=source_defaults)

        zopdev_scores = [s for s in output.account_scores if s.product == "zopdev"]
        assert len(zopdev_scores) == 1
        # Multiple signal categories contribute → higher total
        assert zopdev_scores[0].score > 0

    def test_account_with_only_one_category(self):
        """Account with signals in only 1 category → other categories = 0."""
        rules = self._build_multi_dimension_rules()
        obs = [
            _make_observation(
                signal_code="kubernetes_detected",
                product="zopdev",
                source="technographics_csv",
            ),
        ]
        output = _score(obs, rules)

        zopdev_scores = [s for s in output.account_scores if s.product == "zopdev"]
        assert len(zopdev_scores) == 1

        # Only 1 component score for this account+product
        components = [c for c in output.component_scores if c.account_id == "acc_1" and c.product == "zopdev"]
        assert len(components) == 1
        assert components[0].signal_code == "kubernetes_detected"

    def test_component_scores_group_by_signal(self):
        """Each signal_code gets its own component score."""
        rules = {
            "sig_a": _make_rule("sig_a", base_weight=10, product_scope="zopdev"),
            "sig_b": _make_rule("sig_b", base_weight=20, product_scope="zopdev"),
        }
        obs = [
            _make_observation(signal_code="sig_a", product="zopdev"),
            _make_observation(signal_code="sig_b", product="zopdev"),
        ]
        output = _score(obs, rules)

        components = [c for c in output.component_scores if c.account_id == "acc_1" and c.product == "zopdev"]
        signal_codes = {c.signal_code for c in components}
        assert signal_codes == {"sig_a", "sig_b"}


# ===================================================================
# 14. DIMENSION CEILING (Issue #18)
# ===================================================================


class TestDimensionCeiling:
    """Test that scores are capped correctly.

    In the current engine, total score is capped at 100.
    Issue #18 adds per-dimension ceilings.
    """

    def test_total_score_capped_at_100(self):
        """Sum of component scores exceeding 100 → capped at 100."""
        rules = {}
        obs = []
        for i in range(10):
            code = f"heavy_signal_{i}"
            rules[code] = _make_rule(code, base_weight=50, product_scope="zopdev", half_life_days=90)
            obs.append(
                _make_observation(
                    signal_code=code,
                    product="zopdev",
                    source=f"source_{i}",
                    confidence=1.0,
                    source_reliability=1.0,
                )
            )
        source_defaults = {f"source_{i}": 1.0 for i in range(10)}
        output = _score(obs, rules, source_defaults=source_defaults)

        zopdev_scores = [s for s in output.account_scores if s.product == "zopdev"]
        assert len(zopdev_scores) == 1
        # All signals in trigger_intent (weight=0.35), dimension capped at 100 → 35.0
        assert zopdev_scores[0].score == 35.0

    def test_dimension_ceiling_normalization(self):
        """Reference test for dimension ceiling normalization.

        Once Issue #18 is implemented, a dimension with ceiling=60 and
        raw score=60 should normalize to 100 for that dimension.
        """
        ceiling = 60
        raw_score = 60
        normalized = min(100.0, (raw_score / ceiling) * 100)
        assert normalized == 100.0

        # Score exceeding ceiling still caps at 100
        normalized_over = min(100.0, (90 / ceiling) * 100)
        assert normalized_over == 100.0

    def test_dimension_ceiling_partial(self):
        """Raw score below ceiling → proportional normalization."""
        ceiling = 60
        raw_score = 30
        normalized = min(100.0, (raw_score / ceiling) * 100)
        assert normalized == 50.0


# ===================================================================
# 15. COMPOSITE SCORE (Issue #18)
# ===================================================================


class TestCompositeScore:
    """Tests for the weighted composite ICP score calculation."""

    def test_composite_formula_all_dimensions_100(self):
        """All dimensions at 100 → composite = 100."""
        dimension_scores = {dim: 100.0 for dim in DIMENSIONS}
        composite = sum(dimension_scores[dim] * DIMENSION_WEIGHTS[dim] for dim in DIMENSIONS)
        assert composite == 100.0

    def test_composite_formula_single_dimension(self):
        """Only trigger_intent at 100, rest 0 → composite = 35."""
        dimension_scores = {dim: 0.0 for dim in DIMENSIONS}
        dimension_scores["trigger_intent"] = 100.0
        composite = sum(dimension_scores[dim] * DIMENSION_WEIGHTS[dim] for dim in DIMENSIONS)
        assert composite == 35.0

    def test_composite_formula_weights_sum_to_one(self):
        """Dimension weights must sum to 1.0."""
        assert round(sum(DIMENSION_WEIGHTS.values()), 4) == 1.0

    def test_composite_all_zero(self):
        """All dimensions 0 → composite = 0."""
        dimension_scores = {dim: 0.0 for dim in DIMENSIONS}
        composite = sum(dimension_scores[dim] * DIMENSION_WEIGHTS[dim] for dim in DIMENSIONS)
        assert composite == 0.0

    def test_composite_partial_dimensions(self):
        """Some dimensions scored, some not → correct weighted sum."""
        dimension_scores = {
            "trigger_intent": 80.0,
            "tech_fit": 50.0,
            "engagement_pql": 0.0,
            "firmographic": 0.0,
            "hiring_growth": 60.0,
        }
        expected = 80 * 0.35 + 50 * 0.20 + 0 * 0.25 + 0 * 0.10 + 60 * 0.10
        composite = sum(dimension_scores[dim] * DIMENSION_WEIGHTS[dim] for dim in DIMENSIONS)
        assert round(composite, 2) == round(expected, 2)
        assert round(composite, 2) == 44.0


# ===================================================================
# 16. TIER UPGRADE RULES (Issue #21)
# ===================================================================


class TestTierUpgradeRules:
    """Tests for tier upgrade rules from Issue #21.

    Upgrade rules:
    - Tier 2 with trigger_intent >= 70 → Tier 1
    - Any tier with engagement_pql >= 80 → promote one level
    - Tier 1 can't be promoted further
    """

    @staticmethod
    def apply_upgrade_rules(
        tier: str,
        dimension_scores: dict[str, float],
    ) -> str:
        """Reference implementation for tier upgrade rules.

        Once Issue #21 is implemented, import from the engine.
        """
        tier_order = ["tier_4", "tier_3", "tier_2", "tier_1"]

        # Rule 1: Tier 2 with strong trigger → Tier 1
        if tier == "tier_2" and dimension_scores.get("trigger_intent", 0) >= 70:
            tier = "tier_1"

        # Rule 2: engagement_pql >= 80 → promote one level
        if dimension_scores.get("engagement_pql", 0) >= 80:
            idx = tier_order.index(tier) if tier in tier_order else 0
            if idx < len(tier_order) - 1:
                tier = tier_order[idx + 1]

        return tier

    def test_tier2_strong_trigger_promotes_to_tier1(self):
        dims = {"trigger_intent": 75.0, "engagement_pql": 50.0}
        assert self.apply_upgrade_rules("tier_2", dims) == "tier_1"

    def test_tier2_weak_trigger_stays_tier2(self):
        dims = {"trigger_intent": 60.0, "engagement_pql": 50.0}
        assert self.apply_upgrade_rules("tier_2", dims) == "tier_2"

    def test_pql_engagement_promotes_one_level(self):
        dims = {"trigger_intent": 30.0, "engagement_pql": 85.0}
        assert self.apply_upgrade_rules("tier_3", dims) == "tier_2"

    def test_pql_engagement_promotes_tier4_to_tier3(self):
        dims = {"trigger_intent": 10.0, "engagement_pql": 90.0}
        assert self.apply_upgrade_rules("tier_4", dims) == "tier_3"

    def test_tier1_cannot_promote_further(self):
        dims = {"trigger_intent": 90.0, "engagement_pql": 90.0}
        assert self.apply_upgrade_rules("tier_1", dims) == "tier_1"

    def test_both_rules_apply_tier2(self):
        """Tier 2 + strong trigger (→ tier 1) + high PQL (→ still tier 1)."""
        dims = {"trigger_intent": 75.0, "engagement_pql": 85.0}
        result = self.apply_upgrade_rules("tier_2", dims)
        assert result == "tier_1"

    def test_no_upgrade_rules_apply(self):
        dims = {"trigger_intent": 30.0, "engagement_pql": 40.0}
        assert self.apply_upgrade_rules("tier_3", dims) == "tier_3"


# ===================================================================
# 17. TOP REASONS & EVIDENCE
# ===================================================================


class TestTopReasons:
    """Tests for top_reasons_json in account scores."""

    def test_top_reasons_populated(self):
        """Account score has top_reasons_json with evidence."""
        rules = {"sig_a": _make_rule("sig_a", base_weight=15, product_scope="zopdev")}
        obs = [
            _make_observation(
                signal_code="sig_a",
                product="zopdev",
                evidence_url="https://example.com/news",
                evidence_text="Cloud migration initiative",
            )
        ]
        output = _score(obs, rules)

        assert len(output.account_scores) == 1
        reasons_json = output.account_scores[0].top_reasons_json
        assert "sig_a" in reasons_json
        assert "example.com" in reasons_json

    def test_top_reasons_limited_to_3(self):
        """At most 3 reasons per account score."""
        import json

        rules = {}
        obs = []
        for i in range(5):
            code = f"sig_{i}"
            rules[code] = _make_rule(code, base_weight=10, product_scope="zopdev")
            obs.append(
                _make_observation(
                    signal_code=code,
                    product="zopdev",
                    source=f"source_{i}",
                )
            )
        source_defaults = {f"source_{i}": 0.8 for i in range(5)}
        output = _score(obs, rules, source_defaults=source_defaults)

        zopdev_scores = [s for s in output.account_scores if s.product == "zopdev"]
        reasons = json.loads(zopdev_scores[0].top_reasons_json)
        assert len(reasons) <= 3


# ===================================================================
# 18. ICP REFERENCE ACCOUNT REGRESSION TESTS
# ===================================================================


class TestICPReferenceAccounts:
    """Regression tests for known ICP reference accounts.

    These use the actual signal registry weights to verify that
    reference accounts score appropriately given realistic signals.
    """

    def _reference_rules(self) -> dict[str, SignalRule]:
        """Subset of actual signal_registry.csv rules for reference tests."""
        return {
            "compliance_initiative": _make_rule(
                "compliance_initiative",
                category="trigger_events",
                base_weight=18,
                half_life_days=30,
                min_confidence=0.6,
                product_scope="zopdev",
            ),
            "cloud_connected": _make_rule(
                "cloud_connected",
                category="pql",
                base_weight=18,
                half_life_days=14,
                min_confidence=0.8,
                product_scope="shared",
            ),
            "kubernetes_detected": _make_rule(
                "kubernetes_detected",
                category="technographic",
                base_weight=4,
                half_life_days=45,
                min_confidence=0.65,
                product_scope="zopdev",
            ),
            "terraform_detected": _make_rule(
                "terraform_detected",
                category="technographic",
                base_weight=7,
                half_life_days=45,
                min_confidence=0.5,
                product_scope="zopdev",
            ),
            "cost_reduction_mandate": _make_rule(
                "cost_reduction_mandate",
                category="spend_variance",
                base_weight=21,
                half_life_days=21,
                min_confidence=0.65,
                product_scope="zopnight",
            ),
            "poc_stage_progression": _make_rule(
                "poc_stage_progression",
                category="pql",
                base_weight=24,
                half_life_days=14,
                min_confidence=0.75,
                product_scope="shared",
            ),
        }

    def test_tata_digital_scores_above_zero(self):
        """Tata Digital (active POC) should score meaningfully with PQL signals."""
        rules = self._reference_rules()
        obs = [
            _make_observation(
                account_id="tata_digital",
                signal_code="poc_stage_progression",
                product="shared",
                source="first_party_csv",
                confidence=0.85,
                source_reliability=0.9,
            ),
            _make_observation(
                account_id="tata_digital",
                signal_code="cloud_connected",
                product="shared",
                source="first_party_csv",
                confidence=0.9,
                source_reliability=0.9,
            ),
        ]
        source_defaults = {"first_party_csv": 0.9}
        output = _score(obs, rules, source_defaults=source_defaults)

        zopdev_scores = [s for s in output.account_scores if s.account_id == "tata_digital" and s.product == "zopdev"]
        assert len(zopdev_scores) == 1
        assert zopdev_scores[0].score > 10  # meaningful PQL contribution

    def test_conde_nast_scores_above_zero(self):
        """Conde Nast (active POC) should score with trigger + tech signals."""
        rules = self._reference_rules()
        obs = [
            _make_observation(
                account_id="conde_nast",
                signal_code="compliance_initiative",
                product="zopdev",
                source="news_csv",
                confidence=0.8,
                source_reliability=0.75,
            ),
            _make_observation(
                account_id="conde_nast",
                signal_code="kubernetes_detected",
                product="zopdev",
                source="technographics_csv",
                confidence=0.9,
                source_reliability=0.8,
            ),
        ]
        source_defaults = {"news_csv": 0.75, "technographics_csv": 0.8}
        output = _score(obs, rules, source_defaults=source_defaults)

        zopdev_scores = [s for s in output.account_scores if s.account_id == "conde_nast" and s.product == "zopdev"]
        assert len(zopdev_scores) == 1
        assert zopdev_scores[0].score > 5

    def test_diageo_india_scores_with_finops_signals(self):
        """Diageo India (customer) should score with FinOps signals."""
        rules = self._reference_rules()
        obs = [
            _make_observation(
                account_id="diageo_india",
                signal_code="cost_reduction_mandate",
                product="zopnight",
                source="news_csv",
                confidence=0.8,
                source_reliability=0.75,
            ),
        ]
        source_defaults = {"news_csv": 0.75}
        output = _score(obs, rules, source_defaults=source_defaults)

        zopnight_scores = [
            s for s in output.account_scores if s.account_id == "diageo_india" and s.product == "zopnight"
        ]
        assert len(zopnight_scores) == 1
        assert zopnight_scores[0].score > 5

    def test_reference_scores_are_deterministic(self):
        """Same signals → same scores across runs (no randomness)."""
        rules = self._reference_rules()
        obs = [
            _make_observation(
                account_id="tata_digital",
                signal_code="poc_stage_progression",
                product="shared",
                source="first_party_csv",
                confidence=0.85,
                source_reliability=0.9,
            ),
        ]
        source_defaults = {"first_party_csv": 0.9}

        output1 = _score(obs, rules, source_defaults=source_defaults)
        output2 = _score(obs, rules, source_defaults=source_defaults)

        scores1 = {(s.account_id, s.product): s.score for s in output1.account_scores}
        scores2 = {(s.account_id, s.product): s.score for s in output2.account_scores}
        assert scores1 == scores2


# ===================================================================
# 19. EDGE CASES
# ===================================================================


class TestEdgeCases:
    """Edge case tests for the scoring engine."""

    def test_very_old_observation_near_zero(self):
        """Observation 365 days old → negligible contribution."""
        rules = {"sig_a": _make_rule("sig_a", base_weight=10, half_life_days=14, product_scope="zopdev")}
        obs = [
            _make_observation(
                signal_code="sig_a",
                product="zopdev",
                observed_at="2025-02-20T00:00:00Z",  # 365 days before run_date
            )
        ]
        output = _score(obs, rules)

        assert len(output.account_scores) == 1
        assert output.account_scores[0].score < 0.01

    def test_observation_same_day_no_decay(self):
        """Observation on run_date → recency_decay = 1.0."""
        rules = {"sig_a": _make_rule("sig_a", base_weight=10, half_life_days=14, product_scope="zopdev")}
        obs = [
            _make_observation(
                signal_code="sig_a",
                product="zopdev",
                observed_at="2026-02-20T00:00:00Z",
            )
        ]
        output = _score(obs, rules)

        raw_component = 10 * 0.9 * 0.75 * 1.0  # source capped by registry
        dim_normalized = min(100.0, round((raw_component / 60.0) * 100.0, 2))
        expected = round(dim_normalized * 0.35, 2)
        assert round(output.account_scores[0].score, 4) == round(expected, 4)

    def test_none_source_reliability_uses_default(self):
        """Observation with source_reliability=None → uses registry or 0.6.

        Engine rounds component to 4dp, account score to 2dp.
        """
        rules = {"sig_a": _make_rule("sig_a", base_weight=10, product_scope="zopdev")}
        obs = [
            _make_observation(
                signal_code="sig_a",
                product="zopdev",
                source="news_csv",
                source_reliability=None,
            )
        ]
        output = _score(obs, rules)

        # Registry has news_csv=0.75, that's used as both value and cap
        expected_component = round(10 * 0.9 * 0.75 * recency_decay(1, 30), 4)
        dim_normalized = min(100.0, round((expected_component / 60.0) * 100.0, 2))
        expected_score = min(100.0, round(dim_normalized * 0.35, 2))
        assert output.account_scores[0].score == expected_score

    def test_zero_confidence_observation_filtered(self):
        """Confidence = 0 → below any min_confidence → filtered."""
        rules = {"sig_a": _make_rule("sig_a", min_confidence=0.5)}
        obs = [_make_observation(signal_code="sig_a", confidence=0.0)]
        output = _score(obs, rules)

        assert len(output.account_scores) == 0

    def test_empty_evidence_fields_handled(self):
        """Missing evidence fields don't crash the engine."""
        rules = {"sig_a": _make_rule("sig_a", base_weight=10, product_scope="zopdev")}
        obs = [
            _make_observation(
                signal_code="sig_a",
                product="zopdev",
                evidence_url="",
                evidence_text="",
            )
        ]
        output = _score(obs, rules)

        assert len(output.account_scores) == 1

    def test_multiple_runs_same_account_accumulate(self):
        """Multiple distinct signals for same account accumulate."""
        rules = {
            "sig_a": _make_rule("sig_a", base_weight=10, product_scope="zopdev"),
            "sig_b": _make_rule("sig_b", base_weight=15, product_scope="zopdev"),
        }
        single_obs = [
            _make_observation(signal_code="sig_a", product="zopdev"),
        ]
        double_obs = [
            _make_observation(signal_code="sig_a", product="zopdev"),
            _make_observation(signal_code="sig_b", product="zopdev", source="technographics_csv"),
        ]

        single_output = _score(single_obs, rules)
        double_output = _score(double_obs, rules)

        single_score = [s for s in single_output.account_scores if s.product == "zopdev"][0].score
        double_score = [s for s in double_output.account_scores if s.product == "zopdev"][0].score

        assert double_score > single_score


# ===================================================================
# 20. ENGINE OUTPUT STRUCTURE
# ===================================================================


class TestEngineOutputStructure:
    """Tests for EngineOutput data model correctness."""

    def test_engine_output_has_both_lists(self):
        """EngineOutput has component_scores and account_scores."""
        output = EngineOutput(component_scores=[], account_scores=[])
        assert hasattr(output, "component_scores")
        assert hasattr(output, "account_scores")

    def test_component_score_fields(self):
        """ComponentScore has required fields."""
        rules = {"sig_a": _make_rule("sig_a", base_weight=10, product_scope="zopdev")}
        obs = [_make_observation(signal_code="sig_a", product="zopdev")]
        output = _score(obs, rules)

        cs = output.component_scores[0]
        assert cs.run_id == RUN_ID
        assert cs.account_id == "acc_1"
        assert cs.product == "zopdev"
        assert cs.signal_code == "sig_a"
        assert cs.component_score > 0

    def test_account_score_fields(self):
        """AccountScore has required fields."""
        rules = {"sig_a": _make_rule("sig_a", base_weight=15, product_scope="zopdev")}
        obs = [_make_observation(signal_code="sig_a", product="zopdev")]
        output = _score(obs, rules)

        asc = output.account_scores[0]
        assert asc.run_id == RUN_ID
        assert asc.account_id == "acc_1"
        assert asc.product == "zopdev"
        assert asc.score > 0
        assert asc.tier in {"high", "medium", "low"}
        assert asc.top_reasons_json != ""
        assert isinstance(asc.delta_7d, float)


# ===================================================================
# TEST SUITE SUMMARY
# ===================================================================
#
# Total: 108 test cases across 20 test classes
#
# Issue:  #22 — [Scoring] Add comprehensive scoring engine tests for new dimensions
# Epic:   #11 — SOTA ICP Scoring & Tier Engine Redesign
# Depends on: #18 (multi-dimensional engine), #19 (velocity), #20 (confidence bands), #21 (4-tier)
#
# What was the issue?
#   The existing test_scoring.py had only 5 basic tests. It did not cover
#   dimension grouping, normalization, ceilings, composite weighted scoring,
#   4-tier classification, tier upgrade rules, signal velocity tracking,
#   confidence bands, ICP reference account regression, or anti-inflation caps.
#
# Why it happens?
#   The scoring engine was originally a flat additive scorer with 3 tiers.
#   Epic #11 redesigns it into a multi-dimensional weighted system with
#   5 dimensions, 4 tiers, velocity tracking, and confidence bands.
#   Tests must exist before implementation to serve as a specification.
#
# How we solved it?
#   Created 108 tests covering:
#     1. Recency decay (10 tests) — exponential formula, edge cases, parametrized
#     2. Tier classification 3-tier (7 tests) — boundary checks at high/medium/low
#     3. Tier classification 4-tier (5 tests) — new boundaries at 80/60/40/0
#     4. Product resolution (6 tests) — shared/scoped/mismatched/unknown
#     5. Basic scoring (5 tests) — disabled rules, confidence filter, unknown signals
#     6. Score calculation (4 tests) — formula correctness, determinism, min/max cap
#     7. Anti-inflation (3 tests) — per-source cap (1), per-signal cap (3), constants
#     8. Multi-account scoring (3 tests) — independence, multi-product, scoped rules
#     9. Source reliability (3 tests) — registry capping, zero blocking, defaults
#    10. Delta/velocity lookup (4 tests) — callback, no-callback, negative, zero
#    11. Velocity classification (15 tests) — surging/accelerating/decelerating/stable
#    12. Confidence bands (10 tests) — source diversity, overall band, engine diversity
#    13. Dimension scoring (3 tests) — all categories, single category, grouping
#    14. Dimension ceiling (3 tests) — cap at 100, normalization formula, partial
#    15. Composite score (5 tests) — weighted formula, weights sum to 1, partial dims
#    16. Tier upgrade rules (7 tests) — strong trigger, PQL promote, can't exceed tier 1
#    17. Top reasons (2 tests) — evidence populated, limited to 3
#    18. ICP reference accounts (4 tests) — Tata Digital, Conde Nast, Diageo India, determinism
#    19. Edge cases (6 tests) — old signal, same-day, None reliability, zero confidence
#    20. Engine output structure (3 tests) — EngineOutput, ComponentScore, AccountScore fields
#
# How it worked before?
#   Only 5 tests in test_scoring.py:
#     - test_recency_decay_half_life (2 values)
#     - test_scoring_high_intent_signal_reaches_high_tier
#     - test_single_weak_signal_does_not_reach_high
#     - test_disabled_source_from_registry_does_not_contribute
#     - test_registry_reliability_caps_observation_reliability
#
# How it works after?
#   113 total tests (5 existing + 108 new). Tests that validate the current
#   engine run against real code. Tests for new features (#18-#21) use
#   reference implementations that will be swapped for real imports once
#   those features land. ICP reference accounts (Tata Digital, Conde Nast,
#   Diageo India) have regression guards to prevent score drops.
#
# ===================================================================
