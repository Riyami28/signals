from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal

from src.utils import load_csv_rows

logger = logging.getLogger(__name__)

VALID_DIMENSIONS = frozenset(
    {
        "trigger_intent",
        "tech_fit",
        "engagement_pql",
        "firmographic",
        "hiring_growth",
    }
)

TIER_ORDER = ("tier_1", "tier_2", "tier_3", "tier_4")

VelocityCategory = Literal["surging", "accelerating", "stable", "decelerating"]


@dataclass(frozen=True)
class SignalRule:
    signal_code: str
    product_scope: str
    category: str
    base_weight: float
    half_life_days: float
    min_confidence: float
    enabled: bool
    dimension: str = "trigger_intent"


@dataclass(frozen=True)
class DimensionWeight:
    dimension: str
    weight: float
    ceiling: float = 100.0


@dataclass(frozen=True)
class TierUpgradeRule:
    rule_name: str
    condition_dimension: str
    condition_threshold: float
    current_tier: str
    promote_to_tier: str


@dataclass(frozen=True)
class Thresholds:
    tier_1: float = 80.0
    tier_2: float = 60.0
    tier_3: float = 40.0
    tier_4: float = 0.0
    upgrade_rules: tuple[TierUpgradeRule, ...] = field(default_factory=tuple)

    # Backward-compat aliases for legacy 3-tier code (calibration, evals, improvement).
    @property
    def high(self) -> float:
        return self.tier_1

    @property
    def medium(self) -> float:
        return self.tier_2

    @property
    def low(self) -> float:
        return self.tier_4


def _to_bool(value: str | None) -> bool:
    if value is None:
        return False
    return value.strip().lower() in {"1", "true", "yes", "on"}


def legacy_tier_from_v2(tier: str) -> str:
    """Map v2 4-tier names back to legacy high/medium/low."""
    mapping = {
        "tier_1": "high",
        "tier_2": "high",
        "tier_3": "medium",
        "tier_4": "low",
    }
    return mapping.get(tier, "low")


def load_signal_rules(path: Path) -> dict[str, SignalRule]:
    rows = load_csv_rows(path)
    rules: dict[str, SignalRule] = {}
    for row in rows:
        signal_code = row.get("signal_code", "").strip()
        if not signal_code:
            continue
        try:
            dimension = (row.get("dimension", "trigger_intent") or "trigger_intent").strip()
            if dimension not in VALID_DIMENSIONS:
                logger.warning(
                    "invalid_dimension signal=%s dimension=%s, defaulting to trigger_intent",
                    signal_code,
                    dimension,
                )
                dimension = "trigger_intent"
            rule = SignalRule(
                signal_code=signal_code,
                product_scope=(row.get("product_scope", "shared") or "shared").strip(),
                category=(row.get("category", "uncategorized") or "uncategorized").strip(),
                base_weight=float(row.get("base_weight", "0") or 0),
                half_life_days=max(1.0, float(row.get("half_life_days", "14") or 14)),
                min_confidence=float(row.get("min_confidence", "0") or 0),
                enabled=_to_bool(row.get("enabled", "true")),
                dimension=dimension,
            )
        except ValueError:
            continue
        rules[signal_code] = rule
    return rules


def load_source_registry(path: Path) -> dict[str, float]:
    rows = load_csv_rows(path)
    registry: dict[str, float] = {}
    for row in rows:
        source = row.get("source", "").strip()
        if not source:
            continue
        try:
            reliability = float(row.get("reliability", "0.6") or 0.6)
        except ValueError:
            reliability = 0.6
        enabled = _to_bool(row.get("enabled", "true"))
        registry[source] = max(0.0, min(1.0, reliability)) if enabled else 0.0
    return registry


def load_thresholds(path: Path) -> Thresholds:
    rows = load_csv_rows(path)
    values = {row.get("key", "").strip().lower(): row.get("value", "") for row in rows}

    def _parse(key: str, default: float) -> float:
        raw = values.get(key, str(default))
        try:
            return float(raw)
        except (TypeError, ValueError):
            return default

    upgrade_rules = _load_tier_upgrade_rules(path.parent / "tier_upgrade_rules.csv")
    return Thresholds(
        tier_1=_parse("tier_1", 80.0),
        tier_2=_parse("tier_2", 60.0),
        tier_3=_parse("tier_3", 40.0),
        tier_4=_parse("tier_4", 0.0),
        upgrade_rules=upgrade_rules,
    )


def _load_tier_upgrade_rules(path: Path) -> tuple[TierUpgradeRule, ...]:
    if not path.exists():
        return ()
    rows = load_csv_rows(path)
    rules: list[TierUpgradeRule] = []
    for row in rows:
        rule_name = row.get("rule_name", "").strip()
        if not rule_name:
            continue
        try:
            rules.append(
                TierUpgradeRule(
                    rule_name=rule_name,
                    condition_dimension=row.get("condition_dimension", "").strip(),
                    condition_threshold=float(row.get("condition_threshold", "0") or 0),
                    current_tier=row.get("current_tier", "*").strip(),
                    promote_to_tier=row.get("promote_to_tier", "+1").strip(),
                )
            )
        except (ValueError, TypeError):
            continue
    return tuple(rules)


def load_dimension_weights(path: Path) -> dict[str, DimensionWeight]:
    if not path.exists():
        return {}
    rows = load_csv_rows(path)
    weights: dict[str, DimensionWeight] = {}
    for row in rows:
        dimension = row.get("dimension", "").strip()
        if not dimension or dimension not in VALID_DIMENSIONS:
            continue
        try:
            weights[dimension] = DimensionWeight(
                dimension=dimension,
                weight=float(row.get("weight", "0") or 0),
                ceiling=float(row.get("ceiling", "100") or 100),
            )
        except (ValueError, TypeError):
            continue
    return weights


def load_keyword_lexicon(path: Path) -> dict[str, list[dict[str, str]]]:
    rows = load_csv_rows(path)
    grouped: dict[str, list[dict[str, str]]] = {}
    for row in rows:
        source = row.get("source", "").strip().lower()
        if not source:
            continue
        grouped.setdefault(source, []).append(row)
    return grouped
