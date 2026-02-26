from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from src.utils import load_csv_rows

_VALID_BANDS = {"high", "medium", "explore"}
_VALID_CONFIDENCE_BANDS = {"high", "medium", "low"}


@dataclass(frozen=True)
class PromotionPolicy:
    auto_push_bands: set[str]
    manual_review_bands: set[str]
    require_strict_evidence_for_auto_push: bool
    min_auto_push_evidence_quality: float
    min_auto_push_relevance_score: float
    confidence_auto_push_bands: set[str]
    confidence_manual_review_bands: set[str]


def default_promotion_policy() -> PromotionPolicy:
    return PromotionPolicy(
        auto_push_bands={"high"},
        manual_review_bands={"medium"},
        require_strict_evidence_for_auto_push=True,
        min_auto_push_evidence_quality=0.8,
        min_auto_push_relevance_score=0.65,
        confidence_auto_push_bands={"high"},
        confidence_manual_review_bands={"medium"},
    )


def is_promotion_eligible(
    tier: str,
    confidence_band: str,
    policy: PromotionPolicy,
) -> str:
    """Return promotion status: 'auto_push', 'manual_review', or 'blocked'.

    Low-confidence accounts are blocked from promotion regardless of score/tier.
    """
    if confidence_band in policy.confidence_auto_push_bands:
        if tier == "high":
            return "auto_push"
        return "manual_review"
    if confidence_band in policy.confidence_manual_review_bands:
        return "manual_review"
    return "blocked"


def _to_bool(value: str | None, default: bool) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _to_float(value: str | None, default: float) -> float:
    raw = (value or "").strip()
    if not raw:
        return default
    try:
        parsed = float(raw)
    except ValueError:
        return default
    return max(0.0, min(1.0, parsed))


def _parse_bands(value: str | None, default: set[str]) -> set[str]:
    raw = (value or "").strip()
    if not raw:
        return set(default)
    values = {token.strip().lower() for token in raw.split("|") if token.strip()}
    valid = {token for token in values if token in _VALID_BANDS}
    return valid if valid else set(default)


def _parse_confidence_bands(value: str | None, default: set[str]) -> set[str]:
    raw = (value or "").strip()
    if not raw:
        return set(default)
    values = {token.strip().lower() for token in raw.split("|") if token.strip()}
    valid = {token for token in values if token in _VALID_CONFIDENCE_BANDS}
    return valid if valid else set(default)


def load_promotion_policy(path: Path) -> PromotionPolicy:
    defaults = default_promotion_policy()
    rows = load_csv_rows(path)
    if not rows:
        return defaults

    kv: dict[str, str] = {}
    for row in rows:
        key = (row.get("key", "") or "").strip().lower()
        if not key:
            continue
        kv[key] = (row.get("value", "") or "").strip()

    return PromotionPolicy(
        auto_push_bands=_parse_bands(kv.get("auto_push_bands"), defaults.auto_push_bands),
        manual_review_bands=_parse_bands(kv.get("manual_review_bands"), defaults.manual_review_bands),
        require_strict_evidence_for_auto_push=_to_bool(
            kv.get("require_strict_evidence_for_auto_push"),
            defaults.require_strict_evidence_for_auto_push,
        ),
        min_auto_push_evidence_quality=_to_float(
            kv.get("min_auto_push_evidence_quality"),
            defaults.min_auto_push_evidence_quality,
        ),
        min_auto_push_relevance_score=_to_float(
            kv.get("min_auto_push_relevance_score"),
            defaults.min_auto_push_relevance_score,
        ),
        confidence_auto_push_bands=_parse_confidence_bands(
            kv.get("confidence_auto_push_bands"),
            defaults.confidence_auto_push_bands,
        ),
        confidence_manual_review_bands=_parse_confidence_bands(
            kv.get("confidence_manual_review_bands"),
            defaults.confidence_manual_review_bands,
        ),
    )
