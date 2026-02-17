from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from src.utils import load_csv_rows


@dataclass(frozen=True)
class SignalRule:
    signal_code: str
    product_scope: str
    category: str
    base_weight: float
    half_life_days: float
    min_confidence: float
    enabled: bool


@dataclass(frozen=True)
class Thresholds:
    high: float
    medium: float
    low: float


def _to_bool(value: str | None) -> bool:
    if value is None:
        return False
    return value.strip().lower() in {"1", "true", "yes", "on"}


def load_signal_rules(path: Path) -> dict[str, SignalRule]:
    rows = load_csv_rows(path)
    rules: dict[str, SignalRule] = {}
    for row in rows:
        signal_code = row.get("signal_code", "").strip()
        if not signal_code:
            continue
        try:
            rule = SignalRule(
                signal_code=signal_code,
                product_scope=(row.get("product_scope", "shared") or "shared").strip(),
                category=(row.get("category", "uncategorized") or "uncategorized").strip(),
                base_weight=float(row.get("base_weight", "0") or 0),
                half_life_days=max(1.0, float(row.get("half_life_days", "14") or 14)),
                min_confidence=float(row.get("min_confidence", "0") or 0),
                enabled=_to_bool(row.get("enabled", "true")),
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

    high = _parse("high", 70.0)
    medium = _parse("medium", 45.0)
    low = _parse("low", 0.0)
    return Thresholds(high=high, medium=medium, low=low)


def load_keyword_lexicon(path: Path) -> dict[str, list[dict[str, str]]]:
    rows = load_csv_rows(path)
    grouped: dict[str, list[dict[str, str]]] = {}
    for row in rows:
        source = row.get("source", "").strip().lower()
        if not source:
            continue
        grouped.setdefault(source, []).append(row)
    return grouped
