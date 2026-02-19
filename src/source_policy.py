from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from src.utils import load_csv_rows


def _to_bool(value: str | None, default: bool = True) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _to_int(value: str | None, default: int) -> int:
    raw = (value or "").strip()
    if not raw:
        return default
    try:
        return max(1, int(float(raw)))
    except ValueError:
        return default


@dataclass(frozen=True)
class SourceExecutionPolicy:
    source: str
    max_parallel_workers: int
    requests_per_second: float
    timeout_seconds: int
    retry_attempts: int
    backoff_seconds: int
    batch_size: int
    enabled: bool


def load_source_execution_policy(path: Path) -> dict[str, SourceExecutionPolicy]:
    rows = load_csv_rows(path)
    policies: dict[str, SourceExecutionPolicy] = {}

    for row in rows:
        source = (row.get("source", "") or "").strip().lower()
        if not source:
            continue
        try:
            requests_per_second = float(row.get("requests_per_second", "1.0") or 1.0)
        except ValueError:
            requests_per_second = 1.0

        policy = SourceExecutionPolicy(
            source=source,
            max_parallel_workers=_to_int(row.get("max_parallel_workers"), default=1),
            requests_per_second=max(0.0, requests_per_second),
            timeout_seconds=_to_int(row.get("timeout_seconds"), default=20),
            retry_attempts=max(0, _to_int(row.get("retry_attempts"), default=2)),
            backoff_seconds=max(0, _to_int(row.get("backoff_seconds"), default=2)),
            batch_size=_to_int(row.get("batch_size"), default=100),
            enabled=_to_bool(row.get("enabled"), default=True),
        )
        policies[source] = policy

    return policies

