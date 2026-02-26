from __future__ import annotations

import csv
import hashlib
import json
import re
from datetime import date, datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterable
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from dateutil import parser as date_parser


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def parse_date(value: str | None, timezone_name: str = "UTC") -> date:
    if not value:
        try:
            zone = ZoneInfo(timezone_name)
        except ZoneInfoNotFoundError:
            zone = timezone.utc
        return datetime.now(zone).date()
    return date_parser.parse(value).date()


def parse_datetime(value: str | None) -> datetime:
    if not value:
        return datetime.now(timezone.utc)
    parsed = date_parser.parse(value)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def normalize_domain(value: str) -> str:
    value = (value or "").strip().lower()
    if not value:
        return ""
    value = value.replace("https://", "").replace("http://", "")
    value = value.split("/", 1)[0]
    if value.startswith("www."):
        value = value[4:]
    return value


def stable_hash(payload: Any, prefix: str | None = None, length: int = 16) -> str:
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=True)
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:length]
    if prefix:
        return f"{prefix}_{digest}"
    return digest


def ensure_project_directories(paths: Iterable[Path]) -> None:
    for path in paths:
        path.mkdir(parents=True, exist_ok=True)


def load_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        rows: list[dict[str, str]] = []
        for row in reader:
            normalized: dict[str, str] = {}
            for key, value in row.items():
                if key is None:
                    continue
                if isinstance(value, list):
                    normalized[str(key)] = ",".join(str(item).strip() for item in value if str(item).strip())
                else:
                    normalized[str(key)] = str(value or "").strip()
            rows.append(normalized)
        return rows


def load_account_source_handles(path: Path) -> dict[str, dict[str, str]]:
    rows = load_csv_rows(path)
    handles: dict[str, dict[str, str]] = {}
    for row in rows:
        domain = normalize_domain(row.get("domain", ""))
        if not domain:
            continue
        handles[domain] = row
    return handles


def write_csv_rows(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key, "") for key in fieldnames})


@lru_cache(maxsize=4096)
def _keyword_pattern(keyword: str) -> re.Pattern[str] | None:
    normalized = (keyword or "").strip().lower()
    if not normalized:
        return None
    tokens = [token for token in normalized.split() if token]
    if not tokens:
        return None
    escaped = [re.escape(token) for token in tokens]
    body = r"\s+".join(escaped)
    return re.compile(rf"(?<![a-z0-9]){body}(?![a-z0-9])")


def classify_text(
    text: str,
    lexicon_rows: list[dict[str, str]],
) -> list[tuple[str, float, str]]:
    normalized = re.sub(r"\s+", " ", (text or "").lower()).strip()
    matches: list[tuple[str, float, str]] = []
    for row in lexicon_rows:
        keyword = row.get("keyword", "").lower().strip()
        signal_code = row.get("signal_code", "").strip()
        if not keyword or not signal_code:
            continue
        pattern = _keyword_pattern(keyword)
        if pattern is None:
            continue
        if not pattern.search(normalized):
            continue
        try:
            confidence = float(row.get("confidence", "0.6") or 0.6)
        except ValueError:
            confidence = 0.6
        matches.append((signal_code, confidence, keyword))
    return matches
