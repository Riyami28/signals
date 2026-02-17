from __future__ import annotations

import json
from typing import Any


def rank_top_reasons(reason_rows: list[dict[str, Any]], limit: int = 3) -> list[dict[str, Any]]:
    sorted_rows = sorted(reason_rows, key=lambda row: float(row.get("component_score", 0.0)), reverse=True)
    return sorted_rows[:limit]


def reasons_to_json(reason_rows: list[dict[str, Any]]) -> str:
    return json.dumps(reason_rows, ensure_ascii=True, separators=(",", ":"))
