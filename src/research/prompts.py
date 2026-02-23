"""Prompt construction for extraction and scoring passes."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

_EXTRACTION_TEMPLATE_PATH = Path(__file__).parent.parent.parent / "config" / "research_extraction_prompt.md"
_SCORING_TEMPLATE_PATH = Path(__file__).parent.parent.parent / "config" / "research_scoring_prompt.md"


def _load_template(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def build_extraction_prompt(
    account: dict,
    signals: list[dict],
    pre_enrichment: dict | None = None,
) -> tuple[str, str]:
    """
    Returns (system_prompt, user_prompt) for the extraction pass.

    account dict keys: account_id, company_name, domain, signal_score, signal_tier,
                       delta_7d, top_reasons_json
    signals list: recent signal observations with signal_code, source, evidence_url, evidence_text
    pre_enrichment: dict of fields already filled by waterfall enrichment (optional)
    """
    system_prompt = _load_template(_EXTRACTION_TEMPLATE_PATH)

    signal_lines = []
    for s in signals[:20]:
        line = f"- {s.get('signal_code', 'unknown')} via {s.get('source', 'unknown')}"
        url = s.get("evidence_url", "")
        if url:
            line += f" — {url}"
        text = (s.get("evidence_text", "") or "")[:200]
        if text:
            line += f"\n  Evidence: {text}"
        signal_lines.append(line)

    reasons = account.get("top_reasons_json", "[]")
    if isinstance(reasons, str):
        try:
            reasons = json.loads(reasons)
        except (json.JSONDecodeError, TypeError):
            reasons = []

    user_parts = [
        f"## Company: {account.get('company_name', 'Unknown')}",
        f"Domain: {account.get('domain', '')}",
        f"Signal Score: {account.get('signal_score', 0)} (tier: {account.get('signal_tier', 'unknown')})",
        f"7-day delta: {account.get('delta_7d', 0)}",
        "",
        "## Buying Signals",
        "\n".join(signal_lines) if signal_lines else "(No signal observations available)",
    ]

    if pre_enrichment:
        known_lines = []
        for key, value in pre_enrichment.items():
            if key.endswith("_confidence") or key.startswith("_") or not value:
                continue
            known_lines.append(f"- {key}: {value}")
        if known_lines:
            user_parts.extend([
                "",
                "## Already Known (from structured data sources — do NOT contradict these, only fill gaps)",
                "\n".join(known_lines),
            ])

    user_prompt = "\n".join(user_parts)
    return system_prompt, user_prompt


def build_scoring_prompt(account: dict, research_brief: str) -> tuple[str, str]:
    """
    Returns (system_prompt, user_prompt) for the scoring/personalization pass.
    Input: the prose brief produced by the extraction pass.
    """
    system_prompt = _load_template(_SCORING_TEMPLATE_PATH)

    user_prompt = "\n".join([
        f"## Company: {account.get('company_name', 'Unknown')}",
        f"Domain: {account.get('domain', '')}",
        "",
        "## Research Brief",
        research_brief,
    ])

    return system_prompt, user_prompt


def prompt_hash(extraction_template: str | None = None, scoring_template: str | None = None) -> str:
    """Stable hash of both prompt templates combined. Stored in DB to detect template changes."""
    if extraction_template is None:
        extraction_template = _load_template(_EXTRACTION_TEMPLATE_PATH)
    if scoring_template is None:
        scoring_template = _load_template(_SCORING_TEMPLATE_PATH)
    combined = extraction_template + "|||" + scoring_template
    return hashlib.sha256(combined.encode()).hexdigest()[:16]
