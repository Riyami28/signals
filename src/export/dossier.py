"""Dossier template renderer.

Combines data from scoring, enrichment, and research to produce a full
9-section GTM dossier for an account.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from src.utils import utc_now_iso

logger = logging.getLogger(__name__)

_DIMENSION_LABELS = {
    "trigger_intent": "Trigger / Intent",
    "tech_fit": "Technology Fit",
    "engagement_pql": "Engagement / PQL",
    "firmographic": "Firmographic",
    "hiring_growth": "Hiring & Growth",
}

_TIER_DOSSIER_TYPE = {
    "tier_1": "full",
    "tier_2": "brief",
    "tier_3": "summary",
    "tier_4": "skipped",
    "high": "full",
    "medium": "brief",
    "low": "summary",
}


def render_dossier(
    account: dict,
    research: dict | None = None,
    enrichment: dict | None = None,
    contacts: list[dict] | None = None,
    scores: dict | None = None,
    dimension_scores: dict | None = None,
    signals: list[dict] | None = None,
    dossier_type: str | None = None,
) -> dict:
    """Render a full 9-section dossier as a structured dict.

    Parameters
    ----------
    account : dict
        Account row (account_id, company_name, domain, etc.)
    research : dict | None
        company_research row (research_brief, enrichment_json, etc.)
    enrichment : dict | None
        Parsed enrichment_json fields
    contacts : list[dict] | None
        Combined contacts from contacts + contact_research tables
    scores : dict | None
        Latest account_scores row (score, tier, tier_v2, top_reasons_json)
    dimension_scores : dict | None
        Parsed dimension_scores_json (trigger_intent, tech_fit, etc.)
    signals : list[dict] | None
        Recent signal observations
    dossier_type : str | None
        Override dossier type; defaults from tier.
    """
    research = research or {}
    enrichment = enrichment or {}
    contacts = contacts or []
    scores = scores or {}
    dimension_scores = dimension_scores or {}
    signals = signals or []

    tier = scores.get("tier_v2") or scores.get("tier", "")
    if dossier_type is None:
        dossier_type = _TIER_DOSSIER_TYPE.get(tier, "full")

    company_name = account.get("company_name", "Unknown Company")
    account_id = account.get("account_id", "")

    sections = [
        _section_executive_summary(research, company_name),
        _section_company_overview(enrichment, account),
        _section_cloud_infrastructure(enrichment),
        _section_buying_signals(scores, signals),
        _section_key_decision_makers(contacts),
        _section_pain_hypothesis(research, signals),
        _section_competitive_landscape(enrichment, research),
        _section_recommended_approach(contacts, research, scores),
        _section_icp_fit_analysis(dimension_scores, scores),
    ]

    # For brief dossier, include only sections 1-5 and 9.
    if dossier_type == "brief":
        sections = [sections[i] for i in (0, 1, 3, 4, 8)]
    # For summary dossier, include only sections 1, 2, and 9.
    elif dossier_type == "summary":
        sections = [sections[i] for i in (0, 1, 8)]

    markdown = _render_markdown(company_name, sections, dossier_type, scores)

    return {
        "account_id": account_id,
        "company_name": company_name,
        "generated_at": utc_now_iso(),
        "dossier_type": dossier_type,
        "sections": sections,
        "markdown": markdown,
    }


# ---------------------------------------------------------------------------
# Section renderers
# ---------------------------------------------------------------------------


def _section_executive_summary(research: dict, company_name: str) -> dict:
    brief = research.get("research_brief", "")
    content = brief if brief else f"No research brief available for {company_name}."
    return {"title": "Executive Summary", "content": content}


def _section_company_overview(enrichment: dict, account: dict) -> dict:
    lines = []
    industry = enrichment.get("industry", "")
    if industry:
        sub = enrichment.get("sub_industry", "")
        lines.append(f"**Industry:** {industry}" + (f" — {sub}" if sub else ""))

    employees = enrichment.get("employees") or enrichment.get("employee_range", "")
    if employees:
        lines.append(f"**Size:** {employees} employees")

    revenue = enrichment.get("revenue_range", "")
    if revenue:
        lines.append(f"**Revenue:** {revenue}")

    geo_parts = [
        enrichment.get("city", ""),
        enrichment.get("state", ""),
        enrichment.get("country", ""),
    ]
    geo = ", ".join(p for p in geo_parts if p)
    if geo:
        lines.append(f"**Location:** {geo}")

    website = enrichment.get("website") or account.get("domain", "")
    if website:
        lines.append(f"**Website:** {website}")

    linkedin = enrichment.get("company_linkedin_url", "")
    if linkedin:
        lines.append(f"**LinkedIn:** {linkedin}")

    content = "\n".join(lines) if lines else "No company overview data available."
    return {"title": "Company Overview", "content": content}


def _section_cloud_infrastructure(enrichment: dict) -> dict:
    tech_stack = enrichment.get("tech_stack", [])
    if isinstance(tech_stack, str):
        try:
            tech_stack = json.loads(tech_stack)
        except (json.JSONDecodeError, TypeError):
            tech_stack = [tech_stack] if tech_stack else []

    if tech_stack:
        items = ", ".join(str(t) for t in tech_stack)
        content = f"**Technology Stack:** {items}"
    else:
        content = "No cloud infrastructure data available."
    return {"title": "Cloud Infrastructure Intelligence", "content": content}


def _section_buying_signals(scores: dict, signals: list[dict]) -> dict:
    lines = []

    # Top reasons from scoring.
    reasons_raw = scores.get("top_reasons_json", "[]")
    if isinstance(reasons_raw, str):
        try:
            reasons = json.loads(reasons_raw)
        except (json.JSONDecodeError, TypeError):
            reasons = []
    else:
        reasons = reasons_raw if isinstance(reasons_raw, list) else []

    if reasons:
        for reason in reasons[:5]:
            if isinstance(reason, dict):
                code = reason.get("signal_code", "")
                text = reason.get("reason", reason.get("evidence", ""))
                lines.append(f"- **{code}**: {text}")
            elif isinstance(reason, str):
                lines.append(f"- {reason}")

    # Recent signal observations.
    if signals:
        lines.append("")
        lines.append("**Recent Signals:**")
        for sig in signals[:10]:
            code = sig.get("signal_code", "unknown")
            source = sig.get("source", "")
            date = sig.get("observed_at", "")
            evidence = (sig.get("evidence_text", "") or "")[:100]
            line = f"- {code} via {source}"
            if date:
                line += f" ({date})"
            if evidence:
                line += f" — {evidence}"
            lines.append(line)

    content = "\n".join(lines) if lines else "No buying signals detected."
    return {"title": "Buying Signals & Triggers", "content": content}


def _section_key_decision_makers(contacts: list[dict]) -> dict:
    if not contacts:
        return {"title": "Key Decision Makers", "content": "No contacts identified."}

    lines = []
    for contact in contacts[:5]:
        name = f"{contact.get('first_name', '')} {contact.get('last_name', '')}".strip()
        title = contact.get("title", "")
        email = contact.get("email", "")
        linkedin = contact.get("linkedin_url", "")
        level = contact.get("management_level", "")

        parts = [f"**{name}**"]
        if title:
            parts.append(f"  Title: {title}")
        if level:
            parts.append(f"  Level: {level}")
        if email:
            parts.append(f"  Email: {email}")
        if linkedin:
            parts.append(f"  LinkedIn: {linkedin}")
        lines.append("\n".join(parts))

    content = "\n\n".join(lines)
    return {"title": "Key Decision Makers", "content": content}


def _section_pain_hypothesis(research: dict, signals: list[dict]) -> dict:
    brief = research.get("research_brief", "")

    # Derive pain hypothesis from research brief and signal patterns.
    signal_codes = [s.get("signal_code", "") for s in signals if s.get("signal_code")]
    unique_codes = list(dict.fromkeys(signal_codes))[:5]

    lines = []
    if brief:
        # Extract pain-related sentences from the brief.
        lines.append("**Based on research analysis:**")
        lines.append(brief[:500])

    if unique_codes:
        lines.append("")
        lines.append("**Key signal patterns suggesting pain points:**")
        for code in unique_codes:
            count = signal_codes.count(code)
            lines.append(f"- {code} (observed {count}x)")

    content = "\n".join(lines) if lines else "Insufficient data for pain hypothesis."
    return {"title": "Pain Hypothesis", "content": content}


def _section_competitive_landscape(enrichment: dict, research: dict) -> dict:
    industry = enrichment.get("industry", "")
    tech_stack = enrichment.get("tech_stack", [])
    if isinstance(tech_stack, str):
        try:
            tech_stack = json.loads(tech_stack)
        except (json.JSONDecodeError, TypeError):
            tech_stack = []

    lines = []
    if industry:
        lines.append(f"**Industry:** {industry}")
    if tech_stack:
        lines.append(f"**Current tools:** {', '.join(str(t) for t in tech_stack[:10])}")
    if not lines:
        return {"title": "Competitive Landscape", "content": "No competitive data available."}

    return {"title": "Competitive Landscape", "content": "\n".join(lines)}


def _section_recommended_approach(
    contacts: list[dict],
    research: dict,
    scores: dict,
) -> dict:
    lines = []

    # Best contact to approach.
    if contacts:
        best = contacts[0]
        name = f"{best.get('first_name', '')} {best.get('last_name', '')}".strip()
        title = best.get("title", "")
        lines.append(f"**Recommended first contact:** {name}" + (f" ({title})" if title else ""))

    # Conversation starters from research profile.
    profile = research.get("research_profile", "")
    if "Conversation Starters" in profile:
        idx = profile.index("Conversation Starters")
        starters_section = profile[idx:]
        lines.append("")
        lines.append("**Conversation starters:**")
        for line in starters_section.strip().splitlines():
            stripped = line.strip()
            if stripped.startswith("- "):
                lines.append(stripped)

    # Score context.
    score = scores.get("score")
    tier = scores.get("tier_v2") or scores.get("tier", "")
    if score is not None:
        lines.append("")
        lines.append(f"**Account score:** {score} ({tier})")

    content = "\n".join(lines) if lines else "No recommendation data available."
    return {"title": "Recommended Approach", "content": content}


def _section_icp_fit_analysis(
    dimension_scores: dict,
    scores: dict,
) -> dict:
    if not dimension_scores:
        return {"title": "ICP Fit Analysis", "content": "No dimension score data available."}

    lines = []
    overall = scores.get("score")
    if overall is not None:
        lines.append(f"**Overall Score:** {overall}")
        lines.append("")

    lines.append("| Dimension | Score | Rating |")
    lines.append("|-----------|-------|--------|")

    for dim_key, label in _DIMENSION_LABELS.items():
        value = dimension_scores.get(dim_key)
        if value is not None:
            rating = "High" if value >= 70 else ("Medium" if value >= 40 else "Low")
            lines.append(f"| {label} | {value:.0f} | {rating} |")

    return {"title": "ICP Fit Analysis", "content": "\n".join(lines)}


# ---------------------------------------------------------------------------
# Markdown renderer
# ---------------------------------------------------------------------------


def _render_markdown(
    company_name: str,
    sections: list[dict],
    dossier_type: str,
    scores: dict,
) -> str:
    """Convert sections list into a formatted Markdown document."""
    lines = [
        f"# GTM Dossier: {company_name}",
        "",
        f"*Type: {dossier_type.title()} | "
        f"Score: {scores.get('score', 'N/A')} | "
        f"Tier: {scores.get('tier_v2') or scores.get('tier', 'N/A')}*",
        "",
        "---",
        "",
    ]

    for i, section in enumerate(sections, 1):
        lines.append(f"## {i}. {section['title']}")
        lines.append("")
        lines.append(section["content"])
        lines.append("")
        lines.append("---")
        lines.append("")

    return "\n".join(lines)
