"""Parse Claude API responses for extraction and scoring passes."""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

_VALID_MANAGEMENT_LEVELS = {"C-Level", "VP", "Director", "Manager", "IC"}


@dataclass
class Contact:
    first_name: str
    last_name: str
    title: str = ""
    email: str = ""
    linkedin_url: str = ""
    management_level: str = "IC"
    year_joined: int | None = None


@dataclass
class CompanyEnrichment:
    website: str = ""
    industry: str = ""
    sub_industry: str = ""
    employees: int | None = None
    employee_range: str = ""
    revenue_range: str = ""
    company_linkedin_url: str = ""
    city: str = ""
    state: str = ""
    country: str = ""
    tech_stack: list[str] = field(default_factory=list)
    confidences: dict[str, float] = field(default_factory=dict)


@dataclass
class ParsedExtractionResponse:
    enrichment: CompanyEnrichment
    research_brief: str
    parse_errors: list[str] = field(default_factory=list)


@dataclass
class ParsedScoringResponse:
    contacts: list[Contact]
    conversation_starters: list[str]
    parse_errors: list[str] = field(default_factory=list)


def _extract_json_block(text: str, section_header: str) -> str | None:
    """
    Extract the content of a ```json ... ``` block after a given section header.
    Falls back to first JSON block in text if header not found.
    """
    # Try to find section header first.
    header_pattern = re.compile(
        rf"###?\s*{re.escape(section_header)}\s*\n.*?```(?:json)?\s*\n(.*?)```",
        re.DOTALL | re.IGNORECASE,
    )
    match = header_pattern.search(text)
    if match:
        return match.group(1).strip()

    # Fallback: find first ```json ... ``` block.
    fallback = re.search(r"```(?:json)?\s*\n(.*?)```", text, re.DOTALL)
    if fallback:
        return fallback.group(1).strip()

    # Last resort: find first { ... } or [ ... ] block.
    for opener, closer in [("{", "}"), ("[", "]")]:
        start = text.find(opener)
        if start >= 0:
            depth = 0
            for i in range(start, len(text)):
                if text[i] == opener:
                    depth += 1
                elif text[i] == closer:
                    depth -= 1
                    if depth == 0:
                        return text[start : i + 1]
    return None


def _parse_bullet_list(text: str) -> list[str]:
    """Extract bullet points from text. Handles -, *, and numbered lists."""
    lines = text.strip().splitlines()
    items: list[str] = []
    for line in lines:
        stripped = line.strip()
        # Match: - item, * item, • item, 1. item, 1) item
        match = re.match(r"^(?:[-*•]|\d+[.)]\s*)\s*(.*)", stripped)
        if match:
            content = match.group(1).strip()
            if content:
                items.append(content)
    return items


_CONFIDENCE_THRESHOLD = 0.5

_STRING_FIELDS = [
    "website", "industry", "sub_industry", "employee_range", "revenue_range",
    "company_linkedin_url", "city", "state", "country",
]


def parse_extraction_response(raw_text: str) -> ParsedExtractionResponse:
    """Parse the two-section extraction response."""
    errors: list[str] = []
    enrichment = CompanyEnrichment()

    if not raw_text.strip():
        errors.append("empty response")
        return ParsedExtractionResponse(enrichment=enrichment, research_brief="", parse_errors=errors)

    # Parse enrichment JSON.
    json_str = _extract_json_block(raw_text, "ENRICHMENT_JSON")
    if json_str:
        try:
            data = json.loads(json_str)
            if isinstance(data, dict):
                confidences: dict[str, float] = {}

                for fld in _STRING_FIELDS:
                    conf_key = f"{fld}_confidence"
                    conf = float(data.get(conf_key, 1.0))
                    confidences[fld] = conf
                    if conf < _CONFIDENCE_THRESHOLD:
                        errors.append(f"{fld} confidence {conf:.2f} below threshold, field omitted")
                    else:
                        val = str(data.get(fld, "")).strip()
                        setattr(enrichment, fld, val)

                # employees (int)
                emp_conf = float(data.get("employees_confidence", 1.0))
                confidences["employees"] = emp_conf
                if emp_conf < _CONFIDENCE_THRESHOLD:
                    errors.append(f"employees confidence {emp_conf:.2f} below threshold, field omitted")
                else:
                    raw_emp = data.get("employees")
                    if raw_emp is not None:
                        try:
                            enrichment.employees = int(raw_emp)
                        except (ValueError, TypeError):
                            pass

                # tech_stack (list)
                ts_conf = float(data.get("tech_stack_confidence", 1.0))
                confidences["tech_stack"] = ts_conf
                if ts_conf < _CONFIDENCE_THRESHOLD:
                    errors.append(f"tech_stack confidence {ts_conf:.2f} below threshold, field omitted")
                else:
                    raw_ts = data.get("tech_stack", [])
                    if isinstance(raw_ts, list):
                        enrichment.tech_stack = [str(t).strip() for t in raw_ts if str(t).strip()]

                enrichment.confidences = confidences
            else:
                errors.append("enrichment JSON is not an object")
        except (json.JSONDecodeError, ValueError) as exc:
            errors.append(f"failed to parse enrichment JSON: {exc}")
    else:
        errors.append("no ENRICHMENT_JSON section found")

    # Parse research brief.
    brief = ""
    brief_pattern = re.search(
        r"###?\s*RESEARCH_BRIEF\s*\n(.*?)(?:\n###|\Z)",
        raw_text,
        re.DOTALL | re.IGNORECASE,
    )
    if brief_pattern:
        brief = brief_pattern.group(1).strip()
    else:
        # Fallback: everything after the JSON block.
        json_end = raw_text.rfind("```")
        if json_end >= 0:
            remainder = raw_text[json_end + 3 :].strip()
            # Strip any section header at the start.
            remainder = re.sub(r"^###?\s*\w.*\n", "", remainder).strip()
            if len(remainder) > 50:
                brief = remainder
        if not brief:
            errors.append("no RESEARCH_BRIEF section found")

    return ParsedExtractionResponse(enrichment=enrichment, research_brief=brief, parse_errors=errors)


def parse_scoring_response(raw_text: str) -> ParsedScoringResponse:
    """Parse the two-section scoring response."""
    errors: list[str] = []
    contacts: list[Contact] = []
    starters: list[str] = []

    if not raw_text.strip():
        errors.append("empty response")
        return ParsedScoringResponse(contacts=contacts, conversation_starters=starters, parse_errors=errors)

    # Parse contacts JSON.
    json_str = _extract_json_block(raw_text, "CONTACTS_JSON")
    if json_str:
        try:
            data = json.loads(json_str)
            if isinstance(data, list):
                for item in data:
                    if not isinstance(item, dict):
                        continue
                    first = str(item.get("first_name", "")).strip()
                    last = str(item.get("last_name", "")).strip()
                    if not first or not last:
                        continue
                    level = str(item.get("management_level", "IC")).strip()
                    if level not in _VALID_MANAGEMENT_LEVELS:
                        level = "IC"
                    year = item.get("year_joined")
                    if year is not None:
                        try:
                            year = int(year)
                        except (ValueError, TypeError):
                            year = None
                    contacts.append(Contact(
                        first_name=first,
                        last_name=last,
                        title=str(item.get("title", "")).strip(),
                        email=str(item.get("email", "")).strip(),
                        linkedin_url=str(item.get("linkedin_url", "")).strip(),
                        management_level=level,
                        year_joined=year,
                    ))
            else:
                errors.append("contacts JSON is not an array")
        except (json.JSONDecodeError, ValueError) as exc:
            errors.append(f"failed to parse contacts JSON: {exc}")
    else:
        errors.append("no CONTACTS_JSON section found")

    # Parse conversation starters.
    starters_match = re.search(
        r"###?\s*CONVERSATION_STARTERS?\s*\n(.*?)(?:\n###|\Z)",
        raw_text,
        re.DOTALL | re.IGNORECASE,
    )
    if starters_match:
        starters = _parse_bullet_list(starters_match.group(1))
    else:
        # Fallback: find bullet list after the last ``` block.
        last_block = raw_text.rfind("```")
        if last_block >= 0:
            remainder = raw_text[last_block + 3 :]
            # Remove any section header.
            remainder = re.sub(r"^###?\s*\w.*\n", "", remainder).strip()
            starters = _parse_bullet_list(remainder)
        if not starters:
            errors.append("no CONVERSATION_STARTERS section found")

    return ParsedScoringResponse(contacts=contacts, conversation_starters=starters, parse_errors=errors)
