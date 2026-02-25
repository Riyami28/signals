"""
Pre-LLM waterfall enrichment.
Sources are tried in order: Clearbit -> Hunter -> (future: Wappalyzer)
Each source fills only fields that are currently empty.
Every filled field is tagged with source + confidence in the enrichment dict.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import requests

logger = logging.getLogger(__name__)


@dataclass
class EnrichmentResult:
    """Partial enrichment from one source. Merged with other sources by caller."""
    website: str = ""
    industry: str = ""
    sub_industry: str = ""
    employees: int | None = None
    employee_range: str = ""
    city: str = ""
    state: str = ""
    country: str = ""
    company_linkedin_url: str = ""
    source: str = ""
    confidence: float = 0.0


def enrich_from_clearbit(domain: str, api_key: str) -> EnrichmentResult | None:
    """
    Clearbit Enrichment API: GET https://company.clearbit.com/v2/companies/find?domain={domain}
    Returns None on any error.
    """
    if not api_key:
        return None
    try:
        resp = requests.get(
            f"https://company.clearbit.com/v2/companies/find?domain={domain}",
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=10,
        )
        if resp.status_code != 200:
            logger.debug("clearbit returned status=%d for domain=%s", resp.status_code, domain)
            return None
        data = resp.json()
        geo = data.get("geo") or {}
        metrics = data.get("metrics") or {}
        employees_range = metrics.get("employeesRange", "")
        employees = metrics.get("employees")

        return EnrichmentResult(
            website=str(data.get("url", "")).strip(),
            industry=str(data.get("category", {}).get("industry", "")).strip(),
            sub_industry=str(data.get("category", {}).get("subIndustry", "")).strip(),
            employees=int(employees) if employees is not None else None,
            employee_range=str(employees_range).strip(),
            city=str(geo.get("city", "")).strip(),
            state=str(geo.get("state", "")).strip(),
            country=str(geo.get("country", "")).strip(),
            company_linkedin_url=str(data.get("linkedin", {}).get("handle", "")).strip(),
            source="clearbit",
            confidence=0.9,
        )
    except Exception:
        logger.debug("clearbit enrichment failed for domain=%s", domain, exc_info=True)
        return None


def enrich_from_hunter(domain: str, api_key: str) -> EnrichmentResult | None:
    """
    Hunter.io Domain Search API.
    Returns None on any error.
    """
    if not api_key:
        return None
    try:
        resp = requests.get(
            f"https://api.hunter.io/v2/domain-search?domain={domain}&api_key={api_key}",
            timeout=10,
        )
        if resp.status_code != 200:
            logger.debug("hunter returned status=%d for domain=%s", resp.status_code, domain)
            return None
        data = resp.json().get("data", {})
        return EnrichmentResult(
            website=str(data.get("webmail", "") or "").strip(),
            industry=str(data.get("industry", "")).strip(),
            country=str(data.get("country", "")).strip(),
            city=str(data.get("city", "")).strip(),
            state=str(data.get("state", "")).strip(),
            source="hunter",
            confidence=0.85,
        )
    except Exception:
        logger.debug("hunter enrichment failed for domain=%s", domain, exc_info=True)
        return None


def enrich_from_web_scrape(domain: str, llm_client, settings=None) -> EnrichmentResult | None:
    """
    Web-scrape the company website and use LLM to extract structured data.
    Returns None on any error or if no LLM client is provided.
    """
    if llm_client is None:
        return None
    try:
        from src.research.web_scraper import scrape_company_info

        data = scrape_company_info(domain, llm_client, settings)
        if not data:
            return None
        return EnrichmentResult(
            website=str(data.get("website", "")).strip(),
            industry=str(data.get("industry", "")).strip(),
            sub_industry=str(data.get("sub_industry", "")).strip(),
            employees=int(data["employees"]) if data.get("employees") else None,
            employee_range=str(data.get("employee_range", "")).strip(),
            city=str(data.get("city", "")).strip(),
            state=str(data.get("state", "")).strip(),
            country=str(data.get("country", "")).strip(),
            company_linkedin_url=str(data.get("company_linkedin_url", "")).strip(),
            source="web_scrape",
            confidence=0.7,
        )
    except Exception:
        logger.debug("web scrape enrichment failed for domain=%s", domain, exc_info=True)
        return None


def run_enrichment_waterfall(domain: str, settings, llm_client=None) -> dict:
    """
    Try enrichment sources in order. Return a merged enrichment dict
    with _confidence for each filled field.

    Only fills fields that are currently empty — does not overwrite.
    Order: Clearbit -> Hunter -> Web Scrape + LLM
    """
    clearbit_key = getattr(settings, "clearbit_api_key", "")
    hunter_key = getattr(settings, "hunter_api_key", "")
    has_llm = llm_client is not None

    if not clearbit_key and not hunter_key and not has_llm:
        return {}

    merged: dict = {}
    _FIELDS = [
        "website", "industry", "sub_industry", "employees", "employee_range",
        "city", "state", "country", "company_linkedin_url",
    ]

    sources = []
    if clearbit_key:
        result = enrich_from_clearbit(domain, clearbit_key)
        if result:
            sources.append(result)
    if hunter_key:
        result = enrich_from_hunter(domain, hunter_key)
        if result:
            sources.append(result)
    if has_llm:
        result = enrich_from_web_scrape(domain, llm_client, settings)
        if result:
            sources.append(result)

    for result in sources:
        for fld in _FIELDS:
            if fld in merged:
                continue
            val = getattr(result, fld, None)
            if val is not None and val != "" and val != 0:
                merged[fld] = val
                merged[f"{fld}_confidence"] = result.confidence
                merged[f"{fld}_source"] = result.source

    return merged
