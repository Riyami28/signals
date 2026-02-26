"""Web-scrape company info and extract structured data via LLM."""

from __future__ import annotations

import json
import logging

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

_EXTRACT_PROMPT = """You are a company research assistant. Given the scraped text from a company website, extract structured data in JSON format.

Return ONLY valid JSON with these fields (use empty string if unknown):
{
  "website": "full URL",
  "industry": "primary industry",
  "sub_industry": "sub-industry or specialization",
  "employees": null or integer,
  "employee_range": "e.g. 51-200",
  "city": "HQ city",
  "state": "HQ state/province",
  "country": "HQ country",
  "company_linkedin_url": "LinkedIn company URL",
  "tech_stack": ["list", "of", "technologies"],
  "key_products": ["list", "of", "products/services"],
  "recent_news": "any recent announcements or press releases mentioned"
}"""

_USER_AGENT = "Mozilla/5.0 (compatible; ZopdevSignals/0.1; research)"


def _fetch_page(url: str, timeout: int = 10) -> str:
    """Fetch a page and return extracted text."""
    try:
        resp = requests.get(url, timeout=timeout, headers={"User-Agent": _USER_AGENT})
        if resp.status_code != 200:
            return ""
        soup = BeautifulSoup(resp.text, "html.parser")
        # Remove script/style
        for tag in soup(["script", "style", "nav", "footer", "header"]):
            tag.decompose()
        return soup.get_text(separator="\n", strip=True)[:8000]
    except Exception:
        logger.warning("failed to fetch url=%s", url, exc_info=True)
        return ""


def scrape_company_info(domain: str, llm_client, settings=None) -> dict:
    """
    Scrape company website and use LLM to extract structured data.

    Args:
        domain: Company domain (e.g. "example.com")
        llm_client: Any client with research_company(system, user) -> ResearchResponse
        settings: Optional settings (unused for now, reserved)

    Returns:
        dict with enrichment fields, or empty dict on failure.
    """
    # Fetch homepage and /about
    homepage_text = _fetch_page(f"https://{domain}")
    about_text = _fetch_page(f"https://{domain}/about")

    combined = ""
    if homepage_text:
        combined += f"=== Homepage ({domain}) ===\n{homepage_text}\n\n"
    if about_text:
        combined += f"=== About Page ({domain}/about) ===\n{about_text}\n\n"

    if not combined.strip():
        logger.debug("no content scraped for domain=%s", domain)
        return {}

    user_prompt = f"Company domain: {domain}\n\nScraped website content:\n{combined}"

    try:
        response = llm_client.research_company(_EXTRACT_PROMPT, user_prompt)
        raw = response.raw_text.strip()

        # Extract JSON from response (handle markdown code blocks)
        if "```json" in raw:
            raw = raw.split("```json", 1)[1].split("```", 1)[0]
        elif "```" in raw:
            raw = raw.split("```", 1)[1].split("```", 1)[0]

        data = json.loads(raw)
        logger.info("web_scrape enrichment domain=%s fields=%d", domain, len(data))
        return data
    except Exception:
        logger.warning("web scrape LLM extraction failed for domain=%s", domain, exc_info=True)
        return {}
