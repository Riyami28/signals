"""LinkedIn people discovery via Serper.dev Google Search.

Used as the broad-fetch step when no Apollo key is configured.
Each Serper API call returns up to 10 results and costs exactly 1 credit —
so fetching 1 or 10 results costs the same.

Strategy:
  Search: site:linkedin.com/in "Company Name" (CTO OR VP OR Director OR "Head of")
  Parse title + snippet from Google results to extract:
    - Full name
    - Current job title
    - LinkedIn URL (normalised to linkedin.com/in/...)
    - Whether they are current vs. former (stale flag)
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass

import requests

logger = logging.getLogger(__name__)

_SERPER_ENDPOINT = "https://google.serper.dev/search"
_TIMEOUT_SECONDS = 10

# Regex to split "Name - Title - LinkedIn" or "Name - Title at Company | LinkedIn"
_TITLE_SEP = re.compile(r"\s*[-|]\s*")

# Stale signal in title/snippet
_STALE_RE = re.compile(
    r"\b(ex[-\s]|former|previously|left|now at|moved to|alumni)\b",
    re.IGNORECASE,
)

# Seniority keywords for the Google query — kept broad so LLM does the real filtering
_SENIORITY_TERMS = (
    "CTO OR CDO OR CIO OR CISO OR CPO"
    ' OR "VP Engineering" OR "VP of Engineering" OR "VP Technology"'
    ' OR "Vice President"'
    ' OR "Director of Engineering" OR "Director of Technology"'
    ' OR "Head of Engineering" OR "Head of Technology" OR "Head of Platform"'
    ' OR "Chief Technology" OR "Chief Digital" OR "Chief Information"'
)

_C_LEVEL_RE = re.compile(r"\b(chief|ceo|cto|cfo|coo|cio|ciso|c-level|cdo|cpo)\b", re.IGNORECASE)


def _infer_management_level(title: str) -> str:
    t = (title or "").lower()
    if _C_LEVEL_RE.search(title or ""):
        return "C-Level"
    if any(kw in t for kw in ("vp ", "vice president")):
        return "VP"
    if "director" in t or "head of" in t:
        return "Director"
    if "manager" in t or "lead" in t:
        return "Manager"
    return "IC"


def _normalise_linkedin_url(url: str) -> str:
    """Normalise regional LinkedIn URLs to linkedin.com/in/... format."""
    # in.linkedin.com → linkedin.com, vn.linkedin.com → linkedin.com, etc.
    url = re.sub(r"https?://[a-z]{2}\.linkedin\.com/", "https://www.linkedin.com/", url)
    url = re.sub(r"https?://www\.linkedin\.com/", "https://www.linkedin.com/", url)
    return url.strip()


# Strong role words — unambiguous even in isolation
_STRONG_ROLE_RE = re.compile(
    r"\b(engineer|director|manager|head|vp|chief|cto|coo|cfo|cio|cdo|cpo|"
    r"president|officer|founder|architect|analyst|lead|principal|"
    r"devops|sre|programme|program)\b",
    re.IGNORECASE,
)


def _looks_like_job_title(text: str) -> bool:
    """Return True if text contains a strong, unambiguous job role word.

    Words like 'digital', 'technology', 'platform' are intentionally excluded
    because they appear in company names (e.g. 'Tata Digital', 'Tech Platform').
    A strong role word like 'Director', 'VP', 'CTO' must be present.
    """
    return bool(_STRONG_ROLE_RE.search(text))


def _extract_title_from_snippet(snippet: str) -> str:
    """Try to extract a job title from a LinkedIn snippet.

    Snippets often start with the person's current title, e.g.:
      "I am a Director of Technology with 20+ years..."
      "Head of Engineering at Tata Digital · Experience: ..."
      "VP of Engineering · Tata Digital ..."
    """
    if not snippet:
        return ""
    # Pattern 1: starts with a role keyword
    m = re.match(r"^([A-Z][^·|.]{3,60}?)(?:\s+at\s+|\s*[·|]\s*)", snippet)
    if m:
        candidate = m.group(1).strip()
        if _looks_like_job_title(candidate):
            return candidate
    # Pattern 2: "I am a <title>" opener
    m2 = re.match(r"I am an?\s+([^.·|]{5,60}?)(?:\s+with|\s*[·|.])", snippet, re.IGNORECASE)
    if m2:
        candidate = m2.group(1).strip()
        if _looks_like_job_title(candidate):
            return candidate
    return ""


def _parse_name_and_title(google_title: str, snippet: str = "") -> tuple[str, str, str]:
    """Parse Google result title into (first_name, last_name, job_title).

    Google LinkedIn title formats seen in the wild:
      "Ankur Garg - Head of Engineering at Tata Digital | LinkedIn"
      "Suman Guha - ET AI Top 50 2025, Incoming CDO, Seasoned CTO ..."
      "Manimaran Malaichamy - Tata Digital - LinkedIn"      ← no title in title
      "Vinay Chavan - Director of Engineering (Commerce) @Tata Digital"
    """
    # Strip trailing "| LinkedIn" or "- LinkedIn"
    clean = re.sub(r"\s*[|\-]\s*LinkedIn\s*$", "", google_title, flags=re.IGNORECASE).strip()
    # Split on first " - " to separate name from the rest
    parts = re.split(r"\s+-\s+", clean, maxsplit=1)
    full_name = parts[0].strip()
    job_title = parts[1].strip() if len(parts) > 1 else ""

    # Strip "@Company" or "at Company" from end of title
    job_title = re.sub(r"\s+[@|]?\s*(at|@)\s+[\w\s&,.-]+$", "", job_title, flags=re.IGNORECASE).strip()
    job_title = job_title.rstrip(" ,.")

    # If what we parsed doesn't look like a job title (e.g. just "Tata Digital"),
    # fall back to extracting the title from the Google snippet instead.
    if job_title and not _looks_like_job_title(job_title):
        snippet_title = _extract_title_from_snippet(snippet)
        if snippet_title:
            job_title = snippet_title
        else:
            job_title = ""  # Better to be empty than wrong

    # Split full_name into first / last
    name_parts = full_name.split()
    first = name_parts[0] if name_parts else ""
    last = " ".join(name_parts[1:]) if len(name_parts) > 1 else ""
    return first, last, job_title


@dataclass
class DiscoveredContact:
    first_name: str = ""
    last_name: str = ""
    title: str = ""
    linkedin_url: str = ""
    management_level: str = "IC"
    is_stale: bool = False
    snippet: str = ""


class SerpDiscoverer:
    """Discover senior contacts at a company via Serper.dev Google Search."""

    def __init__(self, api_key: str):
        self._api_key = (api_key or "").strip()

    @property
    def is_configured(self) -> bool:
        return bool(self._api_key)

    def discover_people(
        self,
        company_name: str,
        domain: str,
        limit: int = 10,
    ) -> list[DiscoveredContact]:
        """Search for senior people at a company via LinkedIn SERP.

        Costs exactly 1 Serper credit regardless of `limit` (up to 10).
        For limit > 10, makes ceil(limit/10) calls.

        Args:
            company_name: Display name e.g. "Tata Digital"
            domain: e.g. "tatadigital.com"
            limit: Max contacts to return (1–10 = 1 credit, 11–20 = 2 credits)

        Returns:
            List of DiscoveredContact, non-stale first.
        """
        if not self._api_key:
            return []

        # Use company name in query — more reliable than domain for LinkedIn
        query = f'site:linkedin.com/in "{company_name}" ({_SENIORITY_TERMS})'
        num_per_call = min(limit, 10)

        contacts: list[DiscoveredContact] = []

        try:
            resp = requests.post(
                _SERPER_ENDPOINT,
                headers={
                    "X-API-KEY": self._api_key,
                    "Content-Type": "application/json",
                },
                json={"q": query, "num": num_per_call},
                timeout=_TIMEOUT_SECONDS,
            )
            if resp.status_code != 200:
                logger.warning(
                    "serp_discover: api error status=%d query=%r",
                    resp.status_code,
                    query,
                )
                return []

            data = resp.json()
        except Exception:
            logger.warning("serp_discover: request failed company=%r", company_name, exc_info=True)
            return []

        organic = data.get("organic") or []
        logger.info(
            "serp_discover: company=%r query_results=%d credits=%s",
            company_name,
            len(organic),
            data.get("credits"),
        )

        for result in organic[:limit]:
            google_title = result.get("title", "")
            url = _normalise_linkedin_url(result.get("link", ""))
            snippet = result.get("snippet", "")

            # Skip non-profile URLs
            if "/in/" not in url:
                continue

            first, last, job_title = _parse_name_and_title(google_title, snippet)
            if not first:
                continue

            # Stale check: look in Google title + snippet
            combined = f"{google_title} {snippet}"
            is_stale = bool(_STALE_RE.search(combined))

            contacts.append(
                DiscoveredContact(
                    first_name=first,
                    last_name=last,
                    title=job_title,
                    linkedin_url=url,
                    management_level=_infer_management_level(job_title),
                    is_stale=is_stale,
                    snippet=snippet,
                )
            )

        # Sort: current employees first, then by seniority
        _seniority_order = {"C-Level": 0, "VP": 1, "Director": 2, "Manager": 3, "IC": 4}
        contacts.sort(
            key=lambda c: (
                int(c.is_stale),  # current before stale
                _seniority_order.get(c.management_level, 5),
            )
        )
        return contacts
