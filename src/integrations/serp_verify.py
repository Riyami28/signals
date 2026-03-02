"""LinkedIn employment verification via SERP API (Serper.dev).

Used as the "trust but verify" step after LLM ranking:
  - Search for "{Name}" site:linkedin.com/in using Google via Serper
  - Parse the snippet to check if the person's current company matches
  - If the snippet shows a different employer → flag as stale data

This is only run on the top 3 LLM-ranked contacts, keeping API cost minimal.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field

import requests

logger = logging.getLogger(__name__)

_SERPER_ENDPOINT = "https://google.serper.dev/search"
_TIMEOUT_SECONDS = 10

# Patterns that strongly suggest the person is no longer at the company.
# E.g. snippet says "Former VP at Tata | Now at Infosys"
_FORMER_PATTERNS = re.compile(
    r"\b(former|ex-|previously|left|joined|now at|moved to|ex\s)\b",
    re.IGNORECASE,
)


@dataclass
class SerpVerifyResult:
    """Result of a SERP-based employment verification check."""

    name: str = ""
    domain: str = ""

    # None = not checked (e.g. no Serper key), True = confirmed, False = stale
    employment_verified: bool | None = None

    # Confidence: 1.0 = strong signal, 0.5 = weak signal, 0.0 = inconclusive
    confidence: float = 0.0

    # Raw snippet from Google search result
    snippet: str = ""

    # Human-readable note to store on the contact
    note: str = ""


class SerpVerifier:
    """Verify employment via Google SERP (Serper.dev)."""

    def __init__(self, api_key: str):
        self._api_key = (api_key or "").strip()

    @property
    def is_configured(self) -> bool:
        return bool(self._api_key)

    def verify_employment(
        self,
        name: str,
        company_name: str,
        domain: str,
    ) -> SerpVerifyResult:
        """Check if a person still works at the given company.

        Strategy:
          1. Search Google for `"Name" "company" site:linkedin.com/in`
          2. Inspect the top result's snippet
          3. If snippet contains domain/company → confirmed (employment_verified=True)
          4. If snippet contains former/ex/left patterns → stale (employment_verified=False)
          5. No clear signal → employment_verified=None, confidence=0

        Args:
            name: Full name of the person (e.g. "Rajesh Kumar")
            company_name: Company display name (e.g. "Tata Digital")
            domain: Company domain (e.g. "tatadigital.com")

        Returns:
            SerpVerifyResult with employment_verified, confidence, snippet, note
        """
        if not self._api_key:
            return SerpVerifyResult(name=name, domain=domain)

        # Build search query — name + site:linkedin.com/in
        company_hint = company_name or domain.split(".")[0]
        query = f'"{name}" "{company_hint}" site:linkedin.com/in'

        try:
            resp = requests.post(
                _SERPER_ENDPOINT,
                headers={
                    "X-API-KEY": self._api_key,
                    "Content-Type": "application/json",
                },
                json={"q": query, "num": 3},
                timeout=_TIMEOUT_SECONDS,
            )
            if resp.status_code != 200:
                logger.warning(
                    "serp_verify: api error status=%d query=%r",
                    resp.status_code,
                    query,
                )
                return SerpVerifyResult(name=name, domain=domain)

            data = resp.json()
        except Exception:
            logger.warning("serp_verify: request failed for %r", name, exc_info=True)
            return SerpVerifyResult(name=name, domain=domain)

        organic = data.get("organic") or []
        if not organic:
            # No results — inconclusive
            return SerpVerifyResult(
                name=name,
                domain=domain,
                employment_verified=None,
                confidence=0.0,
                snippet="",
                note="No LinkedIn results found via SERP",
            )

        # Take the first result's snippet
        top = organic[0]
        snippet = top.get("snippet", "")
        title_text = top.get("title", "")
        combined = f"{title_text} {snippet}".lower()

        domain_root = domain.lower().split(".")[0]  # e.g. "tatadigital"
        company_lower = company_hint.lower()

        # Check for stale signal first (explicit "former", "ex-", etc.)
        if _FORMER_PATTERNS.search(combined):
            return SerpVerifyResult(
                name=name,
                domain=domain,
                employment_verified=False,
                confidence=0.8,
                snippet=snippet,
                note=f"SERP snippet suggests stale data: …{snippet[:120]}…",
            )

        # Check for positive signal — company name or domain root in snippet
        if company_lower in combined or domain_root in combined:
            return SerpVerifyResult(
                name=name,
                domain=domain,
                employment_verified=True,
                confidence=0.85,
                snippet=snippet,
                note=f"LinkedIn profile confirms employment at {company_hint}",
            )

        # Inconclusive — snippet exists but no company match
        return SerpVerifyResult(
            name=name,
            domain=domain,
            employment_verified=None,
            confidence=0.3,
            snippet=snippet,
            note="SERP result found but employment status inconclusive",
        )
