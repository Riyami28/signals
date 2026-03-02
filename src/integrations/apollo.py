"""Apollo.io API client for person-level contact enrichment."""

from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass, field
from threading import Lock

import requests

logger = logging.getLogger(__name__)

_GENERIC_EMAIL_PREFIXES = {
    "info",
    "sales",
    "support",
    "contact",
    "hello",
    "admin",
    "help",
    "team",
    "office",
    "marketing",
    "press",
    "media",
    "hr",
    "jobs",
    "careers",
    "billing",
    "noreply",
    "no-reply",
}

_API_BASE = "https://api.apollo.io"
_TIMEOUT_SECONDS = 15

TIER_1_ROLES = [
    ["CTO", "VP Engineering", "Head of Cloud"],
    ["FinOps Lead", "DevOps Manager", "Platform Lead"],
    ["CFO", "Head of IT Operations"],
]
TIER_2_ROLES = [TIER_1_ROLES[0]]

# Broad fetch parameters — cast wide net, LLM does semantic filtering later
BROAD_SENIORITIES = ["director", "vp", "c_suite", "owner", "founder"]
BROAD_DEPARTMENTS = [
    "engineering",
    "information_technology",
    "product_management",
    "operations",
    "finance",
]


@dataclass
class ApolloContact:
    first_name: str = ""
    last_name: str = ""
    title: str = ""
    email: str = ""
    linkedin_url: str = ""
    management_level: str = "IC"
    year_joined: int | None = None
    enrichment_source: str = "apollo"


@dataclass
class ApolloSearchResult:
    contacts: list[ApolloContact] = field(default_factory=list)
    total_found: int = 0
    api_credits_used: int = 0


def _is_generic_email(email: str) -> bool:
    if not email:
        return False
    local_part = email.split("@")[0].lower()
    return local_part in _GENERIC_EMAIL_PREFIXES


_C_LEVEL_RE = re.compile(r"\b(chief|ceo|cto|cfo|coo|cio|ciso|c-level)\b", re.IGNORECASE)


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


class ApolloClient:
    """Apollo.io API client with rate limiting."""

    def __init__(self, api_key: str, rate_limit: int = 50):
        self._api_key = api_key
        self._rate_limit = max(1, rate_limit)
        self._lock = Lock()
        self._request_times: list[float] = []

    def _wait_for_rate_limit(self) -> None:
        window = 60.0
        with self._lock:
            now = time.monotonic()
            cutoff = now - window
            self._request_times = [t for t in self._request_times if t > cutoff]
            if len(self._request_times) >= self._rate_limit:
                earliest = self._request_times[0]
                sleep_time = window - (now - earliest) + 0.1
                if sleep_time > 0:
                    time.sleep(sleep_time)
                    now = time.monotonic()
                    self._request_times = [t for t in self._request_times if t > now - window]
            self._request_times.append(time.monotonic())

    def _post(self, endpoint: str, payload: dict) -> dict:
        self._wait_for_rate_limit()
        headers = {
            "Content-Type": "application/json",
            "Cache-Control": "no-cache",
        }
        payload["api_key"] = self._api_key
        resp = requests.post(
            f"{_API_BASE}{endpoint}",
            json=payload,
            headers=headers,
            timeout=_TIMEOUT_SECONDS,
        )
        if resp.status_code != 200:
            logger.warning("apollo api error endpoint=%s status=%d", endpoint, resp.status_code)
            return {}
        return resp.json()

    def search_people(
        self,
        domain: str,
        title_keywords: list[str] | None = None,
        limit: int = 3,
    ) -> ApolloSearchResult:
        """Search for people at a company by domain and optional title keywords."""
        if not self._api_key:
            return ApolloSearchResult()

        payload: dict = {
            "q_organization_domains": domain,
            "page": 1,
            "per_page": min(limit, 25),
        }
        if title_keywords:
            payload["person_titles"] = title_keywords

        try:
            data = self._post("/v1/mixed_people/search", payload)
            if not data:
                return ApolloSearchResult()

            people = data.get("people") or []
            contacts: list[ApolloContact] = []
            for person in people[:limit]:
                email = (person.get("email") or "").strip()
                if _is_generic_email(email):
                    email = ""
                contact = ApolloContact(
                    first_name=(person.get("first_name") or "").strip(),
                    last_name=(person.get("last_name") or "").strip(),
                    title=(person.get("title") or "").strip(),
                    email=email,
                    linkedin_url=(person.get("linkedin_url") or "").strip(),
                    management_level=_infer_management_level(person.get("title") or ""),
                    enrichment_source="apollo",
                )
                contacts.append(contact)

            return ApolloSearchResult(
                contacts=contacts,
                total_found=int(data.get("pagination", {}).get("total_entries", 0)),
                api_credits_used=1,
            )
        except Exception:
            logger.warning("apollo search_people failed domain=%s", domain, exc_info=True)
            return ApolloSearchResult()

    def search_people_broad(
        self,
        domain: str,
        departments: list[str] | None = None,
        seniority_levels: list[str] | None = None,
        limit: int = 50,
    ) -> ApolloSearchResult:
        """Broad person search by department + seniority (not exact title).

        This casts a wide net (20-50 people) and returns anyone at director+
        level in engineering/IT departments. An LLM then does the semantic
        filtering to identify actual decision makers.

        Apollo API fields used:
          - person_seniorities: ["director", "vp", "c_suite", ...]
          - q_organization_domains: domain
        """
        if not self._api_key:
            return ApolloSearchResult()

        payload: dict = {
            "q_organization_domains": domain,
            "page": 1,
            "per_page": min(limit, 50),
        }
        if seniority_levels:
            payload["person_seniorities"] = seniority_levels
        if departments:
            payload["person_departments"] = departments

        try:
            data = self._post("/v1/mixed_people/search", payload)
            if not data:
                return ApolloSearchResult()

            people = data.get("people") or []
            contacts: list[ApolloContact] = []
            for person in people[:limit]:
                email = (person.get("email") or "").strip()
                if _is_generic_email(email):
                    email = ""
                # Extract department from employment_history or departments list
                dept = ""
                if person.get("departments"):
                    dept = person["departments"][0] if person["departments"] else ""
                contact = ApolloContact(
                    first_name=(person.get("first_name") or "").strip(),
                    last_name=(person.get("last_name") or "").strip(),
                    title=(person.get("title") or "").strip(),
                    email=email,
                    linkedin_url=(person.get("linkedin_url") or "").strip(),
                    management_level=_infer_management_level(person.get("title") or ""),
                    enrichment_source="apollo",
                )
                # Attach department as a dynamic attribute for the dict conversion
                contact._department = dept  # type: ignore[attr-defined]
                contacts.append(contact)

            return ApolloSearchResult(
                contacts=contacts,
                total_found=int(data.get("pagination", {}).get("total_entries", 0)),
                api_credits_used=1,
            )
        except Exception:
            logger.warning("apollo search_people_broad failed domain=%s", domain, exc_info=True)
            return ApolloSearchResult()

    def enrich_person(self, email: str) -> ApolloContact | None:
        """Enrich a person by email address."""
        if not self._api_key or not email:
            return None

        try:
            data = self._post("/v1/people/match", {"email": email})
            if not data or not data.get("person"):
                return None
            person = data["person"]
            return ApolloContact(
                first_name=(person.get("first_name") or "").strip(),
                last_name=(person.get("last_name") or "").strip(),
                title=(person.get("title") or "").strip(),
                email=email,
                linkedin_url=(person.get("linkedin_url") or "").strip(),
                management_level=_infer_management_level(person.get("title") or ""),
                enrichment_source="apollo",
            )
        except Exception:
            logger.warning("apollo enrich_person failed email=%s", email, exc_info=True)
            return None


def find_email_via_hunter(
    domain: str,
    first_name: str,
    last_name: str,
    hunter_api_key: str,
) -> str:
    """Fallback: use Hunter.io email finder when Apollo has no email."""
    if not hunter_api_key or not first_name or not last_name:
        return ""
    try:
        resp = requests.get(
            "https://api.hunter.io/v2/email-finder",
            params={
                "domain": domain,
                "first_name": first_name,
                "last_name": last_name,
                "api_key": hunter_api_key,
            },
            timeout=10,
        )
        if resp.status_code != 200:
            return ""
        data = resp.json().get("data", {})
        email = (data.get("email") or "").strip()
        if _is_generic_email(email):
            return ""
        return email
    except Exception:
        logger.warning(
            "hunter email finder failed domain=%s name=%s %s",
            domain,
            first_name,
            last_name,
            exc_info=True,
        )
        return ""


def search_contacts_for_account(
    domain: str,
    apollo_client: ApolloClient | None,
    hunter_api_key: str = "",
    tier: str = "high",
    limit: int = 3,
) -> list[dict]:
    """
    Search for contacts at a domain using Apollo with Hunter fallback.

    Returns list of contact dicts compatible with db.upsert_contacts().
    """
    role_groups = TIER_1_ROLES if tier in ("high", "tier_1") else TIER_2_ROLES
    all_titles = [title for group in role_groups for title in group]

    contacts: list[ApolloContact] = []

    # Step 1: Apollo people search.
    if apollo_client is not None:
        result = apollo_client.search_people(domain, title_keywords=all_titles, limit=limit)
        contacts = result.contacts
        logger.info(
            "apollo_search domain=%s found=%d limit=%d",
            domain,
            len(contacts),
            limit,
        )

    # Step 2: For contacts missing email, try Hunter fallback.
    if hunter_api_key:
        for contact in contacts:
            if not contact.email and contact.first_name and contact.last_name:
                found_email = find_email_via_hunter(
                    domain,
                    contact.first_name,
                    contact.last_name,
                    hunter_api_key,
                )
                if found_email:
                    contact.email = found_email
                    contact.enrichment_source = "apollo+hunter"

    # Convert to dicts for db.upsert_contacts().
    return [
        {
            "first_name": c.first_name,
            "last_name": c.last_name,
            "title": c.title,
            "email": c.email,
            "linkedin_url": c.linkedin_url,
            "management_level": c.management_level,
            "year_joined": c.year_joined,
            "department": getattr(c, "_department", ""),
        }
        for c in contacts
        if c.first_name and c.last_name
    ]
