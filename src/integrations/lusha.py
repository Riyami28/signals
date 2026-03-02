"""Lusha contact enrichment client.

Lusha enriches contacts with business emails and phone numbers.

API endpoint : POST https://api.lusha.com/person
Auth         : ``api_key`` header (NOT Bearer token)
Rate limits  : Free plan — 10 calls / hour
Response     : {"data": [{"emails": [...], "phones": [...], ...}]}

Usage::

    client = LushaClient(api_key="...")
    result = client.enrich_person(
        first_name="Jane",
        last_name="Doe",
        company_name="Acme Corp",
    )
    if result.found:
        print(result.email, result.phone)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

import requests

logger = logging.getLogger(__name__)

_LUSHA_ENDPOINT = "https://api.lusha.com/person"
_TIMEOUT_SECONDS = 15


@dataclass
class LushaResult:
    email: str = ""
    email_type: str = ""  # "professional", "personal", etc.
    phone: str = ""
    phone_type: str = ""  # "direct", "mobile", etc.
    found: bool = False
    error: str = ""  # "rate_limited", "not_found", "http_4xx", etc.
    raw_emails: list[dict] = field(default_factory=list)
    raw_phones: list[dict] = field(default_factory=list)

    @property
    def is_ok(self) -> bool:
        return not self.error

    @property
    def has_email(self) -> bool:
        return bool(self.email)

    @property
    def has_phone(self) -> bool:
        return bool(self.phone)


class LushaClient:
    """Thin wrapper around the Lusha Person Enrichment API."""

    def __init__(self, api_key: str):
        self._api_key = (api_key or "").strip()

    @property
    def is_configured(self) -> bool:
        return bool(self._api_key)

    def enrich_person(
        self,
        first_name: str = "",
        last_name: str = "",
        company_name: str = "",
        linkedin_url: str = "",
    ) -> LushaResult:
        """Enrich a single person.

        Prefers LinkedIn URL lookup (most accurate) over name+company.
        Requires either:
          - ``linkedin_url``  — LinkedIn profile URL, OR
          - ``first_name`` + ``last_name`` + ``company_name``

        Returns a :class:`LushaResult` with ``found=False`` and an ``error``
        string when lookup fails so callers don't need try/except.
        """
        if not self._api_key:
            return LushaResult(error="not_configured")

        # Build the contact payload
        contact: dict = {}
        if linkedin_url:
            contact["linkedInUrl"] = linkedin_url.strip()
        elif first_name and last_name and company_name:
            contact["fullName"] = f"{first_name.strip()} {last_name.strip()}"
            contact["companyName"] = company_name.strip()
        else:
            return LushaResult(error="insufficient_params")

        try:
            resp = requests.post(
                _LUSHA_ENDPOINT,
                headers={
                    "api_key": self._api_key,
                    "Content-Type": "application/json",
                },
                json={"contacts": [contact]},
                timeout=_TIMEOUT_SECONDS,
            )
        except requests.RequestException as exc:
            logger.warning("lusha: request failed — %s", exc)
            return LushaResult(error="request_failed")

        if resp.status_code == 429:
            logger.warning("lusha: rate limited (free plan = 10 calls/hour). Retry after the hour window resets.")
            return LushaResult(error="rate_limited")

        if resp.status_code == 404:
            logger.debug("lusha: person not found for contact=%s", contact)
            return LushaResult(found=False)

        if not resp.ok:
            logger.warning(
                "lusha: API error status=%d body=%s",
                resp.status_code,
                resp.text[:200],
            )
            return LushaResult(error=f"http_{resp.status_code}")

        try:
            data = resp.json()
        except ValueError:
            logger.warning("lusha: non-JSON response body=%s", resp.text[:200])
            return LushaResult(error="invalid_json")

        persons = data.get("data") or []
        if not persons:
            return LushaResult(found=False)

        person = persons[0]
        raw_emails: list[dict] = person.get("emails") or []
        raw_phones: list[dict] = person.get("phones") or []

        # Pick best email — prefer "professional" type
        best_email = ""
        best_email_type = ""
        for em in raw_emails:
            t = (em.get("type") or "").lower()
            e = (em.get("email") or "").strip()
            if not e:
                continue
            if t == "professional" or not best_email:
                best_email = e
                best_email_type = t

        # Pick best phone — prefer "direct" or "mobile"
        best_phone = ""
        best_phone_type = ""
        for ph in raw_phones:
            t = (ph.get("type") or "").lower()
            p = (ph.get("normalizedNumber") or ph.get("localNumber") or ph.get("internationalNumber") or "").strip()
            if not p:
                continue
            if t in ("direct", "mobile") or not best_phone:
                best_phone = p
                best_phone_type = t

        found = bool(best_email or best_phone)
        logger.info(
            "lusha: enriched contact=%s found=%s email=%s phone=%s",
            contact,
            found,
            bool(best_email),
            bool(best_phone),
        )

        return LushaResult(
            email=best_email,
            email_type=best_email_type,
            phone=best_phone,
            phone_type=best_phone_type,
            found=found,
            raw_emails=raw_emails,
            raw_phones=raw_phones,
        )
