"""
Zoho CRM dedup check — search Leads and Accounts before ingestion.

Prevents duplicate outreach by checking each account against Zoho CRM
before scoring/enrichment.  Queries by domain (primary) and company name
(fallback).  Respects Zoho rate limits (max 10 req/s) via a simple
token-bucket limiter.

crm_status values:
    new              — not found in CRM
    existing_lead    — found as Lead in Zoho
    existing_customer— found as Account/customer in Zoho
    excluded         — manually excluded
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any

import requests

from src.settings import Settings

logger = logging.getLogger(__name__)

_TIMEOUT = 30  # seconds for HTTP calls
_MAX_RETRIES = 2
_RETRY_BACKOFF_BASE = 2  # seconds

# Zoho API rate limit: max 10 requests/second.
_RATE_LIMIT_RPS = 10
_RATE_LIMIT_INTERVAL = 1.0 / _RATE_LIMIT_RPS


class ZohoAuthError(Exception):
    """Raised when OAuth2 token refresh fails."""


class ZohoDedupError(Exception):
    """Raised when a CRM dedup call fails after retries."""


class _RateLimiter:
    """Simple thread-safe rate limiter for Zoho API calls."""

    def __init__(self, min_interval: float = _RATE_LIMIT_INTERVAL) -> None:
        self._min_interval = min_interval
        self._last_call = 0.0
        self._lock = threading.Lock()

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_call
            if elapsed < self._min_interval:
                time.sleep(self._min_interval - elapsed)
            self._last_call = time.monotonic()


class ZohoCRMDedupClient:
    """Zoho CRM v3 search client for dedup checks.

    Uses the same OAuth2 credentials as the push client but is focused on
    read-only search operations against Leads and Accounts modules.
    """

    def __init__(self, settings: Settings) -> None:
        self.client_id = settings.zoho_client_id
        self.client_secret = settings.zoho_client_secret
        self.refresh_token = settings.zoho_refresh_token
        self.base_url = settings.zoho_api_base_url.rstrip("/")
        self.auth_url = settings.zoho_auth_url
        self._access_token: str = ""
        self._token_expires_at: float = 0.0
        self._limiter = _RateLimiter()

    @property
    def is_configured(self) -> bool:
        return bool(self.client_id and self.client_secret and self.refresh_token)

    # ------------------------------------------------------------------
    # OAuth2 token management (same pattern as zoho.py push client)
    # ------------------------------------------------------------------

    def _refresh_access_token(self) -> str:
        resp = requests.post(
            self.auth_url,
            params={
                "refresh_token": self.refresh_token,
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "grant_type": "refresh_token",
            },
            timeout=_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        if "access_token" not in data:
            raise ZohoAuthError(f"Token refresh failed: {data.get('error', 'unknown')}")
        self._access_token = data["access_token"]
        self._token_expires_at = time.monotonic() + data.get("expires_in", 3600) - 300
        return self._access_token

    def _get_token(self) -> str:
        if not self._access_token or time.monotonic() >= self._token_expires_at:
            return self._refresh_access_token()
        return self._access_token

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Zoho-oauthtoken {self._get_token()}",
            "Content-Type": "application/json",
        }

    def _request(self, method: str, path: str) -> dict[str, Any]:
        """Make a rate-limited API request with retry + token refresh on 401."""
        url = f"{self.base_url}/{path.lstrip('/')}"
        last_exc: Exception | None = None

        for attempt in range(_MAX_RETRIES + 1):
            self._limiter.wait()
            try:
                resp = requests.request(
                    method,
                    url,
                    headers=self._headers(),
                    timeout=_TIMEOUT,
                )
                if resp.status_code == 401 and attempt < _MAX_RETRIES:
                    self._access_token = ""
                    continue
                if resp.status_code == 204:
                    return {"data": []}
                resp.raise_for_status()
                return resp.json()
            except requests.RequestException as exc:
                last_exc = exc
                if attempt < _MAX_RETRIES:
                    time.sleep(_RETRY_BACKOFF_BASE * (attempt + 1))
                    continue
                raise ZohoDedupError(f"Zoho API {method} {path} failed: {exc}") from exc

        raise ZohoDedupError(f"Zoho API {method} {path} exhausted retries: {last_exc}")

    # ------------------------------------------------------------------
    # Search helpers
    # ------------------------------------------------------------------

    def search_account(self, domain: str) -> dict[str, Any] | None:
        """Search Zoho Accounts module by Website field (domain)."""
        try:
            data = self._request("GET", f"/Accounts/search?criteria=(Website:equals:{domain})")
            records = data.get("data", [])
            return records[0] if records else None
        except ZohoDedupError:
            logger.debug("zoho_dedup: account search failed for domain=%s", domain, exc_info=True)
            return None

    def search_lead(self, domain: str) -> dict[str, Any] | None:
        """Search Zoho Leads module by Company field (domain)."""
        try:
            data = self._request("GET", f"/Leads/search?criteria=(Company:equals:{domain})")
            records = data.get("data", [])
            return records[0] if records else None
        except ZohoDedupError:
            logger.debug("zoho_dedup: lead search failed for domain=%s", domain, exc_info=True)
            return None

    def search_lead_by_company_name(self, company_name: str) -> dict[str, Any] | None:
        """Fallback: search Zoho Leads by Company name."""
        if not company_name:
            return None
        try:
            data = self._request(
                "GET",
                f"/Leads/search?criteria=(Company:equals:{company_name})",
            )
            records = data.get("data", [])
            return records[0] if records else None
        except ZohoDedupError:
            logger.debug(
                "zoho_dedup: lead search by name failed for company=%s",
                company_name,
                exc_info=True,
            )
            return None

    def is_existing(self, domain: str, company_name: str = "") -> tuple[bool, str]:
        """Check if a domain exists in Zoho CRM as Account or Lead.

        Returns (is_existing, crm_status) where crm_status is one of:
            'new'               — not found
            'existing_customer' — found as Account in Zoho
            'existing_lead'     — found as Lead in Zoho
        """
        # 1. Primary: search Accounts by domain.
        account = self.search_account(domain)
        if account:
            logger.info("zoho_dedup: domain=%s found as existing Account (id=%s)", domain, account.get("id"))
            return True, "existing_customer"

        # 2. Search Leads by domain.
        lead = self.search_lead(domain)
        if lead:
            logger.info("zoho_dedup: domain=%s found as existing Lead (id=%s)", domain, lead.get("id"))
            return True, "existing_lead"

        # 3. Fallback: search Leads by company name.
        if company_name:
            lead = self.search_lead_by_company_name(company_name)
            if lead:
                logger.info(
                    "zoho_dedup: company=%s found as existing Lead (id=%s)",
                    company_name,
                    lead.get("id"),
                )
                return True, "existing_lead"

        return False, "new"


# ---------------------------------------------------------------------------
# High-level dedup check for pipeline integration
# ---------------------------------------------------------------------------


def check_crm_dedup(
    domain: str,
    company_name: str,
    settings: Settings,
    client: ZohoCRMDedupClient | None = None,
) -> str:
    """Check a single domain against Zoho CRM and return crm_status.

    If Zoho is not configured, returns 'new' (pass-through).
    """
    if client is None:
        client = ZohoCRMDedupClient(settings)

    if not client.is_configured:
        return "new"

    _, status = client.is_existing(domain, company_name)
    return status
