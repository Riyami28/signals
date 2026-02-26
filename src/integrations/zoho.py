"""
Zoho CRM integration — push enriched, scored accounts as Accounts, Contacts, and Deals.

Handles OAuth2 token refresh, idempotent upserts (update-or-insert by domain),
and rate-limit-aware retries.

Push policy:
    Tier 1 (high confidence)  → auto-push
    Tier 1 (low confidence)   → manual review first
    Tier 2 (high confidence)  → auto-push
    Tier 2 (low confidence)   → manual review
    Tier 3/4                  → do not push
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any

import requests

from src.settings import Settings

logger = logging.getLogger(__name__)

_TIMEOUT = 30  # seconds for HTTP calls
_MAX_RETRIES = 2
_RETRY_BACKOFF_BASE = 2  # seconds


class ZohoAuthError(Exception):
    """Raised when OAuth2 token refresh fails."""


class ZohoPushError(Exception):
    """Raised when a CRM push call fails after retries."""


class ZohoClient:
    """Zoho CRM v3 API client with OAuth2 token management."""

    def __init__(self, settings: Settings) -> None:
        self.client_id = settings.zoho_client_id
        self.client_secret = settings.zoho_client_secret
        self.refresh_token = settings.zoho_refresh_token
        self.base_url = settings.zoho_api_base_url.rstrip("/")
        self.auth_url = settings.zoho_auth_url
        self._access_token: str = ""
        self._token_expires_at: float = 0.0

    @property
    def is_configured(self) -> bool:
        return bool(self.client_id and self.client_secret and self.refresh_token)

    def _refresh_access_token(self) -> str:
        """Exchange refresh token for a new access token."""
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
        # Zoho tokens typically last 3600s; refresh 5 min early.
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

    def _request(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Make an API request with retry + token refresh on 401."""
        url = f"{self.base_url}/{path.lstrip('/')}"
        last_exc: Exception | None = None

        for attempt in range(_MAX_RETRIES + 1):
            try:
                resp = requests.request(
                    method,
                    url,
                    headers=self._headers(),
                    json=payload,
                    timeout=_TIMEOUT,
                )
                # Token expired — force refresh and retry.
                if resp.status_code == 401 and attempt < _MAX_RETRIES:
                    self._access_token = ""
                    continue
                resp.raise_for_status()
                return resp.json()
            except requests.RequestException as exc:
                last_exc = exc
                if attempt < _MAX_RETRIES:
                    time.sleep(_RETRY_BACKOFF_BASE * (attempt + 1))
                    continue
                raise ZohoPushError(f"Zoho API {method} {path} failed: {exc}") from exc

        raise ZohoPushError(f"Zoho API {method} {path} exhausted retries: {last_exc}")

    # ------------------------------------------------------------------
    # Search helpers (for idempotent upsert)
    # ------------------------------------------------------------------

    def search_account_by_domain(self, domain: str) -> dict[str, Any] | None:
        """Find an existing Zoho Account by Website field."""
        try:
            data = self._request("GET", f"/Accounts/search?criteria=(Website:equals:{domain})")
            records = data.get("data", [])
            return records[0] if records else None
        except ZohoPushError:
            logger.debug("zoho: search for domain=%s failed", domain, exc_info=True)
            return None

    def search_contact_by_email(self, email: str) -> dict[str, Any] | None:
        """Find an existing Zoho Contact by Email field."""
        if not email:
            return None
        try:
            data = self._request("GET", f"/Contacts/search?criteria=(Email:equals:{email})")
            records = data.get("data", [])
            return records[0] if records else None
        except ZohoPushError:
            logger.debug("zoho: search for email=%s failed", email, exc_info=True)
            return None

    # ------------------------------------------------------------------
    # Create / Update records
    # ------------------------------------------------------------------

    def upsert_account(self, account_data: dict[str, Any]) -> dict[str, Any]:
        """Create or update an Account record. Returns Zoho API response data."""
        payload = {"data": [account_data], "duplicate_check_fields": ["Website"]}
        return self._request("POST", "/Accounts/upsert", payload)

    def upsert_contact(self, contact_data: dict[str, Any]) -> dict[str, Any]:
        """Create or update a Contact record. Returns Zoho API response data."""
        payload = {"data": [contact_data], "duplicate_check_fields": ["Email"]}
        return self._request("POST", "/Contacts/upsert", payload)

    def create_deal(self, deal_data: dict[str, Any]) -> dict[str, Any]:
        """Create a Deal record. Returns Zoho API response data."""
        payload = {"data": [deal_data]}
        return self._request("POST", "/Deals", payload)

    def add_tags(self, module: str, record_id: str, tags: list[str]) -> dict[str, Any] | None:
        """Add tags to a record. Returns Zoho API response or None on error."""
        if not tags:
            return None
        tag_str = ",".join(tags)
        try:
            return self._request("POST", f"/{module}/{record_id}/actions/add_tags?tag_names={tag_str}")
        except ZohoPushError:
            logger.debug("zoho: failed to add tags to %s/%s", module, record_id, exc_info=True)
            return None


# ---------------------------------------------------------------------------
# Payload builders
# ---------------------------------------------------------------------------


def build_account_payload(
    company_name: str,
    domain: str,
    score: float,
    tier: str,
    enrichment: dict[str, Any],
    top_reasons: list[dict[str, Any]],
    dimension_scores: dict[str, float] | None = None,
    lead_source: str = "Signals Pipeline",
) -> dict[str, Any]:
    """Build Zoho Account record payload from scored + enriched data."""
    trigger_signals = ", ".join(r.get("signal_code", "") for r in top_reasons[:5])
    pain_hypothesis = ""
    if top_reasons:
        evidence_texts = [r.get("evidence_text", "") for r in top_reasons[:3] if r.get("evidence_text")]
        pain_hypothesis = "; ".join(evidence_texts)[:500]

    tech_stack = enrichment.get("tech_stack", "")
    if not tech_stack:
        tech_keywords = [r.get("signal_code", "") for r in top_reasons if "tech" in r.get("signal_code", "").lower()]
        tech_stack = ", ".join(tech_keywords[:10])

    payload: dict[str, Any] = {
        "Account_Name": company_name,
        "Website": domain,
        "ICP_Score": round(score, 2),
        "ICP_Tier": tier,
        "Lead_Source": lead_source,
        "Trigger_Signals": trigger_signals[:500],
        "Pain_Hypothesis": pain_hypothesis,
        "Enrichment_Date": __import__("datetime").date.today().isoformat(),
    }

    if enrichment.get("employees"):
        payload["Employee_Count"] = enrichment["employees"]
    if enrichment.get("industry"):
        payload["Industry"] = enrichment["industry"]
    if tech_stack:
        payload["Tech_Stack"] = tech_stack[:500]
    if dimension_scores:
        payload["Dimension_Scores"] = json.dumps(dimension_scores)[:1000]

    return payload


def build_contact_payload(
    contact: dict[str, Any],
    zoho_account_id: str,
) -> dict[str, Any]:
    """Build Zoho Contact record payload from enriched contact data."""
    payload: dict[str, Any] = {
        "First_Name": contact.get("first_name", ""),
        "Last_Name": contact.get("last_name", "Unknown"),
        "Account_Name": {"id": zoho_account_id},
    }
    if contact.get("email"):
        payload["Email"] = contact["email"]
    if contact.get("title"):
        payload["Title"] = contact["title"]
    if contact.get("linkedin_url"):
        payload["LinkedIn_URL"] = contact["linkedin_url"]
    if contact.get("management_level"):
        payload["Management_Level"] = contact["management_level"]
    return payload


def build_deal_payload(
    company_name: str,
    zoho_account_id: str,
    score: float,
    tier: str,
    stage: str = "New Lead",
    close_days: int = 60,
) -> dict[str, Any]:
    """Build Zoho Deal record payload for Tier 1 accounts."""
    from datetime import date, timedelta

    closing_date = (date.today() + timedelta(days=close_days)).isoformat()
    return {
        "Deal_Name": f"{company_name} — Signals {tier.title()}",
        "Account_Name": {"id": zoho_account_id},
        "Stage": stage,
        "Closing_Date": closing_date,
        "ICP_Score": round(score, 2),
        "ICP_Tier": tier,
        "Lead_Source": "Signals Pipeline",
    }


def build_tags(tier: str, top_reasons: list[dict[str, Any]]) -> list[str]:
    """Build CRM tags from tier and top signal codes."""
    tags = [f"icp_{tier}", "signals_pipeline"]
    for reason in top_reasons[:3]:
        code = reason.get("signal_code", "")
        if code:
            tags.append(code.replace(" ", "_")[:40])
    return tags
