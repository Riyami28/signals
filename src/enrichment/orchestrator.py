"""Tier-driven enrichment orchestrator.

Determines enrichment depth based on account tier_v2, then dispatches to
the appropriate contact-finding waterfall and dossier generation path.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from src.db.accounts import (
    get_account_domain,
    get_contacts_for_account,
    get_enrichment_contacts,
    insert_contact,
)
from src.integrations.apollo import (
    TIER_1_ROLES,
    TIER_2_ROLES,
    ApolloClient,
    search_contacts_for_account,
)
from src.utils import utc_now_iso

logger = logging.getLogger(__name__)


@dataclass
class EnrichmentResult:
    """Result of enriching a single account."""

    account_id: str
    tier: str
    contacts_found: int = 0
    dossier_type: str = "skipped"  # "full" | "brief" | "summary" | "skipped"
    contacts: list[dict] = field(default_factory=list)
    skipped: bool = False

    @classmethod
    def skip(cls, account_id: str, tier: str) -> EnrichmentResult:
        return cls(
            account_id=account_id,
            tier=tier,
            contacts_found=0,
            dossier_type="skipped",
            contacts=[],
            skipped=True,
        )


def enrich_account(
    conn: Any,
    account_id: str,
    tier_v2: str,
    dimension_scores: dict[str, float],
    settings: Any,
) -> EnrichmentResult:
    """Main dispatch: determine enrichment depth based on tier_v2 and execute.

    Tier dispatch logic:
    - tier_1 (or tier_2 with trigger_intent >= 70): 3 contacts, full dossier
    - tier_2: 1 contact, brief dossier
    - tier_3: 0 contacts, company summary only
    - tier_4: skip entirely
    """
    effective_tier = _resolve_effective_tier(tier_v2, dimension_scores)

    if effective_tier == "tier_4":
        logger.info("enrich_skip account=%s tier=%s", account_id, tier_v2)
        return EnrichmentResult.skip(account_id, tier_v2)

    if effective_tier == "tier_1":
        contact_limit = 3
        role_groups = TIER_1_ROLES
        dossier_type = "full"
    elif effective_tier == "tier_2":
        contact_limit = 1
        role_groups = TIER_2_ROLES
        dossier_type = "brief"
    else:  # tier_3
        contact_limit = 0
        role_groups = []
        dossier_type = "summary"

    contacts: list[dict] = []
    if contact_limit > 0:
        contacts = _find_contacts(
            conn,
            account_id,
            limit=contact_limit,
            role_groups=role_groups,
            settings=settings,
        )

    # Store contacts in the contacts table.
    now = utc_now_iso()
    for contact_dict in contacts:
        insert_contact(
            conn,
            {
                **contact_dict,
                "account_id": account_id,
                "enriched_at": now,
            },
        )

    logger.info(
        "enrich_done account=%s tier=%s effective=%s contacts=%d dossier=%s",
        account_id,
        tier_v2,
        effective_tier,
        len(contacts),
        dossier_type,
    )

    return EnrichmentResult(
        account_id=account_id,
        tier=tier_v2,
        contacts_found=len(contacts),
        dossier_type=dossier_type,
        contacts=contacts,
        skipped=False,
    )


def _resolve_effective_tier(
    tier_v2: str,
    dimension_scores: dict[str, float],
) -> str:
    """Apply tier upgrade rule: tier_2 + trigger_intent >= 70 => tier_1."""
    tier = (tier_v2 or "tier_4").strip().lower()
    if tier not in ("tier_1", "tier_2", "tier_3", "tier_4"):
        return "tier_4"

    if tier == "tier_2":
        trigger_intent = float(dimension_scores.get("trigger_intent", 0))
        if trigger_intent >= 70:
            logger.info(
                "tier_upgrade tier_2->tier_1 trigger_intent=%.1f",
                trigger_intent,
            )
            return "tier_1"

    return tier


def _find_contacts(
    conn: Any,
    account_id: str,
    limit: int,
    role_groups: list[list[str]],
    settings: Any,
) -> list[dict]:
    """Contact finding waterfall: Apollo -> Hunter fallback -> LLM supplementary."""
    domain = get_account_domain(conn, account_id)
    if not domain:
        logger.warning("no_domain account=%s", account_id)
        return []

    # Step 1: Apollo search (with Hunter email fallback built in).
    apollo_client = None
    if getattr(settings, "apollo_api_key", ""):
        apollo_client = ApolloClient(
            api_key=settings.apollo_api_key,
            rate_limit=getattr(settings, "apollo_rate_limit", 50),
        )

    tier_hint = "high" if limit >= 3 else "medium"
    contacts = search_contacts_for_account(
        domain=domain,
        apollo_client=apollo_client,
        hunter_api_key=getattr(settings, "hunter_api_key", ""),
        tier=tier_hint,
        limit=limit,
    )

    logger.info(
        "waterfall_apollo account=%s domain=%s found=%d",
        account_id,
        domain,
        len(contacts),
    )

    # Step 2: Supplement with LLM-extracted contacts if under limit.
    if len(contacts) < limit:
        llm_contacts = _get_llm_contacts(conn, account_id)
        existing_emails = {c.get("email", "").lower() for c in contacts if c.get("email")}
        for lc in llm_contacts:
            if len(contacts) >= limit:
                break
            lc_email = (lc.get("email") or "").lower()
            if lc_email and lc_email in existing_emails:
                continue
            lc["enrichment_source"] = "llm"
            contacts.append(lc)
            if lc_email:
                existing_emails.add(lc_email)

    return contacts[:limit]


def _get_llm_contacts(conn: Any, account_id: str) -> list[dict]:
    """Retrieve contacts previously extracted by LLM research."""
    rows = get_contacts_for_account(conn, account_id)
    return [
        {
            "first_name": r.get("first_name", ""),
            "last_name": r.get("last_name", ""),
            "title": r.get("title", ""),
            "email": r.get("email", ""),
            "linkedin_url": r.get("linkedin_url", ""),
            "management_level": r.get("management_level", "IC"),
            "enrichment_source": "llm",
            "confidence": 0.7,
        }
        for r in rows
        if r.get("first_name") and r.get("last_name")
    ]
