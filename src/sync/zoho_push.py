"""
Zoho CRM push orchestrator.

Reads scored + enriched accounts from the database and pushes eligible records
to Zoho CRM as Accounts, Contacts, and Deals.

Push policy (tier + confidence gating):
    high  tier, high confidence  → auto-push
    high  tier, low  confidence  → skip (manual review)
    medium tier, high confidence → auto-push
    medium tier, low  confidence → skip (manual review)
    low   tier                   → do not push

Idempotent: re-pushing the same account upserts (updates) instead of creating
duplicates. Push status tracked in ``crm_push_log`` table.
"""

from __future__ import annotations

import json
import logging
from typing import Any

from src import db
from src.integrations.zoho import (
    ZohoClient,
    ZohoPushError,
    build_account_payload,
    build_contact_payload,
    build_deal_payload,
    build_tags,
)
from src.settings import Settings
from src.utils import stable_hash, utc_now_iso

logger = logging.getLogger(__name__)

# Minimum ICP score to consider "high confidence" within a tier.
_HIGH_CONFIDENCE_SCORE_THRESHOLD = 15.0


def _classify_confidence(score: float, tier: str) -> str:
    """Classify push confidence based on score within tier."""
    if tier == "high" and score >= 25.0:
        return "high"
    if tier == "high":
        return "low"
    if tier == "medium" and score >= _HIGH_CONFIDENCE_SCORE_THRESHOLD:
        return "high"
    return "low"


def _should_auto_push(tier: str, confidence_band: str, auto_push_tiers: tuple[str, ...]) -> bool:
    """Decide if an account should be auto-pushed based on push policy."""
    if tier not in auto_push_tiers:
        return False
    return confidence_band == "high"


def _push_account(
    client: ZohoClient,
    conn,
    account: dict[str, Any],
    run_id: str,
    settings: Settings,
) -> dict[str, Any]:
    """Push a single account + contacts + deal to Zoho CRM.

    Returns summary dict with push results.
    """
    account_id = account["account_id"]
    company_name = account["company_name"]
    domain = account["domain"]
    score = float(account["score"])
    tier = account["tier"]
    top_reasons_json = account.get("top_reasons_json", "[]")
    enrichment_json = account.get("enrichment_json", "{}")

    top_reasons = json.loads(top_reasons_json) if isinstance(top_reasons_json, str) else top_reasons_json
    enrichment = json.loads(enrichment_json) if isinstance(enrichment_json, str) else enrichment_json

    result: dict[str, Any] = {
        "account_id": account_id,
        "domain": domain,
        "account_pushed": False,
        "contacts_pushed": 0,
        "deal_created": False,
        "error": "",
    }

    # --- Push Account ---
    push_id = stable_hash(
        {"account_id": account_id, "run_id": run_id, "type": "account"},
        prefix="crm",
    )
    account_payload = build_account_payload(
        company_name=company_name,
        domain=domain,
        score=score,
        tier=tier,
        enrichment=enrichment,
        top_reasons=top_reasons,
        lead_source=settings.zoho_lead_source,
    )

    db.insert_crm_push_log(
        conn,
        push_id,
        account_id,
        run_id,
        "account",
        "pending",
        tier=tier,
        confidence_band=_classify_confidence(score, tier),
        payload_json=json.dumps(account_payload),
        commit=False,
    )

    zoho_account_id = ""
    try:
        resp = client.upsert_account(account_payload)
        records = resp.get("data", [])
        if records:
            zoho_account_id = str(records[0].get("details", {}).get("id", ""))
            status = (
                "updated" if records[0].get("code") == "SUCCESS" and records[0].get("action") == "update" else "pushed"
            )
        else:
            status = "pushed"
        now = utc_now_iso()
        db.update_crm_push_status(
            conn,
            push_id,
            status,
            crm_record_id=zoho_account_id,
            pushed_at=now,
            commit=False,
        )
        result["account_pushed"] = True
    except (ZohoPushError, Exception) as exc:
        logger.warning("zoho: failed to push account %s: %s", domain, exc)
        db.update_crm_push_status(
            conn,
            push_id,
            "failed",
            error_summary=str(exc)[:500],
            commit=False,
        )
        result["error"] = str(exc)[:200]
        conn.commit()
        return result

    # --- Push Contacts ---
    if zoho_account_id:
        contacts = db.get_contacts_for_account(conn, account_id)
        for contact in contacts:
            contact_push_id = stable_hash(
                {"contact_id": contact["contact_id"], "run_id": run_id, "type": "contact"},
                prefix="crm",
            )
            contact_payload = build_contact_payload(contact, zoho_account_id)

            db.insert_crm_push_log(
                conn,
                contact_push_id,
                account_id,
                run_id,
                "contact",
                "pending",
                payload_json=json.dumps(contact_payload),
                commit=False,
            )
            try:
                resp = client.upsert_contact(contact_payload)
                contact_records = resp.get("data", [])
                crm_contact_id = ""
                if contact_records:
                    crm_contact_id = str(contact_records[0].get("details", {}).get("id", ""))
                db.update_crm_push_status(
                    conn,
                    contact_push_id,
                    "pushed",
                    crm_record_id=crm_contact_id,
                    pushed_at=utc_now_iso(),
                    commit=False,
                )
                result["contacts_pushed"] += 1
            except (ZohoPushError, Exception) as exc:
                logger.warning("zoho: failed to push contact for %s: %s", domain, exc)
                db.update_crm_push_status(
                    conn,
                    contact_push_id,
                    "failed",
                    error_summary=str(exc)[:500],
                    commit=False,
                )

    # --- Create Deal (Tier 1 / high only) ---
    if zoho_account_id and tier == "high":
        deal_push_id = stable_hash(
            {"account_id": account_id, "run_id": run_id, "type": "deal"},
            prefix="crm",
        )
        deal_payload = build_deal_payload(
            company_name=company_name,
            zoho_account_id=zoho_account_id,
            score=score,
            tier=tier,
            stage=settings.zoho_deal_stage,
            close_days=settings.zoho_deal_close_days,
        )
        db.insert_crm_push_log(
            conn,
            deal_push_id,
            account_id,
            run_id,
            "deal",
            "pending",
            tier=tier,
            payload_json=json.dumps(deal_payload),
            commit=False,
        )
        try:
            resp = client.create_deal(deal_payload)
            deal_records = resp.get("data", [])
            crm_deal_id = ""
            if deal_records:
                crm_deal_id = str(deal_records[0].get("details", {}).get("id", ""))
            db.update_crm_push_status(
                conn,
                deal_push_id,
                "pushed",
                crm_record_id=crm_deal_id,
                pushed_at=utc_now_iso(),
                commit=False,
            )
            result["deal_created"] = True
        except (ZohoPushError, Exception) as exc:
            logger.warning("zoho: failed to create deal for %s: %s", domain, exc)
            db.update_crm_push_status(
                conn,
                deal_push_id,
                "failed",
                error_summary=str(exc)[:500],
                commit=False,
            )

    # --- Tags ---
    if zoho_account_id:
        tags = build_tags(tier, top_reasons)
        client.add_tags("Accounts", zoho_account_id, tags)

    conn.commit()
    return result


def run_zoho_push(
    conn,
    settings: Settings,
    run_id: str,
) -> dict[str, Any]:
    """Push all eligible scored accounts to Zoho CRM.

    Returns summary dict with counts.
    """
    if not settings.zoho_push_enabled:
        logger.info("zoho_push: disabled — set SIGNALS_ZOHO_PUSH_ENABLED=1")
        return {"pushed": 0, "skipped": 0, "failed": 0, "deals": 0, "contacts": 0}

    client = ZohoClient(settings)
    if not client.is_configured:
        logger.info("zoho_push: skipped — Zoho credentials not configured")
        return {"pushed": 0, "skipped": 0, "failed": 0, "deals": 0, "contacts": 0}

    accounts = db.get_accounts_eligible_for_crm_push(conn, run_id)
    logger.info("zoho_push: %d accounts eligible for CRM push", len(accounts))

    pushed = 0
    skipped = 0
    failed = 0
    deals = 0
    contacts = 0

    for account in accounts:
        account_id = account["account_id"]
        tier = account["tier"]
        score = float(account["score"])
        confidence_band = _classify_confidence(score, tier)

        # Check push policy.
        if not _should_auto_push(tier, confidence_band, settings.zoho_auto_push_tiers):
            logger.debug(
                "zoho_push: skipping %s (tier=%s, confidence=%s)",
                account["domain"],
                tier,
                confidence_band,
            )
            # Log skip.
            skip_push_id = stable_hash(
                {"account_id": account_id, "run_id": run_id, "type": "account", "skip": True},
                prefix="crm",
            )
            db.insert_crm_push_log(
                conn,
                skip_push_id,
                account_id,
                run_id,
                "account",
                "skipped",
                tier=tier,
                confidence_band=confidence_band,
                commit=False,
            )
            skipped += 1
            continue

        # Check idempotency — skip if already pushed in this run.
        if db.was_account_pushed_to_crm(conn, account_id):
            logger.debug("zoho_push: already pushed %s", account["domain"])
            skipped += 1
            continue

        result = _push_account(client, conn, account, run_id, settings)
        if result["account_pushed"]:
            pushed += 1
            contacts += result["contacts_pushed"]
            if result["deal_created"]:
                deals += 1
        else:
            failed += 1

    conn.commit()
    summary = {
        "pushed": pushed,
        "skipped": skipped,
        "failed": failed,
        "deals": deals,
        "contacts": contacts,
    }
    logger.info("zoho_push: %s", summary)
    return summary
