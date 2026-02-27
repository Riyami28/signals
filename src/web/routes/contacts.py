"""Contact discovery and enrichment API routes."""

from __future__ import annotations

import json
import logging
import os

from fastapi import APIRouter, HTTPException, Query

from src import db
from src.integrations.apollo import ApolloClient, search_contacts_for_account
from src.integrations.email_verify import EmailVerifier
from src.settings import load_settings
from src.warm_path import compute_warm_paths

logger = logging.getLogger(__name__)

router = APIRouter(tags=["contacts"])

_DM_RANKING_SYSTEM_PROMPT = """\
You are an expert at identifying the best decision makers to contact at a company \
for selling DevOps, Platform Engineering, and FinOps solutions.

Given a list of contacts at a company, select the top 3 decision makers who would be \
most relevant for a sales conversation. For each selected contact, provide:
1. A semantic_role (e.g., "Budget Owner", "Technical Champion", "Executive Sponsor", \
"Engineering Lead", "Infrastructure Decision Maker")
2. An authority_score from 0.0 to 1.0 (how much buying authority they likely have)
3. A brief reason for selection

Respond ONLY with a JSON array of objects, each having:
{"index": <0-based index in the input list>, "semantic_role": "...", \
"authority_score": 0.X, "reason": "..."}

Do not include any explanation outside the JSON array."""


def _get_conn():
    settings = load_settings()
    conn = db.get_connection(settings.pg_dsn)
    db.init_db(conn)
    return conn, settings


@router.post("/contacts/{account_id}/discover")
def discover_contacts(
    account_id: str,
    limit: int = Query(25, ge=3, le=50),
    use_llm_ranking: bool = Query(True),
):
    """Discover decision makers for an account via Apollo + warm paths + LLM ranking.

    Steps:
    1. Fetch contacts from Apollo (search_contacts_for_account)
    2. Store all discovered contacts in contact_research
    3. Run warm path scoring against internal network
    4. Optionally run LLM to pick top 3 and assign semantic_role + authority_score
    5. Return the contact list
    """
    conn, settings = _get_conn()

    try:
        # Verify account exists
        row = conn.execute(
            "SELECT * FROM accounts WHERE account_id = %s", (account_id,)
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Account not found")
        account = dict(row)
        domain = account["domain"]

        # Step 1: Apollo discovery
        apollo_client = None
        if settings.apollo_api_key:
            apollo_client = ApolloClient(
                api_key=settings.apollo_api_key,
                rate_limit=settings.apollo_rate_limit,
            )

        raw_contacts = search_contacts_for_account(
            domain=domain,
            apollo_client=apollo_client,
            hunter_api_key=settings.hunter_api_key,
            tier="high",
            limit=limit,
        )

        if not raw_contacts:
            return {
                "account_id": account_id,
                "contacts": db.get_contacts_for_account(conn, account_id),
                "total_discovered": 0,
                "message": "No contacts found via Apollo. Check API key configuration.",
            }

        # Step 2: Store all discovered contacts
        for c in raw_contacts:
            c["account_id"] = account_id
            c["contact_status"] = "discovered"
            c["enrichment_source"] = c.get("enrichment_source", "apollo")
            db.upsert_single_contact(conn, c)

        # Step 3: Warm path scoring
        network_csv = os.path.join(
            settings.project_root, "config", "internal_network.csv"
        )
        if os.path.exists(network_csv):
            db.load_internal_network(conn, network_csv)
            raw_contacts = compute_warm_paths(conn, raw_contacts, domain)
            # Persist warm scores
            for c in raw_contacts:
                if c.get("warmth_score", 0) > 0:
                    c["account_id"] = account_id
                    db.upsert_single_contact(conn, c)

        # Step 4: Optional LLM ranking
        if use_llm_ranking and (settings.claude_api_key or settings.minimax_api_key):
            try:
                _llm_rank_and_persist(conn, settings, raw_contacts, domain, account_id)
            except Exception:
                logger.warning(
                    "LLM ranking failed for account=%s, returning unranked",
                    account_id,
                    exc_info=True,
                )

        # Step 5: Return fresh data from DB
        contacts = db.get_contacts_for_account(conn, account_id)
        return {
            "account_id": account_id,
            "contacts": contacts,
            "total_discovered": len(raw_contacts),
        }
    finally:
        conn.close()


def _llm_rank_and_persist(
    conn, settings, contacts: list[dict], domain: str, account_id: str
) -> None:
    """Use LLM to rank contacts, persist top 3 with semantic_role + authority_score."""
    from src.research.client import create_research_client

    client = create_research_client(settings)

    contact_lines = []
    for i, c in enumerate(contacts):
        warmth_info = ""
        if c.get("warmth_score", 0) > 0:
            warmth_info = f" [WARM: {c.get('warm_path_reason', '')}]"
        line = (
            f"{i}. {c.get('first_name', '')} {c.get('last_name', '')} "
            f"- {c.get('title', 'Unknown')} "
            f"({c.get('management_level', 'IC')}){warmth_info}"
        )
        contact_lines.append(line)

    user_prompt = (
        f"Company domain: {domain}\n\n"
        f"Contacts found ({len(contacts)}):\n"
        + "\n".join(contact_lines)
    )

    response = client.research_company(_DM_RANKING_SYSTEM_PROMPT, user_prompt)
    raw = response.raw_text.strip()

    # Strip markdown fencing if present
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[-1].rsplit("```", 1)[0].strip()

    ranked = json.loads(raw)
    if not isinstance(ranked, list):
        return

    for rc in ranked[:3]:
        idx = rc.get("index", -1)
        if 0 <= idx < len(contacts):
            contact = contacts[idx]
            contact["semantic_role"] = rc.get("semantic_role", "")
            contact["authority_score"] = float(rc.get("authority_score", 0.0))
            contact["contact_status"] = "ranked"
            contact["account_id"] = account_id
            db.upsert_single_contact(conn, contact)


@router.post("/contacts/{contact_id}/enrich")
def enrich_contact(contact_id: str):
    """Waterfall enrichment for a single contact.

    Steps:
    1. Load contact from DB
    2. If no email: try Apollo enrich -> Hunter email finder
    3. If email found: run email verification
    4. Update contact in DB with results
    5. Return updated contact
    """
    conn, settings = _get_conn()

    try:
        contact = db.get_contact_by_id(conn, contact_id)
        if not contact:
            raise HTTPException(status_code=404, detail="Contact not found")

        row = conn.execute(
            "SELECT domain FROM accounts WHERE account_id = %s",
            (contact["account_id"],),
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Account not found")
        domain = dict(row)["domain"]

        email = (contact.get("email") or "").strip()
        enrichment_source = contact.get("enrichment_source", "")

        # Step 2a: Try Apollo enrich if we have an email but want more data
        if email and settings.apollo_api_key:
            apollo = ApolloClient(
                api_key=settings.apollo_api_key,
                rate_limit=settings.apollo_rate_limit,
            )
            enriched = apollo.enrich_person(email)
            if enriched:
                updates = {}
                if enriched.title and not contact.get("title"):
                    updates["title"] = enriched.title
                if enriched.linkedin_url and not contact.get("linkedin_url"):
                    updates["linkedin_url"] = enriched.linkedin_url
                if enriched.management_level != "IC":
                    updates["management_level"] = enriched.management_level
                if updates:
                    updates["enrichment_source"] = "apollo"
                    db.update_contact_enrichment(conn, contact_id, updates)
                    enrichment_source = "apollo"

        # Step 2b: If no email, try Hunter
        if not email and settings.hunter_api_key:
            from src.integrations.apollo import find_email_via_hunter

            found_email = find_email_via_hunter(
                domain,
                contact.get("first_name", ""),
                contact.get("last_name", ""),
                settings.hunter_api_key,
            )
            if found_email:
                email = found_email
                enrichment_source = (enrichment_source + "+hunter").lstrip("+")
                db.update_contact_enrichment(conn, contact_id, {
                    "email": email,
                    "enrichment_source": enrichment_source,
                })

        # Step 3: Email verification
        verification_status = ""
        email_verified = False
        if email:
            verifier = EmailVerifier(settings)
            if verifier.is_configured:
                result = verifier.verify_with_retry(email)
                email_verified = result.email_verified
                verification_status = result.status.value

                if not result.should_store:
                    email = ""
                    logger.info(
                        "contact enrichment: rejected email for contact=%s status=%s",
                        contact_id,
                        verification_status,
                    )

        # Step 4: Final DB update
        final_status = "verified" if email and email_verified else "enriched"
        db.update_contact_enrichment(conn, contact_id, {
            "email": email,
            "email_verified": email_verified,
            "verification_status": verification_status,
            "enrichment_source": enrichment_source,
            "contact_status": final_status,
        })

        # Step 5: Return updated contact
        updated = db.get_contact_by_id(conn, contact_id)
        return {"contact": updated}
    finally:
        conn.close()
