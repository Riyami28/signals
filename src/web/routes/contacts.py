"""Contact discovery and enrichment API routes.

Full pipeline for a given account:

  POST /api/contacts/{account_id}/discover
    1. Broad Apollo fetch by department + seniority (not exact title matching)
    2. Store all discovered contacts in contact_research
    3. Run warm path scoring against internal_network CSV
    4. LLM semantic filter → picks top 3, assigns semantic_role + authority_score
    5. SERP verification on those top 3 only (cost-effective)
    6. If stale data detected, promote next contact from ranked list
    7. Return full contact list, ranked first

  POST /api/contacts/{contact_id}/enrich
    Waterfall enrichment: Apollo enrich → Hunter email finder → email verification
"""

from __future__ import annotations

import json
import logging
import os
import re

from fastapi import APIRouter, HTTPException, Query

from src import db
from src.integrations.apollo import (
    BROAD_DEPARTMENTS,
    BROAD_SENIORITIES,
    ApolloClient,
    find_email_via_hunter,
    search_contacts_for_account,
)
from src.integrations.email_verify import EmailVerifier
from src.integrations.lusha import LushaClient
from src.integrations.serp_discover import SerpDiscoverer
from src.integrations.serp_verify import SerpVerifier
from src.settings import load_settings
from src.warm_path import compute_warm_paths

logger = logging.getLogger(__name__)

router = APIRouter(tags=["contacts"])

_DM_RANKING_SYSTEM_PROMPT = """\
You are an expert at identifying the best decision makers to contact at a company \
for selling DevOps, Platform Engineering, and FinOps solutions.

Given a broad list of senior contacts at a company (fetched by department and seniority), \
your job is to semantically identify the top 3 most likely PURCHASING DECISION MAKERS.

Key insight: ignore exact keywords — a "Principal Infrastructure Ninja" or \
"Director of Digital Transformation" has the same buying power as a "VP of Cloud".
Look for titles that indicate BUDGET AUTHORITY, TECHNICAL STRATEGY, or EXECUTIVE SPONSORSHIP.

For each of the top 3 selected contacts, provide:
1. A semantic_role (e.g., "Budget Owner", "Technical Champion", "Executive Sponsor", \
"Engineering Lead", "Infrastructure Decision Maker", "FinOps Champion")
2. An authority_score from 0.0 to 1.0 (how much buying authority they likely have)
3. A brief reason for selection (1 sentence)

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
    limit: int = Query(50, ge=3, le=100),
    use_llm_ranking: bool = Query(True),
    use_serp_verify: bool = Query(True),
):
    """Discover decision makers for an account.

    Steps:
    1. Broad Apollo fetch (department + seniority, not exact titles) → 20-50 people
    2. Store all discovered contacts in contact_research
    3. Warm path scoring against internal network CSV
    4. LLM semantic filter → picks top 3, assigns semantic_role + authority_score
    5. SERP verification on top 3 only (verify they still work there)
    6. If SERP flags stale data, promote next contact from the ranked list
    7. Return contacts (ranked first, then discovered by warmth_score desc)
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
        company_name = account.get("company_name", domain)

        # ── Step 1: Broad Apollo fetch ─────────────────────────────────────
        apollo_client = None
        if settings.apollo_api_key:
            apollo_client = ApolloClient(
                api_key=settings.apollo_api_key,
                rate_limit=settings.apollo_rate_limit,
            )

        raw_contacts = _broad_fetch(domain, company_name, apollo_client, settings, limit)

        if not raw_contacts:
            return {
                "account_id": account_id,
                "contacts": db.get_contacts_for_account(conn, account_id),
                "total_discovered": 0,
                "message": (
                    "No contacts found via Apollo broad search. "
                    "Check API key configuration or try a different domain."
                ),
            }

        logger.info(
            "contacts.discover: broad_fetch domain=%s found=%d",
            domain,
            len(raw_contacts),
        )

        # ── Step 2: Store all as 'discovered' ─────────────────────────────
        for c in raw_contacts:
            c["account_id"] = account_id
            c.setdefault("contact_status", "discovered")
            c.setdefault("enrichment_source", "apollo")
            db.upsert_single_contact(conn, c)

        # ── Step 3: Warm path scoring ──────────────────────────────────────
        network_csv = os.path.join(
            settings.project_root, "config", "internal_network.csv"
        )
        if os.path.exists(network_csv):
            db.load_internal_network(conn, network_csv)
            raw_contacts = compute_warm_paths(
                conn, raw_contacts, domain, company_name=company_name
            )
            for c in raw_contacts:
                if c.get("warmth_score", 0) > 0:
                    db.upsert_single_contact(conn, c)

        # ── Step 4: LLM semantic ranking ───────────────────────────────────
        ranked_indices: list[int] = []
        if use_llm_ranking and (settings.claude_api_key or settings.minimax_api_key):
            try:
                ranked_indices = _llm_rank_and_persist(
                    conn, settings, raw_contacts, domain, account_id
                )
                logger.info(
                    "contacts.discover: llm_ranked domain=%s top=%s",
                    domain,
                    ranked_indices,
                )
            except Exception:
                logger.warning(
                    "LLM ranking failed for account=%s, returning unranked",
                    account_id,
                    exc_info=True,
                )

        # ── Step 5 & 6: SERP verification on top 3, fallback if stale ─────
        if use_serp_verify and settings.serper_api_key and ranked_indices:
            _serp_verify_and_fallback(
                conn=conn,
                settings=settings,
                raw_contacts=raw_contacts,
                ranked_indices=ranked_indices,
                company_name=company_name,
                domain=domain,
                account_id=account_id,
            )

        # ── Step 7: Auto-enrich top 2 ranked contacts via Lusha ───────────
        # Lusha has a 10 calls/hour rate limit on the free plan — only enrich
        # the top 2 by authority_score to avoid burning the quota.
        if settings.lusha_api_key and ranked_indices:
            _lusha_enrich_top_n(
                conn=conn,
                settings=settings,
                raw_contacts=raw_contacts,
                ranked_indices=ranked_indices,
                company_name=company_name,
                domain=domain,
                account_id=account_id,
                top_n=2,
            )

        # ── Step 8: Return fresh DB data ──────────────────────────────────
        contacts = db.get_contacts_for_account(conn, account_id)
        return {
            "account_id": account_id,
            "contacts": contacts,
            "total_discovered": len(raw_contacts),
        }
    finally:
        conn.close()


def _broad_fetch(
    domain: str,
    company_name: str,
    apollo_client: ApolloClient | None,
    settings,
    limit: int,
) -> list[dict]:
    """Broad contact fetch — Apollo first, Serper SERP as fallback.

    Priority order:
      1. Apollo broad search (department + seniority) — best data quality
      2. Serper LinkedIn SERP search                  — free, 1 credit per call
      3. Apollo title-based search                    — legacy fallback
    """
    # ── Option 1: Apollo broad search ─────────────────────────────────────
    if apollo_client is not None:
        result = apollo_client.search_people_broad(
            domain=domain,
            departments=BROAD_DEPARTMENTS,
            seniority_levels=BROAD_SENIORITIES,
            limit=limit,
        )
        if result.contacts:
            logger.info(
                "contacts._broad_fetch: apollo returned %d contacts domain=%s",
                len(result.contacts),
                domain,
            )
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
                    "enrichment_source": "apollo",
                }
                for c in result.contacts
                if c.first_name and c.last_name
            ]
        # Apollo returned nothing — fall through to Serper
        logger.info(
            "contacts._broad_fetch: apollo returned 0, trying serper domain=%s",
            domain,
        )

    # ── Option 2: Serper LinkedIn SERP discovery ───────────────────────────
    if settings.serper_api_key:
        discoverer = SerpDiscoverer(settings.serper_api_key)
        serp_contacts = discoverer.discover_people(
            company_name=company_name,
            domain=domain,
            limit=limit,
        )
        if serp_contacts:
            logger.info(
                "contacts._broad_fetch: serper returned %d contacts company=%r",
                len(serp_contacts),
                company_name,
            )
            return [
                {
                    "first_name": c.first_name,
                    "last_name": c.last_name,
                    "title": c.title,
                    "email": "",           # SERP has no email — enrichment step fills this
                    "linkedin_url": c.linkedin_url,
                    "management_level": c.management_level,
                    "year_joined": None,
                    "department": "",
                    "enrichment_source": "serp",
                    # Pre-flag stale contacts so LLM can deprioritise them
                    "employment_verified": False if c.is_stale else None,
                    "employment_note": f"Stale flag from SERP title: {c.snippet[:80]}" if c.is_stale else "",
                }
                for c in serp_contacts
                if c.first_name and c.last_name
            ]

    # ── Option 3: Apollo title-based (legacy) ─────────────────────────────
    logger.info(
        "contacts._broad_fetch: falling back to title-based search domain=%s", domain
    )
    return search_contacts_for_account(
        domain=domain,
        apollo_client=apollo_client,
        hunter_api_key=settings.hunter_api_key,
        tier="high",
        limit=min(limit, 25),
    )


def _llm_rank_and_persist(
    conn, settings, contacts: list[dict], domain: str, account_id: str
) -> list[int]:
    """Use LLM to semantically rank contacts, persist top 3.

    Returns list of 0-based indices of the contacts chosen as top 3.
    """
    from src.research.client import create_research_client

    client = create_research_client(settings)

    contact_lines = []
    for i, c in enumerate(contacts):
        warmth_info = ""
        if c.get("warmth_score", 0) > 0:
            warmth_info = f" [WARM: {c.get('warm_path_reason', '')}]"
        dept_info = f" | {c['department']}" if c.get("department") else ""
        line = (
            f"{i}. {c.get('first_name', '')} {c.get('last_name', '')} "
            f"- {c.get('title', 'Unknown')} "
            f"({c.get('management_level', 'IC')})"
            f"{dept_info}{warmth_info}"
        )
        contact_lines.append(line)

    user_prompt = (
        f"Company domain: {domain}\n\n"
        f"All senior contacts found ({len(contacts)} total — broad fetch):\n"
        + "\n".join(contact_lines)
    )

    response = client.research_company(_DM_RANKING_SYSTEM_PROMPT, user_prompt)
    raw = response.raw_text.strip()

    # Strip <think>...</think> reasoning block (MiniMax / o1-style models)
    if "<think>" in raw:
        raw = re.sub(r"<think>.*?</think>", "", raw, flags=re.DOTALL).strip()

    # Strip markdown fencing if present
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[-1].rsplit("```", 1)[0].strip()

    ranked = json.loads(raw)
    if not isinstance(ranked, list):
        return []

    chosen_indices = []
    for rc in ranked[:3]:
        idx = rc.get("index", -1)
        if 0 <= idx < len(contacts):
            contact = contacts[idx]
            contact["semantic_role"] = rc.get("semantic_role", "")
            contact["authority_score"] = float(rc.get("authority_score", 0.0))
            contact["contact_status"] = "ranked"
            contact["account_id"] = account_id
            db.upsert_single_contact(conn, contact)
            chosen_indices.append(idx)

    return chosen_indices


def _lusha_enrich_top_n(
    conn,
    settings,
    raw_contacts: list[dict],
    ranked_indices: list[int],
    company_name: str,
    domain: str,
    account_id: str,
    top_n: int = 2,
) -> None:
    """Enrich the top-N ranked contacts via Lusha (email + phone).

    Lusha free plan = 10 calls/hour, so we deliberately cap at 2.
    Enrichment is best-effort: failures are logged but never raise.
    """
    lusha = LushaClient(settings.lusha_api_key)
    if not lusha.is_configured:
        return

    # Pick top_n ranked contacts sorted by authority_score desc
    candidates = [
        (idx, raw_contacts[idx])
        for idx in ranked_indices
        if idx < len(raw_contacts)
    ]
    candidates.sort(
        key=lambda t: t[1].get("authority_score", 0.0),
        reverse=True,
    )

    enriched = 0
    for idx, contact in candidates[:top_n]:
        if enriched >= top_n:
            break

        first = contact.get("first_name", "")
        last = contact.get("last_name", "")
        linkedin = (contact.get("linkedin_url") or "").strip()
        existing_email = (contact.get("email") or "").strip()

        # Skip if already has a verified email
        if existing_email:
            logger.info(
                "lusha_auto: %s %s already has email=%s — skipping",
                first,
                last,
                existing_email,
            )
            continue

        logger.info(
            "lusha_auto: enriching %s %s via Lusha (call %d/%d)",
            first,
            last,
            enriched + 1,
            top_n,
        )

        try:
            result = lusha.enrich_person(
                first_name=first,
                last_name=last,
                company_name=company_name or domain,
                linkedin_url=linkedin,
            )
        except Exception:
            logger.warning("lusha_auto: unexpected error for %s %s", first, last, exc_info=True)
            continue

        if result.error == "rate_limited":
            logger.warning("lusha_auto: rate limited — stopping auto-enrich for this run")
            break

        if result.found:
            contact_id = db.upsert_single_contact(conn, {**contact, "account_id": account_id})
            updates: dict = {}
            if result.email:
                updates["email"] = result.email
                updates["enrichment_source"] = (
                    (contact.get("enrichment_source") or "") + "+lusha"
                ).lstrip("+")
            if result.phone:
                updates["phone"] = result.phone
            if updates:
                db.update_contact_enrichment(conn, contact_id, updates)
                logger.info(
                    "lusha_auto: enriched %s %s email=%s phone=%s",
                    first,
                    last,
                    bool(result.email),
                    bool(result.phone),
                )
        else:
            logger.info(
                "lusha_auto: no data found for %s %s (error=%s)",
                first,
                last,
                result.error or "not_found",
            )

        enriched += 1


def _serp_verify_and_fallback(
    conn,
    settings,
    raw_contacts: list[dict],
    ranked_indices: list[int],
    company_name: str,
    domain: str,
    account_id: str,
) -> None:
    """SERP-verify the top 3 ranked contacts. If stale, promote the next best.

    This runs SERP checks ONLY on the top 3 — not the full 50 — keeping API
    cost minimal (the whole point of the plan).

    Stale contacts get employment_verified=False and contact_status demoted
    back to 'discovered'. The next highest-authority unranked contact is then
    promoted to 'ranked'.
    """
    verifier = SerpVerifier(settings.serper_api_key)
    if not verifier.is_configured:
        return

    stale_count = 0
    for idx in list(ranked_indices):
        if idx >= len(raw_contacts):
            continue
        contact = raw_contacts[idx]
        full_name = f"{contact.get('first_name', '')} {contact.get('last_name', '')}".strip()

        verify_result = verifier.verify_employment(
            name=full_name,
            company_name=company_name,
            domain=domain,
        )

        logger.info(
            "serp_verify: name=%r domain=%s verified=%s confidence=%.2f",
            full_name,
            domain,
            verify_result.employment_verified,
            verify_result.confidence,
        )

        # Persist verification result
        contact_id = db.upsert_single_contact(conn, {**contact, "account_id": account_id})
        db.update_contact_enrichment(conn, contact_id, {
            "employment_verified": verify_result.employment_verified,
            "employment_note": verify_result.note,
        })

        if verify_result.employment_verified is False:
            # Stale — demote back to discovered, try to promote next
            stale_count += 1
            db.update_contact_enrichment(conn, contact_id, {
                "contact_status": "discovered",
                "employment_verified": False,
                "employment_note": verify_result.note,
            })
            logger.warning(
                "serp_verify: stale data flagged for %r at %s — demoted",
                full_name,
                domain,
            )
            # Promote the next unranked contact by warmth_score then authority
            _promote_next_contact(conn, raw_contacts, ranked_indices, account_id)


def _promote_next_contact(
    conn, raw_contacts: list[dict], already_ranked: list[int], account_id: str
) -> None:
    """Find the highest-authority unranked contact and promote it to 'ranked'."""
    candidates = [
        (i, c) for i, c in enumerate(raw_contacts)
        if i not in already_ranked
        and c.get("contact_status") != "ranked"
    ]
    if not candidates:
        return

    # Sort by warmth_score desc, then management_level seniority
    _seniority = {"C-Level": 5, "VP": 4, "Director": 3, "Manager": 2, "IC": 1}
    candidates.sort(
        key=lambda t: (
            t[1].get("warmth_score", 0.0),
            _seniority.get(t[1].get("management_level", "IC"), 0),
        ),
        reverse=True,
    )

    next_idx, next_contact = candidates[0]
    next_contact["contact_status"] = "ranked"
    next_contact["account_id"] = account_id
    next_contact.setdefault("semantic_role", "Promoted Decision Maker")
    next_contact.setdefault("authority_score", 0.5)
    db.upsert_single_contact(conn, next_contact)
    already_ranked.append(next_idx)
    logger.info(
        "serp_verify: promoted fallback contact %r %r to ranked",
        next_contact.get("first_name"),
        next_contact.get("last_name"),
    )


@router.post("/contacts/{contact_id}/enrich")
def enrich_contact(contact_id: str):
    """Waterfall enrichment for a single contact.

    Steps:
    1. Load contact from DB
    2. If no email: try Apollo enrich → Hunter email finder
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
            "SELECT domain, company_name FROM accounts WHERE account_id = %s",
            (contact["account_id"],),
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Account not found")
        domain = dict(row)["domain"]
        company_name = dict(row).get("company_name", domain)

        email = (contact.get("email") or "").strip()
        enrichment_source = contact.get("enrichment_source", "")
        phone = (contact.get("phone") or "").strip()

        # Step 2a: Lusha enrichment (email + phone) — try first when no email
        if not email and settings.lusha_api_key:
            lusha = LushaClient(settings.lusha_api_key)
            lusha_result = lusha.enrich_person(
                first_name=contact.get("first_name", ""),
                last_name=contact.get("last_name", ""),
                company_name=company_name,
                linkedin_url=(contact.get("linkedin_url") or "").strip(),
            )
            if lusha_result.found:
                if lusha_result.email:
                    email = lusha_result.email
                    enrichment_source = (enrichment_source + "+lusha").lstrip("+")
                    db.update_contact_enrichment(conn, contact_id, {
                        "email": email,
                        "enrichment_source": enrichment_source,
                    })
                if lusha_result.phone and not phone:
                    phone = lusha_result.phone
                    db.update_contact_enrichment(conn, contact_id, {"phone": phone})
                logger.info(
                    "enrich: lusha found email=%s phone=%s for contact=%s",
                    bool(email),
                    bool(phone),
                    contact_id,
                )
            elif lusha_result.error == "rate_limited":
                logger.warning(
                    "enrich: lusha rate limited for contact=%s — falling through to Hunter",
                    contact_id,
                )

        # Step 2b: Apollo enrich if we have an email but want more data
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

        # Step 2c: If still no email, try Hunter
        if not email and settings.hunter_api_key:
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
