"""Warm path scoring — 4-tier system.

Tier 1 — Direct connection (score 1.0):
    Team member has a 1st-degree LinkedIn connection whose URL or full name
    matches the discovered contact.

Tier 2 — Company insider (score 0.6):
    A team member's connection currently works at the target company
    (connection_company ≈ company domain / name).

Tier 3 — Past colleague (score 0.3):
    A connection in the internal network previously worked at the target
    company (connection's past_companies contains the domain keyword).

Tier 4 — Education overlap (score 0.2):
    A connection shares the same college/university as the contact
    (e.g. both IIT Bombay alumni, stored in connection.education).

Tiers are applied in descending priority.  Once a higher-value tier fires,
lower tiers for the *same contact* are still evaluated and reasons are
appended — but the score is capped at 1.0.
"""

from __future__ import annotations

import logging

from src import db

logger = logging.getLogger(__name__)

# ── Tier scores ───────────────────────────────────────────────────────────────
_TIER1_DIRECT_SCORE = 1.0    # Direct 1st-degree LinkedIn connection
_TIER2_INSIDER_SCORE = 0.6   # Connection currently works at target company
_TIER3_PAST_COL_SCORE = 0.3  # Connection previously worked at target company
_TIER4_EDUCATION_SCORE = 0.2 # Shared educational institution


def compute_warm_paths(
    conn,
    contacts: list[dict],
    account_domain: str,
    company_name: str = "",
) -> list[dict]:
    """Score contacts against internal network for warm path intelligence.

    For each contact:
      Tier 1 — LinkedIn URL or full-name match in internal_network
      Tier 2 — Any connection currently at the target company
      Tier 3 — Any connection whose past_companies includes the domain
      Tier 4 — Any connection with matching education

    Returns the same contacts list with ``warmth_score`` and
    ``warm_path_reason`` populated.
    """
    # Ensure warmth keys on every contact regardless of matches
    for contact in contacts:
        contact.setdefault("warmth_score", 0.0)
        contact.setdefault("warm_path_reason", "")

    # Quick bail-out when network is empty (avoids N queries on pristine DB)
    row = conn.execute("SELECT COUNT(*) AS cnt FROM internal_network").fetchone()
    if not row or dict(row)["cnt"] == 0:
        logger.info("warm_path: internal_network is empty — skipping warm path scoring")
        return contacts

    # ── Tier 2 pre-fetch: insiders at this company (shared across all contacts)
    insiders: list[dict] = []
    if account_domain or company_name:
        insiders = db.find_insiders_at_company(conn, account_domain, company_name)
        if insiders:
            logger.info(
                "warm_path: found %d insiders at domain=%s",
                len(insiders),
                account_domain,
            )

    # Domain keyword for Tier 3 — use the significant part of the domain
    domain_keyword = _extract_domain_keyword(account_domain)

    for contact in contacts:
        warmth = 0.0
        reasons: list[str] = []

        full_name = (
            f"{contact.get('first_name', '')} {contact.get('last_name', '')}".strip()
        )
        linkedin_url = (contact.get("linkedin_url") or "").strip()

        # ── Tier 1: Direct connection ─────────────────────────────────────
        direct_matches = db.find_network_matches(conn, full_name, linkedin_url)
        for match in direct_matches:
            team_member = match.get("team_member", "Talvinder")
            match_type = match.get("match_type", "")
            if match_type == "linkedin":
                warmth = max(warmth, _TIER1_DIRECT_SCORE)
                reasons.append(f"🔗 Direct connection via {team_member}")
            elif match_type == "name":
                # Name match gives a slightly lower confidence than Tier 1
                warmth = max(warmth, _TIER1_DIRECT_SCORE - 0.1)
                reasons.append(f"👤 Likely connection via {team_member}")

        # ── Tier 2: Company insider ───────────────────────────────────────
        if insiders and warmth < _TIER2_INSIDER_SCORE:
            # Pick the first insider — all are currently at the target company
            insider = insiders[0]
            team_member = insider.get("team_member", "Talvinder")
            insider_name = insider.get("connection_name", "")
            warmth = max(warmth, _TIER2_INSIDER_SCORE)
            reasons.append(
                f"🏢 Insider: {team_member} → {insider_name} works at {company_name or account_domain}"
            )

        # ── Tier 3: Past colleague ────────────────────────────────────────
        if domain_keyword and warmth < _TIER3_PAST_COL_SCORE:
            past_col_rows = conn.execute(
                """
                SELECT * FROM internal_network
                WHERE past_companies != ''
                  AND LOWER(past_companies) LIKE %s
                LIMIT 3
                """,
                (f"%{domain_keyword}%",),
            ).fetchall()
            for row in past_col_rows:
                m = dict(row)
                team_member = m.get("team_member", "Talvinder")
                conn_name = m.get("connection_name", "")
                warmth = max(warmth, _TIER3_PAST_COL_SCORE)
                reasons.append(
                    f"👥 Past colleague: {conn_name} ({team_member}) previously at {account_domain}"
                )
                break  # one reason is enough per tier

        # ── Tier 4: Education overlap ─────────────────────────────────────
        contact_education = (contact.get("education") or "").strip()
        if contact_education:
            edu_matches = db.find_education_matches(conn, contact_education)
            if edu_matches and warmth < _TIER4_EDUCATION_SCORE:
                m = edu_matches[0]
                team_member = m.get("team_member", "Talvinder")
                conn_name = m.get("connection_name", "")
                warmth = max(warmth, _TIER4_EDUCATION_SCORE)
                reasons.append(
                    f"🎓 Alumni: {conn_name} ({team_member}) also {contact_education}"
                )

        contact["warmth_score"] = min(warmth, 1.0)
        contact["warm_path_reason"] = " | ".join(reasons)

    return contacts


def _extract_domain_keyword(domain: str) -> str:
    """Return the most significant part of a domain for substring matching.

    e.g. "tatadigital.com" → "tatadigital"
         "hul.co.in"       → ""  (too short, skipped)
         "infosys.com"     → "infosys"
    """
    clean = (domain or "").lower().replace("www.", "").split(".")[0]
    return clean if len(clean) > 3 else ""
