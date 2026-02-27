"""Warm path scoring for contact decision makers.

Cross-references discovered contacts against the internal_network table
(team LinkedIn connections) to calculate warmth scores.

Scoring:
    LinkedIn URL match  → +0.6
    Name match          → +0.4
    Company overlap     → +0.2  (team member worked at the same company)
    Cap at 1.0
"""

from __future__ import annotations

import logging

from src import db

logger = logging.getLogger(__name__)


def compute_warm_paths(
    conn,
    contacts: list[dict],
    account_domain: str,
) -> list[dict]:
    """Score contacts against internal network for warm path intelligence.

    For each contact, checks:
      1. LinkedIn URL match against internal_network
      2. Name match against internal_network
      3. Company overlap (account's domain appears in team member's past_companies)

    Returns the same contacts list with ``warmth_score`` and ``warm_path_reason``
    populated.
    """
    # Always ensure warmth keys exist on every contact
    for contact in contacts:
        contact.setdefault("warmth_score", 0.0)
        contact.setdefault("warm_path_reason", "")

    # Check if internal_network has any rows (avoid N queries on empty table)
    row = conn.execute("SELECT COUNT(*) AS cnt FROM internal_network").fetchone()
    if not row or dict(row)["cnt"] == 0:
        return contacts

    domain_lower = (account_domain or "").lower().split(".")[0]  # e.g. "tatadigital"

    for contact in contacts:
        warmth = 0.0
        reasons: list[str] = []

        full_name = f"{contact.get('first_name', '')} {contact.get('last_name', '')}".strip()
        linkedin_url = (contact.get("linkedin_url") or "").strip()

        matches = db.find_network_matches(conn, full_name, linkedin_url)

        for match in matches:
            team_member = match.get("team_member", "")

            if match["match_type"] == "linkedin":
                warmth += 0.6
                reasons.append(f"Direct connection via {team_member}")
            elif match["match_type"] == "name":
                warmth += 0.4
                reasons.append(f"Possible connection via {team_member}")

            # Company overlap check
            past = (match.get("past_companies") or "").lower()
            if domain_lower and domain_lower in past:
                warmth += 0.2
                reasons.append(f"{team_member} previously at {account_domain}")

        contact["warmth_score"] = min(warmth, 1.0)
        contact["warm_path_reason"] = " | ".join(reasons)

    return contacts
