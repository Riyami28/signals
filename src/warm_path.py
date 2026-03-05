"""Warm path scoring — 4-tier system with fuzzy matching.

Tier 1 — Direct connection (score 1.0):
    Team member has a 1st-degree LinkedIn connection whose URL or full name
    matches the discovered contact using fuzzy matching.
    - LinkedIn URL: Normalized comparison (90%+ similarity required)
    - Full name: Token-based fuzzy matching (85%+ similarity required)

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
from urllib.parse import urlparse, parse_qs

from rapidfuzz import fuzz

from src import db

logger = logging.getLogger(__name__)

# ── Tier scores ───────────────────────────────────────────────────────────────
_TIER1_DIRECT_SCORE = 1.0  # Direct 1st-degree LinkedIn connection
_TIER2_INSIDER_SCORE = 0.6  # Connection currently works at target company
_TIER3_PAST_COL_SCORE = 0.3  # Connection previously worked at target company
_TIER4_EDUCATION_SCORE = 0.2  # Shared educational institution


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

        full_name = f"{contact.get('first_name', '')} {contact.get('last_name', '')}".strip()
        linkedin_url = (contact.get("linkedin_url") or "").strip()

        # ── Tier 1: Direct connection (fuzzy matching) ────────────────────────
        direct_matches = _find_network_matches_fuzzy(
            conn,
            contact_name=full_name,
            linkedin_url=linkedin_url,
            linkedin_threshold=90.0,  # 90%+ for LinkedIn matches
            name_threshold=85.0,      # 85%+ for name matches
        )
        for match in direct_matches:
            team_member = match.get("team_member", "Talvinder")
            match_type = match.get("match_type", "")
            confidence = match.get("confidence", 0.0)

            if match_type == "linkedin":
                warmth = max(warmth, _TIER1_DIRECT_SCORE)
                reasons.append(f"🔗 Direct connection via {team_member} ({confidence:.0f}% confident)")
            elif match_type == "name":
                # Name match gives slightly lower confidence (but still direct tier)
                name_score = (_TIER1_DIRECT_SCORE * confidence) / 100.0
                warmth = max(warmth, name_score)
                reasons.append(f"👤 Likely connection via {team_member} ({confidence:.0f}% name match)")

        # ── Tier 2: Company insider ───────────────────────────────────────
        # STRICT: Only apply Tier 2 if contact ALREADY has a Tier 1 match.
        # This prevents randomly scoring all employees of a company just because
        # one connection works there (e.g., don't give every Coca-Cola employee
        # 0.6 warmth just because Aayush Tuteja works there).
        if warmth > 0 and insiders and warmth < _TIER2_INSIDER_SCORE:
            insider = insiders[0]
            team_member = insider.get("team_member", "Talvinder")
            insider_name = insider.get("connection_name", "")
            warmth = max(warmth, _TIER2_INSIDER_SCORE)
            reasons.append(f"🏢 Insider: {team_member} → {insider_name} works at {company_name or account_domain}")

        # ── Tier 3: Past colleague ────────────────────────────────────────
        # Fires when someone in the network PREVIOUSLY worked at the target company.
        # This is a company-level signal (not contact-level) so it fires without Tier 1.
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
                reasons.append(f"👥 Past colleague: {conn_name} ({team_member}) previously at {account_domain}")
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
                reasons.append(f"🎓 Alumni: {conn_name} ({team_member}) also {contact_education}")

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


def _normalize_linkedin_url(url: str) -> str:
    """Normalize LinkedIn URL for comparison.

    Removes trailing slashes, query parameters, fragments, and normalizes protocol.
    e.g. "https://linkedin.com/in/john-smith?utm=123" → "linkedin.com/in/john-smith"
    """
    if not url:
        return ""

    url = url.strip()
    # Remove protocol
    if "://" in url:
        url = url.split("://", 1)[1]

    # Parse and remove query params and fragments
    if "?" in url:
        url = url.split("?")[0]
    if "#" in url:
        url = url.split("#")[0]

    # Remove trailing slashes
    url = url.rstrip("/").lower()

    return url


def _fuzzy_match_linkedin(url1: str, url2: str, threshold: float = 95.0) -> tuple[bool, float]:
    """Fuzzy match two LinkedIn URLs.

    Compares normalized URLs. Returns (is_match, confidence_score).
    Threshold default 95% = very high confidence required to avoid false positives
    e.g. "abhijeetmehrotra" vs "abhijeetmalhotra" scores 93.8% — correctly rejected.
    """
    if not url1 or not url2:
        return False, 0.0

    norm1 = _normalize_linkedin_url(url1)
    norm2 = _normalize_linkedin_url(url2)

    if not norm1 or not norm2:
        return False, 0.0

    # First try exact match
    if norm1 == norm2:
        return True, 100.0

    # Fall back to fuzzy ratio
    score = fuzz.ratio(norm1, norm2)
    is_match = score >= threshold
    return is_match, float(score)


def _fuzzy_match_name(name1: str, name2: str, threshold: float = 85.0) -> tuple[bool, float]:
    """Fuzzy match two names with STRICT validation of first and last names.

    IMPORTANT: Requires BOTH first name AND last name to match individually.
    This prevents false positives like "Abhijeet Mehrotra" matching "Abhijeet Malhotra".

    Strategy:
    1. Split names into first/last parts
    2. Match first names (must be 85%+)
    3. Match last names (must be 85%+)
    4. Overall score = min(first_match, last_match)

    Returns (is_match, confidence_score).

    Examples:
        "John Smith" vs "John Smith" → 100% match ✓
        "John Smith" vs "John Robert Smith" → 90%+ match ✓
        "Abhijeet Mehrotra" vs "Abhijeet Malhotra" → 75% (fails last name match) ✗
        "Robert Wilson" vs "Bob Wilson" → ~70% on first name (fails) ✗
    """
    if not name1 or not name2:
        return False, 0.0

    name1_clean = name1.strip().lower()
    name2_clean = name2.strip().lower()

    if not name1_clean or not name2_clean:
        return False, 0.0

    # Exact match
    if name1_clean == name2_clean:
        return True, 100.0

    # Split into first and last names
    parts1 = name1_clean.split()
    parts2 = name2_clean.split()

    if len(parts1) < 2 or len(parts2) < 2:
        # Single-word names: fall back to simple token matching with higher threshold
        score = fuzz.token_set_ratio(name1_clean, name2_clean)
        is_match = score >= (threshold + 10)  # Stricter for single-word names
    else:
        # Multi-part names: match BOTH first and last names separately
        first1 = parts1[0]
        last1 = parts1[-1]
        first2 = parts2[0]
        last2 = parts2[-1]

        # Match first names
        first_score = fuzz.ratio(first1, first2)
        # Match last names
        last_score = fuzz.ratio(last1, last2)

        # BOTH must pass the threshold
        first_passes = first_score >= threshold
        last_passes = last_score >= threshold

        if first_passes and last_passes:
            # Both first and last names match — use minimum confidence
            score = min(first_score, last_score)
            is_match = True
        else:
            # One or both names don't match — reject
            score = min(first_score, last_score)
            is_match = False

            logger.debug(
                "warm_path.fuzzy_name_match: rejected name pair - "
                "first '%s' vs '%s' (%.1f%%) last '%s' vs '%s' (%.1f%%)",
                first1,
                first2,
                first_score,
                last1,
                last2,
                last_score,
            )

    logger.debug(
        "warm_path.fuzzy_name_match: name1=%r name2=%r score=%.1f is_match=%s",
        name1,
        name2,
        score,
        is_match,
    )

    return is_match, float(score)


def _find_network_matches_fuzzy(
    conn,
    contact_name: str,
    linkedin_url: str,
    linkedin_threshold: float = 90.0,
    name_threshold: float = 85.0,
) -> list[dict]:
    """Find internal network matches using fuzzy matching.

    STRICT MODE: LinkedIn URL matches MUST also pass name validation to avoid
    false positives (e.g., "abhijeetmehrotra" matching "abhijeetmalhotra").

    Returns list of matches sorted by confidence (highest first).
    Each match includes: team_member, connection_name, match_type, confidence.
    """
    matches = []

    # Strategy 1: Fuzzy LinkedIn URL match (highest confidence)
    # BUT: Only accept if BOTH URL AND NAME match (avoid false positives)
    if linkedin_url and contact_name:
        rows = conn.execute("SELECT * FROM internal_network").fetchall()
        for row in rows:
            net_row = dict(row)
            net_url = (net_row.get("connection_linkedin_url") or "").strip()
            net_name = (net_row.get("connection_name") or "").strip()

            if net_url and net_name:
                # Check LinkedIn URL match (strict: 95%+ threshold)
                url_is_match, url_score = _fuzzy_match_linkedin(
                    linkedin_url, net_url, threshold=95.0
                )

                if url_is_match:
                    # ADDITIONAL VALIDATION: Name must also match reasonably well
                    # This prevents "Abhijeet Mehrotra" matching "Abhijeet Malhotra"
                    name_is_match, name_score = _fuzzy_match_name(
                        contact_name, net_name, threshold=name_threshold
                    )

                    if name_is_match:
                        # Both URL and name match = high confidence
                        net_row["match_type"] = "linkedin"
                        net_row["confidence"] = min(url_score, name_score)
                        matches.append(net_row)
                    else:
                        # URL match but name doesn't = reject (likely false positive)
                        logger.debug(
                            "warm_path: Rejected LinkedIn match: URL matched %s (%.1f) "
                            "but name '%s' didn't match '%s' (%.1f)",
                            net_url,
                            url_score,
                            contact_name,
                            net_name,
                            name_score,
                        )

    # Strategy 2: Fuzzy name match (if no LinkedIn matches or as additional validation)
    if contact_name:
        rows = conn.execute("SELECT * FROM internal_network").fetchall()
        for row in rows:
            net_row = dict(row)
            net_name = (net_row.get("connection_name") or "").strip()
            if net_name:
                is_match, score = _fuzzy_match_name(contact_name, net_name, threshold=name_threshold)
                if is_match:
                    # Check if already matched via LinkedIn (avoid duplicates)
                    already_matched = any(m.get("network_id") == net_row.get("network_id") for m in matches)
                    if not already_matched:
                        net_row["match_type"] = "name"
                        net_row["confidence"] = score
                        matches.append(net_row)

    # Sort by confidence (descending)
    matches.sort(key=lambda m: m.get("confidence", 0.0), reverse=True)

    # Log matches
    if matches:
        logger.info(
            "warm_path.find_network_matches_fuzzy: contact=%r found %d matches",
            contact_name,
            len(matches),
        )
        for m in matches:
            logger.debug(
                "  - %s (team_member=%s, confidence=%.1f, type=%s)",
                m.get("connection_name"),
                m.get("team_member"),
                m.get("confidence", 0.0),
                m.get("match_type"),
            )

    return matches
