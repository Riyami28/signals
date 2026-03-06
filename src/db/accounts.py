from __future__ import annotations

import json
import logging
import uuid
from datetime import date
from pathlib import Path
from typing import Any

from src.models import Account
from src.utils import load_csv_rows, normalize_domain, stable_hash, utc_now_iso

logger = logging.getLogger(__name__)


def _build_account_id(domain: str) -> str:
    return stable_hash({"domain": normalize_domain(domain)}, prefix="acc", length=12)


def get_account_by_domain(conn: Any, domain: str) -> dict[str, Any] | None:
    normalized = normalize_domain(domain)
    if not normalized:
        return None
    cur = conn.execute(
        "SELECT account_id, company_name, domain, source_type, created_at FROM accounts WHERE domain = %s",
        (normalized,),
    )
    return cur.fetchone()


def upsert_account(
    conn: Any,
    company_name: str,
    domain: str,
    source_type: str = "discovered",
    commit: bool = True,
) -> str:
    normalized_domain = normalize_domain(domain)
    if not normalized_domain:
        raise ValueError("domain is required")
    existing = get_account_by_domain(conn, normalized_domain)
    if existing:
        return str(existing["account_id"])

    account_id = _build_account_id(normalized_domain)
    account = Account(
        account_id=account_id,
        company_name=(company_name or normalized_domain).strip(),
        domain=normalized_domain,
        source_type="seed" if source_type == "seed" else "discovered",
    )
    conn.execute(
        """
        INSERT INTO accounts (account_id, company_name, domain, source_type, created_at)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (
            account.account_id,
            account.company_name,
            account.domain,
            account.source_type,
            account.created_at,
        ),
    )
    if commit:
        conn.commit()
    return account.account_id


def seed_accounts(conn: Any, seed_accounts_csv: Path) -> int:
    rows = load_csv_rows(seed_accounts_csv)
    inserted = 0
    for row in rows:
        domain = row.get("domain", "")
        if not domain:
            continue
        existing = get_account_by_domain(conn, domain)
        if existing:
            continue
        upsert_account(
            conn,
            company_name=row.get("company_name", domain),
            domain=domain,
            source_type=row.get("source_type", "seed") or "seed",
        )
        inserted += 1
    return inserted


def update_crm_status(
    conn: Any,
    account_id: str,
    crm_status: str,
    commit: bool = True,
) -> None:
    """Update the crm_status field for an account."""
    try:
        conn.execute(
            "UPDATE accounts SET crm_status = %s WHERE account_id = %s",
            (crm_status, account_id),
        )
    except Exception as exc:
        if "crm_status" in str(exc):
            conn.rollback()
            return
        raise
    if commit:
        conn.commit()


def get_crm_status(conn: Any, account_id: str) -> str:
    """Return crm_status for an account, defaulting to 'new'."""
    try:
        cur = conn.execute(
            "SELECT crm_status FROM accounts WHERE account_id = %s",
            (account_id,),
        )
    except Exception as exc:
        if "crm_status" in str(exc):
            conn.rollback()
            return "new"
        raise
    row = cur.fetchone()
    return str(row["crm_status"]) if row else "new"


def get_accounts_without_crm_check(conn: Any, limit: int = 500) -> list[dict[str, Any]]:
    """Return accounts with crm_status='new'."""
    try:
        cur = conn.execute(
            """
            SELECT account_id, company_name, domain, crm_status
            FROM accounts
            WHERE crm_status = 'new'
            ORDER BY created_at DESC
            LIMIT %s
            """,
            (limit,),
        )
    except Exception as exc:
        if "crm_status" not in str(exc):
            raise
        conn.rollback()
        cur = conn.execute(
            """
            SELECT account_id, company_name, domain, 'new' AS crm_status
            FROM accounts
            ORDER BY created_at DESC
            LIMIT %s
            """,
            (limit,),
        )
    return [dict(row) for row in cur.fetchall()]


def save_dossier(conn: Any, dossier: dict[str, Any]) -> str:
    """Persist a dossier snapshot and return dossier_id."""
    account_id = str(dossier.get("account_id", "")).strip()
    if not account_id:
        raise ValueError("account_id is required")

    cur = conn.execute(
        "SELECT COALESCE(MAX(version), 0) AS max_v FROM dossiers WHERE account_id = %s",
        (account_id,),
    )
    row = cur.fetchone()
    version = int(row["max_v"] if row and row["max_v"] is not None else 0) + 1

    dossier_id = f"dos_{uuid.uuid4().hex[:12]}"
    sections_json = json.dumps(dossier.get("sections", []))
    markdown = str(dossier.get("markdown", ""))
    dossier_type = str(dossier.get("dossier_type", "full") or "full")
    generated_at = str(dossier.get("generated_at", "") or utc_now_iso())

    conn.execute(
        """
        INSERT INTO dossiers (
            dossier_id,
            account_id,
            dossier_type,
            version,
            sections_json,
            markdown,
            generated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (dossier_id, account_id, dossier_type, version, sections_json, markdown, generated_at),
    )
    conn.commit()
    return dossier_id


def get_latest_dossier(conn: Any, account_id: str) -> dict[str, Any] | None:
    """Return most recent dossier for an account."""
    cur = conn.execute(
        """
        SELECT * FROM dossiers
        WHERE account_id = %s
        ORDER BY version DESC, generated_at DESC
        LIMIT 1
        """,
        (account_id,),
    )
    row = cur.fetchone()
    return dict(row) if row else None


def get_dossier_history(conn: Any, account_id: str, limit: int = 25) -> list[dict[str, Any]]:
    """Return dossier history for an account (newest first)."""
    cur = conn.execute(
        """
        SELECT dossier_id, account_id, dossier_type, version, generated_at
        FROM dossiers
        WHERE account_id = %s
        ORDER BY version DESC, generated_at DESC
        LIMIT %s
        """,
        (account_id, max(1, int(limit))),
    )
    return [dict(row) for row in cur.fetchall()]


def account_exists(conn: Any, account_id: str) -> bool:
    cur = conn.execute("SELECT 1 FROM accounts WHERE account_id = %s LIMIT 1", (account_id,))
    return cur.fetchone() is not None


def dump_run_summary(conn: Any, run_id: str) -> dict[str, object]:
    cur = conn.execute(
        """
        SELECT
            COUNT(*) AS score_rows,
            COUNT(DISTINCT account_id) AS account_count
        FROM account_scores
        WHERE run_id = %s
        """,
        (run_id,),
    )
    row = cur.fetchone()
    return {
        "run_id": run_id,
        "score_rows": int(row["score_rows"] if row else 0),
        "account_count": int(row["account_count"] if row else 0),
    }


# ---------------------------------------------------------------------------
# Research CRUD
# ---------------------------------------------------------------------------


def upsert_company_research(
    conn,
    account_id: str,
    *,
    research_brief: str | None = None,
    research_profile: str | None = None,
    enrichment_json: str = "{}",
    research_status: str,
    model_used: str | None = None,
    prompt_hash: str | None = None,
) -> None:
    """Insert or update a company research record."""
    conn.execute(
        """
        INSERT INTO company_research
            (account_id, research_brief, research_profile, enrichment_json,
             research_status, researched_at, model_used, prompt_hash,
             created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP, %s, %s,
                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        ON CONFLICT (account_id) DO UPDATE SET
            research_brief   = EXCLUDED.research_brief,
            research_profile = EXCLUDED.research_profile,
            enrichment_json  = EXCLUDED.enrichment_json,
            research_status  = EXCLUDED.research_status,
            researched_at    = EXCLUDED.researched_at,
            model_used       = EXCLUDED.model_used,
            prompt_hash      = EXCLUDED.prompt_hash,
            updated_at       = CURRENT_TIMESTAMP
        """,
        (account_id, research_brief, research_profile, enrichment_json, research_status, model_used, prompt_hash),
    )
    conn.commit()


def get_company_research(conn, account_id: str) -> dict | None:
    """Return the company_research row or None."""
    row = conn.execute(
        """SELECT account_id, research_brief, research_profile, enrichment_json,
                  research_status, researched_at, model_used, prompt_hash,
                  created_at, updated_at
           FROM company_research WHERE account_id = %s""",
        (account_id,),
    ).fetchone()
    return dict(row) if row else None


def get_accounts_needing_research(
    conn,
    run_date: str,
    score_run_id: str,
    max_accounts: int,
    min_tier: str,
    stale_days: int,
    current_prompt_hash: str,
) -> list[dict]:
    """
    Returns accounts that:
    1. Have a current score at min_tier or above
    2. Have no completed research, OR research older than stale_days,
       OR a different prompt_hash than current_prompt_hash
    3. Limited to max_accounts rows, ordered by signal_score DESC
    """
    tier_filter = ("high",) if min_tier == "high" else ("high", "medium")
    rows = conn.execute(
        """
        SELECT
            a.account_id,
            a.company_name,
            a.domain,
            s.score AS signal_score,
            s.tier AS signal_tier,
            s.delta_7d,
            s.top_reasons_json
        FROM account_scores s
        JOIN accounts a ON a.account_id = s.account_id
        LEFT JOIN company_research cr ON cr.account_id = a.account_id
        WHERE s.run_id = %s
          AND s.tier = ANY(%s)
          AND (
              cr.account_id IS NULL
              OR cr.research_status NOT IN ('completed', 'in_progress')
              OR cr.researched_at::timestamp < (CURRENT_TIMESTAMP - make_interval(days => %s))
              OR cr.prompt_hash IS DISTINCT FROM %s
          )
        ORDER BY s.score DESC
        LIMIT %s
        """,
        (score_run_id, list(tier_filter), stale_days, current_prompt_hash, max_accounts),
    ).fetchall()
    return [dict(r) for r in rows]


def mark_research_in_progress(conn, account_id: str) -> None:
    """Set research_status='in_progress' before making the API call."""
    conn.execute(
        """
        INSERT INTO company_research (account_id, research_status, created_at, updated_at)
        VALUES (%s, 'in_progress', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        ON CONFLICT (account_id) DO UPDATE SET
            research_status = 'in_progress',
            updated_at = CURRENT_TIMESTAMP
        """,
        (account_id,),
    )
    conn.commit()


def upsert_contacts(conn, account_id: str, contacts: list[dict]) -> None:
    """Delete all existing contacts for account, then insert new ones."""
    conn.execute(
        "DELETE FROM contact_research WHERE account_id = %s",
        (account_id,),
    )
    for contact in contacts:
        identifier = contact.get("linkedin_url") or (contact.get("first_name", "") + contact.get("last_name", ""))
        contact_id = stable_hash(
            {"account_id": account_id, "identifier": identifier},
            prefix="contact",
            length=16,
        )
        conn.execute(
            """
            INSERT INTO contact_research
                (contact_id, account_id, first_name, last_name, title,
                 email, linkedin_url, management_level, year_joined,
                 email_verified, verification_status, enrichment_source,
                 contact_status, semantic_role, authority_score,
                 warmth_score, warm_path_reason, department,
                 created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT (contact_id) DO NOTHING
            """,
            (
                contact_id,
                account_id,
                contact.get("first_name", ""),
                contact.get("last_name", ""),
                contact.get("title"),
                contact.get("email"),
                contact.get("linkedin_url"),
                contact.get("management_level"),
                contact.get("year_joined"),
                contact.get("email_verified", False),
                contact.get("verification_status", ""),
                contact.get("enrichment_source", ""),
                contact.get("contact_status", "discovered"),
                contact.get("semantic_role", ""),
                contact.get("authority_score", 0.0),
                contact.get("warmth_score", 0.0),
                contact.get("warm_path_reason", ""),
                contact.get("department", ""),
            ),
        )
    conn.commit()


def get_contacts_for_account(conn, account_id: str) -> list[dict]:
    """Return all contacts for an account, ranked first then by seniority."""
    rows = conn.execute(
        """
        SELECT * FROM contact_research
        WHERE account_id = %s
        ORDER BY
            CASE contact_status WHEN 'ranked' THEN 0 ELSE 1 END,
            authority_score DESC,
            warmth_score DESC,
            CASE management_level
                WHEN 'C-Level' THEN 1
                WHEN 'VP' THEN 2
                WHEN 'Director' THEN 3
                WHEN 'Manager' THEN 4
                WHEN 'IC' THEN 5
                ELSE 6
            END
        """,
        (account_id,),
    ).fetchall()
    return [dict(r) for r in rows]


def upsert_single_contact(conn, contact: dict) -> str:
    """Upsert a single contact, returning contact_id.

    Uses ON CONFLICT DO UPDATE to preserve enrichment data from prior runs.
    """
    account_id = contact["account_id"]
    identifier = contact.get("linkedin_url") or (contact.get("first_name", "") + contact.get("last_name", ""))
    contact_id = stable_hash(
        {"account_id": account_id, "identifier": identifier},
        prefix="contact",
        length=16,
    )
    conn.execute(
        """
        INSERT INTO contact_research
            (contact_id, account_id, first_name, last_name, title,
             email, linkedin_url, management_level, year_joined,
             email_verified, verification_status, enrichment_source,
             contact_status, semantic_role, authority_score,
             warmth_score, warm_path_reason, department,
             employment_verified, employment_note,
             created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s,
                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        ON CONFLICT (contact_id) DO UPDATE SET
            email = COALESCE(NULLIF(EXCLUDED.email, ''), contact_research.email),
            title = COALESCE(NULLIF(EXCLUDED.title, ''), contact_research.title),
            linkedin_url = COALESCE(NULLIF(EXCLUDED.linkedin_url, ''), contact_research.linkedin_url),
            management_level = COALESCE(EXCLUDED.management_level, contact_research.management_level),
            enrichment_source = CASE
                WHEN EXCLUDED.enrichment_source != '' THEN EXCLUDED.enrichment_source
                ELSE contact_research.enrichment_source END,
            contact_status = CASE
                WHEN EXCLUDED.contact_status IN ('ranked', 'enriched', 'verified')
                THEN EXCLUDED.contact_status
                ELSE contact_research.contact_status END,
            semantic_role = CASE
                WHEN EXCLUDED.semantic_role != '' THEN EXCLUDED.semantic_role
                ELSE contact_research.semantic_role END,
            authority_score = CASE
                WHEN EXCLUDED.authority_score > 0 THEN EXCLUDED.authority_score
                ELSE contact_research.authority_score END,
            warmth_score = CASE
                WHEN EXCLUDED.warmth_score > 0 THEN EXCLUDED.warmth_score
                ELSE contact_research.warmth_score END,
            warm_path_reason = CASE
                WHEN EXCLUDED.warm_path_reason != '' THEN EXCLUDED.warm_path_reason
                ELSE contact_research.warm_path_reason END,
            department = CASE
                WHEN EXCLUDED.department != '' THEN EXCLUDED.department
                ELSE contact_research.department END,
            employment_verified = COALESCE(EXCLUDED.employment_verified, contact_research.employment_verified),
            employment_note = CASE
                WHEN EXCLUDED.employment_note != '' THEN EXCLUDED.employment_note
                ELSE contact_research.employment_note END,
            updated_at = CURRENT_TIMESTAMP
        """,
        (
            contact_id,
            account_id,
            contact.get("first_name", ""),
            contact.get("last_name", ""),
            contact.get("title"),
            contact.get("email"),
            contact.get("linkedin_url"),
            contact.get("management_level"),
            contact.get("year_joined"),
            contact.get("email_verified", False),
            contact.get("verification_status", ""),
            contact.get("enrichment_source", ""),
            contact.get("contact_status", "discovered"),
            contact.get("semantic_role", ""),
            contact.get("authority_score", 0.0),
            contact.get("warmth_score", 0.0),
            contact.get("warm_path_reason", ""),
            contact.get("department", ""),
            contact.get("employment_verified"),  # None = not checked
            contact.get("employment_note", ""),
        ),
    )
    conn.commit()
    return contact_id


def update_contact_enrichment(conn, contact_id: str, updates: dict) -> bool:
    """Update specific fields on a single contact after enrichment."""
    allowed_fields = {
        "email",
        "email_verified",
        "verification_status",
        "enrichment_source",
        "contact_status",
        "linkedin_url",
        "title",
        "management_level",
        "warmth_score",
        "warm_path_reason",
        "semantic_role",
        "authority_score",
        "employment_verified",
        "employment_note",  # SERP verification
    }
    set_parts = []
    values = []
    for key, val in updates.items():
        if key in allowed_fields:
            set_parts.append(f"{key} = %s")
            values.append(val)
    if not set_parts:
        return False
    set_parts.append("updated_at = CURRENT_TIMESTAMP")
    values.append(contact_id)
    conn.execute(
        f"UPDATE contact_research SET {', '.join(set_parts)} WHERE contact_id = %s",
        tuple(values),
    )
    conn.commit()
    return True


def get_contact_by_id(conn, contact_id: str) -> dict | None:
    """Return a single contact by ID."""
    row = conn.execute(
        "SELECT * FROM contact_research WHERE contact_id = %s",
        (contact_id,),
    ).fetchone()
    return dict(row) if row else None


def load_internal_network(conn, csv_path: str) -> int:
    """Load internal network CSV into the internal_network table.

    Returns the number of rows imported.
    """
    from pathlib import Path as _Path

    rows = load_csv_rows(_Path(csv_path))
    count = 0
    for row in rows:
        team_member = (row.get("team_member") or "").strip()
        connection_name = (row.get("connection_name") or "").strip()
        if not team_member or not connection_name:
            continue
        network_id = stable_hash(
            {
                "team_member": team_member,
                "connection_name": connection_name,
                "linkedin": row.get("connection_linkedin_url", ""),
            },
            prefix="net",
            length=16,
        )
        conn.execute(
            """
            INSERT INTO internal_network
                (network_id, team_member, connection_name,
                 connection_linkedin_url, connection_title,
                 connection_company, past_companies, relationship_type,
                 education, imported_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (network_id) DO UPDATE SET
                connection_title = EXCLUDED.connection_title,
                connection_company = EXCLUDED.connection_company,
                past_companies = EXCLUDED.past_companies,
                relationship_type = EXCLUDED.relationship_type,
                education = CASE
                    WHEN EXCLUDED.education != '' THEN EXCLUDED.education
                    ELSE internal_network.education
                END
            """,
            (
                network_id,
                team_member,
                connection_name,
                row.get("connection_linkedin_url", ""),
                row.get("connection_title", ""),
                row.get("connection_company", ""),
                row.get("past_companies", ""),
                row.get("relationship_type", "connection"),
                row.get("education", ""),
            ),
        )
        count += 1
    conn.commit()
    return count


def find_network_matches(conn, contact_name: str, linkedin_url: str = "") -> list[dict]:
    """Find internal network matches for a contact by LinkedIn URL or name."""
    matches = []

    # Exact LinkedIn URL match (highest confidence)
    if linkedin_url:
        rows = conn.execute(
            "SELECT * FROM internal_network WHERE connection_linkedin_url = %s",
            (linkedin_url,),
        ).fetchall()
        for row in rows:
            m = dict(row)
            m["match_type"] = "linkedin"
            matches.append(m)

    # Name match (case-insensitive) — only if no LinkedIn match found
    if contact_name and not matches:
        rows = conn.execute(
            "SELECT * FROM internal_network WHERE LOWER(connection_name) = LOWER(%s)",
            (contact_name.strip(),),
        ).fetchall()
        for row in rows:
            m = dict(row)
            m["match_type"] = "name"
            matches.append(m)

    return matches


def find_insiders_at_company(conn, domain: str, company_name: str) -> list[dict]:
    """Tier 2: Find connections currently working at the target company.

    Searches connection_company in internal_network for keywords derived from
    the company's domain and name.  Short keywords (≤3 chars) are excluded to
    avoid false positives (e.g. "hul" matching "Rahul").
    """
    # Build keywords from domain + company name, excluding short parts
    raw_keywords: list[str] = []
    domain_clean = (domain or "").lower().replace("www.", "").split(".")[0]
    if len(domain_clean) > 3:
        raw_keywords.append(domain_clean)

    for word in (company_name or "").split():
        w = word.lower().strip(".,")
        if len(w) > 3:
            raw_keywords.append(w)

    keywords = list(dict.fromkeys(raw_keywords))  # dedup, preserve order
    if not keywords:
        return []

    insiders: list[dict] = []
    seen: set[str] = set()
    for kw in keywords:
        rows = conn.execute(
            "SELECT * FROM internal_network WHERE LOWER(connection_company) LIKE %s",
            (f"%{kw}%",),
        ).fetchall()
        for row in rows:
            d = dict(row)
            nid = d.get("network_id", "")
            if nid not in seen:
                seen.add(nid)
                d["match_type"] = "company_insider"
                insiders.append(d)

    return insiders


def find_education_matches(conn, contact_education: str) -> list[dict]:
    """Tier 4: Find connections who share the same educational institution.

    Args:
        contact_education: e.g. "IIT Bombay" or "IIT Delhi"
    """
    if not contact_education:
        return []
    edu_lower = contact_education.strip().lower()
    rows = conn.execute(
        "SELECT * FROM internal_network WHERE education != '' AND LOWER(education) LIKE %s",
        (f"%{edu_lower}%",),
    ).fetchall()
    results = []
    for row in rows:
        d = dict(row)
        d["match_type"] = "education"
        results.append(d)
    return results


def load_education_from_excel(conn, excel_path: str) -> int:
    """Load IIT Bombay education flags from LinkedIn_ICP_Analysis.xlsx.

    Reads the Excel file, finds rows where the 'IIT Bombay' column is '✓',
    and updates education = 'IIT Bombay' for matching rows in internal_network
    (matched by connection_name, case-insensitive).

    Returns the number of rows updated.
    """
    import openpyxl  # type: ignore[import]

    wb = openpyxl.load_workbook(excel_path, read_only=True, data_only=True)
    ws = wb.active

    rows_iter = ws.iter_rows(values_only=True)
    header_row = next(rows_iter, None)
    if header_row is None:
        wb.close()
        return 0

    headers = [str(h or "").strip().lower() for h in header_row]

    name_idx = next((i for i, h in enumerate(headers) if "full name" in h), None)
    iit_idx = next(
        (i for i, h in enumerate(headers) if "iit bombay" in h or "iit" in h),
        None,
    )

    if name_idx is None or iit_idx is None:
        logger.warning(
            "load_education_from_excel: could not find 'Full Name' or 'IIT Bombay' column in headers: %s",
            headers,
        )
        wb.close()
        return 0

    count = 0
    for row in rows_iter:
        if row is None or len(row) <= max(name_idx, iit_idx):
            continue
        name = str(row[name_idx] or "").strip()
        iit_flag = str(row[iit_idx] or "").strip()

        # Accept both ✓ and common ASCII equivalents
        if not name or iit_flag not in ("✓", "v", "x", "yes", "1", "true", "y"):
            continue

        result = conn.execute(
            """
            UPDATE internal_network
               SET education = 'IIT Bombay'
             WHERE LOWER(connection_name) = LOWER(%s)
               AND (education = '' OR education IS NULL)
            """,
            (name,),
        )
        count += result.rowcount

    conn.commit()
    wb.close()
    logger.info("load_education_from_excel: updated education for %d connections", count)
    return count


def insert_contact(conn, contact: dict) -> str:
    """Insert a contact into the contacts table. Returns contact_id."""
    identifier = contact.get("linkedin_url") or (contact.get("first_name", "") + contact.get("last_name", ""))
    contact_id = stable_hash(
        {"account_id": contact.get("account_id", ""), "identifier": identifier},
        prefix="ctc",
        length=16,
    )
    conn.execute(
        """
        INSERT INTO contacts
            (contact_id, account_id, first_name, last_name, title,
             email, email_verified, phone, linkedin_url,
             enrichment_source, enriched_at, confidence)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (contact_id) DO NOTHING
        """,
        (
            contact_id,
            contact.get("account_id", ""),
            contact.get("first_name", ""),
            contact.get("last_name", ""),
            contact.get("title", ""),
            contact.get("email", ""),
            contact.get("email_verified", False),
            contact.get("phone", ""),
            contact.get("linkedin_url", ""),
            contact.get("enrichment_source", ""),
            contact.get("enriched_at", ""),
            contact.get("confidence", 0.0),
        ),
    )
    conn.commit()
    return contact_id


def get_enrichment_contacts(conn, account_id: str) -> list[dict]:
    """Return all contacts from the contacts table for an account."""
    rows = conn.execute(
        "SELECT * FROM contacts WHERE account_id = %s ORDER BY confidence DESC",
        (account_id,),
    ).fetchall()
    return [dict(r) for r in rows]


def get_account_domain(conn, account_id: str) -> str:
    """Return the domain for an account, or empty string if not found."""
    row = conn.execute(
        "SELECT domain FROM accounts WHERE account_id = %s",
        (account_id,),
    ).fetchone()
    return row["domain"] if row else ""


def create_research_run(conn, run_date: str, score_run_id: str) -> str:
    """Insert a new research_runs row with status='running'. Returns research_run_id."""
    research_run_id = f"rr_{uuid.uuid4().hex[:12]}"
    conn.execute(
        """
        INSERT INTO research_runs
            (research_run_id, run_date, score_run_id, started_at, status)
        VALUES (%s, %s, %s, CURRENT_TIMESTAMP, 'running')
        """,
        (research_run_id, run_date, score_run_id),
    )
    conn.commit()
    return research_run_id


def finish_research_run(
    conn,
    research_run_id: str,
    status: str,
    accounts_attempted: int,
    accounts_completed: int,
    accounts_failed: int,
    accounts_skipped: int,
) -> None:
    """Update research_run with final counts and finished_at timestamp."""
    conn.execute(
        """
        UPDATE research_runs SET
            status = %s,
            accounts_attempted = %s,
            accounts_completed = %s,
            accounts_failed = %s,
            accounts_skipped = %s,
            finished_at = CURRENT_TIMESTAMP
        WHERE research_run_id = %s
        """,
        (status, accounts_attempted, accounts_completed, accounts_failed, accounts_skipped, research_run_id),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Account Labels (Web UI)
# ---------------------------------------------------------------------------


def insert_account_label(conn, account_id: str, label: str, reviewer: str = "web_ui", notes: str = "") -> str:
    import uuid

    label_id = f"lbl_{uuid.uuid4().hex[:12]}"
    conn.execute(
        """
        INSERT INTO account_labels (label_id, account_id, label, reviewer, notes)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (label_id) DO NOTHING
        """,
        (label_id, account_id, label, reviewer, notes),
    )
    conn.commit()
    return label_id


def delete_account_label(conn, label_id: str) -> None:
    conn.execute("DELETE FROM account_labels WHERE label_id = %s", (label_id,))
    conn.commit()


def get_labels_for_account(conn, account_id: str) -> list[dict]:
    rows = conn.execute(
        "SELECT label_id, account_id, label, reviewer, notes, created_at FROM account_labels WHERE account_id = %s ORDER BY created_at DESC",
        (account_id,),
    ).fetchall()
    return [dict(r) for r in rows]


def get_accounts_paginated(
    conn,
    page: int = 1,
    per_page: int = 50,
    sort_by: str = "score",
    sort_dir: str = "desc",
    tier_filter: str = "",
    label_filter: str = "",
    search: str = "",
    source_filter: str = "",
) -> tuple[list[dict], int]:
    """Return paginated accounts joined with latest scores and labels."""
    where_parts = []
    params: list = []

    if search:
        where_parts.append("(a.company_name ILIKE %s OR a.domain ILIKE %s)")
        params.extend([f"%{search}%", f"%{search}%"])

    if tier_filter:
        where_parts.append("best.tier = %s")
        params.append(tier_filter)

    if label_filter:
        where_parts.append(
            "EXISTS (SELECT 1 FROM account_labels al WHERE al.account_id = a.account_id AND al.label = %s)"
        )
        params.append(label_filter)

    if source_filter:
        where_parts.append(
            "EXISTS (SELECT 1 FROM signal_observations so WHERE so.account_id = a.account_id AND so.source = %s)"
        )
        params.append(source_filter)

    where_sql = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""

    sort_map = {
        "score": "COALESCE(best.score, 0)",
        "company_name": "a.company_name",
        "domain": "a.domain",
        "tier": "best.tier",
    }
    order_col = sort_map.get(sort_by, "COALESCE(best.score, 0)")
    order_dir = "DESC" if sort_dir.lower() == "desc" else "ASC"

    # Use the latest FULL run (500+ scores) as the authoritative score source.
    # This avoids two problems:
    # 1. Targeted runs (3-78 accounts) overriding full-run scores with partial data
    # 2. Old pre-cleanup runs having inflated scores from garbage signals
    _full_run_cte = """
        WITH full_run AS (
            SELECT r.run_id
            FROM score_runs r
            JOIN account_scores s ON s.run_id = r.run_id
            WHERE r.status = 'completed'
            GROUP BY r.run_id, r.started_at
            HAVING count(*) > 500
            ORDER BY r.started_at DESC
            LIMIT 1
        )
    """

    count_sql = f"""
        {_full_run_cte}
        SELECT COUNT(*) as total FROM (
            SELECT a.account_id
            FROM accounts a
            LEFT JOIN account_scores best
                ON best.account_id = a.account_id
                AND best.run_id = (SELECT run_id FROM full_run)
                AND best.product = 'zopdev'
            {where_sql}
        ) sub
    """
    total = conn.execute(count_sql, params).fetchone()["total"]

    offset = (page - 1) * per_page
    data_params = list(params) + [per_page, offset]
    data_sql = f"""
        {_full_run_cte}
        SELECT
            a.account_id, a.company_name, a.domain, a.source_type,
            COALESCE(best.score, 0) AS score,
            COALESCE(best.tier, 'low') AS tier,
            cr.research_status,
            (SELECT string_agg(al.label, ',') FROM account_labels al WHERE al.account_id = a.account_id) AS labels,
            (SELECT MAX(so.observed_at) FROM signal_observations so WHERE so.account_id = a.account_id) AS last_signal_date
        FROM accounts a
        LEFT JOIN account_scores best
            ON best.account_id = a.account_id
            AND best.run_id = (SELECT run_id FROM full_run)
            AND best.product = 'zopdev'
        LEFT JOIN company_research cr ON cr.account_id = a.account_id
        {where_sql}
        ORDER BY {order_col} {order_dir}
        LIMIT %s OFFSET %s
    """
    rows = conn.execute(data_sql, data_params).fetchall()
    return [dict(r) for r in rows], total


def get_account_detail(conn, account_id: str) -> dict | None:
    """Full account detail with scores, signals, research, contacts, labels, dimensions, velocity."""
    account = conn.execute("SELECT * FROM accounts WHERE account_id = %s", (account_id,)).fetchone()
    if not account:
        return None
    result = dict(account)

    # Get the most recent score per product across all completed runs.
    # Uses DISTINCT ON to pick the latest score per product, so single-account
    # pipeline runs don't hide scores from prior full runs.
    scores = conn.execute(
        """
        SELECT DISTINCT ON (s.product) s.product, s.score, s.tier,
               s.dimension_scores_json, s.top_reasons_json, s.confidence_band
        FROM account_scores s
        JOIN score_runs r ON s.run_id = r.run_id
        WHERE s.account_id = %s AND r.status = 'completed'
        ORDER BY s.product, r.started_at DESC
        """,
        (account_id,),
    ).fetchall()
    result["scores"] = [dict(r) for r in scores]

    # Include component_score so the UI can show per-signal contributions.
    signals = conn.execute(
        """
        SELECT so.signal_code, so.source, so.evidence_url, so.evidence_text,
               so.observed_at, so.confidence,
               sc.component_score,
               (so.observed_at::date = CURRENT_DATE) AS is_breaking,
               so.speaker_name,
               so.speaker_role,
               so.evidence_quality,
               so.relevance_score,
               so.language,
               so.evidence_sentence,
               so.evidence_sentence_en,
               doc.title        AS doc_title,
               doc.author       AS doc_author,
               doc.published_at AS doc_published_at
        FROM signal_observations so
        LEFT JOIN LATERAL (
            SELECT sc2.component_score
            FROM score_components sc2
            WHERE sc2.account_id = so.account_id
              AND sc2.signal_code = so.signal_code
              AND sc2.run_id = (
                  SELECT run_id FROM score_runs
                  WHERE status = 'completed'
                  ORDER BY started_at DESC LIMIT 1
              )
            ORDER BY sc2.component_score DESC
            LIMIT 1
        ) sc ON true
        LEFT JOIN LATERAL (
            SELECT d.title, d.author, d.published_at
            FROM observation_lineage ol
            JOIN documents d ON d.document_id = ol.document_id
            WHERE ol.obs_id = so.obs_id
            LIMIT 1
        ) doc ON true
        WHERE so.account_id = %s
          AND so.observed_at::timestamptz >= NOW() - INTERVAL '14 days'
        ORDER BY so.observed_at DESC
        LIMIT 50
        """,
        (account_id,),
    ).fetchall()
    result["signals"] = [dict(r) for r in signals]

    result["research"] = get_company_research(conn, account_id)
    result["contacts"] = get_contacts_for_account(conn, account_id)
    result["labels"] = get_labels_for_account(conn, account_id)

    # Dimension scores from the highest-scoring product row
    result["dimension_scores"] = get_dimension_scores(conn, account_id)

    # Per-dimension confidence bands and source lists
    result["dimension_confidence"] = get_dimension_confidence(conn, account_id)

    # Most recent signal date
    result["last_signal_date"] = get_last_signal_date(conn, account_id)

    # Velocity metrics
    result["velocity"] = get_account_velocity(conn, account_id)

    return result


def get_dimension_scores(conn, account_id: str) -> dict:
    """Merge dimension scores across all products (take MAX per dimension) from the latest completed run that includes this account.

    This gives the most complete picture of an account's strength across
    all dimensions, regardless of which product the signal was scored under.
    Uses the latest run that actually contains scores for the given account,
    not just the latest global run (which may be a partial/targeted run).
    """
    rows = conn.execute(
        """
        SELECT s.dimension_scores_json
        FROM account_scores s
        WHERE s.account_id = %s
          AND s.run_id = (
              SELECT s2.run_id
              FROM account_scores s2
              JOIN score_runs r ON s2.run_id = r.run_id
              WHERE s2.account_id = %s
                AND r.status = 'completed'
              ORDER BY r.started_at DESC LIMIT 1
          )
        """,
        (account_id, account_id),
    ).fetchall()
    if not rows:
        return {}

    merged: dict[str, float] = {}
    for row in rows:
        raw = str(row["dimension_scores_json"] or "{}").strip()
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                for dim, val in parsed.items():
                    try:
                        fval = float(val)
                    except (TypeError, ValueError):
                        fval = 0.0
                    merged[dim] = max(merged.get(dim, 0.0), fval)
        except (json.JSONDecodeError, TypeError):
            continue

    return merged


def get_dimension_confidence(conn, account_id: str) -> dict:
    """Merge dimension confidence data across all products for the latest completed run.

    Takes the entry with the highest source_count per dimension.
    """
    rows = conn.execute(
        """
        SELECT s.dimension_confidence_json
        FROM account_scores s
        WHERE s.account_id = %s
          AND s.run_id = (
              SELECT s2.run_id
              FROM account_scores s2
              JOIN score_runs r ON s2.run_id = r.run_id
              WHERE s2.account_id = %s
                AND r.status = 'completed'
              ORDER BY r.started_at DESC LIMIT 1
          )
        """,
        (account_id, account_id),
    ).fetchall()
    if not rows:
        return {}

    merged: dict[str, dict] = {}
    for row in rows:
        raw = str(row["dimension_confidence_json"] or "{}").strip()
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                for dim, val in parsed.items():
                    if not isinstance(val, dict):
                        continue
                    existing = merged.get(dim)
                    if not existing or val.get("source_count", 0) > existing.get("source_count", 0):
                        merged[dim] = val
        except (json.JSONDecodeError, TypeError):
            continue
    return merged


def get_last_signal_date(conn, account_id: str) -> str | None:
    """Return the ISO date string of the most recent signal for an account."""
    cur = conn.execute(
        "SELECT MAX(observed_at) AS last_observed FROM signal_observations WHERE account_id = %s",
        (account_id,),
    )
    row = cur.fetchone()
    if row and row["last_observed"]:
        return str(row["last_observed"])
    return None


def get_account_velocity(conn, account_id: str) -> dict:
    """Compute 7d, 14d, 30d score deltas and classify the trend."""
    rows = conn.execute(
        """
        SELECT s.score, r.run_date
        FROM account_scores s
        JOIN score_runs r ON s.run_id = r.run_id
        WHERE s.account_id = %s
          AND r.status = 'completed'
        ORDER BY r.run_date DESC, s.score DESC
        """,
        (account_id,),
    ).fetchall()

    if not rows:
        return {"7d": 0.0, "14d": 0.0, "30d": 0.0, "category": "stable"}

    # Use the most recent score as the current score
    current_score = float(rows[0]["score"])
    current_date_str = str(rows[0]["run_date"])

    try:
        current_date = date.fromisoformat(current_date_str[:10])
    except (ValueError, TypeError):
        return {"7d": 0.0, "14d": 0.0, "30d": 0.0, "category": "stable"}

    def _find_score_at_offset(days: int) -> float | None:
        from datetime import timedelta

        target = current_date - timedelta(days=days)
        best = None
        best_diff = None
        for r in rows:
            try:
                rd = date.fromisoformat(str(r["run_date"])[:10])
            except (ValueError, TypeError):
                continue
            diff = abs((rd - target).days)
            if diff <= 3 and (best_diff is None or diff < best_diff):
                best = float(r["score"])
                best_diff = diff
        return best

    d7 = _find_score_at_offset(7)
    d14 = _find_score_at_offset(14)
    d30 = _find_score_at_offset(30)

    delta_7d = round(current_score - d7, 2) if d7 is not None else 0.0
    delta_14d = round(current_score - d14, 2) if d14 is not None else 0.0
    delta_30d = round(current_score - d30, 2) if d30 is not None else 0.0

    # Classify trend based on 7d delta
    if delta_7d > 2.0:
        category = "accelerating"
    elif delta_7d < -2.0:
        category = "decelerating"
    else:
        category = "stable"

    return {"7d": delta_7d, "14d": delta_14d, "30d": delta_30d, "category": category}


def get_signal_timeline(
    conn,
    account_id: str,
    limit: int = 50,
    offset: int = 0,
    signal_code: str = "",
    source: str = "",
    date_from: str = "",
    date_to: str = "",
) -> tuple[list[dict], int]:
    """Return enriched scored timeline for an account.

    Unlike the raw signals list on the detail endpoint, the timeline
    enriches each observation with scoring context from the latest run:
    - ``component_score``: what this signal actually contributed to the score
    This lets the UI show *why* each signal mattered and how much it weighed.
    """
    where_parts = ["so.account_id = %s"]
    params: list = [account_id]

    if signal_code:
        where_parts.append("so.signal_code = %s")
        params.append(signal_code)
    if source:
        where_parts.append("so.source = %s")
        params.append(source)
    if date_from:
        where_parts.append("so.observed_at >= %s")
        params.append(date_from)
    if date_to:
        where_parts.append("so.observed_at <= %s")
        params.append(date_to)

    where_sql = " AND ".join(where_parts)

    count_row = conn.execute(
        f"SELECT COUNT(*) AS total FROM signal_observations so WHERE {where_sql}",
        params,
    ).fetchone()
    total = int(count_row["total"]) if count_row else 0

    data_params = list(params) + [max(1, min(limit, 200)), max(0, offset)]
    rows = conn.execute(
        f"""
        SELECT so.signal_code, so.source, so.evidence_url, so.evidence_text,
               so.observed_at, so.confidence, so.source_reliability, so.product,
               sc.component_score
        FROM signal_observations so
        LEFT JOIN LATERAL (
            SELECT sc2.component_score
            FROM score_components sc2
            WHERE sc2.account_id = so.account_id
              AND sc2.signal_code = so.signal_code
              AND sc2.run_id = (
                  SELECT run_id FROM score_runs
                  WHERE status = 'completed'
                  ORDER BY started_at DESC LIMIT 1
              )
            ORDER BY sc2.component_score DESC
            LIMIT 1
        ) sc ON true
        WHERE {where_sql}
        ORDER BY so.observed_at DESC
        LIMIT %s OFFSET %s
        """,
        data_params,
    ).fetchall()

    return [dict(r) for r in rows], total
