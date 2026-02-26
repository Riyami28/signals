from __future__ import annotations

import json
import uuid
from datetime import date
from pathlib import Path
from typing import Any

from src.models import Account
from src.utils import load_csv_rows, normalize_domain, stable_hash, utc_now_iso


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
                 email, linkedin_url, management_level, year_joined, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
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
            ),
        )
    conn.commit()


def get_contacts_for_account(conn, account_id: str) -> list[dict]:
    """Return all contacts for an account, ordered by management_level seniority."""
    rows = conn.execute(
        """
        SELECT contact_id, account_id, first_name, last_name, title,
               email, linkedin_url, management_level, year_joined, created_at
        FROM contact_research
        WHERE account_id = %s
        ORDER BY CASE management_level
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

    where_sql = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""

    sort_map = {
        "score": "COALESCE(best.score, 0)",
        "company_name": "a.company_name",
        "domain": "a.domain",
        "tier": "best.tier",
    }
    order_col = sort_map.get(sort_by, "COALESCE(best.score, 0)")
    order_dir = "DESC" if sort_dir.lower() == "desc" else "ASC"

    count_sql = f"""
        SELECT COUNT(*) as total FROM (
            SELECT a.account_id
            FROM accounts a
            LEFT JOIN LATERAL (
                SELECT score, tier FROM account_scores
                WHERE account_id = a.account_id ORDER BY score DESC LIMIT 1
            ) best ON true
            {where_sql}
        ) sub
    """
    total = conn.execute(count_sql, params).fetchone()["total"]

    offset = (page - 1) * per_page
    data_params = list(params) + [per_page, offset]
    data_sql = f"""
        SELECT
            a.account_id, a.company_name, a.domain, a.source_type,
            COALESCE(best.score, 0) AS score,
            COALESCE(best.tier, 'low') AS tier,
            cr.research_status,
            (SELECT string_agg(al.label, ',') FROM account_labels al WHERE al.account_id = a.account_id) AS labels
        FROM accounts a
        LEFT JOIN LATERAL (
            SELECT score, tier
            FROM account_scores
            WHERE account_id = a.account_id ORDER BY score DESC LIMIT 1
        ) best ON true
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

    scores = conn.execute(
        "SELECT product, score, tier, dimension_scores_json FROM account_scores WHERE account_id = %s ORDER BY score DESC",
        (account_id,),
    ).fetchall()
    result["scores"] = [dict(r) for r in scores]

    signals = conn.execute(
        """SELECT signal_code, source, evidence_url, evidence_text, observed_at
           FROM signal_observations WHERE account_id = %s ORDER BY observed_at DESC LIMIT 50""",
        (account_id,),
    ).fetchall()
    result["signals"] = [dict(r) for r in signals]

    result["research"] = get_company_research(conn, account_id)
    result["contacts"] = get_contacts_for_account(conn, account_id)
    result["labels"] = get_labels_for_account(conn, account_id)

    # Dimension scores from the highest-scoring product row
    result["dimension_scores"] = get_dimension_scores(conn, account_id)

    # Velocity metrics
    result["velocity"] = get_account_velocity(conn, account_id)

    return result


def get_dimension_scores(conn, account_id: str) -> dict:
    """Parse dimension_scores_json from the latest highest-scoring row for this account."""
    row = conn.execute(
        """
        SELECT s.dimension_scores_json
        FROM account_scores s
        JOIN score_runs r ON s.run_id = r.run_id
        WHERE s.account_id = %s
        ORDER BY r.started_at DESC, s.score DESC
        LIMIT 1
        """,
        (account_id,),
    ).fetchone()
    if not row:
        return {}
    raw = str(row["dimension_scores_json"] or "{}").strip()
    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, dict) else {}
    except (json.JSONDecodeError, TypeError):
        return {}


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
    """Return paginated signal observations for an account with optional filters."""
    where_parts = ["account_id = %s"]
    params: list = [account_id]

    if signal_code:
        where_parts.append("signal_code = %s")
        params.append(signal_code)
    if source:
        where_parts.append("source = %s")
        params.append(source)
    if date_from:
        where_parts.append("observed_at >= %s")
        params.append(date_from)
    if date_to:
        where_parts.append("observed_at <= %s")
        params.append(date_to)

    where_sql = " AND ".join(where_parts)

    count_row = conn.execute(
        f"SELECT COUNT(*) AS total FROM signal_observations WHERE {where_sql}",
        params,
    ).fetchone()
    total = int(count_row["total"]) if count_row else 0

    data_params = list(params) + [max(1, min(limit, 200)), max(0, offset)]
    rows = conn.execute(
        f"""
        SELECT signal_code, source, evidence_url, evidence_text, observed_at,
               confidence, source_reliability, product
        FROM signal_observations
        WHERE {where_sql}
        ORDER BY observed_at DESC
        LIMIT %s OFFSET %s
        """,
        data_params,
    ).fetchall()

    return [dict(r) for r in rows], total
