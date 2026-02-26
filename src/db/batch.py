"""Upload batch CRUD operations."""

from __future__ import annotations

import json as _json


def create_upload_batch(conn, batch_id: str, filename: str, row_count: int, metadata: dict | None = None) -> str:
    conn.execute(
        """INSERT INTO upload_batches (batch_id, filename, row_count, status, metadata)
           VALUES (%s, %s, %s, %s, %s)""",
        (batch_id, filename, row_count, "pending", _json.dumps(metadata or {})),
    )
    conn.commit()
    return batch_id


def update_batch_status(conn, batch_id: str, status: str) -> None:
    conn.execute(
        "UPDATE upload_batches SET status = %s WHERE batch_id = %s",
        (status, batch_id),
    )
    conn.commit()


def get_upload_batch(conn, batch_id: str) -> dict | None:
    cur = conn.execute(
        "SELECT batch_id, uploaded_at, filename, row_count, status, metadata FROM upload_batches WHERE batch_id = %s",
        (batch_id,),
    )
    return cur.fetchone()


def insert_batch_company(
    conn,
    batch_id: str,
    company_name: str,
    domain: str,
    industry: str = "",
    employee_count: int | None = None,
    metadata: dict | None = None,
    commit: bool = False,
) -> int:
    cur = conn.execute(
        """INSERT INTO batch_companies (batch_id, company_name, domain, industry, employee_count, metadata)
           VALUES (%s, %s, %s, %s, %s, %s) RETURNING id""",
        (batch_id, company_name, domain, industry, employee_count, _json.dumps(metadata or {})),
    )
    row_id = cur.fetchone()["id"]
    if commit:
        conn.commit()
    return row_id


def link_batch_company_account(conn, batch_company_id: int, account_id: str) -> None:
    conn.execute(
        "UPDATE batch_companies SET account_id = %s WHERE id = %s",
        (account_id, batch_company_id),
    )


def get_batch_companies(conn, batch_id: str) -> list[dict]:
    cur = conn.execute(
        "SELECT id, batch_id, company_name, domain, industry, employee_count, metadata, account_id "
        "FROM batch_companies WHERE batch_id = %s ORDER BY id",
        (batch_id,),
    )
    return [dict(row) for row in cur.fetchall()]


def get_batch_results(conn, batch_id: str, score_run_id: str | None = None) -> list[dict]:
    """Return scored accounts for a batch, joining batch_companies with latest scores."""
    if score_run_id:
        query = """
            SELECT bc.company_name, bc.domain, bc.industry, bc.employee_count,
                   bc.account_id,
                   COALESCE(s.score, 0) AS score,
                   COALESCE(s.tier, 'low') AS tier,
                   COALESCE(s.product, 'zopdev') AS product,
                   COALESCE(s.top_reasons_json, '[]') AS top_reasons_json,
                   COALESCE(s.delta_7d, 0) AS delta_7d
            FROM batch_companies bc
            LEFT JOIN LATERAL (
                SELECT a_s.score, a_s.tier, a_s.product, a_s.top_reasons_json, a_s.delta_7d
                FROM account_scores a_s
                WHERE a_s.account_id = bc.account_id AND a_s.run_id = %s
                ORDER BY a_s.score DESC LIMIT 1
            ) s ON TRUE
            WHERE bc.batch_id = %s
            ORDER BY COALESCE(s.score, 0) DESC
        """
        params = [score_run_id, batch_id]
    else:
        query = """
            SELECT bc.company_name, bc.domain, bc.industry, bc.employee_count,
                   bc.account_id,
                   COALESCE(s.score, 0) AS score,
                   COALESCE(s.tier, 'low') AS tier,
                   COALESCE(s.product, 'zopdev') AS product,
                   COALESCE(s.top_reasons_json, '[]') AS top_reasons_json,
                   COALESCE(s.delta_7d, 0) AS delta_7d
            FROM batch_companies bc
            LEFT JOIN LATERAL (
                SELECT a_s.score, a_s.tier, a_s.product, a_s.top_reasons_json, a_s.delta_7d
                FROM account_scores a_s
                WHERE a_s.account_id = bc.account_id
                ORDER BY a_s.score DESC LIMIT 1
            ) s ON TRUE
            WHERE bc.batch_id = %s
            ORDER BY COALESCE(s.score, 0) DESC
        """
        params = [batch_id]
    cur = conn.execute(query, params)
    return [dict(row) for row in cur.fetchall()]
