from __future__ import annotations

import json
import uuid
from typing import Any

from src.utils import normalize_domain, stable_hash, utc_now_iso


def insert_crawl_frontier(
    conn: Any,
    run_date: str,
    source: str,
    source_event_id: str,
    account_id: str,
    domain: str,
    url: str,
    canonical_url: str,
    url_type: str = "article",
    depth: int = 0,
    priority: float = 0.5,
    max_retries: int = 2,
    payload_json: str = "{}",
    commit: bool = True,
) -> bool:
    frontier_id = stable_hash(
        {
            "run_date": run_date,
            "canonical_url": canonical_url,
            "source": source,
        },
        prefix="frn",
        length=16,
    )
    cur = conn.execute(
        """
        INSERT INTO crawl_frontier (
            frontier_id,
            run_date,
            source,
            source_event_id,
            account_id,
            domain,
            url,
            canonical_url,
            url_type,
            depth,
            priority,
            status,
            retry_count,
            max_retries,
            first_seen_at,
            last_attempt_at,
            last_error,
            payload_json
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'pending', 0, %s, %s, '', '', %s)
        ON CONFLICT DO NOTHING
        RETURNING frontier_id
        """,
        (
            frontier_id,
            run_date,
            (source or "").strip().lower(),
            (source_event_id or "").strip(),
            account_id,
            normalize_domain(domain or ""),
            (url or "").strip(),
            (canonical_url or "").strip(),
            (url_type or "article").strip().lower(),
            max(0, int(depth)),
            max(0.0, float(priority)),
            max(0, int(max_retries)),
            utc_now_iso(),
            (payload_json or "{}")[:12000],
        ),
    )
    inserted = cur.fetchone() is not None
    if commit:
        conn.commit()
    return inserted


def fetch_crawl_frontier_by_status(
    conn: Any,
    run_date: str,
    status: str,
    limit: int = 500,
) -> list[dict[str, Any]]:
    cur = conn.execute(
        """
        SELECT *
        FROM crawl_frontier
        WHERE run_date = %s
          AND status = %s
        ORDER BY priority DESC, first_seen_at ASC
        LIMIT %s
        """,
        (run_date, status, max(1, int(limit))),
    )
    return list(cur.fetchall())


def mark_crawl_frontier_status(
    conn: Any,
    frontier_id: str,
    status: str,
    error_summary: str = "",
    bump_retry: bool = False,
    commit: bool = True,
) -> None:
    if bump_retry:
        conn.execute(
            """
            UPDATE crawl_frontier
            SET status = %s,
                retry_count = retry_count + 1,
                last_attempt_at = %s,
                last_error = %s
            WHERE frontier_id = %s
            """,
            (status, utc_now_iso(), (error_summary or "")[:500], frontier_id),
        )
    else:
        conn.execute(
            """
            UPDATE crawl_frontier
            SET status = %s,
                last_attempt_at = %s,
                last_error = %s
            WHERE frontier_id = %s
            """,
            (status, utc_now_iso(), (error_summary or "")[:500], frontier_id),
        )
    if commit:
        conn.commit()
