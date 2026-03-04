from __future__ import annotations

from datetime import date
from typing import Any

from src.utils import normalize_domain, utc_now_iso


def was_crawled_today(
    conn: Any,
    source: str,
    account_id: str,
    endpoint: str,
) -> bool:
    cur = conn.execute(
        """
        SELECT 1
        FROM crawl_checkpoints
        WHERE source = %s
          AND account_id = %s
          AND endpoint = %s
          AND last_crawled_at::timestamp >= (CURRENT_TIMESTAMP - INTERVAL '20 hours')
        LIMIT 1
        """,
        (source, account_id, endpoint),
    )
    return cur.fetchone() is not None


def mark_crawled(
    conn: Any,
    source: str,
    account_id: str,
    endpoint: str,
    commit: bool = True,
) -> None:
    conn.execute(
        """
        INSERT INTO crawl_checkpoints (source, account_id, endpoint, last_crawled_at)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT(source, account_id, endpoint)
        DO UPDATE SET last_crawled_at = excluded.last_crawled_at
        """,
        (source, account_id, endpoint, utc_now_iso()),
    )
    if commit:
        conn.commit()


def record_crawl_attempt(
    conn: Any,
    source: str,
    account_id: str,
    endpoint: str,
    status: str,
    error_summary: str = "",
    commit: bool = True,
) -> None:
    conn.execute(
        """
        INSERT INTO crawl_attempts (source, account_id, endpoint, attempted_at, status, error_summary)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (source, account_id, endpoint, utc_now_iso(), status, (error_summary or "")[:500]),
    )
    if commit:
        conn.commit()


def fetch_crawl_attempt_summary(conn: Any, run_date: str) -> list[dict[str, Any]]:
    cur = conn.execute(
        """
        SELECT source, status, COUNT(*) AS attempt_count
        FROM crawl_attempts
        WHERE attempted_at::date = %s::date
        GROUP BY source, status
        ORDER BY source, status
        """,
        (run_date,),
    )
    return list(cur.fetchall())


def fetch_latest_crawl_failures(conn: Any, run_date: str, limit: int = 10) -> list[dict[str, Any]]:
    cur = conn.execute(
        """
        SELECT source, account_id, endpoint, status, error_summary, attempted_at
        FROM crawl_attempts
        WHERE attempted_at::date = %s::date
          AND status IN ('http_error', 'exception')
        ORDER BY attempted_at DESC
        LIMIT %s
        """,
        (run_date, max(1, int(limit))),
    )
    return list(cur.fetchall())


def get_twitter_since_id(conn: Any, account_id: str) -> str:
    """Return the latest tweet_id seen for this account, or '' if never fetched."""
    cur = conn.execute(
        "SELECT since_tweet_id FROM twitter_cursors WHERE account_id = %s",
        (account_id,),
    )
    row = cur.fetchone()
    return str(row["since_tweet_id"]) if row else ""


def save_twitter_since_id(conn: Any, account_id: str, tweet_id: str, commit: bool = False) -> None:
    """Upsert the latest tweet_id for an account so next fetch skips already-seen tweets."""
    if not tweet_id:
        return
    conn.execute(
        """
        INSERT INTO twitter_cursors (account_id, since_tweet_id, updated_at)
        VALUES (%s, %s, now())
        ON CONFLICT (account_id) DO UPDATE
            SET since_tweet_id = EXCLUDED.since_tweet_id,
                updated_at     = now()
        WHERE twitter_cursors.since_tweet_id < EXCLUDED.since_tweet_id
        """,
        (account_id, tweet_id),
    )
    if commit:
        conn.commit()


def select_accounts_for_live_crawl(
    conn: Any,
    source: str,
    limit: int,
    include_domains: list[str] | tuple[str, ...] | None = None,
    include_account_ids: list[str] | None = None,
) -> list[dict[str, Any]]:
    bounded_limit = max(1, int(limit))
    domain_filters: list[str] = []
    for raw in list(include_domains or []):
        normalized = normalize_domain(str(raw))
        if not normalized or normalized.endswith(".example"):
            continue
        domain_filters.append(normalized)
    domain_filters = sorted(set(domain_filters))

    where_clauses = [
        "COALESCE(a.domain, '') <> ''",
        "LOWER(a.domain) NOT LIKE %s",
    ]
    params: list[Any] = [str(source).strip(), "%.example"]
    if include_account_ids:
        placeholders = ", ".join("%s" for _ in include_account_ids)
        where_clauses.append(f"a.account_id IN ({placeholders})")
        params.extend(include_account_ids)
    if domain_filters:
        placeholders = ", ".join("%s" for _ in domain_filters)
        where_clauses.append(f"LOWER(a.domain) IN ({placeholders})")
        params.extend(domain_filters)
    where_sql = " AND ".join(where_clauses)

    cur = conn.execute(
        f"""
        SELECT
            a.account_id,
            a.domain,
            a.company_name,
            a.created_at,
            latest.last_attempted_at
        FROM accounts a
        LEFT JOIN (
            SELECT account_id, MAX(attempted_at) AS last_attempted_at
            FROM crawl_attempts
            WHERE source = %s
            GROUP BY account_id
        ) latest
          ON latest.account_id = a.account_id
        WHERE {where_sql}
        ORDER BY
            CASE
                WHEN latest.last_attempted_at IS NULL OR latest.last_attempted_at = '' THEN 0
                ELSE 1
            END,
            latest.last_attempted_at ASC,
            a.created_at ASC
        LIMIT %s
        """,
        tuple(params + [bounded_limit]),
    )
    return list(cur.fetchall())
