from __future__ import annotations

import json
import uuid
from datetime import date
from typing import Any

from src.utils import utc_now_iso


def try_advisory_lock(conn: Any, lock_name: str, owner_id: str, details: str = "") -> bool:
    row = conn.execute("SELECT pg_try_advisory_lock(hashtext(%s)) AS locked", (lock_name,)).fetchone()
    locked = bool(row and bool(row["locked"]))
    conn.execute(
        """
        INSERT INTO run_lock_events (lock_name, owner_id, action, details, created_at)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (
            (lock_name or "")[:120],
            (owner_id or "")[:120],
            "acquired" if locked else "busy",
            (details or "")[:300],
            utc_now_iso(),
        ),
    )
    conn.commit()
    return locked


def release_advisory_lock(conn: Any, lock_name: str, owner_id: str, details: str = "") -> bool:
    row = conn.execute("SELECT pg_advisory_unlock(hashtext(%s)) AS unlocked", (lock_name,)).fetchone()
    unlocked = bool(row and bool(row["unlocked"]))
    conn.execute(
        """
        INSERT INTO run_lock_events (lock_name, owner_id, action, details, created_at)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (
            (lock_name or "")[:120],
            (owner_id or "")[:120],
            "released" if unlocked else "release_missed",
            (details or "")[:300],
            utc_now_iso(),
        ),
    )
    conn.commit()
    return unlocked


def record_stage_failure(
    conn: Any,
    run_type: str,
    run_date: str,
    stage: str,
    error_summary: str,
    duration_seconds: float,
    timed_out: bool,
    retry_task_id: str = "",
    commit: bool = True,
) -> int:
    cur = conn.execute(
        """
        INSERT INTO stage_failures (
            run_type,
            run_date,
            stage,
            duration_seconds,
            timed_out,
            error_summary,
            retry_task_id,
            created_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING failure_id
        """,
        (
            (run_type or "")[:80],
            (run_date or "")[:30],
            (stage or "")[:120],
            max(0.0, float(duration_seconds)),
            1 if timed_out else 0,
            (error_summary or "")[:1000],
            (retry_task_id or "")[:80],
            utc_now_iso(),
        ),
    )
    row = cur.fetchone()
    failure_id = int(row["failure_id"]) if row is not None else 0
    if commit:
        conn.commit()
    return failure_id


def enqueue_retry_task(
    conn: Any,
    task_type: str,
    payload_json: str,
    due_at: str,
    max_attempts: int = 3,
    commit: bool = True,
) -> str:
    task_id = f"retry_{uuid.uuid4().hex[:12]}"
    now_iso = utc_now_iso()
    conn.execute(
        """
        INSERT INTO retry_queue (
            task_id,
            task_type,
            payload_json,
            attempt_count,
            max_attempts,
            status,
            due_at,
            last_error,
            created_at,
            updated_at
        )
        VALUES (%s, %s, %s, 0, %s, 'pending', %s, '', %s, %s)
        """,
        (
            task_id,
            (task_type or "")[:80],
            (payload_json or "{}")[:12000],
            max(1, int(max_attempts)),
            (due_at or now_iso)[:80],
            now_iso,
            now_iso,
        ),
    )
    if commit:
        conn.commit()
    return task_id


def fetch_due_retry_tasks(conn: Any, limit: int = 20, now_iso: str | None = None) -> list[dict[str, Any]]:
    cur = conn.execute(
        """
        SELECT task_id, task_type, payload_json, attempt_count, max_attempts,
               status, due_at, last_error, created_at, updated_at
        FROM retry_queue
        WHERE status = 'pending'
          AND CAST(due_at AS TIMESTAMP) <= CAST(%s AS TIMESTAMP)
        ORDER BY due_at ASC, created_at ASC
        LIMIT %s
        """,
        ((now_iso or utc_now_iso()), max(1, int(limit))),
    )
    return list(cur.fetchall())


def mark_retry_task_running(conn: Any, task_id: str, commit: bool = True) -> None:
    conn.execute(
        """
        UPDATE retry_queue
        SET status = 'running', updated_at = %s
        WHERE task_id = %s
        """,
        (utc_now_iso(), task_id),
    )
    if commit:
        conn.commit()


def mark_retry_task_completed(conn: Any, task_id: str, commit: bool = True) -> None:
    conn.execute(
        """
        UPDATE retry_queue
        SET status = 'completed', updated_at = %s, last_error = ''
        WHERE task_id = %s
        """,
        (utc_now_iso(), task_id),
    )
    if commit:
        conn.commit()


def reschedule_retry_task(
    conn: Any,
    task_id: str,
    attempt_count: int,
    due_at: str,
    error_summary: str,
    commit: bool = True,
) -> None:
    conn.execute(
        """
        UPDATE retry_queue
        SET status = 'pending',
            attempt_count = %s,
            due_at = %s,
            last_error = %s,
            updated_at = %s
        WHERE task_id = %s
        """,
        (
            max(0, int(attempt_count)),
            (due_at or utc_now_iso())[:80],
            (error_summary or "")[:1000],
            utc_now_iso(),
            task_id,
        ),
    )
    if commit:
        conn.commit()


def quarantine_retry_task(
    conn: Any,
    task_id: str,
    task_type: str,
    payload_json: str,
    attempt_count: int,
    error_summary: str,
    commit: bool = True,
) -> None:
    now_iso = utc_now_iso()
    conn.execute(
        """
        UPDATE retry_queue
        SET status = 'quarantined',
            attempt_count = %s,
            last_error = %s,
            updated_at = %s
        WHERE task_id = %s
        """,
        (max(0, int(attempt_count)), (error_summary or "")[:1000], now_iso, task_id),
    )
    conn.execute(
        """
        INSERT INTO quarantine_failures (
            task_id,
            task_type,
            payload_json,
            attempt_count,
            error_summary,
            quarantined_at,
            resolved,
            resolved_at,
            resolution_note
        )
        VALUES (%s, %s, %s, %s, %s, %s, 0, '', '')
        """,
        (
            task_id,
            (task_type or "")[:80],
            (payload_json or "{}")[:12000],
            max(0, int(attempt_count)),
            (error_summary or "")[:1000],
            now_iso,
        ),
    )
    if commit:
        conn.commit()


def fetch_retry_queue_size(conn: Any) -> int:
    row = conn.execute(
        """
        SELECT COUNT(*) AS c
        FROM retry_queue
        WHERE status IN ('pending', 'running')
        """
    ).fetchone()
    return int(row["c"] if row is not None else 0)


def fetch_retry_depth(conn: Any) -> int:
    row = conn.execute(
        """
        SELECT COALESCE(MAX(attempt_count), 0) AS depth
        FROM retry_queue
        WHERE status IN ('pending', 'running')
        """
    ).fetchone()
    return int(row["depth"] if row is not None else 0)


def fetch_quarantine_size(conn: Any) -> int:
    row = conn.execute(
        """
        SELECT COUNT(*) AS c
        FROM quarantine_failures
        WHERE resolved = 0
        """
    ).fetchone()
    return int(row["c"] if row is not None else 0)


def fetch_pending_retry_tasks(conn: Any, limit: int = 100) -> list[dict[str, Any]]:
    cur = conn.execute(
        """
        SELECT task_id, task_type, payload_json, attempt_count, max_attempts,
               status, due_at, last_error, created_at, updated_at
        FROM retry_queue
        WHERE status IN ('pending', 'running')
        ORDER BY due_at ASC
        LIMIT %s
        """,
        (max(1, int(limit)),),
    )
    return list(cur.fetchall())


def requeue_external_discovery_events(conn: Any, run_date: str, include_processed: bool = False) -> int:
    if include_processed:
        where_clause = "processing_status IN ('processed', 'failed')"
    else:
        where_clause = "processing_status = 'failed'"

    cur = conn.execute(
        f"""
        UPDATE external_discovery_events
        SET processing_status = 'pending',
            processed_run_id = '',
            processed_at = '',
            error_summary = ''
        WHERE observed_at::date = %s::date
          AND {where_clause}
        RETURNING event_id
        """,
        (run_date,),
    )
    rows = cur.fetchall()
    conn.commit()
    return len(rows)


def replace_ops_metrics(conn: Any, run_date: str, rows: list[dict[str, Any]]) -> None:
    conn.execute("DELETE FROM ops_metrics WHERE run_date::date = %s::date", (run_date,))
    now_iso = utc_now_iso()
    for row in rows:
        conn.execute(
            """
            INSERT INTO ops_metrics (run_date, recorded_at, metric, value, meta_json)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (
                run_date,
                now_iso,
                str(row.get("metric", "unknown"))[:120],
                float(row.get("value", 0.0)),
                str(row.get("meta_json", "{}"))[:4000],
            ),
        )
    conn.commit()


def fetch_ops_metrics(conn: Any, run_date: str) -> list[dict[str, Any]]:
    cur = conn.execute(
        """
        SELECT run_date, recorded_at, metric, value, meta_json
        FROM ops_metrics
        WHERE run_date::date = %s::date
        ORDER BY metric ASC, recorded_at ASC
        """,
        (run_date,),
    )
    return list(cur.fetchall())


def fetch_latest_event_ingest_lag_seconds(conn: Any, run_date: str) -> float | None:
    row = conn.execute(
        """
        SELECT EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - MAX(CAST(ingested_at AS TIMESTAMP)))) AS lag_seconds
        FROM external_discovery_events
        WHERE observed_at::date <= %s::date
        """,
        (run_date,),
    ).fetchone()
    if row is None or row["lag_seconds"] is None:
        return None
    return max(0.0, float(row["lag_seconds"]))


def fetch_precision_by_band(conn: Any, run_date: str, lookback_days: int = 14) -> list[dict[str, Any]]:
    cur = conn.execute(
        """
        WITH decisions AS (
            SELECT rl.run_id, rl.account_id, rl.decision
            FROM review_labels rl
            JOIN score_runs sr ON sr.run_id = rl.run_id
            WHERE sr.run_date::date <= %s::date
              AND sr.run_date::date >= (%s::date + %s::interval)
              AND rl.decision IN ('approved', 'rejected')
        ),
        best_band AS (
            SELECT
                s.run_id,
                s.account_id,
                CASE MAX(CASE s.tier WHEN 'high' THEN 3 WHEN 'medium' THEN 2 ELSE 1 END)
                    WHEN 3 THEN 'high'
                    WHEN 2 THEN 'medium'
                    ELSE 'low'
                END AS band
            FROM account_scores s
            JOIN decisions d ON d.run_id = s.run_id AND d.account_id = s.account_id
            GROUP BY s.run_id, s.account_id
        )
        SELECT
            b.band,
            COUNT(*) AS sample_size,
            AVG(CASE d.decision WHEN 'approved' THEN 1.0 ELSE 0.0 END) AS approved_rate
        FROM decisions d
        JOIN best_band b ON b.run_id = d.run_id AND b.account_id = d.account_id
        GROUP BY b.band
        ORDER BY b.band
        """,
        (run_date, run_date, f"-{max(1, int(lookback_days))} days"),
    )
    return list(cur.fetchall())


def fetch_lock_event_counts(conn: Any, lookback_hours: int = 24) -> dict[str, int]:
    cur = conn.execute(
        """
        SELECT action, COUNT(*) AS c
        FROM run_lock_events
        WHERE CAST(created_at AS TIMESTAMP) >= (CURRENT_TIMESTAMP - CAST(%s AS INTERVAL))
        GROUP BY action
        """,
        (f"{max(1, int(lookback_hours))} hours",),
    )
    counts: dict[str, int] = {}
    for row in cur.fetchall():
        counts[str(row["action"])] = int(row["c"])
    return counts
