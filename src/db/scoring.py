from __future__ import annotations

import json
import uuid
from datetime import date
from typing import Any

from src.models import AccountScore, ComponentScore, ReviewLabel
from src.utils import utc_now_iso


def create_score_run(conn: Any, run_date: str) -> str:
    run_id = f"run_{uuid.uuid4().hex[:12]}"
    conn.execute(
        """
        INSERT INTO score_runs (run_id, run_date, status, started_at)
        VALUES (%s, %s, 'running', %s)
        """,
        (run_id, run_date, utc_now_iso()),
    )
    conn.commit()
    return run_id


def finish_score_run(
    conn: Any,
    run_id: str,
    status: str,
    error_summary: str | None = None,
) -> None:
    conn.execute(
        """
        UPDATE score_runs
        SET status = %s, finished_at = %s, error_summary = %s
        WHERE run_id = %s
        """,
        (status, utc_now_iso(), error_summary or "", run_id),
    )
    conn.commit()


def fetch_observations_for_scoring(
    conn: Any,
    run_date: str,
    lookback_days: int = 120,
) -> list[dict[str, Any]]:
    cur = conn.execute(
        """
        SELECT obs_id, account_id, signal_code, product, source, observed_at,
               evidence_url, evidence_text, document_id, mention_id,
               evidence_sentence, evidence_sentence_en, matched_phrase, language,
               speaker_name, speaker_role, evidence_quality, relevance_score,
               confidence, source_reliability, raw_payload_hash
        FROM signal_observations
        WHERE observed_at::date <= %s::date
          AND observed_at::date >= (%s::date + %s::interval)
        """,
        (run_date, run_date, f"-{lookback_days} days"),
    )
    return list(cur.fetchall())


def replace_run_scores(
    conn: Any,
    run_id: str,
    component_scores: list[ComponentScore],
    account_scores: list[AccountScore],
) -> None:
    conn.execute("DELETE FROM score_components WHERE run_id = %s", (run_id,))
    conn.execute("DELETE FROM account_scores WHERE run_id = %s", (run_id,))

    for component in component_scores:
        conn.execute(
            """
            INSERT INTO score_components (run_id, account_id, product, signal_code, component_score)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (
                component.run_id,
                component.account_id,
                component.product,
                component.signal_code,
                component.component_score,
            ),
        )

    for score in account_scores:
        conn.execute(
            """
            INSERT INTO account_scores (
                run_id,
                account_id,
                product,
                score,
                tier,
                tier_v2,
                top_reasons_json,
                delta_7d,
                velocity_7d,
                velocity_14d,
                velocity_30d,
                velocity_category,
                confidence_band,
                dimension_scores_json,
                dimension_confidence_json
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                score.run_id,
                score.account_id,
                score.product,
                score.score,
                score.tier,
                score.tier_v2,
                score.top_reasons_json,
                score.delta_7d,
                score.velocity_7d,
                score.velocity_14d,
                score.velocity_30d,
                score.velocity_category,
                score.confidence_band,
                score.dimension_scores_json,
                score.dimension_confidence_json,
            ),
        )
    conn.commit()


def get_score_delta_7d(conn: Any, account_id: str, product: str, run_date: str) -> float:
    cur = conn.execute(
        """
        SELECT s.score
        FROM account_scores s
        JOIN score_runs r ON s.run_id = r.run_id
        WHERE s.account_id = %s
          AND s.product = %s
          AND r.run_date::date <= (%s::date - INTERVAL '7 days')
        ORDER BY r.run_date::date DESC
        LIMIT 1
        """,
        (account_id, product, run_date),
    )
    row = cur.fetchone()
    if not row:
        return 0.0

    cur2 = conn.execute(
        """
        SELECT s.score
        FROM account_scores s
        JOIN score_runs r ON s.run_id = r.run_id
        WHERE s.account_id = %s
          AND s.product = %s
          AND r.run_date::date = %s::date
        ORDER BY r.started_at DESC
        LIMIT 1
        """,
        (account_id, product, run_date),
    )
    current_row = cur2.fetchone()
    if not current_row:
        return 0.0
    return round(float(current_row["score"]) - float(row["score"]), 2)


def get_latest_run_id_for_date(conn: Any, run_date: str) -> str | None:
    cur = conn.execute(
        """
        SELECT run_id
        FROM score_runs
        WHERE run_date::date = %s::date
        ORDER BY started_at DESC
        LIMIT 1
        """,
        (run_date,),
    )
    row = cur.fetchone()
    return None if not row else str(row["run_id"])


def list_runs(conn: Any) -> list[dict[str, Any]]:
    cur = conn.execute(
        "SELECT run_id, run_date, status, started_at, finished_at, error_summary FROM score_runs ORDER BY started_at DESC"
    )
    return list(cur.fetchall())


def fetch_scores_for_run(conn: Any, run_id: str) -> list[dict[str, Any]]:
    cur = conn.execute(
        """
        SELECT
            r.run_date,
            s.account_id,
            a.company_name,
            a.domain,
            s.product,
            s.score,
            s.tier,
            s.tier_v2,
            s.delta_7d,
            s.top_reasons_json
        FROM account_scores s
        JOIN accounts a ON a.account_id = s.account_id
        JOIN score_runs r ON r.run_id = s.run_id
        WHERE s.run_id = %s
        ORDER BY s.score DESC
        """,
        (run_id,),
    )
    return list(cur.fetchall())


def insert_review_label(conn: Any, label: ReviewLabel) -> bool:
    cur = conn.execute(
        """
        INSERT INTO review_labels (review_id, run_id, account_id, decision, reviewer, notes, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
        RETURNING review_id
        """,
        (
            label.review_id,
            label.run_id,
            label.account_id,
            label.decision,
            label.reviewer,
            label.notes,
            label.created_at,
        ),
    )
    conn.commit()
    return cur.fetchone() is not None


def fetch_review_rows_for_date(conn: Any, run_date: str) -> list[dict[str, Any]]:
    cur = conn.execute(
        """
        SELECT rl.review_id, rl.run_id, rl.account_id, rl.decision, rl.reviewer,
               rl.notes, rl.created_at, r.run_date
        FROM review_labels rl
        JOIN score_runs r ON r.run_id = rl.run_id
        WHERE r.run_date::date = %s::date
        """,
        (run_date,),
    )
    return list(cur.fetchall())


def fetch_sources_for_account_window(
    conn: Any,
    account_id: str,
    run_date: str,
    lookback_days: int = 30,
) -> list[str]:
    cur = conn.execute(
        """
        SELECT DISTINCT source
        FROM signal_observations
        WHERE account_id = %s
          AND observed_at::date <= %s::date
          AND observed_at::date >= (%s::date + %s::interval)
        ORDER BY source
        """,
        (account_id, run_date, run_date, f"-{lookback_days} days"),
    )
    return [str(row["source"]) for row in cur.fetchall()]


def fetch_scored_sources_for_run_account(
    conn: Any,
    run_id: str,
    account_id: str,
) -> list[str]:
    cur = conn.execute(
        """
        SELECT top_reasons_json
        FROM account_scores
        WHERE run_id = %s
          AND account_id = %s
        """,
        (run_id, account_id),
    )
    rows = cur.fetchall()

    sources: set[str] = set()
    for row in rows:
        raw = str(row["top_reasons_json"] or "").strip()
        if not raw:
            continue
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if not isinstance(parsed, list):
            continue
        for reason in parsed:
            if not isinstance(reason, dict):
                continue
            source = str(reason.get("source", "")).strip()
            if source:
                sources.add(source)

    return sorted(sources)


def upsert_source_metrics(
    conn: Any,
    run_date: str,
    rows: list[dict[str, float | int | str]],
) -> None:
    for row in rows:
        conn.execute(
            """
            INSERT INTO source_metrics (run_date, source, approved_rate, sample_size)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT(run_date, source)
            DO UPDATE SET approved_rate = excluded.approved_rate,
                          sample_size = excluded.sample_size
            """,
            (
                run_date,
                str(row.get("source", "unknown")),
                float(row.get("approved_rate", 0.0)),
                int(row.get("sample_size", 0)),
            ),
        )
    conn.commit()


def fetch_source_metrics(conn: Any, run_date: str) -> list[dict[str, Any]]:
    cur = conn.execute(
        """
        SELECT run_date, source, approved_rate, sample_size
        FROM source_metrics
        WHERE run_date::date = %s::date
        ORDER BY source
        """,
        (run_date,),
    )
    return list(cur.fetchall())


def fetch_recent_reviews(conn: Any, run_date: str, days: int) -> list[dict[str, Any]]:
    cur = conn.execute(
        """
        SELECT rl.review_id, rl.run_id, rl.account_id, rl.decision, rl.reviewer,
               rl.notes, rl.created_at, r.run_date
        FROM review_labels rl
        JOIN score_runs r ON r.run_id = rl.run_id
        WHERE r.run_date::date <= %s::date
          AND r.run_date::date >= (%s::date + %s::interval)
        ORDER BY r.run_date::date DESC
        """,
        (run_date, run_date, f"-{days} days"),
    )
    return list(cur.fetchall())
