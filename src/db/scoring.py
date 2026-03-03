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

    # --- Batch insert components (1 executemany instead of N individual INSERTs) ---
    if component_scores:
        conn.cursor().executemany(
            """
            INSERT INTO score_components (run_id, account_id, product, signal_code, component_score)
            VALUES (%s, %s, %s, %s, %s)
            """,
            [(c.run_id, c.account_id, c.product, c.signal_code, c.component_score) for c in component_scores],
        )

    # --- Batch insert account scores ---
    if account_scores:
        conn.cursor().executemany(
            """
            INSERT INTO account_scores (
                run_id, account_id, product, score, tier, tier_v2,
                top_reasons_json, delta_7d, velocity_7d, velocity_14d,
                velocity_30d, velocity_category, confidence_band,
                dimension_scores_json, dimension_confidence_json
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            [
                (
                    s.run_id,
                    s.account_id,
                    s.product,
                    s.score,
                    s.tier,
                    s.tier_v2,
                    s.top_reasons_json,
                    s.delta_7d,
                    s.velocity_7d,
                    s.velocity_14d,
                    s.velocity_30d,
                    s.velocity_category,
                    s.confidence_band,
                    s.dimension_scores_json,
                    s.dimension_confidence_json,
                )
                for s in account_scores
            ],
        )

    conn.commit()


def _get_score_at_offset(conn: Any, account_id: str, product: str, run_date: str, days_back: int) -> float | None:
    """Get the most recent score from *days_back* or more days ago."""
    cur = conn.execute(
        """
        SELECT s.score
        FROM account_scores s
        JOIN score_runs r ON s.run_id = r.run_id
        WHERE s.account_id = %s
          AND s.product = %s
          AND r.run_date::date <= (%s::date - INTERVAL '%s days')
        ORDER BY r.run_date::date DESC, r.started_at DESC
        LIMIT 1
        """,
        (account_id, product, run_date, days_back),
    )
    row = cur.fetchone()
    if not row:
        return None
    return float(row["score"])


def get_score_velocity(
    conn: Any, account_id: str, product: str, current_score: float, run_date: str
) -> tuple[float, float, float]:
    """Return (velocity_7d, velocity_14d, velocity_30d) for an account-product pair."""
    results: list[float] = []
    for days in (7, 14, 30):
        past = _get_score_at_offset(conn, account_id, product, run_date, days)
        if past is None:
            results.append(0.0)
        else:
            results.append(round(current_score - past, 2))
    return (results[0], results[1], results[2])


def batch_get_velocity(
    conn: Any,
    run_date: str,
) -> dict[tuple[str, str], dict[str, float | None]]:
    """Fetch historical scores for ALL accounts at 7/14/30 day offsets in 3 queries.

    Returns dict keyed by (account_id, product) -> {"past_7": score, "past_14": score, "past_30": score}.
    Much faster than calling get_score_velocity per-account (3 queries vs 9000+).
    """
    result: dict[tuple[str, str], dict[str, float | None]] = {}

    for days, key in ((7, "past_7"), (14, "past_14"), (30, "past_30")):
        cur = conn.execute(
            """
            SELECT DISTINCT ON (s.account_id, s.product)
                   s.account_id, s.product, s.score
            FROM account_scores s
            JOIN score_runs r ON s.run_id = r.run_id
            WHERE r.run_date::date <= (%s::date - INTERVAL '%s days')
              AND r.status = 'completed'
            ORDER BY s.account_id, s.product, r.run_date::date DESC, r.started_at DESC
            """,
            (run_date, days),
        )
        for row in cur.fetchall():
            pair = (str(row["account_id"]), str(row["product"]))
            if pair not in result:
                result[pair] = {"past_7": None, "past_14": None, "past_30": None}
            result[pair][key] = float(row["score"])

    return result


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
