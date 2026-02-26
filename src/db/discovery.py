from __future__ import annotations

import json
import uuid
from typing import Any

from src.utils import normalize_domain, stable_hash, utc_now_iso


def insert_external_discovery_event(
    conn: Any,
    source: str,
    source_event_id: str,
    observed_at: str,
    title: str,
    text: str,
    url: str = "",
    entry_url: str = "",
    url_type: str = "",
    language_hint: str = "",
    author_hint: str = "",
    published_at_hint: str = "",
    company_name_hint: str = "",
    domain_hint: str = "",
    raw_payload_json: str = "{}",
) -> bool:
    normalized_source = (source or "huginn_webhook").strip().lower()
    normalized_event_id = (source_event_id or "").strip()
    dedupe_key = (
        f"{normalized_source}:{normalized_event_id}"
        if normalized_event_id
        else stable_hash(
            {
                "source": normalized_source,
                "url": (url or "").strip(),
                "entry_url": (entry_url or "").strip(),
                "observed_at": (observed_at or "").strip(),
                "title": (title or "").strip(),
                "text": (text or "").strip(),
            },
            prefix="disc",
            length=24,
        )
    )
    cur = conn.execute(
        """
        INSERT INTO external_discovery_events (
            source,
            source_event_id,
            dedupe_key,
            observed_at,
            title,
            text,
            url,
            entry_url,
            url_type,
            language_hint,
            author_hint,
            published_at_hint,
            company_name_hint,
            domain_hint,
            raw_payload_json,
            ingested_at,
            processing_status,
            processed_run_id,
            processed_at,
            error_summary
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'pending', '', '', '')
        ON CONFLICT DO NOTHING
        RETURNING event_id
        """,
        (
            normalized_source,
            normalized_event_id,
            dedupe_key,
            observed_at,
            title,
            text,
            (url or "").strip(),
            (entry_url or "").strip(),
            (url_type or "").strip().lower(),
            (language_hint or "").strip().lower(),
            (author_hint or "").strip(),
            (published_at_hint or "").strip(),
            (company_name_hint or "").strip(),
            normalize_domain(domain_hint or ""),
            (raw_payload_json or "{}")[:8000],
            utc_now_iso(),
        ),
    )
    conn.commit()
    return cur.fetchone() is not None


def fetch_pending_external_discovery_events(
    conn: Any,
    run_date: str,
    limit: int = 500,
) -> list[dict[str, Any]]:
    cur = conn.execute(
        """
        SELECT event_id, source, source_event_id, dedupe_key, observed_at,
               title, text, url, entry_url, url_type, language_hint,
               author_hint, published_at_hint, company_name_hint, domain_hint,
               raw_payload_json, ingested_at, processing_status,
               processed_run_id, processed_at, error_summary
        FROM external_discovery_events
        WHERE processing_status = 'pending'
          AND observed_at::date <= %s::date
        ORDER BY observed_at::timestamp ASC, event_id ASC
        LIMIT %s
        """,
        (run_date, max(1, int(limit))),
    )
    return list(cur.fetchall())


def mark_external_discovery_event_processed(
    conn: Any,
    event_id: int,
    processed_run_id: str,
    commit: bool = True,
) -> None:
    conn.execute(
        """
        UPDATE external_discovery_events
        SET processing_status = 'processed',
            processed_run_id = %s,
            processed_at = %s,
            error_summary = ''
        WHERE event_id = %s
        """,
        ((processed_run_id or "").strip(), utc_now_iso(), int(event_id)),
    )
    if commit:
        conn.commit()


def mark_external_discovery_event_failed(
    conn: Any,
    event_id: int,
    processed_run_id: str,
    error_summary: str,
    commit: bool = True,
) -> None:
    conn.execute(
        """
        UPDATE external_discovery_events
        SET processing_status = 'failed',
            processed_run_id = %s,
            processed_at = %s,
            error_summary = %s
        WHERE event_id = %s
        """,
        (
            (processed_run_id or "").strip(),
            utc_now_iso(),
            (error_summary or "")[:500],
            int(event_id),
        ),
    )
    if commit:
        conn.commit()


def create_discovery_run(conn: Any, run_date: str, score_run_id: str) -> str:
    discovery_run_id = f"disc_{uuid.uuid4().hex[:12]}"
    conn.execute(
        """
        INSERT INTO discovery_runs (
            discovery_run_id,
            run_date,
            score_run_id,
            created_at,
            status,
            source_events_processed,
            observations_inserted,
            total_candidates,
            crm_eligible_candidates,
            error_summary
        )
        VALUES (%s, %s, %s, %s, 'running', 0, 0, 0, 0, '')
        """,
        (discovery_run_id, run_date, score_run_id, utc_now_iso()),
    )
    conn.commit()
    return discovery_run_id


def finish_discovery_run(
    conn: Any,
    discovery_run_id: str,
    status: str,
    source_events_processed: int,
    observations_inserted: int,
    total_candidates: int,
    crm_eligible_candidates: int,
    error_summary: str = "",
) -> None:
    conn.execute(
        """
        UPDATE discovery_runs
        SET status = %s,
            source_events_processed = %s,
            observations_inserted = %s,
            total_candidates = %s,
            crm_eligible_candidates = %s,
            error_summary = %s
        WHERE discovery_run_id = %s
        """,
        (
            status,
            max(0, int(source_events_processed)),
            max(0, int(observations_inserted)),
            max(0, int(total_candidates)),
            max(0, int(crm_eligible_candidates)),
            (error_summary or "")[:1000],
            discovery_run_id,
        ),
    )
    conn.commit()


def replace_discovery_candidates(
    conn: Any,
    discovery_run_id: str,
    candidates: list[dict[str, object]],
    evidence_rows: list[dict[str, object]],
) -> None:
    conn.execute("DELETE FROM discovery_candidates WHERE discovery_run_id = %s", (discovery_run_id,))
    conn.execute("DELETE FROM discovery_evidence WHERE discovery_run_id = %s", (discovery_run_id,))

    for row in candidates:
        conn.execute(
            """
            INSERT INTO discovery_candidates (
                discovery_run_id,
                score_run_id,
                run_date,
                account_id,
                company_name,
                domain,
                best_product,
                score,
                tier,
                confidence_band,
                cpg_like_group_count,
                primary_signal_count,
                source_count,
                has_poc_progression_first_party,
                relationship_stage,
                vertical_tag,
                is_self,
                exclude_from_crm,
                eligible_for_crm,
                novelty_score,
                rank_score,
                reasons_json
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                discovery_run_id,
                str(row.get("score_run_id", "")),
                str(row.get("run_date", "")),
                str(row.get("account_id", "")),
                str(row.get("company_name", "")),
                str(row.get("domain", "")),
                str(row.get("best_product", "")),
                float(row.get("score", 0.0)),
                str(row.get("tier", "low")),
                str(row.get("confidence_band", "explore")),
                int(row.get("cpg_like_group_count", 0)),
                int(row.get("primary_signal_count", 0)),
                int(row.get("source_count", 0)),
                int(row.get("has_poc_progression_first_party", 0)),
                str(row.get("relationship_stage", "unknown")),
                str(row.get("vertical_tag", "unknown")),
                int(row.get("is_self", 0)),
                int(row.get("exclude_from_crm", 0)),
                int(row.get("eligible_for_crm", 0)),
                float(row.get("novelty_score", 0.0)),
                float(row.get("rank_score", 0.0)),
                str(row.get("reasons_json", "[]")),
            ),
        )

    for row in evidence_rows:
        conn.execute(
            """
            INSERT INTO discovery_evidence (
                discovery_run_id,
                account_id,
                signal_code,
                source,
                evidence_url,
                evidence_text,
                component_score
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT(discovery_run_id, account_id, signal_code, source, evidence_url) DO UPDATE
            SET evidence_text = excluded.evidence_text,
                component_score = excluded.component_score
            """,
            (
                discovery_run_id,
                str(row.get("account_id", "")),
                str(row.get("signal_code", "")),
                str(row.get("source", "")),
                str(row.get("evidence_url", "")),
                str(row.get("evidence_text", ""))[:500],
                float(row.get("component_score", 0.0)),
            ),
        )

    conn.commit()


def get_latest_discovery_run_id_for_date(conn: Any, run_date: str) -> str | None:
    cur = conn.execute(
        """
        SELECT discovery_run_id
        FROM discovery_runs
        WHERE run_date::date = %s::date
          AND status = 'completed'
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (run_date,),
    )
    row = cur.fetchone()
    return None if row is None else str(row["discovery_run_id"])


def fetch_discovery_candidates_for_run(conn: Any, discovery_run_id: str) -> list[dict[str, Any]]:
    cur = conn.execute(
        """
        SELECT discovery_run_id, score_run_id, run_date, account_id, company_name,
               domain, best_product, score, tier, confidence_band,
               cpg_like_group_count, primary_signal_count, source_count,
               has_poc_progression_first_party, relationship_stage, vertical_tag,
               is_self, exclude_from_crm, eligible_for_crm, novelty_score,
               rank_score, reasons_json
        FROM discovery_candidates
        WHERE discovery_run_id = %s
        ORDER BY rank_score DESC, score DESC, company_name ASC
        """,
        (discovery_run_id,),
    )
    return list(cur.fetchall())


def fetch_discovery_run(conn: Any, discovery_run_id: str) -> dict[str, Any] | None:
    cur = conn.execute(
        """
        SELECT discovery_run_id, run_date, score_run_id, created_at, status,
               source_events_processed, observations_inserted, total_candidates,
               crm_eligible_candidates, error_summary
        FROM discovery_runs
        WHERE discovery_run_id = %s
        LIMIT 1
        """,
        (discovery_run_id,),
    )
    return cur.fetchone()
