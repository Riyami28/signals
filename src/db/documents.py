from __future__ import annotations

import json
import uuid
from typing import Any

from src.utils import normalize_domain, stable_hash, utc_now_iso

from .connection import _is_integrity_error


def get_document_by_frontier_id(conn: Any, frontier_id: str) -> dict[str, Any] | None:
    cur = conn.execute(
        """
        SELECT document_id, frontier_id, account_id, domain, source,
               source_event_id, url, canonical_url, content_sha256, title,
               author, published_at, section, language, body_text, body_text_en,
               raw_html, parser_version, evidence_quality, relevance_score,
               fetched_with, outbound_links_json, created_at, updated_at
        FROM documents
        WHERE frontier_id = %s
        LIMIT 1
        """,
        (frontier_id,),
    )
    return cur.fetchone()


def upsert_document(
    conn: Any,
    frontier_id: str,
    account_id: str,
    domain: str,
    source: str,
    source_event_id: str,
    url: str,
    canonical_url: str,
    content_sha256: str,
    title: str,
    author: str,
    published_at: str,
    section: str,
    language: str,
    body_text: str,
    body_text_en: str,
    raw_html: str,
    parser_version: str,
    evidence_quality: float,
    relevance_score: float,
    fetched_with: str,
    outbound_links_json: str = "[]",
    commit: bool = True,
) -> str:
    document_id = stable_hash({"canonical_url": canonical_url}, prefix="doc", length=16)
    now = utc_now_iso()
    savepoint_name = f"sp_doc_{uuid.uuid4().hex[:8]}"
    conn.execute(f"SAVEPOINT {savepoint_name}")
    try:
        conn.execute(
            """
            INSERT INTO documents (
                document_id,
                frontier_id,
                account_id,
                domain,
                source,
                source_event_id,
                url,
                canonical_url,
                content_sha256,
                title,
                author,
                published_at,
                section,
                language,
                body_text,
                body_text_en,
                raw_html,
                parser_version,
                evidence_quality,
                relevance_score,
                fetched_with,
                outbound_links_json,
                created_at,
                updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT(canonical_url) DO UPDATE
            SET frontier_id = excluded.frontier_id,
                account_id = excluded.account_id,
                domain = excluded.domain,
                source = excluded.source,
                source_event_id = excluded.source_event_id,
                url = excluded.url,
                content_sha256 = excluded.content_sha256,
                title = excluded.title,
                author = excluded.author,
                published_at = excluded.published_at,
                section = excluded.section,
                language = excluded.language,
                body_text = excluded.body_text,
                body_text_en = excluded.body_text_en,
                raw_html = excluded.raw_html,
                parser_version = excluded.parser_version,
                evidence_quality = excluded.evidence_quality,
                relevance_score = excluded.relevance_score,
                fetched_with = excluded.fetched_with,
                outbound_links_json = excluded.outbound_links_json,
                updated_at = excluded.updated_at
            """,
            (
                document_id,
                frontier_id,
                account_id,
                normalize_domain(domain or ""),
                (source or "").strip().lower(),
                (source_event_id or "").strip(),
                (url or "").strip(),
                (canonical_url or "").strip(),
                (content_sha256 or "").strip(),
                (title or "")[:500],
                (author or "")[:250],
                (published_at or "").strip(),
                (section or "")[:120],
                (language or "").strip().lower(),
                (body_text or "")[:200000],
                (body_text_en or "")[:200000],
                (raw_html or "")[:200000],
                (parser_version or "")[:80],
                max(0.0, min(1.0, float(evidence_quality))),
                max(0.0, min(1.0, float(relevance_score))),
                (fetched_with or "")[:40],
                (outbound_links_json or "[]")[:12000],
                now,
                now,
            ),
        )
        if commit:
            conn.commit()
        else:
            conn.execute(f"RELEASE SAVEPOINT {savepoint_name}")
    except Exception as exc:
        if not _is_integrity_error(exc):
            raise
        conn.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
        conn.execute(f"RELEASE SAVEPOINT {savepoint_name}")
        # Content hash collisions can happen when multiple URLs resolve to the same article.
        row = conn.execute(
            """
            SELECT document_id
            FROM documents
            WHERE canonical_url = %s
               OR content_sha256 = %s
            LIMIT 1
            """,
            ((canonical_url or "").strip(), (content_sha256 or "").strip()),
        ).fetchone()
        if row is not None:
            return str(row["document_id"])
        raise exc
    return document_id


def fetch_documents_for_run_by_frontier_status(
    conn: Any,
    run_date: str,
    frontier_status: str,
    limit: int = 500,
) -> list[dict[str, Any]]:
    cur = conn.execute(
        """
        SELECT d.document_id, d.frontier_id, d.account_id, d.domain, d.source,
               d.source_event_id, d.url, d.canonical_url, d.content_sha256,
               d.title, d.author, d.published_at, d.section, d.language,
               d.body_text, d.body_text_en, d.raw_html, d.parser_version,
               d.evidence_quality, d.relevance_score, d.fetched_with,
               d.outbound_links_json, d.created_at, d.updated_at,
               f.url_type, f.depth, f.priority, f.payload_json, f.frontier_id, f.source_event_id, f.source
        FROM documents d
        JOIN crawl_frontier f ON f.frontier_id = d.frontier_id
        WHERE f.run_date = %s
          AND f.status = %s
        ORDER BY f.priority DESC, d.updated_at ASC
        LIMIT %s
        """,
        (run_date, frontier_status, max(1, int(limit))),
    )
    return list(cur.fetchall())


def insert_document_mention(
    conn: Any,
    document_id: str,
    account_id: str,
    signal_code: str,
    matched_phrase: str,
    evidence_sentence: str,
    evidence_sentence_en: str,
    language: str,
    speaker_name: str,
    speaker_role: str,
    confidence: float,
    evidence_quality: float,
    relevance_score: float,
    commit: bool = True,
) -> tuple[str, bool]:
    normalized_phrase = (matched_phrase or "").strip().lower()
    mention_id = stable_hash(
        {
            "document_id": document_id,
            "signal_code": signal_code,
            "matched_phrase": normalized_phrase,
        },
        prefix="mnt",
        length=16,
    )
    cur = conn.execute(
        """
        INSERT INTO document_mentions (
            mention_id,
            document_id,
            account_id,
            signal_code,
            matched_phrase,
            evidence_sentence,
            evidence_sentence_en,
            language,
            speaker_name,
            speaker_role,
            confidence,
            evidence_quality,
            relevance_score,
            created_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
        RETURNING mention_id
        """,
        (
            mention_id,
            document_id,
            account_id,
            signal_code,
            normalized_phrase,
            (evidence_sentence or "")[:1500],
            (evidence_sentence_en or "")[:1500],
            (language or "").strip().lower(),
            (speaker_name or "")[:200],
            (speaker_role or "")[:120],
            max(0.0, min(1.0, float(confidence))),
            max(0.0, min(1.0, float(evidence_quality))),
            max(0.0, min(1.0, float(relevance_score))),
            utc_now_iso(),
        ),
    )
    if commit:
        conn.commit()
    return mention_id, (cur.fetchone() is not None)


def insert_observation_lineage(
    conn: Any,
    obs_id: str,
    account_id: str,
    document_id: str,
    mention_id: str,
    source_event_id: str,
    run_date: str,
    commit: bool = True,
) -> bool:
    cur = conn.execute(
        """
        INSERT INTO observation_lineage (
            obs_id,
            account_id,
            document_id,
            mention_id,
            source_event_id,
            run_date,
            created_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
        RETURNING obs_id
        """,
        (
            obs_id,
            account_id,
            document_id,
            mention_id,
            (source_event_id or "")[:250],
            run_date,
            utc_now_iso(),
        ),
    )
    if commit:
        conn.commit()
    return cur.fetchone() is not None


def upsert_people_watchlist_entry(
    conn: Any,
    account_id: str,
    person_name: str,
    role_title: str,
    role_weight: float,
    source_url: str,
    is_active: bool = True,
    commit: bool = True,
) -> str:
    watch_id = stable_hash(
        {"account_id": account_id, "person_name": person_name, "role_title": role_title},
        prefix="pwl",
        length=16,
    )
    now = utc_now_iso()
    conn.execute(
        """
        INSERT INTO people_watchlist (
            watch_id,
            account_id,
            person_name,
            role_title,
            role_weight,
            source_url,
            is_active,
            created_at,
            updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT(account_id, person_name, role_title) DO UPDATE
        SET role_weight = excluded.role_weight,
            source_url = excluded.source_url,
            is_active = excluded.is_active,
            updated_at = excluded.updated_at
        """,
        (
            watch_id,
            account_id,
            (person_name or "")[:200],
            (role_title or "")[:120],
            max(0.0, min(2.0, float(role_weight))),
            (source_url or "")[:500],
            1 if is_active else 0,
            now,
            now,
        ),
    )
    if commit:
        conn.commit()
    return watch_id


def insert_people_activity(
    conn: Any,
    account_id: str,
    person_name: str,
    role_title: str,
    document_id: str,
    activity_type: str,
    summary: str,
    published_at: str,
    url: str,
    commit: bool = True,
) -> bool:
    activity_id = stable_hash(
        {
            "account_id": account_id,
            "person_name": person_name,
            "document_id": document_id,
            "activity_type": activity_type,
        },
        prefix="pac",
        length=16,
    )
    cur = conn.execute(
        """
        INSERT INTO people_activity (
            activity_id,
            account_id,
            person_name,
            role_title,
            document_id,
            activity_type,
            summary,
            published_at,
            url,
            created_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
        RETURNING activity_id
        """,
        (
            activity_id,
            account_id,
            (person_name or "")[:200],
            (role_title or "")[:120],
            document_id,
            (activity_type or "")[:120],
            (summary or "")[:1500],
            (published_at or "")[:80],
            (url or "")[:500],
            utc_now_iso(),
        ),
    )
    if commit:
        conn.commit()
    return cur.fetchone() is not None


def fetch_story_evidence_rows(conn: Any, run_date: str) -> list[dict[str, Any]]:
    cur = conn.execute(
        """
        SELECT
            d.document_id,
            d.account_id,
            a.company_name,
            a.domain,
            d.canonical_url,
            d.title,
            d.author,
            d.published_at,
            d.language,
            d.evidence_quality,
            d.relevance_score,
            d.fetched_with,
            d.updated_at
        FROM documents d
        JOIN accounts a ON a.account_id = d.account_id
        JOIN crawl_frontier f ON f.frontier_id = d.frontier_id
        WHERE f.run_date = %s
        ORDER BY d.evidence_quality DESC, d.relevance_score DESC, d.updated_at DESC
        """,
        (run_date,),
    )
    return list(cur.fetchall())


def fetch_signal_lineage_rows(conn: Any, run_date: str) -> list[dict[str, Any]]:
    cur = conn.execute(
        """
        SELECT
            ol.run_date,
            ol.obs_id,
            a.company_name,
            a.domain,
            so.signal_code,
            so.source,
            so.confidence,
            so.evidence_quality,
            so.relevance_score,
            so.evidence_url,
            so.evidence_sentence,
            so.evidence_sentence_en,
            so.matched_phrase,
            so.language,
            so.speaker_name,
            so.speaker_role,
            ol.document_id,
            ol.mention_id,
            ol.source_event_id
        FROM observation_lineage ol
        JOIN signal_observations so ON so.obs_id = ol.obs_id
        JOIN accounts a ON a.account_id = ol.account_id
        WHERE ol.run_date = %s
        ORDER BY so.evidence_quality DESC, so.relevance_score DESC, so.confidence DESC
        """,
        (run_date,),
    )
    return list(cur.fetchall())
