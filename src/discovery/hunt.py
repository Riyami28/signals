from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import json
import logging
from typing import Any

from src import db
from src.discovery import fetcher, frontier, parser
from src.models import SignalObservation
from src.scoring.rules import load_keyword_lexicon, load_source_registry
from src.settings import Settings
from src.utils import stable_hash, utc_now_iso, write_csv_rows

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class HuntProfile:
    name: str
    frontier_budget: int
    fetch_limit: int
    parse_limit: int
    js_daily_cap: int
    static_workers: int
    parse_workers: int
    js_workers: int


def resolve_profile(name: str) -> HuntProfile:
    normalized = (name or "light").strip().lower()
    if normalized == "balanced":
        return HuntProfile(
            name="balanced",
            frontier_budget=4500,
            fetch_limit=1000,
            parse_limit=900,
            js_daily_cap=120,
            static_workers=6,
            parse_workers=4,
            js_workers=2,
        )
    if normalized == "heavy":
        return HuntProfile(
            name="heavy",
            frontier_budget=10000,
            fetch_limit=2500,
            parse_limit=2200,
            js_daily_cap=300,
            static_workers=10,
            parse_workers=6,
            js_workers=4,
        )
    return HuntProfile(
        name="light",
        frontier_budget=1800,
        fetch_limit=400,
        parse_limit=360,
        js_daily_cap=60,
        static_workers=6,
        parse_workers=4,
        js_workers=2,
    )


def build_frontier(conn, settings: Settings, run_date: date, profile: HuntProfile) -> dict[str, int | str]:
    return frontier.build_frontier(
        conn=conn,
        settings=settings,
        run_date=run_date,
        budget=profile.frontier_budget,
    )


def _parse_payload(payload_json: str) -> dict[str, Any]:
    if not payload_json:
        return {}
    try:
        parsed = json.loads(payload_json)
    except Exception:
        logger.debug("failed to parse hunt payload JSON", exc_info=True)
        return {}
    return parsed if isinstance(parsed, dict) else {}


def fetch_documents(conn, settings: Settings, run_date: date, profile: HuntProfile) -> dict[str, int | str]:
    run_date_str = run_date.isoformat()
    rows = db.fetch_crawl_frontier_by_status(conn, run_date=run_date_str, status="pending", limit=profile.fetch_limit)

    fetched = 0
    failed = 0
    js_used = 0

    for row in rows:
        frontier_row = dict(row)
        # Respect the configured JS daily cap.
        allow_js = js_used < profile.js_daily_cap
        result = fetcher.fetch_frontier_row(frontier_row, settings=settings, allow_js_fallback=allow_js)
        if not result.ok:
            db.mark_crawl_frontier_status(
                conn,
                frontier_id=str(row["frontier_id"]),
                status="failed",
                error_summary=result.error,
                bump_retry=True,
                commit=False,
            )
            failed += 1
            continue

        payload = _parse_payload(str(row["payload_json"] or ""))
        language_hint = str(payload.get("language_hint", "") or "")
        author_hint = str(payload.get("author_hint", "") or "")
        published_hint = str(payload.get("published_at_hint", "") or "")
        db.upsert_document(
            conn=conn,
            frontier_id=str(row["frontier_id"]),
            account_id=str(row["account_id"]),
            domain=str(row["domain"]),
            source=str(row["source"]),
            source_event_id=str(row["source_event_id"]),
            url=str(result.final_url or row["url"]),
            canonical_url=str(row["canonical_url"]),
            content_sha256=result.content_sha256,
            title="",
            author=author_hint,
            published_at=published_hint,
            section="",
            language=language_hint,
            body_text="",
            body_text_en="",
            raw_html=result.raw_html,
            parser_version="raw_fetch_v1",
            evidence_quality=0.0,
            relevance_score=0.0,
            fetched_with=result.fetched_with,
            outbound_links_json="[]",
            commit=False,
        )
        db.mark_crawl_frontier_status(
            conn,
            frontier_id=str(row["frontier_id"]),
            status="fetched",
            error_summary="",
            bump_retry=False,
            commit=False,
        )
        fetched += 1
        if result.fetched_with == "js_render":
            js_used += 1

    conn.commit()
    return {
        "run_date": run_date_str,
        "frontier_rows_seen": len(rows),
        "documents_fetched": fetched,
        "documents_failed": failed,
        "js_fetches_used": js_used,
    }


def _flatten_lexicon(lexicon_by_source: dict[str, list[dict[str, str]]]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    seen: set[tuple[str, str, str]] = set()
    for group in lexicon_by_source.values():
        for row in group:
            signal_code = (row.get("signal_code", "") or "").strip()
            keyword = (row.get("keyword", "") or "").strip().lower()
            confidence = (row.get("confidence", "") or "").strip()
            key = (signal_code, keyword, confidence)
            if not signal_code or not keyword or key in seen:
                continue
            seen.add(key)
            rows.append(row)
    return rows


def extract_documents(conn, settings: Settings, run_date: date, profile: HuntProfile) -> dict[str, int | str]:
    run_date_str = run_date.isoformat()
    docs = db.fetch_documents_for_run_by_frontier_status(
        conn,
        run_date=run_date_str,
        frontier_status="fetched",
        limit=profile.parse_limit,
    )
    lexicon = load_keyword_lexicon(settings.keyword_lexicon_path)
    flattened_lexicon = _flatten_lexicon(lexicon)
    source_registry = load_source_registry(settings.source_registry_path)
    story_source_rel = source_registry.get("story_hunt", 0.78)
    story_js_source_rel = source_registry.get("story_hunt_js", story_source_rel)

    parsed_docs = 0
    listing_pages = 0
    links_enqueued = 0
    mentions_inserted = 0
    observations_inserted = 0
    people_activity_inserted = 0

    for row in docs:
        payload = _parse_payload(str(row["payload_json"] or ""))
        parsed = parser.parse_document(
            html=str(row["raw_html"] or ""),
            url=str(row["url"] or row["canonical_url"] or ""),
            url_type=str(row["url_type"] or "article"),
            language_hint=str(payload.get("language_hint", "") or ""),
            author_hint=str(payload.get("author_hint", "") or row["author"] or ""),
            published_at_hint=str(payload.get("published_at_hint", "") or row["published_at"] or ""),
        )

        document_id = db.upsert_document(
            conn=conn,
            frontier_id=str(row["frontier_id"]),
            account_id=str(row["account_id"]),
            domain=str(row["domain"]),
            source=str(row["source"]),
            source_event_id=str(row["source_event_id"]),
            url=str(row["url"]),
            canonical_url=str(row["canonical_url"]),
            content_sha256=str(row["content_sha256"]),
            title=parsed.title,
            author=parsed.author,
            published_at=parsed.published_at,
            section=parsed.section,
            language=parsed.language,
            body_text=parsed.body_text,
            body_text_en=parsed.body_text_en,
            raw_html=str(row["raw_html"] or ""),
            parser_version=parser.PARSER_VERSION,
            evidence_quality=parsed.evidence_quality,
            relevance_score=parsed.relevance_score,
            fetched_with=str(row["fetched_with"] or "static_http"),
            outbound_links_json=json.dumps(parsed.outbound_links, ensure_ascii=True),
            commit=False,
        )
        parsed_docs += 1

        if parsed.is_listing:
            listing_pages += 1
            for link in parsed.outbound_links[:25]:
                canonical = frontier.canonicalize_url(link)
                if not canonical:
                    continue
                inserted = db.insert_crawl_frontier(
                    conn=conn,
                    run_date=run_date_str,
                    source=str(row["source"]),
                    source_event_id=str(row["source_event_id"]),
                    account_id=str(row["account_id"]),
                    domain=str(row["domain"]),
                    url=link,
                    canonical_url=canonical,
                    url_type="article",
                    depth=max(0, int(row["depth"])) + 1,
                    priority=max(0.0, float(row["priority"]) - 0.1),
                    max_retries=2,
                    payload_json=str(row["payload_json"] or "{}"),
                    commit=False,
                )
                if inserted:
                    links_enqueued += 1
            db.mark_crawl_frontier_status(
                conn,
                frontier_id=str(row["frontier_id"]),
                status="parsed",
                error_summary="listing_expanded",
                commit=False,
            )
            continue

        mentions = parser.extract_mentions(parsed, flattened_lexicon)
        document_source = "story_hunt_js" if str(row["fetched_with"] or "").startswith("js_") else "story_hunt"
        source_reliability = story_js_source_rel if document_source == "story_hunt_js" else story_source_rel
        observed_at = str(parsed.published_at or utc_now_iso())

        for mention in mentions:
            mention_id, mention_inserted = db.insert_document_mention(
                conn=conn,
                document_id=document_id,
                account_id=str(row["account_id"]),
                signal_code=mention.signal_code,
                matched_phrase=mention.matched_phrase,
                evidence_sentence=mention.evidence_sentence,
                evidence_sentence_en=mention.evidence_sentence_en,
                language=mention.language,
                speaker_name=mention.speaker_name,
                speaker_role=mention.speaker_role,
                confidence=mention.confidence,
                evidence_quality=parsed.evidence_quality,
                relevance_score=parsed.relevance_score,
                commit=False,
            )
            if mention_inserted:
                mentions_inserted += 1

            raw_hash = stable_hash(
                {
                    "document_id": document_id,
                    "mention_id": mention_id,
                    "signal_code": mention.signal_code,
                },
                prefix="raw",
            )
            obs_id = stable_hash(
                {
                    "account_id": str(row["account_id"]),
                    "signal_code": mention.signal_code,
                    "source": document_source,
                    "observed_at": observed_at,
                    "raw": raw_hash,
                },
                prefix="obs",
            )
            observation = SignalObservation(
                obs_id=obs_id,
                account_id=str(row["account_id"]),
                signal_code=mention.signal_code,
                product="shared",
                source=document_source,
                observed_at=observed_at,
                evidence_url=str(row["canonical_url"]),
                evidence_text=mention.evidence_sentence_en,
                document_id=document_id,
                mention_id=mention_id,
                evidence_sentence=mention.evidence_sentence,
                evidence_sentence_en=mention.evidence_sentence_en,
                matched_phrase=mention.matched_phrase,
                language=mention.language,
                speaker_name=mention.speaker_name,
                speaker_role=mention.speaker_role,
                evidence_quality=parsed.evidence_quality,
                relevance_score=parsed.relevance_score,
                confidence=mention.confidence,
                source_reliability=source_reliability,
                raw_payload_hash=raw_hash,
            )
            inserted_observation = db.insert_signal_observation(conn, observation, commit=False)
            if inserted_observation:
                observations_inserted += 1
                db.insert_observation_lineage(
                    conn=conn,
                    obs_id=obs_id,
                    account_id=str(row["account_id"]),
                    document_id=document_id,
                    mention_id=mention_id,
                    source_event_id=str(row["source_event_id"]),
                    run_date=run_date_str,
                    commit=False,
                )

            if mention.speaker_name:
                db.upsert_people_watchlist_entry(
                    conn=conn,
                    account_id=str(row["account_id"]),
                    person_name=mention.speaker_name,
                    role_title=mention.speaker_role or "Unknown",
                    role_weight=max(0.1, float(mention.speaker_weight)),
                    source_url=str(row["canonical_url"]),
                    is_active=True,
                    commit=False,
                )
                if db.insert_people_activity(
                    conn=conn,
                    account_id=str(row["account_id"]),
                    person_name=mention.speaker_name,
                    role_title=mention.speaker_role or "Unknown",
                    document_id=document_id,
                    activity_type="public_statement",
                    summary=mention.evidence_sentence_en,
                    published_at=observed_at,
                    url=str(row["canonical_url"]),
                    commit=False,
                ):
                    people_activity_inserted += 1

        db.mark_crawl_frontier_status(
            conn,
            frontier_id=str(row["frontier_id"]),
            status="parsed",
            error_summary="",
            commit=False,
        )

    conn.commit()
    return {
        "run_date": run_date_str,
        "documents_seen": len(docs),
        "documents_parsed": parsed_docs,
        "listing_pages": listing_pages,
        "links_enqueued": links_enqueued,
        "mentions_inserted": mentions_inserted,
        "observations_inserted": observations_inserted,
        "people_activity_inserted": people_activity_inserted,
    }


def write_hunt_reports(conn, settings: Settings, run_date: date) -> dict[str, int | str]:
    run_date_iso = run_date.isoformat()
    run_suffix = run_date.strftime("%Y%m%d")

    story_rows = db.fetch_story_evidence_rows(conn, run_date=run_date_iso)
    lineage_rows = db.fetch_signal_lineage_rows(conn, run_date=run_date_iso)

    story_output: list[dict[str, Any]] = []
    for row in story_rows:
        story_output.append(
            {
                "run_date": run_date_iso,
                "document_id": row["document_id"],
                "account_id": row["account_id"],
                "company_name": row["company_name"],
                "domain": row["domain"],
                "canonical_url": row["canonical_url"],
                "title": row["title"],
                "author": row["author"],
                "published_at": row["published_at"],
                "language": row["language"],
                "evidence_quality": row["evidence_quality"],
                "relevance_score": row["relevance_score"],
                "fetched_with": row["fetched_with"],
                "updated_at": row["updated_at"],
            }
        )

    lineage_output: list[dict[str, Any]] = []
    for row in lineage_rows:
        lineage_output.append(
            {
                "run_date": row["run_date"],
                "obs_id": row["obs_id"],
                "company_name": row["company_name"],
                "domain": row["domain"],
                "signal_code": row["signal_code"],
                "source": row["source"],
                "confidence": row["confidence"],
                "evidence_quality": row["evidence_quality"],
                "relevance_score": row["relevance_score"],
                "evidence_url": row["evidence_url"],
                "evidence_sentence": row["evidence_sentence"],
                "evidence_sentence_en": row["evidence_sentence_en"],
                "matched_phrase": row["matched_phrase"],
                "language": row["language"],
                "speaker_name": row["speaker_name"],
                "speaker_role": row["speaker_role"],
                "document_id": row["document_id"],
                "mention_id": row["mention_id"],
                "source_event_id": row["source_event_id"],
            }
        )

    metrics_rows = [
        {"run_date": run_date_iso, "metric": "documents_total", "value": len(story_output)},
        {
            "run_date": run_date_iso,
            "metric": "documents_quality_gte_0_80",
            "value": sum(1 for row in story_output if float(row["evidence_quality"]) >= 0.8),
        },
        {
            "run_date": run_date_iso,
            "metric": "documents_relevance_gte_0_65",
            "value": sum(1 for row in story_output if float(row["relevance_score"]) >= 0.65),
        },
        {"run_date": run_date_iso, "metric": "lineage_rows_total", "value": len(lineage_output)},
        {
            "run_date": run_date_iso,
            "metric": "lineage_quality_qualified",
            "value": sum(
                1
                for row in lineage_output
                if float(row["evidence_quality"]) >= 0.8 and float(row["relevance_score"]) >= 0.65
            ),
        },
        {
            "run_date": run_date_iso,
            "metric": "accounts_with_lineage",
            "value": len({str(row["domain"]) for row in lineage_output if str(row["domain"]).strip()}),
        },
    ]

    story_path = settings.out_dir / f"story_evidence_{run_suffix}.csv"
    lineage_path = settings.out_dir / f"signal_lineage_{run_suffix}.csv"
    metrics_path = settings.out_dir / f"hunt_quality_metrics_{run_suffix}.csv"

    write_csv_rows(
        story_path,
        story_output,
        fieldnames=[
            "run_date",
            "document_id",
            "account_id",
            "company_name",
            "domain",
            "canonical_url",
            "title",
            "author",
            "published_at",
            "language",
            "evidence_quality",
            "relevance_score",
            "fetched_with",
            "updated_at",
        ],
    )
    write_csv_rows(
        lineage_path,
        lineage_output,
        fieldnames=[
            "run_date",
            "obs_id",
            "company_name",
            "domain",
            "signal_code",
            "source",
            "confidence",
            "evidence_quality",
            "relevance_score",
            "evidence_url",
            "evidence_sentence",
            "evidence_sentence_en",
            "matched_phrase",
            "language",
            "speaker_name",
            "speaker_role",
            "document_id",
            "mention_id",
            "source_event_id",
        ],
    )
    write_csv_rows(
        metrics_path,
        metrics_rows,
        fieldnames=["run_date", "metric", "value"],
    )

    return {
        "story_evidence_rows": len(story_output),
        "signal_lineage_rows": len(lineage_output),
        "hunt_metrics_rows": len(metrics_rows),
        "story_evidence_path": str(story_path),
        "signal_lineage_path": str(lineage_path),
        "hunt_quality_metrics_path": str(metrics_path),
    }
