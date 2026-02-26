from __future__ import annotations

import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote_plus

import feedparser
import requests
from tenacity import retry, stop_after_attempt, wait_fixed

from src import db
from src.http_client import get as http_get
from src.models import SignalObservation
from src.settings import Settings
from src.utils import (
    classify_text,
    load_account_source_handles,
    load_csv_rows,
    stable_hash,
    utc_now_iso,
)

logger = logging.getLogger(__name__)

DEFAULT_REDDIT_TERMS = (
    "(devops OR platform engineering OR cloud cost OR finops OR kubernetes OR terraform OR soc2 OR audit)"
)
_LIVE_PROGRESS_COMMIT_EVERY = 25
_VERBOSE_PROGRESS = os.getenv("SIGNALS_VERBOSE_PROGRESS", "").strip().lower() in {"1", "true", "yes", "on"}


def _emit_progress(message: str) -> None:
    if _VERBOSE_PROGRESS:
        print(message, flush=True)


@retry(stop=stop_after_attempt(2), wait=wait_fixed(1), reraise=True)
def _request_text(url: str, settings: Settings) -> str:
    response = http_get(url, settings)
    response.raise_for_status()
    return response.text


def _reddit_search_rss_url(query: str) -> str:
    return f"https://www.reddit.com/search.rss?q={quote_plus(query)}&sort=new&t=month"


def _build_observation(
    account_id: str,
    signal_code: str,
    source: str,
    observed_at: str,
    confidence: float,
    source_reliability: float,
    evidence_url: str,
    evidence_text: str,
    payload: dict[str, Any],
) -> SignalObservation:
    raw_hash = stable_hash(payload, prefix="raw")
    obs_id = stable_hash(
        {
            "account_id": account_id,
            "signal_code": signal_code,
            "source": source,
            "observed_at": observed_at,
            "raw": raw_hash,
        },
        prefix="obs",
    )
    return SignalObservation(
        obs_id=obs_id,
        account_id=account_id,
        signal_code=signal_code,
        product="shared",
        source=source,
        observed_at=observed_at,
        evidence_url=evidence_url,
        evidence_text=evidence_text[:500],
        confidence=max(0.0, min(1.0, float(confidence))),
        source_reliability=max(0.0, min(1.0, float(source_reliability))),
        raw_payload_hash=raw_hash,
    )


def _parse_entry_observed_at(entry: Any) -> str:
    published_parsed = entry.get("published_parsed")
    if published_parsed:
        try:
            return datetime(*published_parsed[:6], tzinfo=timezone.utc).isoformat()
        except Exception:
            logger.warning("failed to parse published_parsed for entry", exc_info=True)
    published = entry.get("published")
    if published:
        return str(published)
    return utc_now_iso()


def _ingest_entries(
    conn,
    account_id: str,
    source: str,
    reliability: float,
    lexicon_rows: list[dict[str, str]],
    entries: list[Any],
    extra_payload: dict[str, str],
) -> tuple[int, int]:
    inserted = 0
    seen = 0
    for entry in entries[:30]:
        title = str(entry.get("title", ""))
        summary = str(entry.get("summary", ""))
        link = str(entry.get("link", ""))
        text = f"{title}\n{summary}".strip()
        matches = classify_text(text, lexicon_rows)
        for signal_code, confidence, matched_keyword in matches:
            seen += 1
            observation = _build_observation(
                account_id=account_id,
                signal_code=signal_code,
                source=source,
                observed_at=_parse_entry_observed_at(entry),
                confidence=confidence,
                source_reliability=reliability,
                evidence_url=link,
                evidence_text=text,
                payload={
                    "entry": {"title": title, "summary": summary, "link": link},
                    "matched_keyword": matched_keyword,
                    **extra_payload,
                },
            )
            if db.insert_signal_observation(conn, observation, commit=False):
                inserted += 1
    return inserted, seen


def _collect_live_reddit_account(
    conn,
    settings: Settings,
    lexicon_rows: list[dict[str, str]],
    account: dict[str, Any],
    account_index: int,
    handles: dict[str, dict[str, str]],
    rss_source: str,
    rss_reliability: float,
) -> tuple[int, int, int]:
    domain = str(account["domain"])
    if domain.endswith(".example"):
        return 0, 0, 0
    account_id = str(account["account_id"])
    _emit_progress(f"collector=community_live status=account_started account_index={account_index} domain={domain}")
    company_name = str(account["company_name"] or domain)
    handle_row = handles.get(domain, {})
    query = handle_row.get("reddit_query", "").strip()
    if not query:
        query = f'"{company_name}" OR "{domain}" {DEFAULT_REDDIT_TERMS}'

    rss_url = _reddit_search_rss_url(query)
    if db.was_crawled_today(conn, source=rss_source, account_id=account_id, endpoint=rss_url):
        db.record_crawl_attempt(
            conn,
            source=rss_source,
            account_id=account_id,
            endpoint=rss_url,
            status="skipped",
            error_summary="checkpoint_recent",
            commit=False,
        )
        return 0, 0, 1

    try:
        xml_text = _request_text(rss_url, settings)
        parsed = feedparser.parse(xml_text)
    except requests.HTTPError as exc:
        status_code = exc.response.status_code if exc.response is not None else 0
        db.record_crawl_attempt(
            conn,
            source=rss_source,
            account_id=account_id,
            endpoint=rss_url,
            status="http_error",
            error_summary=f"status_code={status_code}",
            commit=False,
        )
        db.mark_crawled(conn, source=rss_source, account_id=account_id, endpoint=rss_url, commit=False)
        return 0, 0, 1
    except Exception as exc:
        db.record_crawl_attempt(
            conn,
            source=rss_source,
            account_id=account_id,
            endpoint=rss_url,
            status="exception",
            error_summary=str(exc),
            commit=False,
        )
        db.mark_crawled(conn, source=rss_source, account_id=account_id, endpoint=rss_url, commit=False)
        return 0, 0, 1

    db.record_crawl_attempt(
        conn,
        source=rss_source,
        account_id=account_id,
        endpoint=rss_url,
        status="success",
        error_summary="",
        commit=False,
    )
    db.mark_crawled(conn, source=rss_source, account_id=account_id, endpoint=rss_url, commit=False)

    inserted_delta, seen_delta = _ingest_entries(
        conn=conn,
        account_id=account_id,
        source=rss_source,
        reliability=rss_reliability,
        lexicon_rows=lexicon_rows,
        entries=list(parsed.entries),
        extra_payload={"query": query, "rss_url": rss_url},
    )
    _emit_progress(
        "collector=community_live status=account_completed "
        f"account_index={account_index} domain={domain} inserted_delta={inserted_delta} seen_delta={seen_delta}"
    )
    return inserted_delta, seen_delta, 1


def _collect_live_reddit_parallel(
    conn,
    settings: Settings,
    lexicon_rows: list[dict[str, str]],
    accounts: list[dict[str, Any]],
    handles: dict[str, dict[str, str]],
    rss_source: str,
    rss_reliability: float,
    db_pool=None,
) -> tuple[int, int]:
    if not accounts:
        return 0, 0

    workers = min(max(1, int(settings.live_workers_per_source)), len(accounts))
    if workers <= 1:
        inserted_total = 0
        seen_total = 0
        processed = 0
        for idx, account in enumerate(accounts, start=1):
            inserted_delta, seen_delta, processed_delta = _collect_live_reddit_account(
                conn=conn,
                settings=settings,
                lexicon_rows=lexicon_rows,
                account=account,
                account_index=idx,
                handles=handles,
                rss_source=rss_source,
                rss_reliability=rss_reliability,
            )
            inserted_total += inserted_delta
            seen_total += seen_delta
            processed += processed_delta
            if processed and processed % _LIVE_PROGRESS_COMMIT_EVERY == 0:
                conn.commit()
                _emit_progress(
                    f"collector=community_live status=checkpoint committed_accounts={processed} "
                    f"inserted_total={inserted_total} seen_total={seen_total}"
                )
        return inserted_total, seen_total

    conn.commit()
    indexed_accounts = list(enumerate(accounts, start=1))
    batches = [indexed_accounts[i::workers] for i in range(workers)]

    def _worker(batch: list[tuple[int, dict[str, Any]]]) -> tuple[int, int]:
        if db_pool is not None:
            worker_conn = db_pool.getconn()
        else:
            worker_conn = db.get_connection(settings.pg_dsn)
        worker_inserted = 0
        worker_seen = 0
        processed = 0
        try:
            for account_index, account in batch:
                inserted_delta, seen_delta, processed_delta = _collect_live_reddit_account(
                    conn=worker_conn,
                    settings=settings,
                    lexicon_rows=lexicon_rows,
                    account=account,
                    account_index=account_index,
                    handles=handles,
                    rss_source=rss_source,
                    rss_reliability=rss_reliability,
                )
                worker_inserted += inserted_delta
                worker_seen += seen_delta
                processed += processed_delta
                if processed and processed % _LIVE_PROGRESS_COMMIT_EVERY == 0:
                    worker_conn.commit()
            worker_conn.commit()
            return worker_inserted, worker_seen
        finally:
            if db_pool is not None:
                db_pool.putconn(worker_conn)
            else:
                worker_conn.close()

    inserted_total = 0
    seen_total = 0
    failed_workers = 0
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(_worker, batch) for batch in batches if batch]
        for future in as_completed(futures):
            try:
                batch_inserted, batch_seen = future.result(timeout=settings.stage_timeout_seconds)
            except Exception as e:
                logger.error("collector_worker_failed source=community error=%s", e, exc_info=True)
                batch_inserted, batch_seen = 0, 0
                failed_workers += 1
            inserted_total += batch_inserted
            seen_total += batch_seen
    logger.info(
        "collection_complete source=community inserted=%d seen=%d failed_workers=%d",
        inserted_total,
        seen_total,
        failed_workers,
    )
    return inserted_total, seen_total


def collect(
    conn,
    settings: Settings,
    lexicon_by_source: dict[str, list[dict[str, str]]],
    source_reliability: dict[str, float],
    db_pool=None,
) -> dict[str, int]:
    inserted = 0
    seen = 0

    lexicon_rows = lexicon_by_source.get("community", [])
    source = "community_csv"
    reliability = source_reliability.get(source, 0.65)

    if reliability > 0:
        for row in load_csv_rows(settings.raw_dir / "community.csv"):
            domain = row.get("domain", "")
            if not domain:
                continue
            company_name = row.get("company_name", "") or domain
            account_id = db.upsert_account(
                conn,
                company_name=company_name,
                domain=domain,
                source_type="discovered",
                commit=False,
            )

            text = row.get("text", "")
            explicit_signal = row.get("signal_code", "")
            if explicit_signal:
                try:
                    explicit_confidence = float(row.get("confidence", "0.7") or 0.7)
                except ValueError:
                    explicit_confidence = 0.7
                matches = [(explicit_signal, explicit_confidence, "explicit")]
            else:
                matches = classify_text(text, lexicon_rows)

            observed_at = row.get("observed_at", "") or utc_now_iso()
            for signal_code, confidence, matched_keyword in matches:
                seen += 1
                observation = _build_observation(
                    account_id=account_id,
                    signal_code=signal_code,
                    source=source,
                    observed_at=observed_at,
                    confidence=confidence,
                    source_reliability=reliability,
                    evidence_url=row.get("url", ""),
                    evidence_text=text,
                    payload={"row": row, "matched_keyword": matched_keyword},
                )
                if db.insert_signal_observation(conn, observation, commit=False):
                    inserted += 1

    if settings.enable_live_crawl:
        handles = load_account_source_handles(settings.account_source_handles_path)
        rss_source = "reddit_rss"
        rss_reliability = source_reliability.get(rss_source, 0.62)
        accounts = db.select_accounts_for_live_crawl(
            conn,
            source=rss_source,
            limit=settings.live_max_accounts,
            include_domains=list(settings.live_target_domains),
        )
        _emit_progress(
            f"collector=community_live status=started accounts={len(accounts)} workers={settings.live_workers_per_source}"
        )
        if rss_reliability <= 0:
            conn.commit()
            return {"inserted": inserted, "seen": seen}

        live_inserted, live_seen = _collect_live_reddit_parallel(
            conn=conn,
            settings=settings,
            lexicon_rows=lexicon_rows,
            accounts=accounts,
            handles=handles,
            rss_source=rss_source,
            rss_reliability=rss_reliability,
            db_pool=db_pool,
        )
        inserted += live_inserted
        seen += live_seen
        _emit_progress(
            "collector=community_live status=completed "
            f"accounts_targeted={len(accounts)} inserted_total={inserted} seen_total={seen}"
        )

    conn.commit()
    return {"inserted": inserted, "seen": seen}
