from __future__ import annotations

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
from src.utils import classify_text, load_account_source_handles, load_csv_rows, stable_hash, utc_now_iso


DEFAULT_NEWS_TERMS = "(soc 2 OR iso 27001 OR hipaa OR pci OR outage OR migration OR cloud cost OR devops)"


@retry(stop=stop_after_attempt(2), wait=wait_fixed(1), reraise=True)
def _request_text(url: str, settings: Settings) -> str:
    response = http_get(url, settings)
    response.raise_for_status()
    return response.text


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


def _match_signals(text: str, lexicon_rows: list[dict[str, str]]) -> list[tuple[str, float, str]]:
    return classify_text(text, lexicon_rows)


def _google_news_rss_url(query: str) -> str:
    return f"https://news.google.com/rss/search?q={quote_plus(query)}&hl=en-US&gl=US&ceid=US:en"


def _parse_entry_observed_at(entry: Any) -> str:
    published_parsed = entry.get("published_parsed")
    if published_parsed:
        try:
            return datetime(*published_parsed[:6], tzinfo=timezone.utc).isoformat()
        except Exception:
            pass
    published = entry.get("published")
    if published:
        return str(published)
    return utc_now_iso()


def _ingest_feed_entries(
    conn,
    account_id: str,
    source: str,
    source_reliability: float,
    lexicon_rows: list[dict[str, str]],
    entries: list[Any],
    extra_payload: dict[str, Any],
) -> tuple[int, int]:
    inserted = 0
    seen = 0

    for entry in entries[:30]:
        title = str(entry.get("title", ""))
        summary = str(entry.get("summary", ""))
        link = str(entry.get("link", ""))
        text = f"{title}\n{summary}".strip()

        matches = _match_signals(text, lexicon_rows)
        if not matches:
            continue

        observed_at = _parse_entry_observed_at(entry)
        for signal_code, confidence, matched_keyword in matches:
            seen += 1
            observation = _build_observation(
                account_id=account_id,
                signal_code=signal_code,
                source=source,
                observed_at=observed_at,
                confidence=confidence,
                source_reliability=source_reliability,
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


def collect(
    conn,
    settings: Settings,
    lexicon_by_source: dict[str, list[dict[str, str]]],
    source_reliability: dict[str, float],
) -> dict[str, int]:
    inserted = 0
    seen = 0

    lexicon_rows = lexicon_by_source.get("news", [])
    source = "news_csv"
    reliability = source_reliability.get(source, 0.75)

    if reliability > 0:
        for row in load_csv_rows(settings.raw_dir / "news.csv"):
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

            title = row.get("title", "")
            content = row.get("content", "")
            text = f"{title}\n{content}".strip()

            explicit_signal = row.get("signal_code", "")
            if explicit_signal:
                try:
                    explicit_confidence = float(row.get("confidence", "0.75") or 0.75)
                except ValueError:
                    explicit_confidence = 0.75
                matches = [(explicit_signal, explicit_confidence, "explicit")]
            else:
                matches = _match_signals(text, lexicon_rows)

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

    feed_source = "rss_feed"
    feed_reliability = source_reliability.get(feed_source, 0.7)
    feeds_path = settings.raw_dir / "news_feeds.csv"
    if feed_reliability > 0:
        feed_rows = load_csv_rows(feeds_path)

        for feed_row in feed_rows:
            domain = feed_row.get("domain", "")
            feed_url = feed_row.get("feed_url", "")
            if not domain or not feed_url:
                continue
            company_name = feed_row.get("company_name", "") or domain
            account_id = db.upsert_account(
                conn,
                company_name=company_name,
                domain=domain,
                source_type="discovered",
                commit=False,
            )

            try:
                parsed = feedparser.parse(feed_url)
            except Exception:
                continue

            local_inserted, local_seen = _ingest_feed_entries(
                conn=conn,
                account_id=account_id,
                source=feed_source,
                source_reliability=feed_reliability,
                lexicon_rows=lexicon_rows,
                entries=list(parsed.entries),
                extra_payload={"feed_url": feed_url},
            )
            inserted += local_inserted
            seen += local_seen

    if settings.enable_live_crawl:
        handles = load_account_source_handles(settings.account_source_handles_path)
        accounts = conn.execute(
            "SELECT account_id, domain, company_name FROM accounts ORDER BY created_at LIMIT ?",
            (settings.live_max_accounts,),
        ).fetchall()

        google_source = "google_news_rss"
        google_reliability = source_reliability.get(google_source, 0.72)

        for account in accounts:
            domain = str(account["domain"])
            if domain.endswith(".example"):
                continue
            account_id = str(account["account_id"])
            company_name = str(account["company_name"] or domain)
            handle_row = handles.get(domain, {})

            override_rss = handle_row.get("news_rss", "").strip()
            news_query = handle_row.get("news_query", "").strip()

            if override_rss:
                feed_url = override_rss
                source_name = feed_source
                reliability_value = feed_reliability
                query_used = ""
            else:
                if not news_query:
                    news_query = f'"{company_name}" OR "{domain}" {DEFAULT_NEWS_TERMS}'
                feed_url = _google_news_rss_url(news_query)
                source_name = google_source
                reliability_value = google_reliability
                query_used = news_query

            if reliability_value <= 0:
                continue

            try:
                if db.was_crawled_today(conn, source=source_name, account_id=account_id, endpoint=feed_url):
                    db.record_crawl_attempt(
                        conn,
                        source=source_name,
                        account_id=account_id,
                        endpoint=feed_url,
                        status="skipped",
                        error_summary="checkpoint_recent",
                        commit=False,
                    )
                    continue
                xml_text = _request_text(feed_url, settings)
                parsed = feedparser.parse(xml_text)
            except requests.HTTPError as exc:
                status_code = exc.response.status_code if exc.response is not None else 0
                db.record_crawl_attempt(
                    conn,
                    source=source_name,
                    account_id=account_id,
                    endpoint=feed_url,
                    status="http_error",
                    error_summary=f"status_code={status_code}",
                    commit=False,
                )
                db.mark_crawled(conn, source=source_name, account_id=account_id, endpoint=feed_url, commit=False)
                continue
            except Exception as exc:
                db.record_crawl_attempt(
                    conn,
                    source=source_name,
                    account_id=account_id,
                    endpoint=feed_url,
                    status="exception",
                    error_summary=str(exc),
                    commit=False,
                )
                db.mark_crawled(conn, source=source_name, account_id=account_id, endpoint=feed_url, commit=False)
                continue
            db.record_crawl_attempt(
                conn,
                source=source_name,
                account_id=account_id,
                endpoint=feed_url,
                status="success",
                error_summary="",
                commit=False,
            )
            db.mark_crawled(conn, source=source_name, account_id=account_id, endpoint=feed_url, commit=False)

            local_inserted, local_seen = _ingest_feed_entries(
                conn=conn,
                account_id=account_id,
                source=source_name,
                source_reliability=reliability_value,
                lexicon_rows=lexicon_rows,
                entries=list(parsed.entries),
                extra_payload={"feed_url": feed_url, "query": query_used},
            )
            inserted += local_inserted
            seen += local_seen

    conn.commit()
    return {"inserted": inserted, "seen": seen}
