from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_fixed

from src import db
from src.http_client import get as http_get
from src.models import SignalObservation
from src.settings import Settings
from src.utils import classify_text, load_csv_rows, stable_hash, utc_now_iso

DISCOVERY_LINK_TOKENS = (
    "digital",
    "technology",
    "innovation",
    "cloud",
    "platform",
    "data",
    "news",
    "press",
    "investor",
    "careers",
    "jobs",
)
MAX_DISCOVERED_LINKS_PER_ACCOUNT = 3
MAX_SCAN_TEXT_CHARS = 8000
_LIVE_PROGRESS_COMMIT_EVERY = 25
_VERBOSE_PROGRESS = os.getenv("SIGNALS_VERBOSE_PROGRESS", "").strip().lower() in {"1", "true", "yes", "on"}


def _emit_progress(message: str) -> None:
    if _VERBOSE_PROGRESS:
        print(message, flush=True)


@retry(stop=stop_after_attempt(2), wait=wait_fixed(1), reraise=True)
def _fetch_page_profile(url: str, settings: Settings) -> tuple[str, list[str]]:
    response = http_get(url, settings)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    text = soup.get_text(" ", strip=True)[:MAX_SCAN_TEXT_CHARS]

    parsed_base = urlparse(url)
    base_domain = parsed_base.netloc.lower().replace("www.", "")

    discovered: list[str] = []
    seen: set[str] = set()

    for anchor in soup.find_all("a", href=True):
        href = str(anchor.get("href", "")).strip()
        if not href or href.startswith("#") or href.startswith("javascript:"):
            continue

        label = anchor.get_text(" ", strip=True).lower()
        absolute = urljoin(url, href)
        parsed = urlparse(absolute)
        if parsed.scheme not in {"http", "https"}:
            continue

        target_domain = parsed.netloc.lower().replace("www.", "")
        if target_domain and target_domain != base_domain:
            continue

        path_text = f"{parsed.path} {parsed.query}".lower()
        if not any(token in path_text or token in label for token in DISCOVERY_LINK_TOKENS):
            continue

        normalized = f"{parsed.scheme}://{parsed.netloc}{parsed.path}".rstrip("/")
        if normalized in seen:
            continue

        seen.add(normalized)
        discovered.append(normalized)
        if len(discovered) >= 8:
            break

    return text, discovered


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


def _collect_live_technographics_account(
    conn,
    settings: Settings,
    lexicon_rows: list[dict[str, str]],
    account: dict[str, Any],
    account_index: int,
    scan_source: str,
    scan_reliability: float,
) -> tuple[int, int, int]:
    account_id = str(account["account_id"])
    domain = str(account["domain"])
    if domain.endswith(".example"):
        return 0, 0, 0
    _emit_progress(
        f"collector=technographics_live status=account_started account_index={account_index} domain={domain}"
    )

    homepage_url = f"https://{domain}"
    urls_to_scan = [homepage_url]
    scanned: set[str] = set()
    inserted_delta = 0
    seen_delta = 0

    while urls_to_scan:
        url = urls_to_scan.pop(0)
        if url in scanned:
            continue
        scanned.add(url)

        if db.was_crawled_today(conn, source=scan_source, account_id=account_id, endpoint=url):
            db.record_crawl_attempt(
                conn,
                source=scan_source,
                account_id=account_id,
                endpoint=url,
                status="skipped",
                error_summary="checkpoint_recent",
                commit=False,
            )
            continue

        try:
            page_text, discovered_links = _fetch_page_profile(url, settings)
        except requests.HTTPError as exc:
            status_code = exc.response.status_code if exc.response is not None else 0
            db.record_crawl_attempt(
                conn,
                source=scan_source,
                account_id=account_id,
                endpoint=url,
                status="http_error",
                error_summary=f"status_code={status_code}",
                commit=False,
            )
            db.mark_crawled(conn, source=scan_source, account_id=account_id, endpoint=url, commit=False)
            continue
        except Exception as exc:
            db.record_crawl_attempt(
                conn,
                source=scan_source,
                account_id=account_id,
                endpoint=url,
                status="exception",
                error_summary=str(exc),
                commit=False,
            )
            db.mark_crawled(conn, source=scan_source, account_id=account_id, endpoint=url, commit=False)
            continue

        db.record_crawl_attempt(
            conn,
            source=scan_source,
            account_id=account_id,
            endpoint=url,
            status="success",
            error_summary="",
            commit=False,
        )
        db.mark_crawled(conn, source=scan_source, account_id=account_id, endpoint=url, commit=False)

        matches = classify_text(page_text, lexicon_rows)
        observed_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        for signal_code, confidence, matched_keyword in matches:
            seen_delta += 1
            observation = _build_observation(
                account_id=account_id,
                signal_code=signal_code,
                source=scan_source,
                observed_at=observed_at,
                confidence=confidence,
                source_reliability=scan_reliability,
                evidence_url=url,
                evidence_text=page_text,
                payload={"url": url, "matched_keyword": matched_keyword},
            )
            if db.insert_signal_observation(conn, observation, commit=False):
                inserted_delta += 1

        if url == homepage_url:
            for link in discovered_links[:MAX_DISCOVERED_LINKS_PER_ACCOUNT]:
                if link not in scanned:
                    urls_to_scan.append(link)

    _emit_progress(
        "collector=technographics_live status=account_completed "
        f"account_index={account_index} domain={domain} inserted_delta={inserted_delta} seen_delta={seen_delta}"
    )
    return inserted_delta, seen_delta, 1


def _collect_live_technographics_parallel(
    conn,
    settings: Settings,
    lexicon_rows: list[dict[str, str]],
    accounts: list[dict[str, Any]],
    scan_source: str,
    scan_reliability: float,
) -> tuple[int, int]:
    if not accounts:
        return 0, 0

    workers = min(max(1, int(settings.live_workers_per_source)), len(accounts))
    if workers <= 1:
        inserted_total = 0
        seen_total = 0
        processed = 0
        for idx, account in enumerate(accounts, start=1):
            inserted_delta, seen_delta, processed_delta = _collect_live_technographics_account(
                conn=conn,
                settings=settings,
                lexicon_rows=lexicon_rows,
                account=account,
                account_index=idx,
                scan_source=scan_source,
                scan_reliability=scan_reliability,
            )
            inserted_total += inserted_delta
            seen_total += seen_delta
            processed += processed_delta
            if processed and processed % _LIVE_PROGRESS_COMMIT_EVERY == 0:
                conn.commit()
                _emit_progress(
                    f"collector=technographics_live status=checkpoint committed_accounts={processed} "
                    f"inserted_total={inserted_total} seen_total={seen_total}"
                )
        return inserted_total, seen_total

    conn.commit()
    indexed_accounts = list(enumerate(accounts, start=1))
    batches = [indexed_accounts[i::workers] for i in range(workers)]

    def _worker(batch: list[tuple[int, dict[str, Any]]]) -> tuple[int, int]:
        worker_conn = db.get_connection(settings.pg_dsn)
        worker_inserted = 0
        worker_seen = 0
        processed = 0
        try:
            for account_index, account in batch:
                inserted_delta, seen_delta, processed_delta = _collect_live_technographics_account(
                    conn=worker_conn,
                    settings=settings,
                    lexicon_rows=lexicon_rows,
                    account=account,
                    account_index=account_index,
                    scan_source=scan_source,
                    scan_reliability=scan_reliability,
                )
                worker_inserted += inserted_delta
                worker_seen += seen_delta
                processed += processed_delta
                if processed and processed % _LIVE_PROGRESS_COMMIT_EVERY == 0:
                    worker_conn.commit()
            worker_conn.commit()
            return worker_inserted, worker_seen
        finally:
            worker_conn.close()

    inserted_total = 0
    seen_total = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(_worker, batch) for batch in batches if batch]
        for future in as_completed(futures):
            batch_inserted, batch_seen = future.result()
            inserted_total += batch_inserted
            seen_total += batch_seen
    return inserted_total, seen_total


def collect(
    conn,
    settings: Settings,
    lexicon_by_source: dict[str, list[dict[str, str]]],
    source_reliability: dict[str, float],
) -> dict[str, int]:
    inserted = 0
    seen = 0

    lexicon_rows = lexicon_by_source.get("technographics", [])
    source = "technographics_csv"
    reliability = source_reliability.get(source, 0.8)

    if reliability > 0:
        for row in load_csv_rows(settings.raw_dir / "technographics.csv"):
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
                    explicit_confidence = float(row.get("confidence", "0.75") or 0.75)
                except ValueError:
                    explicit_confidence = 0.75
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
        scan_source = "website_scan"
        scan_reliability = source_reliability.get(scan_source, 0.6)
        if scan_reliability <= 0:
            conn.commit()
            return {"inserted": inserted, "seen": seen}
        accounts = db.select_accounts_for_live_crawl(
            conn,
            source=scan_source,
            limit=settings.live_max_accounts,
            include_domains=list(settings.live_target_domains),
        )
        _emit_progress(
            f"collector=technographics_live status=started accounts={len(accounts)} workers={settings.live_workers_per_source}"
        )
        live_inserted, live_seen = _collect_live_technographics_parallel(
            conn=conn,
            settings=settings,
            lexicon_rows=lexicon_rows,
            accounts=accounts,
            scan_source=scan_source,
            scan_reliability=scan_reliability,
        )
        inserted += live_inserted
        seen += live_seen
        _emit_progress(
            "collector=technographics_live status=completed "
            f"accounts_targeted={len(accounts)} inserted_total={inserted} seen_total={seen}"
        )

    conn.commit()
    return {"inserted": inserted, "seen": seen}
