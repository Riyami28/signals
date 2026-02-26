from __future__ import annotations

import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
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


FALLBACK_ROLE_SIGNALS = {
    "finops": "finops_role_open",
    "platform engineer": "platform_role_open",
    "platform": "platform_role_open",
    "devops": "devops_role_open",
    "sre": "devops_role_open",
}
_LIVE_PROGRESS_COMMIT_EVERY = 25
_VERBOSE_PROGRESS = os.getenv("SIGNALS_VERBOSE_PROGRESS", "").strip().lower() in {"1", "true", "yes", "on"}


def _emit_progress(message: str) -> None:
    if _VERBOSE_PROGRESS:
        print(message, flush=True)


@retry(stop=stop_after_attempt(2), wait=wait_fixed(1), reraise=True)
def _request(url: str, settings: Settings) -> requests.Response:
    return http_get(url, settings)


def _today_start_iso() -> str:
    now = datetime.now(timezone.utc)
    return now.strftime("%Y-%m-%dT00:00:00+00:00")


def _extract_job_titles_from_jsonld_payload(payload: Any) -> list[str]:
    titles: list[str] = []

    if isinstance(payload, list):
        for item in payload:
            titles.extend(_extract_job_titles_from_jsonld_payload(item))
        return titles

    if not isinstance(payload, dict):
        return titles

    payload_type = payload.get("@type")
    if isinstance(payload_type, list):
        payload_types = [str(v).lower() for v in payload_type]
    elif payload_type:
        payload_types = [str(payload_type).lower()]
    else:
        payload_types = []

    if "jobposting" in payload_types:
        title = str(payload.get("title", "")).strip()
        if title:
            titles.append(title)

    for key in ("@graph", "graph", "itemListElement", "mainEntity"):
        if key in payload:
            titles.extend(_extract_job_titles_from_jsonld_payload(payload[key]))

    return titles


def _extract_job_titles_from_html(html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    titles: list[str] = []

    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        raw = script.string or script.get_text() or ""
        raw = raw.strip()
        if not raw:
            continue
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            continue
        titles.extend(_extract_job_titles_from_jsonld_payload(payload))

    deduped: list[str] = []
    seen: set[str] = set()
    for title in titles:
        normalized = title.strip().lower()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(title.strip())
    return deduped


def _matches_from_text(
    text: str,
    lexicon_rows: list[dict[str, str]],
) -> list[tuple[str, float, str]]:
    matches = classify_text(text, lexicon_rows)
    if matches:
        return matches

    normalized = (text or "").lower()
    fallback: list[tuple[str, float, str]] = []
    for keyword, signal_code in FALLBACK_ROLE_SIGNALS.items():
        if keyword in normalized:
            fallback.append((signal_code, 0.65, keyword))
    return fallback


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


def _insert_matches(
    conn,
    account_id: str,
    source: str,
    reliability: float,
    observed_at: str,
    evidence_url: str,
    evidence_text: str,
    payload: dict[str, Any],
    matches: list[tuple[str, float, str]],
) -> tuple[int, int]:
    inserted = 0
    seen = 0
    for signal_code, confidence, matched_keyword in matches:
        seen += 1
        observation = _build_observation(
            account_id=account_id,
            signal_code=signal_code,
            source=source,
            observed_at=observed_at,
            confidence=confidence,
            source_reliability=reliability,
            evidence_url=evidence_url,
            evidence_text=evidence_text,
            payload={"payload": payload, "matched_keyword": matched_keyword},
        )
        if db.insert_signal_observation(conn, observation, commit=False):
            inserted += 1
    return inserted, seen


def _derive_slug_candidates(domain: str) -> list[str]:
    base = domain.split(".", 1)[0]
    candidates = [base, base.replace("-", ""), base.replace("_", "")]
    result: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        value = candidate.strip().lower()
        if len(value) < 2 or value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result[:3]


def _collect_greenhouse(
    conn,
    account_id: str,
    domain: str,
    row: dict[str, str],
    settings: Settings,
    lexicon_rows: list[dict[str, str]],
    source_reliability: dict[str, float],
) -> tuple[int, int]:
    source = "greenhouse_api"
    reliability = source_reliability.get(source, 0.8)
    if reliability <= 0:
        return 0, 0

    candidates: list[str] = []
    explicit = row.get("greenhouse_board", "").strip()
    if explicit:
        candidates.append(explicit)
    elif settings.auto_discover_job_handles:
        candidates.extend(_derive_slug_candidates(domain))

    inserted_total = 0
    seen_total = 0
    tried: set[str] = set()

    for candidate in candidates:
        if candidate in tried:
            continue
        tried.add(candidate)
        url = f"https://boards-api.greenhouse.io/v1/boards/{candidate}/jobs?content=true"
        if db.was_crawled_today(conn, source=source, account_id=account_id, endpoint=url):
            db.record_crawl_attempt(
                conn,
                source=source,
                account_id=account_id,
                endpoint=url,
                status="skipped",
                error_summary="checkpoint_recent",
                commit=False,
            )
            continue
        try:
            response = _request(url, settings)
            if response.status_code >= 400:
                db.record_crawl_attempt(
                    conn,
                    source=source,
                    account_id=account_id,
                    endpoint=url,
                    status="http_error",
                    error_summary=f"status_code={response.status_code}",
                    commit=False,
                )
                db.mark_crawled(conn, source=source, account_id=account_id, endpoint=url, commit=False)
                continue
            payload = response.json()
        except Exception as exc:
            db.record_crawl_attempt(
                conn,
                source=source,
                account_id=account_id,
                endpoint=url,
                status="exception",
                error_summary=str(exc),
                commit=False,
            )
            db.mark_crawled(conn, source=source, account_id=account_id, endpoint=url, commit=False)
            continue
        db.record_crawl_attempt(
            conn,
            source=source,
            account_id=account_id,
            endpoint=url,
            status="success",
            error_summary="",
            commit=False,
        )
        db.mark_crawled(conn, source=source, account_id=account_id, endpoint=url, commit=False)

        jobs = payload.get("jobs", []) if isinstance(payload, dict) else []
        if not isinstance(jobs, list):
            continue

        for job in jobs[: settings.live_max_jobs_per_source]:
            title = str(job.get("title", ""))
            content = str(job.get("content", ""))
            location = ""
            if isinstance(job.get("location"), dict):
                location = str(job["location"].get("name", ""))
            text = "\n".join([title, location, content]).strip()
            matches = _matches_from_text(text, lexicon_rows)
            if not matches:
                continue

            observed_at = str(job.get("updated_at") or job.get("created_at") or _today_start_iso())
            evidence_url = str(job.get("absolute_url", ""))
            inserted, seen = _insert_matches(
                conn=conn,
                account_id=account_id,
                source=source,
                reliability=reliability,
                observed_at=observed_at,
                evidence_url=evidence_url,
                evidence_text=text,
                payload={"job": job, "board": candidate},
                matches=matches,
            )
            inserted_total += inserted
            seen_total += seen

        if explicit:
            break

    return inserted_total, seen_total


def _collect_lever(
    conn,
    account_id: str,
    domain: str,
    row: dict[str, str],
    settings: Settings,
    lexicon_rows: list[dict[str, str]],
    source_reliability: dict[str, float],
) -> tuple[int, int]:
    source = "lever_api"
    reliability = source_reliability.get(source, 0.8)
    if reliability <= 0:
        return 0, 0

    candidates: list[str] = []
    explicit = row.get("lever_company", "").strip()
    if explicit:
        candidates.append(explicit)
    elif settings.auto_discover_job_handles:
        candidates.extend(_derive_slug_candidates(domain))

    inserted_total = 0
    seen_total = 0
    tried: set[str] = set()

    for candidate in candidates:
        if candidate in tried:
            continue
        tried.add(candidate)

        url = f"https://api.lever.co/v0/postings/{candidate}?mode=json"
        if db.was_crawled_today(conn, source=source, account_id=account_id, endpoint=url):
            db.record_crawl_attempt(
                conn,
                source=source,
                account_id=account_id,
                endpoint=url,
                status="skipped",
                error_summary="checkpoint_recent",
                commit=False,
            )
            continue
        try:
            response = _request(url, settings)
            if response.status_code >= 400:
                db.record_crawl_attempt(
                    conn,
                    source=source,
                    account_id=account_id,
                    endpoint=url,
                    status="http_error",
                    error_summary=f"status_code={response.status_code}",
                    commit=False,
                )
                db.mark_crawled(conn, source=source, account_id=account_id, endpoint=url, commit=False)
                continue
            postings = response.json()
        except Exception as exc:
            db.record_crawl_attempt(
                conn,
                source=source,
                account_id=account_id,
                endpoint=url,
                status="exception",
                error_summary=str(exc),
                commit=False,
            )
            db.mark_crawled(conn, source=source, account_id=account_id, endpoint=url, commit=False)
            continue
        db.record_crawl_attempt(
            conn,
            source=source,
            account_id=account_id,
            endpoint=url,
            status="success",
            error_summary="",
            commit=False,
        )
        db.mark_crawled(conn, source=source, account_id=account_id, endpoint=url, commit=False)

        if not isinstance(postings, list):
            continue

        for posting in postings[: settings.live_max_jobs_per_source]:
            title = str(posting.get("text", ""))
            description = str(posting.get("descriptionPlain", "") or posting.get("description", ""))
            categories = posting.get("categories", {}) if isinstance(posting.get("categories"), dict) else {}
            location = str(categories.get("location", ""))
            team = str(categories.get("team", ""))
            text = "\n".join([title, team, location, description]).strip()

            matches = _matches_from_text(text, lexicon_rows)
            if not matches:
                continue

            created_at = posting.get("createdAt")
            if isinstance(created_at, (int, float)):
                observed_at = datetime.fromtimestamp(float(created_at) / 1000.0, tz=timezone.utc).isoformat()
            else:
                observed_at = _today_start_iso()

            evidence_url = str(posting.get("hostedUrl", ""))
            inserted, seen = _insert_matches(
                conn=conn,
                account_id=account_id,
                source=source,
                reliability=reliability,
                observed_at=observed_at,
                evidence_url=evidence_url,
                evidence_text=text,
                payload={"posting": posting, "company": candidate},
                matches=matches,
            )
            inserted_total += inserted
            seen_total += seen

        if explicit:
            break

    return inserted_total, seen_total


def _collect_careers_pages(
    conn,
    account_id: str,
    domain: str,
    row: dict[str, str],
    settings: Settings,
    lexicon_rows: list[dict[str, str]],
    source_reliability: dict[str, float],
) -> tuple[int, int]:
    source = "careers_live"
    reliability = source_reliability.get(source, 0.65)
    if reliability <= 0:
        return 0, 0

    candidates: list[str] = []
    if row.get("careers_url", "").strip():
        candidates.append(row["careers_url"].strip())
    website_url = row.get("website_url", "").strip()
    homepage_url = website_url or f"https://{domain}"

    # Discover careers links from homepage nav/footer for enterprise sites that
    # don't expose /careers or /jobs directly.
    try:
        homepage_response = _request(homepage_url, settings)
        if homepage_response.status_code >= 400:
            db.record_crawl_attempt(
                conn,
                source=source,
                account_id=account_id,
                endpoint=homepage_url,
                status="http_error",
                error_summary=f"status_code={homepage_response.status_code}",
                commit=False,
            )
        if homepage_response.status_code < 400:
            db.record_crawl_attempt(
                conn,
                source=source,
                account_id=account_id,
                endpoint=homepage_url,
                status="success",
                error_summary="",
                commit=False,
            )
            soup = BeautifulSoup(homepage_response.text, "html.parser")
            for anchor in soup.find_all("a", href=True):
                href = str(anchor.get("href", "")).strip()
                label = anchor.get_text(" ", strip=True).lower()
                href_lower = href.lower()
                if any(token in href_lower or token in label for token in ("career", "job", "hiring", "work-with-us")):
                    candidates.append(urljoin(homepage_url, href))
    except Exception as exc:
        db.record_crawl_attempt(
            conn,
            source=source,
            account_id=account_id,
            endpoint=homepage_url,
            status="exception",
            error_summary=str(exc),
            commit=False,
        )

    if website_url:
        candidates.extend(
            [
                website_url.rstrip("/") + "/careers",
                website_url.rstrip("/") + "/jobs",
            ]
        )
    candidates.extend([f"https://{domain}/careers", f"https://{domain}/jobs"])

    inserted_total = 0
    seen_total = 0
    tried: set[str] = set()

    for url in candidates:
        normalized_url = url.strip().rstrip("/")
        if not normalized_url or normalized_url in tried:
            continue
        tried.add(normalized_url)

        if db.was_crawled_today(conn, source=source, account_id=account_id, endpoint=normalized_url):
            db.record_crawl_attempt(
                conn,
                source=source,
                account_id=account_id,
                endpoint=normalized_url,
                status="skipped",
                error_summary="checkpoint_recent",
                commit=False,
            )
            continue

        try:
            response = _request(normalized_url, settings)
            if response.status_code >= 400:
                db.record_crawl_attempt(
                    conn,
                    source=source,
                    account_id=account_id,
                    endpoint=normalized_url,
                    status="http_error",
                    error_summary=f"status_code={response.status_code}",
                    commit=False,
                )
                db.mark_crawled(conn, source=source, account_id=account_id, endpoint=normalized_url, commit=False)
                continue
            html = response.text
        except Exception as exc:
            db.record_crawl_attempt(
                conn,
                source=source,
                account_id=account_id,
                endpoint=normalized_url,
                status="exception",
                error_summary=str(exc),
                commit=False,
            )
            db.mark_crawled(conn, source=source, account_id=account_id, endpoint=normalized_url, commit=False)
            continue
        db.record_crawl_attempt(
            conn,
            source=source,
            account_id=account_id,
            endpoint=normalized_url,
            status="success",
            error_summary="",
            commit=False,
        )
        db.mark_crawled(conn, source=source, account_id=account_id, endpoint=normalized_url, commit=False)

        soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text(" ", strip=True)
        jsonld_titles = _extract_job_titles_from_html(html)

        candidate_text = "\n".join(jsonld_titles + [text[:2500]]).strip()
        matches = _matches_from_text(candidate_text, lexicon_rows)
        if not matches:
            continue

        observed_at = _today_start_iso()
        inserted, seen = _insert_matches(
            conn=conn,
            account_id=account_id,
            source=source,
            reliability=reliability,
            observed_at=observed_at,
            evidence_url=normalized_url,
            evidence_text=candidate_text,
            payload={"url": normalized_url, "jsonld_titles": jsonld_titles[:20]},
            matches=matches,
        )
        inserted_total += inserted
        seen_total += seen

    return inserted_total, seen_total


def _collect_ashby(
    conn,
    account_id: str,
    domain: str,
    settings: Settings,
    lexicon_rows: list[dict[str, str]],
    source_reliability: dict[str, float],
) -> tuple[int, int]:
    source = "ashby_api"
    reliability = source_reliability.get(source, 0.25)
    if reliability <= 0:
        return 0, 0

    slug_candidates = _derive_slug_candidates(domain)
    inserted_total = 0
    seen_total = 0

    for slug in slug_candidates:
        url = f"https://jobs.ashbyhq.com/{slug}"
        if db.was_crawled_today(conn, source=source, account_id=account_id, endpoint=url):
            continue
        try:
            response = _request(url, settings)
            db.mark_crawled(conn, source=source, account_id=account_id, endpoint=url, commit=False)
            if response.status_code >= 400:
                continue
            titles = _extract_job_titles_from_html(response.text)
            if not titles:
                continue
        except Exception:
            logger.warning("ashby fetch failed for slug=%s", slug, exc_info=True)
            db.mark_crawled(conn, source=source, account_id=account_id, endpoint=url, commit=False)
            continue

        observed_at = _today_start_iso()
        for title in titles[: settings.live_max_jobs_per_source]:
            matches = _matches_from_text(title, lexicon_rows)
            if not matches:
                continue
            inserted, seen = _insert_matches(
                conn=conn,
                account_id=account_id,
                source=source,
                reliability=reliability,
                observed_at=observed_at,
                evidence_url=url,
                evidence_text=title,
                payload={"slug": slug, "title": title},
                matches=matches,
            )
            inserted_total += inserted
            seen_total += seen
        break  # Found valid page, stop trying slugs.

    return inserted_total, seen_total


def _collect_workday(
    conn,
    account_id: str,
    domain: str,
    settings: Settings,
    lexicon_rows: list[dict[str, str]],
    source_reliability: dict[str, float],
) -> tuple[int, int]:
    source = "workday_api"
    reliability = source_reliability.get(source, 0.25)
    if reliability <= 0:
        return 0, 0

    slug_candidates = _derive_slug_candidates(domain)
    board_candidates = [f"{s}_External_Career_Site" for s in slug_candidates]
    inserted_total = 0
    seen_total = 0

    for tenant in slug_candidates:
        for board in board_candidates:
            url = f"https://{tenant}.wd1.myworkdayjobs.com/wday/cxs/{board}/jobs"
            if db.was_crawled_today(conn, source=source, account_id=account_id, endpoint=url):
                continue
            try:
                response = _request(url, settings)
                db.mark_crawled(conn, source=source, account_id=account_id, endpoint=url, commit=False)
                if response.status_code >= 400:
                    continue
                data = response.json()
                postings = data.get("jobPostings") or data.get("jobs") or []
                titles = [p.get("title", "") for p in postings if p.get("title")]
                if not titles:
                    continue
            except Exception:
                logger.warning("workday fetch failed for tenant=%s board=%s", tenant, board, exc_info=True)
                db.mark_crawled(conn, source=source, account_id=account_id, endpoint=url, commit=False)
                continue

            observed_at = _today_start_iso()
            for title in titles[: settings.live_max_jobs_per_source]:
                matches = _matches_from_text(title, lexicon_rows)
                if not matches:
                    continue
                inserted, seen = _insert_matches(
                    conn=conn,
                    account_id=account_id,
                    source=source,
                    reliability=reliability,
                    observed_at=observed_at,
                    evidence_url=url,
                    evidence_text=title,
                    payload={"tenant": tenant, "board": board, "title": title},
                    matches=matches,
                )
                inserted_total += inserted
                seen_total += seen
            return inserted_total, seen_total  # Found valid endpoint, stop.

    return inserted_total, seen_total


def _process_live_account(
    conn,
    settings: Settings,
    lexicon_rows: list[dict[str, str]],
    source_reliability: dict[str, float],
    handles: dict[str, dict[str, str]],
    account: dict[str, Any],
    account_index: int,
) -> tuple[int, int, int]:
    account_id = str(account["account_id"])
    domain = str(account["domain"])
    if domain.endswith(".example"):
        return 0, 0, 0

    _emit_progress(f"collector=jobs_live status=account_started account_index={account_index} domain={domain}")
    row = handles.get(domain, {"domain": domain, "company_name": str(account["company_name"] or domain)})

    gh_inserted, gh_seen = _collect_greenhouse(
        conn,
        account_id,
        domain,
        row,
        settings,
        lexicon_rows,
        source_reliability,
    )
    lever_inserted, lever_seen = _collect_lever(
        conn,
        account_id,
        domain,
        row,
        settings,
        lexicon_rows,
        source_reliability,
    )

    ashby_inserted, ashby_seen = (0, 0)
    workday_inserted, workday_seen = (0, 0)
    if settings.auto_discover_job_handles:
        ashby_inserted, ashby_seen = _collect_ashby(
            conn,
            account_id,
            domain,
            settings,
            lexicon_rows,
            source_reliability,
        )
        workday_inserted, workday_seen = _collect_workday(
            conn,
            account_id,
            domain,
            settings,
            lexicon_rows,
            source_reliability,
        )

    careers_inserted, careers_seen = _collect_careers_pages(
        conn,
        account_id,
        domain,
        row,
        settings,
        lexicon_rows,
        source_reliability,
    )

    inserted_delta = gh_inserted + lever_inserted + ashby_inserted + workday_inserted + careers_inserted
    seen_delta = gh_seen + lever_seen + ashby_seen + workday_seen + careers_seen
    _emit_progress(
        "collector=jobs_live status=account_completed "
        f"account_index={account_index} domain={domain} inserted_delta={inserted_delta} seen_delta={seen_delta}"
    )
    return inserted_delta, seen_delta, 1


def _collect_live_jobs_parallel(
    conn,
    settings: Settings,
    lexicon_rows: list[dict[str, str]],
    source_reliability: dict[str, float],
    accounts: list[dict[str, Any]],
    handles: dict[str, dict[str, str]],
    db_pool=None,
) -> tuple[int, int]:
    if not accounts:
        return 0, 0

    workers = min(max(1, int(settings.live_workers_per_source)), len(accounts))
    if workers <= 1:
        inserted_total = 0
        seen_total = 0
        processed_accounts = 0
        for idx, account in enumerate(accounts, start=1):
            inserted_delta, seen_delta, processed = _process_live_account(
                conn=conn,
                settings=settings,
                lexicon_rows=lexicon_rows,
                source_reliability=source_reliability,
                handles=handles,
                account=account,
                account_index=idx,
            )
            inserted_total += inserted_delta
            seen_total += seen_delta
            processed_accounts += processed
            if processed_accounts and processed_accounts % _LIVE_PROGRESS_COMMIT_EVERY == 0:
                conn.commit()
                _emit_progress(
                    f"collector=jobs_live status=checkpoint committed_accounts={processed_accounts} "
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
                inserted_delta, seen_delta, processed_delta = _process_live_account(
                    conn=worker_conn,
                    settings=settings,
                    lexicon_rows=lexicon_rows,
                    source_reliability=source_reliability,
                    handles=handles,
                    account=account,
                    account_index=account_index,
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
                logger.error("collector_worker_failed source=jobs error=%s", e, exc_info=True)
                batch_inserted, batch_seen = 0, 0
                failed_workers += 1
            inserted_total += batch_inserted
            seen_total += batch_seen
    logger.info(
        "collection_complete source=jobs inserted=%d seen=%d failed_workers=%d",
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

    lexicon_rows = lexicon_by_source.get("jobs", [])
    jobs_path = settings.raw_dir / "jobs.csv"
    rows = load_csv_rows(jobs_path)
    jobs_source = "jobs_csv"
    jobs_reliability = source_reliability.get(jobs_source, 0.75)

    if jobs_reliability > 0:
        for row in rows:
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
            description = row.get("description", "")
            text = f"{title}\n{description}".strip()

            explicit_signal = row.get("signal_code", "")
            if explicit_signal:
                try:
                    explicit_conf = float(row.get("confidence", "0.7") or 0.7)
                except ValueError:
                    explicit_conf = 0.7
                matches = [(explicit_signal, explicit_conf, "explicit")]
            else:
                matches = _matches_from_text(text, lexicon_rows)

            observed_at = row.get("observed_at", "") or utc_now_iso()
            local_inserted, local_seen = _insert_matches(
                conn=conn,
                account_id=account_id,
                source=jobs_source,
                reliability=jobs_reliability,
                observed_at=observed_at,
                evidence_url=row.get("url", ""),
                evidence_text=text,
                payload={"row": row},
                matches=matches,
            )
            inserted += local_inserted
            seen += local_seen

    if settings.enable_live_crawl:
        accounts = db.select_accounts_for_live_crawl(
            conn,
            source="careers_live",
            limit=settings.live_max_accounts,
            include_domains=list(settings.live_target_domains),
        )
        _emit_progress(
            f"collector=jobs_live status=started accounts={len(accounts)} workers={settings.live_workers_per_source}"
        )
        handles = load_account_source_handles(settings.account_source_handles_path)
        live_inserted, live_seen = _collect_live_jobs_parallel(
            conn=conn,
            settings=settings,
            lexicon_rows=lexicon_rows,
            source_reliability=source_reliability,
            accounts=accounts,
            handles=handles,
            db_pool=db_pool,
        )
        inserted += live_inserted
        seen += live_seen
        _emit_progress(
            "collector=jobs_live status=completed "
            f"accounts_targeted={len(accounts)} inserted_total={inserted} seen_total={seen}"
        )

    conn.commit()
    return {"inserted": inserted, "seen": seen}
