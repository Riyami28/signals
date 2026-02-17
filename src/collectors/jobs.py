from __future__ import annotations

from datetime import datetime, timezone
import json
from typing import Any
from urllib.parse import urljoin

from bs4 import BeautifulSoup
import requests
from tenacity import retry, stop_after_attempt, wait_fixed

from src import db
from src.models import SignalObservation
from src.settings import Settings
from src.utils import classify_text, load_account_source_handles, load_csv_rows, stable_hash, utc_now_iso


FALLBACK_ROLE_SIGNALS = {
    "finops": "finops_role_open",
    "platform engineer": "platform_role_open",
    "platform": "platform_role_open",
    "devops": "devops_role_open",
    "sre": "devops_role_open",
}


@retry(stop=stop_after_attempt(2), wait=wait_fixed(1), reraise=True)
def _request(url: str, settings: Settings) -> requests.Response:
    return requests.get(
        url,
        timeout=settings.http_timeout_seconds,
        headers={"User-Agent": settings.http_user_agent},
    )


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
        if db.insert_signal_observation(conn, observation):
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
                )
                db.mark_crawled(conn, source=source, account_id=account_id, endpoint=url)
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
            )
            db.mark_crawled(conn, source=source, account_id=account_id, endpoint=url)
            continue
        db.record_crawl_attempt(
            conn,
            source=source,
            account_id=account_id,
            endpoint=url,
            status="success",
            error_summary="",
        )
        db.mark_crawled(conn, source=source, account_id=account_id, endpoint=url)

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
                )
                db.mark_crawled(conn, source=source, account_id=account_id, endpoint=url)
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
            )
            db.mark_crawled(conn, source=source, account_id=account_id, endpoint=url)
            continue
        db.record_crawl_attempt(
            conn,
            source=source,
            account_id=account_id,
            endpoint=url,
            status="success",
            error_summary="",
        )
        db.mark_crawled(conn, source=source, account_id=account_id, endpoint=url)

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
            )
        if homepage_response.status_code < 400:
            db.record_crawl_attempt(
                conn,
                source=source,
                account_id=account_id,
                endpoint=homepage_url,
                status="success",
                error_summary="",
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
        )

    if website_url:
        candidates.extend([
            website_url.rstrip("/") + "/careers",
            website_url.rstrip("/") + "/jobs",
        ])
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
                )
                db.mark_crawled(conn, source=source, account_id=account_id, endpoint=normalized_url)
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
            )
            db.mark_crawled(conn, source=source, account_id=account_id, endpoint=normalized_url)
            continue
        db.record_crawl_attempt(
            conn,
            source=source,
            account_id=account_id,
            endpoint=normalized_url,
            status="success",
            error_summary="",
        )
        db.mark_crawled(conn, source=source, account_id=account_id, endpoint=normalized_url)

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


def collect(
    conn,
    settings: Settings,
    lexicon_by_source: dict[str, list[dict[str, str]]],
    source_reliability: dict[str, float],
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
            account_id = db.upsert_account(conn, company_name=company_name, domain=domain, source_type="discovered")

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
        accounts = conn.execute(
            "SELECT account_id, domain, company_name FROM accounts ORDER BY created_at LIMIT ?",
            (settings.live_max_accounts,),
        ).fetchall()
        handles = load_account_source_handles(settings.account_source_handles_path)

        for account in accounts:
            account_id = str(account["account_id"])
            domain = str(account["domain"])
            if domain.endswith(".example"):
                continue
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
            inserted += gh_inserted
            seen += gh_seen

            lever_inserted, lever_seen = _collect_lever(
                conn,
                account_id,
                domain,
                row,
                settings,
                lexicon_rows,
                source_reliability,
            )
            inserted += lever_inserted
            seen += lever_seen

            careers_inserted, careers_seen = _collect_careers_pages(
                conn,
                account_id,
                domain,
                row,
                settings,
                lexicon_rows,
                source_reliability,
            )
            inserted += careers_inserted
            seen += careers_seen

    return {"inserted": inserted, "seen": seen}
