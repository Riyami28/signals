from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
import requests
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
            account_id = db.upsert_account(conn, company_name=company_name, domain=domain, source_type="discovered")

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
                if db.insert_signal_observation(conn, observation):
                    inserted += 1

    if settings.enable_live_crawl:
        scan_source = "website_scan"
        scan_reliability = source_reliability.get(scan_source, 0.6)
        if scan_reliability <= 0:
            return {"inserted": inserted, "seen": seen}
        accounts = conn.execute(
            "SELECT account_id, domain FROM accounts ORDER BY created_at LIMIT ?",
            (settings.live_max_accounts,),
        ).fetchall()

        for account in accounts:
            account_id = str(account["account_id"])
            domain = str(account["domain"])
            if domain.endswith(".example"):
                continue

            homepage_url = f"https://{domain}"
            urls_to_scan = [homepage_url]
            scanned: set[str] = set()

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
                    )
                    db.mark_crawled(conn, source=scan_source, account_id=account_id, endpoint=url)
                    continue
                except Exception as exc:
                    db.record_crawl_attempt(
                        conn,
                        source=scan_source,
                        account_id=account_id,
                        endpoint=url,
                        status="exception",
                        error_summary=str(exc),
                    )
                    db.mark_crawled(conn, source=scan_source, account_id=account_id, endpoint=url)
                    continue
                db.record_crawl_attempt(
                    conn,
                    source=scan_source,
                    account_id=account_id,
                    endpoint=url,
                    status="success",
                    error_summary="",
                )
                db.mark_crawled(conn, source=scan_source, account_id=account_id, endpoint=url)

                matches = classify_text(page_text, lexicon_rows)
                observed_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
                for signal_code, confidence, matched_keyword in matches:
                    seen += 1
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
                    if db.insert_signal_observation(conn, observation):
                        inserted += 1

                if url == homepage_url:
                    for link in discovered_links[:MAX_DISCOVERED_LINKS_PER_ACCOUNT]:
                        if link not in scanned:
                            urls_to_scan.append(link)

    return {"inserted": inserted, "seen": seen}
