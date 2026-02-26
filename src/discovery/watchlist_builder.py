from __future__ import annotations

import logging
import math
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date
from typing import Any
from urllib.parse import urlparse

import requests
from tenacity import retry, stop_after_attempt, wait_fixed

from src.discovery.config import is_placeholder_domain
from src.settings import Settings
from src.utils import load_csv_rows, normalize_domain, write_csv_rows

logger = logging.getLogger(__name__)

try:
    import tldextract  # type: ignore

    _OFFLINE_EXTRACTOR = tldextract.TLDExtract(cache_dir=None, suffix_list_urls=())
except Exception:  # pragma: no cover - optional dependency fallback.
    _OFFLINE_EXTRACTOR = None


@dataclass(frozen=True)
class CountrySpec:
    name: str
    qid: str
    region_group: str
    priority: float


TARGET_COUNTRIES: list[CountrySpec] = [
    CountrySpec("India", "Q668", "india", 1.35),
    CountrySpec("United States", "Q30", "us", 1.35),
    CountrySpec("United Kingdom", "Q145", "uk", 1.25),
    CountrySpec("Australia", "Q408", "australia", 1.2),
    CountrySpec("United Arab Emirates", "Q878", "gulf", 1.2),
    CountrySpec("Saudi Arabia", "Q851", "gulf", 1.2),
    CountrySpec("Qatar", "Q846", "gulf", 1.2),
    CountrySpec("Kuwait", "Q817", "gulf", 1.2),
    CountrySpec("Oman", "Q842", "gulf", 1.2),
    CountrySpec("Bahrain", "Q398", "gulf", 1.2),
    CountrySpec("Canada", "Q16", "americas", 1.05),
    CountrySpec("Singapore", "Q334", "apac", 1.05),
    CountrySpec("Malaysia", "Q833", "apac", 1.05),
    CountrySpec("Indonesia", "Q252", "apac", 1.05),
    CountrySpec("France", "Q142", "europe", 1.05),
    CountrySpec("Germany", "Q183", "europe", 1.05),
    CountrySpec("Spain", "Q29", "europe", 1.05),
    CountrySpec("Italy", "Q38", "europe", 1.05),
    CountrySpec("Netherlands", "Q55", "europe", 1.05),
    CountrySpec("Belgium", "Q31", "europe", 1.05),
    CountrySpec("Switzerland", "Q39", "europe", 1.05),
    CountrySpec("Ireland", "Q27", "europe", 1.05),
    CountrySpec("Japan", "Q17", "apac", 1.05),
    CountrySpec("South Korea", "Q884", "apac", 1.05),
    CountrySpec("China", "Q148", "apac", 1.05),
    CountrySpec("Brazil", "Q155", "americas", 1.05),
    CountrySpec("Mexico", "Q96", "americas", 1.05),
    CountrySpec("South Africa", "Q258", "africa", 1.05),
    CountrySpec("New Zealand", "Q664", "apac", 1.05),
    CountrySpec("Thailand", "Q869", "apac", 1.05),
    CountrySpec("Philippines", "Q928", "apac", 1.05),
    CountrySpec("Vietnam", "Q881", "apac", 1.05),
    CountrySpec("Turkey", "Q43", "europe", 1.05),
    CountrySpec("Poland", "Q36", "europe", 1.05),
]

CORE_REGIONS = {"india", "us", "uk", "australia", "gulf"}

POSITIVE_INDUSTRY_PATTERN = re.compile(
    r"food|beverage|consumer goods|fast-moving consumer goods|fmcg|personal care|household|"
    r"cosmetic|beauty|hygiene|detergent|home care|packaged|tobacco|snack|spirits|brewery|"
    r"dairy|confectionery|distill|toiletries|home products",
    re.IGNORECASE,
)
NEGATIVE_INDUSTRY_PATTERN = re.compile(
    r"restaurant|fast food|food service|cafe|hotel|hospitality|retail|supermarket|department store|"
    r"e-commerce|online food ordering|food delivery|bank|insurance|software|media|telecom|"
    r"airline|automotive|pharmaceutical|real estate|construction|logistics|transport|medical|"
    r"appliance|consumer electronics|wholesale trade",
    re.IGNORECASE,
)
NEGATIVE_COMPANY_PATTERN = re.compile(
    r"mcdonald|burger|pizza|cafe|tavern|restaurant|hotel|resort|airline|"
    r"kfc|subway|dunkin|wendy|domino|chipotle|taco bell|cloudkitchens|mrbeast burger",
    re.IGNORECASE,
)

PLACEHOLDER_NAME_PATTERN = re.compile(r"^Q\d+$")
EXCLUDED_HOST_SUFFIXES = {
    "linkedin.com",
    "x.com",
    "twitter.com",
    "facebook.com",
    "instagram.com",
    "youtube.com",
    "tiktok.com",
    "wikipedia.org",
    "wikidata.org",
    "web.archive.org",
    "medium.com",
    "bloomberg.com",
    "reuters.com",
    "crunchbase.com",
    "news.google.com",
}

WIKIDATA_ENDPOINT = "https://query.wikidata.org/sparql"
WIKIDATA_USER_AGENT = "zop-signals-watchlist/1.0"


def _extract_registered_domain(url: str) -> str:
    raw = (url or "").strip()
    if not raw:
        return ""

    parsed = urlparse(raw)
    host = normalize_domain(parsed.netloc or parsed.path)
    if not host:
        return ""

    if _OFFLINE_EXTRACTOR is not None:
        try:
            extracted = _OFFLINE_EXTRACTOR(host)
            if extracted.domain and extracted.suffix:
                host = f"{extracted.domain}.{extracted.suffix}".lower()
        except Exception:
            logger.debug("tldextract failed for host=%s", host, exc_info=True)

    if is_placeholder_domain(host):
        return ""
    for suffix in EXCLUDED_HOST_SUFFIXES:
        if host == suffix or host.endswith(f".{suffix}"):
            return ""
    return host


def _industry_matches(industry_label: str) -> bool:
    text = (industry_label or "").strip()
    if not text:
        return False
    if not POSITIVE_INDUSTRY_PATTERN.search(text):
        return False
    if NEGATIVE_INDUSTRY_PATTERN.search(text):
        return False
    return True


def _company_matches(company_name: str) -> bool:
    text = (company_name or "").strip()
    if not text:
        return False
    return not bool(NEGATIVE_COMPANY_PATTERN.search(text))


def _parse_float(value: str | None) -> float:
    raw = (value or "").strip()
    if not raw:
        return 0.0
    try:
        return float(raw)
    except ValueError:
        return 0.0


def _parse_int(value: str | None) -> int:
    raw = (value or "").strip()
    if not raw:
        return 0
    try:
        return int(float(raw))
    except ValueError:
        return 0


def _industry_bonus(industry_label: str) -> float:
    text = (industry_label or "").lower()
    if "fast-moving consumer goods" in text or "fmcg" in text:
        return 6.0
    if "consumer goods" in text:
        return 5.0
    if any(token in text for token in ("personal care", "household", "cosmetic", "beauty", "hygiene", "detergent")):
        return 4.0
    if any(
        token in text
        for token in (
            "food",
            "beverage",
            "dairy",
            "confectionery",
            "tobacco",
            "spirits",
            "brewery",
            "distill",
            "home care",
            "packaged",
            "toiletries",
        )
    ):
        return 3.0
    return 1.0


def _rank_candidate(
    country: CountrySpec, industry_label: str, sitelinks: int, revenue_usd: float, employees: int
) -> float:
    score = 15.0 * float(country.priority)
    if country.region_group in CORE_REGIONS:
        score += 3.0
    score += _industry_bonus(industry_label)
    score += 0.08 * float(min(max(sitelinks, 0), 160))

    if revenue_usd > 0:
        score += 1.8 * min(math.log10(revenue_usd + 1.0), 13.0)
    if employees > 0:
        score += 1.4 * min(math.log10(float(employees) + 1.0), 7.0)

    return round(score, 4)


def _build_country_query(country_qid: str) -> str:
    return f"""
SELECT DISTINCT ?company ?companyLabel ?countryLabel ?website ?industryLabel ?sitelinks ?revenue ?employees WHERE {{
  ?company wdt:P31/wdt:P279* wd:Q4830453 .
  {{ ?company wdt:P17 wd:{country_qid} . }}
  UNION
  {{ ?company wdt:P159 ?hq . ?hq wdt:P17 wd:{country_qid} . }}

  ?company wdt:P452 ?industry .
  ?industry rdfs:label ?industryLabel .
  FILTER(LANG(?industryLabel) = 'en')

  FILTER(REGEX(LCASE(?industryLabel),
    'food|beverage|consumer goods|fast-moving consumer goods|fmcg|personal care|household|cosmetic|beauty|hygiene|detergent|home care|packaged|tobacco|snack|spirits|brewery|dairy|confectionery|distill|toiletries|home products'
  ))
  FILTER(!REGEX(LCASE(?industryLabel),
    'restaurant|fast food|food service|cafe|hotel|hospitality|retail|supermarket|department store|e-commerce|online food ordering|food delivery|bank|insurance|software|media|telecom|airline|automotive|pharmaceutical|real estate|construction|logistics|transport|medical'
  ))

  ?company wdt:P856 ?website .
  ?company wikibase:sitelinks ?sitelinks .
  OPTIONAL {{ ?company wdt:P2139 ?revenue . }}
  OPTIONAL {{ ?company wdt:P1128 ?employees . }}

  SERVICE wikibase:label {{ bd:serviceParam wikibase:language 'en'. }}
}}
LIMIT 5000
"""


@retry(stop=stop_after_attempt(3), wait=wait_fixed(2), reraise=True)
def _query_wikidata(query: str, timeout_seconds: int = 180) -> dict[str, Any]:
    response = requests.get(
        WIKIDATA_ENDPOINT,
        params={"query": query, "format": "json"},
        headers={"User-Agent": WIKIDATA_USER_AGENT},
        timeout=max(15, int(timeout_seconds)),
    )
    response.raise_for_status()
    return response.json()


def _fetch_country_rows(
    country: CountrySpec,
    refreshed_on: str,
    timeout_seconds: int,
) -> tuple[str, list[dict[str, Any]], str]:
    try:
        payload = _query_wikidata(_build_country_query(country.qid), timeout_seconds=timeout_seconds)
        bindings = payload.get("results", {}).get("bindings", [])
        country_rows = _bindings_to_rows(country, bindings=bindings, refreshed_on=refreshed_on)
        return country.name, country_rows, ""
    except Exception as exc:
        return country.name, [], str(exc)


def _bindings_to_rows(country: CountrySpec, bindings: list[dict[str, Any]], refreshed_on: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for binding in bindings:
        company_name = str(binding.get("companyLabel", {}).get("value", "")).strip()
        if not company_name or PLACEHOLDER_NAME_PATTERN.match(company_name):
            continue
        if not _company_matches(company_name):
            continue

        website_url = str(binding.get("website", {}).get("value", "")).strip()
        domain = _extract_registered_domain(website_url)
        if not domain:
            continue

        industry_label = str(binding.get("industryLabel", {}).get("value", "")).strip()
        if not _industry_matches(industry_label):
            continue

        sitelinks = _parse_int(str(binding.get("sitelinks", {}).get("value", "0")))
        revenue_usd = _parse_float(str(binding.get("revenue", {}).get("value", "0")))
        employees = _parse_int(str(binding.get("employees", {}).get("value", "0")))

        wikidata_uri = str(binding.get("company", {}).get("value", "")).strip()
        wikidata_id = wikidata_uri.rsplit("/", 1)[-1] if wikidata_uri else ""

        ranking_score = _rank_candidate(country, industry_label, sitelinks, revenue_usd, employees)
        rows.append(
            {
                "company_name": company_name,
                "domain": domain,
                "source_type": "seed",
                "country": country.name,
                "region_group": country.region_group,
                "industry_label": industry_label,
                "website_url": website_url,
                "wikidata_id": wikidata_id,
                "sitelinks": sitelinks,
                "revenue_usd": round(revenue_usd, 2) if revenue_usd > 0 else 0.0,
                "employees": employees,
                "ranking_score": ranking_score,
                "data_source": "wikidata",
                "last_refreshed_on": refreshed_on,
            }
        )
    return rows


def _default_news_query(company_name: str, domain: str) -> str:
    target = f'"{company_name}" OR "{domain}"'
    signals = [
        '"SAP S/4HANA rollout"',
        '"ECC sunset"',
        '"ERP modernization phase-2"',
        '"supply chain control tower"',
        '"demand planning platform"',
        '"warehouse digitization"',
        '"margin improvement program"',
        '"cost transformation office"',
        '"policy enforcement"',
        '"audit readiness"',
        '"vendor consolidation"',
        '"procurement opened"',
        '"security review started"',
        '"go-live date set"',
        '"pilot expanded"',
    ]
    return f"{target} ({' OR '.join(signals)})"


def _merge_source_handles(settings: Settings, rows: list[dict[str, Any]]) -> int:
    handles_path = settings.account_source_handles_path
    existing_rows = load_csv_rows(handles_path)
    by_domain: dict[str, dict[str, str]] = {}
    for row in existing_rows:
        domain = normalize_domain(row.get("domain", ""))
        if not domain:
            continue
        by_domain[domain] = {
            "domain": domain,
            "company_name": row.get("company_name", "") or "",
            "greenhouse_board": row.get("greenhouse_board", "") or "",
            "lever_company": row.get("lever_company", "") or "",
            "careers_url": row.get("careers_url", "") or "",
            "website_url": row.get("website_url", "") or "",
            "news_query": row.get("news_query", "") or "",
            "news_rss": row.get("news_rss", "") or "",
            "reddit_query": row.get("reddit_query", "") or "",
        }

    inserted = 0
    for row in rows:
        domain = normalize_domain(str(row.get("domain", "")))
        if not domain or domain in by_domain:
            continue
        company_name = str(row.get("company_name", domain)).strip() or domain
        by_domain[domain] = {
            "domain": domain,
            "company_name": company_name,
            "greenhouse_board": "",
            "lever_company": "",
            "careers_url": "",
            "website_url": str(row.get("website_url", f"https://{domain}")).strip() or f"https://{domain}",
            "news_query": _default_news_query(company_name, domain),
            "news_rss": "",
            "reddit_query": f'"{company_name}" OR "{domain}"',
        }
        inserted += 1

    merged_rows = sorted(by_domain.values(), key=lambda item: item["domain"])
    write_csv_rows(
        handles_path,
        merged_rows,
        fieldnames=[
            "domain",
            "company_name",
            "greenhouse_board",
            "lever_company",
            "careers_url",
            "website_url",
            "news_query",
            "news_rss",
            "reddit_query",
        ],
    )
    return inserted


def build_cpg_watchlist(
    settings: Settings,
    limit: int = 1000,
    merge_handles: bool = True,
) -> dict[str, Any]:
    refreshed_on = date.today().isoformat()

    raw_rows: list[dict[str, Any]] = []
    fetched_per_country: dict[str, int] = {}
    failed_countries: dict[str, str] = {}

    max_workers = min(max(1, int(settings.watchlist_query_workers)), len(TARGET_COUNTRIES))
    timeout_seconds = max(15, int(settings.watchlist_country_query_timeout_seconds))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_by_country = {
            executor.submit(
                _fetch_country_rows,
                country=country,
                refreshed_on=refreshed_on,
                timeout_seconds=timeout_seconds,
            ): country.name
            for country in TARGET_COUNTRIES
        }
        for future in as_completed(future_by_country):
            country_name = future_by_country[future]
            try:
                resolved_name, country_rows, error_summary = future.result()
            except Exception as exc:
                fetched_per_country[country_name] = 0
                failed_countries[country_name] = str(exc)
                continue
            fetched_per_country[resolved_name] = len(country_rows)
            if error_summary:
                failed_countries[resolved_name] = error_summary
                continue
            raw_rows.extend(country_rows)

    by_domain: dict[str, dict[str, Any]] = {}
    for row in raw_rows:
        domain = str(row.get("domain", "")).strip().lower()
        if not domain:
            continue
        existing = by_domain.get(domain)
        if existing is None:
            by_domain[domain] = row
            continue

        current_score = float(row.get("ranking_score", 0.0) or 0.0)
        existing_score = float(existing.get("ranking_score", 0.0) or 0.0)
        current_sitelinks = int(row.get("sitelinks", 0) or 0)
        existing_sitelinks = int(existing.get("sitelinks", 0) or 0)

        if (current_score, current_sitelinks) > (existing_score, existing_sitelinks):
            by_domain[domain] = row

    deduped_rows = list(by_domain.values())
    deduped_rows.sort(
        key=lambda row: (
            float(row.get("ranking_score", 0.0) or 0.0),
            int(row.get("sitelinks", 0) or 0),
            str(row.get("company_name", "")).lower(),
        ),
        reverse=True,
    )

    selected = deduped_rows[: max(1, int(limit))]

    watchlist_rows = [
        {
            "company_name": str(row.get("company_name", "")),
            "domain": str(row.get("domain", "")),
            "source_type": "seed",
            "country": str(row.get("country", "")),
            "region_group": str(row.get("region_group", "")),
            "industry_label": str(row.get("industry_label", "")),
            "website_url": str(row.get("website_url", "")),
            "wikidata_id": str(row.get("wikidata_id", "")),
            "sitelinks": int(row.get("sitelinks", 0) or 0),
            "revenue_usd": float(row.get("revenue_usd", 0.0) or 0.0),
            "employees": int(row.get("employees", 0) or 0),
            "ranking_score": float(row.get("ranking_score", 0.0) or 0.0),
            "data_source": "wikidata",
            "last_refreshed_on": refreshed_on,
        }
        for row in selected
    ]

    write_csv_rows(
        settings.watchlist_accounts_path,
        watchlist_rows,
        fieldnames=[
            "company_name",
            "domain",
            "source_type",
            "country",
            "region_group",
            "industry_label",
            "website_url",
            "wikidata_id",
            "sitelinks",
            "revenue_usd",
            "employees",
            "ranking_score",
            "data_source",
            "last_refreshed_on",
        ],
    )

    handles_inserted = 0
    if merge_handles:
        handles_inserted = _merge_source_handles(settings, watchlist_rows)

    selected_per_country: dict[str, int] = {}
    selected_per_region: dict[str, int] = {}
    for row in watchlist_rows:
        country = str(row.get("country", ""))
        region = str(row.get("region_group", ""))
        selected_per_country[country] = selected_per_country.get(country, 0) + 1
        selected_per_region[region] = selected_per_region.get(region, 0) + 1

    return {
        "requested_limit": max(1, int(limit)),
        "raw_rows": len(raw_rows),
        "deduped_rows": len(deduped_rows),
        "selected_rows": len(watchlist_rows),
        "watchlist_path": str(settings.watchlist_accounts_path),
        "handles_inserted": handles_inserted,
        "fetched_per_country": fetched_per_country,
        "failed_country_count": len(failed_countries),
        "failed_countries": failed_countries,
        "selected_per_country": selected_per_country,
        "selected_per_region": selected_per_region,
    }
