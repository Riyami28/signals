"""Direct website technology scanner — ZERO API keys needed.

Detects technologies used by companies by scanning their website HTML:
- HTTP headers (Server, X-Powered-By, X-Generator)
- Meta tags (generator, framework)
- Script sources (React, Angular, Vue, jQuery, etc.)
- CSS/link patterns (Bootstrap, Tailwind, etc.)
- Cloud/CDN indicators (CloudFront, Akamai, Cloudflare, etc.)
- Known SaaS tools (HubSpot, Salesforce, Marketo, Intercom, etc.)

Fills the tech_fit dimension (20% weight) which was completely empty.
No API keys, no signup, no cost — unlimited usage.
"""

from __future__ import annotations

import asyncio
import logging
import re
import time
from typing import Any

import httpx

from src import db
from src.models import SignalObservation
from src.settings import Settings
from src.utils import stable_hash, utc_now_iso

logger = logging.getLogger(__name__)

# ─── Technology detection patterns ─────────────────────────────────────
# Each pattern: (regex_or_string, tech_name, signal_code, confidence)

# HTTP header patterns
_HEADER_PATTERNS: list[tuple[str, str, str, float]] = [
    ("cloudflare", "Cloudflare CDN", "cloud_platform_messaging", 0.55),
    ("amazons3", "Amazon S3", "cloud_platform_messaging", 0.65),
    ("cloudfront", "Amazon CloudFront", "cloud_platform_messaging", 0.65),
    ("gws", "Google Web Server", "cloud_platform_messaging", 0.55),
    ("microsoft-azure", "Microsoft Azure", "cloud_platform_messaging", 0.65),
    ("azure", "Microsoft Azure", "cloud_platform_messaging", 0.60),
    ("nginx", "Nginx", "cloud_platform_messaging", 0.40),
    ("apache", "Apache", "cloud_platform_messaging", 0.35),
    ("akamai", "Akamai CDN", "cloud_platform_messaging", 0.50),
    ("fastly", "Fastly CDN", "cloud_platform_messaging", 0.50),
    ("vercel", "Vercel", "cloud_platform_messaging", 0.55),
    ("netlify", "Netlify", "cloud_platform_messaging", 0.50),
    ("wp engine", "WP Engine", "enterprise_modernization_program", 0.35),
]

# Script/link patterns to detect from HTML source
_HTML_TECH_PATTERNS: list[tuple[str, str, str, float]] = [
    # Cloud / Infrastructure
    (r"amazonaws\.com", "AWS", "cloud_platform_messaging", 0.70),
    (r"cloudfront\.net", "CloudFront (AWS)", "cloud_platform_messaging", 0.65),
    (r"azure\.com|azureedge\.net|azure\.net", "Microsoft Azure", "cloud_platform_messaging", 0.65),
    (r"googleapis\.com|gstatic\.com", "Google Cloud", "cloud_platform_messaging", 0.55),
    (r"storage\.googleapis\.com", "Google Cloud Storage", "cloud_platform_messaging", 0.65),
    (r"firebaseapp\.com|firebase\.google", "Firebase (Google)", "cloud_platform_messaging", 0.60),
    # Frontend frameworks (signals tech maturity)
    (r"react|reactdom|react-dom", "React.js", "enterprise_modernization_program", 0.40),
    (r"angular|ng-version", "Angular", "enterprise_modernization_program", 0.45),
    (r"vue\.js|vuejs|vue\.min", "Vue.js", "enterprise_modernization_program", 0.40),
    (r"next\.js|nextjs|_next/", "Next.js", "enterprise_modernization_program", 0.45),
    # SaaS / Marketing (signals enterprise tooling adoption)
    (r"hubspot\.com|hs-scripts\.com|hbspt", "HubSpot", "enterprise_modernization_program", 0.55),
    (r"salesforce\.com|force\.com|pardot", "Salesforce", "enterprise_modernization_program", 0.65),
    (r"marketo\.com|marketo\.net|munchkin", "Marketo", "enterprise_modernization_program", 0.60),
    (r"eloqua\.com", "Oracle Eloqua", "erp_s4_migration_milestone", 0.55),
    (r"sap\.com|sapbydesign|sap-ariba", "SAP", "erp_s4_migration_milestone", 0.70),
    (r"oracle\.com|oraclecloud", "Oracle Cloud", "erp_s4_migration_milestone", 0.60),
    (r"workday\.com", "Workday", "enterprise_modernization_program", 0.60),
    (r"servicenow\.com", "ServiceNow", "enterprise_modernization_program", 0.60),
    # Analytics / monitoring (signals tooling maturity)
    (r"datadog|dd-rum|datadoghq", "Datadog", "tooling_sprawl_detected", 0.65),
    (r"newrelic|new-relic|nr-data", "New Relic", "tooling_sprawl_detected", 0.60),
    (r"segment\.com|segment\.io|analytics\.js", "Segment", "tooling_sprawl_detected", 0.50),
    (r"mixpanel\.com", "Mixpanel", "tooling_sprawl_detected", 0.45),
    (r"amplitude\.com", "Amplitude", "tooling_sprawl_detected", 0.45),
    (r"pendo\.io", "Pendo", "tooling_sprawl_detected", 0.50),
    (r"fullstory\.com", "FullStory", "tooling_sprawl_detected", 0.45),
    (r"hotjar\.com", "Hotjar", "tooling_sprawl_detected", 0.40),
    (r"sentry\.io|sentry-cdn", "Sentry", "tooling_sprawl_detected", 0.50),
    # DevOps / CI-CD indicators
    (r"github\.com|github\.io", "GitHub", "gitops_detected", 0.40),
    (r"gitlab\.com", "GitLab", "gitops_detected", 0.50),
    (r"atlassian\.com|bitbucket", "Atlassian/Bitbucket", "gitops_detected", 0.45),
    (r"jira\.com|jira-", "Jira", "tooling_sprawl_detected", 0.50),
    (r"jenkins", "Jenkins", "gitops_detected", 0.55),
    # Communication / productivity
    (r"intercom\.com|intercomcdn", "Intercom", "enterprise_modernization_program", 0.45),
    (r"drift\.com", "Drift", "enterprise_modernization_program", 0.40),
    (r"zendesk\.com|zdassets", "Zendesk", "enterprise_modernization_program", 0.45),
    (r"freshdesk|freshworks", "Freshworks", "enterprise_modernization_program", 0.40),
    (r"slack\.com|slack-edge", "Slack", "enterprise_modernization_program", 0.40),
    # Kubernetes / container (rare on public sites but sometimes in docs/blogs)
    (r"kubernetes|k8s\.io", "Kubernetes", "kubernetes_detected", 0.80),
    (r"docker\.com|docker\.io", "Docker", "kubernetes_detected", 0.55),
    (r"terraform|hashicorp", "Terraform/HashiCorp", "terraform_detected", 0.75),
    # CMS (signals digital maturity)
    (r"wp-content|wordpress", "WordPress", "enterprise_modernization_program", 0.30),
    (r"drupal", "Drupal", "enterprise_modernization_program", 0.35),
    (r"shopify\.com|cdn\.shopify", "Shopify", "enterprise_modernization_program", 0.40),
    (r"contentful\.com", "Contentful", "enterprise_modernization_program", 0.50),
    (r"sanity\.io", "Sanity CMS", "enterprise_modernization_program", 0.45),
    # Tag managers / data layer (signals data maturity)
    (r"googletagmanager|gtm\.js", "Google Tag Manager", "enterprise_modernization_program", 0.35),
    (r"tealium", "Tealium", "enterprise_modernization_program", 0.50),
    (r"ensighten", "Ensighten", "enterprise_modernization_program", 0.45),
    (r"launch\.adobe|adobedtm", "Adobe Launch", "enterprise_modernization_program", 0.55),
]

# Meta generator patterns
_META_GENERATOR_PATTERNS: list[tuple[str, str, str, float]] = [
    (r"wordpress", "WordPress", "enterprise_modernization_program", 0.30),
    (r"drupal", "Drupal", "enterprise_modernization_program", 0.35),
    (r"joomla", "Joomla", "enterprise_modernization_program", 0.30),
    (r"hubspot", "HubSpot CMS", "enterprise_modernization_program", 0.55),
    (r"wix\.com", "Wix", "enterprise_modernization_program", 0.25),
    (r"squarespace", "Squarespace", "enterprise_modernization_program", 0.25),
    (r"adobe experience", "Adobe Experience Manager", "enterprise_modernization_program", 0.65),
    (r"sitecore", "Sitecore", "enterprise_modernization_program", 0.60),
    (r"contentful", "Contentful", "enterprise_modernization_program", 0.50),
]


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


async def _scan_website(
    client: httpx.AsyncClient,
    domain: str,
) -> dict[str, Any]:
    """Fetch website and detect technologies from HTML + headers."""
    result: dict[str, Any] = {
        "domain": domain,
        "technologies": [],
        "headers": {},
        "status": None,
        "error": None,
    }

    url = f"https://{domain}"
    try:
        resp = await client.get(
            url,
            follow_redirects=True,
            timeout=12,
        )
        result["status"] = resp.status_code

        if resp.status_code >= 400:
            # Try http:// as fallback
            try:
                resp = await client.get(
                    f"http://{domain}",
                    follow_redirects=True,
                    timeout=10,
                )
                result["status"] = resp.status_code
            except Exception:
                result["error"] = f"http_{resp.status_code}"
                return result

        # Capture headers
        result["headers"] = dict(resp.headers)
        html = resp.text[:200_000]  # Limit to 200KB to avoid memory issues

    except httpx.ConnectError:
        result["error"] = "connect_error"
        return result
    except httpx.TimeoutException:
        result["error"] = "timeout"
        return result
    except Exception as exc:
        result["error"] = str(exc)[:100]
        return result

    detected: list[tuple[str, str, float]] = []  # (tech_name, signal_code, confidence)
    seen_signals: set[str] = set()  # Dedupe by signal_code

    # 1. Check HTTP headers
    server = str(resp.headers.get("server", "")).lower()
    x_powered = str(resp.headers.get("x-powered-by", "")).lower()
    x_generator = str(resp.headers.get("x-generator", "")).lower()
    via = str(resp.headers.get("via", "")).lower()
    header_text = f"{server} {x_powered} {x_generator} {via}"

    for keyword, tech_name, signal_code, confidence in _HEADER_PATTERNS:
        if keyword in header_text and signal_code not in seen_signals:
            detected.append((tech_name, signal_code, confidence))
            seen_signals.add(signal_code)

    # 2. Check meta generator tag
    gen_match = re.search(
        r'<meta[^>]+name=["\']generator["\'][^>]+content=["\']([^"\']+)',
        html,
        re.IGNORECASE,
    )
    if not gen_match:
        gen_match = re.search(
            r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+name=["\']generator',
            html,
            re.IGNORECASE,
        )
    if gen_match:
        generator = gen_match.group(1).lower()
        for pattern, tech_name, signal_code, confidence in _META_GENERATOR_PATTERNS:
            if re.search(pattern, generator) and signal_code not in seen_signals:
                detected.append((tech_name, signal_code, confidence))
                seen_signals.add(signal_code)

    # 3. Scan HTML for technology patterns
    html_lower = html.lower()
    for pattern, tech_name, signal_code, confidence in _HTML_TECH_PATTERNS:
        if signal_code in seen_signals:
            continue
        if re.search(pattern, html_lower):
            detected.append((tech_name, signal_code, confidence))
            seen_signals.add(signal_code)

    result["technologies"] = detected
    return result


async def _collect_one_account(
    conn,
    client: httpx.AsyncClient,
    account: dict,
    source_reliability: float,
) -> tuple[int, int]:
    """Scan one company's website and insert tech signals."""
    account_id = str(account["account_id"])
    domain = str(account.get("domain", "")).strip()

    if not domain or domain.endswith(".example"):
        return 0, 0

    source_name = "website_techscan"
    endpoint = f"scan:{domain}"
    if db.was_crawled_today(conn, source=source_name, account_id=account_id, endpoint=endpoint):
        return 0, 0

    scan = await _scan_website(client, domain)

    if scan.get("error"):
        db.record_crawl_attempt(
            conn,
            source=source_name,
            account_id=account_id,
            endpoint=endpoint,
            status="error",
            error_summary=str(scan["error"])[:200],
            commit=False,
        )
        db.mark_crawled(conn, source=source_name, account_id=account_id, endpoint=endpoint, commit=False)
        return 0, 0

    technologies = scan.get("technologies", [])

    if not technologies:
        db.record_crawl_attempt(
            conn,
            source=source_name,
            account_id=account_id,
            endpoint=endpoint,
            status="success",
            error_summary="no_tech_detected",
            commit=False,
        )
        db.mark_crawled(conn, source=source_name, account_id=account_id, endpoint=endpoint, commit=False)
        return 0, 0

    inserted = 0
    seen = 0

    tech_names = [t[0] for t in technologies]
    tech_summary = ", ".join(tech_names[:15])

    for tech_name, signal_code, confidence in technologies:
        seen += 1
        observation = _build_observation(
            account_id=account_id,
            signal_code=signal_code,
            source="website_techscan",
            observed_at=utc_now_iso(),
            confidence=confidence,
            source_reliability=source_reliability,
            evidence_url=f"https://{domain}",
            evidence_text=f"Technology detected on {domain}: {tech_name}. Full stack: {tech_summary}",
            payload={
                "domain": domain,
                "detected_tech": tech_name,
                "signal_code": signal_code,
                "all_technologies": tech_names,
                "http_status": scan.get("status"),
            },
        )
        if db.insert_signal_observation(conn, observation, commit=False):
            inserted += 1

    db.record_crawl_attempt(
        conn,
        source=source_name,
        account_id=account_id,
        endpoint=endpoint,
        status="success",
        error_summary="",
        commit=False,
    )
    db.mark_crawled(conn, source=source_name, account_id=account_id, endpoint=endpoint, commit=False)

    return inserted, seen


async def collect(
    conn,
    settings: Settings,
    source_reliability: float = 0.70,
    account_ids: list[str] | None = None,
) -> dict[str, int]:
    """
    Main entry point: scan company websites to detect technologies.

    NO API KEYS NEEDED. Fetches websites directly and parses HTML.

    Args:
        conn: DB connection
        settings: App settings (no special key needed)
        source_reliability: Default reliability for this source
        account_ids: Optional list of specific account IDs to process

    Returns:
        {"inserted": N, "seen": N, "accounts_processed": N}
    """
    # Use the existing techstack setting for max accounts, default 50
    max_accounts = getattr(settings, "techscan_max_accounts", 100)

    if account_ids:
        placeholders = ",".join(["%s"] * len(account_ids))
        accounts = [
            dict(r)
            for r in conn.execute(
                f"SELECT account_id, company_name, domain FROM accounts WHERE account_id IN ({placeholders})",
                tuple(account_ids),
            ).fetchall()
        ]
    else:
        # Prioritize accounts not yet scanned
        accounts = [
            dict(r)
            for r in conn.execute(
                """
                SELECT a.account_id, a.company_name, a.domain
                FROM accounts a
                LEFT JOIN crawl_checkpoints cp
                  ON cp.account_id = a.account_id
                  AND cp.source = 'website_techscan'
                WHERE COALESCE(a.domain, '') <> ''
                ORDER BY
                    CASE WHEN cp.last_crawled_at IS NULL THEN 0 ELSE 1 END,
                    cp.last_crawled_at ASC,
                    a.created_at ASC
                LIMIT %s
                """,
                (max_accounts,),
            ).fetchall()
        ]

    if not accounts:
        return {"inserted": 0, "seen": 0, "accounts_processed": 0}

    logger.info("website_techscan starting accounts=%d", len(accounts))
    t0 = time.monotonic()

    total_inserted = 0
    total_seen = 0
    processed = 0

    # We can be aggressive with concurrency since we're just fetching websites
    # But be polite — 10 concurrent connections
    concurrency = min(10, len(accounts))
    semaphore = asyncio.Semaphore(concurrency)

    async with httpx.AsyncClient(
        headers={
            "User-Agent": "Mozilla/5.0 (compatible; SignalsBot/1.0; +https://zopdev.com)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        },
    ) as client:

        async def _run_one(account: dict) -> tuple[int, int]:
            async with semaphore:
                # Use SAVEPOINT so one account's DB error doesn't abort the whole batch
                try:
                    conn.execute("SAVEPOINT techscan_worker")
                    result = await _collect_one_account(
                        conn=conn,
                        client=client,
                        account=account,
                        source_reliability=source_reliability,
                    )
                    conn.execute("RELEASE SAVEPOINT techscan_worker")
                    # Small delay to be polite
                    await asyncio.sleep(0.1)
                    return result
                except Exception as exc:
                    try:
                        conn.execute("ROLLBACK TO SAVEPOINT techscan_worker")
                    except Exception:
                        pass
                    logger.warning("website_techscan_worker_error: %s", exc)
                    await asyncio.sleep(0.1)
                    return 0, 0

        tasks = [_run_one(acct) for acct in accounts]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    conn.commit()

    for result in results:
        if isinstance(result, Exception):
            logger.warning("website_techscan_worker_error: %s", result)
            continue
        ins, seen = result
        total_inserted += ins
        total_seen += seen
        processed += 1

    dt = time.monotonic() - t0
    logger.info(
        "website_techscan done accounts=%d inserted=%d seen=%d duration=%.1fs",
        processed,
        total_inserted,
        total_seen,
        dt,
    )

    return {"inserted": total_inserted, "seen": total_seen, "accounts_processed": processed}
