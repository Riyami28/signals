"""GitHub MCP Collector — Issues, Discussions, and PR signals via GitHub REST+GraphQL + Claude.

Detects four signal types:
  - github_migration_pr    : PRs removing old infra tools and adding replacements
  - github_infra_issue     : Issues expressing infrastructure pain or operational friction
  - github_evaluation      : Discussion threads comparing or evaluating DevOps/FinOps tools
  - github_stargazer_velocity: Rapid star growth (velocity burst in last N days)

Company resolution strategy (in priority order):
  1. Author email domain matches a known account domain
  2. Author company field fuzzy-matches a known account company_name
  3. Author is a member of a GitHub org that maps to an account domain

Claude Haiku is used to classify ambiguous issues/discussions when keyword
matching alone scores below a confidence threshold.

Source name: github_mcp
Reliability: 0.75
Rate limits: unauthenticated 60 req/hr; authenticated (SIGNALS_GITHUB_TOKEN) 5000 req/hr.

Setup:
  Set SIGNALS_GITHUB_TOKEN in environment for full rate limits.
  Set SIGNALS_CLAUDE_API_KEY for LLM-assisted classification.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any

import httpx

from src import db
from src.models import SignalObservation
from src.settings import Settings
from src.utils import normalize_domain, stable_hash, utc_now_iso

logger = logging.getLogger(__name__)

SOURCE_NAME = "github_mcp"
SOURCE_RELIABILITY = 0.75

GITHUB_API = "https://api.github.com"
GITHUB_GRAPHQL = "https://api.github.com/graphql"

# ─── Signal codes ─────────────────────────────────────────────────────────────

SIGNAL_MIGRATION_PR = "github_migration_pr"
SIGNAL_INFRA_ISSUE = "github_infra_issue"
SIGNAL_EVALUATION = "github_evaluation"
SIGNAL_STARGAZER_VELOCITY = "github_stargazer_velocity"

# ─── Migration tech pairs ──────────────────────────────────────────────────────

# (from_tech, to_tech, pr_search_terms)
_MIGRATION_PAIRS: list[tuple[str, str, list[str]]] = [
    ("terraform", "opentofu", ["opentofu", "migrate from terraform", "terraform to opentofu", "tofu"]),
    ("jenkins", "github-actions", ["github actions", "migrate from jenkins", "replace jenkins", "gha"]),
    ("jenkins", "gitlab-ci", ["gitlab ci", "migrate from jenkins", "gitlab pipeline"]),
    ("datadog", "prometheus", ["migrate from datadog", "prometheus grafana", "open-source monitoring"]),
    ("ansible", "terraform", ["migrate from ansible", "replace ansible", "infrastructure as code"]),
    ("helm", "kustomize", ["kustomize", "migrate from helm", "replace helm charts"]),
    ("aws-codepipeline", "github-actions", ["github actions", "migrate from codepipeline", "replace codepipeline"]),
    ("self-hosted k8s", "eks", ["migrate to eks", "managed kubernetes", "eks migration"]),
    ("self-hosted k8s", "gke", ["migrate to gke", "gke migration", "google kubernetes engine"]),
]

# Popular open-source repos where migrations/evaluations happen in issues/discussions
_DEFAULT_SIGNAL_REPOS = [
    "opentofu/opentofu",
    "hashicorp/terraform",
    "kubernetes/kubernetes",
    "argoproj/argo-cd",
    "grafana/grafana",
    "prometheus/prometheus",
    "cncf/toc",  # CNCF TOC — cloud-native evaluation discussions
    "backstage/backstage",  # IDP evaluations
    "open-cost/opencost",  # FinOps
    "kubecost/cost-analyzer-helm-chart",
]

# ─── Keyword signals for keyword-only classification ──────────────────────────

_INFRA_PAIN_KW = [
    "kubernetes",
    "k8s",
    "terraform",
    "helm",
    "ci/cd",
    "pipeline",
    "infrastructure",
    "devops",
    "platform engineering",
    "finops",
    "cloud cost",
    "cost spike",
    "aws bill",
    "gcp cost",
    "latency",
    "outage",
    "incident",
    "toil",
    "bottleneck",
    "slow deploys",
    "deployment failed",
    "container",
    "microservice",
    "observability",
    "monitoring",
    "alerting",
    "sre",
    "iac",
]

_EVAL_KW = [
    "evaluating",
    "comparing",
    "considering",
    "looking for",
    "alternative to",
    "replace ",
    "switch from",
    "migration from",
    "recommendation",
    "which tool",
    "looking at options",
]

# Confidence threshold below which we ask Claude
_KW_CONFIDENCE_THRESHOLD = 0.65

# ─── Claude classification prompt ─────────────────────────────────────────────

_CLASSIFY_PROMPT = """\
You are a buying-signal analyst for enterprise infrastructure software (DevOps, Platform Engineering, FinOps).

Target company: {company_name} (domain: {domain})

Analyze the GitHub item below and classify the buying signal.

Return a JSON object with exactly these fields:
{{
  "relevant": <true | false>,
  "signal_type": "<one of: github_migration_pr | github_infra_issue | github_evaluation | none>",
  "confidence": <float 0.0-1.0>,
  "tech_from": "<technology being replaced, or empty string>",
  "tech_to": "<technology replacing it, or empty string>",
  "evidence_sentence": "<1-2 sentence summary of the key signal, max 200 chars>"
}}

Relevance rules:
- Set relevant=true ONLY if the author works at the target company (company field or email domain match)
  OR if the item is authored by the target company's GitHub org
- A general discussion unrelated to the target company is NOT relevant
- If relevant=false, set signal_type to none, confidence 0.0, evidence_sentence empty

Signal type definitions:
- github_migration_pr: A pull request that removes or replaces an infrastructure tool
- github_infra_issue: An issue describing infrastructure pain, toil, or operational friction
- github_evaluation: A discussion or issue thread comparing/evaluating DevOps or FinOps tools
- none: No clear infrastructure buying signal

Item type: {item_type}
Repo: {repo}
Title: {title}
Body: {body}
Author: {author_login} (company: {author_company}, email domain: {author_email_domain})
URL: {url}
"""


def _make_headers(token: str | None) -> dict[str, str]:
    h = {
        "Accept": "application/vnd.github.v3+json",
        "User-Agent": "zopdev-signals/0.1",
    }
    if token:
        h["Authorization"] = f"Bearer {token}"
    return h


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
            "evidence_url": evidence_url,
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


# ─── GitHub API helpers ────────────────────────────────────────────────────────


async def _search_issues(
    client: httpx.AsyncClient,
    repo: str,
    query_extra: str,
    token: str | None,
    lookback_days: int,
    per_page: int = 25,
) -> list[dict]:
    cutoff = time.strftime("%Y-%m-%d", time.gmtime(time.time() - lookback_days * 86400))
    q = f"repo:{repo} {query_extra} created:>{cutoff}"
    try:
        resp = await client.get(
            f"{GITHUB_API}/search/issues",
            params={"q": q, "per_page": per_page, "sort": "created", "order": "desc"},
            headers=_make_headers(token),
            timeout=15,
        )
        if resp.status_code in (403, 422):
            logger.warning("github_mcp search_issues status=%s repo=%s", resp.status_code, repo)
            return []
        resp.raise_for_status()
        return resp.json().get("items", [])
    except httpx.HTTPStatusError as exc:
        logger.warning("github_mcp search_issues_http repo=%s status=%s", repo, exc.response.status_code)
        return []
    except Exception as exc:
        logger.debug("github_mcp search_issues_error repo=%s error=%s", repo, exc)
        return []


async def _fetch_discussions(
    client: httpx.AsyncClient,
    repo: str,
    token: str | None,
    lookback_days: int,
    first: int = 20,
) -> list[dict]:
    """Fetch recent discussions via GraphQL (requires auth token)."""
    if not token:
        return []
    owner, _, name = repo.partition("/")
    if not name:
        return []
    query = """
    query($owner: String!, $name: String!, $first: Int!) {
      repository(owner: $owner, name: $name) {
        discussions(first: $first, orderBy: {field: CREATED_AT, direction: DESC}) {
          nodes {
            title body url createdAt
            author { login }
            category { name }
          }
        }
      }
    }
    """
    cutoff_str = time.strftime("%Y-%m-%d", time.gmtime(time.time() - lookback_days * 86400))
    try:
        resp = await client.post(
            GITHUB_GRAPHQL,
            json={"query": query, "variables": {"owner": owner, "name": name, "first": first}},
            headers={**_make_headers(token), "Accept": "application/json"},
            timeout=20,
        )
        resp.raise_for_status()
        nodes = resp.json().get("data", {}).get("repository", {}).get("discussions", {}).get("nodes", [])
        return [n for n in nodes if (n.get("createdAt") or "")[:10] >= cutoff_str]
    except Exception as exc:
        logger.debug("github_mcp discussions_error repo=%s error=%s", repo, exc)
        return []


async def _get_user_profile(
    client: httpx.AsyncClient,
    login: str,
    token: str | None,
    cache: dict[str, dict],
) -> dict[str, str]:
    if login in cache:
        return cache[login]
    try:
        resp = await client.get(
            f"{GITHUB_API}/users/{login}",
            headers=_make_headers(token),
            timeout=10,
        )
        if resp.status_code == 404:
            cache[login] = {}
            return {}
        resp.raise_for_status()
        data = resp.json()
        company = str(data.get("company") or "").lstrip("@").strip()
        email = str(data.get("email") or "")
        email_domain = ""
        if "@" in email:
            d = email.split("@", 1)[1].lower()
            _GENERIC = {"gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "icloud.com"}
            if d not in _GENERIC:
                email_domain = d
        profile = {"company": company, "email_domain": email_domain}
        cache[login] = profile
        return profile
    except Exception as exc:
        logger.debug("github_mcp user_profile_error login=%s error=%s", login, exc)
        cache[login] = {}
        return {}


async def _get_org_members_domains(
    client: httpx.AsyncClient,
    org: str,
    token: str | None,
) -> set[str]:
    """Fetch email domains of public org members (up to first 100)."""
    if not token:
        return set()
    try:
        resp = await client.get(
            f"{GITHUB_API}/orgs/{org}/members",
            params={"per_page": 100},
            headers=_make_headers(token),
            timeout=15,
        )
        if resp.status_code in (404, 403):
            return set()
        resp.raise_for_status()
        members = resp.json()
        domains: set[str] = set()
        for m in members[:30]:  # cap user detail lookups
            login = m.get("login", "")
            if not login:
                continue
            cache: dict[str, dict] = {}
            p = await _get_user_profile(client, login, token, cache)
            if p.get("email_domain"):
                domains.add(p["email_domain"])
            await asyncio.sleep(0.1)
        return domains
    except Exception as exc:
        logger.debug("github_mcp org_members_error org=%s error=%s", org, exc)
        return set()


# ─── Keyword-based classification ─────────────────────────────────────────────


def _kw_classify(title: str, body: str) -> tuple[str | None, float]:
    """Quick keyword classification. Returns (signal_code, confidence) or (None, 0.0)."""
    text = (title + " " + body).lower()

    # Check for evaluation signal first (more specific)
    eval_hits = sum(1 for kw in _EVAL_KW if kw in text)
    infra_hits = sum(1 for kw in _INFRA_PAIN_KW if kw in text)

    if eval_hits >= 1 and infra_hits >= 2:
        conf = min(0.85, 0.55 + eval_hits * 0.05 + infra_hits * 0.03)
        return SIGNAL_EVALUATION, conf
    if infra_hits >= 3:
        conf = min(0.80, 0.50 + infra_hits * 0.05)
        return SIGNAL_INFRA_ISSUE, conf
    if infra_hits >= 1:
        return SIGNAL_INFRA_ISSUE, 0.50  # below threshold → will send to Claude
    return None, 0.0


def _kw_classify_migration(title: str, body: str, tech_from: str, tech_to: str) -> tuple[bool, float]:
    text = (title + " " + body).lower()
    tf, tt = tech_from.lower(), tech_to.lower()
    _REMOVE_WORDS = {"remove", "replace", "migrate", "switch", "delete", "drop", "away from", "moving from"}
    removes_old = tf in text and any(w in text for w in _REMOVE_WORDS)
    adds_new = tt in text
    if removes_old and adds_new:
        return True, 0.85
    if removes_old or adds_new:
        return True, 0.60
    return False, 0.0


# ─── Claude classification ─────────────────────────────────────────────────────


async def _claude_classify(
    item: dict,
    item_type: str,
    repo: str,
    company_name: str,
    domain: str,
    author_login: str,
    author_company: str,
    author_email_domain: str,
    api_key: str,
    client: httpx.AsyncClient,
) -> dict | None:
    title = str(item.get("title") or "")[:300]
    body = str(item.get("body") or "")[:600]
    if not title and not body:
        return None
    url = str(item.get("html_url") or item.get("url") or "")

    prompt = _CLASSIFY_PROMPT.format(
        company_name=company_name or "unknown",
        domain=domain or "unknown",
        item_type=item_type,
        repo=repo,
        title=title,
        body=body,
        author_login=author_login,
        author_company=author_company,
        author_email_domain=author_email_domain,
        url=url,
    )
    try:
        resp = await client.post(
            "https://api.anthropic.com/v1/messages",
            json={
                "model": "claude-haiku-4-5-20251001",
                "max_tokens": 300,
                "messages": [{"role": "user", "content": prompt}],
            },
            headers={
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            timeout=25,
        )
        resp.raise_for_status()
        text = resp.json()["content"][0]["text"].strip()
        if text.startswith("```"):
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
        return json.loads(text)
    except (json.JSONDecodeError, KeyError, httpx.HTTPError) as exc:
        logger.debug("github_mcp claude_classify_failed url=%s error=%s", url[:80], exc)
        return None


# ─── Account matching ──────────────────────────────────────────────────────────


def _match_to_account(
    author_company: str,
    author_email_domain: str,
    domain_to_account: dict[str, str],
    name_to_account: dict[str, str],
) -> str | None:
    norm_email = normalize_domain(author_email_domain) if author_email_domain else ""
    if norm_email and norm_email in domain_to_account:
        return domain_to_account[norm_email]
    if author_company:
        company_lower = author_company.lower().strip()
        if company_lower in name_to_account:
            return name_to_account[company_lower]
        for name, acct_id in name_to_account.items():
            if name in company_lower or company_lower in name:
                return acct_id
    return None


# ─── Main collect entry point ──────────────────────────────────────────────────


async def collect(
    conn,
    settings: Settings,
    source_reliability_dict: dict | None = None,
    account_ids: list[str] | None = None,
    **_kwargs,
) -> dict[str, int]:
    """Collect GitHub Issues, Discussion, and PR signals.

    Strategy:
      1. Scan target signal repos for infra issues, discussions, and migration PRs
      2. For each item, check if author belongs to a known account
      3. Classify using keywords first; use Claude for borderline cases
      4. Insert qualifying observations into the DB
    """
    github_token = str(getattr(settings, "github_token", "") or "")
    claude_key = str(getattr(settings, "claude_api_key", "") or "")
    lookback_days = int(getattr(settings, "github_signal_lookback_days", 30))
    source_reliability = (source_reliability_dict or {}).get(SOURCE_NAME, SOURCE_RELIABILITY)

    if source_reliability <= 0:
        return {"inserted": 0, "seen": 0, "accounts_processed": 0}

    # Determine target repos
    signal_repos = list(getattr(settings, "github_signal_repos", None) or _DEFAULT_SIGNAL_REPOS)

    # Build account lookup maps
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
        accounts = [
            dict(r)
            for r in conn.execute(
                """SELECT a.account_id, a.company_name, a.domain
                   FROM accounts a
                   LEFT JOIN crawl_checkpoints cp
                     ON cp.account_id = a.account_id AND cp.source = %s
                   WHERE COALESCE(a.domain, '') <> ''
                   ORDER BY CASE WHEN cp.last_crawled_at IS NULL THEN 0 ELSE 1 END,
                            cp.last_crawled_at ASC, a.company_name ASC
                   LIMIT 200""",
                (SOURCE_NAME,),
            ).fetchall()
        ]

    if not accounts:
        logger.info("github_mcp: no accounts to process")
        return {"inserted": 0, "seen": 0, "accounts_processed": 0}

    domain_to_account: dict[str, str] = {}
    name_to_account: dict[str, str] = {}
    for acct in accounts:
        d = normalize_domain(str(acct.get("domain") or ""))
        if d:
            domain_to_account[d] = str(acct["account_id"])
        n = str(acct.get("company_name") or "").lower().strip()
        if n:
            name_to_account[n] = str(acct["account_id"])

    logger.info(
        "github_mcp: starting repos=%d accounts=%d lookback_days=%d",
        len(signal_repos),
        len(accounts),
        lookback_days,
    )
    t0 = time.monotonic()
    total_seen = 0
    total_inserted = 0
    user_profile_cache: dict[str, dict] = {}

    async with httpx.AsyncClient() as client:
        # ── Part 1: Scan repos for issues and discussions ──────────────────────

        for repo in signal_repos:
            endpoint = f"github_mcp:repo:{repo}"
            if db.was_crawled_today(conn, source=SOURCE_NAME, account_id="__global__", endpoint=endpoint):
                logger.debug("github_mcp already_crawled_today repo=%s", repo)
                continue

            # Fetch issues with infra pain keywords
            infra_kw_q = "is:issue is:open " + " ".join(
                f'"{kw}"' for kw in ["kubernetes", "terraform", "infrastructure", "devops", "finops", "ci/cd"]
            )
            issues = await _search_issues(client, repo, infra_kw_q, github_token, lookback_days, per_page=30)

            # Fetch discussions if token available
            discussions = await _fetch_discussions(client, repo, github_token, lookback_days)

            items: list[tuple[dict, str]] = [(i, "issue") for i in issues] + [(d, "discussion") for d in discussions]

            for item, item_type in items:
                total_seen += 1
                title = str(item.get("title") or "")[:300]
                body = str(item.get("body") or "")[:800]
                url = str(item.get("html_url") or item.get("url") or "")
                created_at = str(item.get("created_at") or item.get("createdAt") or utc_now_iso())
                login = (item.get("user") or {}).get("login", "") or (item.get("author") or {}).get("login", "")

                if not login:
                    continue

                # Get author profile (cached)
                profile = await _get_user_profile(client, login, github_token, user_profile_cache)
                author_company = profile.get("company", "")
                author_email_domain = profile.get("email_domain", "")

                if login not in user_profile_cache:
                    await asyncio.sleep(0.15)  # rate-limit user lookups

                # Try account match
                account_id = _match_to_account(
                    author_company,
                    author_email_domain,
                    domain_to_account,
                    name_to_account,
                )
                if not account_id:
                    continue

                # Keyword classify
                signal_code, kw_confidence = _kw_classify(title, body)
                if not signal_code:
                    continue

                # Use Claude for borderline cases if API key available
                if kw_confidence < _KW_CONFIDENCE_THRESHOLD and claude_key:
                    acct = next((a for a in accounts if str(a["account_id"]) == account_id), {})
                    classification = await _claude_classify(
                        item=item,
                        item_type=item_type,
                        repo=repo,
                        company_name=str(acct.get("company_name", "")),
                        domain=str(acct.get("domain", "")),
                        author_login=login,
                        author_company=author_company,
                        author_email_domain=author_email_domain,
                        api_key=claude_key,
                        client=client,
                    )
                    await asyncio.sleep(0.3)

                    if classification and classification.get("relevant"):
                        claude_sig = classification.get("signal_type", "")
                        if claude_sig in (SIGNAL_MIGRATION_PR, SIGNAL_INFRA_ISSUE, SIGNAL_EVALUATION):
                            signal_code = claude_sig
                            kw_confidence = float(classification.get("confidence", kw_confidence))
                            # Override evidence text with Claude's summary
                            evidence = str(classification.get("evidence_sentence", ""))
                        else:
                            continue
                    elif classification:
                        continue  # Claude says not relevant
                    else:
                        # Claude failed; keep keyword result only if confidence >= 0.55
                        if kw_confidence < 0.55:
                            continue

                evidence = (
                    f"{login} ({author_company or author_email_domain or 'unknown'}) "
                    f"[{item_type}] in {repo}: {title[:120]}"
                )

                obs = _build_observation(
                    account_id=account_id,
                    signal_code=signal_code,
                    source=SOURCE_NAME,
                    observed_at=created_at[:19] + "Z" if len(created_at) >= 19 else utc_now_iso(),
                    confidence=kw_confidence,
                    source_reliability=source_reliability,
                    evidence_url=url,
                    evidence_text=evidence,
                    payload={
                        "repo": repo,
                        "item_type": item_type,
                        "title": title[:200],
                        "author_login": login,
                        "author_company": author_company,
                        "author_email_domain": author_email_domain,
                    },
                )
                if db.insert_signal_observation(conn, obs, commit=False):
                    total_inserted += 1

            db.record_crawl_attempt(
                conn,
                source=SOURCE_NAME,
                account_id="__global__",
                endpoint=endpoint,
                status="success",
                error_summary="",
                commit=False,
            )
            db.mark_crawled(
                conn,
                source=SOURCE_NAME,
                account_id="__global__",
                endpoint=endpoint,
                commit=False,
            )

            await asyncio.sleep(0.5)

        # ── Part 2: Migration PR sweep across signal repos ─────────────────────

        migration_endpoint = "github_mcp:migration_prs"
        if not db.was_crawled_today(conn, source=SOURCE_NAME, account_id="__global__", endpoint=migration_endpoint):
            for tech_from, tech_to, _search_terms in _MIGRATION_PAIRS:
                for repo in signal_repos:
                    pr_items = await _search_issues(
                        client,
                        repo,
                        query_extra=f'is:pr "{tech_from}" "{tech_to}"',
                        token=github_token,
                        lookback_days=lookback_days,
                        per_page=15,
                    )
                    for item in pr_items:
                        total_seen += 1
                        title = str(item.get("title") or "")[:300]
                        body = str(item.get("body") or "")[:600]
                        url = str(item.get("html_url") or "")
                        created_at = str(item.get("created_at") or utc_now_iso())
                        login = (item.get("user") or {}).get("login", "")

                        is_migration, confidence = _kw_classify_migration(title, body, tech_from, tech_to)
                        if not is_migration or confidence < 0.55:
                            continue

                        profile = await _get_user_profile(client, login, github_token, user_profile_cache)
                        await asyncio.sleep(0.1)
                        author_company = profile.get("company", "")
                        author_email_domain = profile.get("email_domain", "")

                        account_id = _match_to_account(
                            author_company,
                            author_email_domain,
                            domain_to_account,
                            name_to_account,
                        )
                        if not account_id:
                            continue

                        evidence = (
                            f"Migration PR {tech_from}→{tech_to} in {repo} "
                            f"by {login} ({author_company or author_email_domain or 'unknown'}): "
                            f"{title[:100]}"
                        )

                        obs = _build_observation(
                            account_id=account_id,
                            signal_code=SIGNAL_MIGRATION_PR,
                            source=SOURCE_NAME,
                            observed_at=created_at[:19] + "Z" if len(created_at) >= 19 else utc_now_iso(),
                            confidence=confidence,
                            source_reliability=source_reliability,
                            evidence_url=url,
                            evidence_text=evidence,
                            payload={
                                "repo": repo,
                                "item_type": "pr",
                                "title": title[:200],
                                "tech_from": tech_from,
                                "tech_to": tech_to,
                                "author_login": login,
                                "author_company": author_company,
                                "author_email_domain": author_email_domain,
                            },
                        )
                        if db.insert_signal_observation(conn, obs, commit=False):
                            total_inserted += 1

                    await asyncio.sleep(0.3)

            db.record_crawl_attempt(
                conn,
                source=SOURCE_NAME,
                account_id="__global__",
                endpoint=migration_endpoint,
                status="success",
                error_summary="",
                commit=False,
            )
            db.mark_crawled(
                conn,
                source=SOURCE_NAME,
                account_id="__global__",
                endpoint=migration_endpoint,
                commit=False,
            )

        # ── Part 3: Stargazer velocity for configured Zopdev repos ────────────

        zopdev_repos = list(getattr(settings, "github_repos", ()) or ())
        velocity_window = int(getattr(settings, "github_stargazer_velocity_days", 7))

        if zopdev_repos:
            velocity_endpoint = f"github_mcp:stargazer_velocity:{velocity_window}d"
            if not db.was_crawled_today(conn, source=SOURCE_NAME, account_id="__global__", endpoint=velocity_endpoint):
                for repo in zopdev_repos:
                    try:
                        resp = await client.get(
                            f"{GITHUB_API}/repos/{repo}",
                            headers=_make_headers(github_token),
                            timeout=10,
                        )
                        if resp.status_code == 404:
                            continue
                        resp.raise_for_status()
                        star_count = int(resp.json().get("stargazers_count", 0))

                        # Fetch recent stargazers to compute velocity
                        recent_resp = await client.get(
                            f"{GITHUB_API}/repos/{repo}/stargazers",
                            params={"per_page": 100},
                            headers={**_make_headers(github_token), "Accept": "application/vnd.github.v3.star+json"},
                            timeout=15,
                        )
                        recent_resp.raise_for_status()
                        stargazers = recent_resp.json()

                        cutoff_ts = time.time() - velocity_window * 86400
                        recent = [
                            s
                            for s in stargazers
                            if s.get("starred_at", "") and _parse_iso_ts(s["starred_at"]) >= cutoff_ts
                        ]
                        velocity = len(recent)

                        # High velocity threshold: more than 20 stars in velocity_window days
                        if velocity < 20:
                            continue

                        confidence = min(0.9, 0.5 + velocity / 200)
                        total_seen += 1

                        # Map stargazers to known accounts
                        for star_entry in recent:
                            star_login = (star_entry.get("user") or {}).get("login", "")
                            if not star_login:
                                continue
                            p = await _get_user_profile(client, star_login, github_token, user_profile_cache)
                            await asyncio.sleep(0.05)
                            account_id = _match_to_account(
                                p.get("company", ""),
                                p.get("email_domain", ""),
                                domain_to_account,
                                name_to_account,
                            )
                            if not account_id:
                                continue

                            obs = _build_observation(
                                account_id=account_id,
                                signal_code=SIGNAL_STARGAZER_VELOCITY,
                                source=SOURCE_NAME,
                                observed_at=utc_now_iso(),
                                confidence=confidence,
                                source_reliability=source_reliability,
                                evidence_url=f"https://github.com/{repo}",
                                evidence_text=(
                                    f"{repo} gained {velocity} stars in {velocity_window} days "
                                    f"(total {star_count}); {star_login} is from "
                                    f"{p.get('company', '') or p.get('email_domain', 'unknown')}"
                                ),
                                payload={
                                    "repo": repo,
                                    "velocity": velocity,
                                    "velocity_window_days": velocity_window,
                                    "total_stars": star_count,
                                    "author_login": star_login,
                                    "author_company": p.get("company", ""),
                                    "author_email_domain": p.get("email_domain", ""),
                                },
                            )
                            if db.insert_signal_observation(conn, obs, commit=False):
                                total_inserted += 1

                    except Exception as exc:
                        logger.warning("github_mcp stargazer_velocity_error repo=%s error=%s", repo, exc)

                db.record_crawl_attempt(
                    conn,
                    source=SOURCE_NAME,
                    account_id="__global__",
                    endpoint=velocity_endpoint,
                    status="success",
                    error_summary="",
                    commit=False,
                )
                db.mark_crawled(
                    conn,
                    source=SOURCE_NAME,
                    account_id="__global__",
                    endpoint=velocity_endpoint,
                    commit=False,
                )

    conn.commit()

    dt = time.monotonic() - t0
    logger.info(
        "github_mcp: done seen=%d inserted=%d duration=%.1fs",
        total_seen,
        total_inserted,
        dt,
    )
    return {
        "inserted": total_inserted,
        "seen": total_seen,
        "accounts_processed": len(accounts),
    }


def _parse_iso_ts(iso: str) -> float:
    """Parse ISO 8601 timestamp to Unix epoch (best-effort)."""
    import datetime

    iso = iso.rstrip("Z").replace("T", " ")
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.datetime.strptime(iso, fmt).replace(tzinfo=datetime.timezone.utc).timestamp()
        except ValueError:
            continue
    return 0.0
