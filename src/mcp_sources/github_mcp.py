"""GitHub MCP Server — exposes GitHub Issues, Discussions, and PRs as MCP tools.

Provides two tools:
  - search_github_signals: Scan repos for infrastructure signals from a company
  - track_migration_prs: Search public repos for migration PRs (Terraform→OpenTofu, etc.)

Run standalone:
  python -m src.mcp_sources.github_mcp

Requires:
  SIGNALS_GITHUB_TOKEN (optional, increases rate limit from 60 to 5000 req/hr)
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from dataclasses import dataclass, field
from typing import Any

import httpx

logger = logging.getLogger(__name__)

GITHUB_API = "https://api.github.com"
GITHUB_GRAPHQL = "https://api.github.com/graphql"

# Tech migration pairs: (from_tech, to_tech, search_terms)
_DEFAULT_MIGRATION_PAIRS = [
    ("terraform", "opentofu", ["opentofu", "migrate from terraform", "terraform to opentofu"]),
    ("jenkins", "github-actions", ["github actions", "migrate from jenkins", "replace jenkins"]),
    ("jenkins", "gitlab-ci", ["gitlab ci", "migrate from jenkins"]),
    ("self-hosted", "kubernetes", ["kubernetes migration", "migrate to k8s", "containerize"]),
    ("datadog", "prometheus", ["migrate from datadog", "prometheus", "open source monitoring"]),
    ("ansible", "terraform", ["migrate from ansible", "replace ansible", "terraform iac"]),
    ("helm", "kustomize", ["kustomize", "migrate from helm", "replace helm"]),
    ("aws-codepipeline", "github-actions", ["github actions", "migrate from codepipeline"]),
]

# Signal type constants
SIGNAL_MIGRATION_PR = "github_migration_pr"
SIGNAL_INFRA_ISSUE = "github_infra_issue"
SIGNAL_EVALUATION = "github_evaluation"
SIGNAL_STARGAZER_VELOCITY = "github_stargazer_velocity"


@dataclass
class GitHubSignal:
    signal_type: str
    repo: str
    url: str
    title: str
    body_excerpt: str
    author_login: str
    author_company: str
    author_email_domain: str
    created_at: str
    confidence: float
    evidence_text: str
    tech_from: str = ""
    tech_to: str = ""
    item_type: str = ""  # "pr", "issue", "discussion"
    extra: dict[str, Any] = field(default_factory=dict)


def _make_headers(token: str | None) -> dict[str, str]:
    headers = {
        "Accept": "application/vnd.github.v3+json",
        "User-Agent": "zopdev-signals/0.1",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


async def _get_user_profile(
    client: httpx.AsyncClient,
    login: str,
    token: str | None,
) -> dict[str, str]:
    """Fetch company and email from a GitHub user profile. Returns {} on failure."""
    try:
        resp = await client.get(
            f"{GITHUB_API}/users/{login}",
            headers=_make_headers(token),
            timeout=10,
        )
        if resp.status_code == 404:
            return {}
        resp.raise_for_status()
        data = resp.json()
        company = str(data.get("company") or "").lstrip("@").strip()
        email = str(data.get("email") or "").strip()
        email_domain = ""
        if "@" in email:
            parts = email.split("@", 1)
            d = parts[1].lower()
            _GENERIC = {
                "gmail.com",
                "yahoo.com",
                "hotmail.com",
                "outlook.com",
                "protonmail.com",
                "icloud.com",
                "mail.com",
            }
            if d not in _GENERIC:
                email_domain = d
        return {"company": company, "email_domain": email_domain}
    except Exception as exc:
        logger.debug("github_user_profile_error login=%s error=%s", login, exc)
        return {}


async def _search_issues(
    client: httpx.AsyncClient,
    repo: str,
    query_extra: str,
    token: str | None,
    lookback_days: int = 30,
    per_page: int = 30,
) -> list[dict[str, Any]]:
    """Search issues/PRs in a repo using GitHub search API."""
    cutoff = time.strftime(
        "%Y-%m-%d",
        time.gmtime(time.time() - lookback_days * 86400),
    )
    q = f"repo:{repo} {query_extra} created:>{cutoff}"
    try:
        resp = await client.get(
            f"{GITHUB_API}/search/issues",
            params={"q": q, "per_page": per_page, "sort": "created", "order": "desc"},
            headers=_make_headers(token),
            timeout=15,
        )
        if resp.status_code in (403, 422):
            logger.warning("github_search_issues status=%s repo=%s", resp.status_code, repo)
            return []
        resp.raise_for_status()
        return resp.json().get("items", [])
    except Exception as exc:
        logger.debug("github_search_issues_error repo=%s error=%s", repo, exc)
        return []


async def _fetch_discussions(
    client: httpx.AsyncClient,
    repo: str,
    token: str | None,
    lookback_days: int = 30,
    first: int = 20,
) -> list[dict[str, Any]]:
    """Fetch recent GitHub Discussions via GraphQL."""
    if not token:
        # GraphQL requires auth for discussions
        return []

    owner, name = (repo.split("/", 1) + [""])[:2]
    if not name:
        return []

    query = """
    query($owner: String!, $name: String!, $first: Int!) {
      repository(owner: $owner, name: $name) {
        discussions(first: $first, orderBy: {field: CREATED_AT, direction: DESC}) {
          nodes {
            title
            body
            url
            createdAt
            author { login }
            category { name }
          }
        }
      }
    }
    """
    cutoff_ts = time.time() - lookback_days * 86400
    try:
        resp = await client.post(
            GITHUB_GRAPHQL,
            json={"query": query, "variables": {"owner": owner, "name": name, "first": first}},
            headers={**_make_headers(token), "Accept": "application/json"},
            timeout=20,
        )
        resp.raise_for_status()
        data = resp.json()
        nodes = data.get("data", {}).get("repository", {}).get("discussions", {}).get("nodes", [])
        # Filter by date
        result = []
        for node in nodes:
            created = node.get("createdAt", "")
            if created:
                # Simple ISO parse — compare as string (YYYY-MM-DD prefix)
                cutoff_str = time.strftime("%Y-%m-%d", time.gmtime(cutoff_ts))
                if created[:10] >= cutoff_str:
                    result.append(node)
        return result
    except Exception as exc:
        logger.debug("github_discussions_error repo=%s error=%s", repo, exc)
        return []


# ─── Infrastructure pain keywords ────────────────────────────────────────────

_INFRA_PAIN_KEYWORDS = [
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
    "cost optimization",
    "latency",
    "outage",
    "incident",
    "toil",
    "bottleneck",
    "slow deploys",
    "deployment failed",
    "helm chart",
    "container",
    "microservice",
    "observability",
    "monitoring",
    "alerting",
    "sre",
]

_EVAL_KEYWORDS = [
    "evaluating",
    "comparing",
    "considering",
    "looking for",
    "alternative to",
    "replace",
    "switch from",
    "migration",
    "recommendation",
    "which tool",
    "best practice",
    "looking at",
    "vs ",
    " or ",
    "versus",
    "options for",
]


def _is_infra_pain(title: str, body: str) -> tuple[bool, float]:
    text = (title + " " + body).lower()
    hits = sum(1 for kw in _INFRA_PAIN_KEYWORDS if kw in text)
    if hits >= 3:
        return True, min(0.85, 0.5 + hits * 0.05)
    if hits >= 1:
        return True, 0.55
    return False, 0.0


def _is_evaluation(title: str, body: str) -> tuple[bool, float]:
    text = (title + " " + body).lower()
    eval_hits = sum(1 for kw in _EVAL_KEYWORDS if kw in text)
    infra_hits = sum(1 for kw in _INFRA_PAIN_KEYWORDS if kw in text)
    if eval_hits >= 1 and infra_hits >= 1:
        return True, min(0.85, 0.55 + eval_hits * 0.05 + infra_hits * 0.03)
    return False, 0.0


def _is_migration_pr(title: str, body: str, tech_from: str, tech_to: str) -> tuple[bool, float]:
    text = (title + " " + body).lower()
    # Look for removal of old tech + addition of new
    removes_old = tech_from.lower() in text and any(
        w in text for w in ["remove", "replace", "migrate", "switch", "delete", "drop"]
    )
    adds_new = tech_to.lower() in text
    if removes_old and adds_new:
        return True, 0.85
    if removes_old or adds_new:
        return True, 0.65
    return False, 0.0


async def search_github_signals(
    company_name: str,
    domain: str,
    repos: list[str],
    lookback_days: int = 30,
    token: str | None = None,
) -> list[GitHubSignal]:
    """Scan repos for issues and discussions where the company is active.

    Matches author's company field against company_name / domain.
    Returns infrastructure pain, tool evaluations, and migration signals.
    """
    token = token or os.getenv("SIGNALS_GITHUB_TOKEN", "")
    results: list[GitHubSignal] = []

    async with httpx.AsyncClient() as client:
        for repo in repos:
            # Fetch issues (includes PRs)
            infra_items = await _search_issues(
                client,
                repo,
                query_extra=("is:issue " + " ".join(f'"{kw}"' for kw in _INFRA_PAIN_KEYWORDS[:6])),
                token=token,
                lookback_days=lookback_days,
                per_page=30,
            )
            # Fetch discussions
            discussions = await _fetch_discussions(client, repo, token, lookback_days)

            # Profile lookup cache
            _profile_cache: dict[str, dict] = {}

            async def _enrich_and_classify(
                item: dict,
                item_type: str,
            ) -> GitHubSignal | None:
                login = (item.get("user") or {}).get("login", "") or (item.get("author") or {}).get("login", "")
                title = str(item.get("title") or "")[:300]
                body = str(item.get("body") or "")[:800]
                url = str(item.get("html_url") or item.get("url") or "")
                created_at = str(item.get("created_at") or item.get("createdAt") or "")

                # Check company match
                if login and login not in _profile_cache:
                    _profile_cache[login] = await _get_user_profile(client, login, token)
                    await asyncio.sleep(0.1)

                profile = _profile_cache.get(login, {})
                author_company = profile.get("company", "")
                author_email_domain = profile.get("email_domain", "")

                # Company match test
                company_lower = (company_name or "").lower()
                domain_lower = (domain or "").lower()
                is_match = False
                if author_email_domain and domain_lower and author_email_domain == domain_lower:
                    is_match = True
                elif company_lower and author_company and company_lower in author_company.lower():
                    is_match = True

                if not is_match:
                    return None

                # Classify signal type
                is_pain, pain_conf = _is_infra_pain(title, body)
                is_eval, eval_conf = _is_evaluation(title, body)

                if is_eval and eval_conf >= is_pain and eval_conf > 0.5:
                    signal_type = SIGNAL_EVALUATION
                    confidence = eval_conf
                    evidence = f"{login} ({author_company or domain}) evaluating infra tools in {repo}: {title}"
                elif is_pain and pain_conf > 0.5:
                    signal_type = SIGNAL_INFRA_ISSUE
                    confidence = pain_conf
                    evidence = f"{login} ({author_company or domain}) infra pain in {repo}: {title}"
                else:
                    return None

                return GitHubSignal(
                    signal_type=signal_type,
                    repo=repo,
                    url=url,
                    title=title,
                    body_excerpt=body[:300],
                    author_login=login,
                    author_company=author_company,
                    author_email_domain=author_email_domain,
                    created_at=created_at,
                    confidence=confidence,
                    evidence_text=evidence,
                    item_type=item_type,
                )

            for item in infra_items:
                sig = await _enrich_and_classify(item, "issue")
                if sig:
                    results.append(sig)

            for item in discussions:
                sig = await _enrich_and_classify(item, "discussion")
                if sig:
                    results.append(sig)

            await asyncio.sleep(0.5)  # repo-level pacing

    return results


async def track_migration_prs(
    tech_pairs: list[tuple[str, str]] | None = None,
    repos: list[str] | None = None,
    lookback_days: int = 30,
    token: str | None = None,
) -> list[GitHubSignal]:
    """Search public repos for PRs that indicate infrastructure tool migrations.

    Scans target repos (or a default set of popular infra repos) for PRs
    removing old tech and adding new tech.

    tech_pairs: list of (from_tech, to_tech) tuples, e.g. [("terraform", "opentofu")]
    repos: repos to scan (default: popular open-source infra repos)
    """
    token = token or os.getenv("SIGNALS_GITHUB_TOKEN", "")

    if not tech_pairs:
        tech_pairs = [(p[0], p[1]) for p in _DEFAULT_MIGRATION_PAIRS]

    _DEFAULT_TARGET_REPOS = [
        "opentofu/opentofu",
        "hashicorp/terraform",
        "kubernetes/kubernetes",
        "argoproj/argo-cd",
        "grafana/grafana",
        "prometheus/prometheus",
        "actions/runner",
        "jenkinsci/jenkins",
    ]
    target_repos = repos or _DEFAULT_TARGET_REPOS
    results: list[GitHubSignal] = []

    async with httpx.AsyncClient() as client:
        for tech_from, tech_to in tech_pairs:
            for repo in target_repos:
                # Search for PRs mentioning both techs
                pr_items = await _search_issues(
                    client,
                    repo,
                    query_extra=f'is:pr "{tech_from}" "{tech_to}"',
                    token=token,
                    lookback_days=lookback_days,
                    per_page=20,
                )

                for item in pr_items:
                    title = str(item.get("title") or "")[:300]
                    body = str(item.get("body") or "")[:800]
                    url = str(item.get("html_url") or "")
                    created_at = str(item.get("created_at") or "")
                    login = (item.get("user") or {}).get("login", "")

                    is_migration, confidence = _is_migration_pr(title, body, tech_from, tech_to)
                    if not is_migration:
                        continue

                    # Get author profile for company resolution
                    profile: dict[str, str] = {}
                    if login:
                        try:
                            resp = await client.get(
                                f"{GITHUB_API}/users/{login}",
                                headers=_make_headers(token),
                                timeout=10,
                            )
                            if resp.status_code == 200:
                                data = resp.json()
                                company = str(data.get("company") or "").lstrip("@").strip()
                                email = str(data.get("email") or "")
                                email_domain = ""
                                if "@" in email:
                                    d = email.split("@", 1)[1].lower()
                                    if d not in {"gmail.com", "yahoo.com", "hotmail.com"}:
                                        email_domain = d
                                profile = {"company": company, "email_domain": email_domain}
                        except Exception:
                            pass
                        await asyncio.sleep(0.15)

                    evidence = (
                        f"Migration PR {tech_from}→{tech_to} in {repo} "
                        f"by {login} ({profile.get('company', 'unknown')}): {title}"
                    )

                    results.append(
                        GitHubSignal(
                            signal_type=SIGNAL_MIGRATION_PR,
                            repo=repo,
                            url=url,
                            title=title,
                            body_excerpt=body[:300],
                            author_login=login,
                            author_company=profile.get("company", ""),
                            author_email_domain=profile.get("email_domain", ""),
                            created_at=created_at,
                            confidence=confidence,
                            evidence_text=evidence,
                            tech_from=tech_from,
                            tech_to=tech_to,
                            item_type="pr",
                        )
                    )

                await asyncio.sleep(0.3)

    return results


if __name__ == "__main__":
    # Run as standalone MCP server using the mcp library if available.
    # Falls back to a simple test run if mcp is not installed.
    try:
        from mcp.server.fastmcp import FastMCP

        mcp_server = FastMCP("github-signals")

        @mcp_server.tool()
        async def search_github_signals_tool(
            company_name: str,
            domain: str,
            repos: list[str],
            lookback_days: int = 30,
        ) -> list[dict]:
            signals = await search_github_signals(
                company_name=company_name,
                domain=domain,
                repos=repos,
                lookback_days=lookback_days,
            )
            return [s.__dict__ for s in signals]

        @mcp_server.tool()
        async def track_migration_prs_tool(
            tech_pairs: list[list[str]],
            repos: list[str] | None = None,
            lookback_days: int = 30,
        ) -> list[dict]:
            pairs = [(p[0], p[1]) for p in tech_pairs if len(p) >= 2]
            signals = await track_migration_prs(
                tech_pairs=pairs,
                repos=repos,
                lookback_days=lookback_days,
            )
            return [s.__dict__ for s in signals]

        mcp_server.run()

    except ImportError:
        # mcp library not installed — run a quick smoke test
        import json

        async def _smoke_test() -> None:
            print("mcp library not installed; running smoke test...")
            sigs = await track_migration_prs(
                tech_pairs=[("terraform", "opentofu")],
                repos=["opentofu/opentofu"],
                lookback_days=7,
            )
            print(f"Found {len(sigs)} migration signals")
            for s in sigs[:3]:
                print(json.dumps(s.__dict__, indent=2, default=str))

        asyncio.run(_smoke_test())
