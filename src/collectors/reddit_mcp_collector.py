"""Reddit MCP Collector — semantic signal extraction via mcp-server-reddit + Claude.

Uses mcp-server-reddit (pip install mcp-server-reddit) as a subprocess MCP server
to fetch Reddit posts without any API keys, then calls Claude to semantically
classify buying intent rather than relying on keyword matching.

This replaces the three fragile keyword-matching collectors:
  - community.py        (reliability 0.62)
  - reddit_collector.py (reliability 0.65)
  - serper_reddit.py    (reliability 0.65)

With a single LLM-classified collector:
  - reddit_mcp (target reliability 0.80)

Source name: reddit_mcp
Reliability: 0.80

Setup:
  pip install mcp-server-reddit
  Set SIGNALS_CLAUDE_API_KEY in environment (shared with existing LLM research).
"""

from __future__ import annotations

import asyncio
import json
import logging
import socket
import subprocess
import sys
import time
from contextlib import asynccontextmanager
from typing import Any

import httpx

from src import db
from src.models import SignalObservation
from src.settings import Settings
from src.utils import stable_hash, utc_now_iso

logger = logging.getLogger(__name__)

SOURCE_NAME = "reddit_mcp"
SOURCE_RELIABILITY = 0.80

# Subreddits most relevant to DevOps / Platform Eng / FinOps buying signals
TARGET_SUBREDDITS = [
    "devops",
    "kubernetes",
    "platformengineering",
    "finops",
    "aws",
    "googlecloud",
    "azure",
    "sre",
    "terraform",
    "cloudcomputing",
]

# Intent categories Claude will classify posts into
INTENT_CATEGORIES = {
    "active_evaluation": 0.85,  # "We're evaluating tools for X" → high confidence
    "pain_signal": 0.75,  # "Our infra is killing us / struggling with X" → medium-high
    "migration_signal": 0.80,  # "Moving from X to Y" → high
    "hiring_signal": 0.65,  # "We're hiring DevOps/SRE/FinOps" → medium
    "vendor_mention": 0.60,  # Mentions a relevant vendor in context → medium
    "passing_mention": None,  # No buying intent → skip
}

# Maps intent category → signal_code in our registry
INTENT_TO_SIGNAL = {
    "active_evaluation": "tech_evaluation_intent",
    "pain_signal": "infrastructure_pain",
    "migration_signal": "cloud_migration_intent",
    "hiring_signal": "hiring_devops",
    "vendor_mention": "vendor_evaluation",
}

_CLASSIFY_PROMPT = """\
You are a buying-signal analyst for enterprise infrastructure software (DevOps, Platform Engineering, FinOps).

Target company: {company_name} (domain: {domain})

Analyze the Reddit post below. First determine if the post is meaningfully relevant to the target company,
then classify its buying intent.

Return a JSON object with exactly these fields:
{{
  "relevant": <true | false>,
  "relevance_reason": "<one sentence why this post is or is not relevant to the target company>",
  "intent": "<one of: active_evaluation | pain_signal | migration_signal | hiring_signal | vendor_mention | passing_mention>",
  "confidence": <float 0.0-1.0>,
  "evidence_sentence": "<1-2 sentence summary of the key signal, max 200 chars>",
  "signal_code": "<from: tech_evaluation_intent | infrastructure_pain | cloud_migration_intent | hiring_devops | vendor_evaluation | null>"
}}

Relevance rules:
- Set relevant=true ONLY if the post is clearly about the target company or authored by someone at the company
- A post is relevant if it explicitly names the company, its products, its domain, or its employees
- A post discussing the same industry/problem space but NOT mentioning the target company is NOT relevant
- If relevant=false, set intent to "passing_mention" and signal_code to null

Intent definitions (only apply when relevant=true):
- active_evaluation: Poster is actively comparing or trialling tools for DevOps/Platform/FinOps use case
- pain_signal: Poster describes a specific infrastructure, cost, or reliability problem they're trying to solve
- migration_signal: Poster is moving from one platform/tool to another (e.g. Jenkins → GitHub Actions, AWS → GCP)
- hiring_signal: Poster mentions their company is hiring for DevOps, SRE, Platform Eng, or FinOps roles
- vendor_mention: Post meaningfully compares vendors or asks for vendor recommendations
- passing_mention: The post mentions tech terms but has no buying intent or clear pain — skip this one

Post title: {title}
Post body: {body}
Subreddit: r/{subreddit}
"""


def _find_free_port() -> int:
    """Find an available TCP port by binding to port 0."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class _MCPServerProcess:
    """Thin wrapper around the mcp-server-reddit subprocess."""

    _MCP_STARTUP_TIMEOUT = 10  # seconds to wait for server readiness

    def __init__(self) -> None:
        self._proc: subprocess.Popen | None = None
        self._port: int = 0
        self._base_url: str = ""

    async def start(self) -> bool:
        """Start mcp-server-reddit as a local HTTP server. Returns True if started."""
        try:
            self._port = _find_free_port()
            self._base_url = f"http://127.0.0.1:{self._port}"
            self._proc = subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "mcp_server_reddit",
                    "--transport",
                    "streamable-http",
                    "--port",
                    str(self._port),
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            # Wait for server to become ready (health check loop)
            ready = await self._wait_until_ready()
            if not ready:
                self.stop()
                return False
            return True
        except FileNotFoundError:
            logger.warning("mcp-server-reddit not installed. Install with: pip install mcp-server-reddit")
            return False
        except Exception as exc:
            logger.warning("Failed to start reddit_mcp server: %s", exc)
            return False

    async def _wait_until_ready(self) -> bool:
        """Poll the server until it responds or timeout expires."""
        deadline = time.monotonic() + self._MCP_STARTUP_TIMEOUT
        async with httpx.AsyncClient() as client:
            while time.monotonic() < deadline:
                if self._proc and self._proc.poll() is not None:
                    stderr = self._proc.stderr.read().decode(errors="replace") if self._proc.stderr else ""
                    logger.warning("reddit_mcp server exited during startup: %s", stderr[:300])
                    return False
                try:
                    resp = await client.get(f"{self._base_url}/mcp", timeout=2)
                    if resp.status_code < 500:
                        return True
                except (httpx.ConnectError, httpx.TimeoutException):
                    pass
                await asyncio.sleep(0.5)
        logger.warning("reddit_mcp server did not become ready within %ds", self._MCP_STARTUP_TIMEOUT)
        return False

    def stop(self) -> None:
        if self._proc and self._proc.poll() is None:
            self._proc.terminate()
            try:
                self._proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self._proc.kill()
        self._proc = None

    async def call_tool(self, tool_name: str, arguments: dict, client: httpx.AsyncClient) -> Any:
        """Call an MCP tool via the streamable-HTTP transport."""
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {"name": tool_name, "arguments": arguments},
        }
        resp = await client.post(
            f"{self._base_url}/mcp",
            json=payload,
            timeout=30,
            headers={"Content-Type": "application/json"},
        )
        resp.raise_for_status()
        data = resp.json()
        if "error" in data:
            raise RuntimeError(f"MCP tool error: {data['error']}")
        result = data.get("result", {})
        # mcp-server-reddit returns content as a list of text blocks
        content = result.get("content", [])
        if content and isinstance(content[0], dict):
            raw = content[0].get("text", "{}")
            return json.loads(raw)
        return result


async def _classify_with_claude(
    post: dict,
    subreddit: str,
    api_key: str,
    client: httpx.AsyncClient,
    company_name: str = "",
    domain: str = "",
) -> dict | None:
    """Use Claude Haiku to semantically classify a Reddit post and check company relevance."""
    title = str(post.get("title", ""))[:300]
    body = str(post.get("selftext", "") or post.get("body", ""))[:800]
    if not title and not body:
        return None

    prompt = _CLASSIFY_PROMPT.format(
        title=title,
        body=body,
        subreddit=subreddit,
        company_name=company_name or "unknown",
        domain=domain or "unknown",
    )
    try:
        resp = await client.post(
            "https://api.anthropic.com/v1/messages",
            json={
                "model": "claude-haiku-4-5-20251001",
                "max_tokens": 256,
                "messages": [{"role": "user", "content": prompt}],
            },
            headers={
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            timeout=20,
        )
        resp.raise_for_status()
        text = resp.json()["content"][0]["text"].strip()
        # Strip markdown fences if present
        if text.startswith("```"):
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
        return json.loads(text)
    except (json.JSONDecodeError, KeyError, httpx.HTTPError) as exc:
        logger.debug("Claude classification failed for post '%s': %s", title[:60], exc)
        return None


def _make_observation(
    account_id: str,
    classification: dict,
    post: dict,
    subreddit: str,
    source_reliability: float,
) -> SignalObservation | None:
    # Skip posts Claude determined are not relevant to the target company
    if not classification.get("relevant", True):
        return None
    intent = classification.get("intent", "passing_mention")
    if intent == "passing_mention" or intent not in INTENT_TO_SIGNAL:
        return None

    signal_code = classification.get("signal_code") or INTENT_TO_SIGNAL.get(intent)
    if not signal_code:
        return None

    confidence = float(classification.get("confidence", INTENT_CATEGORIES.get(intent, 0.65)))
    evidence_text = str(classification.get("evidence_sentence", ""))[:500]
    post_url = str(post.get("url", "") or post.get("permalink", ""))
    if post_url and not post_url.startswith("http"):
        post_url = f"https://reddit.com{post_url}"

    obs_id = stable_hash(
        {"account_id": account_id, "signal_code": signal_code, "source": SOURCE_NAME, "url": post_url},
        prefix="obs",
    )
    return SignalObservation(
        obs_id=obs_id,
        account_id=account_id,
        signal_code=signal_code,
        product="shared",
        source=SOURCE_NAME,
        observed_at=utc_now_iso(),
        evidence_url=post_url or f"https://reddit.com/r/{subreddit}",
        evidence_text=evidence_text,
        confidence=max(0.0, min(1.0, confidence)),
        source_reliability=max(0.0, min(1.0, source_reliability)),
        raw_payload_hash=stable_hash(post, prefix="raw"),
    )


def _build_match_terms(company_name: str, domain: str) -> list[str]:
    """Build a list of lowercase search terms for fuzzy pre-filtering.

    Generates: full company name, domain, domain root (without TLD),
    and individual name tokens >=3 chars (e.g. "HashiCorp" → ["hashicorp", "hashi"]).
    """
    terms: list[str] = []
    name_lower = company_name.lower().strip()
    if name_lower:
        terms.append(name_lower)

    domain_lower = domain.lower().replace("www.", "").strip()
    if domain_lower:
        terms.append(domain_lower)
        # Domain root without TLD: "hashicorp.com" → "hashicorp"
        root = domain_lower.split(".")[0] if "." in domain_lower else ""
        if root and len(root) >= 3 and root != name_lower:
            terms.append(root)

    # Individual name tokens >=3 chars (skip noise words)
    _NOISE = {"the", "inc", "ltd", "llc", "corp", "group", "company", "and", "for"}
    for token in name_lower.split():
        clean = token.strip(".,()-")
        if len(clean) >= 3 and clean not in _NOISE and clean != name_lower:
            terms.append(clean)

    return terms


def _post_matches_company(post: dict, company_name: str, domain: str) -> bool:
    """Check if a post mentions the company by name, domain, or common variants."""
    terms = _build_match_terms(company_name, domain)
    if not terms:
        return False
    title = str(post.get("title", "")).lower()
    body = str(post.get("selftext", "") or post.get("body", "")).lower()
    url = str(post.get("url", "")).lower()
    text = f"{title} {body} {url}"
    return any(term in text for term in terms)


async def collect(
    conn,
    settings: Settings,
    lexicon_by_source: dict | None = None,
    source_reliability_dict: dict | None = None,
) -> dict[str, int]:
    """Collect Reddit signals using mcp-server-reddit + Claude semantic classification.

    Falls back to empty result if mcp-server-reddit is not installed or
    no Claude API key is configured — never crashes the pipeline.
    """
    if not settings.claude_api_key:
        logger.info("reddit_mcp: no SIGNALS_CLAUDE_API_KEY configured, skipping")
        return {"inserted": 0, "seen": 0}

    source_reliability = (source_reliability_dict or {}).get(SOURCE_NAME, SOURCE_RELIABILITY)
    run_date = utc_now_iso()[:10]

    server = _MCPServerProcess()
    started = await server.start()
    if not started:
        logger.info("reddit_mcp: server not available, skipping")
        return {"inserted": 0, "seen": 0}

    seen = 0
    inserted = 0

    try:
        async with httpx.AsyncClient() as client:
            accounts = db.select_accounts_for_live_crawl(conn, settings, run_date)
            if not accounts:
                logger.info("reddit_mcp: no accounts to scan")
                return {"inserted": 0, "seen": 0}

            # Process accounts in batches to avoid overloading Claude API
            for account in accounts[: settings.live_max_accounts]:
                account_id = str(account["account_id"])
                company_name = str(account.get("company_name", ""))
                domain = str(account.get("domain", ""))
                if not company_name and not domain:
                    continue

                # Check crawl dedup
                if db.was_crawled_today(conn, account_id, run_date, SOURCE_NAME):
                    continue

                # Search each subreddit for this company
                for subreddit in TARGET_SUBREDDITS[:5]:  # Top 5 most relevant
                    try:
                        posts_data = await server.call_tool(
                            "get_subreddit_new_posts",
                            {"subreddit": subreddit, "limit": 20},
                            client,
                        )
                        posts = posts_data if isinstance(posts_data, list) else []

                        # Pre-filter: match company name, domain, or domain root (without TLD)
                        # to avoid wasting Claude tokens on clearly unrelated posts.
                        # Uses multiple name variants to catch abbreviations and common forms.
                        candidate_posts = [p for p in posts if _post_matches_company(p, company_name, domain)]

                        for post in candidate_posts[:3]:  # Max 3 posts per subreddit per account
                            seen += 1
                            classification = await _classify_with_claude(
                                post,
                                subreddit,
                                settings.claude_api_key,
                                client,
                                company_name=company_name,
                                domain=domain,
                            )
                            if not classification:
                                continue
                            obs = _make_observation(account_id, classification, post, subreddit, source_reliability)
                            if obs and db.insert_signal_observation(conn, obs):
                                inserted += 1

                        # Rate-limit Claude calls
                        if candidate_posts:
                            await asyncio.sleep(0.5)

                    except Exception as exc:
                        logger.debug("reddit_mcp subreddit=%s account=%s error: %s", subreddit, account_id, exc)
                        continue

                # Also do a cross-subreddit search for the company by name
                try:
                    search_data = await server.call_tool(
                        "get_subreddit_top_posts",
                        {"subreddit": "devops", "time_filter": "week", "limit": 25},
                        client,
                    )
                    posts = search_data if isinstance(search_data, list) else []
                    # Pre-filter by name/domain before sending to Claude
                    relevant = [p for p in posts if _post_matches_company(p, company_name, domain)]
                    for post in relevant[:2]:
                        seen += 1
                        classification = await _classify_with_claude(
                            post,
                            "devops",
                            settings.claude_api_key,
                            client,
                            company_name=company_name,
                            domain=domain,
                        )
                        if not classification:
                            continue
                        obs = _make_observation(account_id, classification, post, "devops", source_reliability)
                        if obs and db.insert_signal_observation(conn, obs):
                            inserted += 1
                except Exception as exc:
                    logger.debug("reddit_mcp cross-search account=%s error: %s", account_id, exc)

                # Mark crawled for today
                db.mark_crawled(conn, account_id, run_date, SOURCE_NAME)
                await asyncio.sleep(0.1)  # gentle pacing

    finally:
        server.stop()

    logger.info("reddit_mcp: seen=%d inserted=%d", seen, inserted)
    return {"inserted": inserted, "seen": seen}
