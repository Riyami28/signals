"""Google + LLM firmographic enrichment collector.

Fetches company firmographic data (employees, revenue, HQ, industry, type)
from Google search snippets via Serper, then extracts structured fields
using MiniMax LLM. Merges results into company_research.enrichment_json.

Cost per account: 1 Serper call + 1 MiniMax call (~3s total).
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from typing import Any

import httpx

from src import db
from src.settings import Settings

logger = logging.getLogger(__name__)

SERPER_SEARCH_URL = "https://google.serper.dev/search"
MINIMAX_URL = "https://api.minimax.io/v1/chat/completions"
SOURCE_NAME = "firmographic_google"

# ─── LLM prompt ──────────────────────────────────────────────────────

_SYSTEM_PROMPT = "You extract structured company data from Google search snippets. Return ONLY valid JSON. Never guess — use null for unknown fields."

_USER_PROMPT_TEMPLATE = """Extract firmographic data for "{company}" from these Google search snippets.

Return ONLY a JSON object with these fields (use null if not confidently found):
- employee_count: integer or null (e.g. 306000)
- employee_range: string or null ("1-50", "51-200", "201-500", "501-1000", "1001-5000", "5001-10000", "10001+")
- revenue: string or null (e.g. "$92.4 billion")
- revenue_range: string or null ("$0-$1M", "$1M-$10M", "$10M-$50M", "$50M-$100M", "$100M-$500M", "$500M-$1B", "$1B-$10B", "$10B+")
- headquarters: string or null (e.g. "Purchase, New York, USA")
- city: string or null
- state: string or null
- country: string or null
- industry: string or null (e.g. "Food & Beverage")
- sub_industry: string or null (e.g. "Snacks & Beverages")
- company_type: "Public" or "Private" or null
- founded_year: integer or null (e.g. 1965)

IMPORTANT: Only include data you are confident about from the snippets. Use null for anything uncertain.

Snippets:
{snippets}

JSON:"""


# ─── Serper fetch ────────────────────────────────────────────────────

async def _fetch_snippets(
    client: httpx.AsyncClient,
    api_key: str,
    company_name: str,
) -> str:
    """Search Google for company info and return combined snippet text."""
    try:
        resp = await client.post(
            SERPER_SEARCH_URL,
            json={
                "q": f'"{company_name}" company employees revenue headquarters industry',
                "num": 8,
            },
            headers={
                "X-API-KEY": api_key,
                "Content-Type": "application/json",
            },
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()

        lines = []
        for item in data.get("organic", [])[:6]:
            title = item.get("title", "")
            snippet = item.get("snippet", "")
            if title or snippet:
                lines.append(f"- {title}: {snippet}")

        return "\n".join(lines)
    except Exception as exc:
        logger.warning("firmographic_serper_error company=%s error=%s", company_name, exc)
        return ""


# ─── MiniMax LLM extraction ─────────────────────────────────────────

async def _extract_with_llm(
    client: httpx.AsyncClient,
    minimax_key: str,
    minimax_model: str,
    company_name: str,
    snippets_text: str,
) -> dict[str, Any] | None:
    """Use MiniMax LLM to extract structured firmographic data from snippets."""
    if not snippets_text.strip():
        return None

    user_prompt = _USER_PROMPT_TEMPLATE.format(
        company=company_name,
        snippets=snippets_text[:3000],  # Cap snippet length
    )

    try:
        resp = await client.post(
            MINIMAX_URL,
            json={
                "model": minimax_model,
                "max_tokens": 1024,
                "temperature": 0.1,
                "messages": [
                    {"role": "system", "content": _SYSTEM_PROMPT},
                    {"role": "user", "content": user_prompt},
                ],
            },
            headers={
                "Authorization": f"Bearer {minimax_key}",
                "Content-Type": "application/json",
            },
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()

        text = (
            data.get("choices", [{}])[0]
            .get("message", {})
            .get("content", "")
        )

        # Strip <think>...</think> reasoning blocks (MiniMax sometimes adds these)
        text = re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL).strip()

        # Extract JSON from markdown code blocks if present
        if "```" in text:
            parts = text.split("```")
            for part in parts[1:]:
                cleaned = part.replace("json", "", 1).strip()
                if cleaned.startswith("{"):
                    text = cleaned
                    break

        # Find JSON object in response
        start = text.find("{")
        end = text.rfind("}") + 1
        if start >= 0 and end > start:
            return json.loads(text[start:end])

        # Fallback: try to repair truncated JSON (missing closing brace)
        if start >= 0 and end <= start:
            truncated = text[start:]
            # Try adding closing brace
            try:
                return json.loads(truncated + "}")
            except json.JSONDecodeError:
                try:
                    return json.loads(truncated + '"}')
                except json.JSONDecodeError:
                    pass

        logger.warning("firmographic_llm_no_json company=%s response=%s", company_name, text[:200])
        return None

    except json.JSONDecodeError as exc:
        logger.warning("firmographic_llm_json_error company=%s error=%s", company_name, exc)
        return None
    except Exception as exc:
        logger.warning("firmographic_llm_error company=%s error=%s", company_name, exc)
        return None


# ─── Merge into enrichment_json ──────────────────────────────────────

_FIRMOGRAPHIC_FIELDS = frozenset([
    "employee_count", "employee_range", "revenue", "revenue_range",
    "headquarters", "city", "state", "country",
    "industry", "sub_industry", "company_type", "founded_year",
])


def _merge_enrichment(conn, account_id: str, new_fields: dict[str, Any]) -> bool:
    """Merge firmographic fields into existing enrichment_json.

    Only writes fields that are non-null and not already present.
    Returns True if any new data was written.
    """
    # Get existing research record
    existing = db.get_company_research(conn, account_id)
    current: dict[str, Any] = {}
    if existing:
        try:
            current = json.loads(existing.get("enrichment_json", "{}") or "{}")
        except (json.JSONDecodeError, TypeError):
            current = {}

    # Merge: only add fields that are missing or empty
    updated = False
    for key, val in new_fields.items():
        if key not in _FIRMOGRAPHIC_FIELDS:
            continue
        if val is None:
            continue
        if key not in current or not current[key]:
            current[key] = val
            updated = True

    if not updated:
        return False

    # Mark source of firmographic data
    current["firmographic_source"] = "google_llm"

    enrichment_str = json.dumps(current, ensure_ascii=False)

    # Upsert into company_research
    db.upsert_company_research(
        conn,
        account_id,
        research_brief=existing.get("research_brief") if existing else None,
        research_profile=existing.get("research_profile") if existing else None,
        enrichment_json=enrichment_str,
        research_status=existing.get("research_status", "partial") if existing else "partial",
        model_used=existing.get("model_used") if existing else None,
        prompt_hash=existing.get("prompt_hash") if existing else None,
    )
    return True


def _is_already_enriched(conn, account_id: str) -> bool:
    """Check if account already has firmographic data."""
    existing = db.get_company_research(conn, account_id)
    if not existing:
        return False
    try:
        enr = json.loads(existing.get("enrichment_json", "{}") or "{}")
    except (json.JSONDecodeError, TypeError):
        return False

    # Consider enriched if both employee_range and industry are present
    return bool(enr.get("employee_range")) and bool(enr.get("industry"))


# ─── Per-account collection ──────────────────────────────────────────

async def _collect_one_account(
    conn,
    client: httpx.AsyncClient,
    serper_key: str,
    minimax_key: str,
    minimax_model: str,
    account: dict[str, Any],
) -> str:
    """Enrich one account with firmographic data.

    Returns: "enriched", "skipped", or "error"
    """
    account_id = str(account["account_id"])
    company_name = str(account.get("company_name", ""))
    domain = str(account.get("domain", ""))

    if not company_name:
        return "skipped"

    # Skip if already enriched
    if _is_already_enriched(conn, account_id):
        return "skipped"

    # Check crawl checkpoint (don't re-process daily)
    endpoint = f"firmographic:{domain}"
    if db.was_crawled_today(conn, source=SOURCE_NAME, account_id=account_id, endpoint=endpoint):
        return "skipped"

    # Step 1: Fetch Google snippets
    snippets = await _fetch_snippets(client, serper_key, company_name)
    if not snippets:
        db.record_crawl_attempt(
            conn, source=SOURCE_NAME, account_id=account_id,
            endpoint=endpoint, status="exception",
            error_summary="no_snippets", commit=False,
        )
        db.mark_crawled(conn, source=SOURCE_NAME, account_id=account_id, endpoint=endpoint, commit=False)
        return "error"

    # Step 2: Extract via LLM
    extracted = await _extract_with_llm(client, minimax_key, minimax_model, company_name, snippets)
    if not extracted:
        db.record_crawl_attempt(
            conn, source=SOURCE_NAME, account_id=account_id,
            endpoint=endpoint, status="exception",
            error_summary="llm_extraction_failed", commit=False,
        )
        db.mark_crawled(conn, source=SOURCE_NAME, account_id=account_id, endpoint=endpoint, commit=False)
        return "error"

    # Step 3: Merge into enrichment_json
    wrote = _merge_enrichment(conn, account_id, extracted)

    db.record_crawl_attempt(
        conn, source=SOURCE_NAME, account_id=account_id,
        endpoint=endpoint, status="success", error_summary="", commit=False,
    )
    db.mark_crawled(conn, source=SOURCE_NAME, account_id=account_id, endpoint=endpoint, commit=False)

    return "enriched" if wrote else "skipped"


# ─── Main entry point ────────────────────────────────────────────────

async def collect(
    conn,
    settings: Settings,
    account_ids: list[str] | None = None,
    **kwargs,
) -> dict[str, int]:
    """Enrich accounts with firmographic data from Google + LLM.

    Returns:
        {"enriched": N, "skipped": N, "errors": N, "accounts_processed": N}
    """
    serper_key = settings.serper_api_key
    minimax_key = settings.minimax_api_key
    minimax_model = settings.minimax_model

    if not serper_key:
        logger.warning("serper_api_key is empty, skipping firmographic collection")
        return {"enriched": 0, "skipped": 0, "errors": 0, "accounts_processed": 0}

    if not minimax_key:
        logger.warning("minimax_api_key is empty, skipping firmographic collection")
        return {"enriched": 0, "skipped": 0, "errors": 0, "accounts_processed": 0}

    # Fetch accounts
    cursor = conn.cursor()
    if account_ids:
        cursor.execute(
            "SELECT account_id, company_name, domain FROM signals.accounts WHERE account_id = ANY(%s)",
            (account_ids,),
        )
    else:
        cursor.execute(
            "SELECT account_id, company_name, domain FROM signals.accounts ORDER BY company_name LIMIT %s",
            (settings.live_max_accounts,),
        )
    accounts = [dict(row) for row in cursor.fetchall()]

    if not accounts:
        return {"enriched": 0, "skipped": 0, "errors": 0, "accounts_processed": 0}

    logger.info("firmographic starting accounts=%d", len(accounts))
    t0 = time.monotonic()

    enriched = 0
    skipped = 0
    errors = 0

    # Sequential processing — 2 API calls per account, be gentle on rate limits
    async with httpx.AsyncClient() as client:
        for i, account in enumerate(accounts, 1):
            try:
                result = await _collect_one_account(
                    conn, client, serper_key, minimax_key, minimax_model, account,
                )
                if result == "enriched":
                    enriched += 1
                elif result == "skipped":
                    skipped += 1
                else:
                    errors += 1

                # Commit every 20 accounts
                if i % 20 == 0:
                    conn.commit()
                    logger.info(
                        "firmographic progress %d/%d enriched=%d skipped=%d errors=%d",
                        i, len(accounts), enriched, skipped, errors,
                    )

            except Exception as exc:
                logger.warning("firmographic_error account=%s error=%s", account.get("domain"), exc)
                errors += 1

            # Rate limit: ~3s per account (1 Serper + 1 MiniMax call)
            await asyncio.sleep(1.0)

    conn.commit()

    dt = time.monotonic() - t0
    logger.info(
        "firmographic done accounts=%d enriched=%d skipped=%d errors=%d duration=%.1fs",
        len(accounts), enriched, skipped, errors, dt,
    )

    return {
        "enriched": enriched,
        "skipped": skipped,
        "errors": errors,
        "accounts_processed": enriched + skipped + errors,
    }
