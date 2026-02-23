"""Research orchestrator — coordinates the full research flow per account."""

from __future__ import annotations

import json
import logging

from src import db
from src.research.client import ResearchClient
from src.research.enrichment import run_enrichment_waterfall
from src.research.parser import parse_extraction_response, parse_scoring_response
from src.research.prompts import build_extraction_prompt, build_scoring_prompt, prompt_hash

logger = logging.getLogger(__name__)


def run_research_stage(conn, settings, run_date: str, score_run_id: str) -> dict:
    """
    Main entry point. Called from pipeline after scoring.

    Returns summary dict: {attempted, completed, failed, skipped, total_input_tokens, total_output_tokens}
    """
    current_hash = prompt_hash()

    # Skip entirely if no API key.
    if not settings.claude_api_key:
        logger.warning("claude_api_key is empty, skipping research stage")
        return {
            "attempted": 0,
            "completed": 0,
            "failed": 0,
            "skipped": 0,
            "total_input_tokens": 0,
            "total_output_tokens": 0,
        }

    client = ResearchClient(
        api_key=settings.claude_api_key,
        model=settings.claude_model,
        timeout_seconds=settings.research_timeout_seconds,
    )

    research_run_id = db.create_research_run(conn, run_date, score_run_id)

    accounts = db.get_accounts_needing_research(
        conn,
        run_date=run_date,
        score_run_id=score_run_id,
        max_accounts=settings.research_max_accounts,
        min_tier="medium",
        stale_days=settings.research_stale_days,
        current_prompt_hash=current_hash,
    )

    attempted = 0
    completed = 0
    failed = 0
    skipped = 0
    total_input_tokens = 0
    total_output_tokens = 0

    for account in accounts:
        account_id = account["account_id"]
        attempted += 1

        try:
            # Step a: mark in-progress BEFORE any API call.
            db.mark_research_in_progress(conn, account_id)

            # Step b: load signal observations.
            signals = _load_signals(conn, account_id)

            # Step c: run waterfall enrichment.
            pre_enrichment = run_enrichment_waterfall(account.get("domain", ""), settings)

            # Step d: build extraction prompt.
            ext_system, ext_user = build_extraction_prompt(account, signals, pre_enrichment)

            # Step e: call Claude extraction pass.
            ext_response = client.research_company(ext_system, ext_user)
            total_input_tokens += ext_response.input_tokens
            total_output_tokens += ext_response.output_tokens

            # Step f: parse extraction response.
            ext_parsed = parse_extraction_response(ext_response.raw_text)
            if ext_parsed.parse_errors:
                logger.warning(
                    "extraction parse errors for account=%s: %s",
                    account_id,
                    ext_parsed.parse_errors,
                )

            research_brief = ext_parsed.research_brief
            enrichment_dict = _enrichment_to_dict(ext_parsed.enrichment, pre_enrichment)

            # Step g: build scoring prompt.
            score_system, score_user = build_scoring_prompt(account, research_brief)

            # Step h: call Claude scoring pass.
            score_response = client.research_company(score_system, score_user)
            total_input_tokens += score_response.input_tokens
            total_output_tokens += score_response.output_tokens

            # Step i: parse scoring response.
            score_parsed = parse_scoring_response(score_response.raw_text)
            if score_parsed.parse_errors:
                logger.warning(
                    "scoring parse errors for account=%s: %s",
                    account_id,
                    score_parsed.parse_errors,
                )

            # Step j: store results.
            conversation_starters = "\n".join(
                f"- {s}" for s in score_parsed.conversation_starters
            )
            profile = research_brief
            if conversation_starters:
                profile += "\n\n## Conversation Starters\n" + conversation_starters

            db.upsert_company_research(
                conn,
                account_id,
                research_brief=research_brief,
                research_profile=profile,
                enrichment_json=json.dumps(enrichment_dict, ensure_ascii=False),
                research_status="completed",
                model_used=ext_response.model,
                prompt_hash=current_hash,
            )

            contacts_dicts = [
                {
                    "first_name": c.first_name,
                    "last_name": c.last_name,
                    "title": c.title,
                    "email": c.email,
                    "linkedin_url": c.linkedin_url,
                    "management_level": c.management_level,
                    "year_joined": c.year_joined,
                }
                for c in score_parsed.contacts
            ]
            db.upsert_contacts(conn, account_id, contacts_dicts)

            completed += 1
            logger.info(
                "research completed account=%s tokens_in=%d tokens_out=%d",
                account_id,
                ext_response.input_tokens + score_response.input_tokens,
                ext_response.output_tokens + score_response.output_tokens,
            )

        except Exception as exc:
            failed += 1
            logger.warning(
                "research failed for account=%s: %s", account_id, exc, exc_info=True
            )
            # Store partial failure.
            try:
                db.upsert_company_research(
                    conn,
                    account_id,
                    research_status="failed",
                    prompt_hash=current_hash,
                )
            except Exception:
                logger.debug("failed to mark research as failed for account=%s", account_id, exc_info=True)

    db.finish_research_run(
        conn,
        research_run_id,
        status="completed" if failed == 0 else "failed",
        accounts_attempted=attempted,
        accounts_completed=completed,
        accounts_failed=failed,
        accounts_skipped=skipped,
    )

    return {
        "attempted": attempted,
        "completed": completed,
        "failed": failed,
        "skipped": skipped,
        "total_input_tokens": total_input_tokens,
        "total_output_tokens": total_output_tokens,
    }


def _load_signals(conn, account_id: str) -> list[dict]:
    """Load recent signal observations for an account."""
    rows = conn.execute(
        """
        SELECT signal_code, source, evidence_url, evidence_text
        FROM signal_observations
        WHERE account_id = %s
        ORDER BY observed_at DESC
        LIMIT 20
        """,
        (account_id,),
    ).fetchall()
    return [dict(r) for r in rows]


def _enrichment_to_dict(enrichment, pre_enrichment: dict | None) -> dict:
    """Merge LLM enrichment with pre-enrichment into a single dict."""
    result = dict(pre_enrichment or {})
    # Overlay LLM results for fields that are still empty.
    for fld in [
        "website", "industry", "sub_industry", "employee_range", "revenue_range",
        "company_linkedin_url", "city", "state", "country",
    ]:
        llm_val = getattr(enrichment, fld, "")
        if llm_val and fld not in result:
            result[fld] = llm_val
            conf = enrichment.confidences.get(fld, 0.7)
            result[f"{fld}_confidence"] = conf

    if enrichment.employees is not None and "employees" not in result:
        result["employees"] = enrichment.employees
        result["employees_confidence"] = enrichment.confidences.get("employees", 0.7)

    if enrichment.tech_stack and "tech_stack" not in result:
        result["tech_stack"] = enrichment.tech_stack
        result["tech_stack_confidence"] = enrichment.confidences.get("tech_stack", 0.7)

    return result
