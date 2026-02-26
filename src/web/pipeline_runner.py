"""Async pipeline runner that emits SSE events."""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from datetime import date
from pathlib import Path

from src import db
from src.settings import load_settings

logger = logging.getLogger(__name__)

# In-memory event queues keyed by pipeline_run_id
ACTIVE_QUEUES: dict[str, asyncio.Queue] = {}


async def run_pipeline_async(account_ids: list[str], stages: list[str]) -> str:
    """Start pipeline in background thread, return run_id immediately."""
    run_id = f"prun_{uuid.uuid4().hex[:12]}"
    queue: asyncio.Queue = asyncio.Queue()
    ACTIVE_QUEUES[run_id] = queue

    loop = asyncio.get_event_loop()
    loop.create_task(_run_in_thread(run_id, account_ids, stages, queue))
    return run_id


async def _run_in_thread(run_id: str, account_ids: list[str], stages: list[str], queue: asyncio.Queue):
    """Run pipeline stages in a thread and emit events to the queue."""
    try:
        await asyncio.to_thread(_run_pipeline_sync, run_id, account_ids, stages, queue)
    except Exception as exc:
        await queue.put({"type": "error", "message": str(exc)})
    finally:
        await queue.put({"type": "done", "pipeline_run_id": run_id})


def _emit(queue: asyncio.Queue, event: dict):
    """Thread-safe emit to async queue."""
    try:
        queue.put_nowait(event)
    except Exception:
        pass


def _run_pipeline_sync(run_id: str, account_ids: list[str], stages: list[str], queue: asyncio.Queue):
    """Synchronous pipeline execution — runs in a thread."""
    settings = load_settings()
    conn = db.get_connection(settings.pg_dsn)
    db.init_db(conn)

    run_date_obj = date.today()
    run_date = run_date_obj.isoformat()
    score_run_id = None

    try:
        # Record pipeline run
        db.create_ui_pipeline_run(conn, account_ids, stages)

        # --- INGEST ---
        if "ingest" in stages:
            _emit(queue, {"type": "stage", "stage": "ingest", "status": "running", "message": "Collecting signals..."})
            t0 = time.monotonic()
            try:
                from src.collectors import community, first_party, jobs, news, technographics
                from src.scoring.rules import load_keyword_lexicon, load_source_registry
                from src.source_policy import load_source_execution_policy

                source_registry = load_source_registry(settings.source_registry_path)
                keyword_lexicon = load_keyword_lexicon(settings.keyword_lexicon_path)
                exec_policy = load_source_execution_policy(settings.source_execution_policy_path)

                def _collector_enabled(policy_key: str) -> bool:
                    policy = exec_policy.get(policy_key.strip().lower())
                    return bool(policy.enabled) if policy is not None else True

                total_inserted = 0

                # First-party signals
                _emit(queue, {"type": "log", "stage": "ingest", "message": "Ingesting first-party signals..."})
                if _collector_enabled("first_party_csv"):
                    fp_result = first_party.collect(conn, settings, keyword_lexicon, source_registry)
                    total_inserted += fp_result.get("inserted", 0)
                    _emit(
                        queue,
                        {
                            "type": "log",
                            "stage": "ingest",
                            "message": f"First-party: {fp_result.get('inserted', 0)} signals",
                        },
                    )

                # Jobs
                if _collector_enabled("jobs_pages"):
                    _emit(queue, {"type": "log", "stage": "ingest", "message": "Collecting job signals..."})
                    j_result = jobs.collect(conn, settings, keyword_lexicon, source_registry)
                    total_inserted += j_result.get("inserted", 0)
                    _emit(
                        queue,
                        {"type": "log", "stage": "ingest", "message": f"Jobs: {j_result.get('inserted', 0)} signals"},
                    )

                # News
                if _collector_enabled("news_rss"):
                    _emit(queue, {"type": "log", "stage": "ingest", "message": "Collecting news signals..."})
                    n_result = news.collect(conn, settings, keyword_lexicon, source_registry)
                    total_inserted += n_result.get("inserted", 0)
                    _emit(
                        queue,
                        {"type": "log", "stage": "ingest", "message": f"News: {n_result.get('inserted', 0)} signals"},
                    )

                # Technographics
                if _collector_enabled("technographics"):
                    _emit(queue, {"type": "log", "stage": "ingest", "message": "Collecting technographics signals..."})
                    t_result = technographics.collect(conn, settings, keyword_lexicon, source_registry)
                    total_inserted += t_result.get("inserted", 0)
                    _emit(
                        queue,
                        {
                            "type": "log",
                            "stage": "ingest",
                            "message": f"Technographics: {t_result.get('inserted', 0)} signals",
                        },
                    )

                dt = time.monotonic() - t0
                _emit(
                    queue,
                    {
                        "type": "stage",
                        "stage": "ingest",
                        "status": "completed",
                        "message": f"Ingested {total_inserted} signals in {dt:.1f}s",
                    },
                )
            except Exception as exc:
                _emit(queue, {"type": "stage", "stage": "ingest", "status": "failed", "message": str(exc)})
                logger.warning("ingest stage failed: %s", exc, exc_info=True)

        # --- SCORE ---
        if "score" in stages:
            _emit(
                queue, {"type": "stage", "stage": "score", "status": "running", "message": "Running scoring engine..."}
            )
            t0 = time.monotonic()
            try:
                from src.models import AccountScore
                from src.scoring.engine import run_scoring
                from src.scoring.rules import (
                    load_dimension_weights,
                    load_signal_rules,
                    load_source_registry,
                    load_thresholds,
                )

                signal_rules = load_signal_rules(settings.signal_registry_path)
                source_registry = load_source_registry(settings.source_registry_path)
                thresholds = load_thresholds(settings.thresholds_path)
                dimension_weights = load_dimension_weights(settings.dimension_weights_path)

                score_run_id = db.create_score_run(conn, run_date)
                observations = db.fetch_observations_for_scoring(conn, run_date)
                obs_list = [dict(row) for row in observations]
                _emit(queue, {"type": "log", "stage": "score", "message": f"Scoring {len(obs_list)} observations..."})

                result = run_scoring(
                    run_id=score_run_id,
                    run_date=run_date_obj,
                    observations=obs_list,
                    rules=signal_rules,
                    thresholds=thresholds,
                    source_reliability_defaults=source_registry,
                    dimension_weights=dimension_weights,
                    delta_lookup=None,
                )

                # Ensure all accounts have scores (including silent ones)
                existing_scores = {(s.account_id, s.product) for s in result.account_scores}
                account_rows = conn.execute("SELECT account_id FROM accounts").fetchall()
                for row in account_rows:
                    account_id = str(row["account_id"])
                    for product in ("zopdev", "zopday", "zopnight"):
                        if (account_id, product) in existing_scores:
                            continue
                        result.account_scores.append(
                            AccountScore(
                                run_id=score_run_id,
                                account_id=account_id,
                                product=product,
                                score=0.0,
                                tier="low",
                                top_reasons_json="[]",
                                delta_7d=0.0,
                                dimension_scores_json="{}",
                            )
                        )

                db.replace_run_scores(conn, score_run_id, result.component_scores, result.account_scores)
                db.finish_score_run(conn, score_run_id, status="completed", error_summary=None)

                high_count = sum(1 for s in result.account_scores if getattr(s, "tier", "") == "high")
                dt = time.monotonic() - t0
                _emit(
                    queue,
                    {
                        "type": "stage",
                        "stage": "score",
                        "status": "completed",
                        "message": f"Scored {len(result.account_scores)} rows, {high_count} high-tier in {dt:.1f}s",
                        "score_run_id": score_run_id,
                    },
                )
            except Exception as exc:
                _emit(queue, {"type": "stage", "stage": "score", "status": "failed", "message": str(exc)})
                logger.warning("score stage failed: %s", exc, exc_info=True)

        # --- RESEARCH ---
        if "research" in stages:
            # Research can run even without score_run_id (it uses latest scores)
            _emit(
                queue, {"type": "stage", "stage": "research", "status": "running", "message": "Running LLM research..."}
            )
            t0 = time.monotonic()
            try:
                from src.research.orchestrator import run_research_stage

                # If specific accounts selected, temporarily increase max_accounts
                if account_ids:
                    settings.research_max_accounts = max(settings.research_max_accounts, len(account_ids))

                effective_score_run_id = score_run_id
                if not effective_score_run_id:
                    # Find most recent score run
                    row = conn.execute("SELECT run_id FROM score_runs ORDER BY created_at DESC LIMIT 1").fetchone()
                    if row:
                        effective_score_run_id = row["run_id"]

                if effective_score_run_id:
                    result = run_research_stage(
                        conn, settings, run_date, effective_score_run_id, account_ids=account_ids or None
                    )
                    dt = time.monotonic() - t0
                    msg = f"Research: {result['completed']}/{result['attempted']} completed"
                    if result["failed"]:
                        msg += f", {result['failed']} failed"
                    if result["attempted"] == 0:
                        msg = "Research skipped (no API key or no accounts qualify)"
                    msg += f" in {dt:.1f}s"
                    _emit(queue, {"type": "stage", "stage": "research", "status": "completed", "message": msg})
                else:
                    _emit(
                        queue,
                        {
                            "type": "stage",
                            "stage": "research",
                            "status": "completed",
                            "message": "Research skipped (no score run available)",
                        },
                    )
            except Exception as exc:
                _emit(queue, {"type": "stage", "stage": "research", "status": "failed", "message": str(exc)})
                logger.warning("research stage failed: %s", exc, exc_info=True)

        # --- EXPORT ---
        if "export" in stages:
            effective_score_run_id = score_run_id
            if not effective_score_run_id:
                row = conn.execute("SELECT run_id FROM score_runs ORDER BY created_at DESC LIMIT 1").fetchone()
                if row:
                    effective_score_run_id = row["run_id"]

            if effective_score_run_id:
                _emit(
                    queue, {"type": "stage", "stage": "export", "status": "running", "message": "Exporting results..."}
                )
                t0 = time.monotonic()
                try:
                    from src.export import csv_exporter

                    out_path = settings.out_dir / f"sales_ready_{csv_exporter.date_suffix(run_date_obj)}.csv"
                    rows = csv_exporter.export_sales_ready(conn, effective_score_run_id, out_path)
                    dt = time.monotonic() - t0
                    _emit(
                        queue,
                        {
                            "type": "stage",
                            "stage": "export",
                            "status": "completed",
                            "message": f"Sales-ready CSV: {rows} rows exported in {dt:.1f}s",
                        },
                    )
                except Exception as exc:
                    _emit(queue, {"type": "stage", "stage": "export", "status": "failed", "message": str(exc)})
                    logger.warning("export stage failed: %s", exc, exc_info=True)
            else:
                _emit(
                    queue,
                    {
                        "type": "stage",
                        "stage": "export",
                        "status": "completed",
                        "message": "Export skipped (no score run available)",
                    },
                )

        # Finalize
        db.finish_ui_pipeline_run(conn, run_id, "completed", {"score_run_id": score_run_id})

    except Exception as exc:
        logger.error("pipeline run %s failed: %s", run_id, exc, exc_info=True)
        try:
            db.finish_ui_pipeline_run(conn, run_id, "failed", {"error": str(exc)})
        except Exception:
            pass
    finally:
        conn.close()
