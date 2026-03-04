"""Async pipeline runner that emits SSE events."""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from datetime import date

from src import db
from src.settings import load_settings

logger = logging.getLogger(__name__)

# In-memory event queues keyed by pipeline_run_id
ACTIVE_QUEUES: dict[str, asyncio.Queue] = {}


async def run_pipeline_async(account_ids: list[str], stages: list[str], batch_id: str = "") -> str:
    """Start pipeline in background thread, return run_id immediately."""
    run_id = f"prun_{uuid.uuid4().hex[:12]}"
    queue: asyncio.Queue = asyncio.Queue()
    ACTIVE_QUEUES[run_id] = queue

    loop = asyncio.get_event_loop()
    loop.create_task(_run_in_thread(run_id, account_ids, stages, queue, batch_id))
    return run_id


async def _run_in_thread(
    run_id: str, account_ids: list[str], stages: list[str], queue: asyncio.Queue, batch_id: str = ""
):
    """Run pipeline stages in a thread and emit events to the queue."""
    try:
        await asyncio.to_thread(_run_pipeline_sync, run_id, account_ids, stages, queue, batch_id)
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


def _run_pipeline_sync(
    run_id: str, account_ids: list[str], stages: list[str], queue: asyncio.Queue, batch_id: str = ""
):
    """Synchronous pipeline execution — runs in a thread."""
    settings = load_settings()
    conn = db.get_connection(settings.pg_dsn)
    # DB schema is initialized at app startup — no init_db() here to avoid locks

    run_date_obj = date.today()
    run_date = run_date_obj.isoformat()
    score_run_id = None

    try:
        # Record pipeline run
        db.create_ui_pipeline_run(conn, account_ids, stages)

        if batch_id:
            _emit(
                queue,
                {
                    "type": "log",
                    "stage": "setup",
                    "message": f"Processing batch {batch_id} with {len(account_ids)} accounts",
                },
            )

        # --- INGEST ---
        if "ingest" in stages:
            _emit(queue, {"type": "stage", "stage": "ingest", "status": "running", "message": "Collecting signals..."})
            t0 = time.monotonic()
            try:
                from src.scoring.rules import load_keyword_lexicon, load_source_registry
                from src.source_policy import load_source_execution_policy

                source_registry = load_source_registry(settings.source_registry_path)
                keyword_lexicon = load_keyword_lexicon(settings.keyword_lexicon_path)
                exec_policy = load_source_execution_policy(settings.source_execution_policy_path)

                def _collector_enabled(policy_key: str) -> bool:
                    policy = exec_policy.get(policy_key.strip().lower())
                    return bool(policy.enabled) if policy is not None else True

                import asyncio as _asyncio

                total_inserted = 0

                # First-party signals (sync — no HTTP, always fast)
                if _collector_enabled("first_party_csv"):
                    _emit(queue, {"type": "log", "stage": "ingest", "message": "Ingesting first-party signals..."})
                    from src.collectors import first_party

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

                # Legacy HTTP collectors (jobs, news, technographics) — run only if enabled
                # These crawl actual websites and are slow, so skip quickly if disabled.
                # SKIP for single-account runs: legacy collectors cannot filter by account_ids
                # and process ALL accounts, making them extremely slow for targeted runs.
                # The external collectors (serper_news, serper_jobs, website_techscan) already
                # cover the same signal types and support account_ids filtering.
                if account_ids:
                    _emit(
                        queue,
                        {
                            "type": "log",
                            "stage": "ingest",
                            "message": f"Skipping legacy collectors for targeted run ({len(account_ids)} accounts)",
                        },
                    )
                else:
                    legacy_collectors = [
                        ("jobs_pages", "jobs", "Job"),
                        ("news_rss", "news", "News"),
                        ("technographics", "technographics", "Technographics"),
                    ]
                    for policy_key, module_name, label in legacy_collectors:
                        if _collector_enabled(policy_key):
                            _emit(
                                queue, {"type": "log", "stage": "ingest", "message": f"Collecting {label} signals..."}
                            )
                            import importlib

                            mod = importlib.import_module(f"src.collectors.{module_name}")
                            c_result = _asyncio.run(mod.collect(conn, settings, keyword_lexicon, source_registry))
                            ins = c_result.get("inserted", 0)
                            total_inserted += ins
                            _emit(
                                queue,
                                {"type": "log", "stage": "ingest", "message": f"{label}: {ins} signals"},
                            )

                # --- ALL external collectors in PARALLEL ---
                # Serper (Google Search) + Website Tech Scan (zero API) + GNews (optional) + Reddit (community)
                serper_news_enabled = _collector_enabled("serper_news") and settings.serper_api_key
                serper_jobs_enabled = _collector_enabled("serper_jobs") and settings.serper_api_key
                techscan_enabled = _collector_enabled("website_techscan")
                gnews_enabled = _collector_enabled("gnews") and settings.gnews_api_key
                reddit_enabled = _collector_enabled("reddit_api")

                any_external = serper_news_enabled or serper_jobs_enabled or techscan_enabled or gnews_enabled or reddit_enabled

                if any_external:
                    active_sources = []
                    if serper_news_enabled:
                        active_sources.append("serper_news")
                    if serper_jobs_enabled:
                        active_sources.append("serper_jobs")
                    if techscan_enabled:
                        active_sources.append("website_techscan")
                    if gnews_enabled:
                        active_sources.append("gnews")
                    if reddit_enabled:
                        active_sources.append("reddit_api")

                    _emit(
                        queue,
                        {
                            "type": "log",
                            "stage": "ingest",
                            "message": f"Collecting from {len(active_sources)} sources in parallel: {', '.join(active_sources)}...",
                        },
                    )

                    # Build ALL lexicon variants once
                    all_news_lexicon = []
                    for source_key in ("news", "technographics", "community"):
                        all_news_lexicon.extend(r for r in keyword_lexicon.get(source_key, []) if r.get("keyword"))
                    jobs_lexicon = [r for r in keyword_lexicon.get("jobs", []) if r.get("keyword")]

                    async def _run_all_external():
                        """Run ALL external collectors concurrently."""
                        tasks = []
                        task_labels = []

                        # --- Serper collectors ---
                        if serper_news_enabled:
                            from src.collectors import serper_news

                            tasks.append(
                                serper_news.collect(
                                    conn,
                                    settings,
                                    lexicon_rows=all_news_lexicon,
                                    source_reliability=source_registry.get("serper_news", 0.85),
                                    account_ids=account_ids if account_ids else None,
                                )
                            )
                            task_labels.append("serper_news")

                        if serper_jobs_enabled:
                            from src.collectors import serper_jobs

                            tasks.append(
                                serper_jobs.collect(
                                    conn,
                                    settings,
                                    lexicon_rows=jobs_lexicon,
                                    source_reliability=source_registry.get("serper_jobs", 0.80),
                                    account_ids=account_ids if account_ids else None,
                                )
                            )
                            task_labels.append("serper_jobs")

                        # --- Website tech scanner (FREE — no API key needed) ---
                        if techscan_enabled:
                            from src.collectors import website_techscan

                            tasks.append(
                                website_techscan.collect(
                                    conn,
                                    settings,
                                    source_reliability=source_registry.get("website_techscan", 0.70),
                                    account_ids=account_ids if account_ids else None,
                                )
                            )
                            task_labels.append("website_techscan")

                        # --- GNews (optional, needs free gnews.io key) ---
                        if gnews_enabled:
                            from src.collectors import gnews_collector

                            tasks.append(
                                gnews_collector.collect(
                                    conn,
                                    settings,
                                    lexicon_rows=all_news_lexicon,
                                    source_reliability=source_registry.get("gnews", 0.78),
                                    account_ids=account_ids if account_ids else None,
                                )
                            )
                            task_labels.append("gnews")

                        # --- Reddit API (community signals from Reddit discussions) ---
                        reddit_enabled = _collector_enabled("reddit_api")
                        if reddit_enabled:
                            from src.collectors import reddit_collector

                            reddit_lexicon = [r for r in keyword_lexicon.get("community", []) if r.get("keyword")]
                            tasks.append(
                                reddit_collector.collect(
                                    conn,
                                    settings,
                                    lexicon_rows=reddit_lexicon,
                                    source_reliability=source_registry.get("reddit_api", 0.65),
                                    account_ids=account_ids if account_ids else None,
                                )
                            )
                            task_labels.append("reddit_api")

                        results = await asyncio.gather(*tasks, return_exceptions=True)
                        return list(zip(task_labels, results))

                    external_results = _asyncio.run(_run_all_external())

                    for label, result in external_results:
                        if isinstance(result, Exception):
                            logger.warning("collector_error collector=%s error=%s", label, result)
                            _emit(
                                queue,
                                {"type": "log", "stage": "ingest", "message": f"{label}: failed ({result})"},
                            )
                            continue
                        ins = result.get("inserted", 0)
                        accts = result.get("accounts_processed", 0)
                        total_inserted += ins
                        _emit(
                            queue,
                            {
                                "type": "log",
                                "stage": "ingest",
                                "message": f"{label}: {ins} signals from {accts} accounts",
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
                from src.scoring.engine import classify_velocity, run_scoring
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

                # Filter observations to target accounts (single-account or batch runs)
                if account_ids:
                    target_set = set(account_ids)
                    obs_list = [o for o in obs_list if o.get("account_id") in target_set]

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

                # Ensure all target accounts have scores
                existing_scores = {(s.account_id, s.product) for s in result.account_scores}

                if account_ids:
                    # Targeted run: only backfill selected accounts
                    target_account_ids = account_ids
                else:
                    # Full run: backfill all accounts
                    account_rows = conn.execute("SELECT account_id FROM accounts").fetchall()
                    target_account_ids = [str(row["account_id"]) for row in account_rows]

                for acct_id in target_account_ids:
                    for product in ("zopdev", "zopday", "zopnight"):
                        if (acct_id, product) in existing_scores:
                            continue
                        result.account_scores.append(
                            AccountScore(
                                run_id=score_run_id,
                                account_id=acct_id,
                                product=product,
                                score=0.0,
                                tier="low",
                                top_reasons_json="[]",
                                delta_7d=0.0,
                                dimension_scores_json="{}",
                            )
                        )

                # Batch velocity: 3 queries instead of 9000+
                _emit(queue, {"type": "log", "stage": "score", "message": "Computing velocity (batch)..."})
                velocity_cache = db.batch_get_velocity(conn, run_date)

                for score in result.account_scores:
                    pair = (score.account_id, score.product)
                    hist = velocity_cache.get(pair, {})
                    past_7 = hist.get("past_7")
                    past_14 = hist.get("past_14")
                    past_30 = hist.get("past_30")
                    v7 = round(score.score - past_7, 2) if past_7 is not None else 0.0
                    v14 = round(score.score - past_14, 2) if past_14 is not None else 0.0
                    v30 = round(score.score - past_30, 2) if past_30 is not None else 0.0
                    score.velocity_7d = v7
                    score.velocity_14d = v14
                    score.velocity_30d = v30
                    score.velocity_category = classify_velocity(v7)
                    score.delta_7d = v7

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
        result_meta = {"score_run_id": score_run_id}
        if batch_id:
            result_meta["batch_id"] = batch_id
            db.update_batch_status(conn, batch_id, "scored")
        db.finish_ui_pipeline_run(conn, run_id, "completed", result_meta)

    except Exception as exc:
        logger.error("pipeline run %s failed: %s", run_id, exc, exc_info=True)
        try:
            db.finish_ui_pipeline_run(conn, run_id, "failed", {"error": str(exc)})
            if batch_id:
                db.update_batch_status(conn, batch_id, "failed")
        except Exception:
            pass
        if batch_id:
            try:
                db.update_batch_status(conn, batch_id, "failed")
            except Exception:
                pass
    finally:
        conn.close()
