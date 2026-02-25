from __future__ import annotations

from dataclasses import replace
from datetime import date
from datetime import datetime, timedelta, timezone
import json
import logging
import os
from pathlib import Path
import time
import uuid

import typer

from src.logging_config import configure_logging

from src import db
from src.collectors import community, first_party, jobs, news, technographics
from src.discovery import hunt as hunt_pipeline
from src.discovery import pipeline as discovery_pipeline
from src.discovery import watchlist_builder
from src.discovery.config import (
    classify_signal,
    load_account_profiles,
    load_discovery_blocklist,
    load_signal_classes,
)
from src.export import csv_exporter
from src.research.orchestrator import run_research_stage
from src.models import AccountScore
from src.notifier import send_alert
from src.reporting import calibration, icp_playbook, quality
from src.reporting.evals import OutputQualityBar, evaluate_run_output_quality
from src.reporting.improvement import run_threshold_self_improvement
from src.review.import_reviews import import_reviews_for_date, prepare_review_input_for_date
from src.scoring.engine import run_scoring
from src.scoring.rules import load_keyword_lexicon, load_signal_rules, load_source_registry, load_thresholds
from src.settings import Settings, load_settings
from src.source_policy import load_source_execution_policy
from src.sync.google_sheets import sync_outputs
from src.utils import ensure_project_directories, load_csv_rows, normalize_domain, parse_date, write_csv_rows

logger = logging.getLogger(__name__)

app = typer.Typer(add_completion=False, no_args_is_help=True)


@app.callback()
def _app_callback() -> None:
    """Signals pipeline — structured logging enabled at startup."""
    configure_logging(os.environ.get("LOG_LEVEL", "INFO"))

_RUN_DAILY_LOCK_NAME = "signals:run-daily"
_AUTONOMOUS_LOCK_NAME = "signals:run-autonomous-loop"
_RETRY_BACKOFF_SECONDS = [60, 300, 900]


class StageExecutionError(RuntimeError):
    def __init__(self, stage: str, duration_seconds: float, timed_out: bool, message: str):
        super().__init__(message)
        self.stage = stage
        self.duration_seconds = float(duration_seconds)
        self.timed_out = bool(timed_out)


def _run_with_watchdog(stage: str, timeout_seconds: int, fn):
    started = time.monotonic()
    try:
        result = fn()
    except Exception as exc:
        elapsed = time.monotonic() - started
        raise StageExecutionError(
            stage=stage,
            duration_seconds=elapsed,
            timed_out=False,
            message=f"{stage} failed: {str(exc)[:240]}",
        ) from exc
    elapsed = time.monotonic() - started
    if elapsed > float(timeout_seconds):
        raise StageExecutionError(
            stage=stage,
            duration_seconds=elapsed,
            timed_out=True,
            message=f"{stage} exceeded timeout_seconds={timeout_seconds}",
        )
    return result, elapsed


def _retry_due_iso(backoff_seconds: int) -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=max(1, int(backoff_seconds)))).replace(microsecond=0).isoformat()


def _enqueue_retry_task(
    conn,
    settings: Settings,
    task_type: str,
    payload: dict[str, object],
    reason: str,
) -> str:
    task_id = db.enqueue_retry_task(
        conn=conn,
        task_type=task_type,
        payload_json=json.dumps(payload, ensure_ascii=True, sort_keys=True),
        due_at=_retry_due_iso(_RETRY_BACKOFF_SECONDS[0]),
        max_attempts=settings.retry_attempt_limit,
        commit=True,
    )
    retry_depth = db.fetch_retry_depth(conn)
    if retry_depth >= settings.alert_retry_depth_threshold:
        send_alert(
            settings,
            title="Retry depth threshold exceeded",
            body=f"retry_depth={retry_depth} threshold={settings.alert_retry_depth_threshold} task_id={task_id}",
            severity="warn",
        )
    if reason:
        send_alert(
            settings,
            title="Retry task enqueued",
            body=f"task_id={task_id} task_type={task_type} reason={reason[:300]}",
            severity="warn",
        )
    return task_id


def _persist_ops_metrics(conn, settings: Settings, run_date: date) -> dict[str, int | float | str]:
    run_date_str = run_date.isoformat()
    queue_path = settings.out_dir / f"discovery_queue_{run_date.strftime('%Y%m%d')}.csv"
    crm_path = settings.out_dir / f"crm_candidates_{run_date.strftime('%Y%m%d')}.csv"
    queue_rows = load_csv_rows(queue_path)
    crm_rows = load_csv_rows(crm_path)

    ingest_lag = db.fetch_latest_event_ingest_lag_seconds(conn, run_date_str)
    retry_depth = db.fetch_retry_depth(conn)
    retry_queue_size = db.fetch_retry_queue_size(conn)
    quarantine_size = db.fetch_quarantine_size(conn)
    handoff_success_rate = round((len(crm_rows) / len(queue_rows)) if queue_rows else 0.0, 4)

    precision_rows = db.fetch_precision_by_band(conn, run_date_str, lookback_days=settings.ops_metrics_lookback_days)
    lock_events = db.fetch_lock_event_counts(conn, lookback_hours=24)
    lock_busy_24h = int(lock_events.get("busy", 0))
    lock_release_missed_24h = int(lock_events.get("release_missed", 0))
    precision_by_band: dict[str, tuple[float, int]] = {}
    for row in precision_rows:
        band = str(row.get("band", "") or "").strip().lower()
        if not band:
            continue
        precision_by_band[band] = (
            round(float(row.get("approved_rate", 0.0) or 0.0), 4),
            int(row.get("sample_size", 0) or 0),
        )

    high_precision, high_sample = precision_by_band.get("high", (0.0, 0))
    medium_precision, medium_sample = precision_by_band.get("medium", (0.0, 0))

    metric_rows = [
        {"metric": "ingest_lag_seconds", "value": round(float(ingest_lag or 0.0), 2), "meta_json": "{}"},
        {"metric": "handoff_success_rate", "value": handoff_success_rate, "meta_json": "{}"},
        {"metric": "retry_depth", "value": float(retry_depth), "meta_json": "{}"},
        {"metric": "retry_queue_size", "value": float(retry_queue_size), "meta_json": "{}"},
        {"metric": "quarantine_size", "value": float(quarantine_size), "meta_json": "{}"},
        {"metric": "lock_busy_24h", "value": float(lock_busy_24h), "meta_json": "{}"},
        {"metric": "lock_release_missed_24h", "value": float(lock_release_missed_24h), "meta_json": "{}"},
        {
            "metric": "precision_high_band",
            "value": high_precision,
            "meta_json": json.dumps({"sample_size": high_sample}, ensure_ascii=True),
        },
        {
            "metric": "precision_medium_band",
            "value": medium_precision,
            "meta_json": json.dumps({"sample_size": medium_sample}, ensure_ascii=True),
        },
    ]

    db.replace_ops_metrics(conn, run_date_str, metric_rows)
    ops_count = csv_exporter.export_ops_metrics(conn, run_date_str, settings.out_dir / f"ops_metrics_{run_date.strftime('%Y%m%d')}.csv")

    if high_sample > 0 and high_precision < settings.alert_min_high_precision:
        send_alert(
            settings,
            title="High-band precision degraded",
            body=(
                f"run_date={run_date_str} high_precision={high_precision} "
                f"threshold={settings.alert_min_high_precision} sample_size={high_sample}"
            ),
            severity="warn",
        )
    if medium_sample > 0 and medium_precision < settings.alert_min_medium_precision:
        send_alert(
            settings,
            title="Medium-band precision degraded",
            body=(
                f"run_date={run_date_str} medium_precision={medium_precision} "
                f"threshold={settings.alert_min_medium_precision} sample_size={medium_sample}"
            ),
            severity="warn",
        )

    return {
        "ops_metrics_rows": int(ops_count),
        "retry_depth": int(retry_depth),
        "retry_queue_size": int(retry_queue_size),
        "quarantine_size": int(quarantine_size),
        "lock_busy_24h": int(lock_busy_24h),
        "lock_release_missed_24h": int(lock_release_missed_24h),
        "handoff_success_rate": float(handoff_success_rate),
    }


def _bootstrap(settings: Settings | None = None):
    local_settings = settings or load_settings()
    ensure_project_directories(
        [
            local_settings.project_root,
            local_settings.config_dir,
            local_settings.data_dir,
            local_settings.raw_dir,
            local_settings.out_dir,
        ]
    )
    conn = db.get_connection(local_settings.pg_dsn)
    db.init_db(conn)
    seeded_base = db.seed_accounts(conn, local_settings.seed_accounts_path)
    seeded_watchlist = db.seed_accounts(conn, local_settings.watchlist_accounts_path)
    seeded = seeded_base + seeded_watchlist
    return local_settings, conn, seeded


def _collect_all(conn, settings: Settings) -> dict[str, dict[str, int]]:
    lexicon = load_keyword_lexicon(settings.keyword_lexicon_path)
    source_reliability = load_source_registry(settings.source_registry_path)
    execution_policy = load_source_execution_policy(settings.source_execution_policy_path)

    def _collector_enabled(policy_key: str) -> bool:
        policy = execution_policy.get(policy_key.strip().lower())
        return bool(policy.enabled) if policy is not None else True

    results: dict[str, dict[str, int]] = {}
    results["jobs"] = (
        jobs.collect(conn, settings, lexicon, source_reliability)
        if _collector_enabled("jobs_pages")
        else {"inserted": 0, "seen": 0}
    )
    results["news"] = (
        news.collect(conn, settings, lexicon, source_reliability)
        if _collector_enabled("news_rss")
        else {"inserted": 0, "seen": 0}
    )
    results["technographics"] = (
        technographics.collect(conn, settings, lexicon, source_reliability)
        if _collector_enabled("technographics")
        else {"inserted": 0, "seen": 0}
    )
    results["community"] = (
        community.collect(conn, settings, lexicon, source_reliability)
        if _collector_enabled("reddit_api")
        else {"inserted": 0, "seen": 0}
    )
    results["first_party"] = (
        first_party.collect(conn, settings, lexicon, source_reliability)
        if _collector_enabled("first_party_csv")
        else {"inserted": 0, "seen": 0}
    )
    return results


def _baseline_score_7d(conn, account_id: str, product: str, run_date: str) -> float | None:
    cur = conn.execute(
        """
        SELECT s.score
        FROM account_scores s
        JOIN score_runs r ON r.run_id = s.run_id
        WHERE s.account_id = %s
          AND s.product = %s
          AND r.run_date::date <= (%s::date - INTERVAL '7 day')
        ORDER BY r.run_date::date DESC, r.started_at DESC
        LIMIT 1
        """,
        (account_id, product, run_date),
    )
    row = cur.fetchone()
    return None if row is None else float(row["score"])


def _run_scoring(conn, settings: Settings, run_date: date) -> str:
    run_date_str = run_date.isoformat()
    run_id = db.create_score_run(conn, run_date_str)

    rules = load_signal_rules(settings.signal_registry_path)
    thresholds = load_thresholds(settings.thresholds_path)
    source_registry = load_source_registry(settings.source_registry_path)
    signal_classes = load_signal_classes(settings.signal_classes_path)

    try:
        observations = db.fetch_observations_for_scoring(conn, run_date_str)
        result = run_scoring(
            run_id=run_id,
            run_date=run_date,
            observations=[dict(row) for row in observations],
            rules=rules,
            thresholds=thresholds,
            source_reliability_defaults=source_registry,
            delta_lookup=None,
        )

        # Keep account_scores exhaustive so downstream exports/metrics include silent accounts too.
        existing_scores = {(score.account_id, score.product) for score in result.account_scores}
        account_rows = conn.execute("SELECT account_id FROM accounts").fetchall()
        for row in account_rows:
            account_id = str(row["account_id"])
            for product in ("zopdev", "zopday", "zopnight"):
                if (account_id, product) in existing_scores:
                    continue
                result.account_scores.append(
                    AccountScore(
                        run_id=run_id,
                        account_id=account_id,
                        product=product,
                        score=0.0,
                        tier="low",
                        top_reasons_json="[]",
                        delta_7d=0.0,
                    )
                )

        signals_by_account_product: dict[tuple[str, str], set[str]] = {}
        for component in result.component_scores:
            key = (component.account_id, component.product)
            signals_by_account_product.setdefault(key, set()).add(component.signal_code)

        for score in result.account_scores:
            baseline = _baseline_score_7d(conn, score.account_id, score.product, run_date_str)
            score.delta_7d = round(score.score - baseline, 2) if baseline is not None else 0.0
            has_primary = any(
                classify_signal(signal_code, signal_classes) == "primary"
                for signal_code in signals_by_account_product.get((score.account_id, score.product), set())
            )
            if score.tier in {"medium", "high"} and not has_primary:
                score.tier = "low"

        db.replace_run_scores(conn, run_id, result.component_scores, result.account_scores)
        db.finish_score_run(conn, run_id, status="completed", error_summary=None)
        return run_id
    except Exception as exc:
        db.finish_score_run(conn, run_id, status="failed", error_summary=str(exc)[:1000])
        raise


def _review_queue_excluded_domains(settings: Settings) -> set[str]:
    # Always suppress internal domain even if config is incomplete.
    excluded = {"zop.dev"}

    try:
        blocked_domains = load_discovery_blocklist(settings.discovery_blocklist_path)
        excluded.update(blocked_domains)
    except Exception:
        logger.warning("failed to load discovery blocklist from %s", settings.discovery_blocklist_path, exc_info=True)

    try:
        profiles = load_account_profiles(settings.account_profiles_path)
        for domain, profile in profiles.items():
            if profile.is_self or profile.exclude_from_crm:
                excluded.add(domain)
    except Exception:
        logger.warning("failed to load account profiles from %s", settings.account_profiles_path, exc_info=True)

    normalized: set[str] = set()
    for domain in excluded:
        value = normalize_domain(domain)
        if value:
            normalized.add(value)
    return normalized


def _run_exports(conn, settings: Settings, run_date: date, run_id: str) -> dict[str, int | str]:
    paths = csv_exporter.output_paths(settings.out_dir, run_date)

    queue_count = csv_exporter.export_review_queue(
        conn,
        run_id,
        paths["review_queue"],
        excluded_domains=_review_queue_excluded_domains(settings),
    )
    score_count = csv_exporter.export_daily_scores(conn, run_id, paths["daily_scores"])

    quality.compute_and_persist_source_metrics(conn, run_date)
    quality_rows = csv_exporter.export_source_quality(conn, run_date.isoformat(), paths["source_quality"])

    readiness_rows = quality.compute_promotion_readiness(conn, run_date)
    readiness_count = csv_exporter.export_promotion_readiness(readiness_rows, paths["promotion_readiness"])

    return {
        "review_queue": queue_count,
        "daily_scores": score_count,
        "source_quality": quality_rows,
        "promotion_readiness": readiness_count,
        "review_queue_path": str(paths["review_queue"]),
        "daily_scores_path": str(paths["daily_scores"]),
        "source_quality_path": str(paths["source_quality"]),
        "promotion_readiness_path": str(paths["promotion_readiness"]),
    }


def _write_icp_coverage_report(conn, settings: Settings, run_id: str, run_date: date) -> dict[str, int | float | str]:
    reference_path = settings.config_dir / "icp_reference_accounts.csv"
    rows, summary = quality.compute_icp_coverage(conn, run_id=run_id, reference_csv_path=reference_path)
    path = settings.out_dir / f"icp_coverage_{run_date.strftime('%Y%m%d')}.csv"
    write_csv_rows(
        path,
        rows,
        fieldnames=[
            "company_name",
            "domain",
            "relationship_stage",
            "zopdev_score",
            "zopdev_tier",
            "zopday_score",
            "zopday_tier",
            "zopnight_score",
            "zopnight_tier",
            "max_score",
            "max_tier",
        ],
    )
    return {
        "path": str(path),
        "total_accounts": int(summary["total_accounts"]),
        "high_or_medium_accounts": int(summary["high_or_medium_accounts"]),
        "coverage_rate": float(summary["coverage_rate"]),
    }


def _run_ingest_cycle(run_date: date) -> dict[str, int | str]:
    settings, conn, seeded = _bootstrap()
    del seeded
    try:
        collect_results = _collect_all(conn, settings)
        collect_inserted = sum(result["inserted"] for result in collect_results.values())
        collect_seen = sum(result["seen"] for result in collect_results.values())
        return {
            "run_date": run_date.isoformat(),
            "observations_seen": collect_seen,
            "observations_inserted": collect_inserted,
        }
    finally:
        conn.close()


def _run_score_cycle(run_date: date) -> dict[str, int | float | str]:
    settings, conn, seeded = _bootstrap()
    del seeded
    try:
        run_id = _run_scoring(conn, settings, run_date)
        export_result = _run_exports(conn, settings, run_date, run_id)
        icp_report = _write_icp_coverage_report(conn, settings, run_id, run_date)
        return {
            "run_id": run_id,
            "daily_scores_rows": int(export_result["daily_scores"]),
            "review_queue_rows": int(export_result["review_queue"]),
            "icp_coverage_rate": float(icp_report["coverage_rate"]),
        }
    finally:
        conn.close()


def _run_discovery_cycle(run_date: date) -> dict[str, int | str]:
    settings, conn, seeded = _bootstrap()
    del seeded
    try:
        ingest_result = discovery_pipeline.ingest_external_events(conn, settings, run_date)
        score_run_id = _run_scoring(conn, settings, run_date)
        scoring_result = discovery_pipeline.score_discovery_candidates(
            conn=conn,
            settings=settings,
            run_date=run_date,
            score_run_id=score_run_id,
            source_events_processed=int(ingest_result["events_processed"]),
            observations_inserted=int(ingest_result["observations_inserted"]),
        )
        report_result = discovery_pipeline.write_discovery_reports(
            conn=conn,
            settings=settings,
            run_date=run_date,
            discovery_run_id=str(scoring_result["discovery_run_id"]),
        )
        return {
            "discovery_run_id": str(scoring_result["discovery_run_id"]),
            "events_processed": int(ingest_result["events_processed"]),
            "candidates": int(scoring_result["total_candidates"]),
            "crm_candidates_rows": int(report_result["crm_candidates_rows"]),
            "manual_review_rows": int(report_result["manual_review_rows"]),
        }
    finally:
        conn.close()


def _run_hunt_cycle(run_date: date, profile_name: str = "light") -> dict[str, int | float | str]:
    settings, conn, seeded = _bootstrap()
    del seeded
    try:
        profile = hunt_pipeline.resolve_profile(profile_name)
        frontier_result = hunt_pipeline.build_frontier(conn, settings, run_date, profile=profile)
        fetch_result = hunt_pipeline.fetch_documents(conn, settings, run_date, profile=profile)
        extract_result = hunt_pipeline.extract_documents(conn, settings, run_date, profile=profile)

        score_run_id = _run_scoring(conn, settings, run_date)
        scoring_result = discovery_pipeline.score_discovery_candidates(
            conn=conn,
            settings=settings,
            run_date=run_date,
            score_run_id=score_run_id,
            source_events_processed=int(frontier_result["events_seen"]),
            observations_inserted=int(extract_result["observations_inserted"]),
            enforce_quality_gates=True,
            min_evidence_quality=0.8,
            min_relevance_score=0.65,
        )
        report_result = discovery_pipeline.write_discovery_reports(
            conn=conn,
            settings=settings,
            run_date=run_date,
            discovery_run_id=str(scoring_result["discovery_run_id"]),
        )
        hunt_reports = hunt_pipeline.write_hunt_reports(conn, settings, run_date)

        return {
            "run_date": run_date.isoformat(),
            "profile": profile.name,
            "events_seen": int(frontier_result["events_seen"]),
            "frontier_queued": int(frontier_result["frontier_queued"]),
            "documents_fetched": int(fetch_result["documents_fetched"]),
            "documents_parsed": int(extract_result["documents_parsed"]),
            "mentions_inserted": int(extract_result["mentions_inserted"]),
            "observations_inserted": int(extract_result["observations_inserted"]),
            "score_run_id": score_run_id,
            "discovery_run_id": str(scoring_result["discovery_run_id"]),
            "total_candidates": int(scoring_result["total_candidates"]),
            "crm_candidates_rows": int(report_result["crm_candidates_rows"]),
            "manual_review_rows": int(report_result["manual_review_rows"]),
            "story_evidence_rows": int(hunt_reports["story_evidence_rows"]),
            "signal_lineage_rows": int(hunt_reports["signal_lineage_rows"]),
        }
    finally:
        conn.close()


@app.command("migrate")
def migrate() -> None:
    """Apply any pending versioned SQL migrations from the migrations/ directory."""
    settings, conn, _ = _bootstrap()
    try:
        newly_applied = db.run_migrations(conn)
        if newly_applied:
            typer.echo(f"migrations_applied={len(newly_applied)} versions={','.join(str(v) for v in newly_applied)}")
        else:
            typer.echo("migrations_applied=0 status=already_up_to_date")
    finally:
        conn.close()


@app.command("ingest")
def ingest(all_sources: bool = typer.Option(True, "--all/--no-all", help="Run all collectors")) -> None:
    if not all_sources:
        raise typer.BadParameter("Partial ingest is not supported yet. Use --all.")
    settings, conn, seeded = _bootstrap()
    try:
        results = _collect_all(conn, settings)
        inserted_total = sum(result["inserted"] for result in results.values())
        seen_total = sum(result["seen"] for result in results.values())
        typer.echo(f"seeded_accounts={seeded} observations_seen={seen_total} observations_inserted={inserted_total}")
        for name, result in results.items():
            typer.echo(f"collector={name} seen={result['seen']} inserted={result['inserted']}")
    finally:
        conn.close()


@app.command("score")
def score(date_str: str = typer.Option(None, "--date", help="Scoring date YYYY-MM-DD")) -> None:
    settings, conn, seeded = _bootstrap()
    run_date = parse_date(date_str, settings.run_timezone)
    del seeded
    try:
        run_id = _run_scoring(conn, settings, run_date)
        summary = db.dump_run_summary(conn, run_id)
        typer.echo(f"run_id={run_id} account_count={summary['account_count']} score_rows={summary['score_rows']}")
    finally:
        conn.close()


@app.command("export")
def export(date_str: str = typer.Option(None, "--date", help="Export date YYYY-MM-DD")) -> None:
    settings, conn, seeded = _bootstrap()
    run_date = parse_date(date_str, settings.run_timezone)
    del seeded
    try:
        run_id = db.get_latest_run_id_for_date(conn, run_date.isoformat())
        if not run_id:
            raise typer.BadParameter(f"No score run found for date {run_date.isoformat()}")
        result = _run_exports(conn, settings, run_date, run_id)
        typer.echo(
            " ".join(
                [
                    f"review_queue_rows={result['review_queue']}",
                    f"daily_scores_rows={result['daily_scores']}",
                    f"source_quality_rows={result['source_quality']}",
                    f"promotion_readiness_rows={result['promotion_readiness']}",
                ]
            )
        )
    finally:
        conn.close()


@app.command("research")
def research(
    date_str: str = typer.Option(..., "--date", help="Run date YYYY-MM-DD"),
    score_run_id: str = typer.Option(..., "--score-run-id", help="Score run ID to research"),
    max_accounts: int = typer.Option(None, "--max-accounts", help="Override research_max_accounts setting"),
) -> None:
    """Run LLM research on top-scoring accounts from a score run."""
    settings, conn, seeded = _bootstrap()
    del seeded
    run_date = parse_date(date_str, settings.run_timezone)
    if max_accounts is not None:
        settings.research_max_accounts = max_accounts
    try:
        result = run_research_stage(conn, settings, run_date.isoformat(), score_run_id)
        typer.echo(
            " ".join(
                f"{k}={v}" for k, v in result.items()
            )
        )
    finally:
        conn.close()


@app.command("export-sales-ready")
def export_sales_ready_cmd(
    date_str: str = typer.Option(..., "--date", help="Run date YYYY-MM-DD"),
    score_run_id: str = typer.Option(..., "--score-run-id", help="Score run ID to export"),
    output: Path = typer.Option(None, "--output", help="Output path (default: out_dir/sales_ready_{date}.csv)"),
) -> None:
    """Export the unified sales-ready CSV for a given score run."""
    settings, conn, seeded = _bootstrap()
    del seeded
    run_date = parse_date(date_str, settings.run_timezone)
    out_path = output or settings.out_dir / f"sales_ready_{csv_exporter.date_suffix(run_date)}.csv"
    try:
        excluded = _review_queue_excluded_domains(settings)
        rows = csv_exporter.export_sales_ready(conn, score_run_id, out_path, excluded)
        typer.echo(f"sales_ready_rows={rows} path={out_path}")
    finally:
        conn.close()


@app.command("sync-sheet")
def sync_sheet(date_str: str = typer.Option(None, "--date", help="Sync date YYYY-MM-DD")) -> None:
    settings, conn, seeded = _bootstrap()
    run_date = parse_date(date_str, settings.run_timezone)
    del conn, seeded

    result = sync_outputs(settings, run_date)
    typer.echo(
        " ".join(
            [
                f"review_queue_rows={result['review_queue_rows']}",
                f"daily_scores_rows={result['daily_scores_rows']}",
                f"source_quality_rows={result['source_quality_rows']}",
            ]
        )
    )


@app.command("import-reviews")
def import_reviews(date_str: str = typer.Option(None, "--date", help="Import date YYYY-MM-DD")) -> None:
    settings, conn, seeded = _bootstrap()
    run_date = parse_date(date_str, settings.run_timezone)
    del seeded
    try:
        imported = import_reviews_for_date(conn, settings, run_date)
        quality.compute_and_persist_source_metrics(conn, run_date)
        readiness = quality.compute_promotion_readiness(conn, run_date)

        paths = csv_exporter.output_paths(settings.out_dir, run_date)
        csv_exporter.export_source_quality(conn, run_date.isoformat(), paths["source_quality"])
        csv_exporter.export_promotion_readiness(readiness, paths["promotion_readiness"])

        typer.echo(f"imported_reviews={imported}")
    finally:
        conn.close()


@app.command("prepare-review-input")
def prepare_review_input(date_str: str = typer.Option(None, "--date", help="Review date YYYY-MM-DD")) -> None:
    settings = load_settings()
    ensure_project_directories(
        [
            settings.project_root,
            settings.data_dir,
            settings.raw_dir,
            settings.out_dir,
        ]
    )
    run_date = parse_date(date_str, settings.run_timezone)
    prepared = prepare_review_input_for_date(settings, run_date)
    typer.echo(f"prepared_review_rows={prepared}")


@app.command("build-cpg-watchlist")
def build_cpg_watchlist(
    limit: int = typer.Option(1000, "--limit", min=100, help="Maximum watchlist rows to keep"),
    merge_handles: bool = typer.Option(
        True,
        "--merge-handles/--no-merge-handles",
        help="Merge generated domains into account_source_handles.csv",
    ),
) -> None:
    settings = load_settings()
    ensure_project_directories([settings.project_root, settings.config_dir, settings.data_dir])

    result = watchlist_builder.build_cpg_watchlist(
        settings=settings,
        limit=limit,
        merge_handles=merge_handles,
    )

    top_regions = ",".join(
        f"{region}:{count}"
        for region, count in sorted(
            result["selected_per_region"].items(),  # type: ignore[arg-type]
            key=lambda item: item[1],
            reverse=True,
        )
    )
    failed_country_count = int(result.get("failed_country_count", 0) or 0)
    typer.echo(
        " ".join(
            [
                f"requested_limit={result['requested_limit']}",
                f"raw_rows={result['raw_rows']}",
                f"deduped_rows={result['deduped_rows']}",
                f"selected_rows={result['selected_rows']}",
                f"handles_inserted={result['handles_inserted']}",
                f"failed_country_count={failed_country_count}",
                f"watchlist_path={result['watchlist_path']}",
                f"region_split={top_regions}",
            ]
        )
    )
    if failed_country_count > 0:
        failures = result.get("failed_countries", {})  # type: ignore[assignment]
        if isinstance(failures, dict):
            for country, message in sorted(failures.items()):
                typer.echo(f"country_error={country} detail={str(message)[:240]}")


@app.command("migrate-watchlist-from-db")
def migrate_watchlist_from_db(
    limit: int = typer.Option(1000, "--limit", min=100, help="Maximum rows to persist into watchlist_accounts.csv"),
) -> None:
    settings, conn, seeded = _bootstrap()
    del seeded
    try:
        existing_rows = load_csv_rows(settings.watchlist_accounts_path)
        existing_by_domain: dict[str, dict[str, str]] = {}
        for row in existing_rows:
            domain = normalize_domain(row.get("domain", ""))
            if not domain:
                continue
            existing_by_domain[domain] = row

        handle_rows = load_csv_rows(settings.account_source_handles_path)
        website_by_domain: dict[str, str] = {}
        for row in handle_rows:
            domain = normalize_domain(row.get("domain", ""))
            if not domain:
                continue
            website = str(row.get("website_url", "")).strip()
            if website:
                website_by_domain[domain] = website

        account_rows = conn.execute(
            """
            SELECT company_name, domain
            FROM accounts
            WHERE source_type = 'seed'
            ORDER BY created_at::timestamp ASC, company_name ASC
            """
        ).fetchall()

        refreshed_on = date.today().isoformat()
        migrated_rows: list[dict[str, str | int | float]] = []
        preserved_metadata_rows = 0
        for account in account_rows:
            domain = normalize_domain(str(account["domain"] or ""))
            if not domain or domain == "zop.dev" or domain.endswith(".example"):
                continue
            company_name = str(account["company_name"] or domain).strip() or domain

            existing = existing_by_domain.get(domain, {})
            if existing:
                preserved_metadata_rows += 1
            website_url = str(existing.get("website_url", "")).strip() or website_by_domain.get(domain, "") or f"https://{domain}"

            migrated_rows.append(
                {
                    "company_name": company_name,
                    "domain": domain,
                    "source_type": "seed",
                    "country": str(existing.get("country", "")).strip(),
                    "region_group": str(existing.get("region_group", "")).strip(),
                    "industry_label": str(existing.get("industry_label", "")).strip(),
                    "website_url": website_url,
                    "wikidata_id": str(existing.get("wikidata_id", "")).strip(),
                    "sitelinks": int(float(existing.get("sitelinks", "0") or 0)),
                    "revenue_usd": float(existing.get("revenue_usd", "0") or 0),
                    "employees": int(float(existing.get("employees", "0") or 0)),
                    "ranking_score": float(existing.get("ranking_score", "0") or 0),
                    "data_source": str(existing.get("data_source", "migration")).strip() or "migration",
                    "last_refreshed_on": refreshed_on,
                }
            )
            if len(migrated_rows) >= max(1, int(limit)):
                break

        write_csv_rows(
            settings.watchlist_accounts_path,
            migrated_rows,
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
        typer.echo(
            " ".join(
                [
                    f"watchlist_path={settings.watchlist_accounts_path}",
                    f"rows_written={len(migrated_rows)}",
                    f"preserved_metadata_rows={preserved_metadata_rows}",
                ]
            )
        )
    finally:
        conn.close()


@app.command("crawl-diagnostics")
def crawl_diagnostics(
    date_str: str = typer.Option(None, "--date", help="Diagnostics date YYYY-MM-DD"),
    failure_limit: int = typer.Option(10, "--failure-limit", help="Number of recent failures to show"),
) -> None:
    settings, conn, seeded = _bootstrap()
    run_date = parse_date(date_str, settings.run_timezone)
    del seeded
    try:
        run_date_str = run_date.isoformat()
        summary_rows = db.fetch_crawl_attempt_summary(conn, run_date_str)
        failure_rows = db.fetch_latest_crawl_failures(conn, run_date_str, limit=max(1, failure_limit))

        if not summary_rows:
            typer.echo(f"run_date={run_date_str} crawl_attempts=0")
            return

        total_attempts = sum(int(row["attempt_count"]) for row in summary_rows)
        totals_by_status: dict[str, int] = {"success": 0, "http_error": 0, "exception": 0, "skipped": 0}
        source_status_counts: dict[str, dict[str, int]] = {}

        for row in summary_rows:
            source = str(row["source"])
            status = str(row["status"])
            count = int(row["attempt_count"])
            totals_by_status[status] = totals_by_status.get(status, 0) + count
            source_status_counts.setdefault(source, {}).setdefault(status, 0)
            source_status_counts[source][status] += count

        typer.echo(
            " ".join(
                [
                    f"run_date={run_date_str}",
                    f"crawl_attempts={total_attempts}",
                    f"success={totals_by_status.get('success', 0)}",
                    f"http_error={totals_by_status.get('http_error', 0)}",
                    f"exception={totals_by_status.get('exception', 0)}",
                    f"skipped={totals_by_status.get('skipped', 0)}",
                ]
            )
        )

        for source in sorted(source_status_counts):
            source_total = sum(source_status_counts[source].values())
            typer.echo(
                " ".join(
                    [
                        f"source={source}",
                        f"attempts={source_total}",
                        f"success={source_status_counts[source].get('success', 0)}",
                        f"http_error={source_status_counts[source].get('http_error', 0)}",
                        f"exception={source_status_counts[source].get('exception', 0)}",
                        f"skipped={source_status_counts[source].get('skipped', 0)}",
                    ]
                )
            )

        for row in failure_rows:
            error_summary = str(row["error_summary"] or "").replace("\n", " ").replace("\r", " ").strip()
            typer.echo(
                " ".join(
                    [
                        f"failure_source={row['source']}",
                        f"status={row['status']}",
                        f"account_id={row['account_id']}",
                        f"attempted_at={row['attempted_at']}",
                        f"endpoint={row['endpoint']}",
                        f"error={error_summary}",
                    ]
                )
            )
    finally:
        conn.close()


@app.command("calibrate-thresholds")
def calibrate_thresholds(
    date_str: str = typer.Option(None, "--date", help="Calibration date YYYY-MM-DD"),
    medium_target_coverage: float = typer.Option(0.6, "--medium-target-coverage", min=0.0, max=1.0),
    high_target_coverage: float = typer.Option(0.2, "--high-target-coverage", min=0.0, max=1.0),
    write: bool = typer.Option(False, "--write", help="Persist suggested thresholds into config/thresholds.csv"),
) -> None:
    settings, conn, seeded = _bootstrap()
    run_date = parse_date(date_str, settings.run_timezone)
    del seeded
    try:
        run_id = db.get_latest_run_id_for_date(conn, run_date.isoformat())
        if not run_id:
            raise typer.BadParameter(f"No score run found for date {run_date.isoformat()}")

        current_thresholds = load_thresholds(settings.thresholds_path)
        suggestion = calibration.suggest_thresholds_for_run(
            conn=conn,
            run_id=run_id,
            reference_csv_path=settings.config_dir / "icp_reference_accounts.csv",
            medium_target_coverage=medium_target_coverage,
            high_target_coverage=high_target_coverage,
            current_thresholds=current_thresholds,
        )

        if write:
            calibration.write_thresholds(
                settings.thresholds_path,
                high=suggestion.high,
                medium=suggestion.medium,
                low=suggestion.low,
            )

        typer.echo(
            " ".join(
                [
                    f"run_id={run_id}",
                    f"suggested_high={suggestion.high}",
                    f"suggested_medium={suggestion.medium}",
                    f"suggested_low={suggestion.low}",
                    f"icp_accounts={suggestion.icp_accounts}",
                    f"icp_high_coverage={suggestion.icp_high_coverage}",
                    f"icp_medium_coverage={suggestion.icp_medium_coverage}",
                    f"non_icp_accounts={suggestion.non_icp_accounts}",
                    f"non_icp_high_hit_rate={suggestion.non_icp_high_hit_rate}",
                    f"non_icp_medium_hit_rate={suggestion.non_icp_medium_hit_rate}",
                    f"written={int(write)}",
                ]
            )
        )
    finally:
        conn.close()


@app.command("tune-profile")
def tune_profile(
    date_str: str = typer.Option(None, "--date", help="Tuning date YYYY-MM-DD"),
    min_icp_medium_coverage: float = typer.Option(0.6, "--min-icp-medium-coverage", min=0.0, max=1.0),
    max_non_icp_medium_hit_rate: float = typer.Option(0.5, "--max-non-icp-medium-hit-rate", min=0.0, max=1.0),
    max_non_icp_high_hit_rate: float = typer.Option(0.25, "--max-non-icp-high-hit-rate", min=0.0, max=1.0),
    min_scenario_pass_rate: float = typer.Option(0.9, "--min-scenario-pass-rate", min=0.0, max=1.0),
    scenarios_path: str = typer.Option(
        "config/profile_scenarios.csv",
        "--scenarios-path",
        help="Scenario CSV path relative to project root (or absolute path)",
    ),
    write: bool = typer.Option(False, "--write", help="Persist tuned thresholds into config/thresholds.csv"),
) -> None:
    settings, conn, seeded = _bootstrap()
    run_date = parse_date(date_str, settings.run_timezone)
    del seeded
    try:
        run_id = db.get_latest_run_id_for_date(conn, run_date.isoformat())
        if not run_id:
            raise typer.BadParameter(f"No score run found for date {run_date.isoformat()}")

        raw_scenario_path = Path(scenarios_path)
        scenario_path = raw_scenario_path if raw_scenario_path.is_absolute() else (settings.project_root / raw_scenario_path)
        scenarios = calibration.load_scenarios(scenario_path)
        current_thresholds = load_thresholds(settings.thresholds_path)

        suggestion = calibration.suggest_profile_for_run(
            conn=conn,
            run_id=run_id,
            reference_csv_path=settings.config_dir / "icp_reference_accounts.csv",
            scenarios=scenarios,
            min_icp_medium_coverage=min_icp_medium_coverage,
            max_non_icp_medium_hit_rate=max_non_icp_medium_hit_rate,
            max_non_icp_high_hit_rate=max_non_icp_high_hit_rate,
            min_scenario_pass_rate=min_scenario_pass_rate,
            current_thresholds=current_thresholds,
        )

        if write:
            calibration.write_thresholds(
                settings.thresholds_path,
                high=suggestion.high,
                medium=suggestion.medium,
                low=suggestion.low,
            )

        typer.echo(
            " ".join(
                [
                    f"run_id={run_id}",
                    f"suggested_high={suggestion.high}",
                    f"suggested_medium={suggestion.medium}",
                    f"suggested_low={suggestion.low}",
                    f"icp_accounts={suggestion.icp_accounts}",
                    f"icp_medium_coverage={suggestion.icp_medium_coverage}",
                    f"icp_high_coverage={suggestion.icp_high_coverage}",
                    f"non_icp_accounts={suggestion.non_icp_accounts}",
                    f"non_icp_medium_hit_rate={suggestion.non_icp_medium_hit_rate}",
                    f"non_icp_high_hit_rate={suggestion.non_icp_high_hit_rate}",
                    f"scenario_count={suggestion.scenario_count}",
                    f"scenario_pass_rate={suggestion.scenario_pass_rate}",
                    f"constraints_satisfied={int(suggestion.constraints_satisfied)}",
                    f"written={int(write)}",
                ]
            )
        )
    finally:
        conn.close()


@app.command("eval-output")
def eval_output(
    date_str: str = typer.Option(None, "--date", help="Evaluation date YYYY-MM-DD"),
    min_icp_medium_coverage: float = typer.Option(0.6, "--min-icp-medium-coverage", min=0.0, max=1.0),
    min_icp_high_coverage: float = typer.Option(0.2, "--min-icp-high-coverage", min=0.0, max=1.0),
    max_non_icp_medium_hit_rate: float = typer.Option(0.5, "--max-non-icp-medium-hit-rate", min=0.0, max=1.0),
    max_non_icp_high_hit_rate: float = typer.Option(0.25, "--max-non-icp-high-hit-rate", min=0.0, max=1.0),
    min_scenario_pass_rate: float = typer.Option(0.9, "--min-scenario-pass-rate", min=0.0, max=1.0),
    scenarios_path: str = typer.Option(
        "config/profile_scenarios.csv",
        "--scenarios-path",
        help="Scenario CSV path relative to project root (or absolute path)",
    ),
) -> None:
    settings, conn, seeded = _bootstrap()
    run_date = parse_date(date_str, settings.run_timezone)
    del seeded
    try:
        run_id = db.get_latest_run_id_for_date(conn, run_date.isoformat())
        if not run_id:
            raise typer.BadParameter(f"No score run found for date {run_date.isoformat()}")

        raw_scenario_path = Path(scenarios_path)
        scenario_path = raw_scenario_path if raw_scenario_path.is_absolute() else (settings.project_root / raw_scenario_path)
        scenarios = calibration.load_scenarios(scenario_path)
        thresholds = load_thresholds(settings.thresholds_path)
        quality_bar = OutputQualityBar(
            min_icp_medium_coverage=min_icp_medium_coverage,
            min_icp_high_coverage=min_icp_high_coverage,
            max_non_icp_medium_hit_rate=max_non_icp_medium_hit_rate,
            max_non_icp_high_hit_rate=max_non_icp_high_hit_rate,
            min_scenario_pass_rate=min_scenario_pass_rate,
        )
        result = evaluate_run_output_quality(
            conn=conn,
            run_id=run_id,
            reference_csv_path=settings.config_dir / "icp_reference_accounts.csv",
            thresholds=thresholds,
            quality_bar=quality_bar,
            scenarios=scenarios,
        )
        typer.echo(
            " ".join(
                [
                    f"run_id={run_id}",
                    f"threshold_high={result.thresholds.high}",
                    f"threshold_medium={result.thresholds.medium}",
                    f"threshold_low={result.thresholds.low}",
                    f"icp_accounts={result.icp_accounts}",
                    f"non_icp_accounts={result.non_icp_accounts}",
                    f"icp_high_coverage={result.icp_high_coverage}",
                    f"icp_medium_coverage={result.icp_medium_coverage}",
                    f"non_icp_high_hit_rate={result.non_icp_high_hit_rate}",
                    f"non_icp_medium_hit_rate={result.non_icp_medium_hit_rate}",
                    f"scenario_pass_rate={result.scenario_pass_rate}",
                    f"quality_passed={int(result.passed)}",
                    f"failed_checks={'|'.join(result.failed_checks) if result.failed_checks else 'none'}",
                ]
            )
        )
    finally:
        conn.close()


@app.command("self-improve-output")
def self_improve_output(
    date_str: str = typer.Option(None, "--date", help="Self-improvement date YYYY-MM-DD"),
    max_iterations: int = typer.Option(5, "--max-iterations", min=1),
    min_icp_medium_coverage: float = typer.Option(0.6, "--min-icp-medium-coverage", min=0.0, max=1.0),
    min_icp_high_coverage: float = typer.Option(0.2, "--min-icp-high-coverage", min=0.0, max=1.0),
    max_non_icp_medium_hit_rate: float = typer.Option(0.5, "--max-non-icp-medium-hit-rate", min=0.0, max=1.0),
    max_non_icp_high_hit_rate: float = typer.Option(0.25, "--max-non-icp-high-hit-rate", min=0.0, max=1.0),
    min_scenario_pass_rate: float = typer.Option(0.9, "--min-scenario-pass-rate", min=0.0, max=1.0),
    scenarios_path: str = typer.Option(
        "config/profile_scenarios.csv",
        "--scenarios-path",
        help="Scenario CSV path relative to project root (or absolute path)",
    ),
    write: bool = typer.Option(False, "--write", help="Persist tuned thresholds and regenerate scoring outputs"),
) -> None:
    settings, conn, seeded = _bootstrap()
    run_date = parse_date(date_str, settings.run_timezone)
    del seeded
    try:
        run_id = db.get_latest_run_id_for_date(conn, run_date.isoformat())
        if not run_id:
            raise typer.BadParameter(f"No score run found for date {run_date.isoformat()}")

        current_thresholds = load_thresholds(settings.thresholds_path)
        raw_scenario_path = Path(scenarios_path)
        scenario_path = raw_scenario_path if raw_scenario_path.is_absolute() else (settings.project_root / raw_scenario_path)
        scenarios = calibration.load_scenarios(scenario_path)
        quality_bar = OutputQualityBar(
            min_icp_medium_coverage=min_icp_medium_coverage,
            min_icp_high_coverage=min_icp_high_coverage,
            max_non_icp_medium_hit_rate=max_non_icp_medium_hit_rate,
            max_non_icp_high_hit_rate=max_non_icp_high_hit_rate,
            min_scenario_pass_rate=min_scenario_pass_rate,
        )

        result = run_threshold_self_improvement(
            conn=conn,
            run_id=run_id,
            reference_csv_path=settings.config_dir / "icp_reference_accounts.csv",
            current_thresholds=current_thresholds,
            quality_bar=quality_bar,
            max_iterations=max_iterations,
            scenarios=scenarios,
        )

        latest_run_id = run_id
        thresholds_changed = (
            round(result.final_thresholds.high, 4) != round(current_thresholds.high, 4)
            or round(result.final_thresholds.medium, 4) != round(current_thresholds.medium, 4)
            or round(result.final_thresholds.low, 4) != round(current_thresholds.low, 4)
        )
        if write and thresholds_changed:
            calibration.write_thresholds(
                settings.thresholds_path,
                high=result.final_thresholds.high,
                medium=result.final_thresholds.medium,
                low=result.final_thresholds.low,
            )
            latest_run_id = _run_scoring(conn, settings, run_date)
            _run_exports(conn, settings, run_date, latest_run_id)
            _write_icp_coverage_report(conn, settings, latest_run_id, run_date)

        final_eval = result.iterations[-1].evaluation if result.iterations else evaluate_run_output_quality(
            conn=conn,
            run_id=run_id,
            reference_csv_path=settings.config_dir / "icp_reference_accounts.csv",
            thresholds=current_thresholds,
            quality_bar=quality_bar,
            scenarios=scenarios,
        )
        typer.echo(
            " ".join(
                [
                    f"run_id={latest_run_id}",
                    f"iterations={len(result.iterations)}",
                    f"initial_high={current_thresholds.high}",
                    f"initial_medium={current_thresholds.medium}",
                    f"final_high={result.final_thresholds.high}",
                    f"final_medium={result.final_thresholds.medium}",
                    f"quality_passed={int(result.passed)}",
                    f"converged={int(result.converged)}",
                    f"failed_checks={'|'.join(final_eval.failed_checks) if final_eval.failed_checks else 'none'}",
                    f"written={int(write and thresholds_changed)}",
                ]
            )
        )
    finally:
        conn.close()


@app.command("run-daily")
def run_daily(
    date_str: str = typer.Option(None, "--date", help="Run date YYYY-MM-DD"),
    live_max_accounts: int | None = typer.Option(
        None,
        "--live-max-accounts",
        min=1,
        help="Override live crawl account budget for this run only.",
    ),
    live_workers_per_source: int | None = typer.Option(
        None,
        "--live-workers-per-source",
        min=1,
        help="Override live crawl workers per source for this run only.",
    ),
    stage_timeout_seconds: int | None = typer.Option(
        None,
        "--stage-timeout-seconds",
        min=30,
        help="Override per-stage timeout for this run only.",
    ),
) -> None:
    settings, conn, seeded = _bootstrap()
    overrides: dict[str, object] = {}
    if live_max_accounts is not None:
        overrides["live_max_accounts"] = max(1, int(live_max_accounts))
    if live_workers_per_source is not None:
        overrides["live_workers_per_source"] = max(1, int(live_workers_per_source))
    if stage_timeout_seconds is not None:
        overrides["stage_timeout_seconds"] = max(30, int(stage_timeout_seconds))
    if overrides:
        settings = settings.model_copy(update=overrides)
    run_date = parse_date(date_str, settings.run_timezone)
    lock_owner = f"pid{os.getpid()}-{uuid.uuid4().hex[:8]}"
    lock_acquired = False
    try:
        lock_acquired = db.try_advisory_lock(
            conn,
            lock_name=_RUN_DAILY_LOCK_NAME,
            owner_id=lock_owner,
            details=f"run_date={run_date.isoformat()}",
        )
        if not lock_acquired:
            typer.echo(f"status=skipped reason=lock_busy lock_name={_RUN_DAILY_LOCK_NAME}")
            return

        typer.echo(
            f"stage=ingest status=started live_max_accounts={settings.live_max_accounts} "
            f"live_workers_per_source={settings.live_workers_per_source} "
            f"timeout_seconds={settings.stage_timeout_seconds}"
        )
        collect_results, collect_elapsed = _run_with_watchdog(
            "ingest",
            settings.stage_timeout_seconds,
            lambda: _collect_all(conn, settings),
        )
        collect_inserted = sum(result["inserted"] for result in collect_results.values())
        typer.echo(f"stage=ingest status=completed duration_seconds={round(collect_elapsed, 2)} inserted={collect_inserted}")

        typer.echo("stage=score status=started")
        run_id, score_elapsed = _run_with_watchdog("score", settings.stage_timeout_seconds, lambda: _run_scoring(conn, settings, run_date))
        typer.echo(f"stage=score status=completed duration_seconds={round(score_elapsed, 2)} run_id={run_id}")

        # Research stage — non-blocking. If it fails, export still happens.
        research_result = {"attempted": 0, "completed": 0, "failed": 0, "skipped": 0}
        try:
            research_result, _ = _run_with_watchdog(
                "research",
                settings.stage_timeout_seconds,
                lambda: run_research_stage(conn, settings, run_date.isoformat(), run_id),
            )
        except Exception as exc:
            logger.warning("research stage failed, continuing to export: %s", exc, exc_info=True)

        # Sales-ready CSV export.
        excluded = _review_queue_excluded_domains(settings)
        sales_ready_path = settings.out_dir / f"sales_ready_{csv_exporter.date_suffix(run_date)}.csv"
        sales_ready_rows = 0
        try:
            sales_ready_rows = csv_exporter.export_sales_ready(conn, run_id, sales_ready_path, excluded)
        except Exception as exc:
            logger.warning("sales-ready export failed: %s", exc, exc_info=True)

        typer.echo("stage=export status=started")
        export_result, _ = _run_with_watchdog(
            "export",
            settings.stage_timeout_seconds,
            lambda: _run_exports(conn, settings, run_date, run_id),
        )
        typer.echo(
            f"stage=export status=completed review_queue_rows={export_result['review_queue']} "
            f"daily_scores_rows={export_result['daily_scores']}"
        )
        typer.echo("stage=prepare-review-input status=started")
        prepared_reviews, _ = _run_with_watchdog(
            "prepare-review-input",
            settings.stage_timeout_seconds,
            lambda: prepare_review_input_for_date(settings, run_date),
        )
        typer.echo(f"stage=prepare-review-input status=completed prepared_review_rows={prepared_reviews}")

        sync_error = ""
        sync_result = {"review_queue_rows": 0, "daily_scores_rows": 0, "source_quality_rows": 0}
        try:
            typer.echo("stage=sync-sheet status=started")
            sync_result, _ = _run_with_watchdog(
                "sync-sheet",
                settings.stage_timeout_seconds,
                lambda: sync_outputs(settings, run_date),
            )
            typer.echo(
                f"stage=sync-sheet status=completed review_queue_rows={sync_result['review_queue_rows']} "
                f"daily_scores_rows={sync_result['daily_scores_rows']}"
            )
        except Exception as exc:
            sync_error = str(exc)
            typer.echo(f"stage=sync-sheet status=failed error={sync_error[:220]}")

        typer.echo("stage=import-reviews status=started")
        imported, _ = _run_with_watchdog(
            "import-reviews",
            settings.stage_timeout_seconds,
            lambda: import_reviews_for_date(conn, settings, run_date),
        )
        typer.echo(f"stage=import-reviews status=completed imported_reviews={imported}")

        def _refresh_quality_outputs() -> dict[str, int]:
            quality.compute_and_persist_source_metrics(conn, run_date)
            readiness_rows = quality.compute_promotion_readiness(conn, run_date)
            paths = csv_exporter.output_paths(settings.out_dir, run_date)
            quality_rows = csv_exporter.export_source_quality(conn, run_date.isoformat(), paths["source_quality"])
            readiness_count = csv_exporter.export_promotion_readiness(readiness_rows, paths["promotion_readiness"])
            return {"source_quality_rows": quality_rows, "promotion_readiness_rows": readiness_count}

        typer.echo("stage=quality-refresh status=started")
        quality_result, quality_elapsed = _run_with_watchdog(
            "quality-refresh",
            settings.stage_timeout_seconds,
            _refresh_quality_outputs,
        )
        typer.echo(
            f"stage=quality-refresh status=completed duration_seconds={round(quality_elapsed, 2)} "
            f"source_quality_rows={quality_result['source_quality_rows']}"
        )
        typer.echo("stage=icp-coverage-report status=started")
        icp_report, icp_elapsed = _run_with_watchdog(
            "icp-coverage-report",
            settings.stage_timeout_seconds,
            lambda: _write_icp_coverage_report(conn, settings, run_id, run_date),
        )
        typer.echo(
            f"stage=icp-coverage-report status=completed duration_seconds={round(icp_elapsed, 2)} "
            f"icp_coverage={icp_report['coverage_rate']}"
        )
        typer.echo("stage=ops-metrics status=started")
        ops_result, ops_elapsed = _run_with_watchdog(
            "ops-metrics",
            settings.stage_timeout_seconds,
            lambda: _persist_ops_metrics(conn, settings, run_date),
        )
        typer.echo(
            f"stage=ops-metrics status=completed duration_seconds={round(ops_elapsed, 2)} "
            f"ops_metrics_rows={ops_result['ops_metrics_rows']}"
        )

        typer.echo(
            " ".join(
                [
                    f"seeded_accounts={seeded}",
                    f"ingested={collect_inserted}",
                    f"run_id={run_id}",
                    f"review_queue_rows={export_result['review_queue']}",
                    f"daily_scores_rows={export_result['daily_scores']}",
                    f"source_quality_rows={quality_result['source_quality_rows']}",
                    f"promotion_readiness_rows={quality_result['promotion_readiness_rows']}",
                    f"prepared_review_rows={prepared_reviews}",
                    f"imported_reviews={imported}",
                    f"synced_review_queue_rows={sync_result['review_queue_rows']}",
                    f"icp_accounts={icp_report['total_accounts']}",
                    f"icp_high_or_medium={icp_report['high_or_medium_accounts']}",
                    f"icp_coverage={icp_report['coverage_rate']}",
                    f"ops_metrics_rows={ops_result['ops_metrics_rows']}",
                    f"retry_depth={ops_result['retry_depth']}",
                    f"retry_queue_size={ops_result['retry_queue_size']}",
                    f"quarantine_size={ops_result['quarantine_size']}",
                    f"lock_busy_24h={ops_result['lock_busy_24h']}",
                    f"lock_release_missed_24h={ops_result['lock_release_missed_24h']}",
                    f"research_attempted={research_result['attempted']}",
                    f"research_completed={research_result['completed']}",
                    f"sales_ready_rows={sales_ready_rows}",
                    f"sync_error={sync_error}",
                ]
            )
        )
    except StageExecutionError as exc:
        enqueue_retries = os.getenv("SIGNALS_DISABLE_AUTO_RETRY_ENQUEUE", "").strip().lower() not in {"1", "true", "yes"}
        retry_task_id = ""
        if enqueue_retries:
            retry_task_id = _enqueue_retry_task(
                conn,
                settings,
                task_type="run_daily",
                payload={"run_date": run_date.isoformat()},
                reason=str(exc),
            )
        db.record_stage_failure(
            conn,
            run_type="run_daily",
            run_date=run_date.isoformat(),
            stage=exc.stage,
            error_summary=str(exc),
            duration_seconds=exc.duration_seconds,
            timed_out=exc.timed_out,
            retry_task_id=retry_task_id,
            commit=True,
        )
        send_alert(
            settings,
            title="run-daily failed",
            body=(
                f"run_date={run_date.isoformat()} stage={exc.stage} "
                f"timed_out={int(exc.timed_out)} duration_seconds={round(exc.duration_seconds, 2)} "
                f"retry_task_id={retry_task_id}"
            ),
            severity="error",
        )
        raise typer.Exit(code=1)
    except Exception as exc:
        enqueue_retries = os.getenv("SIGNALS_DISABLE_AUTO_RETRY_ENQUEUE", "").strip().lower() not in {"1", "true", "yes"}
        retry_task_id = ""
        if enqueue_retries:
            retry_task_id = _enqueue_retry_task(
                conn,
                settings,
                task_type="run_daily",
                payload={"run_date": run_date.isoformat()},
                reason=str(exc),
            )
        db.record_stage_failure(
            conn,
            run_type="run_daily",
            run_date=run_date.isoformat(),
            stage="run_daily",
            error_summary=str(exc),
            duration_seconds=0.0,
            timed_out=False,
            retry_task_id=retry_task_id,
            commit=True,
        )
        send_alert(
            settings,
            title="run-daily failed",
            body=f"run_date={run_date.isoformat()} error={str(exc)[:400]} retry_task_id={retry_task_id}",
            severity="error",
        )
        raise typer.Exit(code=1)
    finally:
        if lock_acquired:
            db.release_advisory_lock(conn, lock_name=_RUN_DAILY_LOCK_NAME, owner_id=lock_owner)
        conn.close()


@app.command("icp-report")
def icp_report(date_str: str = typer.Option(None, "--date", help="Run date YYYY-MM-DD")) -> None:
    settings, conn, seeded = _bootstrap()
    run_date = parse_date(date_str, settings.run_timezone)
    del seeded
    try:
        run_id = db.get_latest_run_id_for_date(conn, run_date.isoformat())
        if not run_id:
            raise typer.BadParameter(f"No score run found for date {run_date.isoformat()}")
        report = _write_icp_coverage_report(conn, settings, run_id, run_date)
        typer.echo(
            " ".join(
                [
                    f"path={report['path']}",
                    f"total_accounts={report['total_accounts']}",
                    f"high_or_medium_accounts={report['high_or_medium_accounts']}",
                    f"coverage_rate={report['coverage_rate']}",
                ]
            )
        )
    finally:
        conn.close()


@app.command("icp-signal-gaps")
def icp_signal_gaps(
    date_str: str = typer.Option(None, "--date", help="Run date YYYY-MM-DD"),
    playbook_path: str = typer.Option(
        "config/icp_signal_playbook.csv",
        "--playbook-path",
        help="Playbook CSV path relative to project root (or absolute path)",
    ),
    reference_path: str = typer.Option(
        "config/icp_reference_accounts.csv",
        "--reference-path",
        help="ICP reference CSV path relative to project root (or absolute path)",
    ),
) -> None:
    settings, conn, seeded = _bootstrap()
    run_date = parse_date(date_str, settings.run_timezone)
    del seeded
    try:
        run_id = db.get_latest_run_id_for_date(conn, run_date.isoformat())
        if not run_id:
            raise typer.BadParameter(f"No score run found for date {run_date.isoformat()}")

        raw_playbook_path = Path(playbook_path)
        resolved_playbook_path = (
            raw_playbook_path if raw_playbook_path.is_absolute() else (settings.project_root / raw_playbook_path)
        )
        raw_reference_path = Path(reference_path)
        resolved_reference_path = (
            raw_reference_path if raw_reference_path.is_absolute() else (settings.project_root / raw_reference_path)
        )

        rows, summary = icp_playbook.compute_icp_signal_gaps(
            conn=conn,
            run_id=run_id,
            reference_csv_path=resolved_reference_path,
            playbook_path=resolved_playbook_path,
        )

        output_path = settings.out_dir / f"icp_signal_gaps_{run_date.strftime('%Y%m%d')}.csv"
        icp_playbook.write_icp_signal_gap_report(output_path, rows)

        typer.echo(
            " ".join(
                [
                    f"path={output_path}",
                    f"total_accounts={summary['total_accounts']}",
                    f"expected_signals={summary['expected_signals']}",
                    f"observed_signals={summary['observed_signals']}",
                    f"coverage_rate={summary['coverage_rate']}",
                    f"high_priority_gaps={summary['high_priority_gaps']}",
                    f"accounts_with_full_coverage={summary['accounts_with_full_coverage']}",
                ]
            )
        )
    finally:
        conn.close()


@app.command("discover-ingest")
def discover_ingest(date_str: str = typer.Option(None, "--date", help="Discovery ingest date YYYY-MM-DD")) -> None:
    settings, conn, seeded = _bootstrap()
    run_date = parse_date(date_str, settings.run_timezone)
    del seeded
    try:
        result = discovery_pipeline.ingest_external_events(conn, settings, run_date)
        typer.echo(
            " ".join(
                [
                    f"run_date={result['run_date']}",
                    f"events_seen={result['events_seen']}",
                    f"events_processed={result['events_processed']}",
                    f"events_failed={result['events_failed']}",
                    f"signal_matches={result['signal_matches']}",
                    f"observations_inserted={result['observations_inserted']}",
                ]
            )
        )
    finally:
        conn.close()


@app.command("discover-frontier")
def discover_frontier(
    date_str: str = typer.Option(None, "--date", help="Frontier build date YYYY-MM-DD"),
    profile: str = typer.Option("light", "--profile", help="Hunt profile: light|balanced|heavy"),
) -> None:
    settings, conn, seeded = _bootstrap()
    run_date = parse_date(date_str, settings.run_timezone)
    del seeded
    try:
        profile_cfg = hunt_pipeline.resolve_profile(profile)
        result = hunt_pipeline.build_frontier(conn, settings, run_date, profile=profile_cfg)
        typer.echo(
            " ".join(
                [
                    f"run_date={result['run_date']}",
                    f"profile={profile_cfg.name}",
                    f"events_seen={result['events_seen']}",
                    f"frontier_queued={result['frontier_queued']}",
                    f"frontier_duplicates={result['frontier_duplicates']}",
                    f"events_failed={result['events_failed']}",
                ]
            )
        )
    finally:
        conn.close()


@app.command("discover-fetch")
def discover_fetch(
    date_str: str = typer.Option(None, "--date", help="Fetch date YYYY-MM-DD"),
    profile: str = typer.Option("light", "--profile", help="Hunt profile: light|balanced|heavy"),
) -> None:
    settings, conn, seeded = _bootstrap()
    run_date = parse_date(date_str, settings.run_timezone)
    del seeded
    try:
        profile_cfg = hunt_pipeline.resolve_profile(profile)
        result = hunt_pipeline.fetch_documents(conn, settings, run_date, profile=profile_cfg)
        typer.echo(
            " ".join(
                [
                    f"run_date={result['run_date']}",
                    f"profile={profile_cfg.name}",
                    f"frontier_rows_seen={result['frontier_rows_seen']}",
                    f"documents_fetched={result['documents_fetched']}",
                    f"documents_failed={result['documents_failed']}",
                    f"js_fetches_used={result['js_fetches_used']}",
                ]
            )
        )
    finally:
        conn.close()


@app.command("discover-extract")
def discover_extract(
    date_str: str = typer.Option(None, "--date", help="Extraction date YYYY-MM-DD"),
    profile: str = typer.Option("light", "--profile", help="Hunt profile: light|balanced|heavy"),
) -> None:
    settings, conn, seeded = _bootstrap()
    run_date = parse_date(date_str, settings.run_timezone)
    del seeded
    try:
        profile_cfg = hunt_pipeline.resolve_profile(profile)
        result = hunt_pipeline.extract_documents(conn, settings, run_date, profile=profile_cfg)
        typer.echo(
            " ".join(
                [
                    f"run_date={result['run_date']}",
                    f"profile={profile_cfg.name}",
                    f"documents_seen={result['documents_seen']}",
                    f"documents_parsed={result['documents_parsed']}",
                    f"listing_pages={result['listing_pages']}",
                    f"links_enqueued={result['links_enqueued']}",
                    f"mentions_inserted={result['mentions_inserted']}",
                    f"observations_inserted={result['observations_inserted']}",
                    f"people_activity_inserted={result['people_activity_inserted']}",
                ]
            )
        )
    finally:
        conn.close()


@app.command("discover-score")
def discover_score(
    date_str: str = typer.Option(None, "--date", help="Discovery scoring date YYYY-MM-DD"),
    quality_gates: bool = typer.Option(False, "--quality-gates/--no-quality-gates", help="Enforce evidence/relevance gates"),
) -> None:
    settings, conn, seeded = _bootstrap()
    run_date = parse_date(date_str, settings.run_timezone)
    del seeded
    try:
        score_run_id = _run_scoring(conn, settings, run_date)
        result = discovery_pipeline.score_discovery_candidates(
            conn=conn,
            settings=settings,
            run_date=run_date,
            score_run_id=score_run_id,
            source_events_processed=0,
            observations_inserted=0,
            enforce_quality_gates=quality_gates,
            min_evidence_quality=0.8,
            min_relevance_score=0.65,
        )
        typer.echo(
            " ".join(
                [
                    f"score_run_id={score_run_id}",
                    f"quality_gates={int(quality_gates)}",
                    f"discovery_run_id={result['discovery_run_id']}",
                    f"total_candidates={result['total_candidates']}",
                    f"high_candidates={result['high_candidates']}",
                    f"medium_candidates={result['medium_candidates']}",
                    f"explore_candidates={result['explore_candidates']}",
                    f"crm_eligible_candidates={result['crm_eligible_candidates']}",
                ]
            )
        )
    finally:
        conn.close()


@app.command("discover-report")
def discover_report(date_str: str = typer.Option(None, "--date", help="Discovery report date YYYY-MM-DD")) -> None:
    settings, conn, seeded = _bootstrap()
    run_date = parse_date(date_str, settings.run_timezone)
    del seeded
    try:
        discovery_run_id = db.get_latest_discovery_run_id_for_date(conn, run_date.isoformat())
        if not discovery_run_id:
            raise typer.BadParameter(f"No discovery run found for date {run_date.isoformat()}")
        result = discovery_pipeline.write_discovery_reports(conn, settings, run_date, discovery_run_id)
        typer.echo(
            " ".join(
                [
                    f"discovery_run_id={discovery_run_id}",
                    f"discovery_queue_rows={result['discovery_queue_rows']}",
                    f"crm_candidates_rows={result['crm_candidates_rows']}",
                    f"manual_review_rows={result['manual_review_rows']}",
                    f"metrics_rows={result['metrics_rows']}",
                    f"discovery_queue_path={result['discovery_queue_path']}",
                    f"crm_candidates_path={result['crm_candidates_path']}",
                    f"manual_review_path={result['manual_review_path']}",
                    f"discovery_metrics_path={result['discovery_metrics_path']}",
                ]
            )
        )
    finally:
        conn.close()


@app.command("run-discovery")
def run_discovery(
    date_str: str = typer.Option(None, "--date", help="Discovery run date YYYY-MM-DD"),
    profile: str = typer.Option("light", "--profile", help="Hunt profile: light|balanced|heavy"),
) -> None:
    # Backward-compatible command name; runtime is now story-deep hunt only.
    run_date = parse_date(date_str, load_settings().run_timezone)
    result = _run_hunt_cycle(run_date, profile_name=profile)
    typer.echo(
        " ".join(
            [
                f"run_date={result['run_date']}",
                f"profile={result['profile']}",
                f"events_seen={result['events_seen']}",
                f"frontier_queued={result['frontier_queued']}",
                f"documents_fetched={result['documents_fetched']}",
                f"documents_parsed={result['documents_parsed']}",
                f"mentions_inserted={result['mentions_inserted']}",
                f"observations_inserted={result['observations_inserted']}",
                f"score_run_id={result['score_run_id']}",
                f"discovery_run_id={result['discovery_run_id']}",
                f"total_candidates={result['total_candidates']}",
                f"crm_candidates_rows={result['crm_candidates_rows']}",
                f"manual_review_rows={result['manual_review_rows']}",
                f"story_evidence_rows={result['story_evidence_rows']}",
                f"signal_lineage_rows={result['signal_lineage_rows']}",
            ]
        )
    )


@app.command("run-hunt")
def run_hunt(
    date_str: str = typer.Option(None, "--date", help="Hunt run date YYYY-MM-DD"),
    profile: str = typer.Option("light", "--profile", help="Hunt profile: light|balanced|heavy"),
) -> None:
    run_date = parse_date(date_str, load_settings().run_timezone)
    result = _run_hunt_cycle(run_date, profile_name=profile)
    typer.echo(
        " ".join(
            [
                f"run_date={result['run_date']}",
                f"profile={result['profile']}",
                f"events_seen={result['events_seen']}",
                f"frontier_queued={result['frontier_queued']}",
                f"documents_fetched={result['documents_fetched']}",
                f"documents_parsed={result['documents_parsed']}",
                f"mentions_inserted={result['mentions_inserted']}",
                f"observations_inserted={result['observations_inserted']}",
                f"score_run_id={result['score_run_id']}",
                f"discovery_run_id={result['discovery_run_id']}",
                f"total_candidates={result['total_candidates']}",
                f"crm_candidates_rows={result['crm_candidates_rows']}",
                f"manual_review_rows={result['manual_review_rows']}",
                f"story_evidence_rows={result['story_evidence_rows']}",
                f"signal_lineage_rows={result['signal_lineage_rows']}",
            ]
        )
    )


@app.command("run-autonomous-loop")
def run_autonomous_loop(
    ingest_interval_minutes: int = typer.Option(15, "--ingest-interval-minutes", min=1),
    score_interval_minutes: int = typer.Option(60, "--score-interval-minutes", min=5),
    discovery_interval_minutes: int = typer.Option(180, "--discovery-interval-minutes", min=10),
    hunt_profile: str = typer.Option("light", "--hunt-profile", help="Hunt profile: light|balanced|heavy"),
    sleep_seconds: int = typer.Option(5, "--sleep-seconds", min=1),
    once: bool = typer.Option(False, "--once", help="Run one cycle for each due job and exit"),
) -> None:
    settings = load_settings()
    lock_conn = db.get_connection(settings.pg_dsn)
    db.init_db(lock_conn)
    lock_owner = f"pid{os.getpid()}-{uuid.uuid4().hex[:8]}"
    lock_acquired = db.try_advisory_lock(
        lock_conn,
        lock_name=_AUTONOMOUS_LOCK_NAME,
        owner_id=lock_owner,
        details=f"hunt_profile={hunt_profile}",
    )
    if not lock_acquired:
        typer.echo(f"status=skipped reason=lock_busy lock_name={_AUTONOMOUS_LOCK_NAME}")
        lock_conn.close()
        return

    next_ingest_at = 0.0
    next_score_at = 0.0
    next_discovery_at = 0.0

    ingest_every = float(ingest_interval_minutes * 60)
    score_every = float(score_interval_minutes * 60)
    discovery_every = float(discovery_interval_minutes * 60)

    try:
        while True:
            now_mono = time.monotonic()
            run_date = parse_date(None, settings.run_timezone)
            now_iso = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
            did_work = False

            due_ingest = now_mono >= next_ingest_at
            due_discovery = now_mono >= next_discovery_at
            due_score = now_mono >= next_score_at

            # Ordering matters: discovery runs before score so exports reflect same-cycle webhook events.
            if due_ingest:
                did_work = True
                try:
                    ingest_result, _ = _run_with_watchdog(
                        "ingest_cycle",
                        settings.stage_timeout_seconds,
                        lambda: _run_ingest_cycle(run_date),
                    )
                    typer.echo(
                        " ".join(
                            [
                                f"ts={now_iso}",
                                "job=ingest",
                                f"run_date={ingest_result['run_date']}",
                                f"observations_seen={ingest_result['observations_seen']}",
                                f"observations_inserted={ingest_result['observations_inserted']}",
                            ]
                        )
                    )
                except StageExecutionError as exc:
                    retry_task_id = _enqueue_retry_task(
                        lock_conn,
                        settings,
                        task_type="ingest_cycle",
                        payload={"run_date": run_date.isoformat()},
                        reason=str(exc),
                    )
                    db.record_stage_failure(
                        lock_conn,
                        run_type="autonomous_loop",
                        run_date=run_date.isoformat(),
                        stage=exc.stage,
                        error_summary=str(exc),
                        duration_seconds=exc.duration_seconds,
                        timed_out=exc.timed_out,
                        retry_task_id=retry_task_id,
                        commit=True,
                    )
                    send_alert(
                        settings,
                        title="Autonomous ingest job failed",
                        body=(
                            f"run_date={run_date.isoformat()} stage={exc.stage} timed_out={int(exc.timed_out)} "
                            f"duration_seconds={round(exc.duration_seconds, 2)} retry_task_id={retry_task_id}"
                        ),
                        severity="error",
                    )
                    typer.echo(
                        f"ts={now_iso} job=ingest status=failed error={str(exc)[:200]} retry_task_id={retry_task_id}"
                    )
                except Exception as exc:
                    typer.echo(f"ts={now_iso} job=ingest status=failed error={str(exc)[:240]}")
                next_ingest_at = now_mono + ingest_every

            if due_discovery:
                did_work = True
                try:
                    discovery_result, _ = _run_with_watchdog(
                        "discovery_cycle",
                        settings.stage_timeout_seconds,
                        lambda: _run_hunt_cycle(run_date, profile_name=hunt_profile),
                    )
                    typer.echo(
                        " ".join(
                            [
                                f"ts={now_iso}",
                                "job=discovery",
                                f"discovery_run_id={discovery_result['discovery_run_id']}",
                                f"profile={discovery_result.get('profile', hunt_profile)}",
                                f"events_processed={discovery_result.get('events_seen', 0)}",
                                f"candidates={discovery_result.get('total_candidates', 0)}",
                                f"crm_candidates_rows={discovery_result['crm_candidates_rows']}",
                                f"manual_review_rows={discovery_result.get('manual_review_rows', 0)}",
                            ]
                        )
                    )
                except StageExecutionError as exc:
                    retry_task_id = _enqueue_retry_task(
                        lock_conn,
                        settings,
                        task_type="discovery_cycle",
                        payload={"run_date": run_date.isoformat(), "hunt_profile": hunt_profile},
                        reason=str(exc),
                    )
                    db.record_stage_failure(
                        lock_conn,
                        run_type="autonomous_loop",
                        run_date=run_date.isoformat(),
                        stage=exc.stage,
                        error_summary=str(exc),
                        duration_seconds=exc.duration_seconds,
                        timed_out=exc.timed_out,
                        retry_task_id=retry_task_id,
                        commit=True,
                    )
                    send_alert(
                        settings,
                        title="Autonomous discovery job failed",
                        body=(
                            f"run_date={run_date.isoformat()} stage={exc.stage} timed_out={int(exc.timed_out)} "
                            f"duration_seconds={round(exc.duration_seconds, 2)} retry_task_id={retry_task_id}"
                        ),
                        severity="error",
                    )
                    typer.echo(
                        f"ts={now_iso} job=discovery status=failed error={str(exc)[:200]} retry_task_id={retry_task_id}"
                    )
                except Exception as exc:
                    typer.echo(f"ts={now_iso} job=discovery status=failed error={str(exc)[:240]}")
                next_discovery_at = now_mono + discovery_every

            if due_score:
                did_work = True
                try:
                    score_result, _ = _run_with_watchdog(
                        "score_cycle",
                        settings.stage_timeout_seconds,
                        lambda: _run_score_cycle(run_date),
                    )
                    typer.echo(
                        " ".join(
                            [
                                f"ts={now_iso}",
                                "job=score",
                                f"run_id={score_result['run_id']}",
                                f"daily_scores_rows={score_result['daily_scores_rows']}",
                                f"review_queue_rows={score_result['review_queue_rows']}",
                                f"icp_coverage_rate={score_result['icp_coverage_rate']}",
                            ]
                        )
                    )
                except StageExecutionError as exc:
                    retry_task_id = _enqueue_retry_task(
                        lock_conn,
                        settings,
                        task_type="score_cycle",
                        payload={"run_date": run_date.isoformat()},
                        reason=str(exc),
                    )
                    db.record_stage_failure(
                        lock_conn,
                        run_type="autonomous_loop",
                        run_date=run_date.isoformat(),
                        stage=exc.stage,
                        error_summary=str(exc),
                        duration_seconds=exc.duration_seconds,
                        timed_out=exc.timed_out,
                        retry_task_id=retry_task_id,
                        commit=True,
                    )
                    send_alert(
                        settings,
                        title="Autonomous score job failed",
                        body=(
                            f"run_date={run_date.isoformat()} stage={exc.stage} timed_out={int(exc.timed_out)} "
                            f"duration_seconds={round(exc.duration_seconds, 2)} retry_task_id={retry_task_id}"
                        ),
                        severity="error",
                    )
                    typer.echo(
                        f"ts={now_iso} job=score status=failed error={str(exc)[:200]} retry_task_id={retry_task_id}"
                    )
                except Exception as exc:
                    typer.echo(f"ts={now_iso} job=score status=failed error={str(exc)[:240]}")
                next_score_at = now_mono + score_every

            if once and did_work:
                return

            time.sleep(float(sleep_seconds))
    finally:
        if lock_acquired:
            db.release_advisory_lock(lock_conn, lock_name=_AUTONOMOUS_LOCK_NAME, owner_id=lock_owner)
        lock_conn.close()


def _execute_retry_task(task: dict[str, object], settings: Settings) -> None:
    task_type = str(task.get("task_type", "") or "").strip().lower()
    payload_raw = str(task.get("payload_json", "{}") or "{}")
    try:
        payload = json.loads(payload_raw)
    except json.JSONDecodeError as exc:
        raise ValueError("invalid_retry_payload_json") from exc
    if not isinstance(payload, dict):
        raise ValueError("invalid_retry_payload")

    raw_run_date = payload.get("run_date")
    run_date_value = str(raw_run_date).strip() if raw_run_date is not None else ""
    run_date = parse_date(run_date_value or None, settings.run_timezone)
    if task_type == "run_daily":
        previous_flag = os.getenv("SIGNALS_DISABLE_AUTO_RETRY_ENQUEUE")
        os.environ["SIGNALS_DISABLE_AUTO_RETRY_ENQUEUE"] = "1"
        try:
            run_daily(
                date_str=run_date.isoformat(),
                live_max_accounts=None,
                live_workers_per_source=None,
                stage_timeout_seconds=None,
            )
        finally:
            if previous_flag is None:
                os.environ.pop("SIGNALS_DISABLE_AUTO_RETRY_ENQUEUE", None)
            else:
                os.environ["SIGNALS_DISABLE_AUTO_RETRY_ENQUEUE"] = previous_flag
        return

    if task_type == "ingest_cycle":
        _run_ingest_cycle(run_date)
        return
    if task_type == "score_cycle":
        _run_score_cycle(run_date)
        return
    if task_type == "discovery_cycle":
        profile = str(payload.get("hunt_profile", "light") or "light")
        _run_hunt_cycle(run_date, profile_name=profile)
        return
    raise ValueError(f"unsupported_retry_task_type={task_type}")


@app.command("retry-failures")
def retry_failures(
    limit: int = typer.Option(20, "--limit", min=1, help="Maximum due retry tasks to process in this run"),
) -> None:
    settings, conn, seeded = _bootstrap()
    del seeded
    processed = 0
    completed = 0
    rescheduled = 0
    quarantined = 0
    try:
        tasks = db.fetch_due_retry_tasks(conn, limit=limit)
        for task in tasks:
            processed += 1
            task_id = str(task["task_id"])
            task_type = str(task["task_type"])
            payload_json = str(task["payload_json"] or "{}")
            db.mark_retry_task_running(conn, task_id, commit=True)
            try:
                _execute_retry_task(dict(task), settings=settings)
                db.mark_retry_task_completed(conn, task_id, commit=True)
                completed += 1
            except Exception as exc:
                attempt_count = int(task.get("attempt_count", 0) or 0) + 1
                max_attempts = int(task.get("max_attempts", settings.retry_attempt_limit) or settings.retry_attempt_limit)
                if attempt_count >= max_attempts:
                    db.quarantine_retry_task(
                        conn,
                        task_id=task_id,
                        task_type=task_type,
                        payload_json=payload_json,
                        attempt_count=attempt_count,
                        error_summary=str(exc),
                        commit=True,
                    )
                    quarantined += 1
                    send_alert(
                        settings,
                        title="Retry task quarantined",
                        body=f"task_id={task_id} task_type={task_type} attempts={attempt_count} error={str(exc)[:300]}",
                        severity="error",
                    )
                else:
                    backoff_index = min(attempt_count, len(_RETRY_BACKOFF_SECONDS) - 1)
                    db.reschedule_retry_task(
                        conn,
                        task_id=task_id,
                        attempt_count=attempt_count,
                        due_at=_retry_due_iso(_RETRY_BACKOFF_SECONDS[backoff_index]),
                        error_summary=str(exc),
                        commit=True,
                    )
                    rescheduled += 1

        typer.echo(
            " ".join(
                [
                    f"processed={processed}",
                    f"completed={completed}",
                    f"rescheduled={rescheduled}",
                    f"quarantined={quarantined}",
                    f"queue_size={db.fetch_retry_queue_size(conn)}",
                    f"retry_depth={db.fetch_retry_depth(conn)}",
                    f"quarantine_size={db.fetch_quarantine_size(conn)}",
                ]
            )
        )
    finally:
        conn.close()


@app.command("replay-discovery-events")
def replay_discovery_events(
    date_str: str = typer.Option(None, "--date", help="Replay date YYYY-MM-DD"),
    include_processed: bool = typer.Option(
        False,
        "--include-processed/--only-failed",
        help="Replay both failed and processed events instead of failed-only",
    ),
) -> None:
    settings, conn, seeded = _bootstrap()
    run_date = parse_date(date_str, settings.run_timezone)
    del seeded
    try:
        replayed = db.requeue_external_discovery_events(
            conn,
            run_date=run_date.isoformat(),
            include_processed=include_processed,
        )
        typer.echo(
            " ".join(
                [
                    f"run_date={run_date.isoformat()}",
                    f"replayed_events={replayed}",
                    f"include_processed={int(include_processed)}",
                ]
            )
        )
    finally:
        conn.close()


@app.command("backfill-run-daily")
def backfill_run_daily(
    start_date: str = typer.Option(..., "--start-date", help="Backfill start date YYYY-MM-DD"),
    end_date: str = typer.Option(..., "--end-date", help="Backfill end date YYYY-MM-DD"),
    continue_on_error: bool = typer.Option(
        False,
        "--continue-on-error/--stop-on-error",
        help="Continue backfill even if one run fails",
    ),
) -> None:
    settings = load_settings()
    start = parse_date(start_date, settings.run_timezone)
    end = parse_date(end_date, settings.run_timezone)
    if end < start:
        raise typer.BadParameter("end-date must be on or after start-date")

    current = start
    succeeded = 0
    failed = 0
    while current <= end:
        try:
            run_daily(
                date_str=current.isoformat(),
                live_max_accounts=None,
                live_workers_per_source=None,
                stage_timeout_seconds=None,
            )
            succeeded += 1
        except Exception:
            logger.warning("backfill failed for date=%s", current.isoformat(), exc_info=True)
            failed += 1
            if not continue_on_error:
                raise
        current += timedelta(days=1)

    typer.echo(
        " ".join(
            [
                f"start_date={start.isoformat()}",
                f"end_date={end.isoformat()}",
                f"succeeded={succeeded}",
                f"failed={failed}",
            ]
        )
    )


@app.command("ops-metrics")
def ops_metrics(date_str: str = typer.Option(None, "--date", help="Metrics date YYYY-MM-DD")) -> None:
    settings, conn, seeded = _bootstrap()
    run_date = parse_date(date_str, settings.run_timezone)
    del seeded
    try:
        result = _persist_ops_metrics(conn, settings, run_date)
        path = settings.out_dir / f"ops_metrics_{run_date.strftime('%Y%m%d')}.csv"
        typer.echo(
            " ".join(
                [
                    f"run_date={run_date.isoformat()}",
                    f"ops_metrics_rows={result['ops_metrics_rows']}",
                    f"retry_depth={result['retry_depth']}",
                    f"retry_queue_size={result['retry_queue_size']}",
                    f"quarantine_size={result['quarantine_size']}",
                    f"lock_busy_24h={result['lock_busy_24h']}",
                    f"lock_release_missed_24h={result['lock_release_missed_24h']}",
                    f"handoff_success_rate={result['handoff_success_rate']}",
                    f"path={path}",
                ]
            )
        )
    finally:
        conn.close()


@app.command("alert-test")
def alert_test(
    title: str = typer.Option("Signals alert test", "--title"),
    body: str = typer.Option("Manual alert-test invocation.", "--body"),
    severity: str = typer.Option("info", "--severity"),
) -> None:
    settings = load_settings()
    ensure_project_directories([settings.out_dir])
    result = send_alert(settings, title=title, body=body, severity=severity)
    channels = ",".join(str(channel) for channel in result.get("delivered_channels", []))
    errors = ",".join(str(item) for item in result.get("errors", []))
    typer.echo(f"channels={channels} errors={errors}")


@app.command("serve-discovery-webhook")
def serve_discovery_webhook(
    host: str = typer.Option("127.0.0.1", "--host"),
    port: int = typer.Option(8787, "--port"),
    log_level: str = typer.Option("info", "--log-level"),
) -> None:
    try:
        import uvicorn  # type: ignore
    except Exception as exc:
        raise typer.BadParameter("uvicorn is required. Install project dependencies first.") from exc

    from src.discovery.webhook import app as discovery_app

    if discovery_app is None:
        raise typer.BadParameter("fastapi is required. Install project dependencies first.")

    uvicorn.run(discovery_app, host=host, port=port, log_level=log_level)


@app.command("serve-local-ui")
def serve_local_ui(
    host: str = typer.Option("127.0.0.1", "--host"),
    port: int = typer.Option(8788, "--port"),
    log_level: str = typer.Option("info", "--log-level"),
) -> None:
    try:
        import uvicorn  # type: ignore
    except Exception as exc:
        raise typer.BadParameter("uvicorn is required. Install project dependencies first.") from exc

    from src.ui.local_app import app as local_ui_app

    uvicorn.run(local_ui_app, host=host, port=port, log_level=log_level)


@app.command("serve-web")
def serve_web(
    host: str = typer.Option("0.0.0.0", "--host"),
    port: int = typer.Option(8080, "--port"),
    log_level: str = typer.Option("info", "--log-level"),
) -> None:
    """Launch the Signals pipeline web UI."""
    try:
        import uvicorn  # type: ignore
    except Exception as exc:
        raise typer.BadParameter("uvicorn is required. Install project dependencies first.") from exc

    from src.web.app import create_app

    web_app = create_app()
    uvicorn.run(web_app, host=host, port=port, log_level=log_level)


if __name__ == "__main__":
    app()
